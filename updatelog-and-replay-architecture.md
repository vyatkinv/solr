# Архитектура UpdateLog и механизм Update Replay в Apache Solr

## Оглавление

1. [Что такое UpdateLog и зачем он нужен](#1-что-такое-updatelog-и-зачем-он-нужен)
2. [Файловая структура на диске](#2-файловая-структура-на-диске)
3. [TransactionLog: один файл лога](#3-transactionlog-один-файл-лога)
4. [Формат записи в tlog (бинарный)](#4-формат-записи-в-tlog-бинарный)
5. [Машина состояний UpdateLog](#5-машина-состояний-updatelog)
6. [In-memory структуры: map, prevMap, prevMap2](#6-in-memory-структуры-map-prevmap-prevmap2)
7. [Запись обновлений: add(), delete(), deleteByQuery()](#7-запись-обновлений-add-delete-deletebyquery)
8. [Жизненный цикл вокруг commit](#8-жизненный-цикл-вокруг-commit)
9. [RTG: Real-Time Get через UpdateLog](#9-rtg-real-time-get-через-updatelog)
10. [lookupVersion(): поиск последней версии документа](#10-lookupversion-поиск-последней-версии-документа)
11. [RecentUpdates: снимок последних N версий](#11-recentupdates-снимок-последних-n-версий)
12. [Управление файлами: addOldLog(), ротация, удаление](#12-управление-файлами-addoldlog-ротация-удаление)
13. [bufferUpdates(): переход в режим буферизации](#13-bufferupdates-переход-в-режим-буферизации)
14. [applyBufferedUpdates(): применение буфера после recovery](#14-applybufferedupdates-применение-буфера-после-recovery)
15. [copyOverBufferingUpdates(): путь TLOG-реплики](#15-copyoverbufferingupdates-путь-tlog-реплики)
16. [LogReplayer: ядро механизма replay](#16-logreplayer-ядро-механизма-replay)
17. [recoverFromLog(): replay при старте ноды](#17-recoverfromlog-replay-при-старте-ноды)
18. [recoverFromCurrentLog(): TLOG-реплика становится лидером](#18-recoverfromcurrentlog-tlog-реплика-становится-лидером)
19. [applyPartialUpdates(): разрешение цепочки in-place обновлений](#19-applypartialupdates-разрешение-цепочки-in-place-обновлений)
20. [openRealtimeSearcher(): инвалидация кэша](#20-openrealtimesearcher-инвалидация-кэша)
21. [oldDeletes и deleteByQueries: кэши для удалений](#21-olddeletes-и-deletebyqueries-кэши-для-удалений)
22. [Инициализация UpdateLog при старте](#22-инициализация-updatelog-при-старте)
23. [Метрики UpdateLog](#23-метрики-updatelog)
24. [Ресурсы: потоки, память, диск, fsync](#24-ресурсы-потоки-память-диск-fsync)
25. [Полная временная шкала жизни документа в tlog](#25-полная-временная-шкала-жизни-документа-в-tlog)
26. [Ключевые классы и их взаимосвязи](#26-ключевые-классы-и-их-взаимосвязи)

---

## 1. Что такое UpdateLog и зачем он нужен

**UpdateLog** — это Write-Ahead Log (WAL) для Solr. Он решает несколько критических задач:

### Задача 1: Durability при краше (WAL)

Lucene `IndexWriter` при `addDocument()` помещает данные во внутренний буфер и не гарантирует сохранность до `commit()`. Если нода упадёт между обновлением и коммитом, документы потеряются из индекса.

UpdateLog записывает каждое обновление **немедленно** (до `commit()`) в транзакционный лог на диск. При старте после краша Solr проверяет незавершённые tlog-файлы и воспроизводит их в индекс.

### Задача 2: Real-Time Get (RTG)

Пользователь добавил документ — и тут же хочет его получить по ID. Без UpdateLog это невозможно до следующего soft commit (который открывает новый NRT-searcher). UpdateLog хранит in-memory карту `id → LogPtr`, поэтому RTG читает документ **напрямую из tlog-файла**, без открытия searcher'а.

### Задача 3: PeerSync (быстрое восстановление реплики)

При отставании реплики нужно передать только пропущенные обновления — без полной копии индекса. UpdateLog предоставляет `getVersions(N)` — список последних версий — и `lookup(version)` — сам документ по версии. На этом строится весь PeerSync.

### Задача 4: Буферизация при recovery

Пока реплика восстанавливается, лидер продолжает принимать обновления. UpdateLog в режиме `BUFFERING` сохраняет эти обновления в `buffer.tlog`, не трогая индекс. После синхронизации они применяются через `applyBufferedUpdates()`.

### Задача 5: Версионирование

UpdateLog является авторитетным источником для `lookupVersion()` — метода, который `DistributedUpdateProcessor` использует для проверки конфликтов версий и защиты от переупорядочивания.

---

## 2. Файловая структура на диске

```
<dataDir>/tlog/
├── tlog.0000000000000000001    ← старый лог (после commit)
├── tlog.0000000000000000002    ← ещё один старый
├── tlog.0000000000000000003    ← текущий активный (открыт для записи)
└── buffer.tlog.<nanoseconds>  ← только во время recovery
```

**Константы именования** (из `UpdateLog.java`):
```java
public static String LOG_FILENAME_PATTERN = "%s.%019d";  // 19-значный номер
public static String TLOG_NAME             = "tlog";
public static String BUFFER_TLOG_NAME      = "buffer.tlog";
```

Номер файла — монотонно возрастающий `long id`. Новый файл создаётся при каждом hard commit через `ensureLog()`.

**Параметры хранения** (из `solrconfig.xml`):
```xml
<updateLog>
  <str name="dir">${solr.ulog.dir:}</str>
  <int name="numRecordsToKeep">100</int>  <!-- версий в памяти -->
  <int name="maxNumLogsToKeep">10</int>   <!-- максимум файлов -->
  <int name="numVersionBuckets">65536</int>
</updateLog>
```

---

## 3. TransactionLog: один файл лога

**Файл:** `solr/core/src/java/org/apache/solr/update/TransactionLog.java`

Каждый объект `TransactionLog` управляет одним `tlog.*`-файлом через `FileChannel`.

### Ключевые поля

```java
long id;                          // числовой суффикс имени файла
Path tlog;                        // путь к файлу
FileChannel channel;              // NIO-канал для read/write
FastOutputStream fos;             // буферизованный вывод (65536 байт буфер)
int numRecords;                   // счётчик записей (включая заголовок)
boolean isBuffer;                 // true = это buffer.tlog
volatile boolean deleteOnClose = true;  // удалить при close()
AtomicInteger refcount = 1;       // reference counting для сборки мусора
Map<String, Integer> globalStringMap;   // string interning таблица
List<String> globalStringList;          // обратный индекс строк
```

### Reference counting

TransactionLog использует reference counting для управления временем жизни:
- `incref()` — захватить ссылку
- `decref()` — освободить; при `refcount == 0` → `close()` → удалить файл (если `deleteOnClose=true`)
- `try_incref()` — захватить если счётчик > 0 (защита от гонки с удалением)

Это позволяет безопасно использовать один файл из нескольких мест (RTG, PeerSync, replay) без преждевременного удаления.

### LogCodec: расширение JavaBin

```java
public class LogCodec extends JavaBinCodec {
    
    // Переопределяет writeExternString:
    // Если строка есть в globalStringMap → записать её индекс (1 байт)
    // Иначе → записать полную строку (как STR)
    
    // Переопределяет readExternString:
    // Индекс → взять из globalStringList (предзагружен из заголовка)
    
    // Поддержка UUID (дополнительный тип данных)
}
```

`globalStringList` — это таблица имён полей схемы. Заполняется при создании файла. При записи первого документа имена всех полей добавляются в таблицу. Далее каждый документ ссылается на имена полей по индексу вместо полных строк.

---

## 4. Формат записи в tlog (бинарный)

Каждая запись в tlog — это JavaBin-список, за которым следует 4-байтовый размер записи.

### Структура файла

```
┌──────────────────────────────────────────────────┐
│ HEADER                                           │
│  JavaBin MAP {                                   │
│    "SOLR_TLOG": 1,           ← magic + version  │
│    "strings": ["id","title","_version_",...]    │  ← global string table
│  }                                               │
│  <4 bytes: record size>                          │
├──────────────────────────────────────────────────┤
│ RECORD 1 (например, ADD)                        │
│  ARR[3]                                          │
│    INT  0x01              ← операция ADD         │
│    LONG 1620000001234     ← _version_            │
│    SOLRINPUTDOC {         ← полный документ      │
│      "id": "doc1",                               │
│      "title": "Hello",                           │
│      "_version_": 1620000001234                  │
│    }                                             │
│  <4 bytes: record size>                          │
├──────────────────────────────────────────────────┤
│ RECORD 2 (DELETE by ID)                         │
│  ARR[3]                                          │
│    INT  0x02              ← DELETE               │
│    LONG -1620000002000    ← версия (отрицательная)│
│    BYTEARR <id bytes>     ← индексированный ID   │
│  <4 bytes: record size>                          │
├──────────────────────────────────────────────────┤
│ RECORD 3 (DELETE_BY_QUERY)                      │
│  ARR[3]                                          │
│    INT  0x03              ← DBQ                  │
│    LONG -1620000003000                           │
│    STR  "category:old"   ← строка запроса       │
│  <4 bytes: record size>                          │
├──────────────────────────────────────────────────┤
│ RECORD 4 (UPDATE_INPLACE)                       │
│  ARR[5]                                          │
│    INT  0x08              ← UPDATE_INPLACE       │
│    LONG 1620000004000     ← новая версия         │
│    LONG 12345             ← prevPointer (байт в tlog) │
│    LONG 1620000001234     ← prevVersion          │
│    SOLRINPUTDOC { только DV поля }               │
│  <4 bytes: record size>                          │
├──────────────────────────────────────────────────┤
│ RECORD N (COMMIT — конец файла)                 │
│  ARR[3]                                          │
│    INT  0x04              ← COMMIT               │
│    LONG commitVersion                            │
│    STR  "SOLR_TLOG_END"  ← маркер конца         │
│  <4 bytes: record size>                          │
└──────────────────────────────────────────────────┘
```

### Константы операций

```java
// UpdateLog.java
public static final int ADD             = 0x01;
public static final int DELETE          = 0x02;
public static final int DELETE_BY_QUERY = 0x03;
public static final int COMMIT          = 0x04;
public static final int UPDATE_INPLACE  = 0x08;
public static final int OPERATION_MASK  = 0x0f;  // старшие биты = флаги

// Индексы полей в записи:
public static final int FLAGS_IDX        = 0;  // операция + флаги
public static final int VERSION_IDX      = 1;  // _version_
public static final int PREV_POINTER_IDX = 2;  // только для UPDATE_INPLACE
public static final int PREV_VERSION_IDX = 3;  // только для UPDATE_INPLACE
```

### 4-байтовый размер в конце каждой записи

После каждой записи пишется `fos.writeInt((int)(fos.size() - startRecordPosition))` — размер записи в байтах (метод `endRecord()`). Это позволяет `ReverseReader` читать лог **задом наперёд**: он знает конец записи, читает размер и прыгает на начало.

### Маркер конца файла `SOLR_TLOG_END`

Записывается только в COMMIT-записи. При старте Solr проверяет `endsWithCommit()` — читает последние `END_MESSAGE.length() + 4` байт файла. Если маркер есть → файл корректно завершён, воспроизведения не требует. Если нет → файл незавершён (краш) → нужен replay.

---

## 5. Машина состояний UpdateLog

```java
public enum State {
    REPLAYING(0),         // воспроизведение старых tlog при старте
    BUFFERING(1),         // recovery: новые обновления → buffer.tlog
    APPLYING_BUFFERED(2), // recovery завершён: применяем накопленный буфер
    ACTIVE(3)             // нормальная работа
}
```

### Диаграмма переходов

```
                     ┌────────────────────────────┐
                     │ Старт ноды                  │
                     │ init() → getRecentUpdates() │
                     │ startingVersions = ...      │
                     └────────────┬───────────────┘
                                  │
                     ┌────────────▼───────────────┐
        ┌────────────┤   ACTIVE (state=3)          ├────────────┐
        │            │   Нормальная работа         │            │
        │            └──────────────┬──────────────┘            │
        │                           │                            │
        │ recoverFromLog():         │ bufferUpdates():           │ recoverFromCurrentLog():
        │ незавершённые tlog        │ начало recovery            │ TLOG → лидер
        │ при старте                │                            │
        ▼                           ▼                            ▼
┌───────────────┐         ┌────────────────┐          ┌─────────────────┐
│  REPLAYING(0) │         │  BUFFERING(1)  │          │  REPLAYING(0)   │
│               │         │               │          │  (inSortedOrder │
│ LogReplayer   │         │ Входящие update│          │   = true)       │
│ воспроизводит │         │ идут в         │          │                 │
│ старые tlog   │         │ buffer.tlog   │          │ Применяет текущ.│
│               │         │               │          │ tlog к индексу  │
└───────┬───────┘         └───────┬───────┘          └────────┬────────┘
        │                         │                            │
        │ LogReplayer.run()       │ applyBufferedUpdates()     │ LogReplayer.run()
        │ завершился              │                            │ завершился
        │                         ▼                            │
        │              ┌─────────────────────┐                 │
        │              │  APPLYING_BUFFERED  │                 │
        │              │       (2)           │                 │
        │              │                     │                 │
        │              │ LogReplayer читает  │                 │
        │              │ buffer.tlog и       │                 │
        │              │ применяет в индекс  │                 │
        │              └──────────┬──────────┘                 │
        │                         │                            │
        │                LogReplayer.run()                      │
        │                 state = ACTIVE                        │
        └─────────────────────────▼────────────────────────────┘
                           ┌─────────────┐
                           │  ACTIVE(3)  │
                           └─────────────┘
```

### Переходы под Write Lock

Переходы состояния всегда происходят под `versionInfo.blockUpdates()` (write lock), чтобы исключить гонку: никакое обновление не может увидеть "старое" состояние после того, как переход совершён.

---

## 6. In-memory структуры: map, prevMap, prevMap2

UpdateLog ведёт **три in-memory карты** для маппинга `id → LogPtr`:

```java
protected Map<BytesRef, LogPtr> map      // текущий tlog
protected Map<BytesRef, LogPtr> prevMap  // предыдущий tlog (во время commit)
protected Map<BytesRef, LogPtr> prevMap2 // ещё более старый (глубокий commit)

protected TransactionLog prevMapLog;     // tlog для prevMap
protected TransactionLog prevMapLog2;    // tlog для prevMap2
```

### Зачем три карты?

Во время commit происходит переключение `tlog` (старый → `prevTlog`, создаётся новый). В это время могут одновременно:
1. Читаться RTG-запросы к документам из старого tlog (→ нужен `prevMap`)
2. Писаться новые обновления в новый `map`
3. Открываться новый searcher (что переключает `prevMap → prevMap2 → null`)

Три карты обеспечивают корректность RTG на протяжении этих двух overlapping commit-фаз.

### LogPtr

```java
public static class LogPtr {
    final long pointer;          // байтовое смещение в tlog-файле
    final long version;          // _version_ записи
    final long previousPointer;  // для UPDATE_INPLACE: указатель на предыдущую запись
                                 // = -1 если не in-place
}
```

`pointer` — это абсолютная байтовая позиция начала записи в файле. Используется в `TransactionLog.lookup(pos)` для произвольного доступа к записи.

### newMap() — ротация карт при commit

```java
// UpdateLog.java
protected void newMap() {
    prevMap2 = prevMap;      // самая старая — отбрасывается
    prevMapLog2 = prevMapLog;

    prevMap = map;           // текущая становится предыдущей
    prevMapLog = tlog;

    map = new HashMap<>();   // создать новую пустую
}
```

Вызывается в `preCommit()` и `preSoftCommit()`. После `postSoftCommit()` старые карты очищаются вызовом `clearOldMaps()`.

---

## 7. Запись обновлений: add(), delete(), deleteByQuery()

### add(AddUpdateCommand cmd)

```java
public void add(AddUpdateCommand cmd, boolean clearCaches) {
    synchronized (this) {
        
        // 1. Если BUFFERING → писать в buffer.tlog, не в основной
        if ((cmd.getFlags() & UpdateCommand.BUFFERING) != 0) {
            ensureBufferTlog();
            bufferTlog.write(cmd);
            return;
        }
        
        // 2. Найти prevPointer (для in-place update)
        long prevPointer = getPrevPointerForUpdate(cmd);
        
        // 3. Если это REPLAY из старого tlog (recoverFromLog) — НЕ писать снова
        if (!updateFromOldTlogs(cmd)) {
            ensureLog();             // создать tlog если его нет
            pos = tlog.write(cmd, prevPointer);  // записать в tlog
        }
        
        // 4. Обновить in-memory map (если не clearCaches)
        if (!clearCaches) {
            LogPtr ptr = new LogPtr(pos, cmd.getVersion(), prevPointer);
            map.put(cmd.getIndexedId(), ptr);
        } else {
            // clearCaches=true → открыть новый searcher и очистить maps
            openRealtimeSearcher();
        }
    }
}
```

**Условие `updateFromOldTlogs`:**
```java
private boolean updateFromOldTlogs(UpdateCommand cmd) {
    return (cmd.getFlags() & UpdateCommand.REPLAY) != 0 && state == State.REPLAYING;
}
```

При воспроизведении старых tlog'ов мы **не** пишем их снова в новый tlog (иначе получим дублирование). Но обновляем `map` для RTG-доступа в процессе recovery.

### delete(DeleteUpdateCommand cmd)

Аналогично `add()`, но также:
```java
oldDeletes.put(br, ptr);  // кэш удалений для lookupVersion()
```

Удалённые документы не хранятся в Lucene-индексе, поэтому `lookupVersion()` не может найти их там. Кэш `oldDeletes` (последние 1000 удалений) восполняет этот пробел.

### deleteByQuery(DeleteUpdateCommand cmd)

```java
public void deleteByQuery(DeleteUpdateCommand cmd) {
    synchronized (this) {
        // (буферизация и запись аналогично)
        pos = tlog.writeDeleteByQuery(cmd);
        
        // Только если не TLOG-реплика (у неё нет индекса для инвалидации):
        if ((cmd.getFlags() & UpdateCommand.IGNORE_INDEXWRITER) == 0) {
            openRealtimeSearcher();       // ← инвалидировать RTG-кэш!
            trackDeleteByQuery(cmd.getQuery(), cmd.getVersion());
        }
    }
}
```

После DBQ мы не знаем, какие документы были удалены. Поэтому:
1. Открывается новый searcher (который учитывает удаления из IndexWriter)
2. Карты `map`/`prevMap`/`prevMap2` очищаются
3. Запрос регистрируется в `deleteByQueries` (для RTG: если версия документа < версии DBQ → документ мог быть удалён)

---

## 8. Жизненный цикл вокруг commit

Sequence diagram commit-протокола между `DirectUpdateHandler2` и `UpdateLog`:

```
DirectUpdateHandler2.commit():
    │
    ├─ [hard commit только]:
    │   updateLock → synchronized block
    │
    ├─ ulog.preCommit(cmd)
    │     synchronized(this):
    │       newMap()                    ← ротация map → prevMap
    │       globalStrings = prevTlog.getGlobalStrings()
    │       prevTlog = tlog             ← старый tlog становится prevTlog
    │       tlog = null                 ← следующий add() создаст новый
    │       id++                        ← номер следующего файла
    │
    ├─ SolrIndexWriter.commit()         ← Lucene fsync
    │
    ├─ ulog.postCommit(cmd)
    │     synchronized(this):
    │       prevTlog.writeCommit(cmd)   ← записать COMMIT-маркер в старый tlog
    │       addOldLog(prevTlog, true)   ← передать в deque logs
    │       prevTlog = null
    │
    └─ Открыть новый searcher
```

```
DirectUpdateHandler2.softCommit():
    │
    ├─ ulog.preSoftCommit(cmd)
    │     synchronized(this):
    │       newMap()                    ← ротация (но НЕ переключаем tlog!)
    │       map = new HashMap<>()       ← чистая карта для новых обновлений
    │
    ├─ core.openNewSearcher()           ← открыть NRT searcher
    │
    └─ ulog.postSoftCommit(cmd)
          synchronized(this):
            clearOldMaps()              ← prevMap = prevMap2 = null
                                        ← теперь searcher полностью актуален
```

### Почему soft commit тоже ротирует map?

После soft commit открывается новый searcher, который видит все документы, записанные в IndexWriter до этого момента. Старые `prevMap`/`prevMap2` более не нужны для RTG — новый searcher покроет их. Поэтому их можно очистить.

---

## 9. RTG: Real-Time Get через UpdateLog

**Путь Real-Time Get для документа по ID:**

```
RealTimeGetComponent.process():
    
    1. getIndexedId(req) → BytesRef idBytes
    
    2. ulog.lookup(idBytes):
       synchronized(this):
         entry = map.get(idBytes)
         lookupLog = tlog
         
         if entry == null && prevMap != null:
           entry = prevMap.get(idBytes)
           lookupLog = prevMapLog
         
         if entry == null && prevMap2 != null:
           entry = prevMap2.get(idBytes)
           lookupLog = prevMapLog2
         
         if entry == null → return null (нет в tlog)
         lookupLog.incref()   ← защита от удаления файла
       
       // ВНЕ synchronized блока:
       return lookupLog.lookup(entry.pointer)
                              ↑
                    TransactionLog.lookup(pos):
                      synchronized(this): fos.flush()
                      is = channelInputStream(channel, pos)
                      return LogCodec.readVal(is)  ← десериализовать запись
    
    3. Если entry == null:
       → Поиск в открытом searcher'е (обычный поиск по индексу)
    
    4. Если версия < min(DBQ версий):
       → документ мог быть удалён DBQ
       → Дополнительная проверка через searcher
```

**Ключевая особенность:** lookup выполняется **вне synchronized блока** (после захвата `incref`). Это позволяет конкурентную RTG-обработку. Единственное, что синхронизировано — получение `LogPtr` и `incref()`.

---

## 10. lookupVersion(): поиск последней версии документа

`lookupVersion()` используется `DistributedUpdateProcessor` для проверки конфликтов и reordering.

```java
// UpdateLog.java:1123
public Long lookupVersion(BytesRef indexedId) {
    
    // 1. Поиск в in-memory картах (самые свежие)
    synchronized (this) {
        entry = map.get(indexedId);
        // → prevMap, prevMap2 если не нашли
    }
    
    if (entry != null) {
        return entry.version;  // нашли версию в tlog-картах
    }
    
    // 2. Поиск в Lucene-индексе через DocValues
    Long version = versionInfo.getVersionFromIndex(indexedId);
    if (version != null) {
        return version;
    }
    
    // 3. Поиск в кэше удалений (oldDeletes)
    // Удалённые документы не хранятся в индексе!
    synchronized (this) {
        entry = oldDeletes.get(indexedId);
    }
    if (entry != null) {
        return entry.version;
    }
    
    return null;  // версия неизвестна (документ никогда не индексировался)
}
```

**Трёхуровневый поиск:**
1. **tlog maps** — самые свежие, uncommitted изменения
2. **Lucene DocValues** — committed данные (поле `_version_` как NumericDocValuesField)
3. **oldDeletes cache** — удалённые документы (их нет в индексе)

---

## 11. RecentUpdates: снимок последних N версий

`RecentUpdates` — временный объект (closeable), используемый для:
- PeerSync: получить список последних N версий
- Инициализации при старте: `getStartingVersions()`

### Создание снимка

```java
public RecentUpdates getRecentUpdates() {
    Deque<TransactionLog> logList;
    synchronized (this) {
        logList = new ArrayDeque<>(logs);
        for (TransactionLog log : logList) log.incref();
        
        // Добавить текущие логи в начало (новейшие первые):
        if (prevTlog != null) { prevTlog.incref(); logList.addFirst(prevTlog); }
        if (tlog != null)     { tlog.incref();     logList.addFirst(tlog); }
        if (bufferTlog != null){ bufferTlog.incref(); logList.addFirst(bufferTlog); }
    }
    return new RecentUpdates(logList);  // читает логи в конструкторе
}
```

### RecentUpdates.update() — обратное чтение логов

```java
// RecentUpdates.java
private void update() {
    for (TransactionLog oldLog : logList) {  // от новейшего к старейшему
        
        // Читать лог ЗАДОМ НАПЕРЁД через ReverseReader
        TransactionLog.ReverseReader reader = oldLog.getReverseReader();
        
        while (numUpdates < numRecordsToKeep) {
            Object o = reader.next();  // следующая запись снизу
            if (o == null) break;
            
            List<?> entry = (List<?>) o;
            int oper = ((Integer) entry.get(FLAGS_IDX)) & OPERATION_MASK;
            long version = (Long) entry.get(VERSION_IDX);
            
            // Если это buffer.tlog → пометить в bufferUpdates
            if (oldLog.isBuffer) bufferUpdates.add(version);
            
            Update update = new Update();
            update.log = oldLog;
            update.pointer = reader.position();  // позиция в файле
            update.version = version;
            
            updatesForLog.add(update);
            updates.put(version, update);  // версия → Update (для lookup)
            
            if (oper == DELETE_BY_QUERY) deleteByQueryList.add(update);
            if (oper == DELETE)          deleteList.add(new DeleteUpdate(...));
        }
    }
}
```

### FSReverseReader: обратное чтение файла

Работает по 4-байтовым размерам в конце каждой записи:

```
Файл: [Header] [Record1][size1] [Record2][size2] [Record3][size3]
                                                               ↑
ReverseReader начинает здесь: читает size3, прыгает назад на size3 байт
```

```java
public FSReverseReader() {
    prevPos = sz - 4;              // позиция последнего size
    fis.seek(prevPos);
    nextLength = fis.readInt();    // размер последней записи
}

public Object next() {
    long recordStart = prevPos - thisLength;   // начало записи
    prevPos = recordStart - 4;                 // позиция предыдущего size
    fis.seek(prevPos);
    nextLength = fis.readInt();                // размер предыдущей записи
    
    fis.seek(recordStart + 4);                 // перейти к данным
    return codec.readVal(fis);
}
```

**Оптимизация SolrInputDocument при обратном чтении:** `FSReverseReader` использует специализированный `LogCodec`, который **пропускает** десериализацию `SolrInputDocument` (возвращает `null`). Это существенно быстрее при построении `RecentUpdates` — нам нужны только версии и позиции, не сами документы.

### getVersions(N) — список N последних версий

```java
public List<Long> getVersions(int n, long maxVersion) {
    List<Long> ret = new ArrayList<>(n);
    LongSet set = new LongSet(n);  // для дедупликации
    
    for (List<Update> singleList : updateList) {  // по каждому логу
        for (Update ptr : singleList) {
            if (Math.abs(ptr.version) > Math.abs(maxVersion)) continue;
            if (!set.add(ptr.version)) continue;  // дубликат
            ret.add(ptr.version);
            if (--n <= 0) return ret;
        }
    }
    return ret;
}
```

Версии возвращаются в порядке "новейшие первые" (т.к. каждый tlog читался с конца).

### lookup(long version) — документ по версии

```java
public Object lookup(long version) {
    Update update = updates.get(version);  // из HashMap версия → Update
    if (update == null) return null;
    return update.log.lookup(update.pointer);  // читать из tlog по позиции
}
```

Используется в `RealTimeGetComponent` при обработке `getUpdates=RANGE` запросов (PeerSync).

---

## 12. Управление файлами: addOldLog(), ротация, удаление

### addOldLog() — добавить завершённый лог в deque

```java
protected synchronized void addOldLog(TransactionLog oldLog, boolean removeOld) {
    numOldRecords += oldLog.numRecords();
    int currRecords = numOldRecords + (tlog != null ? tlog.numRecords() : 0);
    
    while (removeOld && logs.size() > 0) {
        TransactionLog oldest = logs.peekLast();
        int nrec = oldest.numRecords();
        
        // Удалить самый старый если:
        // 1. Без него у нас ещё >= numRecordsToKeep записей, ИЛИ
        // 2. Количество файлов уже >= maxNumLogsToKeep
        if (currRecords - nrec >= numRecordsToKeep
            || (maxNumLogsToKeep > 0 && logs.size() >= maxNumLogsToKeep)) {
            
            currRecords -= nrec;
            numOldRecords -= nrec;
            logs.removeLast().decref();  // decref → удалит файл если refcount=0
            continue;
        }
        break;
    }
    
    logs.addFirst(oldLog);  // добавить в начало (новейший)
}
```

**Инвариант:** Deque `logs` всегда содержит не менее `numRecordsToKeep` записей суммарно, но не более `maxNumLogsToKeep` файлов.

### ensureLog() — создать новый tlog при необходимости

```java
protected void ensureLog() {
    if (tlog == null) {
        String newLogName = String.format(LOG_FILENAME_PATTERN, TLOG_NAME, id);
        tlog = newTransactionLog(tlogDir.resolve(newLogName), globalStrings, false);
    }
}
```

Вызывается лениво при первом `add()`/`delete()` после commit'а. До первого обновления `tlog == null`.

---

## 13. bufferUpdates(): переход в режим буферизации

Вызывается `RecoveryStrategy` в начале каждой итерации retry-цикла.

```java
// UpdateLog.java:1729
public void bufferUpdates() {
    versionInfo.blockUpdates();  // ← WRITE LOCK: остановить все ADD/DELETE
    try {
        if (state != State.ACTIVE && state != State.BUFFERING) {
            log.warn("Unexpected state for bufferUpdates: {}", state);
            return;
        }
        
        // Удалить предыдущий bufferTlog (если был от неудачного recovery)
        dropBufferTlog();
        deleteBufferLogs();     // удалить все buffer.tlog.* файлы
        
        recoveryInfo = new RecoveryInfo();  // свежая статистика
        state = State.BUFFERING;
    } finally {
        versionInfo.unblockUpdates();
    }
}
```

После этого `DistributedUpdateProcessor` в `shouldBufferUpdate()` видит состояние `BUFFERING` и записывает входящие обновления с флагом `BUFFERING`, что направляет их в `buffer.tlog` вместо основного `tlog`.

### ensureBufferTlog()

```java
protected void ensureBufferTlog() {
    if (bufferTlog != null) return;
    // Имя файла содержит System.nanoTime() для уникальности
    String newLogName = String.format(LOG_FILENAME_PATTERN, BUFFER_TLOG_NAME, System.nanoTime());
    bufferTlog = newTransactionLog(tlogDir.resolve(newLogName), globalStrings, false);
    bufferTlog.isBuffer = true;
}
```

Файл создаётся **лениво** при первом буферизованном обновлении. Если во время recovery не пришло ни одного обновления — `bufferTlog == null` и `applyBufferedUpdates()` просто переходит в ACTIVE без replay.

---

## 14. applyBufferedUpdates(): применение буфера после recovery

Вызывается `RecoveryStrategy.replay()` после успешного PeerSync или Replication.

```java
// UpdateLog.java:1786
public Future<RecoveryInfo> applyBufferedUpdates() {
    versionInfo.blockUpdates();
    try {
        cancelApplyBufferUpdate = false;
        if (state != State.BUFFERING) return null;
        
        synchronized (this) {
            if (bufferTlog == null) {
                state = State.ACTIVE;   // буфер пуст → сразу активны
                return null;
            }
            bufferTlog.incref();        // защита от удаления
        }
        
        state = State.APPLYING_BUFFERED;
    } finally {
        versionInfo.unblockUpdates();
    }
    
    ExecutorCompletionService<RecoveryInfo> cs = new ExecutorCompletionService<>(recoveryExecutor);
    LogReplayer replayer = new LogReplayer(Collections.singletonList(bufferTlog), true);
    //                                                           activeLog=true ↑
    // activeLog=true означает: читать до КОНЦА файла (файл ещё открыт для записи!)
    
    return cs.submit(
        () -> {
            replayer.run();
            dropBufferTlog();    // удалить buffer.tlog после успешного применения
        },
        recoveryInfo
    );
}
```

**`activeLog=true`:** это ключевой флаг для `LogReplayer`. Когда `bufferTlog` ещё открыт для записи (recovery не завершён, лидер может слать обновления), `LogReplayer` не останавливается на конце файла — он ждёт новых данных. Специальная логика "finishing":

```java
// LogReplayer.doReplay():
if (o == null && activeLog) {
    if (!finishing) {
        // 1. Дождаться всех запущенных задач
        waitForAllUpdatesGetExecuted(pendingTasks);
        // 2. Заблокировать новые обновления
        versionInfo.blockUpdates();
        finishing = true;
        // 3. Ещё раз попробовать прочитать (могло успеть записаться)
        o = tlogReader.next();
    }
    // else: finishing=true + null = файл исчерпан и заблокирован → выход
}
```

Это гарантирует, что **все** буферизованные обновления применены, даже те, что пришли в последние миллисекунды перед блокировкой.

---

## 15. copyOverBufferingUpdates(): путь TLOG-реплики

TLOG-реплика не индексирует документы локально. При recovery она не использует `applyBufferedUpdates()`. Вместо этого:

```java
// UpdateLog.java:1278
public void copyOverBufferingUpdates(CommitUpdateCommand cuc) {
    versionInfo.blockUpdates();
    try {
        synchronized (this) {
            state = State.ACTIVE;          // сразу переходим в ACTIVE
            if (bufferTlog == null) return;
            
            // Скопировать обновления из bufferTlog → текущий tlog
            // только те, у которых |version| > cuc.getVersion()
            copyOverOldUpdates(cuc.getVersion(), bufferTlog);
            
            dropBufferTlog();  // удалить buffer.tlog
        }
    } finally {
        versionInfo.unblockUpdates();
    }
}
```

### copyOverOldUpdates() — перенос записей

```java
public void copyOverOldUpdates(long commitVersion, TransactionLog oldTlog) {
    TransactionLog.LogReader logReader = oldTlog.getReader(0);
    while ((o = logReader.next()) != null) {
        int oper = ((Integer) entry.get(0)) & OPERATION_MASK;
        long version = (Long) entry.get(1);
        
        // Копировать только обновления НОВЕЕ commitVersion
        if (Math.abs(version) > commitVersion) {
            switch (oper) {
                case ADD:
                case UPDATE_INPLACE:
                    AddUpdateCommand cmd = convertTlogEntryToAddUpdateCommand(...);
                    cmd.setFlags(UpdateCommand.IGNORE_AUTOCOMMIT);
                    add(cmd);     // записать в текущий tlog
                    break;
                case DELETE: ...
                case DELETE_BY_QUERY: ...
            }
        }
    }
}
```

**Результат:** буферизованные обновления переносятся в основной tlog для RTG-доступа. Индекс не трогается — TLOG-реплика получит его через фоновую репликацию от лидера.

---

## 16. LogReplayer: ядро механизма replay

`LogReplayer` — внутренний класс `UpdateLog`, реализует `Runnable`. Читает tlog-файлы и применяет каждую запись через `UpdateRequestProcessorChain`.

### Инициализация

```java
// LogReplayer.run():
ModifiableSolrParams params = new ModifiableSolrParams();
params.set(DISTRIB_UPDATE_PARAM, FROMLEADER.toString());  // ← не рассылать!
params.set(DistributedUpdateProcessor.LOG_REPLAY, "true");

req = new LocalSolrQueryRequest(uhandler.core, params);
rsp = new SolrQueryResponse();
SolrRequestInfo.setRequestInfo(new SolrRequestInfo(req, rsp));
```

Флаги `FROMLEADER` и `LOG_REPLAY=true` гарантируют:
- `DistributedUpdateProcessor` не пытается пересылать обновления другим нодам
- Процессор принимает версию из команды (не генерирует новую)

### Параллельное воспроизведение

`LogReplayer` использует `OrderedExecutor` для параллельного replay:

```java
OrderedExecutor executor = inSortedOrder ? null : req.getCoreContainer().getReplayUpdatesExecutor();
```

```java
private void execute(UpdateCommand cmd, OrderedExecutor executor, ...) {
    if (executor != null) {
        // Получить bucket hash (то же что DUP использует для локинга)
        Integer bucketHash = getBucketHash(cmd);
        
        executor.execute(bucketHash, () -> {
            invokeCmdOnProc(cmd, procTl.get());  // ThreadLocal processor
            pendingTasks.decrementAndGet();
        });
        pendingTasks.incrementAndGet();
    } else {
        // Однопоточное выполнение (для activeLog=true финальной фазы, для DBQ)
        invokeCmdOnProc(cmd, procTl.get());
    }
}
```

`OrderedExecutor` гарантирует: обновления с одинаковым `bucketHash` (т.е. для одного документа) выполняются последовательно. Обновления для разных документов могут выполняться параллельно.

### DBQ всегда однопоточно

```java
case UpdateLog.DELETE_BY_QUERY:
    waitForAllUpdatesGetExecuted(pendingTasks);   // ← ждать все pending
    execute(cmd, null, ...);                       // ← executor=null, однопоточно
```

DBQ должен выполняться после всех предшествующих обновлений и до всех последующих.

### Флаги команд при replay

```java
cmd.setFlags(UpdateCommand.REPLAY | UpdateCommand.IGNORE_AUTOCOMMIT);
```

- `REPLAY` — сигнал для `UpdateLog.add()`: не писать в новый tlog (уже из tlog читаем)
- `IGNORE_AUTOCOMMIT` — не триггерить autocommit tracker

### Финальный commit

После исчерпания всех записей:
```java
CommitUpdateCommand cmd = new CommitUpdateCommand(req, false);
cmd.setVersion(commitVersion);     // версия последнего COMMIT из tlog
cmd.softCommit = false;
cmd.waitSearcher = true;
cmd.setFlags(UpdateCommand.REPLAY);

uhandler.commit(cmd);             // hard commit
```

Если воспроизводится старый (не активный) tlog — в конец файла записывается COMMIT-маркер, чтобы при следующем старте он не воспроизводился заново.

---

## 17. recoverFromLog(): replay при старте ноды

Вызывается при старте `SolrCore` если есть незавершённые tlog-файлы.

```java
// UpdateLog.java:1197
public Future<RecoveryInfo> recoverFromLog() {
    recoveryInfo = new RecoveryInfo();
    
    List<TransactionLog> recoverLogs = new ArrayList<>();
    
    // Проверить первые 2 лога (newestLogsOnStartup):
    for (TransactionLog ll : newestLogsOnStartup) {
        if (!ll.try_incref()) continue;
        
        if (ll.endsWithCommit()) {
            // Файл корректно завершён → воспроизведение не нужно
            ll.closeOutput();
            ll.decref();
            continue;
        }
        // Незавершённый файл → нужен replay
        recoverLogs.add(ll);
    }
    
    if (recoverLogs.isEmpty()) return null;  // ничего не нужно делать
    
    // Заблокировать обновления, установить состояние REPLAYING
    versionInfo.blockUpdates();
    try {
        state = State.REPLAYING;
        deleteByQueries.clear();
        oldDeletes.clear();
    } finally {
        versionInfo.unblockUpdates();
    }
    
    LogReplayer replayer = new LogReplayer(recoverLogs, false);
    //                                              activeLog=false ↑
    // activeLog=false: читать до конца файла (фиксированный), затем завершить
    
    return cs.submit(replayer, recoveryInfo);
}
```

### Почему только 2 последних файла?

```java
// init():
for (TransactionLog ll : logs) {
    newestLogsOnStartup.addFirst(ll);
    if (newestLogsOnStartup.size() >= 2) break;
}
```

В нормальных условиях при краше незавершёнными могут оказаться только `tlog` и `prevTlog` (два последних). Более старые файлы всегда завершены COMMIT-маркером (потому что `postCommit()` пишет маркер перед передачей в `addOldLog()`).

### inSortedOrder=false для recoverFromLog

При обычном replay из старых логов (`recoverFromLog`) порядок записей уже правильный (хронологический), поэтому `inSortedOrder=false` — использовать `LogReader` (прямое чтение).

---

## 18. recoverFromCurrentLog(): TLOG-реплика становится лидером

Когда TLOG-реплика избирается лидером, она должна применить свой tlog к индексу (потому что сам индекс она получила репликацией от предыдущего лидера, но локально не индексировала обновления).

```java
// UpdateLog.java:1250
public Future<RecoveryInfo> recoverFromCurrentLog() {
    if (tlog == null) return null;  // нечего воспроизводить
    
    map.clear();                    // очистить стале in-memory версии
    recoveryInfo = new RecoveryInfo();
    tlog.incref();
    
    // Заблокировать и установить REPLAYING
    versionInfo.blockUpdates();
    try { state = State.REPLAYING; }
    finally { versionInfo.unblockUpdates(); }
    
    // inSortedOrder=true: сортировать записи по версии перед применением
    LogReplayer replayer = new LogReplayer(Collections.singletonList(tlog), false, true);
    return cs.submit(replayer, recoveryInfo);
}
```

**`inSortedOrder=true`:** TLOG-реплика получала обновления от лидера, но могла получить их не совсем в том порядке (race conditions). Поэтому перед применением используется `SortedLogReader` — сначала строит `TreeMap<version → position>`, затем читает записи в порядке возрастания версии.

### SortedLogReader

```java
// TransactionLog.java
public class SortedLogReader extends LogReader {
    private TreeMap<Long, Long> versionToPos;  // version → file position
    
    public Object next() {
        if (versionToPos == null) {
            // Первый вызов: полное сканирование для построения индекса
            versionToPos = new TreeMap<>();
            while ((o = super.next()) != null) {
                long version = Math.abs((Long) entry.get(VERSION_IDX));
                versionToPos.put(version, currentPos);
            }
        }
        
        if (inOrder) {
            return super.next();   // уже в порядке, не перематывать
        } else {
            // Читать по отсортированным позициям
            long pos = iterator.next();
            fis.seek(pos);
            return super.next();
        }
    }
}
```

---

## 19. applyPartialUpdates(): разрешение цепочки in-place обновлений

Когда для RTG нужно получить актуальный документ, у которого есть цепочка in-place обновлений (не полный документ, а только delta), нужно пройти по цепочке `prevPointer`.

```java
// UpdateLog.java:956
public synchronized long applyPartialUpdates(
    BytesRef id,
    long prevPointer,   // указатель на предыдущую запись
    long prevVersion,   // версия предыдущей записи
    Set<String> onlyTheseFields,  // null = все поля
    SolrDocumentBase latestPartialDoc) {  // документ для заполнения

    List<TransactionLog> lookupLogs = Arrays.asList(tlog, prevMapLog, prevMapLog2);

    while (prevPointer >= 0) {
        List<?> entry = getEntryFromTLog(prevPointer, prevVersion, lookupLogs);
        
        if (entry == null) {
            // Запись не найдена — log был ротирован
            return prevPointer;  // вернуть указатель для поиска в индексе
        }
        
        int flags = (int) entry.get(FLAGS_IDX);
        
        if ((flags & ADD) == ADD) {
            // Нашли полный документ — применить все его поля
            SolrInputDocument fullDoc = (SolrInputDocument) entry.get(entry.size() - 1);
            applyOlderUpdates(latestPartialDoc, fullDoc, onlyTheseFields);
            return 0;  // успех: нашли полный документ в tlog
        }
        
        // Ещё один UPDATE_INPLACE — применить его поля (не перезаписывая уже известные)
        SolrInputDocument partialDoc = (SolrInputDocument) entry.get(entry.size() - 1);
        applyOlderUpdates(latestPartialDoc, partialDoc, onlyTheseFields);
        
        // Перейти к следующему в цепочке
        prevPointer = (long) entry.get(PREV_POINTER_IDX);
        prevVersion = (long) entry.get(PREV_VERSION_IDX);
        
        // Оптимизация: если уже нашли все нужные поля
        if (onlyTheseFields != null && latestPartialDoc.keySet().containsAll(onlyTheseFields)) {
            return 0;
        }
    }
    
    return -1;  // цепочка ведёт в индекс (prevPointer == -1)
}
```

**Цепочка prevPointer:**

```
tlog (от нового к старому):

[UPDATE_INPLACE v=1000, prevPtr=500, prevVer=800, {price: 99}]    ← самый новый
        ↓ prevPtr=500
[UPDATE_INPLACE v=800,  prevPtr=200, prevVer=600, {stock: 5}]
        ↓ prevPtr=200
[ADD v=600, {id:"prod1", price:100, stock:10, title:"Widget"}]     ← полный документ

Результирующий RTG-документ: {id:"prod1", price:99, stock:5, title:"Widget"}
```

---

## 20. openRealtimeSearcher(): инвалидация кэша

Вызывается в трёх случаях:
1. После `deleteByQuery()` — DBQ мог удалить произвольные документы
2. В `add(cmd, clearCaches=true)` — явный запрос на сброс кэша
3. После `replay()` в `RecoveryStrategy` — индекс изменился, кэши устарели

```java
public void openRealtimeSearcher() {
    synchronized (this) {
        // Открыть новый NRT IndexReader поверх текущего IndexWriter
        // Это гарантирует, что следующий RTG-запрос через searcher
        // увидит актуальные данные
        RefCounted<SolrIndexSearcher> holder = uhandler.core.openNewSearcher(true, true);
        holder.decref();
        
        // Очистить in-memory карты — теперь searcher авторитетен
        if (map != null) map.clear();
        if (prevMap != null) prevMap.clear();
        if (prevMap2 != null) prevMap2.clear();
    }
}
```

После очистки карт RTG-запросы пойдут через searcher (индекс), а не через tlog. Это важно: если мы применили изменения в Lucene IndexWriter, они должны быть видны через searcher, а не через старые tlog-записи.

---

## 21. oldDeletes и deleteByQueries: кэши для удалений

### oldDeletes (кэш удалений по ID)

```java
// LRU LinkedHashMap с ограниченным размером
protected LinkedHashMap<BytesRef, LogPtr> oldDeletes =
    new OldDeletesLinkedHashMap(numDeletesToKeep = 1000);

// Переопределяет removeEldestEntry для автоматического вытеснения:
protected boolean removeEldestEntry(Map.Entry<BytesRef, LogPtr> eldest) {
    return size() > numDeletesToKeepInternal;
}
```

Хранит последние 1000 удалений. Нужен для `lookupVersion()`: Lucene-индекс не хранит удалённые документы, поэтому без этого кэша мы бы возвращали `null` вместо версии удаления.

Заполняется при инициализации из `startingUpdates.deleteList`.

### deleteByQueries (кэш DBQ)

```java
protected LinkedList<DBQ> deleteByQueries = new LinkedList<>();  // не более 100

public static class DBQ {
    public String q;       // строка запроса
    public long version;   // версия DBQ (положительная)
}
```

Хранит последние 100 DBQ в порядке убывания версии. Используется:
1. **RTG:** если версия документа < версии DBQ с перекрывающимся запросом → документ был удалён
2. **PeerSync:** `getDeleteByQuery(afterVersion)` возвращает DBQ новее указанной версии

`trackDeleteByQuery()` вставляет DBQ в отсортированный список (по убыванию версии).

---

## 22. Инициализация UpdateLog при старте

```java
public void init(UpdateHandler uhandler, SolrCore core) {
    dataDir = core.getUlogDir();
    
    // Если dataDir тот же → просто перезагрузить versionInfo (core reload)
    if (dataDir.equals(lastDataDir)) {
        versionInfo.reload();
        return;
    }
    
    tlogDir = Path.of(dataDir, TLOG_NAME);
    Files.createDirectories(tlogDir);
    
    // Получить список существующих tlog-файлов (отсортированных по имени)
    tlogFiles = getLogList(tlogDir.toFile());
    id = getLastLogId() + 1;  // следующий id = max(existing) + 1
    
    // Проверить наличие незавершённых buffer.tlog.*
    existOldBufferLog = Files.walk(tlogDir).anyMatch(
        path -> path.getFileName().toString().startsWith(BUFFER_TLOG_NAME + "."));
    
    // Открыть все существующие tlog-файлы (read-only через openExisting=true)
    for (String oldLogName : tlogFiles) {
        oldLog = newTransactionLog(path, null, true);
        addOldLog(oldLog, false);  // removeOld=false при старте!
    }
    
    // Запомнить два новейших лога для потенциального recovery
    for (TransactionLog ll : logs) {
        newestLogsOnStartup.addFirst(ll);
        if (newestLogsOnStartup.size() >= 2) break;
    }
    
    // Создать VersionInfo (инициализирует version buckets)
    versionInfo = new VersionInfo(this, numVersionBuckets);
    
    // Получить список стартовых версий из существующих логов
    try (RecentUpdates startingUpdates = getRecentUpdates()) {
        startingVersions = startingUpdates.getVersions(numRecordsToKeep);
        
        // Восстановить кэш oldDeletes из логов
        for (int i = startingUpdates.deleteList.size() - 1; i >= 0; i--) {
            DeleteUpdate du = startingUpdates.deleteList.get(i);
            oldDeletes.put(new BytesRef(du.id), new LogPtr(-1, du.version));
        }
        
        // Восстановить кэш deleteByQueries
        for (Update update : startingUpdates.deleteByQueryList) {
            List<Object> dbq = (List<Object>) update.log.lookup(update.pointer);
            trackDeleteByQuery((String) dbq.get(2), (Long) dbq.get(1));
        }
    }
}
```

### getStartingVersions()

`startingVersions` — список версий из существующих tlog'ов на момент инициализации. При `recoveringAfterStartup=true` используется в `RecoveryStrategy` как "наш список версий на момент падения" для PeerSync.

### existOldBufferLog()

```java
public boolean existOldBufferLog() {
    return existOldBufferLog;
}
```

Проверяется при старте recovery. Если `true` → предыдущий recovery не завершился (crash во время `applyBufferedUpdates`) → пропустить PeerSync и сразу идти на полную репликацию.

---

## 23. Метрики UpdateLog

```java
// TLOG.* метрики:
bufferedOpsGauge      // количество буферизованных операций (динамически)
"logs.replay.remaining"  // количество оставшихся для replay логов
"bytes.replay.remaining" // суммарный размер оставшихся логов в байтах
"state"                  // текущее состояние (0-3)

applyingBufferedOpsMeter  // скорость применения буфера (ops/sec)
replayOpsMeter            // скорость replay (ops/sec)
copyOverOldUpdatesMeter   // скорость переноса обновлений (ops/sec)
```

Все метрики доступны через Solr Admin UI → Metrics → `TLOG.*`.

---

## 24. Ресурсы: потоки, память, диск, fsync

### Поток recoveryExecutor

```java
ThreadPoolExecutor recoveryExecutor = new ExecutorUtil.MDCAwareThreadPoolExecutor(
    0,               // corePoolSize = 0 (нет постоянных потоков)
    Integer.MAX_VALUE, // maxPoolSize = неограниченно
    1, TimeUnit.SECONDS, // keepAlive = 1 секунда
    new SynchronousQueue<Runnable>(),
    new SolrNamedThreadFactory("recoveryExecutor")
);
```

Один поток создаётся на время `applyBufferedUpdates()`/`recoverFromLog()`, живёт 1 секунду после завершения.

### Параллельный replay: ReplayUpdatesExecutor

`OrderedExecutor` для параллельного replay получается из `CoreContainer.getReplayUpdatesExecutor()`. Это отдельный общий для всех core thread pool.

### Память

| Структура | Размер |
|---|---|
| `map` (текущие uncommitted) | N записей × ~40 байт (LogPtr) + overhead |
| `prevMap`, `prevMap2` | аналогично, живут до `postSoftCommit` |
| `oldDeletes` | max 1000 записей × ~50 байт |
| `deleteByQueries` | max 100 записей × ~60 байт |
| `startingVersions` | `numRecordsToKeep` × 8 байт |
| `RecentUpdates` (временно) | N × ~40 байт + документы по запросу |

### Диск и fsync

**`finish(SyncLevel)` в `TransactionLog`:**

```java
public void finish(UpdateLog.SyncLevel syncLevel) {
    switch (syncLevel) {
        case NONE:  return;               // только write, без flush
        case FLUSH: fos.flush(); break;   // flush буфера в ОС (не fsync)
        case FSYNC: fos.flush(); channel.force(true); break;  // fsync
    }
}
```

Вызывается из `UpdateLog.finish()` в конце каждого запроса (метод `RunUpdateProcessor.finish()`).

По умолчанию `defaultSyncLevel = FLUSH`. Это означает:
- Данные сбрасываются из Java-буфера в ОС-буфер
- Но НЕ синхронизируются с диском через fsync
- При краше ОС (power failure) возможна потеря последних записей
- При краше JVM (SIGKILL) — данные сохранены (в ОС буфере)

Для максимальной надёжности: `syncLevel=FSYNC`, но это значительно дороже (каждый запрос = `fdatasync`).

### FastOutputStream буфер

```java
fos = new FastOutputStream(os, new byte[65536], 0);  // 64KB буфер
```

Writes буферизуются в Java-heap-массиве. `flush()` → `channel.write(ByteBuffer)` → ОС page cache. `channel.force(true)` → реальный flush на диск.

---

## 25. Полная временная шкала жизни документа в tlog

```
t=0ms   Клиент: POST /update {"id":"doc1", "price":100}
           │
t=1ms   DirectUpdateHandler2.addDoc():
           IndexWriter.updateDocuments(term, luceneDocs)  ← Lucene buffer
           ulog.add(cmd):
             ensureLog()     ← если tlog==null, создать tlog.0000000000000000003
             pos = tlog.write(cmd, prevPointer=-1):
               checkWriteHeader()    ← записать header если первая запись
               codec.writeTag(ARR,3) + writeInt(ADD) + writeLong(version) + writeSolrInputDocument()
               endRecord()           ← записать 4-байтовый размер
               return pos=1234       ← байтовая позиция начала записи
             map.put(indexedId, LogPtr(1234, version, -1))
           
t=2ms   RunUpdateProcessor.finish():
           ulog.finish(FLUSH):
             tlog.finish(FLUSH): fos.flush()  ← Java buffer → ОС

t=2ms   RTG: GET /get?id=doc1
           ulog.lookup(idBytes):
             map.get(idBytes) → LogPtr(1234, version)
             tlog.lookup(1234): fos.flush(); codec.readVal(channel, 1234)
           → вернуть SolrInputDocument без открытия searcher'а

t=1000ms  autoSoftCommit:
           ulog.preSoftCommit(): newMap()  ← map → prevMap, map = {}
           core.openNewSearcher()
           ulog.postSoftCommit(): clearOldMaps()  ← prevMap = null
           
           doc1 теперь виден через searcher
           RTG: map пуст → searcher

t=15000ms autoCommit (hard):
           ulog.preCommit():
             newMap()           ← map → prevMap
             prevTlog = tlog    ← текущий tlog станет "старым"
             tlog = null
             id++
           IndexWriter.commit() ← fsync сегментов
           ulog.postCommit():
             prevTlog.writeCommit(cmd)  ← записать COMMIT-маркер
             addOldLog(prevTlog, true)  ← передать в deque, возможно удалить старые
             prevTlog = null
           
           Файл tlog.0000000000000000003 теперь "закрыт" COMMIT-маркером
           Новые обновления → tlog.0000000000000000004

t=30000ms  Новый hard commit:
           tlog.0000000000000000003 → в конце deque logs
           Если логов слишком много → logs.removeLast().decref()
           → TransactionLog.close() → Files.deleteIfExists(tlog)
           → tlog.0000000000000000003 удалён с диска

─── КРАШ (убит процесс) ───────────────────────────────────────

Старт:
  init():
    tlogFiles = ["tlog.0000000000000000004", "tlog.0000000000000000005"]
    newestLogsOnStartup = [tlog5, tlog4]
  
  recoverFromLog():
    tlog5.endsWithCommit() → false (незавершён)  → добавить в recoverLogs
    tlog4.endsWithCommit() → true (завершён)     → пропустить
    
    state = REPLAYING
    LogReplayer([tlog5], activeLog=false):
      for each entry in tlog5:
        ADD  doc_X v=123 → processAdd(cmd) → IndexWriter.updateDocuments()
        ADD  doc_Y v=124 → processAdd(cmd) → IndexWriter.updateDocuments()
        DELETE doc_Z v=-125 → processDelete(cmd) → IndexWriter.updateDocuments(empty)
      
      CommitUpdateCommand(version=0, replay=true)
      uhandler.commit()  ← записать в tlog5: [COMMIT, 0, "SOLR_TLOG_END"]
    
    state = ACTIVE
  
  Все незакоммиченные обновления восстановлены.
```

---

## 26. Ключевые классы и их взаимосвязи

```
UpdateLog
├── TransactionLog tlog              ← текущий файл (write + read)
├── TransactionLog prevTlog          ← предыдущий (во время commit)
├── TransactionLog bufferTlog        ← только при recovery (BUFFERING)
├── Deque<TransactionLog> logs       ← история (newest first)
│
├── Map<BytesRef, LogPtr> map        ← uncommitted в tlog
├── Map<BytesRef, LogPtr> prevMap    ← во время commit
├── Map<BytesRef, LogPtr> prevMap2   ← глубже
│
├── LinkedHashMap<BytesRef, LogPtr> oldDeletes  ← 1000 последних удалений
├── LinkedList<DBQ> deleteByQueries              ← 100 последних DBQ
│
├── VersionInfo versionInfo          ← Lamport clock + bucket lookup
│
├── RecoveryInfo recoveryInfo        ← статистика текущего replay
├── ThreadPoolExecutor recoveryExecutor
│
└── class LogReplayer implements Runnable
      ├── Deque<TransactionLog> translogs
      ├── boolean activeLog          ← читать до конца файла vs ждать
      ├── boolean inSortedOrder      ← SortedLogReader vs LogReader
      └── OrderedExecutor executor   ← параллельный replay по bucket hash

TransactionLog
├── FileChannel channel              ← NIO для r/w
├── FastOutputStream fos             ← 64KB буфер
├── Map<String,Integer> globalStringMap  ← string interning
├── List<String> globalStringList
├── AtomicInteger refcount           ← reference counting
│
├── class LogCodec extends JavaBinCodec
├── class LogReader                  ← прямое чтение (forward)
├── class SortedLogReader            ← по версии (для TLOG recovery)
├── class FSReverseReader            ← обратное чтение (для RecentUpdates)
└── class ChannelFastInputStream     ← буферизованный random-access reader
```

---

## Краткая справочная таблица

| Операция / метод | Когда вызывается | Результат |
|---|---|---|
| `bufferUpdates()` | RecoveryStrategy перед recovery | state=BUFFERING, создаётся buffer.tlog |
| `applyBufferedUpdates()` | После PeerSync/Replication | state=APPLYING_BUFFERED → ACTIVE, buffer.tlog удаляется |
| `copyOverBufferingUpdates()` | TLOG реплика после recovery | state=ACTIVE, буфер переносится в tlog без индексации |
| `recoverFromLog()` | Старт ноды, незавершённые tlog | state=REPLAYING → ACTIVE, документы восстановлены |
| `recoverFromCurrentLog()` | TLOG → новый лидер | state=REPLAYING → ACTIVE, tlog применяется к индексу |
| `openRealtimeSearcher()` | После DBQ, после replay | Новый searcher, карты очищены |
| `lookup(id)` | RTG запрос | SolrInputDocument из tlog по позиции |
| `lookupVersion(id)` | DUP conflict check | Последняя версия (map → index → oldDeletes) |
| `getRecentUpdates()` | PeerSync, startingVersions | RecentUpdates снимок (нужно close()!) |
| `preCommit()` | Перед IndexWriter.commit() | Ротация map, переключение tlog |
| `postCommit()` | После IndexWriter.commit() | COMMIT-маркер в tlog, addOldLog |
| `preSoftCommit()` | Перед openNewSearcher() | Ротация map |
| `postSoftCommit()` | После openNewSearcher() | clearOldMaps |
| `finish(syncLevel)` | Конец каждого запроса | flush/fsync текущего tlog |
| `add(cmd, BUFFERING)` | DUP при state=BUFFERING | Запись в buffer.tlog |
| `add(cmd)` | DUP при state=ACTIVE | Запись в tlog + map.put() |

| Параметр | Default | Источник |
|---|---|---|
| `numRecordsToKeep` | 100 | `<updateLog><int name="numRecordsToKeep">` |
| `maxNumLogsToKeep` | 10 | `<updateLog><int name="maxNumLogsToKeep">` |
| `numVersionBuckets` | 65536 | `<updateLog><int name="numVersionBuckets">` |
| `defaultSyncLevel` | FLUSH | `<updateLog><str name="syncLevel">` |
| `numDeletesToKeep` | 1000 | константа в коде |
| `numDeletesByQueryToKeep` | 100 | константа в коде |
| `buffer.tlog` размер | неограничен | зависит от rate × время recovery |
| `recoveryExecutor keepAlive` | 1 сек | константа в коде |
