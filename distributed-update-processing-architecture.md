# Архитектура распределённой обработки обновлений в Apache Solr

## Оглавление

1. [Обзор: полный путь документа](#1-обзор-полный-путь-документа)
2. [Точка входа: HTTP-обработчик](#2-точка-входа-http-обработчик)
3. [Кодеки и лоадеры: разбор входящих данных](#3-кодеки-и-лоадеры-разбор-входящих-данных)
4. [JavaBin — бинарный протокол Solr](#4-javabin--бинарный-протокол-solr)
5. [Объекты команд обновления](#5-объекты-команд-обновления)
6. [Цепочка UpdateRequestProcessor](#6-цепочка-updaterequestprocessor)
7. [DistributedUpdateProcessor: центральный элемент](#7-distributedupdateprocessor-центральный-элемент)
8. [DocRouter: маршрутизация документов по шардам](#8-docrouter-маршрутизация-документов-по-шардам)
9. [VersionInfo и VersionBucket: версионирование и локинг](#9-versioninfo-и-versionbucket-версионирование-и-локинг)
10. [Алгоритм versionAdd: детали](#10-алгоритм-versionadd-детали)
11. [Защита от переупорядочивания документов](#11-защита-от-переупорядочивания-документов)
12. [Разрешение конфликтов версий](#12-разрешение-конфликтов-версий)
13. [Атомарные обновления (partial updates)](#13-атомарные-обновления-partial-updates)
14. [In-place updates (только DocValues)](#14-in-place-updates-только-docvalues)
15. [SolrCmdDistributor: пересылка между нодами](#15-solrcmddistributor-пересылка-между-нодами)
16. [Delete операции](#16-delete-операции)
17. [Commit-операции в распределённой среде](#17-commit-операции-в-распределённой-среде)
18. [DirectUpdateHandler2: запись в Lucene](#18-directupdatehandler2-запись-в-lucene)
19. [DocumentBuilder: SolrInputDocument → Lucene Document](#19-documentbuilder-solrinputdocument--lucene-document)
20. [CommitTracker: автокоммиты](#20-committracker-автокоммиты)
21. [Полный сквозной пример: индексация батча документов](#21-полный-сквозной-пример-индексация-батча-документов)
22. [Обработка TLOG-реплик](#22-обработка-tlog-реплик)
23. [Буферизация обновлений во время recovery](#23-буферизация-обновлений-во-время-recovery)
24. [Ключевые классы и файлы](#24-ключевые-классы-и-файлы)

---

## 1. Обзор: полный путь документа

Ниже — схема полного пути одного документа от клиентского запроса до Lucene IndexWriter:

```
HTTP POST /solr/collection/update
         │
         ▼
UpdateRequestHandler (ContentStreamHandlerBase)
  Определяет формат (JSON/XML/JavaBin/CSV) по Content-Type
  Получает/создаёт UpdateRequestProcessorChain
         │
         ▼ (stream через loader)
ContentStreamLoader (JavabinLoader / JsonLoader / XMLLoader …)
  Парсит документы в SolrInputDocument
  Создаёт AddUpdateCommand/DeleteUpdateCommand
         │
         ▼ (chain.processAdd())
LogUpdateProcessor
  (после всей цепочки логирует результат)
         │
         ▼
DistributedUpdateProcessor / DistributedZkUpdateProcessor
  ┌─────────────────────────────────────────────────────────┐
  │  Определить роль ноды:                                  │
  │  setupRequest() — NONE / TOLEADER / FROMLEADER          │
  │                                                         │
  │  Если не лидер → forwardToLeader=true                  │
  │    SolrCmdDistributor.distribAdd(ForwardNode[leader])   │
  │    (HTTP POST к лидеру с distrib.update=TOLEADER)       │
  │    return (не продолжать цепочку)                       │
  │                                                         │
  │  Если лидер → isLeader=true                            │
  │    versionAdd() — назначить версию, проверить конфликт  │
  │    doLocalAdd() → next.processAdd()                     │
  │    doDistribAdd() — разослать репликам                  │
  │      SolrCmdDistributor.distribAdd(StdNode[replicas])   │
  │      (HTTP POST с distrib.update=FROMLEADER)            │
  │                                                         │
  │  Если реплика (FROMLEADER) → isLeader=false             │
  │    versionAdd() — проверить версию, защита от reorder  │
  │    doLocalAdd() → next.processAdd()                     │
  │    (не рассылать дальше)                                │
  └─────────────────────────────────────────────────────────┘
         │
         ▼
RunUpdateProcessor
  updateHandler.addDoc(cmd)
         │
         ▼
DirectUpdateHandler2
  1. IndexWriter.updateDocuments(term, luceneDocs)  ← Lucene
  2. ulog.add(cmd)                                  ← Transaction log
  Notify CommitTracker
         │
         ▼
Lucene IndexWriter (in-memory buffer → сегменты → fsync при commit)
```

---

## 2. Точка входа: HTTP-обработчик

### UpdateRequestHandler

**Файл:** `solr/core/src/java/org/apache/solr/handler/UpdateRequestHandler.java`

Наследует `ContentStreamHandlerBase`. Регистрируется для путей `/update`, `/update/json`, `/update/csv`, `/update/bin` и т.д.

**Логика обработки запроса:**

```
ContentStreamHandlerBase.handleRequestBody():
  1. Получить UpdateRequestProcessorChain из solrconfig.xml
     (или цепочку по умолчанию)
  2. Создать цепочку процессоров: chain.createProcessor(req, rsp)
  3. Для каждого ContentStream:
     a. Определить loader по Content-Type или параметру `wt`
     b. loader.load(req, rsp, stream, processor)
  4. processor.finish()
  5. Если изменения → commitWithin
```

### SolrQueryRequest

Контейнер, путешествующий через всю цепочку. Содержит:
- `SolrParams params` — параметры запроса (включая `distrib.update`, `distrib.from`)
- `SolrCore core` — ссылка на ядро (для доступа к UpdateHandler, VersionInfo)
- `IndexSchema schema` — снимок схемы для текущего запроса
- `Map<Object,Object> context` — request-local словарь для передачи данных между процессорами
- Principal (безопасность)
- Трейсинг (OpenTracing)

---

## 3. Кодеки и лоадеры: разбор входящих данных

**Файлы:** `solr/core/src/java/org/apache/solr/handler/loader/`

| Content-Type | Loader | Формат |
|---|---|---|
| `application/javabin` | `JavabinLoader` | Бинарный JavaBin |
| `application/json`, `text/json` | `JsonLoader` | JSON |
| `application/xml`, `text/xml` | `XMLLoader` | XML (Solr update XML) |
| `text/csv` | `CSVLoader` | CSV |
| `application/cbor` | `CborLoader` | CBOR |

### JavabinLoader

```java
JavabinLoader.load():
  1. Обернуть ContentStream в FastInputStream
  2. JavaBinUpdateRequestCodec.unmarshal(stream, handler)
     handler callback вызывается на каждый документ:
       → handler.update(doc, req, deleteIds, deleteQueries)
       → для каждого документа создаётся AddUpdateCommand
       → processor.processAdd(addCmd)
  3. Обработать deleteByIdMap (processor.processDelete())
  4. Обработать deleteByQuery (processor.processDelete())
```

### JsonLoader

Потоковый разбор JSON — использует Jackson `JsonParser`. Поддерживает:
- Одиночный документ: `{...}`
- Массив документов: `[{...},{...}]`
- Команды обновления: `{"add":{...}, "delete":{...}, "commit":{}}`
- Nested child documents через поле `_childDocuments_`

---

## 4. JavaBin — бинарный протокол Solr

**Файл:** `solr/solrj/src/java/org/apache/solr/common/util/JavaBinCodec.java`

JavaBin — компактный бинарный формат Solr для wire-протокола. Каждое значение кодируется тегом (1–3 байта) + данными.

### Теги типов данных

```
NULL            = 0x00
BOOL_TRUE       = 0x01
BOOL_FALSE      = 0x02
BYTE            = 0x03
SHORT           = 0x04
DOUBLE          = 0x05
INT             = 0x06
LONG            = 0x07
FLOAT           = 0x08
DATE            = 0x09
MAP             = 0x0A    ← HashMap
SOLRDOC         = 0x0B    ← SolrDocument (response)
SOLRDOCLST      = 0x0C    ← SolrDocumentList
BYTEARR         = 0x0D
ITERATOR        = 0x0E
END             = 0x0F    ← конец итератора/потока

SOLRINPUTDOC    = 0x10    ← SolrInputDocument (для индексации!)

STR             = 0x20    ← (1<<5): тег + размер (2 бита) + UTF-8 байты
SINT            = 0x40    ← (2<<5): small int, значение в 5 младших битах
SLONG           = 0x60    ← (3<<5): small long
ARR             = 0x80    ← (4<<5): массив
ORDERED_MAP     = 0xA0    ← (5<<5): SimpleOrderedMap
NAMED_LST       = 0xC0    ← (6<<5): NamedList
EXTERN_STRING   = 0xE0    ← (7<<5): ссылка на строку из string cache
```

### String Interning

JavaBin кэширует повторяющиеся строки. Имена полей (например, `"title"`, `"author"`, `"_version_"`) встречаются в каждом документе — JavaBin кодирует их один раз и далее ссылается по индексу через тег `EXTERN_STRING`. Это даёт значительную экономию при пакетной индексации.

### Сериализация SolrInputDocument

```
SOLRINPUTDOC  ← тег 0x10
<size>        ← количество полей (SINT или INT)
<boost>       ← float (для обратной совместимости, всегда 1.0f)
For each field:
  <name>      ← строка (STR или EXTERN_STRING)
  <value>     ← любой JavaBin тип
  (если SolrInputField с boost != 1.0f: сначала MAP {value: v, boost: b})
(для nested child docs: рекурсивно SOLRINPUTDOC внутри значения)
```

### Пример кодирования документа

```
Документ: {"id": "doc1", "_version_": 1620000000000, "title": "Hello"}

Binary (упрощённо):
10            ← SOLRINPUTDOC
83            ← SINT: 3 поля
3F800000      ← float 1.0f (boost)
E0+idx        ← EXTERN_STRING "id"
60 04 doc1    ← STR len=4 "doc1"
E0+idx        ← EXTERN_STRING "_version_"
07 00 000 ... ← LONG 1620000000000
E0+idx        ← EXTERN_STRING "title"
65 Hello      ← STR len=5 "Hello"
```

---

## 5. Объекты команд обновления

**Файл:** `solr/core/src/java/org/apache/solr/update/`

### UpdateCommand (базовый класс)

```java
public abstract class UpdateCommand implements Cloneable {
    public SolrQueryRequest req;
    public long version;           // _version_ поле
    public String route;           // явный ключ маршрутизации
    public int flags;              // битовые флаги

    // Флаги:
    static final int BUFFERING     = 0x00000001; // буферизован при recovery
    static final int REPLAY        = 0x00000002; // воспроизведение из tlog
    static final int PEER_SYNC     = 0x00000004; // PeerSync контекст
    static final int IGNORE_AUTOCOMMIT  = 0x00000008;
    static final int IGNORE_INDEXWRITER = 0x00000010; // только в tlog (TLOG replica)
    static final int CLEAR_CACHES  = 0x00000020;
}
```

### AddUpdateCommand

```java
public class AddUpdateCommand extends UpdateCommand {
    public SolrInputDocument solrDoc;  // входной документ
    public long prevVersion;           // для in-place update: версия предыдущего состояния
    public boolean overwrite = true;   // заменить существующий документ
    public Term updateTerm;            // term для дедупликации (обычно uniqueKey)
    public int commitWithin = -1;      // мс до автокоммита
    
    // Ключевые методы:
    List<Document> makeLuceneDocs()            // SolrInputDocument → Lucene Document(s)
    Document makeLuceneDocForInPlaceUpdate()   // только docValues поля
    BytesRef getIndexedId()                    // хэш уникального ключа
    String getIndexedIdStr()
    boolean isInPlaceUpdate()                  // prevVersion >= 0
    String getPrintableId()
}
```

### DeleteUpdateCommand

```java
public class DeleteUpdateCommand extends UpdateCommand {
    public String id;       // для deleteById
    public String query;    // для deleteByQuery
    public BytesRef indexedId;
    public int commitWithin = -1;
    
    boolean isDeleteById()   // true если id != null
}
```

### CommitUpdateCommand

```java
public class CommitUpdateCommand extends UpdateCommand {
    public boolean softCommit;       // не fsync, только открыть новый searcher
    public boolean openSearcher;     // открыть searcher после commit
    public boolean optimize;         // forceMerge сегментов
    public boolean expungeDeletes;   // forceMergeDeletes
    public int maxOptimizeSegments = 1;
    public Map<String,String> commitData; // метаданные в Lucene commit-точке
}
```

---

## 6. Цепочка UpdateRequestProcessor

**Файл:** `solr/core/src/java/org/apache/solr/update/processor/UpdateRequestProcessorChain.java`

### Конфигурация в solrconfig.xml

```xml
<updateRequestProcessorChain name="default" default="true">
  <processor class="solr.LogUpdateProcessorFactory">
    <int name="maxNumToLog">100</int>
  </processor>
  <processor class="solr.DistributedUpdateProcessorFactory"/>
  <processor class="solr.RunUpdateProcessorFactory"/>
</updateRequestProcessorChain>
```

Если `DistributedUpdateProcessorFactory` отсутствует в цепочке, он автоматически вставляется перед `RunUpdateProcessorFactory` при создании цепочки.

### Построение экземпляра цепочки

Цепочка строится **в обратном порядке** — каждый процессор получает ссылку на следующий (`next`):

```
Factories [Log, Distrib, Run] → строим с конца:
  RunProcessor(next=null)
  DistribProcessor(next=RunProcessor)
  LogProcessor(next=DistribProcessor)

Итого: LogProcessor → DistribProcessor → RunProcessor
```

### Оптимизация: пропуск pre-distrib процессоров

При получении forwarded-запроса (параметр `distrib.update` не пустой) цепочка **пропускает** все процессоры до `DistributingUpdateProcessorFactory`:

```java
// UpdateRequestProcessorChain.java
if (skipToDistrib) {
    if (afterDistrib) {
        if (factory instanceof DistributingUpdateProcessorFactory) {
            afterDistrib = false;
        }
    } else if (!(factory instanceof UpdateRequestProcessorFactory.RunAlways)) {
        continue;  // пропустить
    }
}
```

Это важная оптимизация: логирование, кастомная валидация и другие pre-distrib процессоры выполняются только один раз — на ноде, получившей запрос от клиента. На лидере и репликах запрос обрабатывается быстрее.

### LogUpdateProcessor

Реализует `RunAlways` — всегда выполняется независимо от фазы. Вызывает `next` сначала, затем логирует результат. Собирает статистику: добавленные/удалённые документы.

### RunUpdateProcessor

Конечное звено цепочки. Вызывает `UpdateHandler`:

```java
public void processAdd(AddUpdateCommand cmd) {
    updateHandler.addDoc(cmd);
    super.processAdd(cmd);
    changesSinceCommit = true;
}

public void finish() {
    if (changesSinceCommit && updateHandler.getUpdateLog() != null) {
        updateHandler.getUpdateLog().finish(null);  // обновить tlog
    }
}
```

---

## 7. DistributedUpdateProcessor: центральный элемент

**Файлы:**
- `solr/core/src/java/org/apache/solr/update/processor/DistributedUpdateProcessor.java` (базовый, 1416 строк)
- `solr/core/src/java/org/apache/solr/update/processor/DistributedZkUpdateProcessor.java` (ZK-реализация)

### DistribPhase — фаза распределения

```java
// DistributingUpdateProcessorFactory.java
public static enum DistribPhase {
    NONE,       // Начальная фаза: нода получила запрос от клиента
    TOLEADER,   // Нода пересылает запрос лидеру шарда
    FROMLEADER; // Нода получила запрос от лидера (реплика)
}

public static final String DISTRIB_UPDATE_PARAM = "update.distrib";
```

Параметр `update.distrib` путешествует вместе с HTTP-запросом и определяет логику обработки на каждой ноде.

### Ключевые поля состояния

```java
protected boolean isLeader;           // эта нода — лидер для шарда документа
protected boolean forwardToLeader;    // нужно переслать лидеру
protected boolean isSubShardLeader;   // лидер дочернего шарда (при split)
protected boolean isIndexChanged;     // индекс изменился
protected final int maxRetriesOnForward = 25;   // попыток пересылки к лидеру
protected final int maxRetriesToFollowers = 3;  // попыток к репликам
protected final Replica.Type replicaType;       // NRT / TLOG / PULL
```

### setupRequest() — определение роли ноды

Вызывается в начале `processAdd()`. Для ZK-режима (`DistributedZkUpdateProcessor`):

```
setupRequest(id, doc, route):

1. Если флаги REPLAY или PEER_SYNC:
   isLeader=false, forwardToLeader=false
   return null  (обрабатывать локально, без рассылки)

2. Найти целевой шард:
   Slice slice = coll.getRouter().getTargetSlice(id, doc, route, params, coll)
   Если slice == null → использовать локальный шард

3. Если deleteById без route и несколько шардов:
   broadcastDeleteById = true

4. Если FROMLEADER фаза и мы не sub-shard leader:
   isLeader=false, forwardToLeader=false
   return null  (применять локально как реплика)

5. Получить лидера из ZK:
   Replica leaderReplica = zkStateReader.getLeaderRetry(collection, shardId)
   isLeader = (leaderReplica.getName() == ourCoreNodeName)

6. Если мы лидер (или sub-shard leader):
   forwardToLeader = false
   return список реплик для рассылки (кроме DOWN и устаревших по terms)

7. Если мы НЕ лидер:
   forwardToLeader = true
   return [ForwardNode(leader)]
```

### processAdd() — общий алгоритм

```java
// DistributedUpdateProcessor.java
public void processAdd(AddUpdateCommand cmd) throws IOException {
    
    // 1. Определить роль: isLeader, forwardToLeader, nodes
    setupRequest(cmd);
    
    // 2. Если нужно переслать лидеру — переслать и выйти
    if (forwardToLeader) {
        // SolrCmdDistributor.distribAdd(cmd, [ForwardNode], params)
        doDistribAdd(cmd);
        return;
    }
    
    // 3. Версионирование (назначить или проверить версию)
    boolean dropCmd = versionAdd(cmd);
    if (dropCmd) return;  // дубликат или конфликт
    
    // 4. Применить локально через следующий процессор (RunUpdateProcessor)
    doLocalAdd(cmd);    // → next.processAdd(cmd)
    
    // 5. Если мы лидер — разослать репликам
    if (isLeader || isSubShardLeader) {
        doDistribAdd(cmd);
    }
}
```

---

## 8. DocRouter: маршрутизация документов по шардам

### Иерархия объектов кластера

```
DocCollection (вся коллекция)
  └── Slice (шард, логический раздел)
        ├── DocRouter.Range [min, max]  ← хэш-пространство
        ├── Replica (leader, NRT)       ← ACTIVE
        ├── Replica (NRT)               ← ACTIVE
        └── Replica (PULL)              ← ACTIVE
```

**Range** — отрезок 32-битного целочисленного пространства. Весь диапазон `[Integer.MIN_VALUE, Integer.MAX_VALUE]` делится между шардами поровну при создании коллекции.

### CompositeIdRouter (по умолчанию)

**Файл:** `solrj/src/java/org/apache/solr/common/cloud/CompositeIdRouter.java`

Поддерживает синтаксис `prefix!suffix` для управления размещением документов в шардах.

**Алгоритм хэширования:**

```
Для ID "user!1234":

1. Разбить по "!": parts = ["user", "1234"]

2. Каждая часть хэшируется MurmurHash3_x86_32

3. Применить битовые маски:
   upperMask = 0xFFFF0000  (старшие 16 бит)
   lowerMask = 0x0000FFFF  (младшие 16 бит)
   
   hash = (Hash(parts[0]) & upperMask) | (Hash(parts[1]) & lowerMask)

4. Найти шард, чей Range включает hash:
   for (Slice slice : collection.getSlices()) {
       if (slice.getRange().includes(hash)) return slice;
   }
```

**Управление количеством бит:**
```
"prefix/8!suffix"  → prefix занимает 8 бит (верхние), suffix — 24 бита
"a!b!c"           → трёхчастный: 8+8+16 бит по умолчанию
```

**Цель composite routing:** документы с одинаковым prefix попадают в один шард. Это позволяет делать эффективные joins внутри шарда (например, все документы одного тенанта на одном шарде).

### PlainIdRouter

Простой MurmurHash3 от всего ID без разбиения. Документы распределяются равномерно и случайно.

### ImplicitDocRouter

Шард задаётся явно через параметр `_route_` или поле `router.field`. Нет автоматического хэширования. Используется когда шарды — логические разделы по бизнес-логике.

### getTargetSlice() — поиск целевого шарда

```java
// HashBasedRouter.java
public Slice getTargetSlice(String id, SolrInputDocument sdoc,
                             String route, SolrParams params, DocCollection coll) {
    int hash = sliceHash(id, sdoc, params, coll);
    // sliceHash может использовать router.field вместо id
    return hashToSlice(hash, coll);
}

private Slice hashToSlice(int hash, DocCollection coll) {
    for (Slice slice : coll.getActiveSlices()) {
        DocRouter.Range range = slice.getRange();
        if (range != null && range.includes(hash)) {
            return slice;
        }
    }
    return coll.getActiveSlices().iterator().next(); // fallback
}
```

---

## 9. VersionInfo и VersionBucket: версионирование и локинг

**Файлы:**
- `solr/core/src/java/org/apache/solr/update/VersionInfo.java`
- `solr/core/src/java/org/apache/solr/update/VersionBucket.java`

### Lamport Clock: алгоритм генерации версии

Версии в Solr — это Lamport timestamp, реализованный как:

```java
// VersionInfo.java
public long getNewClock() {
    synchronized (clockSync) {
        long time = System.currentTimeMillis();
        long result = time << 20;          // timestamp в старших 44 битах
        if (result <= vclock) {
            result = vclock + 1;           // монотонный счётчик если время не изменилось
        }
        vclock = result;
        return vclock;
    }
}
```

**Структура версии (long, 64 бита):**
```
Биты 63-20: миллисекунды с epoch (44 бита → 557 лет)
Биты 19-0:  монотонный счётчик (20 бит → 1M версий в мс)
```

Это обеспечивает:
- Монотонность в пределах ноды (счётчик)
- Приблизительную синхронизацию с реальным временем (для TTL и диагностики)
- Обновление при получении чужих версий: `updateClock(version)` продвигает `vclock` если входящая версия новее

### Управление параллелизмом: двухуровневый локинг

```
ReentrantReadWriteLock (vinfo.lock)
│
├── Read lock (lockForUpdate / unlockForUpdate)
│   Захватывается при обработке каждого ADD/DELETE
│   Позволяет параллельное обновление разных документов
│
└── Write lock (blockUpdates / unblockUpdates)
    Захватывается при deleteByQuery
    Блокирует ВСЕ ADD/DELETE пока не завершится DBQ
    Гарантирует согласованное применение DELETE BY QUERY
```

### VersionBucket: гранулярная блокировка по документу

```java
// VersionBucket.java
public class VersionBucket {
    
    // Выполнить функцию под синхронизированным блоком
    public <T, R> R runWithLock(int lockTimeoutMs, CheckedFunction<T, R> function) {
        synchronized (this) {
            return function.apply();
        }
    }
    
    // Для in-place updates: сигнализировать ожидающим потокам
    public void signalAll() { notifyAll(); }
    
    // Ждать зависимого обновления (с таймаутом)
    public void awaitNanos(long nanosTimeout) {
        long millis = TimeUnit.NANOSECONDS.toMillis(nanosTimeout);
        if (millis > 0) wait(millis);
    }
}
```

**Организация bucket'ов:**

```java
// VersionInfo.java
int numBuckets; // округлено до степени двойки (например 1024)
VersionBucket[] buckets;

public VersionBucket bucket(int hash) {
    int slot = hash & (numBuckets - 1);  // младшие log2(numBuckets) бит хэша
    return buckets[slot];
}
```

**Сколько bucket'ов:** по умолчанию `numVersionBuckets` из конфигурации, обычно 65536. Разные документы, чьи ID дают разные `slot`, обрабатываются **параллельно**. Конкуренция только между документами, попавшими в один bucket.

### lookupVersion() — поиск последней версии

```java
public Long lookupVersion(BytesRef idBytes) {
    return ulog.lookupVersion(idBytes);
}
```

Делегирует в UpdateLog, который хранит `Map<BytesRef, LogPtr>` с последней версией каждого известного документа. Если документ не найден в map → версия неизвестна (документ отсутствует или очень старый).

---

## 10. Алгоритм versionAdd: детали

**Файл:** `DistributedUpdateProcessor.java`, метод `versionAdd()` → `doVersionAdd()`

### Полный алгоритм для лидера

```
doVersionAdd (leader logic):

1. Если запрос пришёл из другой коллекции И ulog=ACTIVE:
   → Удалить _version_ из документа, сбросить versionOnUpdate=0

2. Если запрос пришёл из другой коллекции И ulog≠ACTIVE:
   → Записать в буфер (ulog.add() с флагом BUFFERING)
   → Вернуть DROP=true (не продолжать)

3. Обработать атомарные обновления:
   getUpdatedDocument(cmd, versionOnUpdate)
   → Если частичное обновление → смёрджить с существующим документом

4. Если клиент указал _version_:
   lastVersion = vinfo.lookupVersion(idBytes)
   Проверить условие совместимости:
   
   ПРИНЯТЬ если:
   ├── versionOnUpdate == lastVersion  (точное совпадение)
   ├── versionOnUpdate < 0 && lastVersion < 0  (оба "документ удалён")
   └── versionOnUpdate == 1 && lastVersion > 0  (must-exist и документ есть)
   
   КОНФЛИКТ иначе → SolrException(CONFLICT)

5. Назначить новую версию:
   version = vinfo.getNewClock()
   cmd.setVersion(version)
   cmd.solrDoc.setField("_version_", version)
```

### Полный алгоритм для реплики

```
doVersionAdd (replica logic):

1. Принять версию от лидера:
   cmd.setVersion(versionOnUpdate)

2. Если ulog≠ACTIVE и не replay:
   → Записать в буфер
   → Вернуть DROP=true

3. Если in-place update (cmd.prevVersion >= 0):
   lastVersion = vinfo.lookupVersion(idBytes)
   
   Если lastVersion == null || |lastVersion| < prevVersion:
     → Предыдущее обновление ещё не пришло или пропало
     → fetchFullUpdateFromLeader() — получить полный документ через RTG
     → Если лидер вернул DELETE → удалить документ, DROP=true
     → Иначе заменить partial doc на full doc
   
   Если |lastVersion| > prevVersion:
     → Уже применена более новая версия → DROP=true
   
   Если |lastVersion| == prevVersion:
     → Зависимость выполнена, применять in-place

4. Если обычный (не in-place) update:
   lastVersion = vinfo.lookupVersion(idBytes)
   Если |lastVersion| >= versionOnUpdate:
     → Дубликат или reorder → DROP=true

5. Если тип TLOG и не REPLAY:
   → cmd.flags |= IGNORE_INDEXWRITER
   (TLOG реплика не пишет в Lucene index)

6. Применить локально
```

---

## 11. Защита от переупорядочивания документов

**Проблема:** В распределённой системе обновления могут прийти в другом порядке, чем были назначены лидером. Например:
- Лидер назначил версии: doc1/v100, doc1/v200 (обновление)
- Реплика получила: doc1/v200 (сначала!), затем doc1/v100

**Решение в коде:**

```java
// DistributedUpdateProcessor.java:511-520
// Для non-in-place на реплике:
Long lastVersion = vinfo.lookupVersion(cmd.getIndexedId());
if (lastVersion != null && Math.abs(lastVersion) >= versionOnUpdate) {
    // Уже есть версия >= пришедшей → это дубликат или запоздалое обновление
    log.debug("Dropping add update due to version {}", idBytes.utf8ToString());
    return true;  // DROP
}
```

**Что проверяется:**
- `Math.abs(lastVersion)` — берём абсолютное значение, потому что:
  - Положительная версия: документ добавлен/обновлён
  - Отрицательная версия: документ удалён (delete = `-version`)
- Если последняя известная операция имеет версию >= пришедшей → старая операция, игнорируем

**Ограничения:**
- Защита работает только для операций с одним и тем же документом (по ID)
- Глобальный порядок между разными документами не гарантируется

---

## 12. Разрешение конфликтов версий

Solr реализует **optimistic concurrency control** через параметр `_version_` в клиентском запросе.

### Семантика значений _version_

| Значение | Условие | Семантика |
|---|---|---|
| `> 0` | `version == lastVersion` | Обновить только если текущая версия совпадает |
| `0` | всегда | Обновить без проверки (по умолчанию) |
| `-1` | `lastVersion < 0 или null` | Обновить только если документ НЕ существует |
| `1` | `lastVersion > 0` | Обновить только если документ существует |

### Код проверки конфликта

```java
// doVersionAdd (leader), строки ~420-445
if (versionOnUpdate != 0) {
    Long lastVersion = vinfo.lookupVersion(cmd.getIndexedId());
    long foundVersion = lastVersion == null ? -1 : lastVersion;
    
    if (!(
        versionOnUpdate == foundVersion              // точное совпадение
        || (versionOnUpdate < 0 && foundVersion < 0) // оба "не существует"
        || (versionOnUpdate == 1 && foundVersion > 0) // must-exist
    )) {
        // Проверить failOnVersionConflicts параметр
        if (!req.getParams().getBool(CommonParams.FAIL_ON_VERSION_CONFLICTS, true)) {
            return true;  // тихо игнорировать конфликт
        }
        throw new SolrException(ErrorCode.CONFLICT,
            "version conflict for " + cmd.getPrintableId()
            + " expected=" + versionOnUpdate
            + " actual=" + foundVersion);
    }
}
```

**HTTP-ответ при конфликте:** `409 Conflict`

**Параметр `failOnVersionConflicts=false`:** конфликтующие обновления молча отбрасываются вместо исключения.

---

## 13. Атомарные обновления (partial updates)

**Синтаксис:** поле задаётся как `{"set": value}`, `{"add": value}`, `{"inc": n}`, `{"remove": value}` и т.д.

### Обнаружение атомарного обновления

```java
// AtomicUpdateDocumentMerger.isAtomicUpdate()
public static boolean isAtomicUpdate(AddUpdateCommand cmd) {
    SolrInputDocument sdoc = cmd.getSolrInputDocument();
    for (SolrInputField field : sdoc.values()) {
        Object val = field.getValue();
        if (val instanceof Map) {
            return true;  // хотя бы одно поле — Map с модификатором
        }
    }
    return false;
}
```

### Процесс атомарного обновления (leader)

```
getUpdatedDocument(cmd, versionOnUpdate):

1. AtomicUpdateDocumentMerger.computeInPlaceUpdatableFields(cmd)
   Определить, можно ли обновить только docValues (in-place update):
   - Все изменяемые поля — numeric docValues
   - Нет stored-only полей в операциях обновления
   
2. Если можно в-place:
   docMerger.doInPlaceUpdateMerge(cmd, inPlaceFields)
   → cmd.prevVersion = текущая версия документа
   → cmd.solrDoc = только модифицированные docValues поля
   → return true

3. Иначе (или если req параметр requirePartialInplace=true → исключение):
   Получить полный текущий документ:
   SolrInputDocument oldDoc =
     RealTimeGetComponent.getInputDocument(core, idBytes, ...)
   
   Если oldDoc == null:
     Если versionOnUpdate > 0 → SolrException(CONFLICT)
     Иначе → создать новый документ из модификаторов
   
   Иначе:
     mergedDoc = AtomicUpdateDocumentMerger.merge(sdoc, oldDoc)
     cmd.solrDoc = mergedDoc
```

### Модификаторы AtomicUpdateDocumentMerger

| Модификатор | Действие |
|---|---|
| `{"set": v}` | Заменить значение |
| `{"add": v}` | Добавить в multivalued поле |
| `{"add-distinct": v}` | Добавить если отсутствует |
| `{"remove": v}` | Удалить из multivalued |
| `{"removeregex": pattern}` | Удалить по regex |
| `{"inc": n}` | Инкремент числового поля |

---

## 14. In-place updates (только DocValues)

In-place update позволяет обновить только поля типа DocValues без переиндексации полного документа. Это существенно быстрее для частого обновления числовых полей (счётчики, цены, рейтинги).

### Условия применимости

- Все обновляемые поля имеют `docValues="true"` и числовой тип
- Нет indexed или stored полей в обновлении (кроме `_version_`)
- Схема поддерживает in-place (вычисляется через `AtomicUpdateDocumentMerger.computeInPlaceUpdatableFields()`)

### Путь на лидере

```
1. docMerger.doInPlaceUpdateMerge(cmd, fields):
   → Прочитать currentVersion из vinfo
   → cmd.prevVersion = currentVersion
   → cmd.solrDoc содержит только обновляемые DocValues поля

2. versionAdd():
   → Назначить новую версию
   → В tlog записать: [UPDATE_INPLACE, newVersion, prevVersion_ptr, prevVersion, partialDoc]

3. doLocalAdd():
   → DirectUpdateHandler2.updateDocOrDocValues():
     IndexWriter.updateDocValues(uniqueKeyTerm, docValuesFields)
     (без удаления и переиндексации!)
```

### Путь на реплике: ожидание зависимости

```
waitForDependentUpdates(cmd, versionOnUpdate, bucket):

Таймаут: 5 секунд

while (|lastFoundVersion| < cmd.prevVersion && !timeout) {
    bucket.awaitNanos(remainingNanos)  // wait на intrinsic lock bucket'а
    lastFoundVersion = vinfo.lookupVersion(idBytes)
}

Если timeout и зависимость не пришла:
    fetchFullUpdateFromLeader(cmd)  // RTG запрос к лидеру
    GET /get?getInputDocument=id&onlyIfActive=true
    → Получить полный документ вместо partial
```

### HTTP-параметры для in-place

```
distrib.inplace.prevversion = <prevVersion>
```

Реплика получает этот параметр и знает, что обновление in-place и что нужно ждать `prevVersion`.

---

## 15. SolrCmdDistributor: пересылка между нодами

**Файл:** `solr/core/src/java/org/apache/solr/update/SolrCmdDistributor.java`

### Архитектура

```
SolrCmdDistributor
├── StreamingSolrClients clients  ← pool HTTP-клиентов
│     └── ConcurrentUpdateHttp2SolrClient per node
│           └── очередь запросов: capacity 100
├── ExecutorCompletionService completionService
└── List<SolrError> errors        ← накопленные ошибки
```

### Типы нод

**`StdNode`** — обычная реплика:
```java
public class StdNode implements Node {
    ZkCoreNodeProps nodeProps;
    String collection, shardId;
    int maxRetries;  // = MAX_RETRIES_TO_FOLLOWERS_DEFAULT = 3
    
    boolean shouldRetry(SolrError err) {
        // Не перепробовать если это DBQ (опасно — дублирование)
        if (err.req.node instanceof SolrCmdDistributor.StdNode) {
            if (err.req.cmd instanceof DeleteUpdateCommand) {
                if (((DeleteUpdateCommand)err.req.cmd).query != null) {
                    return false;  // DBQ никогда не повторяем
                }
            }
        }
        return retriableError(err) && ++retries <= maxRetries;
    }
}
```

**`ForwardNode`** — пересылка к лидеру:
```java
public class ForwardNode extends StdNode {
    int maxRetries = MAX_RETRIES_ON_FORWARD_DEFAULT = 25;
    
    boolean shouldRetry(SolrError err) {
        // При 404/403/503/ConnectException — обновить URL лидера из ZK
        if (isConnectionError(err)) {
            nodeProps = zkStateReader.getLeaderRetry(collection, shard);
            // и повторить с новым URL
        }
        return retriableError(err) && ++retries <= maxRetries;
    }
}
```

### Отправка команды

```java
// SolrCmdDistributor.distribAdd()
public void distribAdd(AddUpdateCommand cmd, List<Node> nodes,
                       ModifiableSolrParams params, boolean synchronous,
                       RollupRequestReplicationTracker rollup,
                       LeaderRequestReplicationTracker leader) {
    
    for (Node node : nodes) {
        Request sreq = new Request();
        sreq.node = node;
        sreq.cmd = cmd;
        
        UpdateRequest uReq = new UpdateRequest();
        uReq.add(cmd.solrDoc, ...);
        
        if (synchronous) {
            blockAndDoRetries();  // дождаться завершения прошлых
        }
        submit(sreq, synchronous);
    }
}
```

### Параллельная асинхронная отправка

```java
void submit(Request sreq, boolean synchronous) {
    // Получить или создать HTTP клиент для ноды
    SolrClient solrClient = clients.getSolrClient(sreq);
    
    // Отправить асинхронно
    solrClient.request(sreq.uReq);
    
    if (synchronous) {
        // Ждать завершения и обработать ошибки
        blockAndDoRetries();
    }
}
```

`ConcurrentUpdateHttp2SolrClient` накапливает запросы во внутренней очереди (capacity=100) и отправляет их потоком, минимизируя latency за счёт батчинга.

### Обработка ошибок и retry

```java
// doRetriesIfNeeded()
void doRetriesIfNeeded() {
    List<SolrError> allErrors = new ArrayList<>(errors);
    allErrors.addAll(clients.getErrors());  // ошибки от streaming clients
    
    for (SolrError error : allErrors) {
        if (error.req.shouldRetry(error)) {
            // Экспоненциальный backoff: 2^retries * 10ms, max 2000ms
            long waitMs = Math.min(2000, (1L << error.req.retries) * 10);
            Thread.sleep(waitMs);
            submit(error.req, true);  // повторить синхронно
        } else {
            throw new SolrException(SERVER_ERROR, error);
        }
    }
}
```

**Ретраиваемые ошибки:**
- `SocketException`, `SocketTimeoutException`
- `NoHttpResponseException`
- `ConnectTimeoutException`
- HTTP 404, 403, 503

**Не ретраиваемые:**
- DELETE_BY_QUERY ошибки (нельзя дублировать!)
- HTTP 4xx (кроме 404/403)

### Параметры HTTP-запроса к репликам

```
POST /update
update.distrib = FROMLEADER
distrib.from   = http://leader:8983/solr/collection1/  (URL лидера)
_version_      = 1620000000000  (назначенная версия)
distrib.inplace.prevversion = ...  (для in-place)
distrib.from.shard = shard1  (для splits)
distrib.from.parent = shardX  (для sub-shard splits)
distrib.from.collection = other_coll  (для cross-collection routing)
```

### Replication Trackers

```java
// Отслеживание достигнутого replication factor

RollupRequestReplicationTracker   // Накапливает минимум по всем документам батча
LeaderRequestReplicationTracker   // Считает успешные реплики для текущего шарда

// После finish():
int achievedRf = Math.min(rollupTracker.getAchievedRf(),
                          leaderTracker.getAchievedRf());
// Если achievedRf < minRf (из запроса) → предупреждение в ответе
```

---

## 16. Delete операции

### Delete by ID

```
processDelete(DeleteUpdateCommand cmd):

1. setupRequest(cmd):
   Найти шард по ID (или все шарды если broadcastDeleteById)

2. versionDelete(cmd):
   Лидер: 
     Если клиент указал version → проверить конфликт
     Назначить version = -vinfo.getNewClock()  (отрицательная!)
   Реплика:
     cmd.setVersion(-versionOnUpdate)
     Проверить |lastVersion| >= versionOnUpdate → DROP если да

3. doLocalDelete(): ulog.delete(cmd) + IndexWriter.updateDocuments(term, empty)

4. doDistribDeleteById(): разослать репликам
```

**Broadcast delete by ID:** если у документа нет routing-ключа в мультишардовой коллекции — лидер не знает, в каком шарде он лежит. В этом случае `broadcastDeleteById=true`: запрос рассылается лидерам **всех** шардов. Большинство из них определят, что документ не у них, и просто запишут отрицательную версию (что безопасно).

### Delete by Query

DBQ — самая сложная операция. Она должна удалить документы **во всех шардах**:

```
processDelete(DeleteUpdateCommand cmd) с cmd.query != null:

1. versionDeleteByQuery():
   vinfo.blockUpdates()  ← WRITE LOCK: остановить ВСЕ ADD/DELETE!
   
   Лидер: назначить version = -vinfo.getNewClock()
   
   doLocalDeleteByQuery():
     QParser.getQuery() → распарсить запрос
     IndexWriter.deleteDocuments(new DeleteByQueryWrapper(query))
     ulog.deleteByQuery(cmd)
   
   vinfo.unblockUpdates()

2. doDistribDeleteByQuery():
   
   Если NONE фаза (первичная нода):
     forwardDelete() → разослать лидерам всех шардов с фазой TOLEADER
   
   Если мы лидер для своего шарда:
     Разослать репликам с фазой FROMLEADER
     (только NRT и TLOG реплики, PULL не получают)
     Также послать sub-shard leaders если есть splits
```

**Почему Write Lock при DBQ?**
DBQ логически должна "видеть" состояние индекса в один момент времени. Без блокировки гонка: документ добавляется между моментом назначения версии DBQ и реальным удалением. Такой документ был бы "выжившим" после DBQ. Write lock предотвращает это.

---

## 17. Commit-операции в распределённой среде

```
processCommit (DistributedZkUpdateProcessor):

1. Получить лидера из ZK
   isLeader = (наш coreNodeName == лидер)

2. Собрать список нод (все шарды, все реплики коллекции)
   nodes = getCollectionUrls(collection)
   nodes.removeIf(n -> n == localhost)

3. Если commit ещё не помечен как endpoint:
   params.set(COMMIT_END_POINT, "leaders")
   params.set(DISTRIB_UPDATE_PARAM, TOLEADER)
   
   cmdDistrib.distribCommit(cmd, leaders, params)
   ← СИНХРОННО к каждому лидеру шарда

4. Если мы лидер:
   params.set(DISTRIB_UPDATE_PARAM, FROMLEADER)
   params.set(COMMIT_END_POINT, "replicas")
   
   useNodes = getReplicaNodesForLeader(shardId, leaderReplica, 0)
   cmdDistrib.distribCommit(cmd, myReplicas, params)
   ← к нашим репликам
   
   doLocalCommit(cmd)  ← коммитим локально

5. cmdDistrib.blockAndDoRetries()
```

Commit всегда синхронный (`distribCommit` использует `blockAndDoRetries()` сразу). Это гарантирует, что к моменту ответа клиенту все ноды сделали commit.

Soft commit рассылается аналогично, но `softCommit=true` и `openSearcher=true`.

---

## 18. DirectUpdateHandler2: запись в Lucene

**Файл:** `solr/core/src/java/org/apache/solr/update/DirectUpdateHandler2.java`

### addDoc()

```java
public int addDoc(AddUpdateCommand cmd) throws IOException {
    // 1. Увеличить счётчик (atomically)
    addCommands.incrementAndGet();
    
    // 2. Если флаг IGNORE_INDEXWRITER → только в tlog (TLOG реплика)
    if ((cmd.getFlags() & UpdateCommand.IGNORE_INDEXWRITER) != 0) {
        if (ulog != null) ulog.add(cmd);
        return 1;
    }
    
    // 3. Получить IndexWriter (RefCounted)
    RefCounted<IndexWriter> iw = solrCoreState.getIndexWriter(core);
    try {
        IndexWriter writer = iw.get();
        
        if (cmd.overwrite) {
            updateDocOrDocValues(cmd, writer);
        } else {
            // Дубликаты разрешены (append-only режим)
            writer.addDocuments(cmd.makeLuceneDocs());
        }
    } finally {
        iw.decref();
    }
    
    // 4. Записать в transaction log ПОСЛЕ успешной записи в Lucene
    if (ulog != null) ulog.add(cmd);
    
    // 5. Уведомить CommitTracker
    if (commitWithinSoftCommit)
        softCommitTracker.addedDocument(cmd.commitWithin);
    else
        commitTracker.addedDocument(cmd.commitWithin);
    
    return 1;
}
```

### updateDocOrDocValues()

```java
private void updateDocOrDocValues(AddUpdateCommand cmd, IndexWriter writer) {
    if (cmd.isInPlaceUpdate()) {
        // Только обновить DocValues, не трогать остальные поля
        writer.updateDocValues(
            cmd.updateTerm,              // Term uniqueKey=id
            cmd.makeLuceneDocForInPlaceUpdate()  // только DV поля
        );
    } else {
        // Удалить старый документ + добавить новый
        writer.updateDocuments(
            cmd.updateTerm,              // Term для удаления
            cmd.makeLuceneDocs()         // Lucene Document(s), включая nested
        );
    }
}
```

### Порядок записи: IndexWriter → tlog

**Важно:** запись в Lucene идёт ПЕРВОЙ, в tlog — после.

Это кажется контринтуитивным, но обосновано:
- Если краш между записью в IndexWriter и в tlog → на следующий старт IndexWriter не знает об этом документе → при воспроизведении tlog (recovery) лидер перешлёт документ заново
- Если краш между тlog и IndexWriter (если бы было наоборот) → документ есть в tlog но не в индексе → при recovery реплика применит его из tlog. Но у лидера тоже нет в индексе → рассогласование

### deleteByQuery()

```java
public void deleteByQuery(DeleteUpdateCommand cmd) throws IOException {
    Query q = QParser.getQuery(cmd.getQuery(), ...);
    
    // СИНХРОНИЗАЦИЯ: блокировать для атомарности DBQ
    synchronized (solrCoreState.getUpdateLock()) {
        
        // Гарантировать, что RTG-поиск видит текущее состояние
        if (ulog != null) ulog.openRealtimeSearcher();
        
        // Применить к Lucene (wrapper добавляет version-фильтр)
        writer.deleteDocuments(new DeleteByQueryWrapper(q, schema));
        
        // Записать в tlog (внутри sync!)
        if (ulog != null) ulog.deleteByQuery(cmd);
    }
    
    // Уведомить о pending dirty state
    numDocsPending.reset();
}
```

---

## 19. DocumentBuilder: SolrInputDocument → Lucene Document

**Файл:** `solr/core/src/java/org/apache/solr/update/DocumentBuilder.java`

```java
public static Document toDocument(SolrInputDocument sdoc, IndexSchema schema) {
    
    Document out = new Document();
    
    for (SolrInputField sif : sdoc) {
        String name = sif.getName();
        SchemaField sf = schema.getFieldOrNull(name);
        
        // Валидация
        if (sf == null && schema.getDynamicFieldOrNull(name) != null) {
            sf = schema.getDynamicField(name);
        }
        
        // Для каждого значения поля:
        for (Object val : sif) {
            if (sf != null) {
                // SchemaField.getType().createFields() генерирует Lucene IndexableField
                // Например StrField → StringField + StoredField
                //          IntPointField → IntPoint + StoredField + NumericDocValuesField
                List<IndexableField> fields = sf.createFields(val);
                for (IndexableField f : fields) {
                    out.add(f);
                }
            }
        }
        
        // CopyFields: скопировать в другие поля согласно schema.xml
        for (CopyField cf : schema.getCopyFieldsList(name)) {
            SchemaField destField = cf.getDestination();
            List<IndexableField> fields = destField.createFields(val, cf);
            for (IndexableField f : fields) out.add(f);
        }
    }
    
    // Добавить required поля с дефолтами если отсутствуют
    for (SchemaField requiredField : schema.getRequiredFields()) {
        if (out.getField(requiredField.getName()) == null) {
            if (requiredField.getDefaultValue() != null) {
                out.add(requiredField.createField(requiredField.getDefaultValue()));
            }
        }
    }
    
    // Оптимизация: поместить самое большое stored поле последним
    // (Lucene эффективнее хранит большие поля в конце)
    sortLargestFieldLast(out);
    
    return out;
}
```

### Типы Lucene-полей, генерируемых по типам Solr

| Тип Solr | indexed | docValues | stored | Lucene поля |
|---|---|---|---|---|
| `StrField` | да | нет | да | `StringField` + `StoredField` |
| `IntPointField` | да | да | да | `IntPoint` + `NumericDocValuesField` + `StoredField` |
| `TextField` | да | нет | нет | `Field(tokenized)` |
| `BinaryField` | нет | нет | да | `StoredField` |
| `SortableTextField` | да | да | нет | `Field(tokenized)` + `SortedSetDocValuesField` |

### Поле _version_

Хранится как `NumericDocValuesField` (для быстрого чтения через DocValues) и `StoredField` (для RTG). При генерации `IndexFingerprint` читается через DocValues итерацию.

---

## 20. CommitTracker: автокоммиты

**Файл:** `solr/core/src/java/org/apache/solr/update/CommitTracker.java`

`DirectUpdateHandler2` содержит **два** `CommitTracker`: один для hard commit, один для soft commit.

### Тригеры автокоммита

```java
void addedDocument(int commitWithin) {
    
    // 1. Тригер по количеству документов
    _scheduleMaxDocsTriggeredCommitIfNeeded();
    
    // 2. Тригер по времени (commitWithin параметр)
    _scheduleCommitWithinIfNeeded(commitWithin);
    
    // 3. Тригер по размеру tlog (только для hard commit)
    if (tLogFileSizeUpperBound >= 0) {
        _scheduleMaxSizeTriggeredCommitIfNeeded(
            () -> ulog.getCurrentLogSizeFromStream()
        );
    }
}
```

### Параметры из solrconfig.xml

```xml
<autoCommit>
  <maxTime>${solr.autoCommit.maxTime:15000}</maxTime>    <!-- мс -->
  <maxDocs>-1</maxDocs>                                   <!-- -1=отключено -->
  <maxSize>-1</maxSize>                                   <!-- байт, -1=отключено -->
  <openSearcher>false</openSearcher>
</autoCommit>

<autoSoftCommit>
  <maxTime>${solr.autoSoftCommit.maxTime:-1}</maxTime>
  <maxDocs>-1</maxDocs>
</autoSoftCommit>
```

### Hard commit vs Soft commit

**Hard commit (`IndexWriter.commit()`):**
1. Записывает сегменты на диск (fsync)
2. Обновляет `segments_N` файл
3. Ротирует tlog-файл
4. Документы видны после перезапуска
5. Дорого: fsync блокирует на время записи

**Soft commit (`SolrIndexSearcher` reopen):**
1. Открывает новый `DirectoryReader` поверх текущего `IndexWriter`
2. Документы видны для поиска (NRT)
3. Нет fsync — при краше документы теряются (восстанавливаются из tlog)
4. Дёшево: просто обновляет ссылку на searcher

**Взаимодействие:** обычный паттерн — soft commit каждые ~1 секунды для NRT-видимости, hard commit каждые ~15 секунд для durability.

---

## 21. Полный сквозной пример: индексация батча документов

Рассмотрим: клиент отправляет 1000 документов POST-запросом на ноду A коллекции из 2 шардов (shard1, shard2), каждый шард с 2 NRT-репликами.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ CLIENT                                                                      │
│   POST http://nodeA:8983/solr/mycoll/update                                 │
│   Content-Type: application/javabin                                         │
│   Body: [doc1, doc2, ... doc1000] (JavaBin)                                 │
└────────────────────────┬────────────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ NODE A (например, реплика shard1, НЕ лидер)                                 │
│                                                                             │
│ UpdateRequestHandler.handleRequestBody()                                    │
│   chain = getUpdateProcessorChain(req)                                      │
│   processor = chain.createProcessor(req, rsp)                               │
│     → LogProcessor → DistribProcessor → RunProcessor                        │
│                                                                             │
│ JavabinLoader.load(stream, processor):                                      │
│   for each doc in stream:                                                   │
│     cmd = new AddUpdateCommand(req)                                         │
│     cmd.solrDoc = parsedDoc                                                 │
│     processor.processAdd(cmd)                                               │
│                                                                             │
│   processAdd(doc1):  id="user!1001" → hash → shard1                        │
│     DistribProcessor.setupRequest():                                        │
│       coll.getRouter().getTargetSlice("user!1001", ...) → shard1           │
│       leader_shard1 = zkStateReader.getLeaderRetry("mycoll", "shard1")     │
│       isLeader = (leader.name == OUR_node_name) → FALSE (мы реплика)       │
│       forwardToLeader = true                                                │
│       nodes = [ForwardNode(leader_shard1)]                                  │
│     → cmdDistrib.distribAdd(cmd, [ForwardNode], TOLEADER)                   │
│       ┌ асинхронно HTTP POST к лидеру shard1 ┐                             │
│                                                                             │
│   processAdd(doc2):  id="order!5001" → hash → shard2                       │
│     setupRequest() → leader_shard2, forwardToLeader=true                   │
│     → cmdDistrib.distribAdd(cmd, [ForwardNode(leader_shard2)], TOLEADER)   │
│       ┌ асинхронно HTTP POST к лидеру shard2 ┐                             │
│                                                                             │
│   ... (все 1000 документов разделяются по шардам) ...                      │
│                                                                             │
│   processor.finish():                                                       │
│     cmdDistrib.blockAndDoRetries()  ← ждать ответов, retry при ошибках     │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
       │ HTTP POST (update.distrib=TOLEADER)          │ HTTP POST (TOLEADER)
       ▼                                              ▼
┌─────────────────────────┐              ┌─────────────────────────┐
│ LEADER NODE для shard1  │              │ LEADER NODE для shard2  │
│                         │              │                         │
│ (только distrib+run     │              │ (только distrib+run     │
│  процессоры, pre-distrib│              │  процессоры, т.к.       │
│  пропущены т.к.         │              │  update.distrib=TOLEADER│
│  update.distrib=TOLEADER│              │  — не пустой)           │
│                         │              │                         │
│ setupRequest():         │              │ setupRequest():         │
│   isLeader = TRUE       │              │   isLeader = TRUE       │
│   forwardToLeader=false │              │                         │
│                         │              │ versionAdd():           │
│ versionAdd():           │              │   bucket lock           │
│   Если versionOnUpdate  │              │   v = getNewClock()     │
│   == 0 (нет от клиента):│              │   doc._version_ = v     │
│   v = vinfo.getNewClock()│             │                         │
│   cmd.setVersion(v)     │              │ doLocalAdd():           │
│   doc._version_ = v     │              │   RunProcessor:         │
│                         │              │   DirectUpdateHandler2: │
│ doLocalAdd():           │              │   IndexWriter.update()  │
│   RunProcessor →        │              │   ulog.add(cmd)         │
│   DirectUpdateHandler2: │              │                         │
│   writer.updateDocuments│              │ doDistribAdd():         │
│     (term, luceneDocs)  │              │   replica2a, replica2b  │
│   ulog.add(cmd)         │              │   HTTP POST (FROMLEADER)│
│                         │              └─────────────────────────┘
│ doDistribAdd():         │
│   Нашли реплики:        │
│     replica1a: StdNode  │
│     replica1b: StdNode  │
│   HTTP POST FROMLEADER  │
│   к каждой реплике      │
│   (асинхронно через     │
│    ConcurrentUpdateSolr │
│    Client)              │
└─────────────────────────┘
       │ HTTP POST (FROMLEADER)       │ HTTP POST (FROMLEADER)
       ▼                             ▼
┌──────────────────────┐    ┌──────────────────────┐
│ replica1a            │    │ replica1b            │
│                      │    │                      │
│ setupRequest():      │    │ setupRequest():      │
│   FROMLEADER фаза    │    │   isLeader=false     │
│   isLeader=false     │    │   forwardToLeader=f  │
│   forwardToLeader=f  │    │                      │
│                      │    │ versionAdd():        │
│ versionAdd():        │    │   version = от лидера│
│   version = от лидера│    │   |lastVer| < ver?   │
│   проверить reorder  │    │   → OK, применить    │
│                      │    │                      │
│ doLocalAdd():        │    │ doLocalAdd():        │
│   writer.update()    │    │   writer.update()    │
│   ulog.add()         │    │   ulog.add()         │
│                      │    │                      │
│ (не рассылать дальше)│    │ (не рассылать дальше)│
└──────────────────────┘    └──────────────────────┘
```

### Autocommit и видимость

Пока идёт индексация, документы в Lucene BufferedUpdates (in-memory). Они не видны поиску до:
1. **Soft commit** (например, каждую секунду) → открывается новый NRT searcher → документы видны, но не на диске
2. **Hard commit** (например, каждые 15 сек) → fsync + tlog rotation → документы сохранены

```
Timeline для doc1 на лидере shard1:

t=0ms:   IndexWriter.updateDocuments() ← in Lucene buffer
t=0ms:   ulog.add()                    ← в tlog файле (fsync)
t=1000ms: autoSoftCommit срабатывает  ← новый NRT searcher
          doc1 видимый поиску
t=15000ms: autoCommit срабатывает     ← IndexWriter.commit()
           сегмент записан, tlog ротирован
```

---

## 22. Обработка TLOG-реплик

TLOG-реплики получают те же HTTP-запросы что и NRT, но:

**При получении обновления (флаг FROMLEADER):**
```java
// doVersionAdd, строки ~521-526
if (!isSubShardLeader
    && replicaType == Replica.Type.TLOG
    && (cmd.getFlags() & UpdateCommand.REPLAY) == 0) {
    cmd.setFlags(cmd.getFlags() | UpdateCommand.IGNORE_INDEXWRITER);
}
```

Флаг `IGNORE_INDEXWRITER` → `DirectUpdateHandler2.addDoc()` пишет только в tlog, не в Lucene IndexWriter:
```java
if ((cmd.getFlags() & UpdateCommand.IGNORE_INDEXWRITER) != 0) {
    if (ulog != null) ulog.add(cmd);  // только tlog
    return 1;  // пропустить IndexWriter
}
```

**Где тогда индекс у TLOG-реплики?**

TLOG-реплика получает Lucene-индекс через фоновую репликацию от лидера (как PULL-реплика), но при этом ведёт tlog для:
- RTG (real-time get из tlog без searcher)
- Возможности стать лидером (применить tlog → получить полный индекс)
- Recovery через PeerSync (не надо репликацию)

При выборе лидером: TLOG-реплика вначале применяет свой tlog к скопированному индексу, затем становится активной.

**При peerSync параметре `SKIP_COMMIT_ON_LEADER_VERSION_ZERO`:**
```java
// ReplicationHandler.doFetch() для TLOG replicas
solrParams.set(ReplicationHandler.SKIP_COMMIT_ON_LEADER_VERSION_ZERO,
               replicaType == Replica.Type.TLOG);
```

---

## 23. Буферизация обновлений во время recovery

Когда реплика переходит в recovery (`ulog.bufferUpdates()`), все входящие обновления, которые лидер продолжает слать, должны быть сохранены но НЕ применены к индексу.

```java
// shouldBufferUpdate() — условие буферизации
boolean shouldBufferUpdate(AddUpdateCommand cmd,
                            boolean isReplayOrPeersync,
                            UpdateLog.State state) {
    
    // Специальный случай: при переходе APPLYING_BUFFERED → ACTIVE
    // полные (не in-place) обновления можно применять сразу
    if (state == UpdateLog.State.APPLYING_BUFFERED
        && !isReplayOrPeersync
        && !cmd.isInPlaceUpdate()) {
        return false;
    }
    
    // Буферизовать если: не ACTIVE И не воспроизведение
    return state != UpdateLog.State.ACTIVE && !isReplayOrPeersync;
}
```

**При буферизации:**
```java
cmd.setFlags(cmd.getFlags() | UpdateCommand.BUFFERING);
ulog.add(cmd);  // записать в buffer.tlog
return true;    // DROP: не передавать следующему процессору
```

Таким образом документ не попадает ни в Lucene, ни в основной tlog — только в `buffer.tlog`. После завершения recovery, `applyBufferedUpdates()` применяет их.

---

## 24. Ключевые классы и файлы

| Класс | Путь | Роль |
|---|---|---|
| `UpdateRequestHandler` | `core/.../handler/UpdateRequestHandler.java` | HTTP-точка входа |
| `JavabinLoader` | `core/.../handler/loader/JavabinLoader.java` | Десериализация JavaBin |
| `JavaBinCodec` | `solrj/.../util/JavaBinCodec.java` | Бинарный кодек |
| `AddUpdateCommand` | `core/.../update/AddUpdateCommand.java` | Команда добавления |
| `UpdateRequestProcessorChain` | `core/.../update/processor/UpdateRequestProcessorChain.java` | Цепочка процессоров |
| `DistributedUpdateProcessor` | `core/.../update/processor/DistributedUpdateProcessor.java` | Версионирование, конфликты |
| `DistributedZkUpdateProcessor` | `core/.../update/processor/DistributedZkUpdateProcessor.java` | ZK-маршрутизация, репликация |
| `RunUpdateProcessorFactory` | `core/.../update/processor/RunUpdateProcessorFactory.java` | Вызов UpdateHandler |
| `SolrCmdDistributor` | `core/.../update/SolrCmdDistributor.java` | HTTP-рассылка по нодам |
| `StreamingSolrClients` | `core/.../update/StreamingSolrClients.java` | HTTP-клиентский пул |
| `DirectUpdateHandler2` | `core/.../update/DirectUpdateHandler2.java` | Запись в Lucene + tlog |
| `DocumentBuilder` | `core/.../update/DocumentBuilder.java` | SolrInputDoc → LuceneDoc |
| `UpdateLog` | `core/.../update/UpdateLog.java` | Transaction log менеджер |
| `TransactionLog` | `core/.../update/TransactionLog.java` | Один tlog-файл |
| `VersionInfo` | `core/.../update/VersionInfo.java` | Lamport clock, bucket lookup |
| `VersionBucket` | `core/.../update/VersionBucket.java` | Гранулярный лок на документ |
| `CommitTracker` | `core/.../update/CommitTracker.java` | Автокоммиты |
| `AtomicUpdateDocumentMerger` | `core/.../update/processor/AtomicUpdateDocumentMerger.java` | Слияние atomic updates |
| `DocRouter` | `solrj/.../cloud/DocRouter.java` | Абстракция маршрутизации |
| `CompositeIdRouter` | `solrj/.../cloud/CompositeIdRouter.java` | Composite ID хэширование |
| `RealTimeGetComponent` | `core/.../handler/component/RealTimeGetComponent.java` | RTG + getVersions/getUpdates |
| `DocCollection` | `solrj/.../cloud/DocCollection.java` | Состояние коллекции |
| `Slice` | `solrj/.../cloud/Slice.java` | Шард с Range и репликами |
| `Replica` | `solrj/.../cloud/Replica.java` | Реплика: state, type |
| `ZkStateReader` | `solrj-zookeeper/.../cloud/ZkStateReader.java` | Чтение кластерного состояния |
| `UpdateShardHandler` | `core/.../update/UpdateShardHandler.java` | HTTP-клиенты для обновлений |

---

## Краткая справка: параметры и константы

| Параметр / Константа | Значение | Смысл |
|---|---|---|
| `update.distrib` | `NONE/TOLEADER/FROMLEADER` | Фаза распределения |
| `distrib.from` | URL лидера | Откуда пришёл запрос |
| `distrib.from.shard` | shardId | Источник при splits |
| `distrib.from.collection` | collectionName | Источник cross-collection |
| `distrib.inplace.prevversion` | long | Предыдущая версия для in-place |
| `_version_` | long (client) | Optimistic locking |
| `_version_ > 0` | exact version | Совпасть с текущей версией |
| `_version_ = 0` | unconditional | Без проверки |
| `_version_ = -1` | must not exist | Документ не должен существовать |
| `_version_ = 1` | must exist | Документ должен существовать |
| `failOnVersionConflicts` | boolean (default true) | Исключение при конфликте |
| `MAX_RETRIES_ON_FORWARD_DEFAULT` | 25 | Попытки пересылки к лидеру |
| `MAX_RETRIES_TO_FOLLOWERS_DEFAULT` | 3 | Попытки к репликам |
| `DBQ retry` | никогда | DBQ не повторяются |
| `numVersionBuckets` | 65536 | Количество bucket'ов лока |
| `waitForDependentUpdates timeout` | 5 секунд | Ожидание prevVersion |
| `StreamingSolrClients queue` | 100 | Размер очереди HTTP-клиента |
