# Архитектура Recovery для NRT-реплик в Apache Solr

## Оглавление

1. [Типы реплик и их отличия](#1-типы-реплик-и-их-отличия)
2. [Когда и почему начинается recovery](#2-когда-и-почему-начинается-recovery)
3. [Машина состояний реплики](#3-машина-состояний-реплики)
4. [Ключевые классы и их роли](#4-ключевые-классы-и-их-роли)
5. [Общий алгоритм RecoveryStrategy](#5-общий-алгоритм-recoverystrategy)
6. [Transaction Log (UpdateLog / TransactionLog)](#6-transaction-log-updatelog--transactionlog)
7. [PeerSync: быстрая синхронизация через транзакционные логи](#7-peersync-быстрая-синхронизация-через-транзакционные-логи)
8. [IndexFingerprint: верификация консистентности](#8-indexfingerprint-верификация-консистентности)
9. [Replication: полная репликация индекса](#9-replication-полная-репликация-индекса)
10. [Replay буферизованных обновлений](#10-replay-буферизованных-обновлений)
11. [HTTP-запросы и протоколы взаимодействия](#11-http-запросы-и-протоколы-взаимодействия)
12. [Форматы данных](#12-форматы-данных)
13. [Ресурсы: потоки, соединения, память, диск](#13-ресурсы-потоки-соединения-память-диск)
14. [Retry-логика и таймауты](#14-retry-логика-и-таймауты)
15. [Взаимодействие с ZooKeeper](#15-взаимодействие-с-zookeeper)
16. [Полная последовательность recovery для NRT-реплики](#16-полная-последовательность-recovery-для-nrt-реплики)
17. [Отличия recovery по типам реплик](#17-отличия-recovery-по-типам-реплик)
18. [Граничные случаи и защитные механизмы](#18-граничные-случаи-и-защитные-механизмы)
19. [Метрики](#19-метрики)

---

## 1. Типы реплик и их отличия

Solr поддерживает три типа реплик, определённых в `Replica.Type` (`solrj/src/java/org/apache/solr/common/cloud/Replica.java`):

| Характеристика | NRT | TLOG | PULL |
|---|---|---|---|
| Индексирует локально | Да | Нет | Нет |
| Ведёт transaction log | Да | Да | Нет |
| Может стать лидером | Да | Да | Нет |
| Поддерживает RTG (real-time get) | Да | Да | Нет |
| Поддерживает soft commits | Да | Нет | Нет |
| PeerSync при recovery | Да (первая попытка) | Нет | Нет |
| Фаза replay после sync | `applyBufferedUpdates()` | `copyOverBufferingUpdates()` | Отсутствует |
| Метод `requiresTransactionLog()` | true | true | false |

**NRT (Near Real Time)** — основной тип реплики. Она принимает распределённые обновления от лидера, сразу пишет их в локальный Lucene-индекс и в transaction log. Поддерживает RTG из tlog без открытия searcher'а.

---

## 2. Когда и почему начинается recovery

Recovery запускается в нескольких сценариях:

### 2.1. Запуск после падения (`recoveringAfterStartup = true`)
При старте SolrCore (`ZkController.register()`) система проверяет, была ли реплика ранее ACTIVE. Если да — выставляет флаг `recoveringAfterStartup = true` и запускает recovery. Реплика понимает, что она была недоступна и могла пропустить обновления.

### 2.2. Получение обновления с конфликтом версий
`DistributedUpdateProcessor` обнаруживает, что версия входящего обновления несовместима с локальным состоянием (реплика значительно отстала). Это инициирует вызов `SolrCore.getUpdateHandler().getSolrCoreState().doRecovery()`.

### 2.3. Потеря соединения с лидером
`ZkController` отслеживает наличие лидера через эфемерные ZK-узлы. При смене лидера или потере сессии реплика переходит в DOWN и запускает recovery.

### 2.4. Прямой вызов из `CoreAdminHandler`
Операция `REQUESTRECOVERY` может явно попросить реплику начать recovery.

### 2.5. Обнаружение неполной предыдущей репликации
При запуске, если в tlog-директории существует файл `buffer.tlog` от предыдущего незавершённого восстановления, метод `ulog.existOldBufferLog()` возвращает `true`. В этом случае PeerSync пропускается и сразу выполняется полная репликация.

---

## 3. Машина состояний реплики

Состояния определены в `Replica.State` и публикуются в ZooKeeper через `ZkController.publish()`.

```
              ┌─────────────────────────────────┐
              │                                 │
    [старт/сбой]                        [успешное recovery]
              │                                 │
              ▼                                 │
           DOWN ──────────────────────────── ACTIVE
              │                                 ▲
              │ [начало recovery]               │
              ▼                                 │
         RECOVERING ─────────────────────────────
              │
              │ [превышен maxRetries]
              ▼
       RECOVERY_FAILED
```

- **ACTIVE** — реплика обслуживает запросы
- **DOWN** — реплика выставляет себя недоступной до начала recovery (метод `pingLeader()` переводит реплику в DOWN при первой попытке подключения)
- **RECOVERING** — реплика активно восстанавливается; клиенты не направляют к ней запросы
- **RECOVERY_FAILED** — исчерпаны все попытки; реплика не может восстановиться

---

## 4. Ключевые классы и их роли

### `RecoveryStrategy`
`solr/core/src/java/org/apache/solr/cloud/RecoveryStrategy.java`

Основной оркестратор recovery. Реализует `Runnable` и `Closeable`. Запускается в отдельном потоке. Содержит всю логику: выбор между PeerSync и replication, retry-цикл, публикацию состояний в ZK.

Создаётся через внутренний `Builder`, который позволяет заменить реализацию через `solrconfig.xml` (точка расширения `@lucene.experimental`).

### `PeerSyncWithLeader`
`solr/core/src/java/org/apache/solr/update/PeerSyncWithLeader.java`

Реализует быструю синхронизацию NRT-реплики с лидером через сравнение transaction log версий. Использует `HttpSolrClient` для запросов к лидеру. Инкапсулирует логику `MissedUpdatesFinder` и `IndexFingerprint`.

### `PeerSync`
`solr/core/src/java/org/apache/solr/update/PeerSync.java`

Более общий вариант peer sync — для синхронизации между несколькими репликами (не только с лидером). Используется в нескольких контекстах: при leader election, при распределённых обновлениях. Содержит базовые классы `MissedUpdatesFinderBase`, `Updater`, `MissedUpdatesRequest`.

### `UpdateLog`
`solr/core/src/java/org/apache/solr/update/UpdateLog.java`

Управляет набором transaction log-файлов. Ведёт in-memory карту `Map<BytesRef, LogPtr>` для быстрого RTG. Содержит методы для буферизации обновлений во время recovery, применения буфера и получения последних версий. Хранит состояние (`ACTIVE`, `BUFFERING`, `APPLYING_BUFFERED`, `REPLAYING`).

### `TransactionLog`
`solr/core/src/java/org/apache/solr/update/TransactionLog.java`

Отдельный файл transaction log. Хранит бинарные записи обновлений в формате JavaBin. Реализует итератор для последовательного чтения и случайный доступ по позиции (указатель из `LogPtr`).

### `IndexFingerprint`
`solr/core/src/java/org/apache/solr/update/IndexFingerprint.java`

Структура для верификации совпадения индексов двух реплик. Вычисляется как хэш версий всех документов в Lucene-индексе. Используется для финальной проверки после PeerSync.

### `ReplicationHandler`
`solr/core/src/java/org/apache/solr/handler/ReplicationHandler.java`

HTTP-обработчик (`/replication`). Обслуживает как лидерскую часть (отдаёт файлы индекса) так и фоллоуэрскую (скачивает индекс). Метод `doFetch()` используется в RecoveryStrategy для полной репликации.

### `RealTimeGetComponent`
`solr/core/src/java/org/apache/solr/handler/component/RealTimeGetComponent.java`

HTTP-обработчик (`/get`). Обрабатывает запросы `getVersions`, `getUpdates`, `getFingerprint` — все три типа запросов, используемых при PeerSync.

### `ZkController`
`solr/core/src/java/org/apache/solr/cloud/ZkController.java`

Координирует взаимодействие с ZooKeeper: публикует состояния реплик, получает информацию о лидерах, управляет фоновой репликацией TLOG-реплик.

### `VersionInfo`
`solr/core/src/java/org/apache/solr/update/VersionInfo.java`

Управляет версионированием документов. Хранит version buckets — массив блокировок + версий для конкурентного управления версиями документов.

---

## 5. Общий алгоритм RecoveryStrategy

### Точка входа: `run()`

```java
// RecoveryStrategy.java:302
public final void run() {
    try (SolrCore core = cc.getCore(coreName)) {
        doRecovery(core);
    }
}
```

Метод `run()` открывает ссылку на `SolrCore` и делегирует в `doRecovery()`. Время выполнения измеряется через `RTimer`.

### Маршрутизация: `doRecovery()`

```java
// RecoveryStrategy.java:335
public final void doRecovery(SolrCore core) throws Exception {
    this.coreDescriptor = core.getCoreDescriptor();
    if (this.coreDescriptor.getCloudDescriptor().requiresTransactionLog()) {
        doSyncOrReplicateRecovery(core);  // NRT и TLOG
    } else {
        doReplicateOnlyRecovery(core);    // PULL
    }
}
```

### Основной метод для NRT: `doSyncOrReplicateRecovery()`

Полная пошаговая логика:

**Шаг 1: Инициализация**
```java
UpdateLog ulog = core.getUpdateHandler().getUpdateLog();
boolean firstTime = replicaType != Replica.Type.TLOG; // для NRT = true
```

**Шаг 2: Получение стартовых версий**
```java
List<Long> recentVersions = ulog.getRecentUpdates().getVersions(ulog.getNumRecordsToKeep());
List<Long> startingVersions = ulog.getStartingVersions();
```

Если `recoveringAfterStartup = true`, стартовые версии — это те, что были при последней остановке (`startingVersions`). Если существует старый `buffer.tlog`, выставляется `firstTime = false` (PeerSync пропускается).

**Шаг 3: Остановка фоновой репликации для TLOG-реплик**
```java
if (replicaType == Replica.Type.TLOG) {
    zkController.stopReplicationFromLeader(coreName);
}
```

**Шаг 4: Основной retry-цикл**

Цикл выполняется до успеха, принудительного закрытия или исчерпания `maxRetries`:

```
┌──────────────────────────────────────────────────────────────┐
│  RETRY LOOP                                                  │
│                                                              │
│  1. pingLeader() — дождаться живого лидера                  │
│     (при первой попытке — опубликовать DOWN в ZK)           │
│                                                              │
│  2. Проверить, не стали ли мы сами лидером                  │
│     (если да — опубликовать ACTIVE, выйти)                  │
│                                                              │
│  3. ulog.bufferUpdates()                                     │
│     (новые обновления идут в buffer.tlog, не в индекс)      │
│                                                              │
│  4. publish(RECOVERING) в ZK                                 │
│                                                              │
│  5. sendPrepRecoveryCmd() к лидеру                          │
│     (WaitForState: лидер ждёт, пока мы не станем RECOVERING)│
│                                                              │
│  6. Thread.sleep(2500ms)                                     │
│     (пауза для завершения обновлений с устаревшим состоянием)│
│                                                              │
│  7. PeerSync (только если firstTime = true):                │
│     ├── PeerSyncWithLeader.sync(recentVersions)             │
│     ├── Если успех → commit, replay(), break                │
│     └── Если неудача → перейти к репликации                 │
│                                                              │
│  8. Replication:                                             │
│     ├── replicate(leader) — полная загрузка индекса         │
│     └── Если успех → replay(), break                        │
│                                                              │
│  9. Если успех → publish(ACTIVE), recovered()               │
│                                                              │
│  10. Если неудача → waitBetweenRecoveries() (backoff)       │
└──────────────────────────────────────────────────────────────┘
```

---

## 6. Transaction Log (UpdateLog / TransactionLog)

### Файловая структура

Transaction log хранится в директории `<dataDir>/tlog/`. Имена файлов:

```
tlog.0000000000000000001   ← старый лог
tlog.0000000000000000002   ← ещё один
tlog.0000000000000000003   ← текущий активный
buffer.tlog                ← создаётся только во время recovery
```

Номер файла — монотонно возрастающий long-идентификатор.

Паттерн имён: `"%s.%019d"` (константа `LOG_FILENAME_PATTERN`).

### Формат записи в TransactionLog

Каждая запись в tlog — это массив JavaBin:

```
Для ADD:
  [flags|ADD, version:long, SolrInputDocument]
  
  Пример: [0x01, 1620000000000, {id:"doc1", title:"...", _version_:1620000000000}]

Для DELETE (by ID):
  [flags|DELETE, version:long, id:bytes]
  
  Пример: [0x02, -1620000000001, <bytes "doc1">]

Для DELETE_BY_QUERY:
  [flags|DELETE_BY_QUERY, version:long, query:string]
  
  Пример: [0x03, -1620000000002, "category:old"]

Для COMMIT:
  [flags|COMMIT, version:long, SolrParams или null]

Для UPDATE_INPLACE (partial update):
  [flags|UPDATE_INPLACE, version:long, prevPointer:long, prevVersion:long, SolrInputDocument]
```

Константы операций (из `UpdateLog.java`):
```java
public static final int ADD             = 0x01;
public static final int DELETE          = 0x02;
public static final int DELETE_BY_QUERY = 0x03;
public static final int COMMIT          = 0x04;
public static final int UPDATE_INPLACE  = 0x08;
public static final int OPERATION_MASK  = 0x0f;
```

Версии:
- **Положительные** → ADD / UPDATE
- **Отрицательные** → DELETE / DELETE_BY_QUERY

### In-memory карта документов

```java
// UpdateLog.java:209
protected Map<BytesRef, LogPtr> map = new HashMap<>();
```

`map` отображает `id документа (BytesRef)` → `LogPtr { pointer:long, version:long, previousPointer:long }`.

`pointer` — байтовое смещение в файле tlog. Используется для RTG: вместо открытия searcher'а можно читать документ прямо из tlog по указателю.

### Состояния UpdateLog

```java
public enum State {
    REPLAYING(0),          // Воспроизводится буфер (replay phase)
    BUFFERING(1),          // Recovery: новые update идут в buffer.tlog
    APPLYING_BUFFERED(2),  // Recovery: применяется накопленный буфер
    ACTIVE(3)              // Нормальная работа
}
```

### Буферизация во время recovery

`ulog.bufferUpdates()` — переключает UpdateLog в режим `BUFFERING`. Все входящие обновления (которые лидер продолжает слать) идут не в основной tlog и не в индекс, а в отдельный файл `buffer.tlog`. Это позволяет:
1. Не терять обновления во время синхронизации
2. Применить их после синхронизации через `replay()`

---

## 7. PeerSync: быстрая синхронизация через транзакционные логи

PeerSync — это оптимизация, позволяющая избежать полной репликации индекса (которая может занимать минуты), если реплика отстала незначительно. Вместо файлов индекса передаются только пропущенные обновления из transaction log лидера.

### Условие применимости

PeerSync возможен, если:
- Реплика NRT (не TLOG, не PULL)
- Это первая итерация retry-цикла (`firstTime = true`)
- Нет старого `buffer.tlog` (незавершённой репликации)
- Количество пропущенных обновлений ≤ `numRecordsToKeep` (по умолчанию 100)

### Алгоритм PeerSyncWithLeader.sync()

```
sync(startingVersions):
    1. alreadyInSync() — быстрая проверка fingerprint'а
       POST /get?qt=/get&getFingerprint=9223372036854775807
       Если fingerprint совпадает → return SUCCESS (skipped)
    
    2. Получить наши последние N версий из ulog
       ourUpdates = ulog.getRecentUpdates().getVersions(N)
       bufferedUpdates = recentUpdates.getBufferUpdates()
    
    3. Объединить ourUpdates и startingVersions (версии на момент старта)
    
    4. Вычислить пороговые значения:
       ourLowThreshold  = percentile(startingVersions, 0.8) — 80й перцентиль
       ourHighThreshold = percentile(startingVersions, 0.2) — 20й перцентиль
    
    5. doSync(ourUpdates, ourLowThreshold, ourHighThreshold)
```

### Алгоритм doSync()

```
doSync():
    1. getVersions() — получить версии лидера
       POST /get?qt=/get&getVersions=100&fingerprint=true
       Ответ: { versions: [v1, v2, ...], fingerprint: {...} }
    
    2. Построить MissedUpdatesFinder
    
    3. buildMissedUpdatesRequest(leaderVersions):
       MissedUpdatesFinder.find(leaderVersions, leaderUrl)
       → Вернёт одно из:
         ALREADY_IN_SYNC  (наши версии покрывают все версии лидера)
         UNABLE_TO_SYNC   (слишком большое отставание)
         MissedUpdatesRequest { versionsAndRanges, totalRequestedUpdates }
    
    4. requestUpdates(missedUpdatesRequest):
       POST /get?qt=/get&getUpdates=<ranges>&onlyIfActive=false&skipDbq=true
       Ответ: { updates: [...], fingerprint: {...} }
    
    5. handleUpdates(response, numRequested, leaderFingerprint):
       a. Добавить к updates содержимое bufferedUpdates из нашего tlog
       b. Применить фильтр: убрать обновления выше leaderFingerprint.maxVersionEncountered
          (если нет DBQ/DBI в этом gap'е — безопасно)
       c. updater.applyUpdates(updates, leaderUrl)
    
    6. compareFingerprint(leaderFingerprint):
       Вычислить наш IndexFingerprint
       Сравнить с лидерским
       return cmp == 0
```

### MissedUpdatesFinder: алгоритм поиска пропущенных версий

```
MissedUpdatesFinder.find(leaderVersions, updateFrom):
    
    Сортировать leaderVersions по абсолютному значению (убывание)
    leaderLowest = leaderVersions[last]   // самая старая версия лидера
    
    if |ourHighest| < |leaderLowest|:
        // наши версии старее самой старой версии лидера
        return UNABLE_TO_SYNC
    
    completeList = leaderVersions.size() < nUpdates
    // completeList = true → лидер вернул ВСЕ свои версии (их < N)
    
    updatesRequest = handleVersionsWithRanges(leaderVersions, completeList)
    
    if updatesRequest.totalRequestedUpdates > nUpdates:
        return UNABLE_TO_SYNC  // слишком много пропущенных
    
    return updatesRequest
```

`handleVersionsWithRanges()` строит строку диапазонов вида `"1000...500,200...100"`:
- Находит версии, которые есть у лидера, но отсутствуют у нас
- Группирует их в непрерывные диапазоны для минимизации размера запроса
- Формат диапазона: `"high...low"` или одиночное значение `"version"`

### PeerSync.Updater.applyUpdates()

```
applyUpdates(updates, leaderUrl):
    
    1. Отсортировать updates по версии (по абсолютному значению, возрастание)
    
    2. Для каждого update:
       flags = update[0] & OPERATION_MASK
       version = update[1]
       
       switch(flags):
         ADD:
           doc = (SolrInputDocument) update[2]
           // Создать AddUpdateCommand и применить через processor chain
           
         DELETE (by ID):
           id = (BytesRef или String) update[2]
           // Создать DeleteUpdateCommand
           
         DELETE_BY_QUERY:
           query = (String) update[2]
           // Создать DeleteUpdateCommand с query
           
         UPDATE_INPLACE:
           prevPointer = update[2]
           prevVersion = update[3]
           doc = update[4]
           // Создать AddUpdateCommand с inplace флагом
    
    3. Применить через UpdateRequestProcessor с флагами:
       DISTRIB_UPDATE_PARAM = FROMLEADER
       _version_ = version
```

---

## 8. IndexFingerprint: верификация консистентности

`IndexFingerprint` — криптографически-стойкий хэш состояния индекса, позволяющий быстро проверить, совпадают ли два индекса.

### Структура

```java
// IndexFingerprint.java
{
    maxVersionSpecified:  long,  // запрошенный верхний предел версий
    maxVersionEncountered: long, // максимальная версия, встреченная в индексе
    maxInHash:            long,  // максимальная версия, включённая в хэш
    versionsHash:         long,  // FMix64 хэш всех версий <= maxVersionSpecified
    numVersions:          long,  // количество документов, включённых в хэш
    numDocs:              long,  // количество живых документов в индексе
    maxDoc:               long   // maxDoc сегмента (включая удалённые)
}
```

### Алгоритм вычисления

```java
// IndexFingerprint.java:110-136
for (int doc = 0; doc < maxDoc; doc++) {
    if (liveDocs != null && !liveDocs.get(doc)) continue;  // пропустить удалённые
    long v = fv.longVal(doc);                               // прочитать поле _version_
    f.maxVersionEncountered = Math.max(v, f.maxVersionEncountered);
    if (v <= f.maxVersionSpecified) {
        f.maxInHash = Math.max(v, f.maxInHash);
        f.versionsHash += Hash.fmix64(v);                  // FMix64 (Murmur3-компонент)
        f.numVersions++;
    }
}
```

Используется `Hash.fmix64()` — это финальная стадия хэша MurmurHash3, обеспечивающая хорошее перемешивание битов. Суммирование хэшей коммутативно — порядок документов не важен.

### Алгоритм сравнения

```java
// IndexFingerprint.java:153-176
public static int compare(IndexFingerprint f1, IndexFingerprint f2) {
    if (f1.maxVersionSpecified == Long.MAX_VALUE) {
        cmp = Long.compare(f1.maxVersionEncountered, f2.maxVersionEncountered);
        if (cmp != 0) return cmp;
    }
    cmp = Long.compare(f1.maxInHash, f2.maxInHash);
    if (cmp != 0) return cmp;
    cmp = Long.compare(f1.numVersions, f2.numVersions);
    if (cmp != 0) return cmp;
    cmp = Long.compare(f1.versionsHash, f2.versionsHash);
    return cmp;
}
```

Для equality требуется `compare() == 0`.

### Производительность

Вычисление fingerprint'а требует полного сканирования Lucene-индекса через `SolrIndexSearcher`. Это операция O(numDocs). Fingerprint кэшируется в searcher'е — повторные вызовы с тем же `maxVersion` возвращают кэшированный результат без пересканирования.

---

## 9. Replication: полная репликация индекса

Если PeerSync не удался (реплика слишком сильно отстала), выполняется полная копирование индекса с лидера.

### Инициация: `replicate()`

```java
// RecoveryStrategy.java:221-286
private void replicate(String nodeName, SolrCore core, ZkNodeProps leaderprops) {
    // Для NRT/TLOG реплик — сначала сделать commit на лидере
    if (Replica.Type.isLeaderType(replicaType)) {
        commitOnLeader(leaderBaseUrl, leaderCore);
    }
    
    // Получить ReplicationHandler локального core
    ReplicationHandler replicationHandler = core.getRequestHandler(ReplicationHandler.PATH);
    
    // Запустить репликацию (синхронно)
    boolean success = replicationHandler.doFetch(solrParams, false).getSuccessful();
}
```

### commitOnLeader()

Перед репликацией на лидере выполняется `COMMIT` с `openSearcher=false`. Это гарантирует, что все данные в индексе лидера сброшены на диск и образуют стабильный IndexCommit, который можно скопировать.

Запрос:
```
POST /update?commit=true&openSearcher=false
```

### Протокол ReplicationHandler

Полная репликация происходит через `/replication` endpoint:

**Шаг 1: Получение текущей версии индекса**
```
GET /replication?command=indexversion
Ответ: { indexversion: 12345, generation: 67 }
```

**Шаг 2: Список файлов для копирования**
```
GET /replication?command=filelist&indexversion=12345
Ответ: {
    filelist: [
        { name: "_0.cfs", size: 1024000, checksum: "abc123" },
        { name: "_0.cfe", size: 512, ... },
        { name: "segments_5", size: 200, ... }
    ],
    confFiles: [...]
}
```

**Шаг 3: Потоковое скачивание файлов**
```
GET /replication?command=filecontent&file=_0.cfs&generation=67&offset=0
Ответ: <бинарный поток содержимого файла>
```

Файлы скачиваются по одному или параллельно (зависит от конфигурации). Каждый файл верифицируется по checksum. Уже имеющиеся файлы с совпадающим checksum не скачиваются повторно.

После загрузки всех файлов происходит атомарное переключение `IndexWriter` на новую директорию индекса.

---

## 10. Replay буферизованных обновлений

После успешного PeerSync или Replication необходимо применить обновления, накопившиеся в `buffer.tlog` во время синхронизации.

### Для NRT-реплик: `applyBufferedUpdates()`

```java
// RecoveryStrategy.java:844-865
private final Future<RecoveryInfo> replay(SolrCore core) {
    // NRT путь:
    Future<RecoveryInfo> future = core.getUpdateHandler().getUpdateLog().applyBufferedUpdates();
    if (future == null) {
        log.info("No replay needed.");
    } else {
        RecoveryInfo report = future.get();  // ждём завершения
        if (report.failed) throw new SolrException(...);
    }
    core.getUpdateHandler().getUpdateLog().openRealtimeSearcher();
    return future;
}
```

`applyBufferedUpdates()` запускается в `recoveryExecutor` (отдельный thread pool) и возвращает `Future<RecoveryInfo>`. Содержимое `buffer.tlog` читается последовательно и применяется через `UpdateRequestProcessor`.

### Для TLOG-реплик: `copyOverBufferingUpdates()`

```java
// RecoveryStrategy.java:836-843
if (replicaType == Replica.Type.TLOG) {
    SolrQueryRequest req = new LocalSolrQueryRequest(core, new ModifiableSolrParams());
    core.getUpdateHandler().getUpdateLog()
        .copyOverBufferingUpdates(new CommitUpdateCommand(req, false));
    req.close();
    return null;
}
```

TLOG-реплика не индексирует документы — вместо этого она переносит буферизованные записи в основной tlog, делая их доступными для RTG и последующей индексации при необходимости.

### `openRealtimeSearcher()`

После replay вызывается `UpdateLog.openRealtimeSearcher()`. Это инвалидирует кэши, устаревшие по сравнению с новым состоянием индекса, и открывает новый real-time searcher. Без этого шага RTG-запросы могут возвращать устаревшие данные.

### `RecoveryInfo`

```java
public static class RecoveryInfo {
    public long positionOfStart;   // байтовая позиция начала в buffer.tlog
    public int adds;               // количество применённых ADD
    public int deletes;            // количество DELETE by ID
    public int deleteByQuery;      // количество DELETE by query
    public AtomicInteger errors;   // количество ошибок
    public boolean failed;         // общий флаг неудачи
}
```

---

## 11. HTTP-запросы и протоколы взаимодействия

### Запрос 1: WaitForState (PrepRecovery)

**Направление:** Восстанавливающаяся реплика → Лидер

**Endpoint:** `POST /admin/cores?action=PREPRECOVERY`

**Параметры:**
```
nodeName        = <имя ZK-узла восстанавливающейся реплики>
coreNodeName    = <coreNodeName из ZK>
coreName        = <имя core лидера>
state           = recovering
checkLive       = true
onlyIfLeader    = true
onlyIfLeaderActive = true  (если shard не в CONSTRUCTION/RECOVERY/RECOVERY_FAILED)
```

**Семантика:** Лидер держит соединение открытым, пока не увидит в ZooKeeper, что данная реплика перешла в состояние RECOVERING. Таймаут: `conflictWaitMs + 8000ms` (по умолчанию ~18 секунд). Это синхронизационный барьер: лидер знает, что реплика буферизует обновления с этого момента.

**Класс:** `CoreAdminRequest.WaitForState`

---

### Запрос 2: getVersions (PeerSync, шаг 1)

**Направление:** Восстанавливающаяся реплика → Лидер

**Endpoint:** `POST /get?qt=/get&distrib=false&getVersions=100&fingerprint=true`

**Ответ:**
```json
{
    "versions": [1000003, 1000002, 1000001, -999999, 1000000, ...],
    "fingerprint": {
        "maxVersionSpecified": 9223372036854775807,
        "maxVersionEncountered": 1000003,
        "maxInHash": 1000003,
        "versionsHash": -3456789012345678901,
        "numVersions": 150,
        "numDocs": 150,
        "maxDoc": 160
    }
}
```

**Семантика:** Лидер возвращает последние N версий из своего UpdateLog плюс IndexFingerprint своего индекса. Версии отсортированы по абсолютному значению (новейшие первые). Отрицательные версии — операции DELETE.

**Обработчик на стороне лидера:** `RealTimeGetComponent.processGetVersions()`

---

### Запрос 3: getFingerprint (PeerSync, pre-check)

**Направление:** Восстанавливающаяся реплика → Лидер

**Endpoint:** `POST /get?qt=/get&distrib=false&getFingerprint=9223372036854775807`

**Ответ:**
```json
{
    "fingerprint": { ... }
}
```

**Семантика:** Предварительная проверка — если fingerprint уже совпадает, можно пропустить всю процедуру PeerSync. `9223372036854775807` = `Long.MAX_VALUE` означает "fingerprint по всем версиям".

---

### Запрос 4: getUpdates (PeerSync, шаг 2)

**Направление:** Восстанавливающаяся реплика → Лидер

**Endpoint:** `POST /get?qt=/get&distrib=false&getUpdates=1000003...999990,999985&onlyIfActive=false&skipDbq=true`

**Параметр `getUpdates`:** строка диапазонов
- `"1000003...999990"` — все версии с 999990 по 1000003 включительно
- `"999985"` — одиночная версия 999985
- Разделяются запятой

**Параметр `skipDbq`:** если `true`, DELETE_BY_QUERY записи пропускаются (для экономии; они применяются отдельно)

**Параметр `onlyIfActive`:** если `false`, лидер отвечает даже если не в ACTIVE состоянии

**Ответ:**
```json
{
    "updates": [
        [1, 999990, {"id": "doc1", "_version_": 999990, "title": "..."}],
        [2, -999991, "doc2"],
        [1, 1000003, {"id": "doc3", "_version_": 1000003, ...}]
    ],
    "fingerprint": { ... }
}
```

Каждый элемент `updates` — это список:
- `[0]` — flags (1=ADD, 2=DELETE, 3=DBQ, 4=COMMIT, 8=UPDATE_INPLACE)
- `[1]` — version (long)
- `[2+]` — данные (документ, id, query в зависимости от типа)

---

### Запрос 5: Ping лидера (проверка живости)

**Направление:** Восстанавливающаяся реплика → Лидер

**Endpoint:** `GET /admin/ping`

**Семантика:** Проверка, что лидер доступен по сети. При неудаче — пауза 500ms и повтор. При первой неудаче после попытки — публикация DOWN в ZK.

---

### Запрос 6: Commit на лидере (перед полной репликацией)

**Направление:** Восстанавливающаяся реплика → Лидер

**Endpoint:** `POST /update?commit=true&openSearcher=false`

**Семантика:** Принудительный hard commit на лидере, чтобы зафиксировать все pending-обновления в Lucene-сегментах перед копированием файлов. `openSearcher=false` — не открывать searcher, только сбросить на диск.

---

## 12. Форматы данных

### JavaBin (транзакционный лог)

Transaction log использует Apache Solr JavaBin-кодек — компактный бинарный формат, специфичный для Solr. Основные типы:
- `String` → тег + UTF-8 байты с intern-таблицей для повторяющихся строк
- `long` → тег + 8 байт
- `SolrInputDocument` → рекурсивная структура с полями
- `List` → тег + размер + элементы

JavaBin значительно компактнее JSON и поддерживает `String interning` — повторяющиеся строки (имена полей) записываются по одному разу и далее ссылаются по индексу.

### HTTP-ответы getVersions / getUpdates

Сериализуются через стандартный Solr response writer. По умолчанию используется `javabin` (Content-Type: `application/octet-stream`), что обеспечивает компактность и скорость.

### ZooKeeper-данные

Состояние реплики в ZK хранится как JSON-объект в ephemeral-узле:
```
/collections/{collection}/shards/{shard}/replicas/{coreName}
```
Содержимое:
```json
{
    "state": "recovering",
    "base_url": "http://host:8983/solr",
    "core": "collection1_shard1_replica1",
    "node_name": "host:8983_solr",
    "type": "NRT"
}
```

---

## 13. Ресурсы: потоки, соединения, память, диск

### Потоки

**Recovery thread:**
- 1 поток на каждую восстанавливающуюся core
- Создаётся через `SolrCoreState.doRecovery()` → `recoveryStrat.run()` в отдельном потоке
- Поток называется `"recoveryStrat"` + имя core

**recoveryExecutor (для replay):**
```java
// UpdateLog.java — инициализируется при старте UpdateLog
ThreadPoolExecutor recoveryExecutor = new ThreadPoolExecutor(
    0,                    // corePoolSize = 0 (нет постоянных потоков)
    Integer.MAX_VALUE,    // maxPoolSize = unbounded
    1, TimeUnit.SECONDS,  // keepAlive = 1 секунда
    new SynchronousQueue<>()  // очередь нулевого размера
);
```
Потоки создаются по требованию для `applyBufferedUpdates()`. После завершения replay поток живёт 1 секунду и умирает.

**ShardHandler threads (для PeerSync в PeerSync.java):**
Используется `ShardHandlerFactory` для параллельных запросов к нескольким репликам (актуально для `PeerSync`, не `PeerSyncWithLeader`).

### HTTP-соединения

```java
// RecoveryStrategy.java:179-188
private HttpSolrClient.Builder recoverySolrClientBuilder(String baseUrl, String coreName) {
    final UpdateShardHandlerConfig cfg = cc.getConfig().getUpdateShardHandlerConfig();
    return new HttpSolrClient.Builder(baseUrl)
        .withDefaultCollection(coreName)
        .withConnectionTimeout(cfg.getDistributedConnectionTimeout(), MILLISECONDS)
        .withSocketTimeout(cfg.getDistributedSocketTimeout(), MILLISECONDS)
        .withHttpClient(cc.getUpdateShardHandler().getRecoveryOnlyHttpClient());
}
```

- Используется **отдельный HTTP-клиент** `getRecoveryOnlyHttpClient()` — чтобы recovery не конкурировало за соединения с обычными update-запросами
- По умолчанию `distributedConnectionTimeout` = 15 000ms, `distributedSocketTimeout` = 600 000ms
- Для `sendPrepRecoveryCmd()` socket timeout увеличивается: `conflictWaitMs + prepRecoveryReadTimeoutExtraWait` (≈ 10 000 + 8 000 = 18 000ms)

### Память

**UpdateLog in-memory map:**
```java
protected Map<BytesRef, LogPtr> map = new HashMap<>();
```
Размер ≈ `numRecordsToKeep × (размер ключа + 32 байта на LogPtr)`. При 100 записях по 1KB ключей = ~130KB. На практике незначителен.

**PeerSyncWithLeader:**
- `List<Long> ourUpdates` — `N × 8` байт ≈ 800 байт при N=100
- `List<Long> leaderVersions` — аналогично
- `List<Object> updates` — пропущенные обновления: каждый документ полностью в памяти

**IndexFingerprint:**
Объект ~7 long = 56 байт. Вычисление требует загрузки _version_ поля всех документов через `FunctionValues` — итерация по всему индексу, но без загрузки полных документов в память.

**Буфер tlog:**
Хранится на диске (файл `buffer.tlog`), в памяти только указатели (`map`). Размер на диске зависит от rate обновлений × время синхронизации.

### Диск

**Файлы tlog:**
```
numRecordsToKeep × (средний размер документа + overhead)
```
По умолчанию хранится несколько последних tlog-файлов. `maxNumLogsToKeep` (обычно 10) определяет максимальное количество.

**Полная репликация:**
Временно занимает `2 × размер индекса` на диске: старый индекс + новый скачанный. После успешной замены старый удаляется.

---

## 14. Retry-логика и таймауты

### Retry-цикл

```java
// RecoveryStrategy.java:483-503
long loopCount = retries < 5 ? Math.round(Math.pow(2, retries)) : 30;
// Ожидание между попытками (в секундах):
// retries=0: 2^0 = 1  → 1 × 2с = 2с
// retries=1: 2^1 = 2  → 2 × 2с = 4с
// retries=2: 2^2 = 4  → 4 × 2с = 8с
// retries=3: 2^3 = 8  → 8 × 2с = 16с
// retries=4: 2^4 = 16 → 16 × 2с = 32с
// retries≥5: 30       → 30 × 2с = 60с (максимум)

for (int i = 0; i < loopCount; i++) {
    Thread.sleep(startingRecoveryDelayMilliSeconds);  // 2000ms по умолчанию
}
```

| Попытка | Ожидание |
|---------|---------|
| 0 | 2 сек |
| 1 | 4 сек |
| 2 | 8 сек |
| 3 | 16 сек |
| 4 | 32 сек |
| 5+ | 60 сек |

Максимальное количество попыток: `maxRetries = 500` (настраивается через `setMaxRetries()`).

### `waitForUpdatesWithStaleStatePauseMilliSeconds`

```java
// RecoveryStrategy.java:105-107
private int waitForUpdatesWithStaleStatePauseMilliSeconds =
    Integer.getInteger("solr.cloud.wait-for-updates-with-stale-state-pause", 2500);
```

После публикации RECOVERING и получения подтверждения от лидера через `sendPrepRecoveryCmd()`, реплика дополнительно спит 2500ms. Цель: дать лидеру время завершить все обновления, которые начались до того, как лидер увидел смену состояния реплики в ZK. Это предотвращает race condition (SOLR-7141).

### `prepRecoveryReadTimeoutExtraWait`

```java
// RecoveryStrategy.java:912-913
int readTimeout = conflictWaitMs +
    Integer.parseInt(System.getProperty("prepRecoveryReadTimeoutExtraWait", "8000"));
```

Дополнительные 8 секунд к таймауту `sendPrepRecoveryCmd()` сверх `conflictWaitMs` (обычно 10000ms, итого ~18 секунд).

---

## 15. Взаимодействие с ZooKeeper

### Иерархия ZK-узлов

```
/collections/
  └── {collectionName}/
        └── shards/
              └── {shardName}/
                    ├── leader           ← ephemeral, данные о текущем лидере
                    ├── election/        ← ephemeral sequential узлы для выборов
                    │     ├── n_0000000001
                    │     └── n_0000000002
                    └── replicas/        ← persistent узлы реплик
                          ├── core_node1  ← state: active/recovering/down/...
                          └── core_node2
```

### Публикация состояний

```java
// ZkController.publish()
zkStateReader.getZkClient().setData(
    ZkStateReader.getShardLeadersPath(collection, shard),
    Utils.toJSON(stateProps),
    true
);
```

Публикация состояния в ZK — синхронная операция. Лидер подписан на watch на эти узлы и сразу видит смену состояния реплики.

### `getLeaderRetry()`

```java
Replica leader = zkStateReader.getLeaderRetry(collection, shard);
```

Повторяет попытку с таймаутом (обычно 60 секунд) в ожидании, пока лидер не появится в ZK. Используется в начале каждой итерации retry-цикла.

### Leader Election и Recovery

Если во время recovery эта реплика сама стала лидером (из-за ухода всех других реплик):

```java
// RecoveryStrategy.java:606-611
if (cloudDesc.isLeader()) {
    log.warn("We have not yet recovered - but we are now the leader!");
    zkController.publish(this.coreDescriptor, Replica.State.ACTIVE);
    return;  // прерываем recovery, становимся активными как лидер
}
```

---

## 16. Полная последовательность recovery для NRT-реплики

Ниже приведена полная последовательность событий от момента обнаружения необходимости recovery до перехода в ACTIVE:

```
┌─────────────────────────────────────────────────────────────────────┐
│ NRT Replica Recovery — полная последовательность                    │
└─────────────────────────────────────────────────────────────────────┘

Триггер: ZkController.register() обнаружил что реплика была ACTIVE
         → recoveringAfterStartup = true
         → SolrCoreState.doRecovery() создаёт RecoveryStrategy

ПОТОК RECOVERY (отдельный thread):

[1] run() → doRecovery() → doSyncOrReplicateRecovery()

[2] Получить recentVersions из UpdateLog:
    ulog.getRecentUpdates().getVersions(100) → [v100, v99, v98, ...]
    
    Если recoveringAfterStartup = true:
      recentVersions = ulog.getStartingVersions()
      Проверить existOldBufferLog() → если true, firstTime=false

─────────────────────── RETRY LOOP ───────────────────────────────────

[3] pingLeader(ourUrl, coreDescriptor, mayPutReplicaAsDown=true):
    ├── zkStateReader.getLeaderRetry(collection, shard)
    ├── Если numTried==1 и мы были ACTIVE → publish(DOWN)
    ├── HTTP GET http://leader:8983/solr → /admin/ping
    └── При IOException → sleep(500ms) + retry

[4] Проверка: если мы стали лидером → publish(ACTIVE) + return

[5] ulog.bufferUpdates():
    ├── Создать buffer.tlog (или удалить старый и создать новый)
    ├── state = BUFFERING
    └── Все входящие обновления идут в buffer.tlog

[6] publish(RECOVERING) → ZooKeeper

[7] sendPrepRecoveryCmd(leaderBaseUrl, leaderCoreName, slice):
    POST http://leader/admin/cores?action=PREPRECOVERY
         &nodeName=our_node &coreNodeName=core_nodeN
         &coreName=leaderCore &state=recovering
         &checkLive=true &onlyIfLeader=true
    
    Лидер: держит соединение, пока в ZK не увидит нашу реплику
           в состоянии RECOVERING
    Socket timeout = conflictWaitMs (10s) + 8s = ~18s

[8] Thread.sleep(2500ms)
    (пауза на случай race condition с обновлениями от лидера)

─────────── ВЕТКА A: PeerSync (только firstTime=true) ────────────────

[9a] Быстрая проверка alreadyInSync():
     POST http://leader/get?qt=/get&distrib=false
          &getFingerprint=9223372036854775807
     Если fingerprint совпадает → SKIP PeerSync → SUCCESS

[9b] Получить ourUpdates из ulog + bufferedUpdates
     Вычислить ourLowThreshold = percentile(startingVersions, 0.8)

[9c] POST http://leader/get?qt=/get&distrib=false
          &getVersions=100&fingerprint=true
     ← { versions:[v1,v2,...], fingerprint:{...} }

[9d] MissedUpdatesFinder.find(leaderVersions):
     ├── Если ourHighest < leaderLowest → UNABLE_TO_SYNC
     ├── Если все наши версии покрывают версии лидера → ALREADY_IN_SYNC
     └── Иначе → MissedUpdatesRequest("1000...990,985", 15)

     Если UNABLE_TO_SYNC или слишком много пропущенных → перейти к ветке B

[9e] POST http://leader/get?qt=/get&distrib=false
          &getUpdates=1000...990,985
          &onlyIfActive=false&skipDbq=true
     ← { updates:[[1,v,doc],[2,v,id],...], fingerprint:{...} }

[9f] handleUpdates():
     ├── Добавить bufferedUpdates к списку
     ├── Применить фильтр по leaderFingerprint.maxVersionEncountered
     └── updater.applyUpdates(updates)

[9g] compareFingerprint(leaderFingerprint):
     ├── IndexFingerprint.getFingerprint(core, Long.MAX_VALUE)
     │   (полное сканирование Lucene-индекса)
     └── IndexFingerprint.compare(leader, ours) == 0 ?

[9h] ЕСЛИ SUCCESS:
     ├── commit(openSearcher=false)
     ├── replay(core):
     │   ├── ulog.applyBufferedUpdates() → Future<RecoveryInfo>
     │   ├── future.get() (ждём завершения)
     │   └── ulog.openRealtimeSearcher()
     └── successfulRecovery = true → перейти к [12]

─────────── ВЕТКА B: Full Replication ───────────────────────────────

[10a] commitOnLeader(leaderBaseUrl, leaderCore):
      POST http://leader/update?commit=true&openSearcher=false

[10b] replicationHandler.doFetch(params, false):
      GET http://leader/replication?command=indexversion
      ← { indexversion:12345, generation:67 }
      
      GET http://leader/replication?command=filelist&indexversion=12345
      ← { filelist:[{name:"_0.cfs",size:...},...] }
      
      Для каждого файла, которого нет локально:
        GET http://leader/replication?command=filecontent&file=_0.cfs&...
        ← <бинарный поток>
      
      Атомарная замена директории индекса

[10c] replay(core):
      ulog.applyBufferedUpdates() → Future<RecoveryInfo>
      future.get()
      ulog.openRealtimeSearcher()

[10d] successfulRecovery = true

─────────── ЗАВЕРШЕНИЕ ───────────────────────────────────────────────

[12] publish(ACTIVE) → ZooKeeper
[13] recoveryListener.recovered()
[14] close = true (завершить recovery thread)

─────────── ЕСЛИ НЕУДАЧА ─────────────────────────────────────────────

[E] waitBetweenRecoveries():
    ├── retries++
    ├── Если retries >= 500 → recoveryFailed() → publish(RECOVERY_FAILED)
    └── sleep(2^retries × 2s, но не более 60s)
    → вернуться к [3]
```

---

## 17. Отличия recovery по типам реплик

### NRT Recovery

```
1. Пытается PeerSync (firstTime=true)
2. Fallback на Replication
3. replay = applyBufferedUpdates() — применяет updates в Lucene-индекс
```

### TLOG Recovery

```
1. PeerSync НЕ пытается (firstTime=false, потому что replicaType==TLOG)
2. Остановить фоновую репликацию: zkController.stopReplicationFromLeader()
3. Replication (всегда)
4. replay = copyOverBufferingUpdates() — переносит в tlog, НЕ в индекс
5. После: возобновить фоновую репликацию: zkController.startReplicationFromLeader(true)
```

Флаг `SKIP_COMMIT_ON_LEADER_VERSION_ZERO=true` при репликации для TLOG: при replication не нужен повторный commit на лидере (поведение отличается от NRT).

### PULL Recovery (`doReplicateOnlyRecovery()`)

```
1. requiresTransactionLog() = false → отдельный упрощённый путь
2. Нет буферизации updates (нет tlog)
3. Нет PeerSync
4. Только Replication
5. После: zkController.startReplicationFromLeader(false)
   (false = не запускать commit на лидере перед репликацией)
6. Нет фазы replay (нет tlog)
```

---

## 18. Граничные случаи и защитные механизмы

### Race condition при смене состояния ZK (SOLR-7141)

**Проблема:** Между публикацией RECOVERING в ZK и тем, как лидер это увидит, он мог отправить обновления со старым знанием о состоянии реплики (forwarded updates, которые не попали в buffer.tlog).

**Решение:** 2500ms пауза после `sendPrepRecoveryCmd()`. За это время лидер гарантированно завершит все незавершённые forwarded updates.

### Незавершённая предыдущая репликация (buffer.tlog)

**Обнаружение:**
```java
ulog.existOldBufferLog()  // true если buffer.tlog существует на диске
```

**Действие:** `firstTime = false` → PeerSync пропускается, сразу идёт полная репликация. При следующем вызове `ulog.bufferUpdates()` старый `buffer.tlog` удаляется.

### Слишком большое отставание (UNABLE_TO_SYNC)

`MissedUpdatesFinder` возвращает `UNABLE_TO_SYNC` в двух случаях:
1. `Math.abs(ourHighest) < Math.abs(leaderLowest)` — наши версии старее самых старых у лидера
2. `totalRequestedUpdates > nUpdates` — пропущенных обновлений больше, чем умещается в tlog

В обоих случаях PeerSync падает, и recovery продолжается через полную репликацию.

### Мы стали лидером во время recovery

Проверяется в начале каждой итерации:
```java
if (cloudDesc.isLeader()) {
    zkController.publish(coreDescriptor, Replica.State.ACTIVE);
    return;
}
```

### Версионный конфликт: наши данные новее лидерских

Если наши данные новее лидерских (мы были лидером и откатились), `MissedUpdatesFinder` не падает с UNABLE_TO_SYNC — он позволяет продолжить. Но fingerprint-сравнение после PeerSync покажет несовпадение, и мы упадём на полную репликацию, которая перезапишет наш индекс актуальным состоянием.

### Прерывание recovery

`RecoveryStrategy.close()` выставляет `close = true` и вызывает `prevSendPreRecoveryHttpUriRequest.abort()` для принудительного прерывания ожидающего HTTP-запроса к лидеру. Все точки retry-цикла проверяют `isClosed()` и корректно завершают поток.

---

## 19. Метрики

`PeerSyncWithLeader` и `PeerSync` регистрируют метрики в категории `REPLICATION` через `SolrMetricProducer`:

| Метрика | Тип | Описание |
|---------|-----|---------|
| `REPLICATION.peerSync.time` | Timer | Время выполнения PeerSync (только когда действительно выполняется, без skipped) |
| `REPLICATION.peerSync.errors` | Counter | Количество неудачных PeerSync |
| `REPLICATION.peerSync.skipped` | Counter | Количество случаев, когда fingerprint уже совпадал (alreadyInSync) |

Метрики `applyingBufferedOpsMeter`, `replayOpsMeter`, `copyOverOldUpdatesMeter` ведутся в `UpdateLog` для мониторинга производительности replay-фазы.

---

## Краткая справочная таблица

| Параметр | Значение по умолчанию | JVM-свойство / источник |
|---|---|---|
| `maxRetries` | 500 | `RecoveryStrategy.setMaxRetries()` |
| `startingRecoveryDelayMilliSeconds` | 2000ms | `RecoveryStrategy.setStartingRecoveryDelayMilliSeconds()` |
| `waitForUpdatesWithStaleStatePauseMilliSeconds` | 2500ms | `-Dsolr.cloud.wait-for-updates-with-stale-state-pause` |
| `prepRecoveryReadTimeoutExtraWait` | 8000ms | `-DprepRecoveryReadTimeoutExtraWait` |
| `numRecordsToKeep` | 100 | `UpdateLog.numRecordsToKeep` |
| `maxNumLogsToKeep` | 10 | `UpdateLog.maxNumLogsToKeep` |
| `distributedConnectionTimeout` | 15000ms | `solrconfig.xml <updateShardHandlerConfig>` |
| `distributedSocketTimeout` | 600000ms | `solrconfig.xml <updateShardHandlerConfig>` |
| PeerSync disabled | false | `-Dsolr.disableFingerprint=true` |
| conflictWaitMs | 10000ms | `ZkController.getLeaderConflictResolveWait()` |
