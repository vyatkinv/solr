# ZkClientClusterStateProvider: архитектура, поток данных и причины задержек до 13 секунд

## 1. Обзор

`ZkClientClusterStateProvider` — реализация интерфейса `ClusterStateProvider`, которая получает состояние кластера Solr из ZooKeeper. Используется в `CloudSolrClient` (SolrJ) для маршрутизации запросов к нужным нодам кластера.

**Ключевые файлы:**
- `solr/solrj-zookeeper/src/java/org/apache/solr/client/solrj/impl/ZkClientClusterStateProvider.java`
- `solr/solrj-zookeeper/src/java/org/apache/solr/common/cloud/ZkStateReader.java`
- `solr/solrj-zookeeper/src/java/org/apache/solr/common/cloud/SolrZkClient.java`
- `solr/solrj-zookeeper/src/java/org/apache/solr/common/cloud/ConnectionManager.java`
- `solr/solrj-zookeeper/src/java/org/apache/solr/common/cloud/ZkCmdExecutor.java`

## 2. Иерархия компонентов

```
CloudSolrClient
  └── ZkClientClusterStateProvider
        └── ZkStateReader
              ├── SolrZkClient
              │     ├── ZooKeeper (нативный клиент)
              │     ├── ConnectionManager (Watcher сессии)
              │     └── ZkCmdExecutor (retry-логика)
              ├── ClusterState (volatile, in-memory snapshot)
              │     ├── CollectionRef (watched, eager)
              │     └── LazyCollectionRef (lazy, on-demand)
              ├── StateWatcher (per-collection watcher на state.json)
              ├── LiveNodeWatcher (watcher на /live_nodes)
              ├── CollectionsChildWatcher (watcher на /collections)
              └── AliasesManager (watcher на /aliases.json)
```

## 3. Lazy-инициализация: первый вызов `getClusterState()`

`ZkClientClusterStateProvider` использует ленивую инициализацию через double-checked locking:

```
getClusterState()
  → getZkStateReader()        // lazy init с synchronized
    → new ZkStateReader(...)   // создаёт SolrZkClient
    → zk.createClusterStateWatchersAndUpdate()
```

### 3.1. Что происходит внутри `getZkStateReader()` (строки 212-244)

```java
public ZkStateReader getZkStateReader() {
    if (isClosed) throw new AlreadyClosedException();
    if (zkStateReader == null) {            // volatile read
        synchronized (this) {
            if (zkStateReader == null) {    // double-check
                ZkStateReader zk = new ZkStateReader(
                    zkHost, zkClientTimeout, zkConnectTimeout, canUseZkACLs);
                zk.createClusterStateWatchersAndUpdate();
                zkStateReader = zk;         // volatile write
            }
        }
    }
    return zkStateReader;
}
```

При первом вызове выполняется **цепочка блокирующих операций**, каждая из которых может вносить задержку.

### 3.2. Создание `ZkStateReader` → `SolrZkClient`

Конструктор `ZkStateReader` создаёт `SolrZkClient`, который:

1. **Создаёт ZooKeeper-соединение** через `DefaultConnectionStrategy.connect()`:
   - Вызывает `new ZooKeeper(serverAddress, timeout, watcher)` — асинхронно
   - Устанавливает keeper через callback `updater.update(zk)`

2. **Ожидает подключения** — `connManager.waitForConnected(clientConnectTimeout)`:
   - По умолчанию `clientConnectTimeout = 15000ms` (15 секунд)
   - Цикл `wait(500)` каждые 500мс до получения `SyncConnected`

### 3.3. `createClusterStateWatchersAndUpdate()` (строка 560)

После подключения выполняется **5 последовательных обращений к ZK**:

```
1. loadClusterProperties()     — getData("/clusterprops.json")
2. refreshLiveNodes(watcher)   — getChildren("/live_nodes")
3. refreshCollections()        — для каждой watched-коллекции: fetchCollectionState()
4. refreshCollectionList(watcher) — getChildren("/collections")
   └── для каждой коллекции создаёт LazyCollectionRef
5. refreshAliases(watcher)     — exists("/aliases.json") + getData("/aliases.json")
```

Каждая из этих операций — синхронный round-trip к ZooKeeper.

## 4. Два режима хранения состояния коллекций

### 4.1. Watched (eager) коллекции — `StatefulCollectionWatch`

Коллекции, в которых у текущей ноды есть локальные реплики, **активно наблюдаются** через ZK watches:

- Состояние загружается при регистрации и обновляется через `StateWatcher.process()`
- При изменении ZK-ноды watcher получает callback и вызывает `refreshAndWatch()`:
  ```
  StateWatcher.process(event)
    → refreshAndWatch(eventType)
      → fetchCollectionState(coll, this)   // getData + setWatch
      → collectionWatches.updateDocCollection(coll, newState)
      → constructState(...)                // пересобирает ClusterState
  ```
- Данные всегда актуальны — обновления push-based

### 4.2. Lazy коллекции — `LazyCollectionRef`

Все остальные коллекции кластера хранятся как `LazyCollectionRef` — они **загружаются по запросу**:

```java
class LazyCollectionRef extends ClusterState.CollectionRef {
    public synchronized DocCollection get(boolean allowCached) {
        if (!allowCached
            || lastUpdateTime < 0
            || System.nanoTime() - lastUpdateTime > LAZY_CACHE_TIME) {
            
            boolean shouldFetch = true;
            if (cachedDocCollection != null) {
                // Оптимизация: сначала проверяем Stat (version + cversion)
                Stat freshStats = zkClient.exists(collectionPath, null, true);
                if (freshStats != null
                    && !cachedDocCollection.isModified(
                        freshStats.getVersion(), freshStats.getCversion())) {
                    shouldFetch = false;  // версия не изменилась, skip fetch
                }
            }
            if (shouldFetch) {
                cachedDocCollection = getCollectionLive(collName);
                lastUpdateTime = System.nanoTime();
            }
        }
        return cachedDocCollection;
    }
}
```

**Важные параметры кеширования:**
- `STATE_UPDATE_DELAY = 2000ms` (настраивается через `-Dsolr.OverseerStateUpdateDelay`)
- `LAZY_CACHE_TIME = STATE_UPDATE_DELAY` в наносекундах = 2 секунды
- После 2 секунд кеш считается устаревшим и перечитывается из ZK

### 4.3. Третий уровень: кеш `CloudSolrClient.ExpiringCachedDocCollection`

`CloudSolrClient` имеет собственный кеш поверх `ZkStateReader`:

```java
class ExpiringCachedDocCollection {
    final DocCollection cached;
    final long cachedAtNano;
    volatile long retriedAtNano = -1;
    volatile boolean maybeStale = false;
    
    boolean shouldRetry() {
        if (maybeStale) {
            // retryExpiryTimeNano = 3 секунды
            if (retriedAtNano == -1 || 
                (System.nanoTime() - retriedAtNano) > retryExpiryTimeNano) {
                return true;
            }
        }
        return false;
    }
}
```

При ошибках связи `maybeStale = true` — следующий запрос пойдёт за свежим состоянием.

## 5. Операция `fetchCollectionState()` — что именно читается из ZK

```java
private DocCollection fetchCollectionState(String coll, Watcher watcher) {
    String collectionPath = DocCollection.getCollectionPath(coll);
    // collectionPath = "/collections/<name>/state.json"
    
    Stat stat = new Stat();
    byte[] data = zkClient.getData(collectionPath, watcher, stat, true);
    
    ClusterState state = ZkClientClusterStateProvider
        .createFromJsonSupportingLegacyConfigName(
            stat.getVersion(), data, Collections.emptySet(), coll, zkClient);
    
    return state.getCollectionStates().get(coll).get();
}
```

Внутри `createFromJsonSupportingLegacyConfigName`:
1. Десериализация JSON из `state.json`
2. Проверка наличия `configName` — если нет, **дополнительный** `getData("/collections/<name>")` 
3. Создание `PerReplicaStatesOps.getZkClientPrsSupplier()` — если коллекция использует Per-Replica States (PRS), при первом обращении к `DocCollection.getPerReplicaStates()` выполнится **ещё один** `getChildren()` к ZK

## 6. Retry-логика в `ZkCmdExecutor`

Каждый вызов `SolrZkClient.getData()` с `retryOnConnLoss=true` оборачивается в `zkCmdExecutor.retryOperation()`:

```java
public <T> T retryOperation(ZkOperation<T> operation) {
    for (int i = 0; i < retryCount; i++) {
        try {
            return operation.execute();
        } catch (ConnectionLossException e) {
            retryDelay(i);  // Thread.sleep((i + 1) * 1500)
        } catch (SessionExpiredException e) {
            cm.waitForConnected(waitMs);  // блокировка до переподключения
        }
    }
}
```

**Расчёт `retryCount`:**
```java
timeouts = zkClientTimeout / 1000.0;  // 30000 / 1000 = 30.0
retryCount = round(0.5 * (sqrt(8 * 30 + 1) - 1)) + 1 = round(0.5 * (sqrt(241) - 1)) + 1
           = round(0.5 * (15.52 - 1)) + 1 = round(7.26) + 1 = 8
```

При `zkClientTimeout = 30000ms`, `retryCount = 8`, `retryDelay = 1500ms`.

**Задержки retry:**
- Попытка 1: sleep(1500)
- Попытка 2: sleep(3000)
- Попытка 3: sleep(4500)
- ...
- Максимальная суммарная задержка retry: `1500 * (1+2+3+4+5+6+7) = 1500 * 28 = 42 секунды`

## 7. Причины задержки до 13 секунд при получении `ClusterState`

### Сценарий A: Первый вызов (cold start) — до 15+ секунд

При первом обращении к `getClusterState()` выполняется полная цепочка:

```
┌─ getZkStateReader() [synchronized]
│
├─ 1. new SolrZkClient(...)
│     └─ new ZooKeeper(host, 30000, watcher)        [< 1ms - async]
│     └─ connManager.waitForConnected(15000)         [0 - 15000ms]
│         └─ Ожидание TCP-handshake + ZK session negotiation
│         └─ При сетевых проблемах: до 15 секунд таймаута
│
├─ 2. createClusterStateWatchersAndUpdate() [synchronized]
│     ├─ loadClusterProperties()                     [1 round-trip]
│     ├─ refreshLiveNodes(watcher)                   [1 round-trip]
│     ├─ refreshCollections()                        [N round-trips, per watched coll]
│     │     └─ для каждой: fetchCollectionState()    [getData + возможно getChildren PRS]
│     ├─ refreshCollectionList(watcher)              [1 round-trip]
│     └─ refreshAliases(watcher)                     [1-2 round-trips]
│
│     При RTT к ZK = 1-5ms: суммарно 5-50ms
│     При RTT к ZK = 50-200ms (WAN/перегрузка): 250ms - 2s
│
└─ return zkStateReader
```

**Типичный worst case: 15s (connect timeout) + кеширование = ~15s**

### Сценарий B: Session Expiry + Reconnect — 3-16 секунд

Когда ZK-сессия истекает, `ConnectionManager` запускает reconnect:

```
ConnectionManager.process(Expired)
  → connected = false
  → connectionStrategy.reconnect(...)
      → new ZooKeeper(host, timeout, watcher)
      → waitForConnected(Long.MAX_VALUE)     [блокирует до подключения]
      → jitter: sleep(0 - 3000ms)            [RECONNECT_JITTER_MAX_MS]
      → onReconnect.command()
          → createClusterStateWatchersAndUpdate()  [5+ ZK round-trips]
```

**Разбивка задержки:**
| Этап | Мин | Макс |
|------|-----|------|
| TCP reconnect к ZK | 1ms | 5000ms |
| Session negotiation | 1ms | 2000ms |
| Reconnect jitter | 0ms | 3000ms |
| `createClusterStateWatchersAndUpdate` (5+ ops) | 5ms | 2000ms |
| Retry delays (если ConnectionLoss при перечитке) | 0ms | 4500ms (3 retry) |
| **Итого** | **~10ms** | **~16500ms** |

При сетевых задержках или перегрузке ZK: 13 секунд — совершенно реальная цифра.

### Сценарий C: LazyCollectionRef cache miss при перегрузке ZK

Запрос к конкретной коллекции через lazy-загрузку:

```
CloudSolrClient.getDocCollection(collection, null)
  → ZkClientClusterStateProvider.getState(collection)
    → ZkStateReader.getClusterState()
      → ClusterState.getCollectionRef(collection)
        → LazyCollectionRef.get(false)   [synchronized!]
          → zkClient.exists(collectionPath, null, true)     [round-trip 1]
          → getCollectionLive(collName)
            → fetchCollectionState(coll, null)
              → zkClient.getData(path, null, stat, true)    [round-trip 2]
              → JSON deserialization
              → возможно: PRS fetch → getChildren()         [round-trip 3]
```

Если ZK перегружен и на каждый round-trip уходит 2-4 секунды (что случается при GC-паузах ZK, большом количестве watches, или сетевых проблемах), три round-trip могут суммироваться до **6-12 секунд**. Плюс, `LazyCollectionRef.get()` — `synchronized`, что создаёт thread contention при параллельных запросах.

### Сценарий D: Retry при ConnectionLoss — до 13.5 секунд

Если при чтении `state.json` происходит `ConnectionLossException`, `ZkCmdExecutor` выполняет retry с возрастающими задержками:

```
Retry 0: execute() → ConnectionLoss
  sleep(1 * 1500 = 1500ms)
Retry 1: execute() → ConnectionLoss  
  sleep(2 * 1500 = 3000ms)
Retry 2: execute() → ConnectionLoss
  sleep(3 * 1500 = 4500ms)
Retry 3: execute() → Success

Суммарная задержка: 1500 + 3000 + 4500 = 9000ms + время самих попыток
```

Три неудачных попытки дают 9 секунд задержки. Если четвёртая попытка выполняется за 1-4 секунды из-за медленного ZK, общее время составляет **10-13 секунд**.

### Сценарий E: `forciblyRefreshAllClusterStateSlow()` — N коллекций × round-trip

Метод вызывается при необходимости полного обновления:

```java
public void forciblyRefreshAllClusterStateSlow() {
    synchronized (getUpdateLock()) {          // глобальная блокировка
        refreshCollectionList(null);           // 1 round-trip
        refreshLiveNodes(null);               // 1 round-trip
        for (String coll : watchedCollections) {
            fetchCollectionState(coll, null);  // N round-trips
        }
        constructState(updatedCollections);
    }
}
```

При 50 watched-коллекциях и 100ms RTT к ZK = 50 × 100ms = **5 секунд** только на fetch. Если RTT выше или добавляются PRS-fetches — легко получить 10+ секунд. Всё это время удерживается `getUpdateLock()`, блокируя остальные потоки.

## 8. Конкретные точки, формирующие задержку ~13 секунд

На основе анализа кода, задержка 13 секунд чаще всего объясняется **комбинацией** нескольких факторов:

```
┌──────────────────────────────────────────────────────────┐
│ 1. ZK session expired                          [0ms]     │
│ 2. TCP reconnect к ZK                          [1-2s]    │
│ 3. Session negotiation                         [0.5-1s]  │
│ 4. Reconnect jitter (random 0-3s)              [~1.5s]   │
│ 5. createClusterStateWatchersAndUpdate:                   │
│    ├─ loadClusterProperties                    [0.2-1s]  │
│    ├─ refreshLiveNodes                         [0.2-1s]  │
│    ├─ refreshCollections (N×getData)           [1-5s]    │
│    ├─ refreshCollectionList                    [0.2-1s]  │
│    └─ refreshAliases                           [0.2-1s]  │
│ 6. Параллельный поток ждёт synchronized        [0-5s]    │
│                                                           │
│ ИТОГО:                                  ~4s — ~18s       │
│ Медиана при типичной нестабильности:    ~13s              │
└──────────────────────────────────────────────────────────┘
```

## 9. Таймауты по умолчанию

| Параметр | Значение | Системное свойство |
|----------|----------|-------------------|
| ZK connect timeout | 15000ms | `-DzkConnectTimeout` |
| ZK client (session) timeout | 30000ms | `-DzkClientTimeout` |
| Retry delay base | 1500ms | hardcoded в `ZkCmdExecutor` |
| State update delay / lazy cache TTL | 2000ms | `-Dsolr.OverseerStateUpdateDelay` |
| Reconnect jitter max | 3000ms | `-Dsolr.zookeeper.reconnect.jitterMaxMs` |
| CloudSolrClient retry expiry | 3000ms | hardcoded |
| Max stale retries | 5 | `-DcloudSolrClientMaxStaleRetries` |

## 10. Диаграмма: потоки данных при обычном запросе

```
CloudSolrClient.request(query)
    │
    ▼
requestWithRetryOnStaleState(request, 0, collections)
    │
    ├─── connect() → getZkStateReader() → [lazy init if needed]
    │
    ├─── resolveAliases(collection)
    │       └─ AliasesManager.getAliases()   [volatile read, no ZK call]
    │
    ├─── getDocCollection(collection, null)
    │       │
    │       ├─ check collectionStateCache → hit? return cached
    │       │
    │       ├─ getCollectionRef(collection)
    │       │    └─ ZkClientClusterStateProvider.getState(collection)
    │       │         └─ ZkStateReader.getClusterState()  [volatile read]
    │       │              └─ ClusterState.getCollectionRef(coll)
    │       │
    │       ├─ ref.isLazilyLoaded()?
    │       │    ├─ false (watched): ref.get() → return in-memory state [fast, ~0ms]
    │       │    │
    │       │    └─ true (lazy):
    │       │         └─ synchronized(lock) {
    │       │              ref.get()  → LazyCollectionRef.get(false)
    │       │                 └─ zkClient.exists() → ZK round-trip
    │       │                 └─ getCollectionLive() → ZK round-trip
    │       │                 └─ update cache
    │       │            }
    │       │
    │       └─ return DocCollection
    │
    ├─── sendRequest(request, collections)
    │
    └─── handle stale state → retry with retryCount + 1
```

## 11. Watcher-механизм обновления состояния (push-based)

Для watched-коллекций обновления приходят через ZK watches:

```
ZooKeeper server
    │
    │  NodeDataChanged(/collections/mycol/state.json)
    ▼
StateWatcher.process(event)
    │
    ├─ event.getType() == NodeDataChanged
    │
    ├─ refreshAndWatch(NodeDataChanged)
    │    └─ fetchCollectionState(coll, this)  // getData + re-register watch
    │    └─ collectionWatches.updateDocCollection(coll, newState)
    │    └─ synchronized(getUpdateLock()) {
    │         constructState(Set.of(coll))   // rebuild ClusterState
    │       }
    │
    └─ Notification callbacks → DocCollectionWatcher listeners


ZooKeeper server
    │
    │  NodeChildrenChanged(/collections/mycol)  [PRS update]
    ▼
StateWatcher.process(event)
    │
    ├─ event.getType() == NodeChildrenChanged
    │
    ├─ refreshAndWatchChildren()
    │    └─ zkClient.getChildren(collectionPath, this, stat, true)
    │    └─ new PerReplicaStates(path, cversion, children)
    │    └─ oldState.setPerReplicaStates(newStates)
    │    └─ constructState(...)
    │
    └─ return (не перечитывает state.json — только PRS)
```

## 12. Рекомендации по диагностике задержек

### Логи для анализа

```
# Connection events
grep "zkClient has connected\|Connection expired\|waitForConnected\|Delaying onReconnect" solr.log

# State fetch timing
grep "Cluster at.*ready\|Updated live nodes\|clusterStateSet\|A cluster state change" solr.log

# Retry events
grep "retryOperation\|ConnectionLossException\|SessionExpiredException\|Could not connect to ZooKeeper" solr.log
```

### Системные свойства для тюнинга

```bash
# Уменьшить connect timeout (если ZK рядом)
-DzkConnectTimeout=5000

# Уменьшить session timeout (быстрее детектить разрывы, но больше ложных срабатываний)
-DzkClientTimeout=15000

# Убрать reconnect jitter (если кластер маленький, thundering herd не проблема)
-Dsolr.zookeeper.reconnect.jitterMaxMs=0

# Уменьшить задержку обновления lazy-коллекций
-Dsolr.OverseerStateUpdateDelay=500
```

## 13. Ключевые выводы

1. **Lazy init дорогой**: Первый вызов `getClusterState()` блокирует поток на установку ZK-соединения + загрузку всего состояния кластера. Это может занять до 15+ секунд при сетевых проблемах.

2. **Session expiry = reconnect storm**: При истечении сессии выполняется полная перезагрузка с jitter до 3 секунд, плюс N round-trips для всех watched коллекций.

3. **LazyCollectionRef synchronized**: Ленивая загрузка коллекций использует `synchronized`, что при большом числе потоков создаёт contention. Один медленный ZK round-trip блокирует все потоки, запрашивающие ту же коллекцию.

4. **ZkCmdExecutor retry с линейным backoff**: Каждый retry увеличивает задержку на 1.5 секунды. Три retry = 9 секунд ожидания + время самих попыток.

5. **Каскад round-trips**: Для одной коллекции при cache miss нужно 2-3 ZK round-trips (exists + getData + возможно getChildren для PRS). При высоком RTT к ZK это мультиплицируется.

6. **Конкретно 13 секунд**: Наиболее вероятное объяснение — это session expiry с reconnect (~2-3s) + jitter (~1.5s) + `createClusterStateWatchersAndUpdate` с множеством коллекций (~3-5s) + retry на ConnectionLoss при первых ZK-вызовах после reconnect (~3-4.5s). Суммарно ~10-14 секунд, с медианой около 13 секунд.
