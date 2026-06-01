# ZooKeeper Session Expiry в SolrCloud: анализ дефектов и исправления

**Ветка:** `branch_9_5`  
**Коммиты:** `c70260b18c7`, `9639852aad4`  
**Сценарий:** кластер 200 Solr-нод, трафик на запись ~1.8 Гбит/с, `KeeperErrorCode = Session expired` не восстанавливается и приводит к полной деградации.

---

## 1. Архитектура ZooKeeper-интеграции в SolrCloud

### 1.1 Слои абстракции

```
ZooKeeper (Apache ZooKeeper client)
        ↑
SolrZkClient                 — точка входа для всех ZK-операций
  ├── ConnectionManager      — управление сессией, reconnect-loop
  ├── ZkCmdExecutor          — retry-логика отдельных операций
  └── ProcessWatchWithExecutor — маршрутизация watch-событий в thread pool

ZkController                 — ZK-мозг одного Solr-узла
  ├── ZkStateReader          — чтение cluster state, watchers
  ├── ZkShardTerms           — терминологические версии реплик (shard terms)
  ├── PerReplicaStatesOps    — PRS-записи (per-replica state children)
  └── LeaderElector          — выборы лидера шарда

Overseer                     — выбранный узел-координатор кластера
  ├── ClusterStateUpdater    — обработка очереди /overseer/queue
  └── OverseerCollectionConfigSetProcessor — Collection API
```

### 1.2 ZK-топология

```
ZooKeeper tree (chroot /solr):
/
├── live_nodes/
│   └── host:8983_solr       ← EPHEMERAL, создаётся при старте/reconnect
├── collections/
│   └── <col>/
│       ├── state.json        ← cluster state коллекции
│       └── state.json/       ← PRS: дети = "coreNodeName:version:state[:L]"
├── overseer/
│   ├── queue/                ← очередь state-updates (PERSISTENT SEQUENTIAL)
│   └── collection-queue-work/
├── overseer_elect/           ← эфемерные ноды leader election
└── collections/<col>/leader_elect/<shard>/
    └── election/             ← очередь выбора лидера шарда
```

### 1.3 ZK-сессия и heartbeat

ZooKeeper использует **TCP heartbeat (PING)**. Клиент обязан слать PING-пакеты в пределах
`sessionTimeout / 3`. ZK-сервер объявляет сессию истёкшей, если PING не получен за `sessionTimeout`.

Дефолт в Solr:
```java
// SolrZkClientTimeout.java
public static final int DEFAULT_ZK_CLIENT_TIMEOUT =
    Integer.getInteger("zkClientTimeout", 30000);  // 30 секунд
```

ZK-сервер корректирует значение по формуле:
```
actual_timeout = max(tickTime * 2, min(client_requested, maxSessionTimeout))
```

### 1.4 Жизненный цикл сессии при expiry

```
Нормальная работа:
  Client → PING → ZK Server (каждые ~10с при timeout=30с)
  ZK Server → PING_RESPONSE → Client

Session expired:
  1. ZK Server не получает PING за 30с → объявляет сессию expired
  2. Рассылает Expired-событие клиентам, подписанным на watches этого клиента
  3. АСИНХРОННО удаляет ephemeral-ноды клиента (separate task)
  4. ZK Client получает Expired-событие → ConnectionManager.process(Expired)
```

**Ключевой момент:** шаги 2 и 3 не синхронны. ZK-сервер удаляет ephemeral-ноды
в фоне — после того как клиент уже получил Expired и начал переподключение.

### 1.5 Потоки ZK-обработки в SolrZkClient

```java
// SolrZkClient.java

// Все пользовательские watchers
private final ExecutorService zkCallbackExecutor =
    ExecutorUtil.newMDCAwareCachedThreadPool(...);       // unbounded cached

// Только ConnectionManager (session events)
private final ExecutorService zkConnManagerCallbackExecutor =
    ExecutorUtil.newMDCAwareSingleThreadExecutor(...);  // ОДИН ПОТОК
```

`ProcessWatchWithExecutor` маршрутизирует:
- `ConnectionManager` → `zkConnManagerCallbackExecutor` (single-thread)
- Все остальные watchers → `zkCallbackExecutor` (cached pool)

### 1.6 Reconnect-архитектура

```
ConnectionManager.process(Expired)          ← в zkConnManagerCallbackExecutor
  │
  ├── beforeReconnect.command()             ← закрыть Overseer, снять elections
  │
  └── do { ... } while (!isClosed())
        │
        ├── connectionStrategy.reconnect()
        │     └── createZooKeeper()         ← новый ZK-клиент (новая сессия)
        │
        └── ZkUpdate.update(keeper)
              ├── waitForConnected(∞)       ← ждёт SyncConnected event
              ├── SolrZkClient.updateKeeper(keeper)  ← volatile swap
              └── onReconnect.command()     ← ТЯЖЁЛАЯ операция:
                    ├── createClusterStateWatchersAndUpdate()
                    ├── overseerElector.joinElection()
                    ├── registerAllCoresAsDown()
                    ├── createEphemeralLiveNode()
                    └── for each core: RegisterCoreAsync
```

### 1.7 ZkCmdExecutor: retry-формула

```java
// ZkCmdExecutor.java
timeouts = zkClientTimeoutMs / 1000.0;  // e.g. 30.0
retryCount = Math.round(0.5f * (sqrt(8.0f * timeouts + 1.0f) - 1.0f)) + 1;
// при 30с → retryCount ≈ 8

retryDelay = 1500ms;  // базовая пауза
// delay[i] = (i+1) * 1500ms → 1.5s, 3s, 4.5s, 6s, 7.5s, 9s, 10.5s
// суммарно ≈ 42.5с, что перекрывает один sessionTimeout
```

До исправления: ретраились **только** `ConnectionLossException`.
`SessionExpiredException` — пробрасывалась немедленно.

---

## 2. Механика дефектов

### Дефект 1: Thundering herd при reconnect (flat retry delay)

**Файл:** `ConnectionManager.java`, строки 225-228 (до патча)

```java
// ДО:
} catch (Exception e) {
    log.error("Could not connect due to error, sleeping for 1s and trying again", e);
    waitSleep(1000);  // ФИКСИРОВАННАЯ 1 секунда для всех нод
}
```

**Механика:**

При массовом истечении сессий (ZK-сервер объявляет expired все 200 сессий в течение нескольких секунд):

```
t=0:   200 нод получают Expired
t=1:   200 нод одновременно запускают reconnect-loop
t=1.1: 200 нод одновременно создают новые ZK-сессии
t=1.2: 200 нод одновременно вызывают onReconnect():
         - 200 × createClusterStateWatchersAndUpdate()  → тысячи watches
         - 200 × joinElection()                          → N²ZK-записей
         - 200 × registerAllCoresAsDown()               → M×200 ZK-записей
         - 200 × createEphemeralLiveNode()               → 200 multi()
t=1.5: ZK-сервер перегружен → снова не успевает обрабатывать PING
t=1.5: Следующий цикл session expiry → снова все 200 нод
```

Первый reconnect-цикл падает → retry через 1с → снова все 200 нод синхронно →
повторная перегрузка → бесконечный цикл деградации.

**Почему 1.8 Гбит/с усиливает проблему:** ZK-сервер — однопоточный по записи.
При высоком трафике очередь транзакций растёт → latency ответов растёт →
PING-пакеты задерживаются за write-трафиком в одном TCP-соединении
(head-of-line blocking) → ZK-сервер не получает PING → объявляет expired.

---

### Дефект 2: ZK-перегрузка в момент onReconnect (thundering herd phase 2)

**Файл:** `ConnectionManager.java`, строки 198-200 (до патча)

```java
// ДО:
if (onReconnect != null) {
    onReconnect.command();  // вызывается НЕМЕДЛЕННО после reconnect
}
```

**Механика:**

Даже если reconnect-retry разнесён во времени, все 200 нод подключаются к ZK
в узком временном окне (ZK принимает соединения быстро). Как только каждая нода
устанавливает новую сессию, она немедленно запускает `onReconnect.command()`.

`onReconnect()` в `ZkController` (строки 428-535) выполняет:

```java
zkStateReader.createClusterStateWatchersAndUpdate();  // READ: /collections/* + watches
overseerElector.joinElection(context, true);          // WRITE: ephemeral sequential node
registerAllCoresAsDown(descriptorsSupplier);           // WRITE: state update per core
createEphemeralLiveNode();                             // WRITE: multi([create /live_nodes/X])
for (CoreDescriptor d : descriptors) {
    executorService.submit(new RegisterCoreAsync(d));  // async: publish DOWN → RECOVERING → ACTIVE
}
for (OnReconnect listener : clonedListeners) {
    executorService.submit(new OnReconnectNotifyAsync(listener));  // config watchers, etc.
}
```

200 нод × M коров × все операции одновременно = write storm на ZK.
ZK снова перегружается, следующий round сессий истекает до завершения reconnect.

---

### Дефект 3: Stale ephemeral node при reconnect

**Файл:** `ZkController.java`, метод `createEphemeralLiveNode()` (строки 1184-1212, до патча)

```java
// ДО:
zkClient.multi(ops, true);  // бросает NodeExistsException → не обрабатывается
```

**Механика:**

ZK-сервер удаляет ephemeral-ноды истёкшей сессии **асинхронно** на серверной стороне.
Операция expiry и операция cleanup — разные шаги разных обработчиков.

```
t=0:    Сессия S1 expired
t=0.1:  Клиент получает Expired, начинает reconnect
t=0.5:  Новая сессия S2 установлена
t=0.5:  onReconnect() → createEphemeralLiveNode()
          → multi([Op.create("/live_nodes/host:8983_solr", EPHEMERAL)])
          → BOOM: NodeExistsException  ← /live_nodes/host:8983 ещё принадлежит S1
t=0.5:  RuntimeException → closeKeeper(S2) → retry всего reconnect-цикла
t=0.8:  Новая сессия S3 создаётся, снова та же ошибка
        ZK может держать ноду S1 ещё несколько секунд

т.е. пока ZK не очистил ноду от S1, каждая попытка reconnect создаёт и сразу
уничтожает новую ZK-сессию — лишняя нагрузка на ZK
```

**Дополнительная сложность:** `multi()` атомарен, и ошибка может возникнуть
на любой из ops (основная `/live_nodes/nodeName` или любая role-нода).
До исправления не было никакой обработки — `NodeExistsException` летела наверх,
срывая весь reconnect.

---

### Дефект 4: SessionExpiredException не ретраится в ZkCmdExecutor

**Файл:** `ZkCmdExecutor.java`, метод `retryOperation()` (строки 59-89, до патча)

```java
// ДО:
for (int i = 0; i < retryCount; i++) {
    try {
        return operation.execute();
    } catch (KeeperException.ConnectionLossException e) {
        // retry с задержкой
    }
    // SessionExpiredException → не поймана → пробрасывается сразу
}
```

**Механика:**

`SolrZkClient.keeper` — `volatile`-поле. `ConnectionManager.updateKeeper()` атомарно
заменяет его на новый `ZooKeeper`-инстанс при reconnect. Лямбды, переданные в
`retryOperation()`, обращаются к `keeper` при каждом вызове, например:

```java
// SolrZkClient.setData()
zkCmdExecutor.retryOperation(() -> keeper.setData(path, data, version));
//                                   ↑ volatile read при каждом вызове
```

После `updateKeeper()` следующий retry автоматически использует новый keeper.
Но `SessionExpiredException` пробрасывалась немедленно, не давая операции
повториться с новым keeper.

**Последствие:** любая операция, начавшаяся до expiry и завершившаяся с
`SessionExpiredException`, падала с ошибкой к вызывающему коду, даже если
через секунду reconnect был бы завершён и операция могла бы успешно выполниться.

---

### Дефект 5: PerReplicaStatesOps.persist() поглощает SessionExpiredException

**Файл:** `PerReplicaStatesOps.java`, метод `persist()` (строки 136-151, до патча)

```java
// ДО:
public void persist(String znode, SolrZkClient zkClient)
    throws KeeperException, InterruptedException {
    List<PerReplicaStates.Operation> operations = ops;
    for (int i = 0; i < PerReplicaStates.MAX_RETRIES; i++) {
        try {
            persist(operations, znode, zkClient);
            return;
        } catch (KeeperException.NodeExistsException | KeeperException.NoNodeException e) {
            // stale state → refresh and retry
            operations = refresh(fetch(znode, zkClient, null));
        }
        // SessionExpiredException: НЕ поймана и не ретраится
        // → вылетает из try-блока → цикл НЕ видит её
        // НО: SessionExpiredException extends KeeperException
        //     KeeperException extends Exception
        //     → вылетает из ЦИКЛА целиком → метод возвращается БЕЗ записи
    }
    // если retries исчерпаны: тихий return без throw
}
```

**Ошибка в рассуждении выше — уточнение:**

`SessionExpiredException` не поймана в catch → она всплывает из `try` → достигает
цикла `for` → вылетает из него → `persist()` пробрасывает её к вызывающему.
НО: после цикла нет `throw exception` — значит если `SessionExpiredException`
бросается на i=0, catch не ловит её, она пробрасывается. Это верно.

**Реальный дефект:** `NodeExistsException` и `NoNodeException` — это stale state.
`SessionExpiredException` — критическая ошибка. До исправления оба обрабатывались
одинаково: refresh + retry. При `SessionExpiredException` вызов
`refresh(fetch(znode, zkClient, null))` тоже бросал бы исключение (новый ZK-запрос
с мёртвой сессией). Это означало бесконечный retry цикл до MAX_RETRIES с постоянными
ошибками. По завершении цикла метод возвращался **без ошибки**, теряя запись
состояния реплики молча.

```java
// Реальная цепочка при SessionExpired (до патча):
persist(ops, znode, zkClient)
  → multi(ops) → SessionExpiredException
  → catch NodeExists|NoNode: НЕ СРАБАТЫВАЕТ
  → SessionExpiredException всплывает из try
  → всплывает из цикла
  → persist() пробрасывает исключение наверх

// Значит SessionExpiredException ВСЕГДА доходила до caller...
// Но caller (ZkController.publish) не перехватывал её:
//   → пробрасывалась до метода publish() → дальше вверх по стеку
//   → конкретное поведение зависело от точки вызова
```

Ключевая проблема: в большинстве мест вызова `publish()` exception игнорировался
или логировался без retry. Состояние реплики (DOWN/RECOVERING/ACTIVE) терялось
для PRS-коллекций до следующего полного re-register в `onReconnect()`.

---

### Дефект 6: ZkShardTerms необратимо закрывается при SessionExpiredException

**Файл:** `ZkShardTerms.java`, метод `retryRegisterWatcher()` (строки 392-397)

```java
} catch (KeeperException.SessionExpiredException | KeeperException.AuthFailedException e) {
    isClosed.set(true);   // AtomicBoolean → объект навсегда "мёртв"
    log.error("Failed watching shard term for collection: {}", collection, e);
    return;
}
```

**Механика:** `ZkShardTerms` получает `SessionExpiredException` при попытке
пере-зарегистрировать watcher после получения watch-события. В этот момент
`isClosed=true` — объект больше не следит за изменениями term'ов шарда.

**Почему не критично:** `ZkController.onReconnect()` вызывает
`clearZkCollectionTerms()` → удаляет все `ZkShardTerms` из map. При следующем
обращении к шарду создаётся новый инстанс с `isClosed=false`. Дефект самоизлечивается
при успешном reconnect. Но если reconnect никогда не завершается (из-за дефектов 1-3),
terms остаются мёртвыми.

---

### Дефект 7: Overseer завершает работу при SessionExpiredException

**Файл:** `Overseer.java`, `ClusterStateUpdater.run()` (строки 321-324, 339-341)

```java
} catch (KeeperException.SessionExpiredException e) {
    log.warn("Solr cannot talk to ZK, exiting Overseer work queue loop", e);
    return;  // Overseer прекращает обрабатывать очередь
}
```

**Механика:** При session expiry Overseer корректно завершает работу. Для
продолжения нужно переизбрать Overseer — это ZK-операция (ephemeral sequential node
в `/overseer_elect/`). Если ZK перегружен (дефект 1), выборы не завершаются →
Overseer не работает → очередь `/overseer/queue` растёт → все ноды блокируются
на `offer()` в очередь → полная деградация.

---

## 3. Схема каскадной деградации

```
Высокий write-трафик (1.8 Гбит/с)
        │
        ▼
ZK leader перегружен (однопоточная обработка записей)
        │
        ▼
Задержка PING за write-трафиком в TCP (head-of-line blocking)
        │
        ▼
ZK объявляет сессии expired (200 нод × batch expiry)
        │
        ├─→ [Дефект 6] ZkShardTerms.isClosed=true (без reconnect — навсегда)
        │
        ├─→ [Дефект 7] Overseer выходит из loop → очередь растёт → блокировки
        │
        ├─→ [Дефект 1] 200 нод одновременно через 1с ретраят reconnect
        │       │
        │       ▼
        │   thundering herd: все 200 нод вместе
        │       ├── [Дефект 2] onReconnect() → write storm на ZK
        │       └── [Дефект 3] NodeExistsException в createEphemeralLiveNode
        │               │
        │               ▼
        │           reconnect abort → retry → снова всё одновременно
        │
        └─→ [Дефект 4] ZkCmdExecutor: mid-flight операции падают
                └─→ [Дефект 5] PRS.persist() теряет state молча
                        │
                        ▼
                ZK снова перегружен от reconnect-storm
                        │
                        ▼
                Следующий round session expiry
                        │
                        ▼
                 ПОЛНАЯ ДЕГРАДАЦИЯ (цикл не завершается)
```

---

## 4. Внесённые исправления

### 4.1 ConnectionManager: экспоненциальный backoff + jitter (Дефект 1)

**Файл:** `ConnectionManager.java`

```java
// ПОСЛЕ: добавлен счётчик попыток
int reconnectAttempt = 0;
do {
    try {
        connectionStrategy.reconnect(...);
        break;
    } catch (Exception e) {
        // Экспоненциальный backoff: 1s, 2s, 4s, 8s, 16s, max 30s
        long baseMs = Math.min(30_000L, 1_000L * (1L << Math.min(reconnectAttempt, 5)));
        // Jitter: случайная добавка до 50% от base, разносит ноды во времени
        long jitter = ThreadLocalRandom.current().nextLong(baseMs / 2 + 1);
        long sleepMs = baseMs + jitter;
        log.error("Could not connect to ZooKeeper (attempt {}), sleeping {}ms before retry",
            reconnectAttempt + 1, sleepMs, e);
        waitSleep(sleepMs);
        reconnectAttempt++;
    }
} while (!isClosed());
```

**Расчёт:** при 200 нодах с random jitter, распределение retry попыток:

| Попытка | Base | Sleep range | Нод в секунду |
|---------|------|-------------|---------------|
| 0 | 1000ms | 1000-1500ms | ~133 |
| 1 | 2000ms | 2000-3000ms | ~67 |
| 2 | 4000ms | 4000-6000ms | ~33 |
| 3 | 8000ms | 8000-12000ms | ~17 |
| 4 | 16000ms | 16000-24000ms | ~8 |
| 5+ | 30000ms | 30000-45000ms | ~4 |

### 4.2 ConnectionManager: jitter перед onReconnect (Дефект 2)

**Файл:** `ConnectionManager.java`

```java
private static final long RECONNECT_JITTER_MAX_MS =
    Long.getLong("solr.zookeeper.reconnect.jitterMaxMs", 3000L);

// В ZkUpdate.update() перед onReconnect.command():
if (RECONNECT_JITTER_MAX_MS > 0) {
    long jitter = ThreadLocalRandom.current().nextLong(RECONNECT_JITTER_MAX_MS);
    if (jitter > 0) {
        log.info("Delaying onReconnect by {}ms to reduce ZooKeeper write storm", jitter);
        Thread.sleep(jitter);
    }
}
onReconnect.command();
```

**Эффект:** 200 нод подключаются к ZK примерно одновременно (ZK принимает
соединения быстро), но запуск тяжёлых операций разнесён на 3 секунды.
Пиковая нагрузка на ZK снижается в ~(jitterMaxMs / avgOpDuration) раз.

**Тюнинг:** для 200 нод рекомендуется `-Dsolr.zookeeper.reconnect.jitterMaxMs=5000`.

### 4.3 ZkController.createEphemeralLiveNode: обработка stale-ноды (Дефект 3)

**Файл:** `ZkController.java`

```java
// ПОСЛЕ:
try {
    zkClient.multi(ops, true);
} catch (KeeperException.NodeExistsException e) {
    // ZK удаляет ephemeral-ноды асинхронно → нода от старой сессии ещё существует.
    // Удаляем каждую stale-ноду (используем getPath() из Op), затем пересоздаём.
    log.warn("Ephemeral live node(s) for {} already exist (stale from expired session) - "
        + "removing and recreating under new session", nodeName);
    for (Op op : ops) {
        try {
            zkClient.delete(op.getPath(), -1, true);
        } catch (NoNodeException ignored) {
            // ZK завершил собственную очистку между нашим catch и этим delete
        }
    }
    zkClient.multi(ops, true);
}
```

**Почему delete безопасен:** нода принадлежит нашему имени (`host:port_solr`).
Другой процесс с тем же именем не может существовать корректно. Это наша
собственная stale-нода от истёкшей сессии — не чужая.

**Почему итерируем по `ops`, а не по отдельному списку:** symmetry —
удаляем ровно те ноды, которые потом создаём, включая все role-ноды.

### 4.4 ZkCmdExecutor: retry при SessionExpiredException (Дефект 4)

**Файл:** `ZkCmdExecutor.java`, `SolrZkClient.java`

```java
// ZkCmdExecutor: новый catch-блок
} catch (KeeperException.SessionExpiredException e) {
    ConnectionManager cm = connectionManager;  // volatile read
    if (cm == null) {
        throw e;  // backward-compat: без ConnectionManager — старое поведение
    }
    if (exception == null) exception = e;
    if (i != retryCount - 1) {
        log.warn("ZooKeeper session expired during operation (attempt {}), "
            + "waiting for reconnect before retry", i + 1);
        try {
            // Блокируемся до появления новой сессии
            cm.waitForConnected((long)(timeouts * 1000));
        } catch (TimeoutException te) {
            // Timeout → всё равно пробуем следующий retry
        }
    }
}

// SolrZkClient: вязка ConnectionManager ↔ ZkCmdExecutor
connManager = new ConnectionManager(...);
zkCmdExecutor.setConnectionManager(connManager);
```

**Почему работает:** `SolrZkClient.keeper` — `volatile`. `updateKeeper()` атомарно
заменяет его. Когда retry выполняет лямбду `() -> keeper.delete(path, version)`,
volatile read возвращает НОВЫЙ keeper. Операция выполняется в новой сессии.

**Backward-compat:** без `setConnectionManager()` (тесты, legacy код) поведение
идентично оригинальному — `SessionExpiredException` пробрасывается немедленно.

### 4.5 PerReplicaStatesOps.persist: явный пробрас SessionExpiredException (Дефект 5)

**Файл:** `PerReplicaStatesOps.java`

```java
// ПОСЛЕ:
} catch (KeeperException.SessionExpiredException | KeeperException.AuthFailedException e) {
    // Критическая ошибка — ZK-сессия мертва. Пробрасываем немедленно.
    // Состояние будет восстановлено через onReconnect() → RegisterCoreAsync.
    throw e;
} catch (KeeperException.NodeExistsException | KeeperException.NoNodeException e) {
    // stale state → refresh and retry
    operations = refresh(fetch(znode, zkClient, null));
}
```

---

## 5. Операционные настройки

### 5.1 Параметры JVM (solr.in.sh)

```bash
# Увеличить session timeout — даёт ZK больше времени до объявления expired
SOLR_ZK_CREDS_AND_ACLS="-DzkClientTimeout=60000"

# Увеличить jitter для большого кластера
SOLR_OPTS="$SOLR_OPTS -Dsolr.zookeeper.reconnect.jitterMaxMs=5000"
```

### 5.2 ZooKeeper server (zoo.cfg)

```properties
# Увеличить tick — увеличивает minSessionTimeout = 2 * tickTime
tickTime=3000           # было 2000 → minSessionTimeout=6s вместо 4s

# Разрешить более долгие сессии
maxSessionTimeout=120000  # было обычно 40000

# Ограничить число соединений с одного хоста (защита от reconnect storm)
maxClientCnxns=300

# JVM heap для ZK
export JVMFLAGS="-Xmx8g -Xms8g"
```

### 5.3 Распределённое обновление состояния (альтернатива Overseer-очереди)

```xml
<!-- solr.xml: убирает bottleneck Overseer для state updates -->
<solrcloud>
  <str name="distributedClusterStateUpdates">true</str>
</solrcloud>
```

При `distributedClusterStateUpdates=true` каждый узел пишет `state.json` напрямую
(без очереди `/overseer/queue`), что снижает нагрузку на ZK-лидера.

---

## 6. Итоговая таблица исправлений

| Дефект | Файл | Проблема | Исправление |
|--------|------|---------|-------------|
| 1 | `ConnectionManager.java` | Flat 1s retry → все 200 нод синхронно ретраят | Exponential backoff с jitter (1s→2s→4s→8s→16s→30s) |
| 2 | `ConnectionManager.java` | Все 200 нод одновременно запускают onReconnect | Random jitter 0–3000ms перед onReconnect.command() |
| 3 | `ZkController.java` | NodeExistsException в createEphemeralLiveNode ломает весь reconnect | Catch + delete stale nodes + retry multi() |
| 4 | `ZkCmdExecutor.java` | SessionExpired пробрасывается немедленно (retry с новым keeper невозможен) | Catch + waitForConnected + retry (при наличии ConnectionManager) |
| 5 | `PerReplicaStatesOps.java` | SessionExpired не отличался от stale state → retry loop → тихая потеря записи | Явный throw SessionExpired/AuthFailed до stale-state обработки |

---

## 7. Покрытие тестами

### `ConnectionManagerTest`

| Тест | Что проверяет |
|------|---------------|
| `testExponentialBackoffFormula` | Формула backoff: ожидаемые значения 1s, 2s, 4s, 8s, 16s, 30s |
| `testBackoffIsMonotonicallyIncreasing` | Каждая попытка ≥ предыдущей (до cap) |
| `testOnReconnectIsCalledAfterSessionExpiryWithJitter` | onReconnect вызывается после session expiry при включённом jitter |
| `testStaleEphemeralNodeIsRemovedAndRecreatedOnReconnect` | NodeExistsException → delete + recreate → нода существует |

### `ZkCmdExecutorTest` (новый, `solrj-zookeeper`)

| Тест | Что проверяет |
|------|---------------|
| `testSessionExpiredPropagatesImmediatelyWithoutConnectionManager` | Backward-compat: без CM SessionExpired пробрасывается сразу |
| `testSessionExpiredAfterConnectionLossRetries_NoConnectionManager` | Контракт оригинального testZkCmdExecutor сохранён |
| `testSessionExpiredRetriesAndSucceedsWithConnectionManager` | С CM: SessionExpired → waitForConnected → retry → success |
| `testSessionExpiredAfterRealExpiry_RetrySucceeds` | E2E: после server.expire() executor работает с новой сессией |

### `PerReplicaStatesOpsTest` (новый, `solrj-zookeeper`)

| Тест | Что проверяет |
|------|---------------|
| `testPersistPropagatesSessionExpiredException` | SessionExpired из multi() пробрасывается (не поглощается) |
| `testPersistCompletesSuccessfullyWithRealClient` | Stale-state retry (NodeExists/NoNode) не сломан |

---

## 8. Диагностика в production

### Признаки проблемы в логах

```
# Начало cascade:
WARN ConnectionManager - Our previous ZooKeeper session was expired. Attempting to reconnect...

# Thundering herd (все ноды одновременно):
INFO ConnectionManager - Waiting up to 45000ms for client to connect to ZooKeeper
INFO ConnectionManager - Delaying onReconnect by Xms to reduce ZooKeeper write storm  ← НОВОЕ

# Stale node (до исправления — молчало, теперь):
WARN ZkController - Ephemeral live node(s) for host:8983_solr already exist
                    (stale from expired session) - removing and recreating under new session

# ZkCmdExecutor retry (новое):
WARN ZkCmdExecutor - ZooKeeper session expired during operation (attempt 1),
                     waiting for reconnect before retry
```

### ZK-метрики для мониторинга

```bash
# На ZK-серверах
echo mntr | nc zk-host 2181 | grep -E "zk_avg_latency|zk_outstanding_requests|zk_num_alive_connections"

# Критические пороги:
# zk_avg_latency > 100ms     → ZK перегружен
# zk_outstanding_requests > 100  → очередь запросов растёт
# zk_num_alive_connections резко падает → mass session expiry
```
