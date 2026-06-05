# Архитектура ZooKeeper-взаимодействия в Apache Solr

## Оглавление

1. [Обзор: зачем Solr нужен ZooKeeper](#1-обзор-зачем-solr-нужен-zookeeper)
2. [Иерархия ZK-узлов: полная карта путей](#2-иерархия-zk-узлов-полная-карта-путей)
3. [SolrZkClient: обёртка над ZooKeeper](#3-solrzklient-обёртка-над-zookeeper)
4. [ZkController: центральный координатор ноды](#4-zkcontroller-центральный-координатор-ноды)
5. [ZkStateReader: локальный кэш кластерного состояния](#5-zkstatereader-локальный-кэш-кластерного-состояния)
6. [LeaderElector: алгоритм выборов лидера](#6-leaderelector-алгоритм-выборов-лидера)
7. [Overseer: единый координатор кластера](#7-overseer-единый-координатор-кластера)
8. [Очереди Overseer: три ZK-очереди](#8-очереди-overseer-три-zk-очереди)
9. [ClusterStateUpdater: запись состояния в ZK](#9-clusterstateupdater-запись-состояния-в-zk)
10. [ZkStateWriter: батчинг и оптимистичный локинг](#10-zkstatewriter-батчинг-и-оптимистичный-локинг)
11. [Регистрация core: register()](#11-регистрация-core-register)
12. [Публикация состояния реплики: publish()](#12-публикация-состояния-реплики-publish)
13. [Обработка обрыва ZK-сессии](#13-обработка-обрыва-zk-сессии)
14. [Live Nodes: эфемерные узлы живых нод](#14-live-nodes-эфемерные-узлы-живых-нод)
15. [Per-Replica State (PRS): масштабирование state.json](#15-per-replica-state-prs-масштабирование-statejson)
16. [Distributed Cluster State Updates](#16-distributed-cluster-state-updates)
17. [ConfigSets в ZooKeeper](#17-configsets-в-zookeeper)
18. [Создание коллекции: сквозной пример](#18-создание-коллекции-сквозной-пример)
19. [Ключевые классы и файлы](#19-ключевые-классы-и-файлы)
20. [Справочная таблица: параметры и константы](#20-справочная-таблица-параметры-и-константы)

---

## 1. Обзор: зачем Solr нужен ZooKeeper

SolrCloud использует ZooKeeper как:

| Роль | Описание |
|---|---|
| **Service discovery** | Каждая нода регистрирует эфемерный узел в `/live_nodes`; при падении ноды ZK удаляет его автоматически |
| **Distributed lock / leader election** | ZK-рецепт выборов через ephemeral sequential узлы — для лидера каждого шарда и для Overseer |
| **Shared configuration storage** | Схемы и конфиги (`solrconfig.xml`, `schema.xml`) хранятся в `/configs/` и доступны всем нодам |
| **Cluster state registry** | Описание всех коллекций, шардов, реплик, их состояний — в `/collections/*/state.json` |
| **Message queue** | Три очереди в `/overseer/` для передачи команд Overseer-у |
| **Coordination barrier** | `waitForState()` позволяет нодам синхронизироваться на изменениях состояния |

ZooKeeper **не** хранит индексные данные и не участвует в обработке поисковых запросов. Его нагрузка — исключительно метаданные кластера.

---

## 2. Иерархия ZK-узлов: полная карта путей

```
/
├── /live_nodes/                          PERSISTENT контейнер
│   └── hostname:port_solr               EPHEMERAL — живая нода
│                                         Пример: "node1:8983_solr"
│
├── /collections/                         PERSISTENT контейнер
│   └── {collectionName}/
│       ├── state.json                    PERSISTENT — состояние коллекции (JSON)
│       │   {
│       │     "shards": {
│       │       "shard1": {
│       │         "range": "0-2147483647",
│       │         "state": "active",
│       │         "replicas": {
│       │           "core_node1": {
│       │             "core": "coll_shard1_replica_n1",
│       │             "base_url": "http://node1:8983/solr",
│       │             "node_name": "node1:8983_solr",
│       │             "state": "active",
│       │             "leader": "true",
│       │             "type": "NRT"
│       │           }
│       │         }
│       │       }
│       │     },
│       │     "configName": "mycfg",
│       │     "router": {"name": "compositeId"},
│       │     "replicationFactor": 2,
│       │     "nrtReplicas": 1
│       │   }
│       ├── collectionprops.json          PERSISTENT — дополнительные свойства
│       ├── leaders/
│       │   └── {shardId}/
│       │       └── leader               EPHEMERAL — кто сейчас лидер шарда
│       └── leader_elect/
│           └── {shardId}/
│               └── election/            PERSISTENT контейнер
│                   ├── {sesId}-{coreNode}-n_0000000001   EPHEMERAL_SEQUENTIAL
│                   ├── {sesId}-{coreNode}-n_0000000002   EPHEMERAL_SEQUENTIAL
│                   └── ...
│
│       ── (если включён PRS):
│       └── {replicaName}                PERSISTENT или EPHEMERAL — per-replica state
│
├── /overseer_elect/                      PERSISTENT
│   ├── election/                         PERSISTENT
│   │   └── {sesId}-{nodeName}-n_XXXX    EPHEMERAL_SEQUENTIAL — кандидаты
│   └── leader                           EPHEMERAL — текущий Overseer
│       {"id": "{sesId}-{nodeName}-n_XXXX"}
│
├── /overseer/                            PERSISTENT
│   ├── queue/                            ZK-очередь состояний
│   │   └── qn-XXXXXXXXXX               PERSISTENT_SEQUENTIAL — сообщения
│   ├── queue-work/                       Внутренняя резервная очередь
│   ├── collection-queue-work/            Очередь Collection API
│   │   ├── qn-XXXXXXXXXX               PERSISTENT_SEQUENTIAL — запросы
│   │   └── qnr-XXXXXXXXXX              EPHEMERAL_SEQUENTIAL  — ответы
│   ├── collection-map-running/           Текущие async-задачи
│   ├── collection-map-completed/         Завершённые задачи (max 10K)
│   ├── collection-map-failure/           Упавшие задачи (max 10K)
│   └── async_ids/                        Tracking async request IDs
│
├── /configs/                             PERSISTENT
│   └── {configName}/
│       ├── solrconfig.xml               PERSISTENT
│       ├── managed-schema               PERSISTENT
│       └── ...
│
├── /aliases.json                         PERSISTENT — алиасы коллекций
├── /clusterprops.json                    PERSISTENT — глобальные настройки кластера
├── /security.json                        PERSISTENT — настройки безопасности
├── /packages.json                        PERSISTENT — установленные плагины
├── /node_roles/                          PERSISTENT — роли нод
│   └── overseer/default                  список кандидатов для роли overseer
└── /roles.json                           PERSISTENT — устаревшее (legacy)
```

---

## 3. SolrZkClient: обёртка над ZooKeeper

**Файл:** `solr/solrj-zookeeper/src/java/org/apache/solr/common/cloud/SolrZkClient.java`

`SolrZkClient` — тонкая обёртка над Apache ZooKeeper клиентом. Добавляет:

### Retry-логика

Все ZK-операции оборачиваются в `ZkCmdExecutor`, который повторяет при `ConnectionLossException` и `SessionMovedException` вплоть до истечения таймаута:

```java
// Псевдокод retry-wrapper
public byte[] getData(String path, Watcher watcher, ...) {
    return zkCmdExecutor.retryOperation(() ->
        zookeeper.getData(path, wrapWatcher(watcher), stat));
}
```

### Асинхронные watcher'ы

Callback ZK-watcher'ов выполняется в ZK event thread — блокировать его нельзя. Поэтому `SolrZkClient` оборачивает каждый watcher в `ProcessWatchWithExecutor`, который отправляет `watcher.process(event)` в отдельный executor:

```java
private Watcher wrapWatcher(final Watcher watcher) {
    if (watcher == null) return null;
    return event -> executor.execute(() -> watcher.process(event));
}
```

### makePath() — рекурсивное создание пути

```java
public String makePath(String path, byte[] data, CreateMode createMode, ...) {
    // Создать все промежуточные узлы как PERSISTENT
    // Последний узел создать с указанным createMode
    // skipPathParts: сколько первых сегментов не создавать (валидация существования)
    // failOnExists: false → обновить если уже существует
}
```

### multi() — атомарные операции

```java
public List<OpResult> multi(Iterable<Op> ops) {
    // ZK multi — all-or-nothing транзакция
    // Используется для атомарного создания ephemeral live_node
    // и связанных role-узлов
}
```

### Сжатие state.json

При чтении/записи больших ZK-узлов (> `minStateByteLenForCompression`) данные сжимаются через `Compressor` (по умолчанию ZLib). Маркер сжатия хранится в первых байтах узла.

### ACL-провайдер

```java
public interface ZkACLProvider {
    List<ACL> getACLsToAdd(String zNodePath);
}
```

Определяет права доступа для каждого пути. Реализации: `DefaultZkACLProvider` (открытый доступ), `VMParamsAllAndReadonlyDigestZkACLProvider` (digest-аутентификация).

### Метрики

`ZkMetrics` отслеживает: `watchesFired`, `reads`, `writes`, `bytesRead`, `bytesWritten`, `multiOps`, `childFetches`, `existsChecks`, `deletes`. Доступны через JMX/Prometheus.

---

## 4. ZkController: центральный координатор ноды

**Файл:** `solr/core/src/java/org/apache/solr/cloud/ZkController.java` (3054 строки)

`ZkController` создаётся в `CoreContainer` при старте ноды в режиме SolrCloud. Это центральный объект, связывающий локальную ноду с кластером через ZK.

### Конструктор и инициализация

```java
public ZkController(CoreContainer cc, String zkServerAddress,
                    int zkClientConnectTimeout, CloudConfig cloudConfig, ...) {

    // 1. Вычислить nodeName из hostname + port + context
    this.nodeName = generateNodeName(hostName, port, context);
    // Пример: "solr-node1:8983_solr"

    // 2. Настроить стратегию соединения (DefaultConnectionStrategy)
    ZkClientConnectionStrategy strat = ZkClientConnectionStrategy.forName(...);

    // 3. Создать SolrZkClient с reconnect-listeners
    zkClient = new SolrZkClient.Builder()
        .withUrl(zkServerAddress)
        .withTimeout(clientTimeout, MILLISECONDS)
        .withReconnectListener(() -> onReconnect(descriptorsSupplier))
        .withBeforeConnect(() -> beforeReconnect(descriptorsSupplier))
        .withAclProvider(zkACLProvider)
        .withCompressor(compressor)
        .build();

    // 4. Создать ZkStateReader (кэш кластерного состояния)
    zkStateReader = new ZkStateReader(zkClient, () -> cc.securityNodeChanged());

    // 5. Вызвать init()
    init();
}
```

### init(): основной поток инициализации

```java
public void init() {
    // 1. Создать базовые ZK-узлы кластера (если не существуют):
    createClusterZkNodes(zkClient);
    //   /collections, /live_nodes, /node_roles,
    //   /security.json, /clusterprops.json, /aliases.json

    // 2. Поднять все watchers и загрузить начальное состояние кластера
    zkStateReader.createClusterStateWatchersAndUpdate();

    // 3. Прочитать cluster properties (URL scheme и т.д.)
    readUrlScheme();

    // 4. Запустить выборы Overseer (если не zkRunOnly)
    if (!zkRunOnly) {
        overseerElector.setup(new OverseerElectionContext(zkClient, overseer, nodeName));
        overseerElector.joinElection(context, false);
    }

    // 5. Создать эфемерный узел живой ноды
    createEphemeralLiveNode();
    // Создаёт /live_nodes/{nodeName} (EPHEMERAL)
    // + роли если назначены через node_roles

    // 6. Начальная публикация нод как DOWN
    // (каждый core сам перейдёт в ACTIVE после регистрации)
}
```

### Имя ноды

```
nodeName = hostname + ":" + port + "_" + context
Пример: "solr-node1:8983_solr"
         "192.168.1.10:8983_solr"
```

Это уникальный идентификатор ноды в кластере. Используется как имя ephemeral узла в `/live_nodes/`.

### getLeaderRetry()

```java
public Replica getLeaderRetry(String collection, String shard)
        throws InterruptedException {
    // Опрашивать ZkStateReader до 4 секунд (zkReaderGetLeaderRetryTimeoutMs)
    // пока лидер не появится в cluster state и не войдёт в live_nodes
    // Выбросить SERVICE_UNAVAILABLE если таймаут
}
```

### preClose() и close()

```java
public void preClose() {
    // 1. Удалить /live_nodes/{nodeName} — нода объявляет себя мёртвой
    zkClient.delete(LIVE_NODES_ZKNODE + "/" + nodeName, -1, true);
    // 2. Опубликовать все реплики как DOWN
    publishNodeAsDown(nodeName);
    // 3. Остановить фоновую репликацию TLOG-реплик
    stopReplicationFromLeader(coreName);
}

public void close() {
    // Закрыть Overseer, election contexts, ZkStateReader, SolrZkClient
}
```

---

## 5. ZkStateReader: локальный кэш кластерного состояния

**Файл:** `solr/solrj-zookeeper/src/java/org/apache/solr/common/cloud/ZkStateReader.java`

`ZkStateReader` — читающая сторона: поддерживает актуальный in-memory кэш кластерного состояния с помощью ZK-watchers. Доступен как на серверах (через ZkController), так и в клиентах (SolrJ).

### Ключевые поля

```java
private volatile ClusterState clusterState;           // всё состояние кластера
private volatile SortedSet<String> liveNodes;         // живые ноды (из /live_nodes)

// Коллекции с активными watchers (watched collections)
private final ConcurrentHashMap<String, StatefulCollectionWatch> collectionWatches;

// Коллекции без watchers — lazy-load по запросу
private final ConcurrentHashMap<String, LazyCollectionRef> lazyCollectionStates;

// Кэш свойств коллекций
private final ConcurrentHashMap<String, VersionedCollectionProps> watchedCollectionProps;
```

### Три уровня watchers

**1. StateWatcher** — на `/collections/{name}/state.json`

```java
private class StateWatcher implements Watcher {
    public void process(WatchedEvent event) {
        if (event.getType() == NodeDataChanged) {
            // Перечитать state.json из ZK, обновить DocCollection в кэше
            refreshAndWatch(collection);
        }
        if (event.getType() == NodeChildrenChanged) {
            // Для PRS: обновить per-replica states из дочерних узлов
            refreshPrsAndWatch(collection);
        }
    }
}
```

**2. CollectionsChildWatcher** — на `/collections`

```java
private class CollectionsChildWatcher implements Watcher {
    public void process(WatchedEvent event) {
        if (event.getType() == NodeChildrenChanged) {
            // Список коллекций изменился: добавлена или удалена коллекция
            // Обновить lazyCollectionStates
            // Вызвать CloudCollectionsListeners
        }
    }
}
```

**3. LiveNodeWatcher** — на `/live_nodes`

```java
private class LiveNodeWatcher implements Watcher {
    public void process(WatchedEvent event) {
        if (event.getType() == NodeChildrenChanged) {
            // Нода вошла или вышла из кластера
            // Обновить liveNodes множество
            // Вызвать LiveNodesListeners
        }
    }
}
```

Дополнительно: watchers на `/clusterprops.json`, `/aliases.json`, `/security.json`.

### waitForState() — барьер синхронизации

```java
public void waitForState(String collection, long timeout, TimeUnit unit,
                          CollectionStatePredicate predicate)
        throws InterruptedException, TimeoutException {

    CountDownLatch latch = new CountDownLatch(1);

    // Зарегистрировать watcher: при каждом изменении состояния проверять predicate
    DocCollectionWatcher watcher = (collectionState) -> {
        if (predicate.matches(liveNodes, collectionState)) {
            latch.countDown();
            return true;  // remove watcher
        }
        return false;
    };

    collectionWatches.computeIfAbsent(collection, ...).addWatcher(watcher);

    // Немедленно проверить текущее состояние
    DocCollection coll = getCollectionOrNull(collection);
    if (predicate.matches(liveNodes, coll)) return;

    // Ждать
    if (!latch.await(timeout, unit)) {
        throw new TimeoutException("Timeout waiting for " + collection);
    }
}
```

Используется в:
- `RecoveryStrategy.sendPrepRecoveryCmd()` — ждать перехода реплики в RECOVERING
- `CreateCollectionCmd` — ждать появления collection в state
- `CollectionsHandler` — ждать ACTIVE всех реплик

### forceUpdateCollection()

```java
public DocCollection forceUpdateCollection(String collection) {
    // Принудительно перечитать state.json из ZK (минуя кэш)
    // Полезно когда нельзя ждать watcher'а
    Stat stat = new Stat();
    byte[] data = zkClient.getData("/collections/" + collection + "/state.json", null, stat);
    DocCollection dc = ClusterState.createFromJson(collection, data, stat.getVersion());
    // Обновить кэш
    collectionWatches.get(collection).updateDocCollection(dc);
    return dc;
}
```

### Lazy collection references

Коллекции, на которые нет активных watchers, кэшируются как `LazyCollectionRef`:

```java
private class LazyCollectionRef extends CollectionRef {
    // Перечитывать из ZK не чаще раз в 2 секунды
    // Проверять ZK stat.version — если не изменился, вернуть кэш
    public DocCollection get() {
        if (isStale()) {
            refetch from ZK;
        }
        return cached;
    }
}
```

---

## 6. LeaderElector: алгоритм выборов лидера

**Файл:** `solr/core/src/java/org/apache/solr/cloud/LeaderElector.java`

Реализует классический ZK-рецепт выборов лидера через ephemeral sequential узлы. Используется для:
- Выборов лидера **каждого шарда** коллекции
- Выборов **Overseer**-а (один на весь кластер)

### Формат имени узла

```
/{electionPath}/election/{sessionId}-{coreNodeName}-n_{seqNum}

Пример:
/collections/mycoll/leader_elect/shard1/election/
    17354782934891234-core_node1-n_0000000001   ← лидер (seq=1)
    17354782934891234-core_node2-n_0000000002   ← следит за seq=1
    17354782934891234-core_node3-n_0000000003   ← следит за seq=2
```

- `sessionId` — ZK session ID (уникален для живой сессии)
- `coreNodeName` — имя реплики в кластере
- `n_{seqNum}` — монотонно возрастающий порядковый номер, выдаётся ZK при создании EPHEMERAL_SEQUENTIAL

### joinElection() — вступить в выборы

```java
public int joinElection(ElectionContext context, boolean replacement, boolean joinAtHead)
        throws KeeperException, InterruptedException, IOException {

    String id = sessionId + "-" + context.id;  // "sessionId-coreNodeName"

    // Создать EPHEMERAL_SEQUENTIAL узел
    if (joinAtHead) {
        // Специальный режим: занять позицию сразу после текущего лидера
        // Используется при ручном управлении очерёдностью
        String firstInLine = sortedNodes.get(1);  // второй в очереди
        int firstSeq = getSeq(firstInLine);
        leaderSeqPath = electionPath + "/" + id + "-n_" + firstSeq;
        zkClient.create(leaderSeqPath, null, CreateMode.EPHEMERAL, false);
    } else {
        // Стандартный режим: занять следующий свободный номер
        leaderSeqPath = zkClient.create(
            electionPath + "/" + id + "-n_",
            null,
            CreateMode.EPHEMERAL_SEQUENTIAL, false);
    }

    // Обработка ConnectionLossException: проверить, был ли узел создан
    // (ZK мог получить запрос, но ответ потерян в сети)

    checkIfIamLeader(context, replacement);
    return getSeq(leaderSeqPath);
}
```

### checkIfIamLeader() — проверка лидерства

```java
private void checkIfIamLeader(ElectionContext context, boolean replacement) {

    // 1. Получить все узлы очереди
    List<String> seqs = zkClient.getChildren(electionPath, null, true);
    sortSeqs(seqs);  // сортировка по seqNum (числовая, не лексикографическая!)

    // 2. Удалить дубликаты нашей сессии (защита от повторного joinElection)
    String prefix = sessionId + "-" + context.id + "-";
    for (String node : seqs) {
        if (!node.equals(myNode) && node.startsWith(prefix)) {
            zkClient.delete(electionPath + "/" + node, -1, true);
        }
    }

    if (myNode.equals(seqs.get(0))) {
        // МЫ — ЛИДЕР (наш seq-номер наименьший)
        runIamLeaderProcess(context, replacement);
        // → context.runLeaderProcess() → ZkController.publish(ACTIVE)
    } else {
        // Найти узел непосредственно перед нами в очереди
        String toWatch = null;
        for (String node : seqs) {
            if (myNode.equals(node)) break;
            toWatch = node;
        }

        // Поставить data-watch на предшественника
        zkClient.getData(electionPath + "/" + toWatch,
            new ElectionWatcher(myNode, toWatch, mySeq, context),
            null, true);
        // Когда предшественник исчезнет → ElectionWatcher.process() → checkIfIamLeader()
    }
}
```

### ElectionWatcher — срабатывает при уходе предшественника

```java
private class ElectionWatcher implements Watcher {
    public void process(WatchedEvent event) {
        if (event.getType() == None) return;  // сессионное событие, не watch
        if (canceled) {
            zkClient.delete(myNode, -1, true);  // удалить себя из очереди
            return;
        }
        // Проверить — не стали ли мы теперь лидером?
        checkIfIamLeader(context, true);
    }
}
```

### sortSeqs() — числовая сортировка

```java
public static void sortSeqs(List<String> seqs) {
    // Сортировать по ЧИСЛОВОМУ значению seqNum, а не лексикографически!
    // "n_10" > "n_9" (числово), но "n_10" < "n_9" (лексикографически)
    seqs.sort(Comparator.comparingInt(LeaderElector::getSeq)
                        .thenComparing(Function.identity()));
}
```

### ElectionContext — контекст конкретных выборов

Две реализации:

**`ShardLeaderElectionContext`** — для шарда коллекции:
- `runLeaderProcess()` → инициирует recovery процесс для отставших реплик, устанавливает флаг `leader=true` в ZK, публикует лидера

**`OverseerElectionContext`** — для Overseer:
- `runLeaderProcess()` → запускает `Overseer.start()` на этой ноде

---

## 7. Overseer: единый координатор кластера

**Файл:** `solr/core/src/java/org/apache/solr/cloud/Overseer.java` (1256 строк)

**Overseer** — единственная нода в кластере, которая пишет в `/collections/*/state.json`. Это гарантирует отсутствие конфликтов при параллельном обновлении состояния кластера.

### Три компонента Overseer

```
Overseer
├── ClusterStateUpdater (поток)
│     Читает /overseer/queue
│     Применяет мутации к ClusterState
│     Пишет state.json через ZkStateWriter
│
├── OverseerCollectionConfigSetProcessor (thread pool)
│     Читает /overseer/collection-queue-work
│     Делегирует в OverseerCollectionMessageHandler
│     и OverseerConfigSetMessageHandler
│
└── LeaderElector (Overseer election)
      Путь: /overseer_elect/election/
      Результат: /overseer_elect/leader (EPHEMERAL)
```

### Выборы Overseer

Каждая нода участвует в выборах Overseer через `LeaderElector` с `OverseerElectionContext`. Путь выборов: `/overseer_elect/election/`.

Текущий Overseer хранит свой ID в `/overseer_elect/leader` (EPHEMERAL):
```json
{"id": "17354782934891234-solr-node1:8983_solr-n_0000000001"}
```

`amILeader()` в `ClusterStateUpdater` периодически сверяет свой ID с этим узлом:
```java
private LeaderStatus amILeader() {
    ZkNodeProps props = ZkNodeProps.load(
        zkClient.getData(OVERSEER_ELECT + "/leader", null, null, true));
    String id = props.getStr("id");
    if (myId.equals(id)) return LeaderStatus.YES;
    return LeaderStatus.NO;
}
```

### Мутаторы кластерного состояния

Каждое сообщение из очереди обрабатывается соответствующим мутатором:

| Операция | Мутатор | Что делает |
|---|---|---|
| `CREATE` (collection) | `ClusterStateMutator.createCollection()` | Добавить DocCollection в state |
| `DELETE` (collection) | `ClusterStateMutator.deleteCollection()` | Удалить DocCollection |
| `CREATESHARD` | `CollectionMutator.createShard()` | Добавить Slice в коллекцию |
| `DELETESHARD` | `CollectionMutator.deleteShard()` | Удалить Slice |
| `ADDREPLICA` | `SliceMutator.addReplica()` | Добавить реплику в шард |
| `STATE` | `ReplicaMutator.setState()` | Изменить состояние реплики |
| `LEADER` | `SliceMutator.setShardLeader()` | Установить лидера шарда |
| `DELETECORE` | `SliceMutator.removeReplica()` | Удалить реплику |
| `DOWNNODE` | `NodeMutator.downNode()` | Пометить все реплики ноды как DOWN |
| `UPDATESHARDSTATE` | `SliceMutator.updateShardState()` | Изменить состояние шарда |
| `MODIFYCOLLECTION` | `CollectionMutator.modifyCollection()` | Изменить свойства коллекции |

---

## 8. Очереди Overseer: три ZK-очереди

### Очередь 1: State Update Queue `/overseer/queue`

**Направление:** любая нода → Overseer  
**Назначение:** изменить cluster state (state.json)  
**Реализация:** `ZkDistributedQueue` — FIFO-очередь через PERSISTENT_SEQUENTIAL узлы

```
/overseer/queue/
    qn-0000000001   {"operation":"state", "collection":"mycoll",
                     "shard":"shard1", "replica":"core_node1",
                     "state":"recovering"}
    qn-0000000002   {"operation":"leader", "collection":"mycoll",
                     "shard":"shard1", "base_url":"http://node1:8983/solr"}
    qn-0000000003   {"operation":"downnode", "node_name":"node2:8983_solr"}
```

**Параметры:**
- Батч: до 1000 сообщений за раз (`peekElements(1000, 3000ms)`)
- Ожидание новых: до 3 секунд (long poll)
- Максимум в очереди: 20000 (`STATE_UPDATE_MAX_QUEUE`)

### Очередь 2: Work Queue `/overseer/queue-work`

**Назначение:** резервная — если Overseer упал в середине батча, новый Overseer начнёт обработку отсюда. Гарантирует at-least-once доставку.

**Логика:**
```
При старте нового Overseer:
  if (workQueue не пуст) {
      // Обработать workQueue первым (восстановление после краша)
      processWorkQueue();
  }
  // Затем перейти на stateUpdateQueue
```

### Очередь 3: Collection API Queue `/overseer/collection-queue-work`

**Направление:** `CollectionsHandler` → Overseer  
**Назначение:** API-запросы создания/удаления коллекций, шардов, реплик  
**Реализация:** `OverseerTaskQueue` — расширение ZkDistributedQueue с request-response паттерном

**Request-Response паттерн:**

```
Клиент (CollectionsHandler):
  1. Создать EPHEMERAL_SEQUENTIAL response node:
     /overseer/collection-queue-work/qnr-0000000042  ← ответ придёт сюда
  2. Поставить watch на qnr-0000000042
  3. Создать PERSISTENT_SEQUENTIAL request node:
     /overseer/collection-queue-work/qn-0000000043
     {"operation":"create", "name":"mycoll", ...,
      "replyTo":"/overseer/collection-queue-work/qnr-0000000042"}
  4. Ждать срабатывания watch на qnr-0000000042 (таймаут configurable)

Overseer (OverseerCollectionConfigSetProcessor):
  1. Прочитать qn-0000000043
  2. Выполнить CreateCollectionCmd
  3. Записать результат в qnr-0000000042:
     {"status":"0", "success":{"node1:8983_solr": {...}}}
  4. Удалить qn-0000000043

Клиент:
  5. Watch сработал → прочитать результат из qnr-0000000042
  6. Удалить qnr-0000000042
  7. Вернуть ответ клиенту
```

**Почему сначала response node?** Если создать request первым, а response node — вторым, есть риск что Overseer обработает запрос до создания response node и записать некуда. Создание response первым устраняет эту гонку.

---

## 9. ClusterStateUpdater: запись состояния в ZK

`ClusterStateUpdater` — единственный поток, пишущий в `state.json`. Это фундаментальное проектное решение: сериализованная запись устраняет конфликты.

### Основной цикл обработки

```java
// Overseer.ClusterStateUpdater.run()
while (!isClosed) {
    
    // 1. Убедиться, что мы всё ещё лидер
    if (amILeader() == NO) break;

    // 2. При необходимости обновить полное состояние кластера
    if (refreshClusterState) {
        reader.forciblyRefreshAllClusterStateSlow();
        clusterState = reader.getClusterState();
        zkStateWriter = new ZkStateWriter(reader, ...);

        // 3. Сначала обработать workQueue (восстановление)
        while (!workQueue.isEmpty()) {
            ZkNodeProps msg = workQueue.peek();
            clusterState = processQueueItem(msg, clusterState, zkStateWriter, false, null);
            workQueue.poll();
        }
        clusterState = zkStateWriter.writePendingUpdates();  // flush
    }

    // 4. Батч-чтение из stateUpdateQueue
    ArrayDeque<Pair<String, byte[]>> queue =
        stateUpdateQueue.peekElements(1000, 3000L, (x) -> true);
    // peekElements: взять до 1000 элементов, ждать до 3 сек новых

    // 5. Обработать каждое сообщение
    Set<String> processedNodes = new HashSet<>();
    for (Pair<String, byte[]> item : queue) {
        ZkNodeProps message = ZkNodeProps.load(item.second());
        processedNodes.add(item.first());

        // Применить мутацию (in-memory)
        clusterState = processQueueItem(message, clusterState, zkStateWriter,
            true,                    // enableBatching=true
            () -> {
                stateUpdateQueue.remove(processedNodes);  // удалить из ZK
                processedNodes.clear();
            });
    }

    // 6. Сбросить батч в ZK
    clusterState = zkStateWriter.writePendingUpdates();
    stateUpdateQueue.remove(processedNodes);
}
```

### Обработка ошибок

- `BadVersionException` → `refreshClusterState = true` → перечитать состояние и начать заново
- `KeeperException.SessionExpiredException` → выйти из цикла (сессия потеряна)
- Иные исключения: `log.error()`, `refreshClusterState = true`
- `isBadMessage()` — NONODE/NODEEXISTS = структурная ошибка → пропустить сообщение

---

## 10. ZkStateWriter: батчинг и оптимистичный локинг

**Файл:** `solr/core/src/java/org/apache/solr/cloud/overseer/ZkStateWriter.java`

### Буферизация in-memory

`ZkStateWriter` аккумулирует `ZkWriteCommand` в памяти и записывает в ZK батчами:

```java
public ClusterState enqueueUpdate(ClusterState clusterState,
                                   List<ZkWriteCommand> cmds,
                                   ZkWriteCallback callback) {
    for (ZkWriteCommand cmd : cmds) {
        // Применить мутацию к in-memory ClusterState
        clusterState = clusterState.copyWith(cmd.name, cmd.collection);
        updates.put(cmd.name, cmd);  // буфер
    }
    if (callback != null) callback.run();  // удалить из очереди ZK
    return clusterState;
}
```

### writePendingUpdates() — запись в ZK

```java
public ClusterState writePendingUpdates() {
    for (Map.Entry<String, ZkWriteCommand> entry : updates.entrySet()) {
        String collection = entry.getKey();
        DocCollection dc = entry.getValue().collection;

        // Сериализовать в JSON
        byte[] data = Utils.toJSON(dc);

        // Опционально сжать (ZLib если > minStateByteLenForCompression)
        if (data.length > minStateByteLenForCompression) {
            data = compressor.compress(data);
        }

        // Оптимистичный update с проверкой версии (CAS)
        int znodeVersion = dc.getZNodeVersion();
        if (znodeVersion == -1) {
            // Новая коллекция — создать узел
            zkClient.create("/collections/" + collection + "/state.json",
                data, CreateMode.PERSISTENT, true);
        } else {
            // Обновить существующий с проверкой версии
            zkClient.setData("/collections/" + collection + "/state.json",
                data, znodeVersion, true);
            // BadVersionException если кто-то параллельно изменил
        }
    }
    updates.clear();
    return reader.getClusterState();
}
```

### Версионный CAS

ZK хранит версию каждого узла (`Stat.version`). `setData(path, data, version)` атомарно:
- Если текущая версия в ZK совпадает с `version` → обновить
- Если нет → выбросить `BadVersionException`

`DocCollection.znodeVersion` содержит версию на момент последнего чтения. При параллельном изменении (например, другой Overseer в момент переключения) CAS-обновление падает, что заставляет перечитать актуальное состояние.

---

## 11. Регистрация core: register()

`ZkController.register()` — ключевой метод, вызываемый при старте каждого `SolrCore`. Семь шагов:

```
register(coreName, desc, recoveringAfterStartup, skipRecovery, ...):

Шаг 1: Ожидание появления реплики в cluster state
  Waiters: zkStateReader.waitForState(collection, 30s,
               предикат: replica exists in state)
  Зачем: Overseer мог ещё не обработать предыдущее ADD_REPLICA

Шаг 2: Регистрация в ZkShardTerms
  zkShardTerms.register(collection, shard, coreName)
  Shard Terms: числовые "голоса" реплик для определения
  актуальности при выборах

Шаг 3: Вступление в выборы лидера (NRT/TLOG)
  if (replicaType != PULL) {
      joinElection(context, false)
  }

Шаг 4: Найти текущего лидера шарда
  Replica leader = getLeader(cloudDesc, waitForLeaderTimeoutMs)
  Ждать до leaderVoteWait миллисекунд

Шаг 5: Для NRT: воспроизвести tlog до регистрации
  if (replicaType == NRT && recoveringAfterStartup) {
      ulog.recoverFromLog()  // replay незавершённых tlog'ов
  }

Шаг 6: Определить нужен ли recovery
  boolean needsRecovery = checkRecovery(recoveringAfterStartup, leader)
  Критерии:
    - NRT/TLOG: сравнить наши shard terms с лидером
    - TLOG: нужна репликация индекса от лидера
    - PULL: всегда нужна репликация

Шаг 7: Опубликовать состояние
  if (needsRecovery) {
      startRecovery()           // → publish(RECOVERING)
  } else {
      publish(ACTIVE)
  }
```

---

## 12. Публикация состояния реплики: publish()

```java
public void publish(CoreDescriptor cd, Replica.State state) {
    CloudDescriptor cloudDesc = cd.getCloudDescriptor();

    // 1. Собрать свойства реплики
    MapWriter stateProps = ew -> {
        ew.put(ZkStateReader.STATE_PROP, state.toString())
          .put(ZkStateReader.NODE_NAME_PROP, nodeName)
          .put(ZkStateReader.BASE_URL_PROP, baseUrl)
          .put(ZkStateReader.CORE_NAME_PROP, coreName)
          .put(ZkStateReader.SHARD_ID_PROP, shardId)
          .put(ZkStateReader.COLLECTION_PROP, collection)
          .put(ZkStateReader.CORE_NODE_NAME_PROP, coreNodeName)
          .put(ZkStateReader.REPLICA_TYPE, replicaType);
    };

    // 2. При переходе в ACTIVE: убедиться, что searcher открыт
    if (state == ACTIVE) {
        core.getSearcher(false, true, null, false);
    }

    // 3. Обновить ShardTerms
    if (state == RECOVERING) {
        shardTerms.startRecovering(coreNodeName);
    } else if (state == ACTIVE) {
        shardTerms.doneRecovering(coreNodeName);
    }

    // 4. Отправить в Overseer через очередь (или distributed updater)
    if (distributedClusterStateUpdater.isDistributedStateUpdate()) {
        // Прямая запись в ZK (без Overseer)
        distributedClusterStateUpdater.publishReplicaState(cd, state, ...);
    } else {
        // Через Overseer queue: /overseer/queue
        overseerJobQueue.offer(Utils.toJSON(
            new ZkNodeProps(stateProps)));
    }
}
```

---

## 13. Обработка обрыва ZK-сессии

Обрыв ZK-сессии — самое критичное событие. Все эфемерные узлы (live_node, election-узлы) удаляются автоматически. Нужно воссоздать всё с нуля.

### beforeReconnect() — вызывается ДО нового соединения

```java
private void beforeReconnect(Supplier<List<CoreDescriptor>> descriptors) {
    // 1. Остановить Overseer (если мы были Overseer)
    overseer.close();

    // 2. Отменить все election contexts
    closeOutstandingElections(descriptors);

    // 3. Снять флаги лидерства со всех CoreDescriptor
    markAllAsNotLeader(descriptors);
}
```

### onReconnect() — вызывается ПОСЛЕ нового соединения

```java
private void onReconnect(Supplier<List<CoreDescriptor>> descriptors)
        throws SessionExpiredException {

    // 1. Очистить локальный кэш shard terms
    clearZkCollectionTerms();

    // 2. Воссоздать watchers и перечитать кластерное состояние
    zkStateReader.createClusterStateWatchersAndUpdate();

    // 3. Перезапустить выборы Overseer
    if (!zkRunOnly) {
        ElectionContext context = new OverseerElectionContext(zkClient, overseer, nodeName);
        overseerElector.setup(context);
        overseerElector.joinElection(context, true);  // replacement=true
    }

    // 4. Воссоздать эфемерный live_node
    createEphemeralLiveNode();

    // 5. Перерегистрировать все локальные cores асинхронно
    for (CoreDescriptor cd : descriptors.get()) {
        ExecutorService executor = cc.getUpdateShardHandler().getRecoveryExecutor();
        executor.submit(new RegisterCoreAsync(cd, true, true));
    }

    // 6. Уведомить всех OnReconnect listeners
    for (OnReconnect listener : reconnectListeners) {
        listener.command();
    }
}
```

**Порядок критичен:** watchers должны быть установлены ДО перерегистрации cores, иначе изменения состояния могут быть пропущены.

### OnReconnect listeners

Типичные подписчики:
- `ConfigDirListener` — перечитать конфиги если обновились за время отсутствия
- `UnloadCoreOnDeletedWatcher` — выгрузить core если реплика была удалена
- Схема-watchers на `managed-schema`

---

## 14. Live Nodes: эфемерные узлы живых нод

### Создание эфемерного узла

```java
public void createEphemeralLiveNode() {
    // Атомарно создать несколько узлов через ZK multi():

    List<Op> ops = new ArrayList<>();

    // 1. Основной live_node
    ops.add(Op.create(
        ZkStateReader.LIVE_NODES_ZKNODE + "/" + nodeName,
        null,
        acls,
        CreateMode.EPHEMERAL));

    // 2. Если нода имеет роль — добавить в /node_roles
    for (String role : nodeRoles) {
        ops.add(Op.create(
            "/node_roles/" + role + "/" + nodeName,
            null, acls, CreateMode.EPHEMERAL));
    }

    zkClient.multi(ops, true);  // atomic!
}
```

### Что происходит при падении ноды

1. ZK-сессия истекает (`sessionTimeout`, обычно 30–60 сек)
2. ZK автоматически удаляет все EPHEMERAL узлы этой сессии
3. `LiveNodeWatcher` на других нодах срабатывает
4. `ZkStateReader` обновляет `liveNodes` множество
5. Реплики мёртвой ноды автоматически считаются DOWN (их нода вне `liveNodes`)
6. `Overseer` получает уведомление и обрабатывает `DOWNNODE`

### `publishNodeAsDown()` — graceful shutdown

При штатной остановке ноды:
```java
public void publishNodeAsDown(String nodeName) {
    // Найти все реплики на этой ноде
    for (Replica replica : allReplicasOnNode(nodeName)) {
        ZkNodeProps m = new ZkNodeProps(
            "operation", "downnode",
            "node_name", nodeName);
        overseerJobQueue.offer(Utils.toJSON(m));
        // Overseer обработает через NodeMutator.downNode()
    }
}
```

---

## 15. Per-Replica State (PRS): масштабирование state.json

**Проблема:** при большом количестве реплик (тысячи) единый `state.json` становится узким местом:
- Каждое изменение состояния любой реплики перезаписывает весь файл
- Все ноды читают полный файл при каждом watch

**Решение:** Per-Replica State (PRS) — хранить состояние каждой реплики в отдельном ZK-узле.

### Структура PRS

```
/collections/{collectionName}/
    state.json              ← статическая структура (шарды, роутер, конфиг)
    {replicaName}           ← EPHEMERAL — состояние конкретной реплики
```

Содержимое `{replicaName}` узла:
```
"active"  или  "recovering"  или  "down"
```

### Когда включается PRS

```java
// DocCollection.java
public boolean isPerReplicaState() {
    return Boolean.parseBoolean(
        (String) properties.get("perReplicaState"));
}
```

Устанавливается при создании коллекции: `CREATE?perReplicaState=true`.

### PerReplicaStates — управление

```java
// PerReplicaStates.java
public class PerReplicaStates {
    // Атомарно обновить состояние реплики:
    // - Удалить старый узел
    // - Создать новый с новым состоянием
    public static void setReplicaState(SolrZkClient zkClient,
                                        String collection,
                                        String replicaName,
                                        Replica.State state) {
        String path = "/collections/" + collection + "/" + replicaName;
        String oldNode = findCurrentStateNode(replicaName);
        // multi: delete old + create new
        List<Op> ops = List.of(
            Op.delete(oldNode, -1),
            Op.create(path, state.toString().getBytes(), acls, EPHEMERAL));
        zkClient.multi(ops, true);
    }
}
```

---

## 16. Distributed Cluster State Updates

**Проблема Overseer:** при интенсивной нагрузке (много коллекций, частые state updates) Overseer становится bottleneck — всё пишется через один поток.

**Решение:** `DistributedClusterStateUpdater` — каждая нода пишет своё состояние **напрямую** в ZK без Overseer.

```java
// ZkController.java
DistributedClusterStateUpdater distributedClusterStateUpdater =
    new DistributedClusterStateUpdater(cloudConfig.getDistributedClusterStateUpdates());

// При publish():
if (distributedClusterStateUpdater.isDistributedStateUpdate()) {
    // Прямая запись в ZK
    distributedClusterStateUpdater.publishReplicaState(cd, state, ...);
} else {
    // Через Overseer queue (legacy)
    overseerJobQueue.offer(Utils.toJSON(stateProps));
}
```

Включается через `cloudConfig.distributedClusterStateUpdates=true`. В этом режиме `/overseer/queue` используется только для операций уровня коллекций (CREATE, DELETE и т.п.), не для replica state updates.

---

## 17. ConfigSets в ZooKeeper

### Структура

```
/configs/
    _default/               ← дефолтный configset
        solrconfig.xml
        managed-schema
        lang/
        ...
    myconfig/               ← пользовательский configset
        solrconfig.xml
        schema.xml
        ...
```

### Загрузка и watch

При создании `SolrCore` конфигурация загружается из ZK:
```java
// ZkSolrResourceLoader
byte[] data = zkClient.getData("/configs/" + configName + "/" + resource, null, null, true);
```

`ConfigDirListener` (реализация `OnReconnect`) следит за изменениями:
```java
// ZkController.getConfigDirListener()
// Watcher на /configs/{configName}/
// При изменении → zkController.fireConfigChanged(collection)
// → core.scheduleReloadAsap() если коллекция использует этот конфиг
```

### ConfigSet API через Overseer

Операции с ConfigSet (UPLOAD, DELETE, RELOAD) идут через `/overseer/collection-queue-work` и обрабатываются `OverseerConfigSetMessageHandler`.

---

## 18. Создание коллекции: сквозной пример

Полный путь запроса `CREATE collection`:

```
Клиент
│  POST /solr/admin/collections?action=CREATE&name=mycoll&numShards=2&replicationFactor=2
│
▼
CollectionsHandler (любая нода кластера)
│  1. Проверить валидность параметров
│  2. Создать response node в ZK:
│     /overseer/collection-queue-work/qnr-0000000042 (EPHEMERAL_SEQUENTIAL)
│  3. Поставить watch на qnr-0000000042
│  4. Создать request node:
│     /overseer/collection-queue-work/qn-0000000043 (PERSISTENT_SEQUENTIAL)
│     {"operation":"create","name":"mycoll","numShards":"2","replicationFactor":"2",
│      "replyTo":"/overseer/collection-queue-work/qnr-0000000042"}
│  5. Ждать watch (таймаут: defaultCollectionCreateWait)
│
▼
OverseerCollectionConfigSetProcessor (на ноде Overseer)
│  1. Прочитать qn-0000000043
│  2. Передать в OverseerCollectionMessageHandler
│
▼
CreateCollectionCmd.call()
│  1. Проверить, что configName существует в /configs/
│  2. Создать ZK-узел /collections/mycoll/ (PERSISTENT)
│  3. Отправить сообщение в /overseer/queue:
│     {"operation":"create","name":"mycoll","numShards":2,...}
│  4. Ждать появления mycoll в cluster state (ZkStateReader.waitForState)
│
▼
ClusterStateUpdater (поток на Overseer)
│  1. Прочитать из /overseer/queue
│  2. ClusterStateMutator.createCollection() → новый DocCollection in memory
│  3. ZkStateWriter.enqueueUpdate()
│  4. ZkStateWriter.writePendingUpdates():
│     Создать /collections/mycoll/state.json:
│     {"shards":{"shard1":{...},"shard2":{...}}, "configName":"_default", ...}
│
▼
CreateCollectionCmd продолжает:
│  5. Вычислить размещение реплик (PlacementPlugin):
│     shard1/replica1 → node1, shard1/replica2 → node2
│     shard2/replica1 → node2, shard2/replica2 → node1
│  6. Отправить CREATE_CORE на каждую ноду через ShardHandler (параллельно)
│
▼
Каждая нода (node1, node2):
│  1. Создать SolrCore
│  2. ZkController.register() → joinElection
│  3. Выборы лидера шарда
│  4. publish(ACTIVE) → /overseer/queue или прямо в ZK
│
▼
CreateCollectionCmd:
│  7. Дождаться ACTIVE всех реплик (waitForState, 30 сек)
│  8. Записать ответ в qnr-0000000042
│
▼
CollectionsHandler:
│  6. Watch сработал → прочитать ответ
│  7. Вернуть клиенту {"responseHeader":{"status":0},...}
```

**Итоговые ZK-записи:**
```
/collections/mycoll/state.json                 ← создан
/collections/mycoll/leaders/shard1/leader      ← создан (EPHEMERAL)
/collections/mycoll/leaders/shard2/leader      ← создан (EPHEMERAL)
/collections/mycoll/leader_elect/shard1/election/{seq}  ← 2 узла
/collections/mycoll/leader_elect/shard2/election/{seq}  ← 2 узла
```

---

## 19. Ключевые классы и файлы

| Класс | Файл | Роль |
|---|---|---|
| `ZkController` | `cloud/ZkController.java` | Главный координатор ноды: регистрация, публикация состояний, reconnect |
| `SolrZkClient` | `solrj-zookeeper/.../SolrZkClient.java` | Обёртка над ZK-клиентом: retry, async watchers, ACL, сжатие |
| `ZkStateReader` | `solrj-zookeeper/.../ZkStateReader.java` | Локальный кэш cluster state: watchers, waitForState |
| `LeaderElector` | `cloud/LeaderElector.java` | ZK-рецепт выборов лидера для шардов и Overseer |
| `Overseer` | `cloud/Overseer.java` | Единый координатор кластера: три компонента |
| `ClusterStateUpdater` | внутренний класс `Overseer.java` | Поток чтения очереди и записи state.json |
| `ZkStateWriter` | `cloud/overseer/ZkStateWriter.java` | Батчинг и CAS-запись state.json |
| `OverseerTaskQueue` | `cloud/OverseerTaskQueue.java` | Очередь Collection API с request-response |
| `ZkDistributedQueue` | `cloud/ZkDistributedQueue.java` | Базовая FIFO ZK-очередь |
| `ClusterState` | `solrj-zookeeper/.../ClusterState.java` | In-memory модель: Map\<collection, DocCollection\> |
| `DocCollection` | `solrj-zookeeper/.../DocCollection.java` | Коллекция: шарды, реплики, роутер, версия |
| `Slice` | `solrj-zookeeper/.../Slice.java` | Шард: реплики, range, лидер |
| `Replica` | `solrj-zookeeper/.../Replica.java` | Реплика: state, type, node, URL |
| `ElectionContext` | `cloud/ElectionContext.java` | Абстракция контекста выборов |
| `ShardLeaderElectionContext` | `cloud/ShardLeaderElectionContext.java` | Выборы лидера шарда |
| `OverseerElectionContext` | `cloud/OverseerElectionContext.java` | Выборы Overseer |
| `DistributedClusterStateUpdater` | `cloud/DistributedClusterStateUpdater.java` | Прямая запись без Overseer |
| `PerReplicaStates` | `solrj-zookeeper/.../PerReplicaStates.java` | PRS: управление per-replica ZK-узлами |
| `ReplicaMutator` | `cloud/overseer/ReplicaMutator.java` | Мутации состояния реплик |
| `SliceMutator` | `cloud/overseer/SliceMutator.java` | Мутации шардов и лидеров |
| `NodeMutator` | `cloud/overseer/NodeMutator.java` | Обработка падения ноды (DOWNNODE) |
| `ClusterStateMutator` | `cloud/overseer/ClusterStateMutator.java` | Создание/удаление коллекций |
| `ZkShardTerms` | `cloud/ZkShardTerms.java` | Shard terms для определения актуальности реплик |

---

## 20. Справочная таблица: параметры и константы

| Параметр / Константа | Значение | Описание |
|---|---|---|
| `STATE_UPDATE_DELAY` | 2000 мс | Задержка между батчами в ClusterStateUpdater |
| `STATE_UPDATE_BATCH_SIZE` | 10 000 | Макс. сообщений за один батч |
| `STATE_UPDATE_MAX_QUEUE` | 20 000 | Макс. размер `/overseer/queue` |
| `NUM_RESPONSES_TO_STORE` | 10 000 | Макс. записей в collection-map-completed/failure |
| `peekElements batch` | 1 000 | Элементов за раз из stateUpdateQueue |
| `peekElements wait` | 3 000 мс | Long poll ожидание новых элементов |
| `zkReaderGetLeaderRetryTimeoutMs` | 4 000 мс | Таймаут `getLeaderRetry()` |
| `leaderVoteWait` | 180 000 мс | Ожидание завершения выборов лидера шарда |
| `leaderConflictResolveWait` | 10 000 мс | Таймаут WaitForState в RecoveryStrategy |
| `waitForState` коллекция | 30 сек | Ожидание в `CreateCollectionCmd` |
| `OVERSEER_ELECT` | `/overseer_elect` | Путь выборов Overseer |
| `LIVE_NODES_ZKNODE` | `/live_nodes` | Путь живых нод |
| `COLLECTIONS_ZKNODE` | `/collections` | Путь коллекций |
| `CONFIGS_ZKNODE` | `/configs` | Путь конфигсетов |
| `ZK node format` | `{sesId}-{coreNode}-n_{seq}` | Формат узла election очереди |
| `ZK queue request` | `qn-XXXXXXXXXX` | Запрос в OverseerTaskQueue |
| `ZK queue response` | `qnr-XXXXXXXXXX` | Ответ в OverseerTaskQueue |
| `minStateByteLenForCompression` | конфигурируемо | Порог сжатия state.json |
| `sessionTimeout` | 30–60 сек | Время до удаления ephemeral узлов при падении |
| `ZkCmdExecutor retry timeout` | `clientTimeout` | Суммарный таймаут retry операций |
| `OverseerTaskProcessor.MAX_PARALLEL_TASKS` | 10 | Параллельность Collection API задач |
| `LazyCollectionRef stale` | 2 сек | TTL lazy collection кэша |
