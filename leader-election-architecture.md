# Архитектура Leader Election в Apache Solr

## Оглавление

1. [Обзор: два типа выборов](#1-обзор-два-типа-выборов)
2. [ZK-узлы election: структура и форматы](#2-zk-узлы-election-структура-и-форматы)
3. [LeaderElector: базовый алгоритм ZK-рецепта](#3-leaderelector-базовый-алгоритм-zk-рецепта)
4. [ElectionContext: контракт конкретных выборов](#4-electioncontext-контракт-конкретных-выборов)
5. [ZkShardTerms: "голоса" актуальности реплик](#5-zkshardterms-голоса-актуальности-реплик)
6. [ShardLeaderElectionContext: полный процесс выборов лидера шарда](#6-shardleaderelectioncontext-полный-процесс-выборов-лидера-шарда)
7. [ShardLeaderElectionContextBase: запись лидера в ZK](#7-shardleaderelectioncontextbase-запись-лидера-в-zk)
8. [SyncStrategy: синхронизация перед принятием лидерства](#8-syncstrategy-синхронизация-перед-принятием-лидерства)
9. [OverseerElectionContext: выборы Overseer](#9-overseerelectioncontext-выборы-overseer)
10. [Регистрация реплики в выборах: joinElection()](#10-регистрация-реплики-в-выборах-joinelection)
11. [Обрыв сессии и повторные выборы](#11-обрыв-сессии-и-повторные-выборы)
12. [Граничные случаи и защитные механизмы](#12-граничные-случаи-и-защитные-механизмы)
13. [Полная последовательность: от падения лидера до нового ACTIVE](#13-полная-последовательность-от-падения-лидера-до-нового-active)
14. [Ключевые классы и файлы](#14-ключевые-классы-и-файлы)
15. [Параметры и константы](#15-параметры-и-константы)

---

## 1. Обзор: два типа выборов

В SolrCloud есть два независимых механизма выборов, оба основаны на одном `LeaderElector`, но с разными контекстами:

| Тип | Путь в ZK | Контекст | Победитель |
|---|---|---|---|
| **Лидер шарда** | `/collections/{coll}/leader_elect/{shard}/election/` | `ShardLeaderElectionContext` | Лидер конкретного шарда коллекции |
| **Overseer** | `/overseer_elect/election/` | `OverseerElectionContext` | Единый координатор всего кластера |

**Лидеров шардов** в кластере много: по одному на каждый шард каждой коллекции. Они принимают записи, рассылают обновления репликам.

**Overseer** — один на весь кластер. Обрабатывает операции с cluster state (создание коллекций, регистрация лидеров, обработка отказов нод).

---

## 2. ZK-узлы election: структура и форматы

### Election queue — очередь кандидатов

```
/collections/{collName}/leader_elect/{shardId}/election/
    {sessionId}-{coreNodeName}-n_0000000001   EPHEMERAL_SEQUENTIAL  ← лидер
    {sessionId}-{coreNodeName}-n_0000000002   EPHEMERAL_SEQUENTIAL  ← следит за n_1
    {sessionId}-{coreNodeName}-n_0000000003   EPHEMERAL_SEQUENTIAL  ← следит за n_2
```

Разбор имени узла (паттерны в `LeaderElector.java`):
```java
// LEADER_SEQ: извлечь порядковый номер
Pattern.compile(".*?/?.*?-n_(\\d+)")
// SESSION_ID: извлечь sessionId-coreNodeName
Pattern.compile(".*?/?(.*?-.*?)-n_\\d+")
// NODE_NAME: извлечь только coreNodeName
Pattern.compile(".*?/?(.*?-)(.*?)-n_\\d+")
```

Пример реального узла:
```
144115188075855890-core_node3-n_0000000002
├── 144115188075855890  = ZK session ID
├── core_node3          = coreNodeName реплики
└── n_0000000002        = порядковый номер (выдаётся ZK при EPHEMERAL_SEQUENTIAL)
```

### Leader znode — текущий лидер шарда

```
/collections/{collName}/leaders/{shardId}/leader   EPHEMERAL

Содержимое (JSON):
{
  "core":      "mycoll_shard1_replica_n1",
  "node_name": "node1:8983_solr",
  "base_url":  "http://node1:8983/solr",
  "state":     "active",
  "type":      "NRT"
}
```

Создаётся атомарной ZK multi-операцией в `ShardLeaderElectionContextBase.runLeaderProcess()`.

### Terms znode — актуальность реплик

```
/collections/{collName}/terms/{shardId}   PERSISTENT

Содержимое (JSON):
{
  "core_node1": 5,
  "core_node2": 5,
  "core_node3": 4,
  "core_node3_recovering": 1
}
```

Версия узла (ZK `Stat.version`) используется для CAS-обновлений.

---

## 3. LeaderElector: базовый алгоритм ZK-рецепта

**Файл:** `solr/core/src/java/org/apache/solr/cloud/LeaderElector.java`

Реализует классический ZK-рецепт leader election через ephemeral sequential узлы. Суть: у кого наименьший порядковый номер — тот лидер; остальные следят за своим предшественником.

### joinElection() — вступить в очередь

```java
public int joinElection(ElectionContext context, boolean replacement, boolean joinAtHead) {

    String id = sessionId + "-" + context.id;  // "sessionId-coreNodeName"
    String electionPath = context.electionPath + "/election";

    while (cont) {
        try {
            if (joinAtHead) {
                // ── Особый режим: занять позицию сразу за текущим лидером ──
                // Используется при переключении лидеров без очереди ожидания.
                List<String> nodes = getSortedElectionNodes(electionPath);
                if (nodes.size() < 2) {
                    // Очередь пуста или только лидер — создать обычный EPHEMERAL_SEQUENTIAL
                    leaderSeqPath = zkClient.create(
                        electionPath + "/" + id + "-n_", null,
                        CreateMode.EPHEMERAL_SEQUENTIAL, false);
                } else {
                    // Взять seq-номер второго в очереди (сразу за лидером)
                    int firstSeq = getSeq(nodes.get(1));
                    leaderSeqPath = electionPath + "/" + id + "-n_" + firstSeq;
                    zkClient.create(leaderSeqPath, null, CreateMode.EPHEMERAL, false);
                }
            } else {
                // ── Стандартный режим: занять следующий свободный номер ──
                leaderSeqPath = zkClient.create(
                    electionPath + "/" + id + "-n_", null,
                    CreateMode.EPHEMERAL_SEQUENTIAL, false);
            }
            cont = false;

        } catch (ConnectionLossException e) {
            // ── Защита от потери соединения ──
            // Запрос мог дойти до ZK, но ответ потерян → проверить, создан ли узел
            boolean foundId = zkClient.getChildren(electionPath)
                .stream().anyMatch(e -> getNodeId(e).equals(id));
            if (!foundId) cont = true;
            // до 20 попыток с паузой 50мс
        }
    }

    context.leaderSeqPath = leaderSeqPath;
    checkIfIamLeader(context, replacement);
    return getSeq(leaderSeqPath);
}
```

### checkIfIamLeader() — проверка и слежение

```java
private void checkIfIamLeader(ElectionContext context, boolean replacement) {

    // 1. Получить все узлы очереди
    List<String> seqs = zkClient.getChildren(electionPath, null, true);
    sortSeqs(seqs);  // числовая сортировка! n_9 < n_10

    // 2. Проверить, что наш узел существует
    String myNodeName = lastSegment(context.leaderSeqPath);
    if (!seqs.contains(myNodeName)) {
        log.warn("Our node is no longer in line to be leader");
        return;
    }

    // 3. Удалить дубликаты (защита от двойной регистрации после reconnect)
    String prefix = sessionId + "-" + context.id + "-";
    for (String node : seqs) {
        if (!node.equals(myNodeName) && node.startsWith(prefix)) {
            zkClient.delete(electionPath + "/" + node, -1, true);
        }
    }

    if (myNodeName.equals(seqs.get(0))) {
        // ── МЫ ЛИДЕР (наш seq наименьший) ──
        runIamLeaderProcess(context, replacement);
        // → context.runLeaderProcess(weAreReplacement=replacement, 0)

    } else {
        // ── НЕ ЛИДЕР: найти предшественника и поставить watch ──
        String toWatch = seqs.get(0);
        for (String node : seqs) {
            if (myNodeName.equals(node)) break;
            toWatch = node;  // последний до нас
        }

        try {
            // getData с watcher (не exists!) — getData удаляет watch при удалении узла
            zkClient.getData(
                electionPath + "/" + toWatch,
                new ElectionWatcher(myNodeName, toWatch, mySeq, context),
                null, true);

        } catch (NoNodeException e) {
            // Предшественник уже исчез — проверить снова
            checkIfIamLeader(context, true);
        }
    }
}
```

### sortSeqs() — числовая сортировка

```java
public static void sortSeqs(List<String> seqs) {
    seqs.sort(
        Comparator.comparingInt(LeaderElector::getSeq)
                  .thenComparing(Function.identity())
    );
}
// Ключевой момент: n_0000000010 > n_0000000009
// лексикографически "10" < "9", но числово 10 > 9
// Неправильная сортировка привела бы к неверному определению лидера!
```

### ElectionWatcher — срабатывает при уходе предшественника

```java
private class ElectionWatcher implements Watcher {
    final String myNode;      // наш путь в очереди
    final String watchedNode; // путь предшественника
    private boolean canceled = false;

    public void process(WatchedEvent event) {
        if (EventType.None.equals(event.getType())) return; // сессионные события не трогаем

        if (canceled) {
            // Мы отменили участие (например, во время shutdown)
            // Удалить себя из очереди
            zkClient.delete(myNode, -1, true);
            return;
        }

        // Предшественник исчез → проверить, стали ли мы лидером
        checkIfIamLeader(context, true);
        // weAreReplacement=true: кто-то уже был лидером до нас
    }
}
```

### Почему getData, а не exists?

`zkClient.getData(path, watcher)` ставит **data watch**, который срабатывает при удалении узла. `exists(path, watcher)` ставит **exists watch** — он тоже срабатывает при удалении, но также при **создании** (NodeCreated). Для election это не критично, но `getData` несёт ещё и данные узла, что полезно для диагностики. Важнее другое: `getData` бросает `NoNodeException` если узел уже удалён в момент вызова — это сразу запускает `checkIfIamLeader(context, true)` вместо потери события.

---

## 4. ElectionContext: контракт конкретных выборов

**Файл:** `solr/core/src/java/org/apache/solr/cloud/ElectionContext.java`

Абстрактный базовый класс. Определяет что делать **когда мы победили**.

```java
public abstract class ElectionContext implements Closeable {
    final String electionPath;    // путь к election/ директории
    final ZkNodeProps leaderProps; // свойства будущего лидера (URL, core name и т.д.)
    final String id;              // coreNodeName — идентификатор кандидата
    final String leaderPath;      // путь куда записать /leaders/{shard}/leader
    volatile String leaderSeqPath; // наш ephemeral sequential узел в очереди

    // Главный метод — вызывается когда мы стали лидером
    abstract void runLeaderProcess(boolean weAreReplacement, int pauseBeforeStartMs);

    // Удалить наш ephemeral узел из очереди
    public void cancelElection() {
        zkClient.delete(leaderSeqPath, -1, true);
    }
}
```

### Иерархия контекстов

```
ElectionContext (abstract)
├── ShardLeaderElectionContextBase
│     ├── ShardLeaderElectionContext        ← полная логика (syncing, terms, recovery)
│     └── (используется напрямую для тестов)
└── OverseerElectionContext                 ← запускает Overseer.start()
```

---

## 5. ZkShardTerms: "голоса" актуальности реплик

**Файл:** `solr/core/src/java/org/apache/solr/cloud/ZkShardTerms.java`

**Проблема без terms:** Если лидер падает, а реплика с устаревшим индексом побеждает в race condition выборов — данные теряются.

**Решение:** каждая реплика шарда имеет числовой **term** (версию). Только реплика с максимальным term может стать лидером.

### ZK-путь

```
/collections/{collName}/terms/{shardId}   PERSISTENT

Пример содержимого:
{
  "core_node1": 5,          ← лидер, самый высокий term
  "core_node2": 5,          ← в синхронизации с лидером
  "core_node3": 4,          ← отстаёт (был в recovery)
  "core_node3_recovering": 1 ← маркер: core_node3 сейчас восстанавливается
}
```

### Семантика term

| Значение | Значение |
|---|---|
| 0 | Зарегистрирован, но никогда не получал обновлений |
| N (максимальный) | Актуальная реплика, может стать лидером |
| < max | Отставшая реплика, нужен recovery перед лидерством |
| Ключ `{name}_recovering` | Реплика сейчас в процессе recovery |

### canBecomeLeader()

```java
// ShardTerms.java
public boolean canBecomeLeader(String coreNodeName) {
    Long myTerm = terms.get(coreNodeName);
    if (myTerm == null) return false;       // не зарегистрирован
    if (isRecovering(coreNodeName)) return false;  // в recovery

    // Проверить, нет ли реплики с более высоким term
    long maxTerm = terms.values().stream().mapToLong(Long::longValue).max().orElse(0);
    return myTerm >= maxTerm;
}
```

### Обновление terms — CAS через ZK version

```java
// ZkShardTerms.mutate()
private void mutate(Function<ShardTerms, ShardTerms> action) {
    while ((newTerms = action.apply(terms.get())) != null) {
        // CAS: setData с version-проверкой
        // Если version не совпадает (concurrent update) → refreshTerms() → повторить
        if (forceSaveTerms(newTerms)) break;
    }
}

private boolean saveTerms(ShardTerms newTerms) {
    Stat stat = zkClient.setData(znodePath,
        Utils.toJSON(newTerms),
        newTerms.getVersion(),   // ← версия для CAS
        true);
    // BadVersionException → перечитать из ZK → повторить
}
```

### Жизненный цикл terms

```
Replica регистрируется:
  registerTerm(coreNodeName)
    → terms["core_node3"] = 0  (если ещё не было)

Реплика начинает recovery:
  startRecovering(coreNodeName)
    → добавить ключ "core_node3_recovering" = 1

Лидер видит, что реплика отстала → повышает свой term:
  ensureTermsIsHigher(leader, {laggingReplicas})
    → leader.term++, replicasNeedingRecovery.term остаётся

Реплика догнала лидера:
  doneRecovering(coreNodeName)
    → setTermEqualsToLeader(coreNodeName)
    → удалить ключ "core_node3_recovering"

При смене лидера (победитель с setTermToMax):
  setTermEqualsToLeader(coreNodeName)
    → terms["core_node3"] = max(всех terms)
```

### Watcher на terms-узел

```java
private void registerWatcher() {
    Watcher watcher = event -> {
        if (EventType.None == event.getType()) return;
        retryRegisterWatcher();   // переставить watch
        refreshTerms();           // обновить кэш
        // Оповестить всех CoreTermWatcher listeners
    };
    zkClient.exists(znodePath, watcher, true);
}
```

`ZkShardTerms` хранит terms в памяти (`AtomicReference<ShardTerms>`) и автоматически обновляет при изменении ZK-узла. Watcher переустанавливается после каждого срабатывания.

### skipSendingUpdatesTo()

```java
public boolean skipSendingUpdatesTo(String coreNodeName) {
    return !terms.get().haveHighestTermValue(coreNodeName);
}
```

Лидер использует этот метод при рассылке обновлений: реплики с устаревшим term **не получают** обновления. Это предотвращает отправку обновлений репликам, которые ещё в recovery и не могут их корректно применить.

---

## 6. ShardLeaderElectionContext: полный процесс выборов лидера шарда

**Файл:** `solr/core/src/java/org/apache/solr/cloud/ShardLeaderElectionContext.java`

`runLeaderProcess()` — самый сложный метод в election-стеке. Вызывается когда `LeaderElector` определил, что мы победили (у нас наименьший seq-номер).

### Полный алгоритм runLeaderProcess()

```
runLeaderProcess(weAreReplacement, pauseBeforeStart):

┌─────────────────────────────────────────────────────────────────────┐
│ Шаг 1: Throttle guard                                               │
│   lt.minimumWaitBetweenActions()  ← не чаще N раз в секунду        │
│   lt.markAttemptingAction()                                         │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│ Шаг 2: Если несколько реплик → очистить лидера в cluster state     │
│   Отправить в Overseer:                                             │
│   {"operation":"leader", "shard":"shard1", "collection":"mycoll"}   │
│   (без leader-свойств = сброс текущего лидера)                      │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│ Шаг 3: Ожидание других кандидатов                                   │
│                                                                      │
│   if (!weAreReplacement):                                           │
│     waitForReplicasToComeUp(leaderVoteWait=180s)                    │
│     Ждать пока количество узлов в /election/ ≥                      │
│     количеству NRT+TLOG реплик в cluster state.                     │
│     Зачем: дать всем репликам шанс зарегистрироваться              │
│     перед тем, как кто-то займёт лидерство                         │
│                                                                      │
│   else (weAreReplacement=true):                                     │
│     areAllReplicasParticipating()                                   │
│     Быстрая проверка без ожидания                                   │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│ Шаг 4: Проверка shard terms                                         │
│                                                                      │
│   zkShardTerms = zkController.getShardTerms(collection, shardId)   │
│                                                                      │
│   if (registered && !canBecomeLeader(coreNodeName)):                │
│     // Мы зарегистрированы с term, но term не максимальный          │
│                                                                      │
│     waitForEligibleBecomeLeaderAfterTimeout(                        │
│         zkShardTerms, coreNodeName, leaderVoteWait):                │
│       // Ждать leaderVoteWait мс, проверяя каждые 500мс:            │
│       // появились ли в /election/ реплики с более высоким term?    │
│                                                                      │
│       if (replicasWithHigherTermParticipated()):                    │
│         return false  → rejoinLeaderElection()                     │
│         // Есть лучший кандидат → выйти из выборов, уйти в recovery │
│                                                                      │
│       if (timeout && никого лучше нет):                             │
│         return true, setTermToMax = true                            │
│         // ПРЕДУПРЕЖДЕНИЕ: потенциальная потеря данных!             │
│         // Вынуждены принять лидерство т.к. некому больше           │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│ Шаг 5: Отмена текущего recovery (если было запущено)               │
│   core.getUpdateHandler().getSolrCoreState().cancelRecovery()       │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│ Шаг 6: Пауза если weAreReplacement                                  │
│   Thread.sleep(2500ms)                                              │
│   Ждать завершения "плавающих" обновлений,                          │
│   которые могли отправляться к старому лидеру                       │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│ Шаг 7: SyncStrategy.sync() — синхронизация с другими репликами     │
│                                                                      │
│   result = syncStrategy.sync(zkController, core, leaderProps,      │
│                               weAreReplacement)                     │
│   // PeerSync со всеми живыми NRT репликами шарда                   │
│   // Результат: success=true если мы актуальны                      │
│                                                                      │
│   if (!success && !hasRecentUpdates):                               │
│     // У нас нет данных, проверить есть ли у других                 │
│     if (otherHasVersions) → success = false (не можем лидировать)  │
│     else → success = true (никто не имеет данных — лидируем)       │
│                                                                      │
│   if (!success):                                                    │
│     rejoinLeaderElection(core)   ← идти в recovery и повторить     │
│     return                                                           │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│ Шаг 8: Специальная обработка TLOG-реплики                           │
│                                                                      │
│   if (replicaType == TLOG && weAreReplacement):                     │
│     zkController.stopReplicationFromLeader(coreName)                │
│     // TLOG реплика становится лидером → нужно применить           │
│     // накопленный tlog к индексу                                   │
│     future = core.getUpdateHandler().getUpdateLog()                 │
│                  .recoverFromCurrentLog()                            │
│     future.get()  // ждём завершения                                │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│ Шаг 9: Если setTermToMax (победили после таймаута)                  │
│   log.error("WARNING: Potential data loss...")                      │
│   zkShardTerms.setTermEqualsToLeader(coreNodeName)                  │
│   // Принудительно выровнять term — иначе term будет ниже           │
│   // чем нужен и другие реплики не получат обновления              │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│ Шаг 10: Записать лидера в ZK                                        │
│   super.runLeaderProcess() →                                        │
│   ShardLeaderElectionContextBase.runLeaderProcess()                 │
│   // Создать /collections/{coll}/leaders/{shard}/leader (EPHEMERAL) │
│   // Уведомить Overseer о смене лидера                              │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│ Шаг 11: Опубликовать ACTIVE                                         │
│   core.getCoreDescriptor().getCloudDescriptor().setLeader(true)    │
│   publishActiveIfRegisteredAndNotActive(core)                       │
│   // zkController.publish(cd, Replica.State.ACTIVE)                 │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│ Шаг 12: Запросить recovery у отстающих реплик                       │
│   syncStrategy.requestRecoveries()                                  │
│   // HTTP-запросы к репликам с более низким term                    │
└─────────────────────────────────────────────────────────────────────┘
```

### rejoinLeaderElection() — когда мы не подходим

```java
private void rejoinLeaderElection(SolrCore core) {
    log.info("There may be a better leader candidate than us - going back into recovery");

    // 1. Удалить наш ephemeral узел из очереди
    cancelElection();

    // 2. Запустить recovery (станем репликой, догоним нового лидера)
    core.getUpdateHandler().getSolrCoreState()
        .doRecovery(cc, core.getCoreDescriptor());

    // 3. Снова вступить в очередь
    leaderElector.joinElection(this, true);
    // После recovery, когда станем актуальны, checkIfIamLeader снова вызовет runLeaderProcess
}
```

---

## 7. ShardLeaderElectionContextBase: запись лидера в ZK

**Файл:** `solr/core/src/java/org/apache/solr/cloud/ShardLeaderElectionContextBase.java`

Базовый класс содержит ZK-механику записи лидера. `runLeaderProcess()` здесь выполняется после всех проверок в `ShardLeaderElectionContext`.

### Атомарная регистрация лидера

```java
void runLeaderProcess(boolean weAreReplacement, int pauseBeforeStartMs) {

    String parent = getZkParent(leaderPath);
    // parent = /collections/{coll}/leaders/{shard}
    // leaderPath = /collections/{coll}/leaders/{shard}/leader

    RetryUtil.retryOnException(NodeExistsException.class, 60000, 5000, () -> {
        synchronized (lock) {
            // Атомарная multi-операция:
            List<Op> ops = List.of(
                // 1. Убедиться, что наш election-узел ещё существует
                Op.check(leaderSeqPath, -1),
                // 2. Создать /leaders/{shard}/leader (EPHEMERAL)
                Op.create(leaderPath, Utils.toJSON(leaderProps),
                          acls, CreateMode.EPHEMERAL),
                // 3. Инкрементировать версию родителя (для cancelElection CAS)
                Op.setData(parent, null, -1)
            );

            List<OpResult> results = zkClient.multi(ops, true);

            // Запомнить версию родителя (нужна для cancelElection)
            for (OpResult result : results) {
                if (result.getType() == OpCode.setData) {
                    leaderZkNodeParentVersion = ((SetDataResult) result).getStat().getVersion();
                }
            }
        }
    });
    // NodeExistsException: предыдущий leader-узел ещё не удалён → повторить через 5с, до 60с
```

**Зачем три операции в multi?**
1. `check(leaderSeqPath)` — гарантирует что мы ещё в очереди (сессия не истекла пока мы ждали)
2. `create(leaderPath)` — создать ephemeral leader-узел
3. `setData(parent)` — инкрементировать version у родителя для безопасного cancelElection

### cancelElection() — безопасное удаление лидера

```java
public void cancelElection() {
    synchronized (lock) {
        if (leaderZkNodeParentVersion != null) {
            // Атомарно: проверить версию родителя + удалить leader-узел
            // Если версия изменилась (другой лидер зарегистрировался) → не удалять
            List<Op> ops = List.of(
                Op.check(parent, leaderZkNodeParentVersion),
                Op.delete(leaderPath, -1)
            );
            zkClient.multi(ops, true);
            leaderZkNodeParentVersion = null;
        }
    }
}
```

Это предотвращает удаление `leader`-узла нового лидера старым лидером в момент shutdown.

### Уведомление Overseer о лидере

После создания `/leaders/{shard}/leader` отправляется сообщение в Overseer (или прямо в ZK при distributed state updates):

```java
ZkNodeProps m = new ZkNodeProps(
    "operation", "leader",
    "shard",     shardId,
    "collection", collection,
    "node_name", nodeName,
    "base_url",  baseUrl,
    "core",      coreName,
    "state",     "active"
);
// → SliceMutator.setShardLeader() → обновить state.json
```

Для PRS-коллекций дополнительно:
```java
PerReplicaStatesOps.flipLeader(replicaNames, id, prs)
    .persist(collZNode, zkClient);
// Атомарно пометить нашу реплику как leader=true,
// остальные — leader=false
```

---

## 8. SyncStrategy: синхронизация перед принятием лидерства

**Файл:** `solr/core/src/java/org/apache/solr/cloud/SyncStrategy.java`

Перед тем как объявить себя лидером, кандидат синхронизируется с другими живыми репликами шарда через PeerSync. Это гарантирует, что новый лидер имеет все обновления которые были у предыдущего.

### sync()

```java
PeerSync.PeerSyncResult sync(ZkController zkController, SolrCore core,
                              ZkNodeProps leaderProps, boolean weAreReplacement) {

    // 1. Собрать URL всех живых NRT-реплик шарда (кроме себя)
    List<String> replicas = getReplicaUrls(zkController, collection, shard);

    if (replicas.isEmpty()) {
        return PeerSyncResult.success();  // некого синхронизировать
    }

    // 2. Запустить PeerSync
    // doFingerprint=true, onlyIfActive=false (реплики могут быть RECOVERING)
    PeerSync peerSync = new PeerSync(core, replicas, nUpdates,
        cantReachIsSuccess=true,   // если реплика недоступна → считать успехом
        onlyIfActive=false,
        doFingerprint=true);

    PeerSyncResult result = peerSync.sync();
    if (result.isSuccess()) return result;

    // 3. Если PeerSync не удался → попробовать PeerSync с самими репликами
    // (они могут знать о версиях которых нет у нас)
    return result;
}
```

### requestRecoveries() — инициировать recovery на отставших

После того как лидер установлен:
```java
public void requestRecoveries() {
    // HTTP-запросы к репликам с более низким term
    for (String replicaUrl : replicasNeedingRecovery) {
        // POST /admin/cores?action=REQUESTRECOVERY&core={coreName}
        requestRecovery(replicaUrl);
    }
}
```

---

## 9. OverseerElectionContext: выборы Overseer

**Файл:** `solr/core/src/java/org/apache/solr/cloud/OverseerElectionContext.java`

Аналогичный контекст но для Overseer.

### ZK-структура

```
/overseer_elect/election/
    {sesId}-{nodeName}-n_0000000001   EPHEMERAL_SEQUENTIAL  ← Overseer
    {sesId}-{nodeName}-n_0000000002   EPHEMERAL_SEQUENTIAL  ← резервный

/overseer_elect/leader                EPHEMERAL
    {"id": "{sesId}-{nodeName}-n_0000000001"}
```

### runLeaderProcess()

```java
void runLeaderProcess(boolean weAreReplacement, int pauseBeforeStart) {
    // 1. Создать /overseer_elect/leader (EPHEMERAL) с нашим ID
    zkClient.makePath(OVERSEER_ELECT + "/leader",
        Utils.toJSON(Map.of("id", myId)),
        CreateMode.EPHEMERAL, true);

    // 2. Запустить Overseer
    overseer.start(myId);
    // → запустить ClusterStateUpdater поток
    // → запустить OverseerCollectionConfigSetProcessor
}
```

### amILeader() — периодическая проверка

`ClusterStateUpdater.run()` каждую итерацию проверяет лидерство:
```java
private LeaderStatus amILeader() {
    ZkNodeProps props = ZkNodeProps.load(
        zkClient.getData(OVERSEER_ELECT + "/leader", null, null, true));
    String id = props.getStr("id");

    if (myId.equals(id)) return LeaderStatus.YES;
    return LeaderStatus.NO;
    // DONT_KNOW при ConnectionLossException
}
```

Если возвращается `NO` → цикл завершается, `checkIfIamStillLeader()` в отдельном потоке:
1. Прочитать `/overseer_elect/leader`
2. Если там наш ID → **удалить** его (освободить лидерство)
3. Перезарегистрироваться через `rejoinOverseerElection()`

---

## 10. Регистрация реплики в выборах: joinElection()

В `ZkController.register()` вызов `joinElection()` происходит на шаге 3:

```java
// ZkController.java
private void joinElection(CoreDescriptor cd, boolean afterExpiration) {
    String collection = cd.getCloudDescriptor().getCollectionName();
    String shardId = cd.getCloudDescriptor().getShardId();
    String coreNodeName = cd.getCloudDescriptor().getCoreNodeName();

    // Создать ElectionContext для этой реплики
    ZkNodeProps leaderProps = new ZkNodeProps(
        ZkStateReader.CORE_NAME_PROP, coreName,
        ZkStateReader.NODE_NAME_PROP, zkController.getNodeName(),
        ZkStateReader.BASE_URL_PROP, baseUrl,
        ZkStateReader.COLLECTION_PROP, collection,
        ZkStateReader.SHARD_ID_PROP, shardId,
        ZkStateReader.CORE_NODE_NAME_PROP, coreNodeName,
        ZkStateReader.REPLICA_TYPE, replicaType.toString()
    );

    ShardLeaderElectionContext context = new ShardLeaderElectionContext(
        leaderElector, shardId, collection, coreNodeName,
        leaderProps, zkController, cc);

    leaderElector.setup(context);    // создать /election/ директорию если нет
    leaderElector.joinElection(context, afterExpiration);
}
```

**PULL-реплики не вступают в выборы** — они не могут стать лидером:
```java
// ZkController.register()
if (replicaType != Replica.Type.PULL) {
    joinElection(cd, recoveringAfterStartup);
}
```

---

## 11. Обрыв сессии и повторные выборы

ZK-сессия истекает если нода не отвечает дольше `sessionTimeout` (обычно 30–60 сек). Все EPHEMERAL узлы удаляются, включая election-узлы.

### beforeReconnect() — до нового соединения

```java
// ZkController.java
private void beforeReconnect(Supplier<List<CoreDescriptor>> descriptors) {
    // 1. Остановить Overseer (если были)
    overseer.close();

    // 2. Отменить все election contexts
    closeOutstandingElections(descriptors);
    // → для каждой CoreDescriptor: electionContext.cancelElection()
    // (удаляет leaderSeqPath если он ещё жив; при expired сессии это no-op)

    // 3. Снять флаги лидерства
    markAllAsNotLeader(descriptors);
    // → cd.getCloudDescriptor().setLeader(false) для всех cores
}
```

### onReconnect() — после нового соединения

```java
private void onReconnect(Supplier<List<CoreDescriptor>> descriptors) {
    clearZkCollectionTerms();
    zkStateReader.createClusterStateWatchersAndUpdate();

    // Пересоздать live_node с новой сессией
    createEphemeralLiveNode();

    // Перезапустить Overseer-выборы
    if (!zkRunOnly) {
        overseerElector.setup(new OverseerElectionContext(...));
        overseerElector.joinElection(context, true);  // replacement=true
    }

    // Перерегистрировать все cores (включая joinElection для шардов)
    for (CoreDescriptor cd : descriptors.get()) {
        recoveryExecutor.submit(new RegisterCoreAsync(cd, true, afterExpiration=true));
        // → ZkController.register() → joinElection() → ...
    }
}
```

**Ключевой момент:** при повторной регистрации после истечения сессии все seq-номера начинаются заново — старые ephemeral узлы удалены. Новые узлы получат новые seq-номера. Это нормально — election будет проведена заново.

---

## 12. Граничные случаи и защитные механизмы

### 12.1. Дубликаты в election-очереди

**Проблема:** ConnectionLossException при `joinElection()` — не знаем, создался ли узел.

**Решение:**
```java
// joinElection(): ConnectionLoss handling
List<String> entries = zkClient.getChildren(electionPath);
boolean foundId = entries.stream()
    .anyMatch(e -> getNodeId(e).equals(id));
if (foundId) cont = false;  // узел уже создан, не создавать снова
```

Дополнительно в `checkIfIamLeader()`:
```java
// Если в очереди несколько узлов с нашим session+coreNodeName → удалить все кроме последнего
String prefix = sessionId + "-" + context.id + "-";
for (String node : seqs) {
    if (!node.equals(myNode) && node.startsWith(prefix)) {
        zkClient.delete(node, -1, true);
    }
}
```

### 12.2. "Split-brain" — два лидера одновременно

**Ситуация:** старый лидер завис (GC pause), ZK посчитал его мёртвым, выбрал нового. Старый "воскрес" и думает что он лидер.

**Защита через leader_path:**
- Новый лидер создал `/leaders/{shard}/leader` с EPHEMERAL
- Старый лидер при попытке записать лидера получит `NodeExistsException`
- Его сессия истекла → все его EPHEMERAL узлы уже удалены

**Защита через leaderZkNodeParentVersion в cancelElection():**
- Каждая регистрация лидера инкрементирует версию родителя
- `cancelElection()` проверяет версию через `Op.check(parent, version)` — если версия изменилась (новый лидер уже зарегистрировался), старый лидер не может удалить нового

### 12.3. Лидер с устаревшим индексом

**Защита через terms:**
1. Перед принятием лидерства: `canBecomeLeader()` проверяет что наш term максимальный
2. Если нет — `waitForEligibleBecomeLeaderAfterTimeout()` ждёт `leaderVoteWait`
3. Если за это время появилась реплика с более высоким term → `rejoinLeaderElection()`
4. Если таймаут → принимаем лидерство с предупреждением (потенциальная потеря данных)

### 12.4. Зависание в SyncStrategy.sync()

`cantReachIsSuccess=true` в `PeerSync` означает: если реплика недоступна по сети — это не ошибка синхронизации. Это важно: реплика могла упасть раньше и не иметь наших последних обновлений.

### 12.5. Race между cancelElection и runLeaderProcess

`ShardLeaderElectionContextBase` использует `synchronized (lock)` в обоих `cancelElection()` и `runLeaderProcess()`. Это предотвращает ситуацию когда:
- Поток 1: выполняет `runLeaderProcess()` → только что создал `/leaders/{shard}/leader`
- Поток 2: вызывает `cancelElection()` → должен его удалить

Без синхронизации возможна ситуация когда `cancelElection()` читает `leaderZkNodeParentVersion = null` (ещё не установлено в `runLeaderProcess()`) и не удаляет leader-узел.

### 12.6. leaderVoteWait таймаут

```java
// waitForReplicasToComeUp():
if (System.nanoTime() > timeoutAt) {
    log.info("Was waiting for replicas to come up, but they are taking too long " +
             "- assuming they won't come back till later");
    return false;
    // false = не все реплики участвуют, но продолжаем (не ждать вечно)
}
```

При таймауте лидерство принимается даже если не все реплики успели зарегистрироваться. Реплики, которые поднимутся позже, пройдут recovery.

---

## 13. Полная последовательность: от падения лидера до нового ACTIVE

```
t=0    ЛИДЕР (node1, shard1) падает — ZK-сессия истекает
         │
         │ (sessionTimeout = 30сек, ZK ждёт)
         │
t=30s  ZK удаляет все EPHEMERAL узлы node1:
         /live_nodes/node1:8983_solr
         /collections/mycoll/leader_elect/shard1/election/
             {sesId_node1}-core_node1-n_0000000001   ← удалён!
         /collections/mycoll/leaders/shard1/leader   ← удалён!
         │
         ▼
       ElectionWatcher на node2 срабатывает
       (node2 следил за n_0000000001 — предшественником)
         │
         ▼
       checkIfIamLeader(context, replacement=true)
         │ Получить список: [core_node2-n_0000000002, core_node3-n_0000000003]
         │ Наш n_0000000002 == seqs[0] → МЫ ЛИДЕР
         │
         ▼
       runLeaderProcess(weAreReplacement=true)

         Шаг 2: Очистить лидера в state.json через Overseer
         Шаг 3: areAllReplicasParticipating() → быстрая проверка
         Шаг 4: canBecomeLeader("core_node2")
                terms = {"core_node1":5, "core_node2":5, "core_node3":4}
                term[core_node2]=5 == max(5) → ДА, можем
         Шаг 5: cancelRecovery()
         Шаг 6: sleep(2500ms)
         Шаг 7: SyncStrategy.sync()
                PeerSync с [node3] → cantReachIsSuccess=true
                Fingerprint совпадает → SUCCESS
         Шаг 8: replicaType=NRT → пропустить
         Шаг 9: setTermToMax=false → пропустить
         Шаг 10: ShardLeaderElectionContextBase.runLeaderProcess()
                 ZK multi([
                   check(leaderSeqPath),
                   create(/leaders/shard1/leader, {node2 props}, EPHEMERAL),
                   setData(/collections/mycoll/leaders/shard1, null, -1)
                 ])
                 Послать Overseer: {"operation":"leader","shard":"shard1",...}
         Шаг 11: core.cd.setLeader(true)
                 zkController.publish(ACTIVE)
                 → /overseer/queue: {"operation":"state","state":"active",...}
         Шаг 12: syncStrategy.requestRecoveries()
                 → core_node3 имеет term=4 < max(5)
                 → POST /admin/cores?action=REQUESTRECOVERY&core=... к node3

t=35s  НОВЫЙ ЛИДЕР: node2, shard1
         /collections/mycoll/leaders/shard1/leader = {node2 props}
         state.json: shard1.replicas.core_node2.leader=true

t=36s  node3 получает REQUESTRECOVERY
         → RecoveryStrategy запускается
         → PeerSyncWithLeader с node2
         → doneRecovering("core_node3")
         → terms: {"core_node2":5, "core_node3":5}
         → publish(ACTIVE)
```

---

## 14. Ключевые классы и файлы

| Класс | Файл | Роль |
|---|---|---|
| `LeaderElector` | `cloud/LeaderElector.java` | ZK-рецепт: создание seq-узлов, watch на предшественника, sortSeqs |
| `ElectionContext` | `cloud/ElectionContext.java` | Абстракция: id, paths, cancelElection(), runLeaderProcess() |
| `ShardLeaderElectionContextBase` | `cloud/ShardLeaderElectionContextBase.java` | ZK-механика: создание /leaders/{shard}/leader через multi() |
| `ShardLeaderElectionContext` | `cloud/ShardLeaderElectionContext.java` | Бизнес-логика: terms-проверка, SyncStrategy, TLOG-обработка |
| `OverseerElectionContext` | `cloud/OverseerElectionContext.java` | Запускает Overseer.start() при победе |
| `ZkShardTerms` | `cloud/ZkShardTerms.java` | Terms-узел в ZK: CAS-обновление, watcher, canBecomeLeader() |
| `ShardTerms` | `solrj/.../ShardTerms.java` | Immutable data class: Map\<coreNodeName, Long\> + логика сравнения |
| `SyncStrategy` | `cloud/SyncStrategy.java` | PeerSync перед принятием лидерства + requestRecoveries() |
| `ZkController` | `cloud/ZkController.java` | Оркестрирует joinElection(), onReconnect(), beforeReconnect() |

---

## 15. Параметры и константы

| Параметр | Значение | Источник | Описание |
|---|---|---|---|
| `leaderVoteWait` | 180 000 мс | `cloudConfig.getLeaderVoteWait()` | Ожидание всех реплик в election очереди и кандидатов с высоким term |
| `leaderConflictResolveWait` | 10 000 мс | `cloudConfig.getLeaderConflictResolveWait()` | Таймаут `sendPrepRecoveryCmd()` в RecoveryStrategy |
| `joinAtHead` | false (обычно) | `joinElection(context, replacement, joinAtHead)` | Занять позицию сразу за лидером (ручное управление) |
| Pause при `weAreReplacement` | 2 500 мс | `ShardLeaderElectionContext` константа | Ожидание завершения обновлений после смены лидера |
| `NodeExistsException` retry | 60 000 мс / 5 000 мс | `RetryUtil.retryOnException(...)` | Ждать освобождения `/leaders/{shard}/leader` |
| Term = 0 | регистрация | `registerTerm()` | Начальное значение: нода зарегистрирована, данных нет |
| Term = max | лидер | `canBecomeLeader()` | Условие лидерства |
| `cantReachIsSuccess` | true | `SyncStrategy.sync()` | Недоступная реплика не блокирует принятие лидерства |
| `_recovering` suffix | строка | `isRecovering()` | Маркер recovery в terms-карте |
| Election path | `/collections/{c}/leader_elect/{s}` | `ShardLeaderElectionContextBase` | Путь election директории |
| Leader path | `/collections/{c}/leaders/{s}/leader` | `ZkStateReader.getShardLeadersPath()` | Путь записи лидера |
| Terms path | `/collections/{c}/terms/{s}` | `ZkShardTerms` | Путь terms-узла |
