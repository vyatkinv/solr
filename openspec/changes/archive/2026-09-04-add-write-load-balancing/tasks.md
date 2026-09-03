# Tasks: add-write-load-balancing

## 1. Фикс апстрим-бага (отдельный коммит)

- [x] 1.1 Исправить `SimplePlacementFactory.addedWeightOfAdditionalReplica`: инкрементальный штраф за реплику того же шарда — от `shardReplicaCountWithout`, а не `colReplicaCountWithout`
- [x] 1.2 Регрессионный тест: инкрементально накопленный вес ноды с существующим стеком реплик одного шарда равен весу, вычисленному по формуле `replicas + 5·Σ(c−1)² + 1000·Σ(s−1)²`

## 2. Доступ к алиасам в placement API

- [x] 2.1 Добавить `default Map<String, List<String>> getCollectionAliases()` (→ `Map.of()`) в `ClusterStateProvider` (solrj)
- [x] 2.2 Переопределить в `ZkClientClusterStateProvider`: делегирование `zkStateReader.getAliases()` (уточнить точный аксессор карты стандартных алиасов в `Aliases`)
- [x] 2.3 Переопределить в `DelegatingClusterStateProvider` с делегированием обёрнутому провайдеру (критично для CREATE-пути — `CreateCollectionCmd.wrapCloudManager`)
- [x] 2.4 Добавить `Cluster.getCollectionAliases()` в интерфейс `Cluster` и захват карты в конструкторе `SimpleClusterAbstractionsImpl.ClusterImpl` из `solrCloudManager.getClusterStateProvider()`
- [x] 2.5 Расширить тестовые `Builders.ClusterBuilder`/placement context: поддержка карты алиасов в мокнутом кластере

## 3. Write-балансировка в Simple

- [x] 3.1 Константа `WRITE_ALIAS_SUFFIX = "_WRITE"` в `OrderedNodePlacementPlugin` (рядом с `MAX_REPLICAS_PER_NODE_PROPERTY`)
- [x] 3.2 В `SimplePlacementPlugin.getBaseWeightedNodes`: построить write-set из `placementContext.getCluster().getCollectionAliases()` — стандартные алиасы с суффиксом `_WRITE`; гейт «механизм активен ⇔ set-источник непуст»; правило write-ности: коллекция отсутствует в state (CREATE) ИЛИ входит в write-set
- [x] 3.3 `SameCollWeightedNode`: поле `writeReplicas`, инъекция write-set через конструктор; аккумулирование в `initReplicaWeights`/`addProjectedReplicaWeights`/`removeProjectedReplicaWeights` только для NRT/TLOG реплик write-коллекций
- [x] 3.4 Формула веса: `calcWeight` включает `WRITE_MULT · writeReplicas`; `calcRelevantWeightWithReplica` добавляет `WRITE_MULT` только для проектируемых write-реплик NRT/TLOG (`WRITE_MULT = 10 000`)
- [x] 3.5 `canAddReplica` → вызов `super` (жёсткий запрет второй реплики шарда на ноде); пометить `SAME_SHARD_MULT` комментарием о мёртвом коде

## 4. Тесты поведения

- [x] 4.1 Идентификация write-коллекций: CREATE → write; член `_WRITE`-алиаса → write; коллекция только в READ-алиасе → не write
- [x] 4.2 Bounded ramp: 6 нод + пустая, 3 сбалансированные головы, 12 микроколлекций 1×RF2 → разброс write-реплик по нодам ≤ 2 в каждый момент
- [x] 4.3 Ловушка RF=2: при большом перекосе write-счётчиков обе реплики шарда не стекаются на пустую ноду
- [x] 4.4 Приоритет ограничений: конфликт с `placement.maxReplicasPerNode` разрешается в пользу лимита
- [x] 4.5 Read-only ADDREPLICA предпочитает ноду с меньшим write-счётчиком (при большем total-счётчике)
- [x] 4.6 Ротация: голова вышла из `_WRITE`-алиаса → её реплики не считаются; следующее размещение возвращается на освободившуюся ноду
- [x] 4.7 Обратная совместимость: без `_WRITE`-алиасов размещения идентичны прежним (существующие тесты плагинов зелёные)
- [x] 4.8 Интеграционный smoke (MiniSolrCloudCluster): с существующим `_WRITE`-алиасом CREATE распределяет реплики равномерно (проверяет цепочку делегирования через `wrapCloudManager`)

## 5. Верификация

- [x] 5.1 Прогнать тесты затронутых модулей: `solrj`, `solrj-zookeeper`, `solr/core` (placement-пакет)
- [x] 5.2 Прогнать полный чек сборки для изменённых модулей (компиляция, precommit/форматирование)
