# Design: add-write-load-balancing

## Context

Продукт пишет в головные коллекции, раскрытые алиасами с суффиксом `_WRITE`; ротация — CREATE новой коллекции + мгновенный свап алиаса. Удельная скорость записи на шард единая, поэтому единица write-нагрузки — одна реплика write-шарда. Прод-плагин — Simple (дефолт); AZ/spread_domain не используются. Мотивация — в `proposal.md` (Why).

Проверенные факты кода (ветка форка):

- Placement API не видит алиасы: `Cluster` (`solr/core/.../cluster/Cluster.java`) содержит только ноды и коллекции. Алиасы живут в `/aliases.json`, не в cluster state.
- Цепочка на Overseer-пути: `OverseerCollectionMessageHandler.cloudManager` → `Overseer.getSolrCloudManager()` → `ZkController.getSolrCloudManager()` (`ZkController.java:860`) → `SolrClientCloudManager(CloudLegacySolrClient(ZkClientClusterStateProvider(zkStateReader)))`. Полная карта алиасов доступна через `ZkStateReader.getAliases()` (`ZkStateReader.java:2132`).
- **Нюанс CREATE-пути**: `CreateCollectionCmd.wrapCloudManager` (`CreateCollectionCmd.java:546-572`) оборачивает провайдер в анонимный наследник `DelegatingClusterStateProvider`, подменяя только state-методы. Новый метод алиасов обязан делегироваться в `DelegatingClusterStateProvider`, иначе в момент CREATE (главный сценарий!) карта будет пустой.
- Точка аккумулирования per-node данных уже существует: `OrderedNodePlacementPlugin.getWeightedNodes` (`OrderedNodePlacementPlugin.java:353-362`) обходит **все** коллекции кластера, вызывая `initReplica → initReplicaWeights`. Прецедент канала per-коллекционных настроек — `placement.maxReplicasPerNode` (`OrderedNodePlacementPlugin.java:72`), считывается через `SolrCollection.getCustomProperty()`.
- Формула Simple (`SimplePlacementFactory.SameCollWeightedNode`): `replicas + 5·Σ(c−1)² + 1000·Σ(s−1)²`; `canAddReplica` переопределён и уже вызывает `withinMaxReplicasPerNode`.
- Апстрим-баг: `SimplePlacementFactory.java:181` — инкрементальный штраф за реплику того же шарда вычисляется от счётчика коллекции (`colReplicaCountWithout`) вместо `shardReplicaCountWithout`.

## Goals / Non-Goals

**Goals:**

- Инвариант bounded ramp: `write(нода) ≤ write(любая нода) + O(реплики одной операции)`.
- Побайтово прежнее поведение в кластерах без `_WRITE`-алиасов.
- Ноль обращений к `AttributeFetcher` (детерминизм, дешевизна, тестопригодность).
- Форма, пригодная для upstream-контрибуции.

**Non-Goals:**

- Взвешивание write-коллекций по реальной скорости (assumption: удельная скорость шарда единая).
- Отдельный вес лидера (дистрибуция/версии) — можно добавить позже множителем.
- Карантин/прогрев новых нод (PULL-fencing) — другой механизм, при необходимости поверх.
- Изменение поведения Affinity/MinimizeCores/Random — меняется только Simple и общая база.
- Пер-коллекционная конфигурация суффикса/множителей.

## Decisions

### 1. Канал write-набора — алиасы (не properties, не имена)

Истина о write-головах уже существует в `_WRITE`-алиасах; ротационный сервис меняет только их. Штамп `property.placement.write` (вариант B) дублирует истину, требует reconciliation и ломается при сбое между операциями; конвенция имён коллекций хрупка к дрейфу. Алиасы дают нулевую связность с продуктом и мгновенную самокоррекцию (свап мгновенный по условию).

### 2. Форма API-расширения и гейт

- `ClusterStateProvider` (solrj): `default Map<String, List<String>> getCollectionAliases()` → `Map.of()`. Переопределён в `ZkClientClusterStateProvider` (делегирует `zkStateReader.getAliases().getCollectionAliasListMap()`) и в `DelegatingClusterStateProvider` (делегирует обёрнутому провайдеру — критично из-за `wrapCloudManager`, см. Context).
- `Cluster.getCollectionAliases()` — поднято в `ClusterImpl` (захват в конструкторе из `solrCloudManager.getClusterStateProvider()`).
- **Гейт**: механизм активен ⇔ существует ≥1 алиас с суффиксом `_WRITE` (стандартного, не routed типа). Без гейта write-инкременты проекций меняли бы размещения всех CREATE во всех кластерах — нарушено бы требование обратной совместимости.
- Write-набор: `isWrite(coll) = gate ∧ (coll отсутствует в cluster state ∨ coll ∈ ∪ коллекций алиасов *_WRITE)`. Отсутствие в state покрывает CREATE/SPLIT/RESTORE («новая коллекция всегда write» — правило продукта); PULL-реплики не считаются и не инкрементируют (write-поток на них не приходит).

*Альтернативы*: читать `/aliases.json` напрямую из плагина через `DistribStateManager` (минуя API — хрупко, мимо тестовой инфраструктуры); узкий метод `Cluster.getWriteCollections(suffix)` (меньше поверхность, но не переиспользуемо и хуже для upstream).

### 3. Весовая модель Simple

```
weight(node)     = existingReplicas + 5·Σ(c−1)² + WRITE_MULT · writeReplicas(node)
relevant(node,r) = weight(node) + 1 + 5·c + [isWrite(r) ∧ type(r) ∈ {NRT,TLOG}] · WRITE_MULT
WRITE_MULT = 10 000
```

- Член `WRITE_MULT · writeReplicas(node)` — часть **веса ноды**, действует для всех placement-решений, включая read-only размещения (единая «эффективная загрузка»: read-реплики уходят на write-лёгкие ноды) — это требование спецификации, и оно не требует отдельного кода.
- Инкремент при проекции — только для write-реплик.
- `WRITE_MULT = 10 000`: выше любого правдоподобного разброса cores/коллекционных штрафов (~10³), безопасен, потому что стекинг реплик шарда запрещён жёстко (решение 4).

*Альтернативы*: лексикографический ключ (primary write, secondary simple-weight) — отвергнут: фреймворк сравнивает одиночный int, а смешивание с TreeSet-сортировкой и tie-логикой `NodeHeap` ломается изящнее через аддитивный член; clamp `K·min(w,CAP)` — нужен только при мягкой анти-аффинити, см. ниже.

### 4. Жёсткая анти-аффинити в Simple — только при включённом write-балансе

`canAddReplica` вызывает `super` (базовая проверка «нет второй реплики шарда на ноде» + `maxReplicasPerNode`) **только когда write-балансировка включена** (есть `*_WRITE`-алиасы). Причина — ловушка RF=2 во время рампа: при перекосе write-счётчиков (пустая нода 0 vs 60) выигрыш write-баланса от стекинга обеих реплик шарда (`WRITE_MULT · 60 = 600 000`) превышал бы мягкий штраф `1000`, и плагин складывал бы обе реплики на пустую ноду. Без алиасов сохраняется историческое поведение Simple (мягкий штраф, стекинг как последний резерв) — требование спецификации об идентичности поведения без `_WRITE`-алиасов включает и вырожденные кластеры (например, одиночная нода с RF=2, как в `HttpClusterStateSSLTest`). Уточнение внесено по результату имплементации: первоначальный вариант с безусловным `super` нарушал требование обратной совместимости.

`SAME_SHARD_MULT` остаётся живым кодом при выключенном механизме и мёртвым при включённом; комментарий в коде документирует это. При включённом механизме поведение деградирует как у остальных трёх встроенных плагинов: в вырожденном кластере (некуда класть) — `PlacementException` вместо стекинга.

### 5. Точки врезки в плагине

- Write-set вычисляется один раз на вызов `computePlacements`/`computeBalancing` — в `SimplePlacementPlugin.getBaseWeightedNodes` из `placementContext.getCluster().getCollectionAliases()` (+ проверка наличия коллекции в state для гейта CREATE-случая).
- `SameCollWeightedNode` получает write-set через конструктор; поля `writeReplicas` (int) и ссылка на write-set. Аккумулирование — в `initReplicaWeights`/`addProjectedReplicaWeights`/`removeProjectedReplicaWeights` (существующие хуки; цикл по всем коллекциям уже в базе). `calcWeight`/`calcRelevantWeightWithReplica` включают член по формуле из решения 3.
- `computeBalancing` не меняется: вес нод уже включает write-член → `BALANCE_REPLICAS` переносит write-реплики с тяжёлых нод. Спец-кейсы не вводятся (балансировка — операторская, запускается в спокойное окно; перенос горячей головы дорог — копирование при индексации, поведение как сегодня для остальных реплик).

### 6. Суффикс — константа

`public static final String WRITE_ALIAS_SUFFIX = "_WRITE"` в `OrderedNodePlacementPlugin` (рядом с `MAX_REPLICAS_PER_NODE_PROPERTY`). Конфигурация потребовала бы конфиг у Simple-фабрики — ломающее изменение сигнатуры `NoConfig`-фабрики (тот же аргумент, что для `maxReplicasPerNode` в `PLACEMENT-PLUGINS.MD`).

### 7. Фикс апстрим-бага — отдельным коммитом

`SimplePlacementFactory.java:181`: `SAME_SHARD_MULT * (colReplicaCountWithout * 2 - 1)` → `shardReplicaCountWithout`. Чинится до врезки write-члена, отдельным коммитом внутри change — чистая история для upstream (баг независим от фичи, заметен в инкрементальном весе при существующем стекинге реплик шарда).

## Risks / Trade-offs

- [Запаздывание обзора кластера под нагрузкой (документировано в `SimpleClusterAbstractionsImpl` — до ~минуты)] → ошибка write-карты ограничена числом concurrently ротирующихся голов; свап мгновенный; следующее placement-вычисление самокорректируется.
- [Жёсткая анти-аффинити: placement падает там, где раньше стекалось] → соответствует семантике трёх остальных плагинов; для прод-топологии (RF=2, много нод) недостижимо; сообщение об ошибке уже улучшено работой `maxReplicasPerNode`.
- [Анонимные обёртки провайдера «забывают» новый метод] → делегирование заложено в `DelegatingClusterStateProvider` + тест, покрывающий именно CREATE-путь (мимо wrapCloudManager алиасы не должны теряться).
- [Routed-алиас с суффиксом `_WRITE`] → при построении write-набора фильтровать по типу алиаса (только standard collection aliases).
- [Новая нода всё же получает write до выравнивания (bounded ramp ≠ карантин)] → осознанный выбор (см. эксплорацию): перегруз ограничен `+O(1)` к максимуму кластера против 3–5× сегодня.
- [Upstream-приёмка] → дефолт-метод на интерфейсе обратно совместим; гейт сохраняет поведение всех существующих пользователей.

## Migration Plan

1. Деплой сборки форка на кластер. Плагин не перерегистрируется (Simple — дефолт).
2. Механизм включается автоматически при обнаружении `_WRITE`-алиасов — в проде сразу. Существующие размещения не переносятся; инвариант устанавливается на новых размещениях (ротации быстрые, равновесие достигается само).
3. Откат — откат сборки; состояние кластера не менялось (никаких миграций данных/форматов).
