# PlacementPlugin-ы в Solr: обзор реализаций

Документ описывает существующие реализации `PlacementPlugin` (пакет
`org.apache.solr.cluster.placement.plugins`), их принцип работы, настройки и возможности.
Актуально для ветки `branch_9_5`.

## Общая архитектура

Плагин размещения отвечает на вопрос «на какие ноды поставить новые реплики» (метод
`computePlacements`) и «как перебалансировать существующие реплики» (метод `computeBalancing`).
Он вызывается Overseer-ом при операциях CREATE, ADDREPLICA, SPLITSHARD, RESTORE, MOVEREPLICA,
BALANCE_REPLICAS и т.д. через `PlacementPluginAssignStrategy`.

Ключевые интерфейсы:

- `PlacementPlugin` — сама логика размещения (`computePlacements`, `computeBalancing`,
  `verifyAllowedModification` — проверка допустимости удаления реплик/коллекций).
- `PlacementPluginFactory<T extends PlacementPluginConfig>` — фабрика, создающая инстанс плагина;
  конфигурируется JSON-объектом через cluster plugins API. Имя плагина в реестре фиксировано:
  `.placement-plugin` (`PlacementPluginFactory.PLUGIN_NAME`).
- `AttributeFetcher` / `AttributeValues` — получение метрик нод (число ядер, свободный диск),
  system properties нод и метрик реплик (размер индекса).

### Выбор плагина

`PlacementPluginFactoryLoader` определяет, какой плагин используется:

1. Если через `/api/cluster/plugin` зарегистрирован плагин с именем `.placement-plugin` — он и
   используется (обновляется на лету через listener реестра плагинов, обёртка
   `DelegatingPlacementPluginFactory`).
2. Иначе — системное свойство `solr.placementplugin.default` со значениями
   `simple` | `affinity` | `minimizecores` | `random`.
3. Иначе — по умолчанию `SimplePlacementFactory`.

Пример регистрации плагина с конфигом:

```bash
curl -X POST -H 'Content-type:application/json' -d '{
  "add": {
    "name": ".placement-plugin",
    "class": "org.apache.solr.cluster.placement.plugins.AffinityPlacementFactory",
    "config": { "minimalFreeDiskGB": 10, "prioritizedFreeDiskGB": 50 }
  }
}' http://localhost:8983/api/cluster/plugin
```

Удаление (возврат к дефолту): `{"remove": ".placement-plugin"}`.

## Базовый класс: OrderedNodePlacementPlugin

Все четыре встроенных плагина наследуются от `OrderedNodePlacementPlugin` и различаются только
тем, как они «взвешивают» ноды. Плагин-наследник реализует `getBaseWeightedNodes(...)`,
возвращающий для каждой ноды объект `WeightedNode` со следующим контрактом:

- `calcWeight()` — текущий «вес» ноды (чем меньше, тем привлекательнее нода для размещения);
- `calcRelevantWeightWithReplica(replica)` — проекция веса, если добавить данную реплику;
- `canAddReplica(replica)` — можно ли ставить реплику (базовая реализация запрещает вторую
  реплику того же шарда на одной ноде);
- `canRemoveReplicas(replicas)` — проверка ограничений при удалении;
- `addProjectedReplicaWeights` / `removeProjectedReplicaWeights` — инкрементальное обновление
  веса при (гипотетическом) добавлении/удалении реплики.

Алгоритм **размещения** (`computePlacements`): для каждого запроса и каждого
шарда/типа реплики строится min-heap нод по проецируемому весу; реплики по одной ставятся на
самую «лёгкую» подходящую ноду, вес обновляется, при необходимости куча пересортировывается.
Есть механика отложенных запросов: если на лучшую позицию претендует больше равных нод, чем
осталось реплик, запрос откладывается в конец очереди (чтобы несколько параллельных запросов
распределялись справедливее). Если подходящих нод не хватает — `PlacementException`.

Алгоритм **балансировки** (`computeBalancing`): ноды сортируются по весу; пока вес самой
«лёгкой» ноды меньше веса самой «тяжёлой», плагин пытается перенести реплику с тяжёлой на
лёгкую. Перенос принимается, если суммарный вес пары нод после переноса уменьшается (или хотя бы
не увеличивает вес более тяжёлой ноды). Результат — карта перемещений `Replica -> Node`.

Таким образом, чтобы написать свой плагин, обычно достаточно унаследоваться от
`OrderedNodePlacementPlugin` и определить собственную схему весов.

## 1. SimplePlacementFactory (по умолчанию)

**Класс:** `SimplePlacementFactory` / `SimplePlacementPlugin`
**Конфигурация:** нет (`PlacementPluginFactory.NoConfig`).

Реализует логику старого `LegacyAssignStrategy`: выбирает ноды с наименьшим числом cores, штрафуя
совместное размещение реплик одной коллекции и особенно одного шарда.

Вес ноды (`SameCollWeightedNode`) — сумма:

- число реплик на ноде;
- `5 × Σ по коллекциям (реплик коллекции на ноде − 1)²`;
- `1000 × Σ по шардам (реплик шарда на ноде − 1)²`.

Квадраты нужны, чтобы концентрация многих реплик одной коллекции/шарда на одной ноде штрафовалась
сильнее, чем то же количество, размазанное по разным коллекциям.

Особенность: **разрешает** несколько реплик одного шарда на одной ноде (`canAddReplica` всегда
`true`) — но со штрафом 1000× это происходит только когда других вариантов нет. Не запрашивает
никаких метрик у нод (не учитывает диск и уже существующие cores других коллекций, видимых только
через метрики) — считает только реплики, известные из состояния кластера.

## 2. MinimizeCoresPlacementFactory

**Класс:** `MinimizeCoresPlacementFactory`
**Конфигурация:** нет.

Учебный пример плагина (так помечен в javadoc). Вес ноды = число cores на ней (метрика
`NodeMetricImpl.NUM_CORES`, запрашивается через `AttributeFetcher`). Реплики просто ставятся на
ноды с наименьшим числом cores. Две реплики одного шарда на одну ноду не ставятся (базовое
поведение `canAddReplica`). Если метрику cores получить не удалось — `PlacementException`.

В отличие от Simple, учитывает реальное количество cores на ноде (включая не-cloud cores), но не
различает коллекции/шарды при взвешивании и не смотрит на диск.

## 3. RandomPlacementFactory

**Класс:** `RandomPlacementFactory`
**Конфигурация:** нет.

Случайное размещение: вес всех нод равен 0, а выбор среди равных делается случайным tiebreaker-ом
(`getTiebreaker()` возвращает случайное число, перегенерируемое после каждого добавления/удаления
реплики). Две реплики одного шарда на одной ноде запрещены. В тестах воспроизводим: seed берётся
из системного свойства `tests.seed`.

Используется в основном для тестов и как простейший пример.

## 4. AffinityPlacementFactory

**Класс:** `AffinityPlacementFactory` / `AffinityPlacementPlugin`
**Конфигурация:** `AffinityPlacementConfig` — единственный плагин с богатой конфигурацией.

Самый функциональный плагин, воспроизводит поведение старого Autoscaling-конфига: распределение
реплик шарда по зонам доступности, привязка типов реплик к типам нод, учёт свободного диска,
ко-локация коллекций. Минимизация cores на ноду — только после выполнения этих ограничений.

### Схема весов (AffinityNode)

Вес ноды — сумма (множители задают приоритет ограничений):

| Слагаемое | Вклад |
|---|---|
| число cores на ноде | ×1 |
| свободный диск < `prioritizedFreeDiskGB` | +100 |
| дисбаланс по spread domain: `Σ по коллекции/шарду (реплик в домене ноды − минимум по доменам)²` | ×10 000 |
| дисбаланс по AZ: `Σ по коллекции/шарду/типу реплики (реплик в AZ ноды − минимум по AZ)²` | ×1 000 000 |

Итоговый порядок приоритетов: (1) равномерность реплик шарда+типа по AZ, (2) равномерность реплик
шарда по spread domain, (3) не размещать там, где диска меньше `prioritizedFreeDiskGB`,
(4) минимум реплик на ноде. Информация о заполнении AZ/доменов разделяется между нодами через
общий `AffinityPlacementContext` на время одного вычисления.

`canAddReplica` дополнительно запрещает: вторую реплику шарда на ноде; неподдерживаемый нодой тип
реплики; ноду неправильного `node_type` для коллекции; ноду без реплик secondary-коллекции (для
`withCollection` / `withCollectionShards`); ноду, где после добавления свободный диск упадёт ниже
`minimalFreeDiskGB` (размер реплики оценивается по метрике `INDEX_SIZE_GB` лидера шарда).

`canRemoveReplicas` / `verifyDeleteCollection` защищают ко-локацию: нельзя удалить последнюю
реплику secondary-коллекции с ноды, где живут реплики primary-коллекции, и нельзя удалить
secondary-коллекцию, пока существует primary.

### System properties нод (задаются при старте ноды, `-D...`)

| Свойство | Назначение |
|---|---|
| `availability_zone` | Имя AZ ноды. Ноды без свойства попадают в общую псевдо-AZ. Балансировка по AZ включается, если AZ больше одной. |
| `spread_domain` | Домен распределения (стойка, физический хост). Используется при `spreadAcrossDomains=true`; если хоть у одной ноды не задан — механизм отключается. |
| `replica_type` | CSV-список допустимых типов реплик на ноде (`NRT,TLOG,PULL`, регистронезависимо). Не задано = принимает любые. |
| `node_type` | Произвольные метки типа ноды (CSV, логическое ИЛИ), для сопоставления с `collectionNodeType`. |

### Параметры конфига (`AffinityPlacementConfig`)

| Параметр | По умолчанию | Описание |
|---|---|---|
| `minimalFreeDiskGB` | 5 | Жёсткий лимит: нода со свободным диском меньше этого значения исключается из размещения (с учётом прогнозируемого размера реплики). 0 или меньше — отключено. |
| `prioritizedFreeDiskGB` | 100 | Мягкий лимит: ноды с диском меньше этого значения получают штраф +100 к весу, но остаются допустимыми. |
| `withCollection` | `{}` | Ко-локация коллекций: ключ — primary-коллекция, значение — secondary. Реплики primary ставятся только на ноды, где уже есть secondary; удаление защищается. |
| `withCollectionShards` | `{}` | То же, но с пошардовым соответствием (реплика шарда N primary — только там, где есть шард N secondary). Ключи должны не пересекаться с `withCollection` (проверяется в `validate()`). |
| `collectionNodeType` | `{}` | Привязка коллекций к типам нод: ключ — коллекция, значение — CSV допустимых `node_type`. |
| `spreadAcrossDomains` | `false` | Включает распределение реплик шарда по разным `spread_domain` (внутри AZ). |
| `maxReplicasPerShardInDomain` | −1 | Максимум реплик одного шарда (одного типа) в одном spread domain. Объявлен в конфиге; в текущем коде плагина в ветке 9.5 в расчётах не задействован. |

### Запрашиваемые метрики

Через `AttributeFetcher`: `NUM_CORES` и `FREE_DISK_GB` по нодам, `INDEX_SIZE_GB` по репликам
(лидерам) затрагиваемых коллекций, плюс перечисленные system properties. Ноды, для которых не
удалось получить диск или число cores, исключаются из решений (с warning в логе).

## Откуда вызывается PlacementPlugin

Единая точка входа — `Assign.createAssignStrategy(coreContainer)`
(`solr/core/src/java/org/apache/solr/cloud/api/collections/Assign.java`). Она берёт фабрику из
`CoreContainer.getPlacementPluginFactory()` (это `DelegatingPlacementPluginFactory`, см. выше),
создаёт инстанс плагина и оборачивает его в `PlacementPluginAssignStrategy`. Затем команды
Overseer-а (`OverseerCollectionMessageHandler` / distributed collection processing) вызывают у
стратегии один из методов: `assign(...)`, `computeReplicaBalancing(...)`,
`verifyDeleteCollection(...)`, `verifyDeleteReplicas(...)`.

Места вызова (все — команды Collections API, выполняемые на Overseer):

| Команда / класс | Операция API | Что вызывается у плагина |
|---|---|---|
| `CreateCollectionCmd` | CREATE | `computePlacements` — размещение всех реплик новой коллекции |
| `AddReplicaCmd` (через `Assign.getNodesForNewReplicas`) | ADDREPLICA, а также MOVEREPLICA и другие команды, добавляющие реплики через него | `computePlacements` |
| `SplitShardCmd` | SPLITSHARD | `computePlacements` — размещение реплик под-шардов |
| `RestoreCmd` | RESTORE | `computePlacements` — размещение реплик восстанавливаемой коллекции |
| `ReplaceNodeCmd` | REPLACENODE | `computePlacements` — куда переносить реплики со снимаемой ноды |
| `MigrateReplicasCmd` | MIGRATE_REPLICAS | `computePlacements` |
| `BalanceReplicasCmd` | BALANCE_REPLICAS | `computeBalancing` — единственный потребитель балансировки |
| `DeleteReplicaCmd` | DELETEREPLICA, DELETENODE | `verifyAllowedModification` — плагин может запретить удаление (ко-локация в Affinity) |
| `DeleteCollectionCmd` | DELETE | `verifyAllowedModification` |
| `CollectionsRepairEventListener` | автолечение по событию `NODES_DOWN` (cluster event listener) | `computePlacements` — досоздание потерянных реплик |

Особый случай: для системных коллекций (префикс `.sys.`), когда явно указан список нод,
`PlacementPluginAssignStrategy` не зовёт плагин, а раскладывает реплики round-robin по указанным
нодам.

## Где можно переопределить плагин

**Только глобально на кластер.** В Solr 9.x плагин размещения — один на весь кластер; выбрать
разные плагины для разных коллекций нельзя. В `Assign.createAssignStrategy` это зафиксировано
прямо в комментарии: «If a cluster wide placement plugin is configured (and that's the only way
to define a placement plugin)». Старые механизмы пер-коллекционного управления (rule-based
placement со snitch, Autoscaling policy) удалены в Solr 9.0.

Уровни переопределения (по убыванию приоритета):

1. **Cluster plugins API** — регистрация под именем `.placement-plugin` через
   `POST /api/cluster/plugin` (хранится в ZooKeeper `/clusterprops.json`, подхватывается на лету
   без рестарта через listener в `PlacementPluginFactoryLoader`). Здесь же задаётся конфиг
   плагина (`"config": {...}`), изменяемый командой `update`. Если в `solr.xml` включён
   `<clusterPlugins>` (`NodeConfigClusterPluginsSource`), плагины объявляются статически в
   `solr.xml` и API становится read-only.
2. **Системное свойство** `-Dsolr.placementplugin.default=simple|affinity|minimizecores|random`
   на нодах — дефолт кластера, когда плагин не зарегистрирован через API. Задаёт только тип,
   без конфига (Affinity в этом случае работает с дефолтным `AffinityPlacementConfig`).
3. **Зашитый дефолт** — `SimplePlacementFactory`.

На уровне **коллекции/запроса** можно влиять не на выбор плагина, а на входные данные для него:

- `createNodeSet` (CREATE, RESTORE) и `node`/`createNodeSet` (ADDREPLICA, SPLITSHARD) — сужают
  множество нод-кандидатов, которое передаётся плагину как `targetNodes` запроса; плагин выбирает
  только среди них.
- Для Affinity — пер-коллекционные ограничения задаются в его конфиге (`collectionNodeType`,
  `withCollection`, `withCollectionShards`) и через system properties нод (`availability_zone`,
  `replica_type`, `node_type`, `spread_domain`). То есть «настройка под коллекцию» возможна, но
  живёт в конфиге единственного глобального плагина, а не в свойствах коллекции.

## Сводная таблица

| Плагин | Конфиг | Метрики нод | Уникальность шарда на ноде | Основной критерий |
|---|---|---|---|---|
| Simple (дефолт) | — | нет | нет (штраф ×1000) | меньше реплик той же коллекции/шарда, затем меньше реплик всего |
| MinimizeCores | — | NUM_CORES | да | минимум cores на ноду |
| Random | — | нет | да | случайный выбор |
| Affinity | AffinityPlacementConfig | NUM_CORES, FREE_DISK_GB, INDEX_SIZE_GB | да | AZ → spread domain → диск → минимум cores; + типы нод/реплик, ко-локация |

## Практика: аномалия при вводе новой (пустой) ноды

### Симптом и причина

При добавлении пустой ноды в кластер с дефолтным плагином все новые коллекции и реплики
складываются на неё, нода принимает всю нагрузку на запись свежих коллекций и упирается в CPU
(saturation / thread starvation).

Это не сбой, а целевая функция весовых плагинов. У `SimplePlacementPlugin` вес ноды при
размещении **новой** коллекции сводится к общему числу реплик на ноде (штрафы за ту же
коллекцию/шард для новой коллекции равны нулю у всех нод) — пустая нода строго выигрывает каждое
сравнение, пока не наберёт столько же реплик, сколько у остальных. Диск здесь ни при чём: Simple
метрики вообще не запрашивает. Механизм requeue при ничьих не помогает — у новой ноды не ничья,
а строгий минимум. Смена плагина на `MinimizeCores` или `Affinity` сама по себе ничего не меняет:
их доминанта веса та же («минимум cores на ноду»), гравитация к пустой ноде сохраняется.
Настройки вида «не более N новых реплик на ноду за период» нет ни в одном плагине.

### Решение 1 (рекомендуемое): BALANCE_REPLICAS при вводе ноды

Убрать «гравитационную яму» до того, как на ноду поедут новые коллекции: сразу после добавления
ноды запустить балансировку (Solr 9.3+, v2 API, использует тот же placement-плагин):

```bash
curl -X POST -H 'Content-type:application/json' -d '{
  "async": "balance-after-node-add-1"
}' http://localhost:8983/api/cluster/replicas/balance
```

Тело может содержать `"nodes": [...]` (по умолчанию — все live-ноды); `async` стоит использовать
всегда — переносов может быть много. Балансировка переносит существующие реплики с нагруженных
нод на новую, пока веса не сравняются. После этого новая нода несёт ~среднее число реплик и
новые коллекции распределяются равномерно. Ключевое отличие: на ноду переезжает смешанная
нагрузка старых коллекций, а не 100% записи одной свежей. Балансировку выполнять в спокойные
часы (перенос реплик — это копирование данных). Это штатный механизм, добавленный ровно для
этого сценария; шаг стоит включить в рангбук ввода ноды.

### Решение 2 (структурное): Affinity + разметка нод

Защита инвариантом, а не процедурой — перейти на `AffinityPlacementFactory`:

- **`-Dreplica_type=PULL` на новой ноде** — на неё не может быть размещена ни NRT-, ни
  TLOG-реплика, т.е. вся цепочка записи обходит ноду стороной; только PULL (чтение). После
  прогрева свойство снимается (рестарт ноды). Честная оговорка: это откладывает, а не устраняет
  выравнивание — после снятия метки дефицит cores закрывается плагином, но уже смесью реплик
  разных коллекций.
- **`availability_zone` / `spreadAcrossDomains`** — веса за дисбаланс по AZ (×1 000 000)
  доминируют над числом cores, поэтому новая нода получит максимум одну реплику каждого шарда
  (реплики того же шарда/типа обязаны уехать в другие зоны) — на неё придёт ~1/RF записи новой
  коллекции вместо 100%. Смягчение, не устранение: внутри своей зоны нода по-прежнему самая
  привлекательная.
- **`node_type`-фенсинг** — новой ноде `-Dnode_type=warming`, критичным коллекциям в конфиге
  плагина `collectionNodeType: {"hot_coll": "live"}`. Работает, но `collectionNodeType`
  перечисляется поколлекционно — для динамически создаваемых коллекций неудобно.

### Решение 3 (тактическое): опции запроса CREATE

Единственный рычаг на уровне CREATE в 9.x — `createNodeSet`: `CreateCollectionCmd` пересекает
live-ноды с этим списком и передаёт результат плагину как `targetNodes`
(`CreateCollectionCmd.java`, `buildReplicaPositions`), плагин выбирает только среди них:

```
/admin/collections?action=CREATE&name=hot_coll&numShards=8&replicationFactor=2
  &createNodeSet=node1:8983_solr,node2:8983_solr    # без новой ноды
```

Вариант `createNodeSet=EMPTY` создаёт коллекцию без реплик — дальше ручные ADDREPLICA с явным
`node=...`. Опция `createNodeSet.shuffle` с placement-плагинами бесполезна (плагин всё равно
пересортировывает кандидатов по весам).

Ограничения подхода: это дисциплина клиента, а не политика кластера (забыли в одном месте —
аномалия вернулась; списки нод протухают); ограничение действует только на сам CREATE —
последующие ADDREPLICA/SPLITSHARD требуют своих параметров, а автолечение
(`CollectionsRepairEventListener`) и MOVEREPLICA о нём не знают. Надёжно, если все коллекции
создаются через собственный сервис-обёртку — тогда исключение ноды живёт в одной точке.

### Решение 4: Random как постоянный плагин

`RandomPlacementFactory` — единственная реализация, чей выбор не зависит от заполненности ноды:
феномен пустой ноды отсутствует в принципе, новая нода получает ~1/N новых реплик. Цена: кластер
больше никто не балансирует (ни cores, ни диск), дисбаланс накапливается дрейфом. Осмысленно при
большом числе однотипных короткоживущих коллекций; для нескольких больших разнородных — плохой
выбор. Random можно включать и временно (плагин меняется на лету через `/api/cluster/plugin`)
только на окно ввода ноды.

### Можно ли обойтись совсем без балансировки?

Чисто настройками — нет: у Simple/MinimizeCores настроек нет, у Affinity доминанта веса та же.
«Никогда не звать balance» достижимо только с Random (со всеми издержками). «Новая нода не
пробивает SLA по записи» — достижимо конфигурацией Affinity (AZ + PULL-прогрев), но нода будет
входить в строй медленно. Оптимум по совокупности: Affinity + одна команда `BALANCE_REPLICAS` в
рангбуке ввода ноды.

### Историческая справка: maxShardsPerNode

До Solr 9 существовал параметр CREATE `maxShardsPerNode` — жёсткий потолок числа реплик **данной
коллекции** на одной ноде (по умолчанию 1, `-1` — без ограничений). С ним описанная аномалия для
отдельной коллекции была невозможна по построению. Удалён в **Solr 9.0.0** (SOLR-12847) вместе со
старой машинерией размещения (LegacyAssignStrategy, rule/snitch, autoscaling policy).

Прямого эквивалента в 9.x нет. Что осталось:

- встроенная анти-аффинность «одна реплика *шарда* на ноду» (у Simple — штраф ×1000, у остальных
  запрет) — слабее: разные шарды одной коллекции свободно складываются на одну ноду;
- Affinity + AZ/spread domains — потолок «одна реплика шарда (по типу) на зону/домен»,
  приближается к эффекту `maxShardsPerNode` при разбиении кластера на зоны;
- `maxReplicasPerShardInDomain` (Affinity) — похож по духу, но про домены, а не ноды, и в 9.5
  в расчётах не используется.

Итог: жёсткий потолок в Solr 9 заменили мягким стремлением весовой модели к балансу; сценарий
ввода пустой ноды — та дыра, которую мягкая модель не закрывает, отсюда процедурное
(BALANCE_REPLICAS) или структурное (Affinity + зоны) решение.

### Проверка ветки main (снимок 2026-06-08)

Признаков того, что проблему «осознали и вернули» лимит, в main нет:

- `maxShardsPerNode` не возвращён; в `CollectionTooManyReplicasTest` встроенную анти-аффинность
  «одна реплика шарда на ноду» прямо называют «equivalent to maxShardsPerNode=1» — вопрос считают
  закрытым этим (более слабым) ограничением.
- Набор плагинов тот же (4 штуки), дефолт всё ещё Simple, TODO про SOLR-16492 (сделать Affinity
  дефолтом) висит без движения.
- `maxReplicasPerShardInDomain` в Affinity по-прежнему объявлен, но не используется.
- `OrderedNodePlacementPlugin` переработан (SOLR-17331: `Pending`→`Outstanding`, счётчик
  `placementCount` против зацикливания requeue), но потолков «реплик на ноду» не появилось.
- В changelog 9.6+ по placement только фиксы/оптимизации (SOLR-17257, SOLR-17331, SOLR-17644,
  SOLR-17079 — объявление плагинов в `solr.xml`).

## Предложение для Apache JIRA: вернуть per-node лимит как opt-in

### Набросок тикета

**Project:** SOLR · **Issue Type:** Improvement · **Components:** SolrCloud · **Affects:** 9.x, main

**Summary:**

> Weight-based placement plugins overload newly added empty nodes; add an opt-in per-node
> replica cap (successor of maxShardsPerNode)

**Description:**

```
h3. Problem

All built-in weight-based placement plugins (SimplePlacementFactory — the default,
MinimizeCoresPlacementFactory, AffinityPlacementFactory) share the same greedy strategy via
OrderedNodePlacementPlugin: place each replica on the node with the lowest weight, where the
weight is dominated by the replica/core count on the node.

When a fresh empty node joins the cluster, it strictly wins every placement comparison until its
replica count catches up with the rest of the cluster. In practice this means every shard of
every newly created collection lands on the new node. For a write-heavy new collection this
concentrates ~100% of its indexing load on a single node, causing CPU saturation / thread
starvation and SLA violations. The tie-requeue logic in OrderedNodePlacementPlugin does not
help: the empty node is not tied, it is a strict minimum.

h3. Existing mitigations and why they are insufficient

* {{BALANCE_REPLICAS}} (SOLR-17038): works, but it is a runbook procedure, not a cluster
  invariant. There is a race window between node join and balance completion, and every operator
  has to know to do it.
* {{createNodeSet}} on CREATE: client-side discipline; not enforced for later
  ADDREPLICA/SPLITSHARD, ignored by CollectionsRepairEventListener and MOVEREPLICA.
* Affinity {{availability_zone}} / {{spreadAcrossDomains}} / {{replica_type=PULL}} fencing:
  partial or operationally heavy (node restarts to change sysprops), and unavailable to users of
  the default plugin.
* {{maxShardsPerNode}} used to cap exactly this by construction, but was removed in SOLR-12847
  without a functional replacement. The related Affinity config {{maxReplicasPerShardInDomain}}
  is declared but has been a no-op since the SOLR-16806 rewrite.

h3. Proposal

Re-introduce a hard per-node cap as a strictly opt-in constraint, disabled by default, enforced
generically in OrderedNodePlacementPlugin so all built-in plugins (including the default) and
custom subclasses get it for free:

* A per-collection property, e.g. {{placement.maxReplicasPerNode}} — maximum number of replicas
  of the given collection allowed on a single node (any shard, any type). Unset / {{-1}} =
  unlimited = exactly today's behavior.
* The placement API already supports reading it with no interface changes:
  {{SolrCollection.getCustomProperty()}} resolves {{property.*}} entries of the collection
  state, and MODIFYCOLLECTION already persists arbitrary {{property.*}} keys.
* Enforcement point: the base {{WeightedNode.canAddReplica()}} — which is consulted both by
  {{computePlacements()}} and {{computeBalancing()}}, so placements, balancing and repair all
  respect the cap automatically.
* The name deliberately differs from the old {{maxShardsPerNode}} (which was a frequent source
  of confusion, since it actually counted replicas).

h3. Compatibility

Default behavior is unchanged: the constraint only activates when the property is explicitly set
on a collection. No changes to plugin configs, no new APIs, no upgrade impact.

I am willing to work on a PR if the approach is acceptable.
```

### Рекомендованная реализация (opt-in, по умолчанию выключено)

**Почему свойство коллекции, а не конфиг плагина.** Три из четырёх плагинов (включая дефолтный
Simple) — `NoConfig`; добавление конфига им — ломающее изменение сигнатур фабрик. А канал
«свойство коллекции → плагин» уже существует и никем не используется:
`SolrCollection.getCustomProperty(name)` читает `property.<name>` из состояния коллекции
(`SimpleClusterAbstractionsImpl.SolrCollectionImpl#getCustomProperty`), а MODIFYCOLLECTION уже
умеет записывать произвольные `property.*` в state (`CollectionMutator#modifyCollection`).
Плюс семантика правильная: лимит — свойство коллекции, а не глобальная политика.

**Точка врезки — одна**: базовый `WeightedNode.canAddReplica()` в `OrderedNodePlacementPlugin`
(сейчас проверяет только «нет второй реплики того же шарда»):

```java
// в WeightedNode.canAddReplica(), после существующей проверки шарда
String maxStr = replica.getShard().getCollection()
    .getCustomProperty("placement.maxReplicasPerNode"); // property.placement.maxReplicasPerNode
if (maxStr != null) {
    int max = Integer.parseInt(maxStr);
    if (max > 0 && getReplicasForCollectionOnNode(collectionName) >= max) return false;
}
```

Поскольку `canAddReplica()` вызывается и из `computePlacements`, и из `computeBalancing`, и
опосредованно из автолечения — лимит соблюдается везде без дополнительного кода. Побочное место
одно: у Simple `canAddReplica` переопределён (`return true`) — нужно позвать `super`.

**Включение пользователем:**

```
MODIFYCOLLECTION&collection=hot_coll&property.placement.maxReplicasPerNode=2
```

(запись в state работает уже сейчас; сахар в CREATE — отдельным коммитом при желании).

**Детали, которые спросят в ревью — заложить сразу:**

- fail-fast как у старого `maxShardsPerNode`: если `numShards × RF > cap × |targetNodes|`,
  кандидаты закончатся и размещение упадёт с `PlacementException` — улучшить сообщение об
  ошибке, явно указав на лимит;
- валидация значения: нечисловое/нулевое — игнорировать с warning, не ронять размещение;
- взаимодействие с DELETENODE/REPLACENODE: при жёстком лимите переносить реплики может быть
  некуда — ожидаемо, но задокументировать;
- тесты: юнит на `canAddReplica` для всех четырёх плагинов; интеграционный сценарий «пустая
  нода + CREATE с лимитом» (инфраструктура в `solr/core/src/test/.../cluster/placement/`);
  тест, что balancing не нарушает лимит;
- имя: не возвращать `maxShardsPerNode` — его убирали в том числе из-за путаницы
  «shards vs replicas»; `placement.maxReplicasPerNode` точнее.

**Почему это «никому не мешает»:** без свойства — ноль изменений поведения (одна дешёвая
проверка `getCustomProperty` → null); нет новых параметров API, миграций state и изменений
интерфейсов placement API — минимальная поверхность для возражений. Ожидаемый контраргумент в
обсуждении — «сделайте это конфигом Affinity»; ответ: аномалия воспроизводится именно на
дефолтном плагине, решение должно быть доступно без перехода на Affinity.

## Как написать свой плагин

1. Реализовать `PlacementPluginFactory<MyConfig>` (конфиг-бин реализует `PlacementPluginConfig`,
   поля аннотируются `@JsonProperty`).
2. Проще всего унаследовать плагин от `OrderedNodePlacementPlugin` и реализовать
   `getBaseWeightedNodes()` со своим `WeightedNode` — размещение и балансировка достанутся из
   базового класса. Полный контроль — реализовать `PlacementPlugin` напрямую.
3. Положить jar в classpath всех нод и зарегистрировать через
   `POST /api/cluster/plugin` под именем `.placement-plugin`.
