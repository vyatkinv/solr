# Подбор параметров коллекции: шарды, solr.xml, solrconfig.xml

Профиль нагрузки: **write-only** (поисковый трафик пренебрежительно мал).
Кластер: 230 нод, 8 CPU, 32 GB RAM, SSD.
Целевой throughput: 200 000 doc/sec, ~1 KB/doc.

---

## 1. Анатомия write-пути и откуда берутся соединения

```
Клиент → Нода-координатор (любая)
  → DistributedZkUpdateProcessor
    → StreamingSolrClients.getSolrClient(leaderUrl)
        — один ConcurrentUpdateJettySolrClient на каждый уникальный URL лидера
        — все шарят один updateOnlyClient (→ общий asyncTracker)
      → Runner (runnerCount=1 по умолчанию)
          — открывает ОДИН streaming POST и насыщает его документами
          — ждёт pollQueueTimeMillis=10с после опустения очереди
          — закрывает POST → соединение возвращается в пул
          → следующий батч: переиспользует соединение из пула (keep-alive)

Лидер шарда → реплика:
  → тот же путь через StreamingSolrClients с recoveryOnlyClient
```

### Сколько соединений реально возникает

**На каждом координаторе (исходящие):**

```
575 шардов → 575 ConcurrentUpdateJettySolrClient → 575 одновременных runner'ов
→ 575 активных HTTP-запросов к 230 destination-нодам
HTTP/1.1: 575 TCP-соединений (по одному на каждый шард-лидер)
HTTP/2:   ~920 TCP-соединений (4 на каждую из 230 destination-нод)
          но 575 шардов мультиплексируются поверх этих 920
```

**На каждой ноде-лидере (входящие):**

```
Хостит 2.5 шарда в среднем.
230 координаторов, каждый держит runner к каждому шарду на этой ноде.
HTTP/1.1: 230 координаторов × 2.5 шарда/нода = 575 входящих TCP
HTTP/2:   230 координаторов × 4 соединения  = 920 входящих TCP
```

Итого: и HTTP/1.1, и HTTP/2 дают **порядка 600–900 входящих соединений на ноду** в steady state. Это само по себе не проблема для Jetty. Проблема — в **скорости создания** новых соединений.

---

## 2. Корень проблемы: HTTP/1.1 без работающего keep-alive

### Почему соединения "живут очень мало и их много"

`ConcurrentUpdateJettySolrClient.doSendUpdateStream()` открывает один HTTP POST, стримит документы, потом **закрывает тело запроса** (когда очередь пуста дольше `pollQueueTimeMillis`). Соединение при этом уходит обратно в пул.

Если keep-alive работает — следующий батч переиспользует то же соединение. Новых TCP нет.

Если keep-alive **не работает** (Connection: close от прокси/балансировщика, или соединение успело умереть в пуле) — каждый батч = новый TCP handshake.

**Расчёт нагрузки при сломанном keep-alive:**

```
200 000 doc/sec ÷ 575 шардов = 348 doc/sec на шард

На одном координаторе: 348 doc/sec ÷ 230 нод ≈ 1.5 doc/sec к конкретному шарду
Очередь queueSize=100: заполняется за 100 / 1.5 ≈ 67 сек
→ каждые 67 сек этот runner отправляет батч из 100 документов

Всего по кластеру:
  230 координаторов × 575 шардов / 67 сек = ~1985 новых TCP в секунду (кластер-wide)
  
  Но если инgest идёт через 5 dedicated ingest-нод:
  5 × 575 шардов / (100 doc / (200k/5 / 575)) = 5 × 575 × (200k/5/575/100) ≈ 5 × 575 × 0.7 = ~2000 новых TCP/сек
```

2000 новых TCP/сек при `acceptQueueSize=128` = **гарантированное переполнение**.

### Почему это не видно при нормальном keep-alive

С работающим keep-alive:
- Первоначальное установление: 575 TCP-соединений от координатора к лидерам (разово, за несколько секунд при старте)
- Steady-state: те же 575 соединений просто переиспользуются снова и снова
- Новых TCP = 0 (кроме случаев ребалансировки или падения нод)

### Диагностика: определить факт сломанного keep-alive

```bash
# На ноде-лидере: смотреть TIME_WAIT (признак закрытых соединений)
ss -tn state time-wait '( dport = :8983 or sport = :8983 )' | wc -l
# Если > 500 — соединения активно закрываются

# Скорость создания новых соединений
watch -n 1 'ss -tn state established "( dport = :8983 )" | wc -l'
# Если число прыгает — соединения открываются/закрываются

# Есть ли SYN-дропы
netstat -s | grep -E "SYNs to LISTEN|failed connection attempts"
# Растущий счётчик = accept queue переполняется
```

---

## 3. Почему очередь HTTP/2 доходит до 3000

При переключении на HTTP/2 все `ConcurrentUpdateJettySolrClient` на ноде шарят **один** `updateOnlyClient`, у которого один `asyncTracker`:

```java
// HttpJettySolrClient.AsyncTracker
maxOutstandingRequests = 1000  // solr.solrj.http.jetty.async_requests.max
maxRequestsQueuedPerDestination = 1000 × 3 = 3000  // устанавливается на Jetty HttpClient
```

**Что происходит при 575 шардах:**

```
575 runner'ов одновременно пытаются отправить батч
→ все 575 вызывают asyncTracker.queuedListener.requestQueued()
→ семафор available.acquire() — 575 пермитов сразу
→ 575 < 1000, в семафор влезает

Но: Jetty HTTP/2 клиент дополнительно ставит в очередь запросы
    когда все 4 TCP-соединения до destination насыщены (нет свободных h2 streams).
    Предел очереди Jetty = maxRequestsQueuedPerDestination = 3000.
```

На практике очередь 3000 набирается когда:
1. Лидер-нода медленно обрабатывает (высокая merge-нагрузка, I/O wait)
2. Все 4 h2c-соединения до неё насыщены (много одновременных стримов)
3. Новые запросы встают в очередь Jetty и набирают 3000

Это **не баг** — это защита от OOM. Но порог нужно либо увеличить, либо устранить причину насыщения.

---

## 4. Оптимальное число шардов для write-only

### Формула

Для write-heavy шарды влияют на два фактора:

| Фактор | Зависимость | Примечание |
|---|---|---|
| Соединений на координатор | `O(shards)` | Каждый шард = 1 runner = 1 TCP-соединение |
| Throughput на шард | `total_throughput / num_shards` | Должен быть ниже предела ядра шарда |
| Время recovery | `index_size / num_shards × RF` | Меньше шардов/ноду → быстрее |

**Максимальный throughput одного шарда:**
- При простой схеме (1KB документы): ~20 000–50 000 doc/sec
- С учётом merge overhead, репликации: практический предел ~10 000 doc/sec
- При 10× safety margin: ~1 000 doc/sec — целевой максимум на шард

**Минимум шардов для 200 000 doc/sec:**
```
200 000 / 1 000 = 200 шардов минимум
```

**Оптимум для кластера 230 нод с RF=2:**

| Шардов | Реплик (×RF=2) | Шарды/нода | doc/sec на шард | Соединений/координатор | Оценка |
|---|---|---|---|---|---|
| 115 | 230 | 0.5 (часть нод = реплика) | 1739 | 115 | Слишком мало — throughput на пределе |
| 230 | 460 | 2 | 870 | 230 | **Оптимально** |
| 345 | 690 | 3 | 580 | 345 | Допустимо, ненужный overhead |
| 575 | 1150 | 5 | 348 | 575 | Текущее — вдвое больше нужного |

**Рекомендация: 230 шардов, RF=2.**

Почему именно 230:
- Ровно по одному primary-шарду на ноду → нет "горячих" нод
- 870 doc/sec на шард = 12× запас до практического предела
- Соединений на координатор: 230 вместо 575 (−60%)
- Recovery: при падении ноды → 1 шард восстанавливается (vs 2.5 при текущих 575)

---

## 5. Исправление acceptQueueSize

При 230 шардах и 230 координаторах: при старте одновременно устанавливается ~230 TCP от каждого координатора к каждой ноде-лидеру. При HTTP/2 это 4 × 230 = 920 TCP к одной ноде разом.

`acceptQueueSize=128` не справляется. Нужно:

```bash
# solr.in.sh
SOLR_OPTS="$SOLR_OPTS -Dsolr.jetty.http.acceptQueueSize=4096"
```

```bash
# /etc/sysctl.d/99-solr.conf (на всех нодах)
net.core.somaxconn = 65535
net.ipv4.tcp_max_syn_backlog = 65535
# Применить: sysctl -p
```

Проверка:
```bash
ss -tlnp | grep 8983
# Recv-Q = текущая очередь (должна быть 0 или близко к 0)
# Send-Q = configured backlog (должна быть 4096)
```

---

## 6. Параметры solr.in.sh (полный набор)

```bash
# Убрать:
# -Dsolr.http1=true          ← главное изменение

# Добавить:
SOLR_HEAP="12g"
SOLR_JAVA_MEM="-Xms12g -Xmx12g"

# HTTP/2 настройки
SOLR_OPTS="$SOLR_OPTS -Dsolr.http2.maxConnectionsPerDestination=8"
SOLR_OPTS="$SOLR_OPTS -Dsolr.http.client.selectors=4"
# asyncTracker: увеличить если снова видим переполнение 3000-очереди
SOLR_OPTS="$SOLR_OPTS -Dsolr.solrj.http.jetty.async_requests.max=2000"

# Jetty-сервер
SOLR_OPTS="$SOLR_OPTS -Dsolr.jetty.http.acceptQueueSize=4096"
SOLR_OPTS="$SOLR_OPTS -Dsolr.jetty.http.acceptors=4"
SOLR_OPTS="$SOLR_OPTS -Dsolr.jetty.threads.max=1000"

# Update pipeline
SOLR_OPTS="$SOLR_OPTS -Dsolr.cloud.client.updateQueueSize=200"
SOLR_OPTS="$SOLR_OPTS -Dsolr.cloud.replication.runners=2"
SOLR_OPTS="$SOLR_OPTS -Dsolr.cloud.client.pollQueueTime=5000"

# Replication chunk
SOLR_OPTS="$SOLR_OPTS -Dsolr.replication.packetSize=4194304"

# GC
GC_TUNE="$GC_TUNE -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:G1HeapRegionSize=32m"
```

---

## 7. Параметры solrconfig.xml для write-heavy

```xml
<indexConfig>
  <!--
    Default: 16 MB → при 870 doc/sec (870 KB/sec) flush каждые 18 сек → много мелких сегментов.
    256 MB → flush каждые ~5 мин → меньше сегментов → меньше merge-нагрузки.
    На 32 GB RAM, 2 шарда/нода: 512 MB суммарно под буферы → приемлемо.
  -->
  <ramBufferSizeMB>256</ramBufferSizeMB>

  <mergePolicyFactory class="org.apache.solr.index.TieredMergePolicyFactory">
    <int name="maxMergeAtOnce">10</int>
    <int name="segmentsPerTier">10</int>
    <int name="maxMergedSegmentMB">5120</int>
  </mergePolicyFactory>

  <!--
    2 merge-потока: 1 ядро для индексирования + 1 для merge + 6 свободных для Jetty.
    При 870 doc/sec merge-нагрузка невысокая → 2 потоков хватит.
  -->
  <mergeScheduler class="org.apache.lucene.index.ConcurrentMergeScheduler">
    <int name="maxThreadCount">2</int>
    <int name="maxMergeCount">6</int>
  </mergeScheduler>
</indexConfig>

<updateHandler class="solr.DirectUpdateHandler2">
  <updateLog>
    <str name="dir">${solr.ulog.dir:}</str>
    <int name="numRecordsToKeep">100</int>
    <int name="maxNumLogsToKeep">10</int>
  </updateLog>

  <!--
    Hard commit каждые 15 сек: ограничивает размер tlog (max ~13 000 doc при 870 doc/sec).
    openSearcher=false: не открывает новый searcher → нет CPU spike на поисковые кэши.
  -->
  <autoCommit>
    <maxTime>${solr.autoCommit.maxTime:15000}</maxTime>
    <openSearcher>false</openSearcher>
  </autoCommit>

  <!--
    Soft commit нужен только если нужна near-real-time видимость документов.
    Для write-only без поиска — можно отключить (очень большое значение).
  -->
  <autoSoftCommit>
    <maxTime>${solr.autoSoftCommit.maxTime:3000}</maxTime>
  </autoSoftCommit>
</updateHandler>

<query>
  <!-- При write-only кэши почти бесполезны — минимизировать их размер -->
  <filterCache size="64" initialSize="64" autowarmCount="0"/>
  <queryResultCache size="64" initialSize="64" autowarmCount="0"/>
  <documentCache size="256" initialSize="256" autowarmCount="0"/>
  <enableLazyFieldLoading>true</enableLazyFieldLoading>
</query>
```

---

## 8. solr.xml

```xml
<solr>
  <solrcloud>
    <int name="distribUpdateSoTimeout">600000</int>
    <int name="distribUpdateConnTimeout">60000</int>
    <int name="zkClientTimeout">60000</int>
    <!-- Сжимать ClusterState: при 230 нодах state может быть большим -->
    <int name="minStateByteLenForCompression">1024</int>
  </solrcloud>

  <shardHandlerFactory name="shardHandlerFactory"
                       class="HttpShardHandlerFactory">
    <int name="socketTimeout">600000</int>
    <int name="connTimeout">60000</int>
  </shardHandlerFactory>

  <updateShardHandler>
    <int name="maxUpdateConnections">100000</int>
    <int name="maxUpdateConnectionsPerHost">100000</int>
    <int name="distributedSocketTimeout">600000</int>
    <int name="distributedConnectionTimeout">60000</int>
    <int name="maxRecoveryThreads">-1</int>
  </updateShardHandler>
</solr>
```

---

## 9. Приоритизированный план действий

### Немедленно (без reindex)

1. **Убрать `-Dsolr.http1=true`** — rolling restart, нода за нодой
   - Ожидаемый эффект: исчезновение short-lived connections, снятие нагрузки с accept queue

2. **Поставить `acceptQueueSize=4096`** и OS sysctl — rolling restart
   - Защищает от burst при старте/reconnect

3. **Проверить есть ли прокси** между Solr-нодами, который принудительно выставляет `Connection: close` — если есть, убрать или настроить keep-alive

4. **Диагностика HTTP/2**: сразу после перехода на h2 проверить
   ```bash
   # Должно быть ~4 соединения на пару нод, не 575
   ss -tn state established '( dport = :8983 )' \
     | awk '{print $5}' | cut -d: -f1 | sort | uniq -c | sort -rn | head -10
   ```

### Следующий цикл (планировать reindex)

5. **Уменьшить число шардов с 575 до 230** — требует пересоздания коллекции
   - Снижает: outgoing connections на координатор −60%, recovery time −60%

6. **Увеличить `ramBufferSizeMB` до 256** в solrconfig.xml — rolling reload
   - Снижает merge-нагрузку, уменьшает кол-во сегментов

---

## 10. Метрики для наблюдения

```bash
# Steady-state соединений после перехода на HTTP/2
# Ожидаемо: ~4 на пару нод (а не 575)
ss -tn state established '( dport = :8983 )' | awk '{print $5}' | \
  cut -d: -f1 | sort | uniq -c | sort -rn | head -20

# Скорость появления новых соединений
watch -n 5 'ss -s | grep -E "TCP|estab"'

# HTTP/2 pool через патч (Solr 11: только Prometheus-формат)
curl -s -H "Accept: text/plain" 'http://localhost:8983/solr/admin/metrics' | \
  grep -E '^solr_update_client_connections'

# Размер accept queue (должен быть 0 в steady state)
ss -tlnp | grep 8983

# SYN-дропы (должны быть 0)
nstat -az | grep -E "TcpExtListenDrops|TcpExtListenOverflows"
```
