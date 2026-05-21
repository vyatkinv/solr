# Межнодовая репликация в SolrCloud: устройство и оптимизация TCP-соединений

## 1. Два независимых потока данных

В SolrCloud есть два принципиально разных вида межнодовой коммуникации:

| Поток | Когда | Протокол | Файл |
|---|---|---|---|
| **Update distribution** | Каждый документ в реальном времени | HTTP POST (javabin) | `SolrCmdDistributor` → `StreamingSolrClients` |
| **Full index replication** | Recovery после сбоя | HTTP GET custom binary (`filestream`) | `IndexFetcher` ← `ReplicationHandler` |

Они используют **разные HTTP-клиенты** (`updateOnlyClient` и `recoveryOnlyClient`) из `UpdateShardHandler`.

---

## 2. Update Distribution: путь документа от лидера к репликам

```
Клиент → любая нода
  → DistributedZkUpdateProcessor
      (если нода — не лидер → форвард лидеру через updateOnlyClient)
  → Лидер: DistributedZkUpdateProcessor.doDistribAdd()
    → SolrCmdDistributor.distribAdd()
      → StreamingSolrClients.getSolrClient(replicaUrl)
          — один ConcurrentUpdateJettySolrClient на каждый URL реплики
          — создаётся лениво при первом обращении
        → ConcurrentUpdateBaseSolrClient
            — внутренняя очередь документов (withQueueSize)
            — N runner-потоков (withThreadCount = runnerCount)
          → HttpJettySolrClient (shared updateOnlyClient)
            → Jetty HTTP client → TCP → Реплика
```

### Ключевые параметры update path

| Параметр | Класс | Дефолт | System property |
|---|---|---|---|
| Размер очереди документов на реплику | `StreamingSolrClients:79` | 100 | `solr.cloud.client.updateQueueSize` |
| Runner-потоков на реплику | `StreamingSolrClients:43` | 1 | `solr.cloud.replication.runners` |
| Время ожидания в очереди | `StreamingSolrClients:45` | 10 000 мс | `solr.cloud.client.pollQueueTime` |
| Макс. соединений на хост (HTTP/1.1) | `UpdateShardHandlerConfig` | 100 000 | `maxUpdateConnectionsPerHost` в solr.xml |
| Макс. соединений на хост (HTTP/2) | `HttpJettySolrClient:300` | **4** | `solr.http2.maxConnectionsPerDestination` |

---

## 3. Full Index Replication: путь при recovery

```
Нода: state → RECOVERING (публикуется в ZooKeeper)
  → RecoveryStrategy.run()
    → replicate(leaderProps)
      → ReplicationHandler.doFetch(params, false)
        → IndexFetcher.fetchLatestIndex()
          │
          ├─ fetchFileList()          — 1 HTTP GET: получить список файлов с лидера
          │
          └─ downloadIndexFiles()    — N HTTP GET, строго последовательно
               для каждого файла:
                 DirectoryFileFetcher.fetchFile()
                   → getStream()    — открыть HTTP-соединение
                   → fetchPackets() — читать пакетами по PACKET_SZ байт
                   → fsyncService   — fsync в отдельном треде
```

### Критически важно

Файлы скачиваются **строго последовательно** — один за другим, один HTTP-запрос = один файл. Параллелизма нет. При большом индексе это главный узкое место recovery.

### Ключевые параметры replication path

| Параметр | Класс | Дефолт | System property |
|---|---|---|---|
| Размер пакета (chunk) | `ReplicationAPIBase:69` | 1 MB | `solr.replication.packetSize` |
| idle timeout при скачивании | `IndexFetcher:285` | 120 000 мс | `socketTimeout` в `<replication>` |
| Макс. recovery-потоков на ноду | `UpdateShardHandlerConfig` | -1 (unbounded) | `maxRecoveryThreads` в solr.xml |

---

## 4. HTTP-клиентский слой: архитектура

### Иерархия клиентов

```
UpdateShardHandler
├── updateOnlyClient  (HttpJettySolrClient)
│     └── Jetty HttpClient (общий для всех реплик при апдейтах)
│           └── ConnectionPool per Destination
│                 HTTP/1.1: макс. maxUpdateConnectionsPerHost соединений
│                 HTTP/2:   макс. 4 соединения (→ solr.http2.maxConnectionsPerDestination)
│
└── recoveryOnlyClient (HttpJettySolrClient)
      └── Jetty HttpClient (отдельный, requestTimeout=∞)
            └── используется в IndexFetcher через withHttpClient()

HttpShardHandlerFactory
└── defaultClient (HttpJettySolrClient)
      └── Jetty HttpClient (для query fanout на шарды)
```

### Создание Jetty HttpClient (`HttpJettySolrClient:275–312`)

```java
ClientConnector clientConnector = new ClientConnector();
clientConnector.setSelectors(                                   // NIO потоки
    EnvUtils.getPropertyAsInteger("solr.http.client.selectors", 2));

if (HTTP/1.1) {
    transport = new HttpClientTransportOverHTTP(clientConnector);
    httpClient = new HttpClient(transport);
    httpClient.setMaxConnectionsPerDestination(maxConnectionsPerHost);  // из конфига
} else {
    http2client = new HTTP2Client(clientConnector);
    transport = new HttpClientTransportOverHTTP2(http2client);
    httpClient = new HttpClient(transport);
    httpClient.setMaxConnectionsPerDestination(                         // было захардкожено = 4
        EnvUtils.getPropertyAsInteger("solr.http2.maxConnectionsPerDestination", 4));
}
```

---

## 5. HTTP/1.1 vs HTTP/2: принципиальная разница

### HTTP/1.1

- **1 запрос = 1 соединение.** Параллельные запросы к одному host требуют N соединений.
- `maxConnectionsPerHost = 100 000` → де-факто неограниченно.
- При `runnerCount = 1` и `updateOnlyClient` на реплику: **1 активное TCP-соединение** на пару (лидер, реплика) в момент отправки апдейтов.
- При query fanout на 230 шардов: потенциально 230 одновременных соединений с одного координатора.

### HTTP/2

- **Мультиплексирование:** N запросов по одному TCP через HTTP/2 streams.
- Число TCP ограничено `maxConnectionsPerDestination = 4` (было захардкожено, теперь конфигурируемо).
- Jetty HTTP/2 client соблюдает `MAX_CONCURRENT_STREAMS` из SETTINGS frame сервера (обычно 100–1024).
- Фактическая параллельность: `4 TCP × 100–1024 streams = 400–4096 запросов` к одному хосту через 4 соединения.

### Сравнительная таблица

| Аспект | HTTP/1.1 | HTTP/2 |
|---|---|---|
| TCP на (лидер → реплика) при апдейтах | 1 (runnerCount=1) | 1–4 |
| TCP при query fanout (230 шардов) | до 230 | 4 per shard host (если шарды на разных нодах) |
| TCP при recovery | 1 (последовательная загрузка) | 1 из пула 4 |
| Влияние `maxConnectionsPerHost` | прямое | не применяется |
| Влияние `maxConnectionsPerDestination` | Jetty default 64 (но override 100000) | **4 (было)** → конфигурируемо |
| Head-of-line blocking | да (на уровне TCP) | нет (у h2c) |
| TLS overhead | per connection | per connection, амортизируется |

### Включение HTTP/2

```bash
# В SOLR_OPTS / solr.in.sh
# Если не задан — HTTP/2 уже используется по умолчанию (solr.http1=false)
SOLR_OPTS="$SOLR_OPTS -Dsolr.http1=false"
```

Проверить, что сервер поддерживает: Solr 9.x поддерживает h2c (cleartext HTTP/2) по умолчанию.

---

## 6. Параметры и их влияние на количество TCP-соединений

### Настраиваемые через solr.xml

```xml
<updateShardHandler>
  <int name="maxUpdateConnections">100000</int>
  <int name="maxUpdateConnectionsPerHost">100000</int>  <!-- HTTP/1.1: лимит TCP на хост -->
  <int name="distributedSocketTimeout">600000</int>     <!-- idle timeout, мс -->
  <int name="distributedConnectionTimeout">60000</int>  <!-- connect timeout, мс -->
  <int name="maxRecoveryThreads">-1</int>               <!-- параллельных recovery -->
</updateShardHandler>
```

### Настраиваемые через JVM system properties

| Property | Дефолт | Влияние |
|---|---|---|
| `solr.http1` | `false` | `true` → HTTP/1.1 для всех клиентов |
| `solr.http2.maxConnectionsPerDestination` | `4` | HTTP/2: макс. TCP на destination |
| `solr.http.client.selectors` | `2` | NIO I/O потоков в Jetty client |
| `solr.cloud.replication.runners` | `1` | Runner-потоков на реплику (update path) |
| `solr.cloud.client.pollQueueTime` | `10000` мс | Время ожидания runner в пустой очереди |
| `solr.cloud.client.updateQueueSize` | `100` | Буфер документов на реплику |
| `solr.solrj.http.jetty.async_requests.max` | `1000` | Макс. одновременных async запросов |
| `solr.replication.packetSize` | `1048576` (1MB) | Размер чанка при передаче файлов |

---

## 7. Рекомендуемая конфигурация для минимизации TCP

### Переход на HTTP/2 (главный шаг)

HTTP/2 сокращает TCP соединения с `N × 100 000 (дефолт)` до `N × maxConnectionsPerDestination`:

```bash
# solr.in.sh
SOLR_OPTS="$SOLR_OPTS -Dsolr.http1=false"
```

### Стартовые значения для кластера 230 нод

```bash
SOLR_OPTS="$SOLR_OPTS \
  -Dsolr.http1=false \
  -Dsolr.http2.maxConnectionsPerDestination=8 \
  -Dsolr.http.client.selectors=4 \
  -Dsolr.replication.packetSize=4194304 \
  -Dsolr.cloud.client.updateQueueSize=100"
```

### Когда увеличивать `maxConnectionsPerDestination`

Смотреть на метрику `solr.update.client.connections{state=queued}`:
- `queued = 0` стабильно → текущего значения достаточно
- `queued > 0` при нагрузке → увеличить на 50%

Типичные значения:
- Лёгкая нагрузка (< 500 doc/sec на реплику): 4 соединения достаточно
- Средняя (500–2000 doc/sec): 8–16 соединений
- Высокая (> 2000 doc/sec): 16–32 соединения

### Когда увеличивать `selectors`

Проверить CPU потоков `h2sc-selector-*`:
```bash
top -H -p $(pgrep -f solr) | grep h2sc
```
Если CPU > 70% → увеличить `solr.http.client.selectors` до 4.

---

## 8. Наблюдаемые метрики (что мониторить)

### Из Solr (Prometheus / JMX)

| Метрика | Диагноз когда |
|---|---|
| `solr.update.client.connections{state=queued,client=update}` > 0 | Пул насыщен, нужно больше `maxConnectionsPerDestination` |
| `solr.update.client.connections{state=idle}` >> `active` | Пул избыточен, можно уменьшить |
| `solr.update.client.connections{state=pending}` постоянно > 0 | Частые новые TCP соединения, SYN flood риск |
| `solr.core.replication.download_speed` | Скорость recovery в байт/сек |
| `solr.core.executor.updateOnlyExecutor.queue` > 0 | Апдейты ждут потоков, увеличить `updateQueueSize` или `runners` |
| `solr_client_request_duration` P99 растёт | Latency деградация, смотреть queued и pending |

### OS-уровень

```bash
# Суммарное число TCP к Solr-портам
ss -tn state established '( dport = :8983 or sport = :8983 )' | wc -l

# По destination (топ пиров)
ss -tn state established '( dport = :8983 )' \
  | awk '{print $5}' | cut -d: -f1 | sort | uniq -c | sort -rn | head -10

# CLOSE_WAIT / TIME_WAIT (признак flow проблем)
ss -tn '( dport = :8983 or sport = :8983 )' | awk '{print $1}' | sort | uniq -c
```
