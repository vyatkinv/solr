# Методика нагрузочного тестирования: подбор HTTP-параметров SolrCloud

## Условия задачи

| Параметр | Значение |
|---|---|
| Кластер | 230 нод |
| Железо | 8 CPU, 32 GB RAM, SSD |
| Целевой трафик | 1.6 Гбит/с |
| Размер документа | ~1 KB |
| Расчётный RPS | ≈200 000 doc/sec суммарно по кластеру |
| На лидера шарда | ≈870–2000 doc/sec (зависит от топологии) |
| Цель | Минимизировать TCP-соединения без потери throughput |

**Расчёт:**
```
1.6 Gbps / (1 KB × 8 бит) = 200 000 doc/sec
200 000 / 230 нод ≈ 870 doc/sec на ноду
При RF=2: каждый лидер отправляет 870 doc/sec своей реплике
```

---

## Параметры, которые подбираем

| Property | Дефолт | Что регулирует |
|---|---|---|
| `solr.http1` | `false` | Протокол: HTTP/1.1 или HTTP/2 |
| `solr.http2.maxConnectionsPerDestination` | `4` | Макс. TCP на хост (HTTP/2) |
| `solr.http.client.selectors` | `2` | NIO I/O потоков в Jetty клиенте |
| `solr.cloud.client.updateQueueSize` | `100` | Буфер документов на реплику |
| `solr.cloud.replication.runners` | `1` | Параллельных HTTP потоков на реплику |
| `solr.replication.packetSize` | `1048576` | Размер чанка при recovery (байт) |

---

## Метрики мониторинга

### Уже доступны в Solr (без изменений кода)

| Метрика | Что показывает | Связанный параметр |
|---|---|---|
| `solr.core.executor.updateOnlyExecutor.queue` | Очередь задач отправки реплике | `updateQueueSize`, `runners` |
| `solr.core.executor.updateOnlyExecutor.active` | Активных потоков отправки | `runners` |
| `solr.core.executor.recoveryExecutor.active` | Сколько recovery идёт параллельно | `maxRecoveryThreads` |
| `solr.client.request.async_permits{state=available}` | Свободные async слоты (query path) | `async_requests.max` |
| `solr_client_request_duration` (histogram) | P50/P99 latency межнодовых HTTP запросов | все |
| `solr.core.replication.download_speed` | Скорость recovery в байт/сек | `packetSize` |
| `solr.core.replication.downloaded_size` | Прогресс recovery в байтах | — |
| `solr.core.replication.is_replicating` | 1 если recovery идёт прямо сейчас | — |
| `solr.core.replication.time_elapsed` | Секунд идёт текущий recovery | — |

### Добавлено патчем `0001-unhardcode-http-replication-params-and-add-metrics.patch`

Метрики экспортируются в Prometheus text format (имена с подчёркиваниями):

| Метрика (Prometheus) | Labels | Что показывает | Ключевой индикатор |
|---|---|---|---|
| `solr_update_client_connections` | `client="update", state="active"` | Соединений, обрабатывающих запрос | baseline |
| `solr_update_client_connections` | `client="update", state="idle"` | Открытых пустых соединений | если >> active → пул избыточен |
| `solr_update_client_connections` | `client="update", state="pending"` | TCP-handshake в процессе | если > 0 стабильно → частые разрывы |
| `solr_update_client_connections` | `client="update", state="queued"` | Запросов, ждущих соединения | **> 0 → пул насыщен** |
| `solr_update_client_connections` | `client="recovery", state=*` | То же для recovery-клиента | — |
| `solr_query_client_connections` | `state=*` | То же для query fanout клиента | — |

### OS-уровень (собирать на каждой ноде)

```bash
# Суммарное число установленных TCP к Solr-портам
ss -tn state established '( dport = :8983 or sport = :8983 )' | wc -l

# Топ-10 пиров по числу соединений
ss -tn state established '( dport = :8983 )' \
  | awk '{print $5}' | cut -d: -f1 | sort | uniq -c | sort -rn | head -10

# Проблемные состояния
ss -tn '( dport = :8983 or sport = :8983 )' | awk '{print $1}' | sort | uniq -c

# CPU NIO selector-потоков
top -H -b -n1 -p $(pgrep -f solr) | grep h2sc
```

---

## Топология тестового кластера

Рекомендуется тестировать на **20 нодах** (~10% кластера):

```
5 коллекций × 4 шарда × RF=2 = 20 нод
Каждая нода: лидер 2 шардов + реплика 2 шардов
Соединений на ноду: 2–4 destination (реалистично для 230-нодного кластера)
```

Переносимость результатов: параметры пула соединений масштабируются линейно по числу destinations на ноду. Если в тесте нода имеет 4 destinations — результаты прямо применимы к продакшну.

---

## Генераторы нагрузки

### Индексирующая нагрузка (непрерывная)

```bash
#!/usr/bin/env bash
# load-index.sh — N параллельных потоков, каждый шлёт батчи по 1000 docs
TARGET_RPS=${1:-10000}    # doc/sec
THREADS=${2:-16}
BATCH=1000
SOLR_URL="http://solr-lb:8983/solr/perf_test"

send_batch() {
  python3 -c "
import json, random, string, sys
n = int(sys.argv[1])
docs = [{'id': str(random.randint(1,10**12)),
         'text_t': ''.join(random.choices(string.ascii_lowercase+' ', k=900)),
         'num_i': random.randint(0, 1000000)}
        for _ in range(n)]
print(json.dumps(docs))
" $BATCH | curl -s -X POST "${SOLR_URL}/update?commit=false" \
    -H 'Content-Type: application/json' -d @- > /dev/null
}

export -f send_batch SOLR_URL BATCH

# Запуск
SLEEP=$(python3 -c "print($THREADS * $BATCH / $TARGET_RPS)")
for i in $(seq 1 $THREADS); do
  while true; do send_batch; sleep $SLEEP; done &
done
wait
```

### Симуляция recovery (стресс-тест)

```bash
#!/usr/bin/env bash
# recovery-stress.sh — периодически роняет и поднимает ноды
SOLR_PORT=8983
ZK_HOSTS="zk1:2181,zk2:2181,zk3:2181"
CYCLES=${1:-5}

wait_for_active() {
  local deadline=$((SECONDS + 300))
  while [[ $SECONDS -lt $deadline ]]; do
    local recovering
    recovering=$(curl -s "http://localhost:${SOLR_PORT}/solr/admin/cores?action=STATUS&wt=json" \
      | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(sum(1 for c in d['status'].values()
          if c.get('cloudDesc',{}).get('replicaState') == 'RECOVERING'))
" 2>/dev/null)
    [[ "${recovering}" == "0" ]] && { echo "All active"; return 0; }
    echo "  Recovering cores: ${recovering}, waiting..."
    sleep 5
  done
  echo "TIMEOUT waiting for recovery" >&2; return 1
}

for i in $(seq 1 "$CYCLES"); do
  echo "=== Recovery cycle $i/$CYCLES ==="
  bin/solr stop -p "$SOLR_PORT" -force
  sleep 3
  bin/solr start -p "$SOLR_PORT" -z "$ZK_HOSTS"
  echo "  Node started, waiting for recovery..."
  wait_for_active
  echo "  Cycle $i complete"
  sleep 10
done
```

### Сбор метрик в CSV

```bash
#!/usr/bin/env bash
# collect-metrics.sh — пишет ключевые метрики в CSV каждые 5 секунд
OUTPUT="${1:-metrics.csv}"
SOLR_URL="http://localhost:8983/solr"

echo "ts,tcp_total,active_update,idle_update,queued_update,active_recovery,queued_recovery,download_speed_bps,p99_ms" > "$OUTPUT"

while true; do
  TS=$(date +%s)

  # TCP count
  TCP=$(ss -tn state established '( dport = :8983 or sport = :8983 )' 2>/dev/null | wc -l)

  # Solr 11: метрики только в Prometheus/OpenMetrics формате (wt=json возвращает 400)
  METRICS=$(curl -s -H "Accept: text/plain" "${SOLR_URL}/admin/metrics" 2>/dev/null)

  # Парсинг Prometheus text format: последнее поле строки = значение
  get_metric() {
    echo "$METRICS" | grep "^${1}{" | grep "${2}" | awk '{print $NF}' | head -1
  }

  ACTIVE_UPDATE=$(get_metric "solr_update_client_connections" 'client="update",state="active"' || echo 0)
  IDLE_UPDATE=$(get_metric   "solr_update_client_connections" 'client="update",state="idle"'   || echo 0)
  QUEUED_UPDATE=$(get_metric "solr_update_client_connections" 'client="update",state="queued"' || echo 0)
  ACTIVE_RECV=$(get_metric   "solr_update_client_connections" 'client="recovery",state="active"' || echo 0)
  QUEUED_RECV=$(get_metric   "solr_update_client_connections" 'client="recovery",state="queued"' || echo 0)

  echo "${TS},${TCP},${ACTIVE_UPDATE:-0},${IDLE_UPDATE:-0},${QUEUED_UPDATE:-0},${ACTIVE_RECV:-0},${QUEUED_RECV:-0},0,0" >> "$OUTPUT"
  sleep 5
done
```

---

## Фаза 0: Baseline (HTTP/1.1, дефолты)

```bash
# solr.in.sh
SOLR_OPTS="-Dsolr.http1=true"
# Все остальные параметры — дефолты из кода
```

**Запуск:** 30 минут под нагрузкой 100% от целевой.

**Фиксировать каждые 5 минут:**

| Показатель | Инструмент | Ожидаемое значение |
|---|---|---|
| TCP соединений на ноду (`X₀`) | `ss` | 5–50 (по нагрузке) |
| `active_update` (`A₀`) | новая метрика | ~ runnerCount × destinations |
| `idle_update` (`I₀`) | новая метрика | обычно = active |
| `queued_update` (`Q₀`) | новая метрика | должен быть 0 |
| P99 latency (`L₀`) | `solr_client_request_duration` | зависит от нагрузки |
| Throughput (`T₀`) | update handler metrics | целевые 200K doc/sec |
| CPU per node | `htop` | < 70% |

> Если `Q₀ > 0` уже в baseline — проблема глубже (сеть, диск, GC).

---

## Фаза 1: Включение HTTP/2

```bash
SOLR_OPTS=""    # solr.http1 не задаём → HTTP/2 (это уже дефолт)
# Или явно:
SOLR_OPTS="-Dsolr.http1=false"
# maxConnectionsPerDestination = 4 (дефолт)
# selectors = 2 (дефолт)
```

**Ожидаемый результат:**

```
TCP count = N_destinations × 4
Для ноды с 4 destinations: ≤ 16 TCP
Сокращение vs HTTP/1.1: зависит от baseline, обычно 3–10×
```

**Критерии перехода к следующей фазе:**

| Условие | Действие |
|---|---|
| `queued_update = 0` && P99 ≤ L₀ × 1.1 && throughput ≥ T₀ | **Стоп. Конфиг найден** |
| `queued_update > 0` стабильно | Переходить к Фазе 2 |
| P99 вырос > 20% | Проверить selectors (Фаза 3) и queued (Фаза 2) |
| Throughput упал > 5% | Смотреть executor queue, возможно нужен runners > 1 |

---

## Фаза 2: Подбор `maxConnectionsPerDestination`

**Метод: итеративный, с шагом ×2 до исчезновения queued.**

```bash
# Итерация 1
SOLR_OPTS="-Dsolr.http2.maxConnectionsPerDestination=8"

# Итерация 2 (если queued_update ещё > 0 в пике)
SOLR_OPTS="-Dsolr.http2.maxConnectionsPerDestination=16"

# Итерация 3 (редко нужна)
SOLR_OPTS="-Dsolr.http2.maxConnectionsPerDestination=32"
```

**Формула нижней оценки:**

```
min_connections_per_dest = ceil(peak_rps_per_dest × avg_request_duration_sec)

Пример для вашего кластера:
  peak_rps_per_dest = 2000 doc/sec (лидер → реплика)
  avg_duration = 5ms = 0.005 sec
  min_connections = ceil(2000 × 0.005) = ceil(10) = 10
→ Ставить maxConnectionsPerDestination = 16 (с запасом 50%)
```

**Таблица решений:**

| `queued > 0` при нагрузке % | `idle / active` ratio | Действие |
|---|---|---|
| Нет | > 3 | Уменьшить connections на 25% |
| Нет | 1–3 | Оптимально |
| < 1% времени | любое | Немного увеличить (10–20%) |
| > 1% времени | любое | Удвоить |

**Критерий остановки:**
- `queued = 0` стабильно при **пиковой нагрузке** (не среднеквадратичной)
- `idle < 2 × active` (нет сильного перепровижининга)
- P99 ≤ L₀ × 1.1

---

## Фаза 3: Подбор `solr.http.client.selectors`

**Когда нужна:** CPU потоков `h2sc-selector-*` > 70% при пиковой нагрузке.

```bash
# Проверка нагрузки на selector потоки
top -H -b -n3 -d2 -p $(pgrep -f solr) | grep "h2sc-selector" | awk '{sum+=$9} END{print "total cpu:", sum"%"}'
```

```bash
# Тест с 4 selectors
SOLR_OPTS="-Dsolr.http2.maxConnectionsPerDestination=<N из Фазы 2> \
           -Dsolr.http.client.selectors=4"
```

**Ориентиры:**

| `maxConnectionsPerDestination` | Destinations на ноду | Рекомендуемые selectors |
|---|---|---|
| 4–8 | 1–10 | 2 (дефолт) |
| 8–16 | 10–50 | 4 |
| > 16 | > 50 | 4–8 |

**Ограничение:** `selectors` не должно превышать `CPU_cores / 2`. На 8-ядерной машине — максимум 4.

---

## Фаза 4: Подбор `updateQueueSize`

```bash
SOLR_OPTS="<предыдущие параметры> -Dsolr.cloud.client.updateQueueSize=200"
```

**Когда увеличивать:**
- `solr.core.executor.updateOnlyExecutor.queue > 0` при нагрузке
- В логах `Caused by: java.lang.RuntimeException: Server refused connection` или rejected update

**Когда уменьшать:**
- Очередь всегда пустая + нужно снизить memory footprint
- Каждый слот ≈ 1 KB документ: `200 × 1 KB × 230 нод = 46 MB` — незначительно

**Влияние на TCP:** косвенное. Большая очередь → runners батчат больше документов в один HTTP-запрос → меньше запросов → меньше давления на пул соединений.

---

## Фаза 5: Recovery stress-test и подбор `packetSize`

### Запуск теста

```bash
#!/usr/bin/env bash
# Запустить нагрузку на 50% от целевой
./load-index.sh 100000 8 &
LOAD_PID=$!

# Провести 3 цикла recovery под нагрузкой
./recovery-stress.sh 3

kill $LOAD_PID
```

### Подбор packetSize

```bash
# Тест 1: дефолт 1 MB
SOLR_OPTS="<параметры> -Dsolr.replication.packetSize=1048576"

# Тест 2: 4 MB (обычно оптимум на SSD)
SOLR_OPTS="<параметры> -Dsolr.replication.packetSize=4194304"

# Тест 3: 8 MB
SOLR_OPTS="<параметры> -Dsolr.replication.packetSize=8388608"
```

**Метрика:** `solr.core.replication.download_speed` (байт/сек).

**Ожидаемое поведение на SSD:**

| packetSize | download_speed | Примечание |
|---|---|---|
| 1 MB | ~100–200 MB/s | baseline |
| 4 MB | ~200–400 MB/s | меньше round-trips |
| 8 MB | ~300–500 MB/s | diminishing returns |
| 16 MB | ~300–500 MB/s | нет прироста, больше память |

**Критерий:** время recovery `index_size_GB / speed_GB_s` должно быть < SLA.

**Важно:** `packetSize` влияет на оба конца — и на сервер (лидер читает файл этими чанками), и на клиент (реплика аллоцирует буфер). Менять синхронно.

---

## Матрица результатов

| # | Конфигурация SOLR_OPTS | TCP/нода | `queued` | P99, мс | doc/sec | Recovery MB/s |
|---|---|---|---|---|---|---|
| 0 | `-Dsolr.http1=true` (baseline) | | | | | |
| 1 | HTTP/2 defaults (maxConn=4) | | | | | |
| 2 | maxConn=8 | | | | | |
| 3 | maxConn=16 | | | | | |
| 4 | maxConn=N + selectors=4 | | | | | |
| 5 | + packetSize=4M | | | | | |
| 6 | + packetSize=8M | | | | | |

---

## Итоговые рекомендуемые значения (стартовая точка)

Для **230 нод, 8 CPU, SSD, 1.6 Гбит/с, ~1KB документы**:

```bash
# solr.in.sh
SOLR_OPTS="$SOLR_OPTS \
  -Dsolr.http1=false \
  -Dsolr.http2.maxConnectionsPerDestination=8 \
  -Dsolr.http.client.selectors=4 \
  -Dsolr.replication.packetSize=4194304 \
  -Dsolr.cloud.client.updateQueueSize=100 \
  -Dsolr.cloud.replication.runners=1 \
  -Dsolr.cloud.client.pollQueueTime=10000"
```

Ожидаемый результат:
- TCP соединений на ноду: **20–40** вместо сотен при HTTP/1.1
- Throughput: ≥ 200 000 doc/sec суммарно
- Recovery speed: 200–400 MB/s (≥2× ускорение vs дефолтный 1MB packetSize)
- `queued = 0` при нормальной нагрузке

После тестирования скорректировать `maxConnectionsPerDestination` по формуле из Фазы 2.

---

## Применение патча

```bash
cd /path/to/solr-repo
git apply 0001-unhardcode-http-replication-params-and-add-metrics.patch
./gradlew :solr:core:compileJava :solr:solrj-jetty:compileJava -x test
```

Новые метрики появятся в `/solr/admin/metrics` и Prometheus-экспортере автоматически после перезапуска.
