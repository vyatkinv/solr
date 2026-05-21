# PoC: проверка патчей на 3-узловом кластере

Проверяет три вещи:
1. `acceptQueueSize` default = 1000 (патч 2)
2. Новые метрики `solr_update_client_connections` и `solr_query_client_connections` (патч 1)
3. HTTP/2 vs HTTP/1.1: разница в числе TCP-соединений под нагрузкой

---

## Требования

- Docker + Docker Compose v2
- Python 3.8+
- `ss` / `curl` (обычно есть)

---

## Быстрый старт

```bash
# 0. Из корня репозитория — собрать дистрибутив (~5 минут)
./gradlew -p solr/packaging assemble -x test

cd poc/

# 1. Скопировать дистрибутив в рабочую директорию
cp ../solr/packaging/build/distributions/solr-11.0.0-SNAPSHOT.tgz .

# 2. Собрать образ и запустить кластер
docker compose up -d --build

# 3. Дождаться готовности всех 3 нод (health check ~30–60 сек)
#    Все три должны показать "healthy" в колонке Status:
docker compose ps
#    Или подождать автоматически:
#    until [ "$(docker compose ps --format json | python3 -c "import sys,json; d=[json.loads(l) for l in sys.stdin]; print(sum(1 for x in d if x.get('Health')=='healthy'))")" = "3" ]; do sleep 5; done

# 4. Создать тестовую коллекцию (3 шарда, RF=2)
chmod +x setup-collection.sh && ./setup-collection.sh

# 5. Запустить верификацию патчей (без нагрузки)
chmod +x verify.sh && ./verify.sh
```

---

## Проверка под нагрузкой

В двух терминалах:

```bash
# Терминал 1 — нагрузка (5000 doc/s, 60 сек)
python3 load.py --rate 5000 --duration 60

# Терминал 2 — метрики (пока идёт нагрузка)
watch -n 3 './verify.sh 2>/dev/null | grep -E "HTTP|connections|backlog|✓|✗"'
```

---

## Сравнение HTTP/1.1 vs HTTP/2

Автоматически перезапускает кластер три раза с разными настройками и
показывает число TCP-соединений под нагрузкой.

```bash
chmod +x compare-http-versions.sh && ./compare-http-versions.sh
```

Ожидаемые результаты:

| Режим | TCP на порт | `state=queued` |
|---|---|---|
| HTTP/1.1 | много (по 1 на шард) | 0 (очередь не используется) |
| HTTP/2 default (4 conn) | ~4–8 | 0 при лёгкой нагрузке |
| HTTP/2 maxConn=2 | ~2–4 | может расти при нагрузке |

---

## Что именно проверяется

### acceptQueueSize (патч 2)
```bash
# Внутри контейнера
docker compose exec solr1 ss -tlnp | grep 8983
# Столбец Send-Q должен быть 1000, а не 128
```

### Метрики connection pool (патч 1)

Solr 11 отдаёт метрики только в формате Prometheus/OpenMetrics (`wt=json` возвращает 400).

```bash
# Все connection-pool метрики разом
curl -s -H "Accept: text/plain" 'http://localhost:8983/solr/admin/metrics' | \
  grep -E '^solr_(update|query)_client_connections'

# Только update-клиент
curl -s -H "Accept: text/plain" 'http://localhost:8983/solr/admin/metrics' | \
  grep '^solr_update_client_connections'
```

Ожидаемый вывод (Prometheus text format):
```
solr_update_client_connections{client="update",state="active"} 2.0
solr_update_client_connections{client="update",state="idle"}   1.0
solr_update_client_connections{client="update",state="pending"} 0.0
solr_update_client_connections{client="update",state="queued"}  0.0
solr_update_client_connections{client="recovery",state="active"} 0.0
solr_query_client_connections{state="active"} 0.0
...
```

### Конфигурируемость параметров (патч 1)
```bash
# Запустить с кастомным packetSize и убедиться что он применился
docker compose exec solr1 curl -s \
  'http://localhost:8983/solr/admin/info/system?wt=json' | \
  python3 -c "import sys,json; d=json.load(sys.stdin); \
  print(d.get('jvm',{}).get('jmx',{}))"
```

---

## Остановка

```bash
docker compose down -v   # удалить контейнеры и volume
```
