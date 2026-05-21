# PoC: проверка патчей на 3-узловом кластере

Проверяет три вещи:
1. `acceptQueueSize` default = 1000 (патч 2)
2. Новые метрики `solr.update.client.connections` и `solr.query.client.connections` (патч 1)
3. HTTP/2 vs HTTP/1.1: разница в числе TCP-соединений под нагрузкой

---

## Требования

- Docker + Docker Compose v2
- Python 3.8+
- `ss` / `curl` (обычно есть)

---

## Быстрый старт

```bash
cd poc/

# 1. Скопировать дистрибутив в рабочую директорию
cp ../solr/packaging/build/distributions/solr-11.0.0-SNAPSHOT.tgz .

# 2. Собрать образ и запустить кластер
docker compose up -d --build

# 3. Дождаться готовности всех 3 нод (Solr стартует ~30 сек)
docker compose ps

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
```bash
curl 'http://localhost:8983/solr/admin/metrics?prefix=solr.update.client.connections&wt=json' | python3 -m json.tool
curl 'http://localhost:8983/solr/admin/metrics?prefix=solr.query.client.connections&wt=json'  | python3 -m json.tool
```

Ожидаемые ключи метрик:
```
solr.update.client.connections{client=update,state=active}
solr.update.client.connections{client=update,state=idle}
solr.update.client.connections{client=update,state=pending}
solr.update.client.connections{client=update,state=queued}
solr.update.client.connections{client=recovery,...}
solr.query.client.connections{state=active}
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
