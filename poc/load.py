#!/usr/bin/env python3
"""
Генерирует write-нагрузку: отправляет документы по одному или батчами.

Использование:
  python3 load.py                    # 5000 doc/sec, 30 сек, батч 100
  python3 load.py --rate 20000 --duration 60 --batch 200
  python3 load.py --rate 1000 --duration 120 --threads 4
"""
import argparse
import json
import random
import string
import time
import threading
import urllib.request
import urllib.error
from collections import deque

SOLR_URL = "http://localhost:8983/solr/test_write/update?commit=false"
HEADERS = {"Content-Type": "application/json"}

stats = {"sent": 0, "errors": 0}
stats_lock = threading.Lock()
rate_window = deque(maxlen=10)  # last 10 seconds for moving avg


def random_doc(i):
    payload = "".join(random.choices(string.ascii_letters, k=800))
    return {
        "id": f"doc-{i}-{random.randint(0, 99999)}",
        "title_s": f"Document {i}",
        "body_t": payload,
        "ts_tdt": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "seq_l": i,
    }


def send_batch(docs):
    data = json.dumps(docs).encode()
    req = urllib.request.Request(SOLR_URL, data=data, headers=HEADERS, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10):
            pass
        return len(docs)
    except Exception as e:
        with stats_lock:
            stats["errors"] += 1
        return 0


def worker(rate_per_thread, duration, batch_size, start_event, doc_counter):
    start_event.wait()
    t_end = time.monotonic() + duration
    interval = batch_size / rate_per_thread  # seconds between batches

    while time.monotonic() < t_end:
        batch_start = time.monotonic()
        base = next(doc_counter)
        docs = [random_doc(base + j) for j in range(batch_size)]
        sent = send_batch(docs)
        with stats_lock:
            stats["sent"] += sent

        elapsed = time.monotonic() - batch_start
        sleep_for = interval - elapsed
        if sleep_for > 0:
            time.sleep(sleep_for)


def reporter(duration, start_event):
    start_event.wait()
    t0 = time.monotonic()
    prev_sent = 0
    while time.monotonic() - t0 < duration:
        time.sleep(1)
        now_sent = stats["sent"]
        rps = now_sent - prev_sent
        prev_sent = now_sent
        rate_window.append(rps)
        avg = sum(rate_window) / len(rate_window)
        elapsed = time.monotonic() - t0
        print(
            f"  t={elapsed:5.1f}s  rate={rps:6d} doc/s  avg={avg:6.0f} doc/s  "
            f"total={now_sent:8d}  errors={stats['errors']}",
            flush=True,
        )


class AtomicCounter:
    def __init__(self):
        self._v = 0
        self._lock = threading.Lock()

    def __next__(self):
        with self._lock:
            v = self._v
            self._v += 1
        return v * 1000


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--rate", type=int, default=5000, help="Target doc/sec (total)")
    p.add_argument("--duration", type=int, default=30, help="Duration in seconds")
    p.add_argument("--batch", type=int, default=100, help="Docs per HTTP request")
    p.add_argument("--threads", type=int, default=4, help="Sender threads")
    p.add_argument("--url", default=SOLR_URL, help="Solr update URL")
    args = p.parse_args()

    global SOLR_URL
    SOLR_URL = args.url

    rate_per_thread = args.rate // args.threads
    print(
        f"Load test: {args.rate} doc/s target, {args.duration}s, "
        f"batch={args.batch}, threads={args.threads}"
    )
    print(f"  → {rate_per_thread} doc/s per thread, URL: {SOLR_URL}")
    print()

    start_event = threading.Event()
    counter = AtomicCounter()

    threads = [
        threading.Thread(
            target=worker,
            args=(rate_per_thread, args.duration, args.batch, start_event, counter),
            daemon=True,
        )
        for _ in range(args.threads)
    ]
    rep = threading.Thread(
        target=reporter, args=(args.duration, start_event), daemon=True
    )
    rep.start()
    for t in threads:
        t.start()

    start_event.set()
    for t in threads:
        t.join()
    rep.join()

    print(f"\nDone. Total sent: {stats['sent']}, errors: {stats['errors']}")


if __name__ == "__main__":
    main()
