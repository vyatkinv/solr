#!/usr/bin/env python3
"""
ZooKeeper Session Expiry — Proof of Concept
============================================

Воспроизводит три дефекта Solr + демонстрирует исправления:

  Scenario 1 (OLD)  Thundering herd: плоский 1s retry + мгновенный onReconnect
                    → все 20 нод бьют ZK одновременно → перегрузка → каскад

  Scenario 2 (NEW)  То же, но exponential backoff + random jitter перед
                    onReconnect → нагрузка размазана → полное восстановление

  Scenario 3 (OLD)  Stale ephemeral node: NodeExistsError после reconnect
                    → reconnect-цикл падает, нода не восстанавливается

  Scenario 4 (NEW)  delete stale node + recreate → успех

Инфраструктура:
  ZooKeeper 3.9  ←  Toxiproxy (partition injects real session expiry)  ←  Python/kazoo
"""

from __future__ import annotations
import os, sys, time, random, threading, logging
from collections import deque
from dataclasses import dataclass, field
from typing import Optional
import requests
from kazoo.client import KazooClient, KazooState
from kazoo.exceptions import NodeExistsError, NoNodeError, SessionExpiredError, ConnectionLoss
from kazoo.retry import KazooRetry

# ── Config ────────────────────────────────────────────────────────────────────
ZK_DIRECT       = os.environ.get("ZK_DIRECT",           "localhost:2181")
ZK_PROXY        = os.environ.get("ZK_PROXY",            "localhost:21810")
TOXIPROXY_URL   = os.environ.get("TOXIPROXY_URL",       "http://localhost:8474")
N_NODES         = int(os.environ.get("N_NODES",          "20"))
SESSION_TIMEOUT = int(os.environ.get("SESSION_TIMEOUT_MS","8000")) / 1000
LIVE_PATH       = "/solr/live_nodes"
STATE_PATH      = "/solr/state"
JITTER_MAX_MS   = 3000      # max random delay before onReconnect (NEW behaviour)
OVERLOAD_THRESH = 40        # simulated ZK saturation limit (writes/s)

# ── Terminal colours ──────────────────────────────────────────────────────────
B = "\033[1m"; R = "\033[31m"; G = "\033[32m"; Y = "\033[33m"
C = "\033[36m"; RESET = "\033[0m"

logging.basicConfig(level=logging.WARNING,
                    format="%(asctime)s %(levelname)-5s %(message)s",
                    datefmt="%H:%M:%S")

# ─────────────────────────────────────────────────────────────────────────────
# Metrics
# ─────────────────────────────────────────────────────────────────────────────
@dataclass
class Metrics:
    label: str
    writes:          int = 0
    write_errors:    int = 0
    reconnect_ok:    int = 0
    reconnect_fail:  int = 0
    stale_handled:   int = 0
    cascades:        int = 0
    _lock:  threading.Lock = field(default_factory=threading.Lock, repr=False)
    _ts:    deque          = field(default_factory=lambda: deque(maxlen=20_000), repr=False)
    _times: list           = field(default_factory=list, repr=False)

    def write(self):
        with self._lock:
            self.writes += 1
            self._ts.append(time.monotonic())

    def write_err(self):
        with self._lock: self.write_errors += 1

    def ok(self, elapsed: float):
        with self._lock:
            self.reconnect_ok += 1
            self._times.append(elapsed)

    def fail(self):
        with self._lock: self.reconnect_fail += 1

    def stale(self):
        with self._lock: self.stale_handled += 1

    def cascade(self):
        with self._lock: self.cascades += 1

    def peak_wps(self, window: float = 1.0) -> int:
        with self._lock: ts = sorted(self._ts)
        if len(ts) < 2: return len(ts)
        peak, left = 0, 0
        for right in range(len(ts)):
            while ts[right] - ts[left] > window: left += 1
            peak = max(peak, right - left + 1)
        return peak

    def recovery_time(self) -> float:
        with self._lock: return max(self._times) if self._times else float("inf")

    def recovered(self) -> int:
        with self._lock: return len(self._times)

# ─────────────────────────────────────────────────────────────────────────────
# Simulated ZK overload (mirrors "1.8 Gbit/s" congestion scenario)
# ─────────────────────────────────────────────────────────────────────────────
class ZkLoad:
    """
    Shared write-rate tracker.  When writes/s > OVERLOAD_THRESH, there is a
    rising probability that a write fails — simulating ZK dropping client
    connections due to request-queue saturation.
    """
    def __init__(self, m: Metrics):
        self._m = m
        self._lock = threading.Lock()
        self._ts: deque = deque(maxlen=10_000)

    def write(self, zk: KazooClient, path: str, data: bytes) -> bool:
        with self._lock:
            now = time.monotonic()
            self._ts.append(now)
            wps = sum(1 for t in self._ts if now - t <= 1.0)

        if wps > OVERLOAD_THRESH:
            factor = min(1.0, (wps - OVERLOAD_THRESH) / OVERLOAD_THRESH)
            if random.random() < factor * 0.85:
                self._m.write_err()
                self._m.cascade()
                return False
        try:
            if zk.exists(path): zk.set(path, data)
            else:               zk.create(path, data, makepath=True)
            self._m.write()
            return True
        except (SessionExpiredError, ConnectionLoss):
            self._m.write_err()
            self._m.cascade()
            return False

    def wps(self) -> int:
        with self._lock:
            now = time.monotonic()
            return sum(1 for t in self._ts if now - t <= 1.0)

# ─────────────────────────────────────────────────────────────────────────────
# Toxiproxy helpers
# ─────────────────────────────────────────────────────────────────────────────
def _toxi(method: str, path: str, **kw):
    r = requests.request(method, f"{TOXIPROXY_URL}{path}", **kw)
    r.raise_for_status()
    return r.json() if r.content else {}

def block_zk():
    try: _toxi("DELETE", "/proxies/zookeeper/toxics/partition")
    except: pass
    _toxi("POST", "/proxies/zookeeper/toxics", json={
        "name": "partition", "type": "timeout", "stream": "upstream",
        "toxicity": 1.0, "attributes": {"timeout": 0},
    })

def unblock_zk():
    try: _toxi("DELETE", "/proxies/zookeeper/toxics/partition")
    except: pass

# ─────────────────────────────────────────────────────────────────────────────
# ZK admin helpers
# ─────────────────────────────────────────────────────────────────────────────
def admin_client() -> KazooClient:
    c = KazooClient(hosts=ZK_DIRECT, timeout=10,
                    connection_retry=KazooRetry(max_tries=10, delay=0.5))
    c.start(timeout=20); return c

def setup_paths(admin: KazooClient):
    admin.ensure_path(LIVE_PATH); admin.ensure_path(STATE_PATH)

def reset_paths(admin: KazooClient):
    for p in (LIVE_PATH, STATE_PATH):
        try: admin.delete(p, recursive=True)
        except NoNodeError: pass
    admin.ensure_path(LIVE_PATH); admin.ensure_path(STATE_PATH)

# ─────────────────────────────────────────────────────────────────────────────
# onReconnect simulation
# ─────────────────────────────────────────────────────────────────────────────
def on_reconnect_old(node_id: int, zk: KazooClient,
                     load: ZkLoad, m: Metrics) -> bool:
    """
    Mirrors ZkController.onReconnect() — OLD: no NodeExistsError handling.
    Returns True on full success.
    """
    live  = f"{LIVE_PATH}/node-{node_id:03d}"
    state = f"{STATE_PATH}/node-{node_id:03d}"
    try:
        zk.get_children(LIVE_PATH)
        for shard in range(2):
            if not load.write(zk, f"{state}/shard-{shard}", b"DOWN"):
                m.fail(); return False
        # OLD: NodeExistsError is NOT handled → exception propagates
        zk.create(live, b"live", ephemeral=True, makepath=True)
        m.write()
        for shard in range(2):
            if not load.write(zk, f"{state}/shard-{shard}", b"ACTIVE"):
                m.fail(); return False
        return True
    except NodeExistsError:
        # OLD behaviour: exception propagates, reconnect fails
        m.fail(); return False
    except (SessionExpiredError, ConnectionLoss):
        m.fail(); return False

def on_reconnect_new(node_id: int, zk: KazooClient,
                     load: ZkLoad, m: Metrics) -> bool:
    """
    NEW: NodeExistsError → delete stale node + recreate (Bug 3 fix).
    """
    live  = f"{LIVE_PATH}/node-{node_id:03d}"
    state = f"{STATE_PATH}/node-{node_id:03d}"
    try:
        zk.get_children(LIVE_PATH)
        for shard in range(2):
            if not load.write(zk, f"{state}/shard-{shard}", b"DOWN"):
                m.fail(); return False
        # NEW: handle stale ephemeral node left from expired session
        try:
            zk.create(live, b"live", ephemeral=True, makepath=True)
        except NodeExistsError:
            m.stale()
            try: zk.delete(live)
            except NoNodeError: pass
            zk.create(live, b"live", ephemeral=True, makepath=True)
        m.write()
        for shard in range(2):
            if not load.write(zk, f"{state}/shard-{shard}", b"ACTIVE"):
                m.fail(); return False
        return True
    except (SessionExpiredError, ConnectionLoss):
        m.fail(); return False

# ─────────────────────────────────────────────────────────────────────────────
# Node thread  — uses kazoo state listener so we detect the real
#                LOST → CONNECTED (new session) transition, not a stale one.
# ─────────────────────────────────────────────────────────────────────────────
def node_thread(node_id: int,
                ready_barrier: threading.Barrier,
                start_expiry: threading.Event,
                m: Metrics, load: ZkLoad,
                reconnect_fn,     # on_reconnect_old or on_reconnect_new
                jitter_max_ms: int,
                recovery_deadline: float):
    """
    One simulated Solr node.

    State machine:
      SETUP → wait for expiry signal → wait for LOST event (true expiry)
      → wait for new-session CONNECTED event → optional jitter
      → run onReconnect ops (with retry on failure)
    """
    # ── Events driven by kazoo state transitions ──────────────────────────
    lost_ev       = threading.Event()   # session truly expired
    new_conn_ev   = threading.Event()   # new session CONNECTED after LOST

    def listener(state):
        if state == KazooState.LOST:
            lost_ev.set()
            new_conn_ev.clear()
        elif state == KazooState.CONNECTED and lost_ev.is_set():
            new_conn_ev.set()

    try:
        zk = KazooClient(
            hosts=ZK_PROXY,
            timeout=SESSION_TIMEOUT,
            connection_retry=KazooRetry(max_tries=-1, delay=0.1, max_delay=0.5),
        )
        zk.add_listener(listener)
        zk.start(timeout=20)

        # Pre-expiry: create ephemeral live-node (represents Solr being "live")
        live = f"{LIVE_PATH}/node-{node_id:03d}"
        try:
            zk.create(live, b"live", ephemeral=True, makepath=True)
        except NodeExistsError:
            pass

        ready_barrier.wait()        # signal: "I'm up, waiting for partition"
        start_expiry.wait()         # main thread fires this after unblock_zk()

        # ── Wait for ZK to declare the session expired (LOST state) ──────
        if not lost_ev.wait(timeout=SESSION_TIMEOUT * 2 + 5):
            m.fail(); return        # session never expired — test setup issue

        t_expired = time.monotonic()

        # ── Wait for kazoo to establish a NEW session (CONNECTED after LOST) ──
        if not new_conn_ev.wait(timeout=30):
            m.fail(); return        # couldn't reconnect at all

        # ── Reconnect loop ────────────────────────────────────────────────
        attempt = 0
        while time.monotonic() < recovery_deadline:
            # OLD: no jitter  |  NEW: random delay before heavy ZK ops
            if jitter_max_ms > 0:
                jitter_s = random.uniform(0, jitter_max_ms / 1000)
                time.sleep(jitter_s)

            ok = reconnect_fn(node_id, zk, load, m)
            if ok:
                m.ok(time.monotonic() - t_expired)
                break

            # onReconnect failed (ZK overloaded or stale node unhandled)
            # OLD: flat 1s retry  |  NEW: exponential backoff
            if jitter_max_ms > 0:
                base_ms = min(30_000, 1_000 * (1 << min(attempt, 5)))
                sleep_ms = base_ms + random.randrange(base_ms // 2 + 1)
            else:
                sleep_ms = 1_000

            # Re-wait for connection if it dropped again during ops
            if zk.state != KazooState.CONNECTED:
                new_conn_ev.clear()
                if not new_conn_ev.wait(timeout=sleep_ms / 1000 + 10):
                    break

            time.sleep(sleep_ms / 1000)
            attempt += 1

    except Exception as exc:
        logging.warning("node-%03d unhandled: %s", node_id, exc)
    finally:
        try: zk.stop(); zk.close()
        except: pass

# ─────────────────────────────────────────────────────────────────────────────
# Thundering-herd scenario runner
# ─────────────────────────────────────────────────────────────────────────────
def run_scenario(label: str, reconnect_fn, jitter_max_ms: int,
                 m: Metrics, load: ZkLoad, admin: KazooClient):
    print(f"\n{'─'*62}")
    print(f"{B}  {label}{RESET}")
    print(f"{'─'*62}")
    print(f"  Nodes: {N_NODES}  |  Session TO: {SESSION_TIMEOUT:.0f}s  |"
          f"  ZK overload @ >{OVERLOAD_THRESH} writes/s"
          + (f"  |  jitter: 0–{jitter_max_ms}ms" if jitter_max_ms else "  |  jitter: none"))

    reset_paths(admin)

    ready_barrier     = threading.Barrier(N_NODES + 1)
    start_expiry      = threading.Event()
    recovery_deadline = time.monotonic() + 90.0

    threads = [
        threading.Thread(
            target=node_thread,
            args=(i, ready_barrier, start_expiry,
                  m, load, reconnect_fn, jitter_max_ms, recovery_deadline),
            daemon=True,
        )
        for i in range(N_NODES)
    ]
    for t in threads: t.start()

    # Wait until all nodes are connected and have their live-nodes
    ready_barrier.wait()
    time.sleep(0.5)
    live_before = len(admin.get_children(LIVE_PATH))
    print(f"  {G}▸ {live_before}/{N_NODES} nodes live before partition{RESET}")

    # Inject partition
    print(f"  {Y}▸ Injecting network partition via toxiproxy…{RESET}")
    block_zk()

    wait_s = SESSION_TIMEOUT + 3.0
    print(f"  {Y}▸ Waiting {wait_s:.0f}s for ZK to expire sessions…{RESET}")
    time.sleep(wait_s)

    # Restore network — sessions are now truly expired on ZK side
    unblock_zk()
    print(f"  {Y}▸ Network restored — kazoo will establish new sessions{RESET}")
    start_expiry.set()   # unblock node threads

    # ── Progress tracking ─────────────────────────────────────────────────
    t_start = time.monotonic()
    last_rep = -1.0
    while time.monotonic() < recovery_deadline:
        rec     = m.recovered()
        wps     = load.wps()
        elapsed = time.monotonic() - t_start
        if elapsed - last_rep >= 2.0 or rec == N_NODES:
            fill  = int(rec / N_NODES * 30)
            bar   = "█" * fill + "░" * (30 - fill)
            print(f"  [{bar}] {rec:3}/{N_NODES}  {wps:3} wr/s  "
                  f"{m.cascades:4} cascades  {elapsed:5.1f}s")
            last_rep = elapsed
        if rec == N_NODES:
            break
        time.sleep(0.5)

    for t in threads: t.join(timeout=3)
    live_after = len(admin.get_children(LIVE_PATH))

    # ── Result table ──────────────────────────────────────────────────────
    print()
    print(f"  {'─'*48}")
    rows = [
        ("Nodes recovered",         f"{m.recovered()} / {N_NODES}"),
        ("Recovery time (s)",       f"{m.recovery_time():.1f}"
                                    if m.recovery_time() < 1e9 else "∞  (timeout)"),
        ("Peak ZK writes/s",        str(m.peak_wps())),
        ("Total ZK writes",         str(m.writes)),
        ("Write errors",            str(m.write_errors)),
        ("Cascade expiries",        str(m.cascades)),
        ("Stale nodes handled",     str(m.stale_handled)),
        ("Reconnect failures",      str(m.reconnect_fail)),
        ("Live nodes in ZK after",  str(live_after)),
    ]
    for k, v in rows:
        print(f"  {k:<32} {v}")

    ok = m.recovered() == N_NODES
    color = G if ok else R
    print(f"\n  {color}{B}{'✓ FULL RECOVERY' if ok else '✗ NO FULL RECOVERY'}{RESET}")
    return m

# ─────────────────────────────────────────────────────────────────────────────
# Stale live-node demonstration
# ─────────────────────────────────────────────────────────────────────────────
def demo_stale_node(admin: KazooClient):
    print(f"\n{'═'*62}")
    print(f"{B}  Scenarios 3 & 4 — Stale Ephemeral Live-Node{RESET}")
    print(f"{'═'*62}")
    print("  Race condition: ZK async cleanup vs fast reconnect")
    print("  S1 = 'old session' (keeps node alive)  |  S2 = 'new session'\n")

    path = f"{LIVE_PATH}/stale-demo"

    # Ensure path exists
    admin.ensure_path(LIVE_PATH)
    try: admin.delete(path)
    except NoNodeError: pass

    # S1 creates ephemeral node (simulates: session expired but node still exists)
    s1 = KazooClient(hosts=ZK_DIRECT, timeout=30,
                     connection_retry=KazooRetry(max_tries=5))
    s1.start(timeout=10)
    s1.create(path, b"s1-old-session", ephemeral=True, makepath=True)
    print(f"  {G}▸ S1 created {path}{RESET}")
    print(f"    (ZK has not yet cleaned up the node from the expired session)\n")

    s2 = KazooClient(hosts=ZK_DIRECT, timeout=10,
                     connection_retry=KazooRetry(max_tries=5))
    s2.start(timeout=10)

    # ── OLD ───────────────────────────────────────────────────────────────
    print(f"  {B}OLD — no NodeExistsError handling:{RESET}")
    try:
        s2.create(path, b"s2-new-session", ephemeral=True)
        print(f"  {G}  Created (unexpected — node should still exist){RESET}")
    except NodeExistsError:
        print(f"  {R}  ✗ NodeExistsError → ConnectionManager.update() catches it as")
        print(f"        RuntimeException → closeKeeper(newSession) → retry whole loop")
        print(f"        → creates+destroys a ZK session per attempt until ZK cleanup{RESET}")

    # ── NEW ───────────────────────────────────────────────────────────────
    print(f"\n  {B}NEW — delete stale node + recreate:{RESET}")
    try:
        s2.create(path, b"s2-new-session", ephemeral=True)
        print(f"  {G}  Created on first try{RESET}")
    except NodeExistsError:
        print(f"  {Y}  NodeExistsError caught — deleting stale node from expired session…{RESET}")
        stale = s2.get(path)[0]
        print(f"  {Y}  Stale data: {stale}{RESET}")
        s2.delete(path)
        s2.create(path, b"s2-new-session", ephemeral=True)
        d, stat = s2.get(path)
        print(f"  {G}  ✓ Recreated  data={d}  owner=session {stat.ephemeralOwner:#x}{RESET}")

    print(f"\n  OLD: {R}✗ FAILED{RESET}   exception propagates, reconnect loop aborted")
    print(f"  NEW: {G}✓ SUCCESS{RESET}  stale node cleaned up, node back in /live_nodes")

    for c in (s1, s2):
        c.stop(); c.close()

# ─────────────────────────────────────────────────────────────────────────────
# Summary
# ─────────────────────────────────────────────────────────────────────────────
def summary(m_old: Metrics, m_new: Metrics):
    print(f"\n{'═'*62}")
    print(f"{B}  SUMMARY — OLD vs NEW  ({N_NODES} nodes, {SESSION_TIMEOUT:.0f}s session TO){RESET}")
    print(f"{'═'*62}")
    print(f"  {'Metric':<34} {'OLD':>8}   {'NEW':>8}")
    print(f"  {'─'*54}")

    def row(label, ov, nv, lo_good=True):
        os = f"{ov:.1f}" if isinstance(ov, float) else str(ov)
        ns = f"{nv:.1f}" if isinstance(nv, float) else str(nv)
        oc = (R if (lo_good and ov > nv) or (not lo_good and ov < nv) else G)
        nc = (G if (lo_good and nv < ov) or (not lo_good and nv > ov) else G)
        if ov == nv: oc = nc = RESET
        print(f"  {label:<34} {oc}{os:>8}{RESET}   {nc}{ns:>8}{RESET}")

    row("Nodes recovered (↑)",      m_old.recovered(), m_new.recovered(), lo_good=False)
    row("Recovery time s (↓)",      m_old.recovery_time(), m_new.recovery_time())
    row("Peak ZK writes/s (↓)",     m_old.peak_wps(),    m_new.peak_wps())
    row("Write errors (↓)",         m_old.write_errors,  m_new.write_errors)
    row("Cascade expiries (↓)",     m_old.cascades,      m_new.cascades)
    row("Reconnect failures (↓)",   m_old.reconnect_fail,m_new.reconnect_fail)
    row("Stale nodes handled (↑)",  m_old.stale_handled, m_new.stale_handled, lo_good=False)

    print(f"\n  {'─'*54}")
    ok_old = m_old.recovered() == N_NODES
    ok_new = m_new.recovered() == N_NODES
    print(f"  Full recovery:     "
          f"OLD {'  ' + G + '✓' if ok_old else R + '✗'}{RESET}"
          f"  →  NEW {'  ' + G + '✓' if ok_new else R + '✗'}{RESET}")
    print(f"  Peak load down:    {G}✓{RESET}" if m_new.peak_wps() < m_old.peak_wps() else
          f"  Peak load down:    {Y}≈{RESET}")
    print(f"  Cascades stopped:  {G}✓{RESET}" if m_new.cascades < m_old.cascades else
          f"  Cascades stopped:  {Y}≈{RESET}")

# ─────────────────────────────────────────────────────────────────────────────
# Bootstrap
# ─────────────────────────────────────────────────────────────────────────────
def wait_services():
    print(f"{C}Waiting for Toxiproxy…{RESET}", flush=True)
    deadline = time.time() + 90
    while time.time() < deadline:
        try:
            r = requests.get(f"{TOXIPROXY_URL}/proxies", timeout=2)
            if r.ok: break
        except: pass
        time.sleep(1)
    else:
        sys.exit("Toxiproxy not ready")

    # Ensure proxy exists (may already be defined via config file)
    try:
        _toxi("POST", "/proxies", json={
            "name": "zookeeper", "listen": "0.0.0.0:21810",
            "upstream": "zookeeper:2181", "enabled": True,
        })
    except: pass

    print(f"{C}Waiting for ZooKeeper…{RESET}", flush=True)
    deadline = time.time() + 90
    while time.time() < deadline:
        try:
            c = KazooClient(hosts=ZK_DIRECT, timeout=3)
            c.start(timeout=5); c.stop(); c.close(); break
        except: time.sleep(1)
    else:
        sys.exit("ZooKeeper not ready")

    unblock_zk()
    print(f"{G}Services ready.{RESET}\n")


def main():
    print(f"""
{B}{'═'*62}
  ZooKeeper Session Expiry — Proof of Concept
  {N_NODES}-node Solr cluster simulation
{'═'*62}{RESET}
  Scenarios 1+2: Thundering herd (flat retry vs backoff+jitter)
  Scenarios 3+4: Stale ephemeral live-node (crash vs fix)

  ZK direct  : {ZK_DIRECT}
  ZK proxy   : {ZK_PROXY}   (Toxiproxy — injects real session expiry)
  Session TO : {SESSION_TIMEOUT:.0f}s
  Jitter max : {JITTER_MAX_MS}ms (NEW behaviour)
""")

    wait_services()
    admin = admin_client()
    setup_paths(admin)

    # ── Thundering herd: Scenarios 1 & 2 ─────────────────────────────────
    print(f"{B}{'═'*62}")
    print(f"  Scenarios 1 & 2 — Thundering Herd")
    print(f"{'═'*62}{RESET}")

    m_old  = Metrics("OLD")
    load_old = ZkLoad(m_old)
    run_scenario(
        f"Scenario 1 — OLD  (flat 1s retry, onReconnect fires immediately)",
        on_reconnect_old, 0, m_old, load_old, admin,
    )

    time.sleep(3); unblock_zk()

    m_new  = Metrics("NEW")
    load_new = ZkLoad(m_new)
    run_scenario(
        f"Scenario 2 — NEW  (exp backoff + {JITTER_MAX_MS}ms jitter before onReconnect)",
        on_reconnect_new, JITTER_MAX_MS, m_new, load_new, admin,
    )

    summary(m_old, m_new)

    # ── Stale live-node: Scenarios 3 & 4 ─────────────────────────────────
    demo_stale_node(admin)

    admin.stop(); admin.close()

    print(f"\n{B}{'═'*62}{RESET}")
    rec = m_new.recovered()
    if rec == N_NODES:
        print(f"{G}{B}  ALL {N_NODES} NODES RECOVERED with the fix.{RESET}")
        print(f"{G}  Thundering-herd eliminated. Stale-node bug fixed.{RESET}")
    else:
        print(f"{Y}  {rec}/{N_NODES} nodes recovered (may need longer deadline or "
              f"lower overload threshold).{RESET}")
    print(f"{B}{'═'*62}{RESET}\n")


if __name__ == "__main__":
    main()
