"""
Runs the full lab scenario end to end:

  1. Replication       : 20 -> 10 on the primary, secondary eventually gets 10
  2. Stale read        : replication paused, primary 10 -> 9, secondary still says 10
  3. Recovery          : replication resumed, secondary converges to 9

Starts the primary and secondary as separate processes, drives them with the
HTTP client, and prints every replica log line plus the client's observations.

  python demo.py
"""

import subprocess
import sys
import threading
import time

import client

PY = sys.executable


def stream(proc):
    for line in proc.stdout:
        print(line, end="", flush=True)


def start(args):
    p = subprocess.Popen([PY, "-u", "replica.py", *args], stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT, text=True)
    threading.Thread(target=stream, args=(p,), daemon=True).start()
    return p


def wait_up(url):
    for _ in range(50):
        try:
            client._req(url + "/health")
            return
        except Exception:
            time.sleep(0.1)
    raise RuntimeError(f"{url} did not start")


def show(label):
    p = client.read(client.PRIMARY)
    s = client.read(client.SECONDARY)
    stale = "  <-- STALE READ" if s["value"] != p["value"] else ""
    print(f"    [CLIENT] {label:<28} primary: stock={p['value']} (v{p['version']})"
          f" | secondary: stock={s['value']} (v{s['version']}){stale}", flush=True)
    time.sleep(0.05)  # let replica log lines flush before the next step
    return p["value"], s["value"]


def wait_converged(timeout=10):
    start_t = time.time()
    while time.time() - start_t < timeout:
        p = client.read(client.PRIMARY)["value"]
        s = client.read(client.SECONDARY)["value"]
        if p == s:
            return time.time() - start_t
        time.sleep(0.1)
    raise AssertionError("replicas did not converge")


def banner(text):
    print(f"\n{'=' * 78}\n {text}\n{'=' * 78}", flush=True)


def main():
    sec = start(["--role", "secondary", "--port", "8002"])
    pri = start(["--role", "primary", "--port", "8001", "--peer", client.SECONDARY])
    try:
        wait_up(client.PRIMARY)
        wait_up(client.SECONDARY)
        time.sleep(0.2)
        show("initial state")

        # ------------------------------------------------------------------
        banner("TEST 1: REPLICATION  (write stock=10 to primary)")
        client.set_delay(1.0)  # 1s of replication lag so "eventually" is visible
        time.sleep(0.05)
        client.write("stock", 10)
        p, s = show("right after write")
        assert p == 10
        took = wait_converged()
        p, s = show(f"after {took:.1f}s")
        assert p == s == 10
        print("    [RESULT] PASS - secondary received the write; both replicas return 10")

        # ------------------------------------------------------------------
        banner("TEST 2: STALE READ  (pause replication, write stock=9)")
        client.set_delay(0)
        client.pause_replication()
        time.sleep(0.05)
        client.write("stock", 9)
        for i in range(3):
            time.sleep(1)
            p, s = show(f"t+{i + 1}s (replication paused)")
            assert p == 9 and s == 10
        print("    [RESULT] PASS - primary returns 9, secondary returns old value 10 (stale read)")

        # ------------------------------------------------------------------
        banner("TEST 3: RECOVERY & CONVERGENCE  (resume replication)")
        client.resume_replication()
        took = wait_converged()
        time.sleep(0.05)
        p, s = show(f"after resume (+{took:.1f}s)")
        assert p == s == 9
        print("    [RESULT] PASS - secondary caught up to 9; replicas have converged")

        banner("ALL TESTS PASSED")
    finally:
        pri.terminate()
        sec.terminate()
        pri.wait()
        sec.wait()


if __name__ == "__main__":
    main()
