"""
A single inventory replica (Primary or Secondary) exposed over HTTP.

Primary:
  - accepts writes from clients
  - appends each write to a replication queue
  - a background worker ships queued writes to the Secondary (async replication)
  - replication can be paused / resumed / delayed through admin endpoints

Secondary:
  - read-only for clients
  - applies writes it receives from the Primary on /replicate

Run:
  python replica.py --role primary   --port 8001 --peer http://127.0.0.1:8002
  python replica.py --role secondary --port 8002
"""

import argparse
import json
import queue
import threading
import time
import urllib.request
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse


_print_lock = threading.Lock()


def log(role, msg):
    ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    with _print_lock:
        print(f"[{ts}] [{role.upper():9}] {msg}", flush=True)


class Replica:
    def __init__(self, role, peer=None, initial_stock=20):
        self.role = role
        self.peer = peer
        self.lock = threading.Lock()
        self.data = {"stock": initial_stock}
        self.version = 0  # last applied write sequence number

        # replication state (primary only)
        self.repl_queue = queue.Queue()
        self.repl_enabled = threading.Event()
        self.repl_enabled.set()
        self.repl_delay = 0.0
        if role == "primary":
            threading.Thread(target=self._replication_worker, daemon=True).start()

    # ---------- client operations ----------
    def read(self, key):
        with self.lock:
            return self.data.get(key), self.version

    def write(self, key, value):
        if self.role != "primary":
            raise PermissionError("writes must go to the primary")
        with self.lock:
            self.version += 1
            self.data[key] = value
            entry = {"key": key, "value": value, "version": self.version}
        log(self.role, f"WRITE  {key}={value} (v{entry['version']}) committed locally")
        self.repl_queue.put(entry)
        log(self.role, f"QUEUE  v{entry['version']} queued for replication "
                       f"(pending={self.repl_queue.qsize()})")
        return entry

    # ---------- replication ----------
    def apply(self, entry):
        """Secondary applies a replicated write (ignores old/duplicate versions)."""
        with self.lock:
            if entry["version"] <= self.version:
                return False
            self.data[entry["key"]] = entry["value"]
            self.version = entry["version"]
        log(self.role, f"APPLY  {entry['key']}={entry['value']} (v{entry['version']}) from primary")
        return True

    def _replication_worker(self):
        while True:
            entry = self.repl_queue.get()
            while True:
                if not self.repl_enabled.is_set():
                    log(self.role, f"PAUSED replication is paused; v{entry['version']} waiting")
                    self.repl_enabled.wait()
                    log(self.role, "RESUME replication resumed")
                if self.repl_delay:
                    time.sleep(self.repl_delay)
                if not self.repl_enabled.is_set():
                    continue  # paused while we slept, go back and wait
                try:
                    _post(self.peer + "/replicate", entry)
                    log(self.role, f"SHIP   v{entry['version']} -> secondary OK")
                    break
                except Exception as e:  # secondary down: retry until it comes back
                    log(self.role, f"SHIP   v{entry['version']} failed ({e}); retrying")
                    time.sleep(0.5)

    def pause(self):
        self.repl_enabled.clear()
        log(self.role, "ADMIN  replication PAUSED (simulating network delay/partition)")

    def resume(self):
        self.repl_enabled.set()
        log(self.role, "ADMIN  replication RESUMED")


def _post(url, body):
    req = urllib.request.Request(url, data=json.dumps(body).encode(),
                                 headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=2) as r:
        return json.loads(r.read() or b"{}")


def make_handler(replica):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):  # silence default access log
            pass

        def _send(self, code, body):
            payload = json.dumps(body).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def _body(self):
            n = int(self.headers.get("Content-Length", 0))
            return json.loads(self.rfile.read(n) or b"{}")

        def do_GET(self):
            u = urlparse(self.path)
            if u.path == "/read":
                key = parse_qs(u.query).get("key", ["stock"])[0]
                value, version = replica.read(key)
                self._send(200, {"replica": replica.role, "key": key,
                                 "value": value, "version": version})
            elif u.path == "/health":
                self._send(200, {"ok": True, "role": replica.role})
            else:
                self._send(404, {"error": "not found"})

        def do_POST(self):
            path = urlparse(self.path).path
            try:
                if path == "/write":
                    b = self._body()
                    self._send(200, replica.write(b["key"], b["value"]))
                elif path == "/replicate" and replica.role == "secondary":
                    self._send(200, {"applied": replica.apply(self._body())})
                elif path == "/admin/pause" and replica.role == "primary":
                    replica.pause()
                    self._send(200, {"replication": "paused"})
                elif path == "/admin/resume" and replica.role == "primary":
                    replica.resume()
                    self._send(200, {"replication": "running"})
                elif path == "/admin/delay" and replica.role == "primary":
                    replica.repl_delay = float(self._body().get("seconds", 0))
                    log(replica.role, f"ADMIN  replication delay set to {replica.repl_delay}s")
                    self._send(200, {"delay": replica.repl_delay})
                else:
                    self._send(404, {"error": "not found"})
            except PermissionError as e:
                self._send(403, {"error": str(e)})

    return Handler


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--role", choices=["primary", "secondary"], required=True)
    p.add_argument("--port", type=int, required=True)
    p.add_argument("--peer", help="secondary URL (primary only)")
    p.add_argument("--initial", type=int, default=20, help="initial stock value")
    a = p.parse_args()
    if a.role == "primary" and not a.peer:
        p.error("--peer is required for the primary")

    replica = Replica(a.role, a.peer, a.initial)
    server = ThreadingHTTPServer(("127.0.0.1", a.port), make_handler(replica))
    log(a.role, f"listening on http://127.0.0.1:{a.port}  initial stock={replica.data['stock']}")
    server.serve_forever()


if __name__ == "__main__":
    main()
