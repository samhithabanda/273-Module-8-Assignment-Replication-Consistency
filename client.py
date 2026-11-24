"""Small HTTP client for talking to the replicas."""

import json
import urllib.request

PRIMARY = "http://127.0.0.1:8001"
SECONDARY = "http://127.0.0.1:8002"


def _req(url, body=None):
    data = json.dumps(body).encode() if body is not None else None
    method = "POST" if body is not None else "GET"
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=2) as r:
        return json.loads(r.read())


def read(replica_url, key="stock"):
    return _req(f"{replica_url}/read?key={key}")


def write(key, value):
    return _req(f"{PRIMARY}/write", {"key": key, "value": value})


def pause_replication():
    return _req(f"{PRIMARY}/admin/pause", {})


def resume_replication():
    return _req(f"{PRIMARY}/admin/resume", {})


def set_delay(seconds):
    return _req(f"{PRIMARY}/admin/delay", {"seconds": seconds})


if __name__ == "__main__":
    import sys
    if len(sys.argv) >= 2 and sys.argv[1] == "write":
        print(write("stock", int(sys.argv[2])))
    elif len(sys.argv) >= 2 and sys.argv[1] in ("pause", "resume"):
        print(pause_replication() if sys.argv[1] == "pause" else resume_replication())
    else:
        print("primary:  ", read(PRIMARY))
        print("secondary:", read(SECONDARY))
