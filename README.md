# 273 – Module 8 Assignment: Replication & Consistency

A small replicated inventory system in Python (standard library only) that shows
**asynchronous primary → secondary replication**, a **stale read** when replication
is delayed, and **convergence** once replication resumes.

## Files

| File | Purpose |
|------|---------|
| `replica.py` | One replica process (Primary or Secondary), an HTTP server holding `stock` |
| `client.py`  | HTTP client: read from either replica, write to the primary, pause/resume replication |
| `demo.py`    | Starts both replicas as separate processes and runs all three tests automatically |
| `evidence/demo_output.txt` | Saved terminal output from a full run |

## How to run

Requires Python 3.8+ and nothing else (no `pip install`).

**Option A: run everything automatically (recommended)**

```bash
python3 demo.py
```

This starts the Primary on port 8001 and the Secondary on port 8002, runs Tests 1–3,
checks each result with assertions, prints `ALL TESTS PASSED`, and shuts both replicas down.

**Option B: run it by hand in three terminals**

```bash
# terminal 1
python3 replica.py --role secondary --port 8002

# terminal 2
python3 replica.py --role primary --port 8001 --peer http://127.0.0.1:8002

# terminal 3
python3 client.py              # read stock from both replicas
python3 client.py write 10     # write to the primary
python3 client.py pause        # pause replication (simulates lag/partition)
python3 client.py write 9
python3 client.py              # primary=9, secondary=10  -> stale read
python3 client.py resume       # resume replication
python3 client.py              # both = 9                 -> converged
```

## Architecture

```
                        ┌──────────────────────────┐
         write(stock)   │                          │
   ┌───────────────────►│   PRIMARY  (port 8001)   │
   │                    │   stock, version         │
   │      read(stock)   │                          │
   ├───────────────────►│   replication queue ──┐  │
   │                    │   + worker thread     │  │
   │                    │   (pause/resume/delay)│  │
┌──┴─────┐              └───────────────────────┼──┘
│ CLIENT │                                      │  async: POST /replicate
│        │                                      │  {key, value, version}
└──┬─────┘              ┌───────────────────────▼──┐
   │      read(stock)   │                          │
   └───────────────────►│  SECONDARY (port 8002)   │
                        │  stock, version          │
                        │  (read-only)             │
                        └──────────────────────────┘
```

How it works:

1. The client sends every **write** to the Primary. The Primary applies it locally,
   bumps a version number, acknowledges the client right away, and puts the change on a replication queue.
2. A background **replication worker** on the Primary ships queued changes to the
   Secondary over HTTP. This is *asynchronous* replication: the client does not wait for the Secondary.
3. The Secondary applies a change only if its version is newer than what it already has,
   so duplicates and out-of-order retries are ignored.
4. The client can **read** from either replica.
5. Admin endpoints on the Primary (`/admin/pause`, `/admin/resume`, `/admin/delay`)
   simulate replication lag or a network partition. If the Secondary is unreachable,
   the worker retries until it comes back.

Initial state: both replicas start with `stock = 20`. Test 1 sets it to `10`, and Test 2 then changes it from `10` to `9`.

## Evidence

All output below comes from `python3 demo.py` (full log in
[`evidence/demo_output.txt`](evidence/demo_output.txt)). Lines tagged `[PRIMARY]` /
`[SECONDARY]` are the replica processes' own logs. `[CLIENT]` lines are what a
client sees when it reads from each replica. `vN` is the replica's applied version.

### 1. Replication

The Primary is updated to `stock = 10`. A 1-second replication delay is set so the
"eventually" part is visible: right after the write only the Primary has 10, and about 1 second later the
Secondary applies it and both replicas return the same value.

```
==============================================================================
 TEST 1: REPLICATION  (write stock=10 to primary)
==============================================================================
[19:46:46.937] [PRIMARY  ] ADMIN  replication delay set to 1.0s
[19:46:46.989] [PRIMARY  ] WRITE  stock=10 (v1) committed locally
[19:46:46.989] [PRIMARY  ] QUEUE  v1 queued for replication (pending=1)
    [CLIENT] right after write            primary: stock=10 (v1) | secondary: stock=20 (v0)  <-- STALE READ
[19:46:48.001] [SECONDARY] APPLY  stock=10 (v1) from primary
[19:46:48.001] [PRIMARY  ] SHIP   v1 -> secondary OK
    [CLIENT] after 1.1s                   primary: stock=10 (v1) | secondary: stock=10 (v1)
    [RESULT] PASS - secondary received the write; both replicas return 10
```

- ✅ The Primary receives the new value (`stock=10 (v1) committed locally`)
- ✅ The Secondary eventually receives it (`APPLY stock=10 (v1) from primary`)
- ✅ Both return `10` after replication completes

### 2. Stale read

Replication is paused, then the Primary is updated from `10` to `9`. The change sits
in the Primary's queue. For 3 seconds, reads from the Secondary keep returning the old value.

```
==============================================================================
 TEST 2: STALE READ  (pause replication, write stock=9)
==============================================================================
[19:46:48.159] [PRIMARY  ] ADMIN  replication delay set to 0.0s
[19:46:48.160] [PRIMARY  ] ADMIN  replication PAUSED (simulating network delay/partition)
[19:46:48.216] [PRIMARY  ] WRITE  stock=9 (v2) committed locally
[19:46:48.216] [PRIMARY  ] QUEUE  v2 queued for replication (pending=1)
[19:46:48.216] [PRIMARY  ] PAUSED replication is paused; v2 waiting
    [CLIENT] t+1s (replication paused)    primary: stock=9 (v2) | secondary: stock=10 (v1)  <-- STALE READ
    [CLIENT] t+2s (replication paused)    primary: stock=9 (v2) | secondary: stock=10 (v1)  <-- STALE READ
    [CLIENT] t+3s (replication paused)    primary: stock=9 (v2) | secondary: stock=10 (v1)  <-- STALE READ
    [RESULT] PASS - primary returns 9, secondary returns old value 10 (stale read)
```

- ✅ The Primary returns `9`
- ✅ The Secondary keeps returning the older value `10`
- ✅ That is a **stale read**: a client reading from the Secondary sees a value that is no
  longer current (version v1, while the latest write is v2)

### 3. Recovery and convergence

Replication is resumed. The queued write (v2) is shipped right away, the
Secondary applies it, and both replicas agree again.

```
==============================================================================
 TEST 3: RECOVERY & CONVERGENCE  (resume replication)
==============================================================================
[19:46:51.399] [PRIMARY  ] ADMIN  replication RESUMED
[19:46:51.399] [PRIMARY  ] RESUME replication resumed
[19:46:51.400] [SECONDARY] APPLY  stock=9 (v2) from primary
[19:46:51.401] [PRIMARY  ] SHIP   v2 -> secondary OK
    [CLIENT] after resume (+0.0s)         primary: stock=9 (v2) | secondary: stock=9 (v2)
    [RESULT] PASS - secondary caught up to 9; replicas have converged

==============================================================================
 ALL TESTS PASSED
==============================================================================
```

- ✅ The Secondary updates to `9`
- ✅ Both replicas return the same value (`9`, version v2) again

## Reflection

Reading from a replica is fast and spreads load away from the primary, but with
asynchronous replication the replica can fall behind, so a reader can get an old value,
as in Test 2, where the Secondary kept reporting `stock = 10` after the real stock was `9`.
To guarantee the latest value, the system has to read from the primary or wait for the
replica to catch up (for example by using synchronous replication, quorum reads, or
read-your-writes version checks). Each of those adds latency, puts more load on the primary, and lowers availability
when replicas or the network are slow or down. So the tradeoff is
**latency and availability versus consistency**: fast replica reads give you eventual
consistency, while guaranteed-fresh reads cost time and fault tolerance. For something
like inventory, a stale read could make you oversell an item, so critical reads such as checkout
should go to the primary, while browsing pages can tolerate slightly stale replica data.
