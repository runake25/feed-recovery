# Feed Recovery: Scope and Build Guide (Python, v2)

A small, reliable market-data feed that targets three failures:
messages get lost, a late joiner needs a snapshot, and a slow reader must not stall everyone else.
You deliver one implementation, one set of numbers from controlled runs, and one writeup:
**what broke, why, the fix, the recovery time.**

---

## 1. Scope

### In scope
- **Publisher**: one toy order book, sending one sequenced diff per tick over **unicast UDP**, one datagram per diff. It keeps a ring buffer of recent diffs for retransmit, serves snapshots, and takes a new epoch every time the process starts.
- **Subscribers** (100): each keeps its own book and detects gaps and reordering. A small gap is fixed by retransmit; a gap that is too wide, or a new epoch, means taking a snapshot.
- **Snapshot cache**: a separate process. It merges simultaneous requests into one upstream fetch, holds one snapshot for the publisher's *current* epoch, and serves it for a set time (the TTL).
- **Slow-reader isolation on the reliable (TCP) recovery path**: one client that stops reading must not hold up the responses to the other 99.
- **Checker**: compares book hashes and records time-to-consistent for each fault. It also runs one **divergence alert**, which fires when a book diverges while the process is still up.
- **Harness**: runs each scenario N times and writes the raw results to disk.
- **Writeup**: one page.

### Out of scope (keep this list in the README and stick to it)
- Matching engine, message bus or broker, chaos framework, ITCH parser
- **Multicast**, multiple hosts, publisher failover, persistence, TLS or auth
- Any link to the crypto dashboard, any UI, Docker or k8s
- A second language

---

## 2. Feed source: synthetic, seeded

Every measured run uses a **seeded generator**:
- A fixed mid price, with a grid of **1,000 price levels per side** created at startup.
- Each tick changes 1–10 levels, **only within the top 20 levels per side**. The deep levels never change after startup. That keeps the snapshot realistically large, and it gives the corruption test somewhere to hide (trap 7).
- The same seed gives the same book, so a failed run replays exactly.

**Do not plug a live exchange feed into the measured path.** Its own gaps, reconnects, and changing message rate would blur your recovery numbers.
**Optional, after M10:** record a real L2 depth stream once, replay it at a fixed tick rate, and convert decimal prices and quantities to integer ticks when loading.

---

## 3. Transport

| Channel | Transport | Why |
|---|---|---|
| Diff stream | **Unicast UDP**, one datagram per diff | This is what live market-data feeds use (ITCH, MDP, and the feed in the exchange talk): the sender never waits for the reader, losses become holes in the sequence, and datagrams can arrive out of order |
| Control session | TCP, subscriber → publisher | `Subscribe(udp_port)` → `Hello(epoch, head_seq)`. The publisher learns where to send. A closed session tells the subscriber the publisher has died |
| Retransmit | TCP request/response → publisher | The process that still has the message resends it |
| Snapshot | TCP, subscriber → snapcache → publisher | The same shape as an exchange snapshot server (for example, Glimpse) |
| Reports | TCP → checker | |

- **Loss injection** happens in the publisher's **dropper**. It leaves out whole datagrams, holds a datagram back for one tick (reordering), or sends one twice (duplication), all from a seeded random generator. It never drops TCP segments: the kernel would resend them, and its retransmission timer (often about 200 ms) would replace your own recovery time.
- **Multicast is out of scope.** Unicast to 100 addresses exercises the same protocol.

### Where a slow reader can and cannot stall things
- **On the UDP stream, it cannot.** `sendto` never waits for the reader. If a subscriber stops reading, its kernel buffer fills and the kernel drops its datagrams, so the other 99 are unaffected **by construction**. Even a naive fan-out loop with a queue per subscriber never fills those queues: the code draining each queue is a `sendto`, and that call never blocks on the reader. In the writeup, the healthy 99's live-stream p99 during a stuck stream reader is a **control** that confirms this. It is not a result, and no queue earns credit for it.
- **On the TCP recovery path, it can.** The snapcache sends one finished snapshot to every client waiting for it. If that broadcast awaits each client's `drain()` in turn, one client that has stopped reading holds back the snapshot for every other client in a restart storm. This is the slow-consumer problem of a reliable bus, at the layer where it actually happens. The broken version and its fix are in M7.

---

## 4. Process layout

```
           +----------------- publisher -----------------+
 tick ---> | book -> diff -> ring                        |
           |   fan-out -> dropper -> UDP sendto ---------+--> subscriber 1..100 (UDP)
           |  control server (TCP)  <--------------------+--- subs: Subscribe / Hello
           |  retransmit server (TCP, reads ring) <------+--- subs (gap)
           |  snapshot server (TCP, Semaphore(1)) <-- snapcache <-- subs (snapshot)
           +---------------------------------------------+
  publisher (epoch, seq, hash) ---> checker <--- subfleets (per-tick batches)
  harness: starts and kills processes, injects faults, collects results
```

```
feed-recovery/
  feed/
    book.py       # generator, apply, incremental hash
    wire.py       # struct codecs
    pub.py        # tick loop, ring, UDP fan-out, control/retransmit/snapshot servers
    sub.py        # pure state machine + asyncio I/O shell
    cache.py      # snapcache
    checker.py    # consistency + alert
    hist.py       # log-linear histogram
    fault.py      # dropper (drop/reorder/dup), stuck readers, corruption
    config.py     # parameters + startup invariant checks
  cmd/            # publisher, subfleet, snapcache, checker, harness
  tests/
  results/<date>-<host>/
  WRITEUP.md
```

Run the 100 subscribers as asyncio tasks spread over **about 4 `subfleet` processes**, so that one garbage-collector pause doesn't move every subscriber's tail at once.

---

## 5. Fixed design choices

- **Book**: integer prices (in ticks) and integer quantities. **No floats.**
- **Diffs set absolute values**: `(side, price, new_qty)`, where `qty = 0` deletes the level. Applying the same diff twice is harmless; applying an older diff after a newer one is still wrong.
- **Every tick sends a datagram**, even when the diff is empty, so ticks double as heartbeats.
- **Epoch** = the publisher's start time in nanoseconds. It only increases across restarts. Sequence numbers start again at 1 in each epoch.
- **Incremental book hash**: `H = Σ h(side, price, qty) mod 2^64`, where `h` is an 8-byte `blake2b` of the packed level. On each change, subtract the old level's `h` and add the new one. This costs O(number of changes) per tick and needs no sorting. Rehashing 2,000 levels every tick in 25 subscribers per process would not fit in 10 ms. Compute the full hash only when you apply a snapshot, and test that it equals the incremental hash.
- **One choke point.** The book has a single `set_level(side, price, qty)` that updates the stored level and the running hash together. Nothing else touches level storage, and snapshot loading, diff apply, and the ghost-level injection all go through it. There must be **no `await` inside it**: in asyncio, two statements with no `await` between them can't be interleaved. If you use real threads or the free-threaded build, this needs a lock. Snapshot copies and report reads happen between ticks, so they never see a half-updated book.
- **Self-check.** Every 100 ticks each book does a full recompute and compares it with the running hash. A mismatch is counted as a book-integrity error, separate from a divergence from the publisher.
- **Bucketed hash (optional extension to M8).** Split each side into 32 price-range buckets, keep a running hash per bucket (updated the same way: `H_b += h(new) - h(old)`), and define the top hash as the combination of the 64 bucket hashes. Reports still carry only the 8-byte top hash. On a mismatch, the checker requests that subscriber's 64 bucket hashes and compares them with the publisher's, so the alert names the price range that differs ("bid levels 4,500–4,531") instead of just "book differs". Going further, a tree of buckets (Merkle-style) narrows it to a single price.
- **Keep the hash out of the wire format.** Detecting divergence stays the checker's job.
- **Snapshots** are copied synchronously between ticks, inside the publisher's event loop, so they always match an exact seq.

### Parameters (`config.py` checks the invariants at startup and refuses to run if any fail)

| Parameter | Start | Invariant / note |
|---|---|---|
| Tick interval | 10 ms | 1 ms is a stretch goal in CPython |
| Cache TTL | 2 s | |
| MaxGap | `ceil(1.5 × TTL / tick)` = 300 | **Must be > TTL / tick**, or every cached snapshot leads to another snapshot request (trap 2) |
| Ring depth | 10,000 | **Must be ≥ 2 × MaxGap** |
| Retransmit timeout | 50 ms | one retry, then snapshot |
| Snapshot timeout | 500 ms | |
| Max diff entries | 10 | **Datagram ≤ 1,200 bytes** (the encoder asserts this) |
| Snapshot cost | 20 ms, `Semaphore(1)` | one request at a time, like a real exchange snapshot server |
| Recovery socket buffers | `SO_SNDBUF` / `SO_RCVBUF` 4 KiB, write-buffer high mark 4 KiB | **Must add up to less than snapshot size** (~34 KB) so a stuck client blocks `drain()`. Linux doubles the value you set; record what `getsockopt` reports |
| Recovery writer deadline | 250 ms | how long a stuck recovery client gets before it is cut off |
| Report cadence | every tick | one batch per subfleet per tick |

---

## 6. Wire format

Little-endian. Build each layout once with `struct.Struct('<...')`.

**UDP datagram** (no length prefix; the datagram boundary is the frame):
```
Diff       = type:u8 epoch:u64 seq:u64 sched_ns:i64 n:u16 { side:u8 price:i64 qty:i64 } * n
```

**TCP frames** (`frame = len:u32 | type:u8 | body`):
```
Subscribe  = udp_port:u16
Hello      = epoch:u64 head_seq:u64
ResendReq  = epoch:u64 from:u64 to:u64
ResendResp = status:u8 (OK | GONE | WRONG_EPOCH) count:u32, then `count` Diff frames
SnapReq    = min_epoch:u64          # highest epoch this client has seen on the stream, else 0
Snapshot   = epoch:u64 seq:u64 n:u32 { side:u8 price:i64 qty:i64 } * n
Report     = fleet_id:u16 ts_ns:i64 k:u16 { sub_id:u16 state:u8 epoch:u64 seq:u64 hash:u64 } * k
PubReport  = ts_ns:i64 epoch:u64 seq:u64 hash:u64
Cut        = reason:u8
```

`sched_ns` is the time the tick **was scheduled** to fire. A stalled publisher therefore shows up in the latency numbers instead of being hidden (coordinated omission).

---

## 7. Subscriber state machine

Write it as a **pure function**, `step(state, event) -> (state, actions)`, that touches no sockets and no clock.

State carried: `epoch`, `expected` (the next seq to apply), `pending: dict[seq → diff]`, `outstanding_resend` (at most one), and `retries`.

**`advance()`**, called after anything is added to `pending`:
1. While `expected` is in `pending`: apply that diff, then `expected += 1`.
2. If `pending` is empty, go to **LIVE**.
3. Otherwise there is a hole. If `max(pending) - expected > MaxGap`, request a snapshot and go to **SYNCING**.
4. Otherwise, if no resend is outstanding, request a resend of `[expected, min(pending) - 1]` and go to **RECOVERING**.

| State | Event | Action → next state |
|---|---|---|
| INIT | start | `SnapReq(min_epoch = highest seen or 0)` → SYNCING |
| SYNCING | Diff | add to `pending`, keyed by (epoch, seq) |
| SYNCING | Snapshot (E, S) | if `pending` has an epoch > E: discard the snapshot and request again with `min_epoch` set. Otherwise adopt E, load the book, set `expected = S+1`, drop pending ≤ S, then `advance()` |
| LIVE | Diff with a newer epoch | add to `pending`, `SnapReq(min_epoch = new)` → SYNCING |
| LIVE | seq < expected | duplicate → drop it |
| LIVE | seq == expected | apply it, `expected += 1` |
| LIVE | seq > expected | add to `pending`, `advance()` |
| RECOVERING | Diff (same epoch) | drop it if seq < expected; otherwise add to `pending` and `advance()`. **A late live datagram can fill the hole before the resend arrives.** |
| RECOVERING | ResendResp OK | add its diffs ≥ expected to `pending`, clear `outstanding_resend`, `advance()`. **Go LIVE only once `pending` has drained, meaning everything is contiguous.** |
| RECOVERING | GONE / WRONG_EPOCH / timeout | retry once, then snapshot → SYNCING |
| any | control session closed, or 3 ticks of silence | → INIT |

A resend response that arrives after a live datagram has already filled the hole contains only duplicates. They are dropped; that is expected.

---

## 8. Snapshot cache rules

- The cache holds **one TCP session to the publisher**. It learns the current epoch from `Hello`.
- **When that session dies, throw away the cached snapshot**, then reconnect. The new `Hello` brings the new epoch.
- It stores **one** snapshot: the one for the publisher's current epoch. It serves that snapshot only if it is younger than the TTL **and** its epoch is ≥ the request's `min_epoch`. Otherwise it fetches a new one, sharing a single in-progress fetch (a future) among everyone waiting.
- **The client adopts whatever epoch the snapshot carries.** Requests never ask for a particular old epoch.
- Requests that arrive while the cache is disconnected wait for the reconnect, up to the snapshot timeout.
- Count upstream fetches per epoch. That count is the headline number for the restart storm.
- Broadcast to waiting clients by `put_nowait` into each client's own writer task, never by awaiting each client in turn (see M7).

---

## 9. Milestones

**For each fault, build and measure the broken version before you fix it.**

| # | Build | Done when |
|---|---|---|
| M0 | `book`: generator, apply, incremental hash | After 100k diffs, the incremental hash equals a full recompute at every step |
| M1 | `wire` codecs | Round-trip tests pass. Hypothesis property tests on every decoder pass. Truncated input, oversized input, and a wrong `count` raise clean errors. The encoder rejects datagrams over 1,200 bytes |
| M2 | Publisher + 1 subscriber: control session + UDP stream, no faults | Hashes match for a 60 s run |
| M3 | Checker + per-tick reports; scale to 100 subscribers | Baseline run: every subscriber is consistent every tick. Save the baseline latency histogram. Unplanned gaps (any not caused by the dropper) = 0 |
| M4 | Dropper (drop / reorder / dup), `pending` buffer, ring, retransmit | 1% random loss, a 50-datagram burst, and reorder-by-one all recover **without any snapshot**. A test shows a late live datagram filling a hole before its resend lands |
| M5 | Snapshot, epoch, MaxGap; publisher kill/restart **without the cache** | A 400-datagram burst recovers via snapshot. Restart: upstream snapshot fetches ≈ 100. Record time-to-consistent and how far the tick loop slips |
| M6 | `snapcache` with session-based invalidation | Same restart: **1 upstream fetch per epoch**. A test shows a cached snapshot from the dead epoch is never served once the session has dropped |
| M7 | Slow reader on the recovery path. **Broken:** the snapcache broadcast does `write` + `await drain()` for each waiter in turn, with one stuck client among 100 during a restart storm. **Fixed:** the broadcast does `put_nowait` into per-client writer tasks; a writer past its deadline cuts its client | Broken: the healthy 99's time-to-consistent jumps. Fixed: it matches the storm with no stuck client, and the stuck client is cut and later resyncs. **Control:** a stuck *stream* reader leaves the healthy 99's live p99 at baseline |
| M8 | Divergence alert | A subscriber with an injected ghost level stays LIVE with its seq advancing; the alert fires within K reports while its liveness check stays green |
| M9 | Harness: every scenario × N = 30, raw CSV per run | One command reproduces every number |
| M10 | Writeup | §13 |

Rough pace: one milestone every 2–4 evenings, so about 4–5 weeks part-time.

---

## 10. Scenarios

| Scenario | Fault | Broken baseline | Fixed | Headline |
|---|---|---|---|---|
| Loss | 1% random; 50-burst; reorder; dup; 400-burst | snapshot on every gap | retransmit for small gaps, snapshot for wide ones | time-to-consistent distribution |
| Restart storm | kill + restart the publisher; 100 clients, no reconnect jitter | no cache | snapcache | upstream fetches; time-to-consistent |
| Recovery-path slow reader | 1 of 100 stops reading its snapshot connection during the storm | serial `await drain()` broadcast | per-client writers + deadline cut | healthy 99 time-to-consistent p99 |
| Stream slow reader (control) | 1 subscriber stops reading UDP for 10 s | — | — | healthy 99 live p99 (unchanged by construction); the stuck one's recovery time |
| Divergence | ghost level in one book | liveness check only | hash-based alert | time-to-alert |

---

## 11. Measurement rules

- **Time-to-consistent**: from the fault to the first report in which the subscriber's hash equals the publisher's hash at that seq **and** the subscriber is at most 1 seq behind. The fault time is:
  - Loss: the `sched_ns` of the first dropped datagram
  - Restart storm and recovery-path slow reader: the moment the harness kills the publisher
  - Stream slow reader: the moment the reader resumes
  - Divergence: the injection time (report time-to-alert)
- **Floors, stated explicitly**:
  - Detection: you can't notice a lost datagram before the next one arrives, so the floor is 1 tick.
  - Reporting: reports go out once per tick, which adds up to 1 more tick.
  - At 10 ms ticks, nothing can measure below about 10–20 ms. Show how this changes at a faster tick.
- **Two numbers that are never merged**:
  - Per-message apply time: from datagram decoded to book updated, using `time.perf_counter_ns()`.
  - Time-to-consistent: across processes, using `time.time_ns()` on one host, so one kernel clock.
- **Distributions, not averages**: p50, p99, p99.9, max, and N. Write your own log-linear histogram.
- **Tick loop**: sleep until `start + k × interval`. Count the ticks you miss.
- **Unplanned loss**: count gaps the dropper didn't cause (real kernel drops) and report them separately.
- **Say the machine**: CPU model, cores, kernel, CPython version, uvloop yes or no, free-threaded or not, date, WSL2 or not, and the socket buffer sizes `getsockopt` actually returned. Pick one host and never mix numbers from different hosts.
- Log garbage-collection pauses through `gc.callbacks`, and record whether you used `gc.freeze()`.

---

## 12. Traps

1. **The built-in `hash()` is randomized for each process.** Use `blake2b`.
2. **TTL, MaxGap, and ring depth are linked.** A snapshot up to TTL old leaves a gap of up to TTL / tick diffs. If MaxGap is below that, the subscriber asks for another snapshot. If the ring is shallower than MaxGap, the resend comes back GONE and the subscriber again asks for a snapshot. Both are loops, so `config.py` refuses to start rather than let you find them in the data.
3. **The retransmit server gets stampeded next.** After a restart, 100 clients each ask for up to about 200 diffs at once. Measure it.
4. **The cache must not serve a dead epoch.** Clients that saw the restart as silence send `min_epoch = 0` and might get the old snapshot if the cache hasn't noticed the dropped session yet. The state machine recovers (newer epoch on the stream → snapshot again with `min_epoch`). Count how often this race happens instead of hiding it.
5. **`writer.write()` never blocks.** It buffers in memory without limit until `await drain()`. Set `transport.set_write_buffer_limits(...)` on the recovery path so that backpressure actually reaches your code.
6. **Loopback TCP buffers are megabytes.** A stuck recovery client blocks `drain()` only if the snapshot is larger than both kernel buffers plus the write-buffer high mark combined. The small buffers and the 1,000-level book are a deliberate scale-down; list them under threats to validity. They are not needed on the UDP stream.
7. **Corruption can heal itself.** Absolute diffs overwrite a damaged level the next time that level changes. Put the ghost level deep in the book (beyond the top 20), where diffs never reach.
8. **Reconnect jitter hides the storm.** Leave it out of the measured run, and mention it in the writeup as a second defence.
9. **Many concurrent sleeps cost nothing in asyncio.** `Semaphore(1)` on the snapshot server is what makes the stampede visible.
10. **One event loop and one garbage collector per process.** That is why the fleet is split into 4 processes.
11. **UDP reorders and duplicates datagrams.** It rarely does so on loopback, so inject both. Without injection, the `pending` buffer and duplicate handling never run.
12. **A stuck reader must *stop reading***, for example by waiting on an `asyncio.Event`. A task that busy-loops blocks its whole event loop, which is a different failure.
14. **A running hash is only as good as its choke point.** If any code path changes a level without going through `set_level`, the hash describes the old book and the checker sees a match on a wrong book. Order doesn't matter (hash first or level first fails the same way); funnelling every change through one method does. The M0 test and the 100-tick self-check both exist to catch this.
13. **Subscriber receive buffers can overflow.** A garbage-collection pause in a subfleet can cause real kernel drops. That is why you count unplanned loss.

---

## 13. Writeup outline (one page plus figures)

1. **Setup**: host, configuration table with the invariants, date, N, runtime details, actual socket buffer sizes.
2. **For each scenario**: what broke (with the number from the broken version), why, the fix, and the recovery time as a distribution. One plot each.
3. **Per-message apply time**, in its own section and labelled as a different boundary.
4. **Threats to validity**: single host, unicast instead of multicast, injected loss, artificial snapshot cost, small recovery-socket buffers, WSL2 or burstable CPU, CPython.
5. **The three sentences**, all from the same run:
   - *Jane Street*: Lossy UDP fan-out with retransmit from the process that still has the message. In a restart storm of 100 clients, the snapcache let 1 request reach the publisher. The alert watches the book hash, not the process.
   - *Jump*: On `<machine>`, one stuck consumer on the reliable snapshot path. With a serial broadcast, the healthy 99's time-to-consistent p99 was X; with per-client writers and a cut it was X′, against Y with no stuck client. On the UDP stream, a stuck reader can't slow the sender, so the live p99 is shown only as a control.
   - *Citadel Securities*: Time-to-consistent-book after a gap was p99 Z ms on `<host>`, `<date>`, with a stated floor of 1–2 ticks. Per-message apply time is W µs, which is a separate boundary.

**If asked "why Python":** the claims are about protocol behaviour: recovery time, request merging, and isolating a slow consumer. CPython makes absolute latency worse, but it doesn't change those results. Per-message time stays labelled as Python-on-this-host.

---

## 14. Tests that stay green

- Unit: book (including incremental vs full hash, and a test that fails if anything other than `set_level` writes level storage), codecs, histogram, config invariants. If you build buckets: after a ghost-level injection, the differing bucket reported is the one containing the ghost price.
- Hypothesis property tests: decoders, plus random event streams fed to `step()`. **Property:** with any interleaving of drop, reorder, dup, and resend, a subscriber that reaches LIVE has a hash equal to the publisher's at that seq.
- Scripted state-machine cases: gap, dup, reorder that fills a hole, resend after the hole was already filled, epoch change during RECOVERING, GONE, timeout, snapshot older than the buffered epoch, control session lost during SYNCING.
- Cache: merging of simultaneous requests, invalidation on session loss, `min_epoch` floor, broadcast with one stuck client.
- One in-process integration test over loopback (publisher + 3 subscribers + cache).
- A harness smoke run (N = 1) before each commit.
- `mypy --strict` or strict `pyright`; `@dataclass(slots=True)` for messages and state.

---

## Changes from v1
- The diff stream moved from TCP to unicast UDP. Loss is injected by leaving out whole datagrams; reordering and duplication are added.
- The slow-reader stall moved to the TCP recovery path (the snapcache broadcast). On UDP it can't happen, so the live-stream p99 became a control.
- Cache: invalidated when the publisher session drops; stores one snapshot for the current epoch; uses a `min_epoch` floor instead of matching the client's old epoch.
- State machine: a `pending` buffer keyed by seq. Live datagrams can fill holes, and the subscriber goes LIVE only when everything is contiguous.
- `ResendResp` and `Snapshot` carry counts. Reports go out every tick, and that floor is stated.
- MaxGap is derived from TTL / tick and checked against ring depth at startup.
- The book hash is incremental, and the book is 1,000 levels deep with only the top 20 changing.
