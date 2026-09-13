# ADR 0012 — A memory budget for the JVMs, not a heap cap; G1, and GC logs always on

**Status:** accepted (M8)
**Supersedes:** `-Xmx256m -XX:+UseSerialGC` in a 512M container (M0–M8)
**Related:** ADR 0009 (admission control — why a pause at high utilisation becomes a long queue)

## Context

Until M8 each service ran with `-Xmx256m -XX:+UseSerialGC` in a 512M container: sized as if the heap
were the process. Once the load test had removed the lock and queueing bottlenecks, garbage
collection set the latency tail — a 6.7 s pause on the gateway, 1.9 s on the orchestrator — and none
of it could be explained, because nothing had logged *why* a pause happened. Micrometer records how
long a pause took, not what it was doing.

Measured on the orchestrator at rest:

| Region | Size |
|---|---|
| Heap, committed (cap 256) | 171 MiB |
| Metaspace (27,546 classes — Spring Boot, Hibernate, Kafka, OpenTelemetry) | 149 MiB |
| Code cache | 82 MiB |
| Native: thread stacks, GC and JIT working memory | ~96 MiB |
| **Total** | **~500 MiB**, in a 512M container, with the heap 85 MiB short of its cap |

The heap was about half the process. During a knee run the container hit `memory.max` 506 times and
the kernel swapped 76 MiB of the JVM's *own heap* out. The slowest pause of that run, a 1.26 s full
collection on the orchestrator, was paging, visible from three sides at once:

- the GC log's CPU line: `Real 1.26 s` against `User 0.55 s + Sys 0.12 s` — the collector was
  waiting, not working;
- in the same 14 s, swapped memory rose from 4 to 75 MiB and major page faults from 197 to 6,878;
- the mark phase, which visits every live object, took 1.0 s of the 1.26 s.

In Kubernetes, which has no swap, the same budget is an OOMKill.

A 256 MiB heap is not a tuning problem at this size. The pauses were three different mistakes, and
each one only showed up once the one before it was fixed.

## Decision 1 — GC logging always on, and read it before tuning

```
-Xlog:gc*,safepoint:file=/tmp/gc.log:time,uptime,level,tags:filecount=5,filesize=20m
```

Rotated, inside the container (`docker exec <container> cat /tmp/gc.log`). The per-pause `gc,cpu`
line is the diagnosis:

- **User ≈ Real** — collection work, CPU-bound;
- **Sys large** — the kernel: page faults, zeroing pages;
- **Real ≫ User + Sys** — waiting: swap, or a descheduled process.

`safepoint` adds the time to reach a safepoint, which the pause metric does not include. Count full
collections from completion lines only (those ending in `ms`): unified logging writes a `gc,start`
line with the same text, and the first comparison counted every full GC twice.

## Decision 2 — every region capped, and the caps sum below the container limit

| Region | Flag | Cap (MiB) | Observed |
|---|---|---|---|
| Heap | `-Xms256m -Xmx256m` | 256 | live after full GC 47–111 MiB |
| Metaspace | `-XX:MaxMetaspaceSize=176m` | 176 | 129–150 |
| Code cache | `-XX:ReservedCodeCacheSize=144m` | 144 | 67–82 |
| Direct buffers | `-XX:MaxDirectMemorySize=32m` | 32 | < 2 |
| Native, uncapped | stacks, JIT, symbols, G1's remembered sets and bitmaps | ~140 | |
| **Sum** | | **~748** | **< 768M** container limit |

A region that outgrows its cap now fails *loudly* — `OutOfMemoryError: Metaspace`, "CodeCache is
full" — instead of the container swapping itself into multi-second pauses. The budget lives as a
table in `infra/docker-compose.yml` beside the flags, and the three services' limit is a separate
`x-jvm-mem-limit` anchor so it cannot drift from the budget: **change one cap, re-add the column.**

Three defaults were each sized for a much smaller application and each caused early full GCs:

- **`-Xms` = `-Xmx`.** The heap started at 8 MiB and grew by collecting; the young generation started
  tiny, promoted early and filled the old one (five allocation-failure full GCs in one run).
- **`-XX:MetaspaceSize=160m`.** The first metaspace GC threshold is 21 MiB by default, so warm-up
  crossed it repeatedly on the way to ~150 MiB.
- **`-XX:SweeperThreshold=60`.** Since JDK 20, compiled code is unloaded by a GC whenever the code
  cache grows by this percentage of its reserved size — 15% of 240 MiB by default, which fired full
  collections of 350–700 ms during warm-up. These were logged as `CodeCache GC Threshold` and first
  read as code-cache exhaustion; the code heaps held about 41 of 116 MiB.

## Decision 3 — G1, not SerialGC, on GC-log evidence

Fixing the budget made SerialGC's full collections *rarer and longer*. The 1,000-user run filled a
174 MiB old generation that held 89 MiB of live data, and collecting it took 1.86 s, all of it CPU
(User 1.83 s ≈ Real 1.86 s — not paging this time). A serial old-generation collection costs time
proportional to the whole generation, single-threaded and stop-the-world. G1 marks the old
generation concurrently and reclaims it in mixed collections under a pause target.

- **GC threads capped** (`ParallelGCThreads=2`, `ConcGCThreads=1`). Three JVMs on one machine must
  not each start 13 parallel GC threads, the default for 16 CPUs.
- **`-XX:+AlwaysPreTouch`.** `-Xms` reserves the heap without touching it, so G1's first evacuations
  copied into never-touched pages and the kernel faulted and zeroed each one *inside* the pause: 500
  ms young pauses at boot, with Sys 1.24 s against User 0.17 s. Pre-touching pays it once, before
  the service takes traffic. The budget already counts the whole heap as resident, so it costs no
  memory the budget had not assumed.
- G1's remembered sets and marking bitmaps cost roughly 10–15% of the heap, counted in the native
  row of the budget.

## Results

| Knee run, 5 → 40/s | 512M, SerialGC | 768M budget, SerialGC |
|---|---|---|
| Completed | 7,897 of 7,897 | 8,026 of 8,026 |
| Worst window p99 | 6.5 s | 4.4 s |
| Orchestrator full GCs | 5 allocation, 2 metaspace, 1 code cache | 1 (code cache, 341 ms) |
| Worst pause: account / gateway / orchestrator | 294 / 702 / 1,262 ms | 80 / 78 / 341 ms |
| Orchestrator peak memory / swapped / `memory.max` hits / major faults | 509 of 512 MiB / 76 MiB / 506 / 7,053 | 563 of 768 MiB / 0 / 0 / 42 |

| 1,000 users, 30–90 s think | SerialGC | G1 |
|---|---|---|
| Completed | 7,278 of 7,278 | 7,307 of 7,307 |
| Worst 30 s window, settle p99 | 5.11 s (with a 1.86 s full GC in it) | **3.34 s** |
| Worst GC pause during the run | 1,858 ms | **74 ms** |

Boot pauses with `AlwaysPreTouch`: 33–40 ms, from 500–531 ms. I1–I5 and S1–S4 held in every run.

## The same mistake in Jaeger

Jaeger's in-memory store is bounded by `MEMORY_MAX_TRACES`, and the Compose file had called 20,000 "a
hard ceiling". It counts traces, not bytes. Sampled through a run, its heap grew ~3.3 KiB per span,
~27 KiB per trace — so 20,000 traces was ~530 MiB in a 384M container, and Jaeger was OOM-killed in
six of seven knee runs (payments were unaffected, as the tracing design requires). Go's default GC
also lets a heap reach twice its live data. Now `MEMORY_MAX_TRACES=5000` plus `GOMEMLIMIT=300MiB`:
it stored 111,711 traces over a 60/s run and survived both 1,000-user runs, peaking at 237 MiB. If
traces grow (more spans, larger attributes), the count is re-derived from bytes per trace.

## Consequences

- **+768 MB of limit across the three services** on a 16 GB machine where memory is the binding
  constraint. The measured working set rose by less, about 600 MiB each once the fixed-size heap is
  fully touched.
- **The budget is a measurement of this code**, like the admission limit. More classes (a new
  library) raise metaspace; more threads raise native. A region hitting its cap is now an error
  message rather than a slow system, which is the point of capping it.
- **Kubernetes requests and limits (M10) must come from this table**, not from `-Xmx`.
- **Unexplained, and recorded as such:** the gateway's 6.7 s pause before this work. That container
  never swapped and nothing had logged the pause. With logging always on, a recurrence will be
  explained.

## Alternatives rejected

- **A bigger `-Xmx` in the same container.** Moves the heap further into memory the rest of the JVM
  needed; the swapping gets worse, not better.
- **`-XX:MaxRAMPercentage` instead of fixed sizes.** Sizes the heap as a share of the limit, which
  is the same heap-is-the-process assumption expressed as a ratio.
- **ZGC or Shenandoah.** Not measured here. Their lower pause targets buy nothing this system can
  use: G1 took the worst pause from 1.86 s to 74 ms, far below anything the saga's latency can see,
  and a different collector's overhead would have to be re-fitted into a budget where every MiB is
  already allocated.
