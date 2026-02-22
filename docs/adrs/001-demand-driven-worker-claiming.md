# ADR-001: Demand-Driven Worker Job Claiming

## Status
Accepted

## Context

The durable job worker (`worker.clj`) needs to execute jobs concurrently. The original implementation was single-threaded and sequential. When adding concurrency, three approaches were prototyped and benchmarked:

- **A**: JDK ExecutorService with `invokeAll` (batch)
- **B**: Missionary structured concurrency (batch)
- **C**: Injectable `:execute-batch-fn` with pluggable executor (batch)

All three used **batch claiming**: claim N jobs, execute all N concurrently, **wait for the entire batch to complete**, then claim the next N. This creates head-of-line blocking: if one job in a batch of 5 takes 10 seconds while the others finish in 200ms, four slots sit idle for 9.8 seconds before the next batch is claimed.

The question was whether to fix this with:
1. N independent worker threads (each claims 1 job)
2. A single coordinator with demand-driven claiming (claim up to `available_slots`)

Research into production PostgreSQL-backed job queues revealed a clear industry consensus.

## Decision

Use **demand-driven batch claiming with a single coordinator**, matching the Oban (Elixir) pattern:

1. A single coordinator thread tracks available concurrency slots via a `java.util.concurrent.Semaphore`
2. Each loop iteration: `drainPermits()` to get available capacity, then `claim-jobs!` with `LIMIT available`
3. Each claimed job is submitted individually to a fixed `ExecutorService`
4. When a job completes, its thread releases a semaphore permit and wakes the coordinator
5. The coordinator immediately loops and claims more jobs to fill freed slots

This eliminates the batch concept entirely. There is no `execute-batch-fn` option. The `ExecutorService` is always created internally.

**Rationale**: Every major PostgreSQL-backed job queue (Oban, Solid Queue, pg-boss) uses single-coordinator batch claiming. The reason is rooted in PostgreSQL internals: `SELECT ... FOR UPDATE SKIP LOCKED` forces an Index Scan (not Index Only Scan) because lock status lives in heap tuple headers. With N concurrent claimers, the Kth query must skip K-1 already-locked rows, creating O(N^2) total work. A single coordinator issuing one `LIMIT N` query avoids this entirely.

Per-worker claiming (used by Sidekiq, Faktory) is only viable with Redis, where `BRPOP` is O(1) and wakes exactly one consumer. PostgreSQL has no equivalent primitive.

## Alternatives Considered

### 1. Batch claiming with wait-for-all (original)
- **What**: Claim N jobs, run all N concurrently, wait for all N to finish, claim next N
- **Pros**: Simple loop structure, single claim query per batch
- **Cons**: Head-of-line blocking -- one slow job blocks claiming for all slots. With claim-limit=5 and one 10s job among four 200ms jobs, four slots are idle for 9.8s
- **Why not chosen**: No production job queue does this. The idle time compounds under load -- 50 queued jobs with 5 slots takes ~10x longer than necessary

### 2. N independent worker threads
- **What**: Each of N threads independently loops: claim 1 job, execute, repeat
- **Pros**: Simplest possible architecture, no coordination, each thread is self-contained
- **Cons**: N concurrent `SKIP LOCKED` queries create O(N^2) index scanning. Thundering herd on wake (all N threads hit DB simultaneously). N connections needed just for claiming. Effective poll rate is N/T queries/second when idle
- **Why not chosen**: PostgreSQL mailing list reports document CPU spiking to 100% at ~128 concurrent `SKIP LOCKED` processes (Thomas Munro). While our scale (5-20 workers) wouldn't trigger this, the design doesn't scale and the single-coordinator pattern is equally simple

### 3. Missionary structured concurrency (batch)
- **What**: Use `m/join` + `m/timeout` for concurrent batch execution
- **Pros**: Cancellation propagation, structured concurrency guarantees
- **Cons**: Still batch-based (wait-for-all), adds missionary dependency to worker, overkill for "submit to thread pool"
- **Why not chosen**: Missionary adds complexity without benefit when the concurrency model is just "submit jobs to an ExecutorService." The structured concurrency guarantees don't help because jobs are independent (no parent-child relationship). The demand-driven semaphore approach is simpler and uses only JDK primitives

### 4. Injectable `:execute-batch-fn`
- **What**: Allow callers to inject a custom batch execution function
- **Pros**: Maximum flexibility, supports any concurrency model
- **Cons**: The batch abstraction itself is the wrong abstraction -- it forces wait-for-all semantics. Custom executors can't participate in demand-driven claiming because they don't know about the semaphore
- **Why not chosen**: The abstraction boundary is wrong. Concurrency is an internal concern of the worker, not something callers should plug in. The only variation callers need is the concurrency level (`:concurrency` option)

## Consequences

### Positive
- Jobs are picked up immediately as slots free -- no head-of-line blocking
- Single claim query per cycle regardless of concurrency level
- No thundering herd on wake -- only the coordinator wakes
- Simpler API -- no `execute-batch-fn`, `worker-missionary`, or batch concepts
- JDK-only implementation (Semaphore + ExecutorService), no external dependencies

### Negative
- Cannot customize the execution model (e.g., Missionary, virtual threads). The ExecutorService is hardcoded. If this becomes a requirement, could add an `:executor-factory` option later
- Slightly more complex coordinator loop than the batch version (semaphore management)

### Neutral
- `worker_missionary.clj` was removed — no callers existed in any repo
- The `:concurrency` option now means "how many jobs can run simultaneously" rather than "batch size." This is a more intuitive meaning

## Implementation

### Files Changed (this repo)
- `src/main/com/fulcrologic/statecharts/jobs/worker.clj` -- rewrite poll loop with semaphore + per-job submission, remove `default-execute-batch!` and `execute-batch-fn`
- `src/main/com/fulcrologic/statecharts/jobs/worker_missionary.clj` -- removed (no callers)
- `src/test/com/fulcrologic/statecharts/jobs/test_helpers.clj` -- shared test infrastructure (fixtures, job creation, tracker, handler factories)
- `src/test/com/fulcrologic/statecharts/jobs/worker_benchmark_test.clj` -- collapse redundant 3-approach tests to single runs
- `src/test/com/fulcrologic/statecharts/jobs/worker_edge_case_test.clj` -- 10 edge-case tests (failure recovery, concurrency invariants, lifecycle)

### Consumer-side changes (brian repo, already merged on main)
- `projects/brian/src/main/brian/statecharts/runtime.clj` -- remove `:execute-batch-fn`, keep `:concurrency 5`

### Key Implementation Details
- `Semaphore(concurrency)` starts with all permits available
- `drainPermits()` atomically gets all available permits (returns 0 when all busy)
- When no permits available, coordinator blocks on `wake-signal.poll(timeout)` -- woken by job completion or external `wake!`
- Job completion callback: `sem.release()` + `wake-signal.offer(:job-done)`
- Unused permits returned after claim: `sem.release(available - claimed)`

## References

- [PostgreSQL SKIP LOCKED contention at scale](https://postgrespro.com/list/thread-id/2505440) -- Thomas Munro on O(N^2) behavior
- [FOR UPDATE SKIP LOCKED forces Index Scan](https://www.alexstoica.com/blog/postgres-select-for-update-perf)
- [Oban architecture: single Producer per queue](https://hexdocs.pm/oban/Oban.html)
- [Oban.py deep dive: demand-driven claiming](https://www.dimamik.com/posts/oban_py/)
- [Sidekiq 7: per-thread BRPOP (Redis-only)](https://www.mikeperham.com/how-sidekiq-works/)
- [Solid Queue: batch claiming with thread pool](https://github.com/rails/solid_queue)
- [Recall.ai: LISTEN/NOTIFY scaling limits](https://www.recall.ai/blog/postgres-listen-notify-does-not-scale)
