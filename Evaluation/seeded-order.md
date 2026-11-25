# Seeded job ordering in AsyncSequenceValidation

**Verdict: the idea fits inside the existing scheduler, with no new dependency, and it catches #473 case 12, which FIFO misses. It finds nothing in today's marble tests.**

- **FIFO is unchanged.** Under FIFO, the scheduler runs the same items in the same order as at `963eecf`: every one of the 10,484 steps across all 102 `validate { }` tests matches. A unit test pins six golden schedules recorded from the unmodified scheduler.
- **The existing marble tests: 0 failures** under FIFO and seeds 1...1000, with `SWIFT_DETERMINISTIC_HASHING=1` (102,102 test runs). So there are no operator bugs and no marble-diagram ambiguities to classify. One caveat: 62 of the 96 diagrams that reach the scheduler have no choice point at all, so for them every seed is the FIFO schedule. All 20 throttle and timer diagrams fall in that group.
- **#473 case 12 is caught.** With the pre-fix code (`088f92a`), FIFO releases the base iterator, but **56 of 1,000 seeds leak it with `bounded(1)` and 16 of 1,000 with `.unbounded`**. With the fix (`f1b8dcc`), **0 of 1,000** leak, and with PR #480's fix (`f5284b2`), **0 of 10,000** leak. Deleting only the 3-line cancelled check in `iterate()`'s demand section, which both fixes share, brings back the same seed lists in each. The leaking interleaving is shown [below](#case-12-the-interleaving).
- **Operator scenarios, first pass: a known but unmerged `buffer` bug, and known `flatMapLatest` bugs.** For all four buffer policies, a `break` out of `for await` never releases the base iterator. With `.unbounded`, `bufferingLatest` and `bufferingOldest`, the producer task also keeps pulling the base, about 520 elements a second on the real runtime. It is already reported upstream as open PR apple/swift-async-algorithms#352 (May 2025), which uses the same fix. My first search missed that PR, so an earlier version of this report wrongly called the bug new. The PR shows only the `bounded(1)` leak; a comment there now adds the unbounded policies and the regression tests. **This branch also fixes it**: each `buffer` iterator holds a small class whose `deinit` interrupts the storage, the same pattern `merge`, `zip` and `combineLatest` use. The `flatMapLatest` leaks match #461. One more case, `flatMapLatest` after a `break`, is not on #461's list. `zip`, `merge` and `combineLatest` passed every scenario under FIFO and 1,000 seeds. Every one of these bugs also fails under FIFO, so the [scenarios](#operator-scenarios-first-pass) found them, not the seeds.
- **Act on these before upstreaming:** the case-12 check on this branch is opt-in (`ASYNC_VALIDATION_SHARE_CASE12=1`), because `main` doesn't have a #473 fix and the check fails there. It adds public API (`SchedulingStrategy`, `test(theme:scheduling:_:)`). It was run only on macOS 27.3 with Swift 6.4.2, not on Linux.

## What was built

The change lives in `Sources/AsyncSequenceValidation/`.

| Piece | Where | What it does |
|---|---|---|
| `SchedulingStrategy` | `SchedulingStrategy.swift` | Public. `.fifo` (the default) or `.seeded(UInt64)`. `.current` reads `ASYNC_VALIDATION_SCHEDULE_SEED` and falls back to `.fifo` when the variable is unset or not an unsigned integer. |
| `SplitMix64` | `SchedulingStrategy.swift` | Internal. The reference algorithm, so a seed gives the same sequence on any platform. Checked against the published vector for seed 1234567. |
| Seeded drain | `WorkQueue.swift`, `State.takeRandomReady()` | Gathers every ready item and picks one with the generator. Ready means a task job, or a clock wake-up or input emission whose deadline has passed. It runs that item, then picks again, so work the item enqueues competes with work that was already ready. Virtual time advances only when nothing is ready. |
| FIFO drain | `WorkQueue.swift`, `drain()` / `advance()` | Unchanged except for an `observer?(item.description)` call before each `item.run()`, which is nil unless a test passes one. |
| Entry points | `Test.swift` | `test(theme:_:)` now uses `.current`. New public `test(theme:scheduling:_:)`. `@_spi(Testing) test(theme:scheduling:observer:_:)` reports each step, for testing the scheduler itself. |
| Replay hint | `AsyncAlgorithms_XCTest/ValidationTest.swift` | Under a seed, each failure message ends with `[schedule seed N; replay with ASYNC_VALIDATION_SCHEDULE_SEED=N SWIFT_DETERMINISTIC_HASHING=1]`. Under FIFO the messages are unchanged, so `expectedFailures:` strings still match. |
| Trace | `ASYNC_VALIDATION_SCHEDULE_TRACE=1` | Prints each seeded decision: `[schedule seed 2] step 29 @tick 0 pick 1 of 2: work #29`. |

How the seeded drain differs from FIFO. FIFO takes only the head of each job's queue, collects a round, sorts it by input side and then by enqueue order, and runs the whole round. The seeded drain takes any ready item, one at a time. Any FIFO order is also a possible seeded order.

### Tests on this branch

`Tests/AsyncAlgorithmsTests/TestSeededSchedule.swift`, all passing:

- `test_fifo_schedule_matches_pre_seeding_golden`: six diagrams (three-way merge, same-tick merge, debounce, map through a child `Task`, cancellation, zip). Each FIFO schedule must equal a golden copy recorded from `963eecf`. That copy was recorded with only a print added before each `item.run()`.
- `test_default_strategy_is_fifo`: with no seed in the environment, the default path schedules exactly like `.fifo`.
- `test_seed_replays_identically`: five seeds × six diagrams give the same schedule and output twice.
- `test_seeds_explore_orders_fifo_never_takes`: `merge("a-c-|", "b-d-|")` always gives `[ab]-[cd]-|` under FIFO. Within 50 seeds, some seed gives `[ba]…`.
- `test_seeded_schedule_keeps_unambiguous_outputs`: diagrams with no two events on the same tick give the FIFO output under 20 seeds.
- `test_splitmix64_matches_reference_vector`, plus two small tests of strategy selection.

`Tests/AsyncAlgorithmsTests/TestSeededShareRelease.swift`: #473 case 12, skipped unless `ASYNC_VALIDATION_SHARE_CASE12=1` is set.

The full suite gives 371 tests, 0 failures and 2 skips (the opt-in case-12 tests). `swift format lint` is clean on the changed files.

## FIFO is unchanged: evidence

1. **Whole-suite differential.** A scratch worktree at `963eecf` printed each item's description before `item.run()`. A copy of this branch printed the same thing from the observer call. Both ran all 102 `validate { }` tests under FIFO with `SWIFT_DETERMINISTIC_HASHING=1`. The two outputs are byte-identical: 10,484 steps.
2. **Golden unit test.** Six of those schedules are checked into `TestSeededSchedule.swift`, so a future change to FIFO fails a test.
3. **Same entry point.** `validate { }` and `AsyncSequenceValidationDiagram.test(theme:_:)` keep their signatures. They pick FIFO when the seed variable is unset.

## The existing marble tests under seeds

Method: build once, then run the 102 test methods that call `validate` with `xcrun xctest`. One run used no seed, then one run per seed 1...1000, all with `SWIFT_DETERMINISTIC_HASHING=1`. A run of all 102 tests takes about 0.1 s, so 1,001 schedules take under two minutes.

**Result: no test failed under any schedule.** There is nothing to classify as an operator bug or a marble ambiguity.

| Test class | `validate` tests | Tests with a choice point | Failures, FIFO + 1,000 seeds |
|---|---|---|---|
| TestChunk | 33 | 20 | 0 |
| TestValidationDiagram | 32 | 0 | 0 |
| TestThrottle | 16 | 0 | 0 |
| TestBuffer | 9 | 6 | 0 |
| TestDebounce | 6 | 6 | 0 |
| TestTimer | 4 | 0 | 0 |
| TestMerge2 | 1 | 1 | 0 |
| TestMerge3 | 1 | 1 | 0 |
| **Total** | **102** | **34** | **0** |

How much the seeds explored, over seeds 1...200:

- 96 diagram runs reach the scheduler. The other 6 tests check diagram parse errors and stop before scheduling.
- **62 of the 96 diagrams have exactly one schedule**: no step ever has more than one ready item. These are single pipelines, where each step wakes exactly one task.
- The other 34 diagrams get 175 to 200 distinct schedules each, and all of them pass.
- Over 20 seeds, 191,226 decisions had 1 candidate, 14,830 had 2, 3,440 had 3 and 80 had 4. That is about 9% real choices.

**Why no ambiguities showed up.** Every existing diagram avoids putting two inputs' events on the same tick in a way that changes the output. The new `mergeSameTick` diagram shows what an ambiguity looks like: `merge("a-c-|", "b-d-|")` is `[ab]-[cd]-|` under FIFO, but some seeds give `[ba]…`, as a real concurrent runtime can. If a contributor writes such a diagram, a seeded run fails and names the seed.

## #473 case 12

### The check

`share()` on a stored sequence, `break` out of `for await` after the first element, then `shared = nil`. The check runs inside a diagram: input `"a--|"`, mapped through a closure that runs case 12 and returns `"x"`, expected output `"x--|"`. Afterwards it asks whether the tracked base iterator was deallocated. Each schedule drains until nothing is ready, so a task parked forever shows up as a deterministic leak. No timeout is involved.

Run in scratch worktrees, each with this branch's scheduler change applied. The fix was not merged into this branch.

| Library | FIFO | `bounded(1)`: seeds leaking | `.unbounded`: seeds leaking |
|---|---|---|---|
| `088f92a`, before the fix | released | **56 / 1000** (2, 21, 70, 112, 128, …) | **16 / 1000** (212, 231, 238, 243, …) |
| `f1b8dcc`, the fix (`fix-473-share-leak`) | released | 0 / 1000 | 0 / 1000 |
| `f1b8dcc` without the 3-line `isCancelled` check in `iterate()`'s demand section | released | 56 / 1000, same seeds as `088f92a` | 16 / 1000, same seeds |
| PR #480 head `f5284b2` (another contributor's fix, based on `088f92a`) | released | 0 / 10000 | 0 / 10000 |
| `f5284b2` without the same 3-line demand-section check | released | 56 / 1000, same seeds as `088f92a` | 16 / 1000, same seeds |

Seed 2 leaked in three separate processes with `SWIFT_DETERMINISTIC_HASHING=1` and in three without it. This scenario keeps one consumer in share()'s `sides` dictionary, so dictionary order doesn't matter here.

The rate is lower than the external prototype's 130 of 1,000 for the same case. The two schedulers have different scheduling points and ready sets, so their rates aren't comparable. Neither predicts real-runtime frequency.

### Case 12: the interleaving

From `088f92a` with `bounded(1)`, using the scheduler trace plus temporary prints in a scratch copy of `AsyncShareSequence.swift`:

| Step | FIFO (released) | Seed 2 (leaked) |
|---|---|---|
| 1 | iteration: emits 1, which resumes the consumer | iteration: emits 1, which resumes the consumer |
| 2 | iteration: re-enters `iterate()` and passes the limit check | iteration: re-enters `iterate()` (step 28, `pick 2 of 2`) |
| 3 | iteration: demand check, no consumer waiting, **parks on `demand`** | consumer (step 29, `pick 1 of 2`): `break` calls `unregisterSide`. Then `shared = nil` calls `cancel()`, which **finds no `demand` parked** and resumes nothing |
| 4 | consumer: `break` calls `unregisterSide`. `shared = nil` calls `cancel()`, which finds `demand` parked and resumes it | iteration (step 30): passes the limit check. Demand check: no consumer waiting, **parks on `demand`** |
| 5 | iteration: sees `.cancelled`, ends, and releases the base iterator | nothing will ever resume it. The task, buffer and base iterator stay alive |

With `.unbounded` there is no limit section. The window is the whole of `iterate()` after `emit 1`. In seed 212 the iteration task's next job waits through nine other steps, and the consumer breaks and releases the sequence during them. This is the same race the prototype's independent analysis found and then reproduced on the real runtime. The fix's part 3 closes it: the demand section returns at once when the iteration is already cancelled.

## Operator scenarios: first pass

`Tests/AsyncAlgorithmsTests/TestSeededOperatorRelease.swift` runs 45 resource-release scenarios over five operators: `zip`, `merge`, `combineLatest` (6 each), `flatMapLatest` (7) and `buffer` (5 per policy, 4 policies). It is opt-in (`ASYNC_VALIDATION_EXPLORE=1`), because it reports open bugs.

**How it works.**
- Each scenario runs inside one diagram tick, so the scheduler owns every job.
- Base sequences are tracked sources. Each `next()` starts with `Task.yield()` and returns nil once the source is cancelled. A source can be finite, fail after N elements, or park on a gate, which either honours or ignores cancellation. An "infinite" source is capped at 500 elements, and reaching the cap counts as a violation.
- When the diagram ends, three things are checked: a base iterator that was never deallocated (**leak**), a scenario that never finished (**hang**), and a failed output invariant (**wrong**).
- Scenarios: break after the first element; run to completion; one base finishes early; one base throws while another is parked; cancel the consumer while a base is parked on a gate, once with a base that ignores cancellation and once with one that honours it. `flatMapLatest` has its own variants: outer throws while an inner is parked, inner throws, and outer finishes before the inner.
- FIFO plus seeds 1...1000 for every scenario took 45 s in all.

**Results**

| Scenario | FIFO | Seeds 1...1000 | Real runtime | Status |
|---|---|---|---|---|
| `buffer`, all 4 policies, break after first | leak (`bounded(1)`), or base pulled to the cap (the others) | 1000/1000 | confirmed: base never released; unbounded policies pull about 520 elements a second | **known: open PR apple/swift-async-algorithms#352, same fix; also fixed on this branch.** The producer `Task` captures the storage strongly, and only `deinit` calls `interrupted()`, so dropping the iterator never cancels the producer. With the fix, all four scenarios pass under FIFO and 1,000 seeds |
| `buffer`, all 4 policies, cancel while the base ignores cancellation | base pulled to the cap | 1000/1000 | not run | **design question, not a bug.** After cancellation the producer loop keeps pulling a base that ignores cancellation. `zip`, `merge` and `combineLatest` stop in the same scenario |
| `flatMapLatest`, break after first | outer pulled to the cap | 1000/1000 leak (outer, inner) | confirmed: inner iterator never released | **not on #461's list.** The storage `deinit` cancels nothing. Still leaks on #462's head `af849f7`: 1000/1000 seeds, and FIFO pulls the outer to the cap |
| `flatMapLatest`, cancel while the inner honours cancellation | leak (inner) | 1000/1000 | not rerun | known: #461, the `cancelled()` path. **Fixed by apple/swift-async-algorithms#462**: 0/1000 seeds leak on its head `af849f7` |
| `flatMapLatest`, outer throws while the inner is parked | leak (inner) | 901/1000 | not rerun | known: #461, the `outerThrew` path. The 99 clean seeds are orders where the error arrives while no consumer is waiting. **Fixed by #462**: 0/1000 seeds leak |
| `zip`, `merge`, `combineLatest`: all 18 scenarios | ok | 0 failures | | |
| `flatMapLatest`: run to completion, outer finishes first, inner throws, cancel while the inner ignores cancellation | ok | 0 failures | | the inner-throws scenario uses a finite outer, so it doesn't exercise #461's `innerThrew` path |
| `buffer`: run to completion, base throws, cancel while the base honours cancellation (4 policies each) | ok | 0 failures | | |

**What the seeds added this time: nothing beyond FIFO.** Every bug above fails under FIFO, so the leak and hang checks did the finding. The seeds' contribution is negative evidence: 18 `zip`/`merge`/`combineLatest` scenarios hold under 1,000 interleavings each.

**Not done.**
- Running these scenarios against your `fix-472` branch. They were run against PR #462 (head `af849f7`, with this branch's scheduler and scenario commits cherry-picked on top): its two #461 fixes hold under FIFO and 1,000 seeds, all 7 `flatMapLatest` scenarios except break-after-first pass, and the `break` leak remains.
- Getting the `buffer` fix merged upstream. The open PR #352 already has the fix, and a comment there offers the regression tests.
- Real-runtime reruns of the two known #461 paths.

### The `buffer` fix

`AsyncBufferSequence.Iterator` now holds an `InternalClass`, created only for bounded and unbounded storage. Its `deinit` calls `storage.interrupted()` once the last copy of the iterator is gone. `interrupted()` already existed, and the storage's own `deinit` calls it, but that `deinit` could never run while the producer task held the storage. `interrupted()` cancels the producer task and resumes a producer parked on a full buffer. It does nothing in the `.finished` state, so the extra call from the storage's `deinit` is harmless. The transparent case (limit 0) runs no task and is unchanged.

Regression tests in `TestBuffer`, on the real runtime:
- A `break` under each of the four policies must release the base iterator within 2 s. Without the fix, each of the four fails within about 4 s when run alone. Run together, they hang the test process, because the leftover unbounded producers never stop pulling.
- One test checks the opposite: dropping one copy of a bounded iterator must not stop the producer while another copy is still iterating.
- The full suite gives 381 tests, 0 failures and 7 skips (the opt-in scenario tests).

Not changed: after cancellation, the producer still pulls a base that ignores cancellation until that base ends. That is the "design question" row above.

## Limitations and open questions

- **Granularity is one job.** Choices happen only between jobs the runtime enqueues. A task's synchronous stretch between suspensions is never split, which is also true on the real runtime. Single-pipeline diagrams therefore get no choices.
- **Jobs outside the hook escape.** `Task.sleep` on a real clock, `MainActor`, custom executors and Dispatch callbacks bypass `swift_task_enqueueGlobal_hook`, as they already do for FIFO. The scheduler doesn't detect escapes.
- **Dictionary iteration needs `SWIFT_DETERMINISTIC_HASHING=1`** for a seed to replay across processes. The failure message says so.
- **Not run on Linux.** SplitMix64 and the candidate order are platform-independent by construction, since `jobs` are enumerated in first-seen order and no addresses or hashes are involved. Linux replay is still unverified.
- **The case-12 check is opt-in on `main`.** It becomes an ordinary test once a #473 fix lands. Both `f1b8dcc` and PR #480 pass it.
- **Public API surface.** An upstream proposal adds `SchedulingStrategy`, `test(theme:scheduling:_:)` and two environment variables. A smaller alternative is to keep everything behind the environment variable and `@_spi`.

<details><summary>Reproducing</summary>

All commands run from the repository root. `S` is any scratch directory.

```bash
swift build --build-tests
B=.build/debug/AsyncAlgorithmsTests.xctest
```

Run every `validate` test under one seed, with a trace:

```bash
ASYNC_VALIDATION_SCHEDULE_SEED=2 ASYNC_VALIDATION_SCHEDULE_TRACE=1 SWIFT_DETERMINISTIC_HASHING=1 xcrun xctest -XCTest AsyncAlgorithmsTests.TestChunk $B
```

Sweep seeds (the list of 102 `validate` tests was produced by scanning the test files for `validate {` / `validate(` calls):

```bash
for s in $(seq 1 1000); do ASYNC_VALIDATION_SCHEDULE_SEED=$s SWIFT_DETERMINISTIC_HASHING=1 xcrun xctest -XCTest "$(paste -sd, validate_tests.txt)" $B 2>&1 | grep -q "with 0 failures" || echo "seed $s failed"; done
```

Case 12 before and after the fix (scratch worktrees; nothing is merged into this branch):

```bash
git diff 963eecf -- Sources > $S/strategy.patch
git worktree add --detach $S/prefix 088f92a
git worktree add --detach $S/fixed f1b8dcc
for d in prefix fixed; do (cd $S/$d && git apply $S/strategy.patch && cp $OLDPWD/Sources/AsyncSequenceValidation/SchedulingStrategy.swift Sources/AsyncSequenceValidation/ && cp $OLDPWD/Tests/AsyncAlgorithmsTests/TestSeeded*.swift Tests/AsyncAlgorithmsTests/ && swift build --build-tests && ASYNC_VALIDATION_SHARE_CASE12=1 SWIFT_DETERMINISTIC_HASHING=1 xcrun xctest -XCTest AsyncAlgorithmsTests.TestSeededShareRelease .build/debug/AsyncAlgorithmsTests.xctest | grep '^case12'); done
```

Expected output:

```
prefix: case12 bounded(1): fifo released, 56 of 1000 seeds leaked: [2, 21, 70, 112, 128, ...]
prefix: case12 unbounded: fifo released, 16 of 1000 seeds leaked: [212, 231, 238, 243, ...]
fixed:  case12 bounded(1): fifo released, 0 of 1000 seeds leaked: []
fixed:  case12 unbounded: fifo released, 0 of 1000 seeds leaked: []
```

`ASYNC_VALIDATION_SHARE_CASE12_SEEDS=N` changes the seed count.

</details>
