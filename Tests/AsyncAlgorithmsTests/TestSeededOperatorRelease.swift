//===----------------------------------------------------------------------===//
//
// This source file is part of the Swift Async Algorithms open source project
//
// Copyright (c) 2022 Apple Inc. and the Swift project authors
// Licensed under Apache License v2.0 with Runtime Library Exception
//
// See https://swift.org/LICENSE.txt for license information
//
//===----------------------------------------------------------------------===//

#if canImport(Darwin) || canImport(Glibc) || canImport(Musl) || canImport(Bionic) || canImport(wasi_pthread)
import XCTest
import AsyncAlgorithms
import AsyncSequenceValidation

/// Resource-release and termination scenarios for operators that run their own tasks, explored under
/// the validation scheduler's seeded job ordering.
///
/// Each scenario runs inside one tick of a validation diagram, so the scheduler owns every job. When
/// the diagram ends, the scheduler has nothing left to run, and three things are checked:
/// - **leak**: an iterator of a base sequence was never deallocated;
/// - **hang**: the scenario never finished (some task is parked forever);
/// - **wrong**: an invariant on the output failed.
///
/// Opt-in, because it reports open bugs: set `ASYNC_VALIDATION_EXPLORE=1`. Other variables:
/// - `ASYNC_VALIDATION_EXPLORE_SEEDS`: seeds 1...N (default 1000), plus FIFO;
/// - `ASYNC_VALIDATION_EXPLORE_ONLY`: run only the scenarios whose `operator/scenario` name contains this;
/// - `ASYNC_VALIDATION_EXPLORE_SEED`: run only this seed (with `ASYNC_VALIDATION_SCHEDULE_TRACE=1` to trace it).
///
/// Run with `SWIFT_DETERMINISTIC_HASHING=1` so that a seed replays across processes.
@available(AsyncAlgorithms 1.3, *)
final class TestSeededOperatorRelease: XCTestCase {
  typealias Strategy = AsyncSequenceValidationDiagram.SchedulingStrategy
  typealias Stream = any AsyncSequence<String, any Error>

  // MARK: - Probe and sources

  /// Records which base iterators were made and released, which sources are parked on their gate, and
  /// any invariant violations.
  final class Probe: @unchecked Sendable {
    private let lock = NSLock()
    private var made = [String: Int]()
    private var released = [String: Int]()
    private var enteredNames = Set<String>()
    private var waiters = [String: [UnsafeContinuation<Void, Never>]]()
    private var _violations = [String]()
    private var watched = [(name: String, object: () -> AnyObject?)]()
    private var _clock: AsyncSequenceValidationDiagram.Clock?
    private var _finished = false

    /// Set once the scenario body has returned.
    var finished: Bool {
      get { lock.withLock { _finished } }
      set { lock.withLock { _finished = newValue } }
    }

    /// The diagram's virtual clock, for scenarios that need time to pass.
    var clock: AsyncSequenceValidationDiagram.Clock {
      get { lock.withLock { _clock! } }
      set { lock.withLock { _clock = newValue } }
    }

    /// Reports `object` as a leak if it is still alive when the scenario is checked.
    func watch(_ name: String, _ object: AnyObject) {
      lock.withLock { watched.append((name, { [weak object] in object })) }
    }

    final class Tracker: Sendable {
      let probe: Probe
      let name: String
      init(_ probe: Probe, _ name: String) {
        self.probe = probe
        self.name = name
        probe.lock.withLock { probe.made[name, default: 0] += 1 }
      }
      deinit {
        let probe = self.probe
        let name = self.name
        probe.lock.withLock { probe.released[name, default: 0] += 1 }
      }
    }

    func violation(_ message: String) {
      lock.withLock { _violations.append(message) }
    }

    var violations: [String] { lock.withLock { _violations } }

    var unreleased: [String] {
      lock.withLock {
        made.keys.sorted().filter { made[$0]! != released[$0, default: 0] }
          + watched.filter { $0.object() != nil }.map(\.name)
      }
    }

    func entered(_ name: String) {
      let resume = lock.withLock { () -> [UnsafeContinuation<Void, Never>] in
        enteredNames.insert(name)
        return waiters.removeValue(forKey: name) ?? []
      }
      for continuation in resume { continuation.resume() }
    }

    func awaitEntered(_ name: String) async {
      await withUnsafeContinuation { (continuation: UnsafeContinuation<Void, Never>) in
        let now = lock.withLock { () -> Bool in
          if enteredNames.contains(name) { return true }
          waiters[name, default: []].append(continuation)
          return false
        }
        if now { continuation.resume() }
      }
    }
  }

  /// A gate that `enter()` waits on until `open()`. Waiting ignores cancellation.
  final class Gate: @unchecked Sendable {
    private let lock = NSLock()
    private var isOpen = false
    private var waiters = [UnsafeContinuation<Void, Never>]()

    func enter() async {
      await withUnsafeContinuation { (continuation: UnsafeContinuation<Void, Never>) in
        let now = lock.withLock { () -> Bool in
          if isOpen { return true }
          waiters.append(continuation)
          return false
        }
        if now { continuation.resume() }
      }
    }

    func open() {
      let woken = lock.withLock { () -> [UnsafeContinuation<Void, Never>] in
        isOpen = true
        defer { waiters.removeAll() }
        return waiters
      }
      for waiter in woken { waiter.resume() }
    }
  }

  struct ScenarioError: Error, Equatable {}

  /// A base sequence of `offset + 1, offset + 2, ...`.
  ///
  /// Every `next()` starts with `Task.yield()`, a scheduling point the real runtime would also allow,
  /// and returns nil once cancelled. With `count` nil it is "infinite", capped at `cap` elements: a
  /// source that reaches the cap records a violation, because the operator kept pulling.
  struct Source: AsyncSequence, Sendable {
    typealias Element = Int
    typealias Failure = any Error
    static let cap = 500

    let name: String
    let probe: Probe
    var count: Int? = nil
    var offset = 0
    /// Throw instead of producing element number `failAfter + 1`.
    var failAfter: Int? = nil
    /// After producing this many elements, wait on `gate` before each further element.
    var gateAfter: Int? = nil
    var gate: Gate? = nil
    var honoursCancellation = true
    /// Sleep this long on the diagram clock before each element.
    var interval: AsyncSequenceValidationDiagram.Clock.Step? = nil

    struct AsyncIterator: AsyncIteratorProtocol, Sendable {
      let source: Source
      let tracker: Probe.Tracker
      var produced = 0

      mutating func next() async throws -> Int? {
        await Task.yield()
        if let interval = source.interval {
          do {
            try await source.probe.clock.sleep(for: interval)
          } catch {
            if source.honoursCancellation { return nil }
          }
        }
        if let gateAfter = source.gateAfter, produced >= gateAfter, let gate = source.gate {
          source.probe.entered(source.name)
          if source.honoursCancellation {
            await withTaskCancellationHandler {
              await gate.enter()
            } onCancel: {
              gate.open()
            }
          } else {
            await gate.enter()
          }
        }
        if source.honoursCancellation && Task.isCancelled { return nil }
        if let failAfter = source.failAfter, produced == failAfter {
          source.probe.entered("\(source.name) threw")
          throw ScenarioError()
        }
        if produced == (source.count ?? Source.cap) {
          if source.count == nil {
            source.probe.violation("\(source.name) was pulled \(Source.cap) times")
          }
          return nil
        }
        produced += 1
        return source.offset + produced
      }
    }

    func makeAsyncIterator() -> AsyncIterator {
      AsyncIterator(source: self, tracker: Probe.Tracker(probe, name))
    }
  }

  static func collect(_ stream: Stream, limit: Int = .max) async -> (values: [String], error: (any Error)?) {
    var values = [String]()
    do {
      for try await value in stream {
        values.append(value)
        if values.count >= limit { break }
      }
      return (values, nil)
    } catch {
      return (values, error)
    }
  }

  /// Starts a consumer that asks for elements until it is cancelled, waits until `parked` is parked on
  /// its gate, cancels the consumer, waits for it to finish, then opens `gate` if given.
  static func cancelWhileParked(
    _ probe: Probe,
    parked: String,
    open gate: Gate?,
    _ make: @escaping @Sendable () -> Stream
  ) async {
    let consumer = Task {
      _ = await collect(make())
    }
    await probe.awaitEntered(parked)
    consumer.cancel()
    _ = await consumer.value
    gate?.open()
  }

  // MARK: - Scenarios

  struct Scenario: Sendable {
    let name: String
    /// How many ticks of virtual time the diagram runs (scenarios without a clock finish in tick 0).
    var ticks = 3
    let body: @Sendable (Probe) async -> Void

    init(name: String, ticks: Int = 3, body: @escaping @Sendable (Probe) async -> Void) {
      self.name = name
      self.ticks = ticks
      self.body = body
    }
  }

  /// `zip`, `merge` and `combineLatest` share these, with `make` building the operator from two bases.
  static func twoBaseScenarios(
    _ op: String,
    make: @escaping @Sendable (Source, Source) -> Stream,
    checkComplete: @escaping @Sendable ([String]) -> String?,
    earlyFinish: (limit: Int, check: @Sendable ([String]) -> String?)
  ) -> [Scenario] {
    [
      Scenario(name: "\(op)/break-after-first") { probe in
        let stream = make(Source(name: "a", probe: probe), Source(name: "b", probe: probe, offset: 100))
        let (values, error) = await collect(stream, limit: 1)
        if values.count != 1 || error != nil { probe.violation("got \(values) \(String(describing: error))") }
      },
      Scenario(name: "\(op)/run-to-completion") { probe in
        let stream = make(
          Source(name: "a", probe: probe, count: 3),
          Source(name: "b", probe: probe, count: 3, offset: 100)
        )
        let (values, error) = await collect(stream)
        if let error { probe.violation("unexpected error \(error)") }
        if let problem = checkComplete(values) { probe.violation(problem) }
      },
      Scenario(name: "\(op)/one-base-finishes-early") { probe in
        let stream = make(Source(name: "a", probe: probe, count: 1), Source(name: "b", probe: probe, offset: 100))
        let (values, error) = await collect(stream, limit: earlyFinish.limit)
        if let error { probe.violation("unexpected error \(error)") }
        if let problem = earlyFinish.check(values) { probe.violation(problem) }
      },
      Scenario(name: "\(op)/throw-while-other-parked") { probe in
        let gate = Gate()
        let stream = make(
          Source(name: "a", probe: probe, failAfter: 1),
          Source(name: "b", probe: probe, offset: 100, gateAfter: 1, gate: gate)
        )
        let (values, error) = await collect(stream)
        if !(error is ScenarioError) {
          probe.violation("expected the failure, got \(values) \(String(describing: error))")
        }
      },
      Scenario(name: "\(op)/cancel-while-base-ignores-cancellation") { probe in
        let gate = Gate()
        await cancelWhileParked(probe, parked: "a", open: gate) {
          make(
            Source(name: "a", probe: probe, gateAfter: 0, gate: gate, honoursCancellation: false),
            Source(name: "b", probe: probe, count: 2, offset: 100)
          )
        }
      },
      Scenario(name: "\(op)/cancel-while-base-honours-cancellation") { probe in
        let gate = Gate()
        await cancelWhileParked(probe, parked: "a", open: nil) {
          make(
            Source(name: "a", probe: probe, gateAfter: 0, gate: gate),
            Source(name: "b", probe: probe, count: 2, offset: 100)
          )
        }
      },
    ]
  }

  static var zipScenarios: [Scenario] {
    twoBaseScenarios(
      "zip",
      make: { a, b in zip(a, b).map { "\($0.0),\($0.1)" } },
      checkComplete: { $0 == ["1,101", "2,102", "3,103"] ? nil : "got \($0)" },
      earlyFinish: (.max, { $0 == ["1,101"] ? nil : "got \($0)" })
    )
  }

  static var combineLatestScenarios: [Scenario] {
    // Each output pairs the latest of each side, so both components never decrease, and the last
    // output is (3, 103).
    let monotonic: @Sendable ([String]) -> String? = { values in
      let pairs = values.map { $0.split(separator: ",").map { Int($0)! } }
      for (previous, next) in zip(pairs, pairs.dropFirst()) where next[0] < previous[0] || next[1] < previous[1] {
        return "not monotonic: \(values)"
      }
      return nil
    }
    return twoBaseScenarios(
      "combineLatest",
      make: { a, b in combineLatest(a, b).map { "\($0.0),\($0.1)" } },
      checkComplete: { values in
        if values.last != "3,103" { return "last is not 3,103: \(values)" }
        return monotonic(values)
      },
      earlyFinish: (
        5,
        { values in
          guard values.allSatisfy({ $0.hasPrefix("1,") }) else { return "got \(values)" }
          return monotonic(values)
        }
      )
    )
  }

  static var mergeScenarios: [Scenario] {
    // Each side's elements arrive in order, and none is lost or invented.
    let ordered: @Sendable ([String], Int, Int) -> String? = { values, aCount, bCount in
      let numbers = values.map { Int($0)! }
      let a = numbers.filter { $0 < 100 }
      let b = numbers.filter { $0 > 100 }
      if a != Array(1...aCount) { return "side a: \(values)" }
      if b != Array(101...(100 + bCount)) { return "side b: \(values)" }
      return nil
    }
    return twoBaseScenarios(
      "merge",
      make: { a, b in merge(a, b).map { "\($0)" } },
      checkComplete: { ordered($0, 3, 3) },
      earlyFinish: (
        5,
        { values in
          let numbers = values.map { Int($0)! }
          let b = numbers.filter { $0 > 100 }
          if numbers.filter({ $0 < 100 }).count > 1 || b != (0..<b.count).map({ 101 + $0 }) { return "got \(values)" }
          return nil
        }
      )
    )
  }

  static var flatMapLatestScenarios: [Scenario] {
    [
      Scenario(name: "flatMapLatest/break-after-first") { probe in
        let stream: Stream = Source(name: "outer", probe: probe)
          .flatMapLatest { x in Source(name: "inner\(x)", probe: probe, offset: x * 1000) }
          .map { "\($0)" }
        let (values, error) = await collect(stream, limit: 1)
        if values.count != 1 || error != nil { probe.violation("got \(values) \(String(describing: error))") }
      },
      Scenario(name: "flatMapLatest/run-to-completion") { probe in
        let stream: Stream = Source(name: "outer", probe: probe, count: 3)
          .flatMapLatest { x in Source(name: "inner\(x)", probe: probe, count: 2, offset: x * 10) }
          .map { "\($0)" }
        let (values, error) = await collect(stream)
        if let error { probe.violation("unexpected error \(error)") }
        // The last inner sequence is never replaced, so it is emitted in full, last. Earlier inners
        // contribute only in order.
        let numbers = values.map { Int($0)! }
        if Array(numbers.suffix(2)) != [31, 32] || numbers != numbers.sorted() {
          probe.violation("got \(values)")
        }
      },
      Scenario(name: "flatMapLatest/outer-finishes-before-inner") { probe in
        let stream: Stream = Source(name: "outer", probe: probe, count: 1)
          .flatMapLatest { x in Source(name: "inner\(x)", probe: probe, count: 3, offset: x * 10) }
          .map { "\($0)" }
        let (values, error) = await collect(stream)
        if values != ["11", "12", "13"] || error != nil {
          probe.violation("got \(values) \(String(describing: error))")
        }
      },
      Scenario(name: "flatMapLatest/outer-throws-while-inner-parked") { probe in
        let gate = Gate()
        let stream: Stream = Source(name: "outer", probe: probe, failAfter: 1)
          .flatMapLatest { x in Source(name: "inner\(x)", probe: probe, offset: x * 10, gateAfter: 0, gate: gate) }
          .map { "\($0)" }
        let (values, error) = await collect(stream)
        if !(error is ScenarioError) {
          probe.violation("expected the failure, got \(values) \(String(describing: error))")
        }
      },
      Scenario(name: "flatMapLatest/outer-throws-between-next-calls") { probe in
        // #472: the outer fails while the consumer is between `next()` calls, so no demand is pending.
        // The next `next()` must throw that failure, not finish with nil.
        let outerGate = Gate()
        let innerGate = Gate()
        let stream: Stream = Source(name: "outer", probe: probe, failAfter: 1, gateAfter: 1, gate: outerGate)
          .flatMapLatest { x in Source(name: "inner\(x)", probe: probe, offset: x * 10, gateAfter: 1, gate: innerGate) }
          .map { "\($0)" }
        var iterator = stream.makeAsyncIterator()
        do {
          let first = try await iterator.next()
          if first != "11" { probe.violation("first element was \(String(describing: first))") }
          // Let the outer fail, and stay out of `next()` until it has thrown.
          outerGate.open()
          await probe.awaitEntered("outer threw")
          let second = try await iterator.next()
          probe.violation("expected the outer failure, got \(String(describing: second))")
        } catch is ScenarioError {
          // expected
        } catch {
          probe.violation("unexpected error \(error)")
        }
      },
      Scenario(name: "flatMapLatest/inner-throws") { probe in
        let stream: Stream = Source(name: "outer", probe: probe, count: 1)
          .flatMapLatest { x in Source(name: "inner\(x)", probe: probe, offset: x * 10, failAfter: 1) }
          .map { "\($0)" }
        let (values, error) = await collect(stream)
        if values != ["11"] || !(error is ScenarioError) {
          probe.violation("got \(values) \(String(describing: error))")
        }
      },
      Scenario(name: "flatMapLatest/cancel-while-inner-ignores-cancellation") { probe in
        let gate = Gate()
        await cancelWhileParked(probe, parked: "inner1", open: gate) {
          Source(name: "outer", probe: probe, count: 1)
            .flatMapLatest { x in
              Source(name: "inner\(x)", probe: probe, gateAfter: 0, gate: gate, honoursCancellation: false)
            }
            .map { "\($0)" }
        }
      },
      Scenario(name: "flatMapLatest/cancel-while-inner-honours-cancellation") { probe in
        let gate = Gate()
        await cancelWhileParked(probe, parked: "inner1", open: nil) {
          Source(name: "outer", probe: probe, count: 1)
            .flatMapLatest { x in Source(name: "inner\(x)", probe: probe, gateAfter: 0, gate: gate) }
            .map { "\($0)" }
        }
      },
    ]
  }

  static var bufferScenarios: [Scenario] {
    let policies: [(String, AsyncBufferSequencePolicy)] = [
      ("bounded(1)", .bounded(1)),
      ("unbounded", .unbounded),
      ("bufferingLatest(1)", .bufferingLatest(1)),
      ("bufferingOldest(1)", .bufferingOldest(1)),
    ]
    return policies.flatMap { label, policy -> [Scenario] in
      [
        Scenario(name: "buffer.\(label)/break-after-first") { probe in
          let stream: Stream = Source(name: "base", probe: probe).buffer(policy: policy).map { "\($0)" }
          let (values, error) = await collect(stream, limit: 1)
          if values != ["1"] || error != nil { probe.violation("got \(values) \(String(describing: error))") }
        },
        Scenario(name: "buffer.\(label)/run-to-completion") { probe in
          let stream: Stream = Source(name: "base", probe: probe, count: 5).buffer(policy: policy).map { "\($0)" }
          let (values, error) = await collect(stream)
          if let error { probe.violation("unexpected error \(error)") }
          let numbers = values.map { Int($0)! }
          let increasing = numbers == numbers.sorted() && Set(numbers).count == numbers.count
          let complete: Bool
          switch label {
          case "bufferingLatest(1)": complete = numbers.last == 5
          case "bufferingOldest(1)": complete = numbers.first == 1
          default: complete = numbers == [1, 2, 3, 4, 5]
          }
          if !increasing || !complete { probe.violation("got \(values)") }
        },
        Scenario(name: "buffer.\(label)/base-throws") { probe in
          let stream: Stream = Source(name: "base", probe: probe, failAfter: 2).buffer(policy: policy).map { "\($0)" }
          let (values, error) = await collect(stream)
          if !(error is ScenarioError) {
            probe.violation("expected the failure, got \(values) \(String(describing: error))")
          }
        },
        Scenario(name: "buffer.\(label)/cancel-while-base-ignores-cancellation") { probe in
          let gate = Gate()
          await cancelWhileParked(probe, parked: "base", open: gate) {
            Source(name: "base", probe: probe, gateAfter: 0, gate: gate, honoursCancellation: false)
              .buffer(policy: policy).map { "\($0)" }
          }
        },
        Scenario(name: "buffer.\(label)/cancel-while-base-honours-cancellation") { probe in
          let gate = Gate()
          await cancelWhileParked(probe, parked: "base", open: nil) {
            Source(name: "base", probe: probe, gateAfter: 0, gate: gate).buffer(policy: policy).map { "\($0)" }
          }
        },
      ]
    }
  }

  /// The three-input `zip`, `merge` and `combineLatest` run the two-input scenarios with a third base,
  /// "c", that copies b's length and is offset by 200.
  static func mirror(_ b: Source) -> Source {
    Source(name: "c", probe: b.probe, count: b.count, offset: 200)
  }

  static var zip3Scenarios: [Scenario] {
    twoBaseScenarios(
      "zip3",
      make: { a, b in zip(a, b, mirror(b)).map { "\($0.0),\($0.1),\($0.2)" } },
      checkComplete: { $0 == ["1,101,201", "2,102,202", "3,103,203"] ? nil : "got \($0)" },
      earlyFinish: (.max, { $0 == ["1,101,201"] ? nil : "got \($0)" })
    )
  }

  static var combineLatest3Scenarios: [Scenario] {
    let monotonic: @Sendable ([String]) -> String? = { values in
      let triples = values.map { $0.split(separator: ",").map { Int($0)! } }
      for (previous, next) in zip(triples, triples.dropFirst()) where zip(previous, next).contains(where: { $1 < $0 }) {
        return "not monotonic: \(values)"
      }
      return nil
    }
    return twoBaseScenarios(
      "combineLatest3",
      make: { a, b in combineLatest(a, b, mirror(b)).map { "\($0.0),\($0.1),\($0.2)" } },
      checkComplete: { values in
        if values.last != "3,103,203" { return "last is not 3,103,203: \(values)" }
        return monotonic(values)
      },
      earlyFinish: (
        5,
        { values in
          guard values.allSatisfy({ $0.hasPrefix("1,") }) else { return "got \(values)" }
          return monotonic(values)
        }
      )
    )
  }

  static var merge3Scenarios: [Scenario] {
    let sides: @Sendable ([String]) -> (a: [Int], b: [Int], c: [Int]) = { values in
      let numbers = values.map { Int($0)! }
      return (numbers.filter { $0 < 100 }, numbers.filter { $0 > 100 && $0 < 200 }, numbers.filter { $0 > 200 })
    }
    return twoBaseScenarios(
      "merge3",
      make: { a, b in merge(a, b, mirror(b)).map { "\($0)" } },
      checkComplete: { values in
        let (a, b, c) = sides(values)
        return a == [1, 2, 3] && b == [101, 102, 103] && c == [201, 202, 203] ? nil : "got \(values)"
      },
      earlyFinish: (
        5,
        { values in
          let (a, b, c) = sides(values)
          let inOrder = b == Array(101..<(101 + b.count)) && c == Array(201..<(201 + c.count))
          return a.count <= 1 && inOrder ? nil : "got \(values)"
        }
      )
    )
  }

  static var debounceScenarios: [Scenario] {
    // The base sleeps 2 steps on the diagram clock before each element; debounce waits for 1 quiet
    // step, so every element comes through.
    let ticks = 40
    @Sendable func base(
      _ probe: Probe,
      count: Int? = nil,
      failAfter: Int? = nil,
      gateAfter: Int? = nil,
      gate: Gate? = nil,
      honours: Bool = true
    ) -> Source {
      Source(
        name: "base",
        probe: probe,
        count: count,
        failAfter: failAfter,
        gateAfter: gateAfter,
        gate: gate,
        honoursCancellation: honours,
        interval: .steps(2)
      )
    }
    @Sendable func debounced(_ source: Source, _ probe: Probe) -> Stream {
      source.debounce(for: .steps(1), clock: probe.clock).map { "\($0)" }
    }
    return [
      Scenario(name: "debounce/break-after-first", ticks: ticks) { probe in
        let (values, error) = await collect(debounced(base(probe), probe), limit: 1)
        if values != ["1"] || error != nil { probe.violation("got \(values) \(String(describing: error))") }
      },
      Scenario(name: "debounce/run-to-completion", ticks: ticks) { probe in
        let (values, error) = await collect(debounced(base(probe, count: 3), probe))
        if values != ["1", "2", "3"] || error != nil { probe.violation("got \(values) \(String(describing: error))") }
      },
      Scenario(name: "debounce/base-throws", ticks: ticks) { probe in
        let (values, error) = await collect(debounced(base(probe, failAfter: 1), probe))
        if !(error is ScenarioError) {
          probe.violation("expected the failure, got \(values) \(String(describing: error))")
        }
      },
      Scenario(name: "debounce/cancel-while-base-ignores-cancellation", ticks: ticks) { probe in
        let gate = Gate()
        await cancelWhileParked(probe, parked: "base", open: gate) {
          debounced(base(probe, gateAfter: 1, gate: gate, honours: false), probe)
        }
      },
      Scenario(name: "debounce/cancel-while-base-honours-cancellation", ticks: ticks) { probe in
        let gate = Gate()
        await cancelWhileParked(probe, parked: "base", open: nil) {
          debounced(base(probe, gateAfter: 1, gate: gate), probe)
        }
      },
      Scenario(name: "debounce/cancelled-between-next-calls", ticks: ticks) { probe in
        // #475: the consumer takes one element, is cancelled while it is outside `next()`, and then
        // calls `next()` again, which returns nil. The base must still be released.
        let gate = Gate()
        let consumer = Task {
          var iterator = debounced(base(probe, gateAfter: 1, gate: gate), probe).makeAsyncIterator()
          let first = try? await iterator.next()
          if first != "1" { probe.violation("first element was \(String(describing: first))") }
          probe.entered("consumer has the first element")
          let hold = Gate()
          await withTaskCancellationHandler {
            await hold.enter()
          } onCancel: {
            hold.open()
          }
          let second = try? await iterator.next()
          if second != nil { probe.violation("expected nil after cancellation, got \(String(describing: second))") }
        }
        await probe.awaitEntered("consumer has the first element")
        consumer.cancel()
        await consumer.value
      },
    ]
  }

  /// `AsyncChannel` and `AsyncThrowingChannel` share these. A channel that is still alive when the
  /// scenario is checked is reported as a leak: something still holds it, such as a parked sender.
  struct ChannelUnderTest: Sendable {
    let channel: AnyObject & Sendable
    let send: @Sendable (Int) async -> Void
    let finish: @Sendable () -> Void
    let stream: @Sendable () -> Stream
  }

  static func channelScenarios(_ name: String, make: @escaping @Sendable () -> ChannelUnderTest) -> [Scenario] {
    [
      Scenario(name: "\(name)/two-senders-then-finish") { probe in
        let channel = make()
        probe.watch("channel", channel.channel)
        let consumer = Task { await collect(channel.stream()) }
        let a = Task { for value in 1...3 { await channel.send(value) } }
        let b = Task { for value in 101...103 { await channel.send(value) } }
        await a.value
        await b.value
        channel.finish()
        let (values, error) = await consumer.value
        let numbers = values.map { Int($0)! }
        if error != nil || numbers.filter({ $0 < 100 }) != [1, 2, 3] || numbers.filter({ $0 > 100 }) != [101, 102, 103]
        {
          probe.violation("got \(values) \(String(describing: error))")
        }
      },
      Scenario(name: "\(name)/consumer-breaks-while-senders-suspended") { probe in
        let channel = make()
        probe.watch("channel", channel.channel)
        let a = Task { for value in 1...3 { await channel.send(value) } }
        let b = Task { for value in 101...103 { await channel.send(value) } }
        let (values, _) = await collect(channel.stream(), limit: 1)
        if values.count != 1 { probe.violation("got \(values)") }
        // Senders still parked in `send` must be resumed by `finish()`.
        channel.finish()
        await a.value
        await b.value
      },
      Scenario(name: "\(name)/cancel-suspended-sender") { probe in
        let channel = make()
        probe.watch("channel", channel.channel)
        let sender = Task { await channel.send(1) }
        sender.cancel()
        await sender.value
        channel.finish()
        let (values, error) = await collect(channel.stream())
        if error != nil || values.count > 1 { probe.violation("got \(values) \(String(describing: error))") }
      },
      Scenario(name: "\(name)/cancel-waiting-consumer") { probe in
        let channel = make()
        probe.watch("channel", channel.channel)
        let consumer = Task { await collect(channel.stream()) }
        consumer.cancel()
        let (values, error) = await consumer.value
        if !values.isEmpty || error != nil { probe.violation("got \(values) \(String(describing: error))") }
        channel.finish()
      },
      Scenario(name: "\(name)/finish-while-consumer-waiting") { probe in
        let channel = make()
        probe.watch("channel", channel.channel)
        let consumer = Task { await collect(channel.stream()) }
        await Task.yield()
        channel.finish()
        let (values, error) = await consumer.value
        if !values.isEmpty || error != nil { probe.violation("got \(values) \(String(describing: error))") }
      },
      Scenario(name: "\(name)/two-consumers-one-sender") { probe in
        let channel = make()
        probe.watch("channel", channel.channel)
        let first = Task { await collect(channel.stream()) }
        let second = Task { await collect(channel.stream()) }
        for value in 1...6 { await channel.send(value) }
        channel.finish()
        let (one, errorOne) = await first.value
        let (two, errorTwo) = await second.value
        let a = one.map { Int($0)! }
        let b = two.map { Int($0)! }
        // Every element reaches exactly one consumer, and each consumer sees its share in order.
        if errorOne != nil || errorTwo != nil || (a + b).sorted() != Array(1...6) || a != a.sorted() || b != b.sorted()
        {
          probe.violation("got \(one) and \(two)")
        }
      },
    ]
  }

  static var asyncChannelScenarios: [Scenario] {
    channelScenarios("AsyncChannel") {
      let channel = AsyncChannel<Int>()
      return ChannelUnderTest(
        channel: channel,
        send: { await channel.send($0) },
        finish: { channel.finish() },
        stream: { channel.map { "\($0)" } }
      )
    }
  }

  static var asyncThrowingChannelScenarios: [Scenario] {
    channelScenarios("AsyncThrowingChannel") {
      let channel = AsyncThrowingChannel<Int, any Error>()
      return ChannelUnderTest(
        channel: channel,
        send: { await channel.send($0) },
        finish: { channel.finish() },
        stream: { channel.map { "\($0)" } }
      )
    }
      + [
        Scenario(name: "AsyncThrowingChannel/fail-while-sender-and-consumer-waiting") { probe in
          let channel = AsyncThrowingChannel<Int, any Error>()
          probe.watch("channel", channel)
          let sender = Task { await channel.send(2) }
          let consumer = Task { await collect(channel.map { "\($0)" }) }
          channel.fail(ScenarioError())
          // The parked sender must be resumed, and the consumer must end with the failure (possibly
          // after element 2, if it was taken before the failure).
          await sender.value
          let (values, error) = await consumer.value
          if !(error is ScenarioError) || !(values.isEmpty || values == ["2"]) {
            probe.violation("expected the failure, got \(values) \(String(describing: error))")
          }
        }
      ]
  }

  // MARK: - Running

  enum Verdict: Equatable, CustomStringConvertible {
    case ok
    case leak([String])
    case hang
    case wrong(String)

    var kind: String {
      switch self {
      case .ok: return "ok"
      case .leak: return "leak"
      case .hang: return "hang"
      case .wrong: return "wrong"
      }
    }

    var description: String {
      switch self {
      case .ok: return "ok"
      case .leak(let names): return "leak \(names.joined(separator: ","))"
      case .hang: return "hang"
      case .wrong(let message): return "wrong: \(message)"
      }
    }
  }

  static func run(_ scenario: Scenario, _ strategy: Strategy) throws -> Verdict {
    let probe = Probe()
    let body = scenario.body
    let tail = String(repeating: "-", count: scenario.ticks - 1) + "|"
    let (_, failures) = try AsyncSequenceValidationDiagram.test(theme: .ascii, scheduling: strategy) { diagram in
      "a" + tail
      diagram.inputs[0].map { _ -> String in
        probe.clock = diagram.clock
        await body(probe)
        probe.finished = true
        return "x"
      }
      "x" + tail
    }
    // A scenario that uses the clock emits its "x" whenever it finishes, so the diagram's own
    // expectation can fail without anything being wrong; whether the body returned is what counts.
    _ = failures
    if !probe.finished {
      // The scenario's tasks are still parked. When the diagram ended it cancelled them, and any
      // clean-up now runs on the real executor; let it finish before the next diagram installs the
      // hook again.
      Thread.sleep(forTimeInterval: 0.02)
      return .hang
    }
    if let violation = probe.violations.first { return .wrong(violation) }
    let unreleased = probe.unreleased
    if !unreleased.isEmpty { return .leak(unreleased) }
    return .ok
  }

  static func environment(_ name: String) -> String? {
    getenv(name).map { String(cString: $0) }
  }

  func explore(_ scenarios: [Scenario]) throws {
    try XCTSkipUnless(
      Self.environment("ASYNC_VALIDATION_EXPLORE") == "1",
      "set ASYNC_VALIDATION_EXPLORE=1 to explore operator scenarios under seeded schedules"
    )
    let seedCount = Self.environment("ASYNC_VALIDATION_EXPLORE_SEEDS").flatMap { UInt64($0) } ?? 1000
    let seeds: [UInt64] =
      Self.environment("ASYNC_VALIDATION_EXPLORE_SEED").flatMap { UInt64($0) }.map { [$0] }
      ?? Array(1...seedCount)
    let only = Self.environment("ASYNC_VALIDATION_EXPLORE_ONLY")
    for scenario in scenarios where only.map({ scenario.name.contains($0) }) ?? true {
      let fifo = try Self.run(scenario, .fifo)
      var bad = [String: [UInt64]]()
      var examples = [String: String]()
      for seed in seeds {
        let verdict = try Self.run(scenario, .seeded(seed))
        if verdict != .ok {
          bad[verdict.kind, default: []].append(seed)
          if examples[verdict.kind] == nil { examples[verdict.kind] = "seed \(seed): \(verdict)" }
        }
      }
      let summary = bad.keys.sorted().map { kind in
        "\(kind) \(bad[kind]!.count)/\(seeds.count) [\(bad[kind]!.prefix(8).map(String.init).joined(separator: ","))] e.g. \(examples[kind]!)"
      }
      print(
        "EXPLORE \(scenario.name) | fifo \(fifo) | \(summary.isEmpty ? "all \(seeds.count) seeds ok" : summary.joined(separator: " | "))"
      )
      XCTAssertEqual(fifo, .ok, "\(scenario.name) under FIFO")
      XCTAssertTrue(bad.isEmpty, "\(scenario.name): \(summary.joined(separator: "; "))")
    }
  }

  func test_explore_zip() throws { try explore(Self.zipScenarios) }
  func test_explore_combineLatest() throws { try explore(Self.combineLatestScenarios) }
  func test_explore_merge() throws { try explore(Self.mergeScenarios) }
  func test_explore_flatMapLatest() throws { try explore(Self.flatMapLatestScenarios) }
  func test_explore_buffer() throws { try explore(Self.bufferScenarios) }
  func test_explore_zip3() throws { try explore(Self.zip3Scenarios) }
  func test_explore_combineLatest3() throws { try explore(Self.combineLatest3Scenarios) }
  func test_explore_merge3() throws { try explore(Self.merge3Scenarios) }
  func test_explore_debounce() throws { try explore(Self.debounceScenarios) }
  func test_explore_asyncChannel() throws { try explore(Self.asyncChannelScenarios) }
  func test_explore_asyncThrowingChannel() throws { try explore(Self.asyncThrowingChannelScenarios) }
}
#endif
