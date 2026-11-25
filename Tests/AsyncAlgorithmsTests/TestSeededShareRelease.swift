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

/// #473 "case 12" run on the validation scheduler: `share()` on a stored sequence, break out of
/// `for await` after the first element, then release the sequence. Before the #473 fix the
/// iteration task can park on its demand continuation after `cancel()` has already run, so it is
/// never resumed and keeps the base iterator alive. That only happens when the consumer's break and
/// release run between two critical sections of the iteration task, which FIFO never schedules.
///
/// Opt-in, because `main` does not have the fix yet and the check fails there: set
/// `ASYNC_VALIDATION_SHARE_CASE12=1`. Set `ASYNC_VALIDATION_SHARE_CASE12_SEEDS` to change how many
/// seeds are tried (default 1000).
@available(AsyncAlgorithms 1.1, *)
final class TestSeededShareRelease: XCTestCase {
  typealias Strategy = AsyncSequenceValidationDiagram.SchedulingStrategy

  final class ReleasedFlag: @unchecked Sendable {
    private let lock = NSLock()
    private var value = false
    var isSet: Bool {
      lock.lock()
      defer { lock.unlock() }
      return value
    }
    func set() {
      lock.lock()
      value = true
      lock.unlock()
    }
  }

  final class ReleaseTracker: Sendable {
    let released: ReleasedFlag
    init(_ released: ReleasedFlag) { self.released = released }
    deinit { released.set() }
  }

  /// Counts up forever; its iterator flags its own release.
  struct TrackedCounter: AsyncSequence, Sendable {
    typealias Element = Int
    typealias Failure = Never
    let released: ReleasedFlag
    struct AsyncIterator: AsyncIteratorProtocol {
      let tracker: ReleaseTracker
      var current = 0
      mutating func next() async -> Int? {
        if Task.isCancelled { return nil }
        current += 1
        return current
      }
    }
    func makeAsyncIterator() -> AsyncIterator { AsyncIterator(tracker: ReleaseTracker(released)) }
  }

  /// Case 12 itself: returns "x" once the consumer has broken out and released the sequence.
  static func storedShareReleasedAfterBreak(
    released: ReleasedFlag,
    bufferingPolicy: AsyncBufferSequencePolicy
  ) async -> String {
    var shared = Optional(TrackedCounter(released: released).share(bufferingPolicy: bufferingPolicy))
    var first: Int?
    for await value in shared! {
      first = value
      break
    }
    shared = nil
    return first == 1 ? "x" : "?"
  }

  /// Runs case 12 once and says whether the base iterator was released by the time the scheduler
  /// had nothing left to run.
  static func baseIteratorReleased(
    _ strategy: Strategy,
    bufferingPolicy: AsyncBufferSequencePolicy
  ) throws -> Bool {
    let released = ReleasedFlag()
    let (result, failures) = try AsyncSequenceValidationDiagram.test(theme: .ascii, scheduling: strategy) {
      "a--|"
      $0.inputs[0].map { _ in
        await storedShareReleasedAfterBreak(released: released, bufferingPolicy: bufferingPolicy)
      }
      "x--|"
    }
    precondition(failures.isEmpty, "\(failures) \(result.reconstituteActual(theme: .ascii))")
    return released.isSet
  }

  static func leakingSeeds(bufferingPolicy: AsyncBufferSequencePolicy, seeds: ClosedRange<UInt64>) throws -> [UInt64] {
    try seeds.filter { try !baseIteratorReleased(.seeded($0), bufferingPolicy: bufferingPolicy) }
  }

  var seedCount: UInt64 {
    getenv("ASYNC_VALIDATION_SHARE_CASE12_SEEDS").flatMap { UInt64(String(cString: $0)) } ?? 1000
  }

  func skipUnlessOptedIn() throws {
    try XCTSkipUnless(
      getenv("ASYNC_VALIDATION_SHARE_CASE12").map { String(cString: $0) } == "1",
      "set ASYNC_VALIDATION_SHARE_CASE12=1 to run #473 case 12 under seeded schedules"
    )
  }

  func check(_ bufferingPolicy: AsyncBufferSequencePolicy, _ label: String) throws {
    let fifo = try Self.baseIteratorReleased(.fifo, bufferingPolicy: bufferingPolicy)
    let leaks = try Self.leakingSeeds(bufferingPolicy: bufferingPolicy, seeds: 1...seedCount)
    print(
      "case12 \(label): fifo \(fifo ? "released" : "LEAKED"), \(leaks.count) of \(seedCount) seeds leaked: \(leaks.prefix(20))"
    )
    XCTAssertTrue(fifo, "FIFO leaked the base iterator")
    XCTAssertEqual(leaks, [], "seeds that leaked the base iterator")
  }

  func test_case12_bounded1_never_leaks_under_any_seed() throws {
    try skipUnlessOptedIn()
    try check(.bounded(1), "bounded(1)")
  }

  func test_case12_unbounded_never_leaks_under_any_seed() throws {
    try skipUnlessOptedIn()
    try check(.unbounded, "unbounded")
  }
}
#endif
