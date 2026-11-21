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

#if compiler(>=6.2)

import XCTest
import AsyncAlgorithms
import Synchronization

@available(macOS 15.0, *)
final class TestShare: XCTestCase {

  // MARK: - Basic Functionality Tests

  func test_share_delivers_elements_to_multiple_consumers() async {
    let source = [1, 2, 3, 4, 5]
    let shared = source.async.share()
    let gate1 = Gate()
    let gate2 = Gate()

    async let consumer1 = Task.detached {
      var results = [Int]()
      var iterator = shared.makeAsyncIterator()
      gate1.open()
      await gate2.enter()
      while let value = await iterator.next(isolation: nil) {
        results.append(value)
      }
      return results
    }

    async let consumer2 = Task.detached {
      var results = [Int]()
      var iterator = shared.makeAsyncIterator()
      gate2.open()
      await gate1.enter()
      while let value = await iterator.next(isolation: nil) {
        results.append(value)
      }
      return results
    }
    let results1 = await consumer1.value
    let results2 = await consumer2.value

    XCTAssertEqual(results1, [1, 2, 3, 4, 5])
    XCTAssertEqual(results2, [1, 2, 3, 4, 5])
  }

  func test_share_with_single_consumer() async {
    let source = [1, 2, 3, 4, 5]
    let shared = source.async.share()

    var results = [Int]()
    for await value in shared {
      results.append(value)
    }

    XCTAssertEqual(results, [1, 2, 3, 4, 5])
  }

  func test_share_with_empty_source() async {
    let source = [Int]()
    let shared = source.async.share()

    var results = [Int]()
    for await value in shared {
      results.append(value)
    }

    XCTAssertEqual(results, [])
  }

  // MARK: - Buffering Policy Tests

  func test_share_with_bounded_buffering() async {
    var gated = GatedSequence([1, 2, 3, 4, 5])
    let shared = gated.share(bufferingPolicy: .bounded(2))

    let results1 = Mutex([Int]())
    let results2 = Mutex([Int]())
    let gate1 = Gate()
    let gate2 = Gate()

    let consumer1 = Task {
      var iterator = shared.makeAsyncIterator()
      gate1.open()
      await gate2.enter()
      // Consumer 1 reads first element
      if let value = await iterator.next(isolation: nil) {
        results1.withLock { $0.append(value) }
      }
      // Delay to allow consumer 2 to get ahead
      try? await Task.sleep(for: .milliseconds(10))
      // Continue reading
      while let value = await iterator.next(isolation: nil) {
        results1.withLock { $0.append(value) }
      }
    }

    let consumer2 = Task {
      var iterator = shared.makeAsyncIterator()
      gate2.open()
      await gate1.enter()
      // Consumer 2 reads all elements quickly
      while let value = await iterator.next(isolation: nil) {
        results2.withLock { $0.append(value) }
      }
    }

    // Advance the gated sequence to make elements available
    gated.advance()  // 1
    gated.advance()  // 2
    gated.advance()  // 3
    gated.advance()  // 4
    gated.advance()  // 5

    await consumer1.value
    await consumer2.value

    // Both consumers should receive all elements
    XCTAssertEqual(results1.withLock { $0 }.sorted(), [1, 2, 3, 4, 5])
    XCTAssertEqual(results2.withLock { $0 }.sorted(), [1, 2, 3, 4, 5])
  }

  func test_share_with_no_buffering() async {
    let shared = [1, 2, 3, 4, 5].async.share(bufferingPolicy: .bounded(0))

    let expectation1 = XCTestExpectation(description: "Consumer 1 finished looping")
    let expectation2 = XCTestExpectation(description: "Consumer 2 finished looping")

    let results1 = Mutex([Int]())
    let results2 = Mutex([Int]())
    let gate1 = Gate()
    let gate2 = Gate()

    let consumer1 = Task {
      var iterator = shared.makeAsyncIterator()
      gate2.open()
      await gate1.enter()
      while let value = await iterator.next(isolation: nil) {
        results1.withLock { $0.append(value) }
        // Add some delay to consumer 1
        try? await Task.sleep(for: .milliseconds(1))
      }
      expectation1.fulfill()
    }

    let consumer2 = Task {
      var iterator = shared.makeAsyncIterator()
      gate1.open()
      await gate2.enter()
      while let value = await iterator.next(isolation: nil) {
        results2.withLock { $0.append(value) }
      }
      expectation2.fulfill()
    }

    await fulfillment(of: [expectation1, expectation2], timeout: 5)

    await consumer1.value
    await consumer2.value

    // Both consumers should receive all elements
    XCTAssertEqual(results1.withLock { $0 }.sorted(), [1, 2, 3, 4, 5])
    XCTAssertEqual(results2.withLock { $0 }.sorted(), [1, 2, 3, 4, 5])
  }

  func test_share_with_no_buffering_multiple() async {
    for _ in 0..<10 {
      await test_share_with_no_buffering()
    }
  }

  func test_share_with_unbounded_buffering() async {
    let source = [1, 2, 3, 4, 5]
    let shared = source.async.share(bufferingPolicy: .unbounded)

    let results1 = Mutex([Int]())
    let results2 = Mutex([Int]())
    let gate1 = Gate()
    let gate2 = Gate()

    let consumer1 = Task {
      var iterator = shared.makeAsyncIterator()
      gate2.open()
      await gate1.enter()
      while let value = await iterator.next(isolation: nil) {
        results1.withLock { $0.append(value) }
        // Add some delay to consumer 1
        try? await Task.sleep(for: .milliseconds(1))
      }
    }

    let consumer2 = Task {
      var iterator = shared.makeAsyncIterator()
      gate1.open()
      await gate2.enter()
      while let value = await iterator.next(isolation: nil) {
        results2.withLock { $0.append(value) }
      }
    }

    await consumer1.value
    await consumer2.value

    XCTAssertEqual(results1.withLock { $0 }, [1, 2, 3, 4, 5])
    XCTAssertEqual(results2.withLock { $0 }, [1, 2, 3, 4, 5])
  }

  func test_share_with_bufferingLatest_buffering() async {
    var gated = GatedSequence([1, 2, 3, 4, 5])
    let shared = gated.share(bufferingPolicy: .bufferingLatest(2))

    let fastResults = Mutex([Int]())
    let slowResults = Mutex([Int]())
    let gate1 = Gate()
    let gate2 = Gate()

    let fastConsumer = Task.detached {
      var iterator = shared.makeAsyncIterator()
      gate2.open()
      await gate1.enter()
      while let value = await iterator.next(isolation: nil) {
        fastResults.withLock { $0.append(value) }
      }
    }

    let slowConsumer = Task.detached {
      var iterator = shared.makeAsyncIterator()
      gate1.open()
      await gate2.enter()
      // Read first element immediately
      if let value = await iterator.next(isolation: nil) {
        slowResults.withLock { $0.append(value) }
      }
      // Add significant delay to let buffer fill up and potentially overflow
      try? await Task.sleep(for: .milliseconds(50))
      // Continue reading remaining elements
      while let value = await iterator.next(isolation: nil) {
        slowResults.withLock { $0.append(value) }
      }
    }

    // Release all elements quickly to test buffer overflow behavior
    gated.advance()  // 1
    try? await Task.sleep(for: .milliseconds(5))
    gated.advance()  // 2
    try? await Task.sleep(for: .milliseconds(5))
    gated.advance()  // 3
    try? await Task.sleep(for: .milliseconds(5))
    gated.advance()  // 4
    try? await Task.sleep(for: .milliseconds(5))
    gated.advance()  // 5

    await fastConsumer.value
    await slowConsumer.value

    let slowResultsArray = slowResults.withLock { $0 }

    // Slow consumer should get the first element plus the latest elements in buffer
    // With bufferingLatest(2), when buffer overflows, older elements are discarded
    XCTAssertTrue(slowResultsArray.count >= 1, "Should have at least the first element")
    XCTAssertEqual(slowResultsArray.first, 1, "Should start with first element")

    // Due to bufferingLatest policy, the slow consumer should favor newer elements
    // It may miss some middle elements but should get the latest ones
    let receivedSet = Set(slowResultsArray)
    XCTAssertTrue(receivedSet.isSubset(of: Set([1, 2, 3, 4, 5])))

    // With bufferingLatest, we expect the slow consumer to get newer elements
    // when it finally catches up after the delay
    if slowResultsArray.count > 1 {
      let laterElements = Set(slowResultsArray.dropFirst())
      // Should have received some of the later elements (4, 5) due to bufferingLatest
      XCTAssertTrue(
        laterElements.contains(4) || laterElements.contains(5) || laterElements.contains(3),
        "BufferingLatest should favor keeping newer elements"
      )
    }
  }

  func test_share_with_bufferingOldest_buffering() async {
    var gated = GatedSequence([1, 2, 3, 4, 5])
    let shared = gated.share(bufferingPolicy: .bufferingOldest(2))

    let fastResults = Mutex([Int]())
    let slowResults = Mutex([Int]())
    let gate1 = Gate()
    let gate2 = Gate()

    let fastConsumer = Task {
      var iterator = shared.makeAsyncIterator()
      gate2.open()
      await gate1.enter()
      while let value = await iterator.next(isolation: nil) {
        fastResults.withLock { $0.append(value) }
      }
    }

    let slowConsumer = Task {
      var iterator = shared.makeAsyncIterator()
      gate1.open()
      await gate2.enter()
      // Read first element immediately
      if let value = await iterator.next(isolation: nil) {
        slowResults.withLock { $0.append(value) }
      }
      // Add significant delay to let buffer fill up and potentially overflow
      try? await Task.sleep(for: .milliseconds(50))
      // Continue reading remaining elements
      while let value = await iterator.next(isolation: nil) {
        slowResults.withLock { $0.append(value) }
      }
    }

    // Release all elements quickly to test buffer overflow behavior
    gated.advance()  // 1
    try? await Task.sleep(for: .milliseconds(5))
    gated.advance()  // 2
    try? await Task.sleep(for: .milliseconds(5))
    gated.advance()  // 3
    try? await Task.sleep(for: .milliseconds(5))
    gated.advance()  // 4
    try? await Task.sleep(for: .milliseconds(5))
    gated.advance()  // 5

    await fastConsumer.value
    await slowConsumer.value

    let slowResultsArray = slowResults.withLock { $0 }

    // Slow consumer should get the first element plus the oldest elements that fit in buffer
    // With bufferingOldest(2), when buffer overflows, newer elements are ignored
    XCTAssertTrue(slowResultsArray.count >= 1, "Should have at least the first element")
    XCTAssertEqual(slowResultsArray.first, 1, "Should start with first element")

    // Due to bufferingOldest policy, the slow consumer should favor older elements
    let receivedSet = Set(slowResultsArray)
    XCTAssertTrue(receivedSet.isSubset(of: Set([1, 2, 3, 4, 5])))

    // With bufferingOldest, when the buffer is full, newer elements are ignored
    // So the slow consumer should be more likely to receive earlier elements
    if slowResultsArray.count > 1 {
      let laterElements = Array(slowResultsArray.dropFirst())
      // Should have received earlier elements due to bufferingOldest policy
      // Elements 4 and 5 are less likely to be received since they're newer
      let hasEarlierElements = laterElements.contains(2) || laterElements.contains(3)
      let hasLaterElements = laterElements.contains(4) && laterElements.contains(5)

      // BufferingOldest should favor keeping older elements when buffer is full
      // So we should be more likely to see earlier elements than later ones
      XCTAssertTrue(
        hasEarlierElements || !hasLaterElements,
        "BufferingOldest should favor keeping older elements over newer ones"
      )
    }
  }

  // MARK: - Cancellation Tests

  func test_share_cancellation_of_single_consumer() async {
    let shared = Indefinite(value: 42).async.share()

    let finished = expectation(description: "finished")
    let iterated = expectation(description: "iterated")

    let task = Task {
      var firstIteration = false
      for await _ in shared {
        if !firstIteration {
          firstIteration = true
          iterated.fulfill()
        }
      }
      finished.fulfill()
    }

    // Wait for the task to start iterating
    await fulfillment(of: [iterated], timeout: 1.0)

    // Cancel the task
    task.cancel()

    // Verify the task finishes
    await fulfillment(of: [finished], timeout: 1.0)
  }

  func test_share_cancellation_with_multiple_consumers() async {
    let shared = Indefinite(value: 42).async.share()

    let consumer1Finished = expectation(description: "consumer1Finished")
    let consumer2Finished = expectation(description: "consumer2Finished")
    let consumer1Iterated = expectation(description: "consumer1Iterated")
    let consumer2Iterated = expectation(description: "consumer2Iterated")

    let consumer1 = Task {
      var firstIteration = false
      for await _ in shared {
        if !firstIteration {
          firstIteration = true
          consumer1Iterated.fulfill()
        }
      }
      consumer1Finished.fulfill()
    }

    let consumer2 = Task {
      var firstIteration = false
      for await _ in shared {
        if !firstIteration {
          firstIteration = true
          consumer2Iterated.fulfill()
        }
      }
      consumer2Finished.fulfill()
    }

    // Wait for both consumers to start
    await fulfillment(of: [consumer1Iterated, consumer2Iterated], timeout: 1.0)

    // Cancel only consumer1
    consumer1.cancel()

    // Consumer1 should finish
    await fulfillment(of: [consumer1Finished], timeout: 1.0)

    // Consumer2 should still be running, so cancel it too
    consumer2.cancel()
    await fulfillment(of: [consumer2Finished], timeout: 1.0)
  }

  func test_share_cancellation_cancels_source_when_no_consumers() async {
    let source = Indefinite(value: 1).async
    let shared = source.share()

    let finished = expectation(description: "finished")
    let iterated = expectation(description: "iterated")

    let task = Task {
      var iterator = shared.makeAsyncIterator()
      if await iterator.next(isolation: nil) != nil {
        iterated.fulfill()
      }
      // Task will be cancelled here, so iteration should stop
      while await iterator.next(isolation: nil) != nil {
        // Continue iterating until cancelled
      }
      finished.fulfill()
    }

    await fulfillment(of: [iterated], timeout: 1.0)
    task.cancel()
    await fulfillment(of: [finished], timeout: 1.0)
  }

  // MARK: - Error Handling Tests

  func test_share_propagates_errors_to_all_consumers() async {
    let source = [1, 2, 3, 4, 5].async.map { value in
      if value == 3 {
        throw TestError.failure
      }
      return value
    }
    let shared = source.share()

    let consumer1Results = Mutex([Int]())
    let consumer2Results = Mutex([Int]())
    let consumer1Error = Mutex<Error?>(nil)
    let consumer2Error = Mutex<Error?>(nil)
    let gate1 = Gate()
    let gate2 = Gate()

    let consumer1 = Task {
      do {
        var iterator = shared.makeAsyncIterator()
        gate2.open()
        await gate1.enter()
        while let value = try await iterator.next() {
          consumer1Results.withLock { $0.append(value) }
        }
      } catch {
        consumer1Error.withLock { $0 = error }
      }
    }

    let consumer2 = Task {
      do {
        var iterator = shared.makeAsyncIterator()
        gate1.open()
        await gate2.enter()
        while let value = try await iterator.next() {
          consumer2Results.withLock { $0.append(value) }
        }
      } catch {
        consumer2Error.withLock { $0 = error }
      }
    }

    await consumer1.value
    await consumer2.value

    // Both consumers should receive the first two elements
    XCTAssertEqual(consumer1Results.withLock { $0 }, [1, 2])
    XCTAssertEqual(consumer2Results.withLock { $0 }, [1, 2])

    // Both consumers should receive the error
    XCTAssertTrue(consumer1Error.withLock { $0 is TestError })
    XCTAssertTrue(consumer2Error.withLock { $0 is TestError })
  }

  // MARK: - Timing and Race Condition Tests

  func test_share_with_late_joining_consumer() async {
    var gated = GatedSequence([1, 2, 3, 4, 5])
    let shared = gated.share(bufferingPolicy: .unbounded)

    let earlyResults = Mutex([Int]())
    let lateResults = Mutex([Int]())

    // Start early consumer
    let earlyConsumer = Task {
      var iterator = shared.makeAsyncIterator()
      while let value = await iterator.next(isolation: nil) {
        earlyResults.withLock { $0.append(value) }
      }
    }

    // Advance some elements
    gated.advance()  // 1
    gated.advance()  // 2

    // Give early consumer time to consume
    try? await Task.sleep(for: .milliseconds(10))

    // Start late consumer
    let lateConsumer = Task {
      var iterator = shared.makeAsyncIterator()
      while let value = await iterator.next(isolation: nil) {
        lateResults.withLock { $0.append(value) }
      }
    }

    // Advance remaining elements
    gated.advance()  // 3
    gated.advance()  // 4
    gated.advance()  // 5

    await earlyConsumer.value
    await lateConsumer.value

    // Early consumer gets all elements
    XCTAssertEqual(earlyResults.withLock { $0 }, [1, 2, 3, 4, 5])
    // Late consumer only gets elements from when it joined
    XCTAssertTrue(lateResults.withLock { $0.count <= 5 })
  }

  func test_share_iterator_independence() async {
    let source = [1, 2, 3, 4, 5]
    let shared = source.async.share()

    var iterator1 = shared.makeAsyncIterator()
    var iterator2 = shared.makeAsyncIterator()

    // Both iterators should independently get the same elements
    let value1a = await iterator1.next(isolation: nil)
    let value2a = await iterator2.next(isolation: nil)

    let value1b = await iterator1.next(isolation: nil)
    let value2b = await iterator2.next(isolation: nil)

    XCTAssertEqual(value1a, 1)
    XCTAssertEqual(value2a, 1)
    XCTAssertEqual(value1b, 2)
    XCTAssertEqual(value2b, 2)
  }

  // MARK: - Memory and Resource Management Tests

  func test_share_cleans_up_when_all_consumers_finish() async {
    let source = [1, 2, 3]
    let shared = source.async.share()

    var results = [Int]()
    for await value in shared {
      results.append(value)
    }

    XCTAssertEqual(results, [1, 2, 3])

    // Create a new iterator after the sequence finished
    var newIterator = shared.makeAsyncIterator()
    let value = await newIterator.next(isolation: nil)
    XCTAssertNil(value)  // Should return nil since source is exhausted
  }

  func test_share_multiple_sequential_consumers() async {
    let source = [1, 2, 3, 4, 5]
    let shared = source.async.share(bufferingPolicy: .unbounded)

    // First consumer
    var results1 = [Int]()
    for await value in shared {
      results1.append(value)
    }

    // Second consumer (starting after first finished)
    var results2 = [Int]()
    for await value in shared {
      results2.append(value)
    }

    XCTAssertEqual(results1, [1, 2, 3, 4, 5])
    XCTAssertEqual(results2, [])  // Should be empty since source is exhausted
  }

  // MARK: - Resource Release Tests

  // Inline iteration that breaks: the sequence is released before its only iterator (bounded(1)).
  func test_share_releases_base_iterator_after_inline_iteration() async {
    let released = expectation(description: "base iterator released")
    for await _ in TrackedCounter(onIteratorRelease: { released.fulfill() }).share() {
      break
    }
    await fulfillment(of: [released], timeout: 2)
  }

  // Inline iteration that breaks, with an unbounded buffer: the sequence is released before its
  // only iterator.
  func test_share_releases_base_iterator_after_inline_iteration_unbounded() async {
    let released = expectation(description: "base iterator released")
    let source = TrackedCounter(onIteratorRelease: { released.fulfill() })
    for await _ in source.share(bufferingPolicy: .unbounded) {
      break
    }
    await fulfillment(of: [released], timeout: 2)
  }

  // Inline iteration of a finite base run to the end: the sequence is released before its only
  // iterator.
  func test_share_releases_base_iterator_after_inline_iteration_to_completion() async {
    let released = expectation(description: "base iterator released")
    var results = [Int]()
    for await value in TrackedElements([1, 2, 3], onIteratorRelease: { released.fulfill() }).share() {
      results.append(value)
    }
    XCTAssertEqual(results, [1, 2, 3])
    await fulfillment(of: [released], timeout: 2)
  }

  // Inline iteration of a throwing base that breaks before the base fails: the sequence is
  // released before its only iterator.
  func test_share_releases_base_iterator_after_inline_iteration_of_throwing_base() async {
    let released = expectation(description: "base iterator released")
    let source = TrackedFailingElements([1, 2], onIteratorRelease: { released.fulfill() })
    do {
      for try await value in source.share() {
        XCTAssertEqual(value, 1)
        break
      }
    } catch {
      XCTFail("unexpected error \(error)")
    }
    await fulfillment(of: [released], timeout: 2)
  }

  // One iterator takes an element while a second never calls next(), so the bounded buffer is full.
  // Both iterators are released, then the sequence, while the iteration is parked on the full buffer.
  func test_share_releases_base_iterator_when_sequence_released_last_with_full_buffer() async {
    let released = expectation(description: "base iterator released")
    var shared = Optional(TrackedCounter(onIteratorRelease: { released.fulfill() }).share())
    var consumer = Optional(shared!.makeAsyncIterator())
    var idle = Optional(shared!.makeAsyncIterator())
    let first = await consumer!.next(isolation: nil)
    XCTAssertEqual(first, 1)
    XCTAssertNotNil(idle)
    consumer = nil
    idle = nil
    shared = nil
    await fulfillment(of: [released], timeout: 2)
  }

  // Same as above, but the idle iterator is released first, which wakes the iteration from the full
  // buffer; the other iterator and then the sequence are released straight after.
  func test_share_releases_base_iterator_when_idle_iterator_released_first() async {
    let released = expectation(description: "base iterator released")
    var shared = Optional(TrackedCounter(onIteratorRelease: { released.fulfill() }).share())
    var consumer = Optional(shared!.makeAsyncIterator())
    var idle = Optional(shared!.makeAsyncIterator())
    let first = await consumer!.next(isolation: nil)
    XCTAssertEqual(first, 1)
    XCTAssertNotNil(idle)
    idle = nil
    consumer = nil
    shared = nil
    await fulfillment(of: [released], timeout: 2)
  }

  // The only consumer is cancelled while a cancellation-ignoring base is inside next(); the sequence
  // is then released, and only after that does the base produce its element (bounded(1)).
  func test_share_releases_base_iterator_after_consumer_cancelled_during_base_next() async {
    await assertReleasesAfterConsumerCancelledDuringBaseNext(bufferingPolicy: .bounded(1))
  }

  // Same as above, with an unbounded buffer.
  func test_share_releases_base_iterator_after_consumer_cancelled_during_base_next_unbounded() async {
    await assertReleasesAfterConsumerCancelledDuringBaseNext(bufferingPolicy: .unbounded)
  }

  // The sequence is released while its only consumer is suspended in next(), then the consumer task
  // is cancelled; the base honours cancellation (unbounded).
  func test_share_releases_base_iterator_when_consumer_cancelled_after_sequence_released_unbounded() async {
    await assertReleasesWhenConsumerCancelledAfterSequenceReleased(
      bufferingPolicy: .unbounded,
      honoursCancellation: true
    )
  }

  // Same as above, but with a cancellation-ignoring base and a bounded(1) buffer.
  func test_share_releases_base_iterator_when_consumer_cancelled_after_sequence_released_ignoring_cancellation()
    async
  {
    await assertReleasesWhenConsumerCancelledAfterSequenceReleased(
      bufferingPolicy: .bounded(1),
      honoursCancellation: false
    )
  }

  // Same as above, but with a base that honours cancellation and a bounded(1) buffer.
  func test_share_releases_base_iterator_when_consumer_cancelled_after_sequence_released_honouring_cancellation()
    async
  {
    await assertReleasesWhenConsumerCancelledAfterSequenceReleased(
      bufferingPolicy: .bounded(1),
      honoursCancellation: true
    )
  }

  // A stored sequence is iterated until a break, then released after its iterator (bounded(1)).
  func test_share_releases_base_iterator_when_stored_sequence_released_after_break() async {
    await assertReleasesWhenStoredSequenceReleasedAfterBreak(bufferingPolicy: .bounded(1))
  }

  // Same as above, with an unbounded buffer.
  func test_share_releases_base_iterator_when_stored_sequence_released_after_break_unbounded() async {
    await assertReleasesWhenStoredSequenceReleasedAfterBreak(bufferingPolicy: .unbounded)
  }

  // Inline iteration of a throwing base that fails on its first next().
  func test_share_releases_base_iterator_when_throwing_base_fails_immediately() async {
    let released = expectation(description: "base iterator released")
    let source = TrackedFailingElements([], onIteratorRelease: { released.fulfill() })
    do {
      for try await _ in source.share() {
        XCTFail("unexpected element")
      }
      XCTFail("expected the base to fail")
    } catch {
      XCTAssertEqual(error as? TestError, .failure)
    }
    await fulfillment(of: [released], timeout: 2)
  }

  // Inline iteration of a throwing base that fails after two elements.
  func test_share_releases_base_iterator_when_throwing_base_fails_after_elements() async {
    let released = expectation(description: "base iterator released")
    let source = TrackedFailingElements([1, 2], onIteratorRelease: { released.fulfill() })
    var results = [Int]()
    do {
      for try await value in source.share() {
        results.append(value)
      }
      XCTFail("expected the base to fail")
    } catch {
      XCTAssertEqual(error as? TestError, .failure)
    }
    XCTAssertEqual(results, [1, 2])
    await fulfillment(of: [released], timeout: 2)
  }

  // MARK: - Resource Release Helpers

  private func assertReleasesAfterConsumerCancelledDuringBaseNext(
    bufferingPolicy: AsyncBufferSequencePolicy
  ) async {
    let released = expectation(description: "base iterator released")
    let gate = Gate()
    let (entered, enteredContinuation) = AsyncStream<Void>.makeStream()
    let source = GatedTrackedSource(
      honoursCancellation: false,
      gate: gate,
      entered: { enteredContinuation.yield() },
      onIteratorRelease: { released.fulfill() }
    )
    var shared = Optional(source.share(bufferingPolicy: bufferingPolicy))
    let iterator = shared!.makeAsyncIterator()
    let consumer = Task.detached {
      var iterator = iterator
      return await iterator.next(isolation: nil)
    }
    var enteredIterator = entered.makeAsyncIterator()
    // The iteration is now inside the base's next(), which ignores cancellation.
    await enteredIterator.next()
    consumer.cancel()
    let value = await consumer.value
    XCTAssertNil(value)
    shared = nil
    // Let the base produce an element after the iteration has been cancelled.
    gate.open()
    await fulfillment(of: [released], timeout: 2)
  }

  private func assertReleasesWhenConsumerCancelledAfterSequenceReleased(
    bufferingPolicy: AsyncBufferSequencePolicy,
    honoursCancellation: Bool
  ) async {
    let released = expectation(description: "base iterator released")
    let gate = Gate()
    let (entered, enteredContinuation) = AsyncStream<Void>.makeStream()
    let source = GatedTrackedSource(
      honoursCancellation: honoursCancellation,
      gate: gate,
      entered: { enteredContinuation.yield() },
      onIteratorRelease: { released.fulfill() }
    )
    var shared = Optional(source.share(bufferingPolicy: bufferingPolicy))
    let iterator = shared!.makeAsyncIterator()
    let consumer = Task.detached {
      var iterator = iterator
      return await iterator.next(isolation: nil)
    }
    var enteredIterator = entered.makeAsyncIterator()
    // The iteration is inside the base's next(), so the consumer is suspended in next().
    await enteredIterator.next()
    shared = nil
    consumer.cancel()
    let value = await consumer.value
    XCTAssertNil(value)
    if !honoursCancellation {
      // The base ignored the cancellation; let it produce an element now.
      gate.open()
    }
    await fulfillment(of: [released], timeout: 2)
  }

  private func assertReleasesWhenStoredSequenceReleasedAfterBreak(
    bufferingPolicy: AsyncBufferSequencePolicy
  ) async {
    let released = expectation(description: "base iterator released")
    var shared = Optional(
      TrackedCounter(onIteratorRelease: { released.fulfill() }).share(bufferingPolicy: bufferingPolicy)
    )
    for await value in shared! {
      XCTAssertEqual(value, 1)
      break
    }
    shared = nil
    await fulfillment(of: [released], timeout: 2)
  }
}

// MARK: - Helper Types

private enum TestError: Error, Equatable {
  case failure
}

private final class ReleaseTracker: Sendable {
  let onRelease: @Sendable () -> Void
  init(_ onRelease: @escaping @Sendable () -> Void) { self.onRelease = onRelease }
  deinit { onRelease() }
}

/// Counts up from 1 until its task is cancelled. Its iterator calls
/// `onIteratorRelease` when it is deallocated.
private struct TrackedCounter: AsyncSequence, Sendable {
  let onIteratorRelease: @Sendable () -> Void

  struct AsyncIterator: AsyncIteratorProtocol {
    let tracker: ReleaseTracker
    var current = 0

    mutating func next() async -> Int? {
      if Task.isCancelled { return nil }
      current += 1
      return current
    }
  }

  func makeAsyncIterator() -> AsyncIterator {
    AsyncIterator(tracker: ReleaseTracker(onIteratorRelease))
  }
}

/// Yields `elements` and then finishes. Its iterator calls `onIteratorRelease`
/// when it is deallocated.
private struct TrackedElements: AsyncSequence, Sendable {
  let elements: [Int]
  let onIteratorRelease: @Sendable () -> Void

  init(_ elements: [Int], onIteratorRelease: @escaping @Sendable () -> Void) {
    self.elements = elements
    self.onIteratorRelease = onIteratorRelease
  }

  struct AsyncIterator: AsyncIteratorProtocol {
    let tracker: ReleaseTracker
    var remaining: ArraySlice<Int>

    mutating func next() async -> Int? {
      remaining.popFirst()
    }
  }

  func makeAsyncIterator() -> AsyncIterator {
    AsyncIterator(tracker: ReleaseTracker(onIteratorRelease), remaining: elements[...])
  }
}

/// Yields `elements` and then throws `TestError.failure`. Its iterator calls
/// `onIteratorRelease` when it is deallocated.
private struct TrackedFailingElements: AsyncSequence, Sendable {
  let elements: [Int]
  let onIteratorRelease: @Sendable () -> Void

  init(_ elements: [Int], onIteratorRelease: @escaping @Sendable () -> Void) {
    self.elements = elements
    self.onIteratorRelease = onIteratorRelease
  }

  struct AsyncIterator: AsyncIteratorProtocol {
    let tracker: ReleaseTracker
    var remaining: ArraySlice<Int>

    mutating func next() async throws -> Int? {
      guard let element = remaining.popFirst() else {
        throw TestError.failure
      }
      return element
    }
  }

  func makeAsyncIterator() -> AsyncIterator {
    AsyncIterator(tracker: ReleaseTracker(onIteratorRelease), remaining: elements[...])
  }
}

/// Counts up from 1, but each call to `next()` first calls `entered` and then waits
/// for `gate` to open. When `honoursCancellation` is true, cancelling the iterating
/// task ends the wait and `next()` returns nil; otherwise the wait ignores
/// cancellation. Its iterator calls `onIteratorRelease` when it is deallocated.
private struct GatedTrackedSource: AsyncSequence, Sendable {
  let honoursCancellation: Bool
  let gate: Gate
  let entered: @Sendable () -> Void
  let onIteratorRelease: @Sendable () -> Void

  struct AsyncIterator: AsyncIteratorProtocol {
    let tracker: ReleaseTracker
    let honoursCancellation: Bool
    let gate: Gate
    let entered: @Sendable () -> Void
    var current = 0

    mutating func next() async -> Int? {
      entered()
      if honoursCancellation {
        await withTaskCancellationHandler {
          await gate.enter()
        } onCancel: { [gate] in
          gate.open()
        }
        if Task.isCancelled { return nil }
      } else {
        await gate.enter()
      }
      current += 1
      return current
    }
  }

  func makeAsyncIterator() -> AsyncIterator {
    AsyncIterator(
      tracker: ReleaseTracker(onIteratorRelease),
      honoursCancellation: honoursCancellation,
      gate: gate,
      entered: entered
    )
  }
}

#endif
