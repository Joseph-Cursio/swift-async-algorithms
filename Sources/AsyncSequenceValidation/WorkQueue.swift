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

@available(AsyncAlgorithms 1.0, *)
struct WorkQueue: Sendable {
  enum Item: CustomStringConvertible, Comparable {
    case blocked(Token, AsyncSequenceValidationDiagram.Clock.Instant, UnsafeContinuation<Void, Error>)
    case emit(
      Token,
      AsyncSequenceValidationDiagram.Clock.Instant,
      UnsafeContinuation<String?, Error>,
      Result<String?, Error>,
      Int
    )
    case work(Token, @Sendable () -> Void)
    case cancelled(Token)

    func run() {
      switch self {
      case .blocked(_, _, let continuation):
        continuation.resume()
      case .emit(_, _, let continuation, let result, _):
        continuation.resume(with: result)
      case .work(_, let work):
        work()
      case .cancelled:
        break
      }
    }

    var description: String {
      switch self {
      case .blocked(let token, let when, _):
        return "wakeup #\(token) @\(when) "
      case .emit(let token, let when, _, let result, let side):
        return "emit #\(token) @\(when) result \(result) side \(side)"
      case .work(let token, _):
        return "work #\(token)"
      case .cancelled(let token):
        return "cancelled #\(token)"
      }
    }

    var token: Token {
      switch self {
      case .blocked(let token, _, _): return token
      case .emit(let token, _, _, _, _): return token
      case .work(let token, _): return token
      case .cancelled(let token): return token
      }
    }

    var isCancelled: Bool {
      switch self {
      case .cancelled: return true
      default: return false
      }
    }

    func cancelling() -> Item {
      switch self {
      case .blocked(let token, _, let continuation):
        return .work(token) {
          continuation.resume(throwing: CancellationError())
        }
      case .emit(let token, _, let continuation, _, _):
        return .work(token) {
          continuation.resume(returning: nil)
        }
      default: return self
      }
    }

    // the side order is repsected first since that is the logical flow of predictable events
    // then the generation is taken into account
    static func < (_ lhs: Item, _ rhs: Item) -> Bool {
      switch (lhs, rhs) {
      case (.emit(_, _, _, _, let lhs), .emit(_, _, _, _, let rhs)):
        return lhs < rhs
      default:
        return lhs.token.generation < rhs.token.generation
      }
    }

    // all tokens are distinct so we know the generation of when it was enqueued
    // always means distinct equality (for ordering)
    static func == (_ lhs: Item, _ rhs: Item) -> Bool {
      return lhs.token == rhs.token
    }
  }

  struct State {
    // the nil Job in these two structures represent the root job in the TaskDriver
    var queues = [Job?: [Item]]()
    var jobs: [Job?] = [nil]
    var items = [Token: Item]()

    var now = AsyncSequenceValidationDiagram.Clock.Instant(when: .zero)
    var generation = 0

    // nil selects the historical FIFO drain; a generator selects the seeded drain
    var random: SplitMix64?
    var traceLabel: String?
    var decisions = 0
    // called with the description of every item just before it runs, under either strategy
    var observer: (@Sendable (String) -> Void)?

    // Seeded scheduling: pick one item, uniformly by the generator, from every item that is
    // ready now. Unlike the FIFO drain this does not take only the head of each job's queue, and
    // it does not batch: the caller runs the item and asks again, so work the item enqueues
    // competes with work that was already ready.
    mutating func takeRandomReady() -> Item? {
      var candidates = [(job: Job?, index: Int, generation: Int)]()
      // Candidates are numbered in enqueue order (token generation), not by the queue they sit in.
      // Queues are keyed by job address, and a task allocated where a finished one used to be gets
      // the same key, so grouping by queue would make the meaning of a seed depend on memory reuse.
      // The set of ready items does not depend on the grouping, only their order would.
      for job in jobs {
        guard var queue = queues[job] else { continue }
        var index = 0
        while index < queue.count {
          switch queue[index] {
          case .cancelled(let token):
            items.removeValue(forKey: token)
            queue.remove(at: index)
            continue
          case .work(let token, _):
            candidates.append((job, index, token.generation))
          case .blocked(let token, let when, _), .emit(let token, let when, _, _, _):
            if when <= now {
              candidates.append((job, index, token.generation))
            }
          }
          index += 1
        }
        queues[job] = queue
      }
      guard candidates.count > 0, random != nil else {
        return nil
      }
      candidates.sort { $0.generation < $1.generation }
      let choice = random!.next(upperBound: candidates.count)
      let (job, index, _) = candidates[choice]
      let item = queues[job]!.remove(at: index)
      items.removeValue(forKey: item.token)
      if let traceLabel = traceLabel {
        print("[\(traceLabel)] step \(decisions) @\(now) pick \(choice + 1) of \(candidates.count): \(item)")
      }
      decisions += 1
      return item
    }

    mutating func drain() -> [Item] {
      var items = [Item]()
      // store off the jobs such that we can only visit the active queues
      var jobs = self.jobs

      while true {
        let startingCount = items.count
        var jobsToRemove = Set<Int>()
        // iterate in order of the jobs from when they have been seen
        // the dictionary is not ordered for its keys so make sure we iterate stably
        for jobIndex in 0..<jobs.count {
          let job = jobs[jobIndex]
          // this needs to be reassigned out because it is mutated by removal
          if var queue = queues[job] {
            switch queue.first {
            case .none:
              break
            case .cancelled(let token):
              self.items.removeValue(forKey: token)
              // clean out any cancelled items
              queue.removeFirst()
            case .blocked(let token, let when, _):
              if when <= now {
                self.items.removeValue(forKey: token)
                items.append(queue.removeFirst())
              } else {
                // this job is blocked by a wait
                jobsToRemove.insert(jobIndex)
              }
              break
            case .emit(let token, let when, _, _, _):
              if when <= now {
                self.items.removeValue(forKey: token)
                items.append(queue.removeFirst())
              } else {
                // this job is blocked by a wait
                jobsToRemove.insert(jobIndex)
              }
              break
            case .work(let token, _):
              self.items.removeValue(forKey: token)
              items.append(queue.removeFirst())
            }
            queues[job] = queue
            // if there is nothing left in this queue then don't bother with it anymore
            if queue.count == 0 {
              jobsToRemove.insert(jobIndex)
            }
          }
        }
        // clear out the iteration for the next pass
        for index in jobsToRemove.sorted().reversed() {
          jobs.remove(at: index)
        }
        // if we have not actually added anything in this loop
        // or if there are no more jobs to work with
        // break out of this particular drain
        if items.count == startingCount || jobs.count == 0 {
          break
        }
      }

      return items
    }
  }

  let state = ManagedCriticalState(State())

  var now: AsyncSequenceValidationDiagram.Clock.Instant {
    state.withCriticalRegion { $0.now }
  }

  struct Token: Hashable, CustomStringConvertible {
    var generation: Int

    var description: String {
      return generation.description
    }
  }

  func prepare() -> Token {
    state.withCriticalRegion { state in
      defer { state.generation += 1 }
      return Token(generation: state.generation)
    }
  }

  func cancel(_ token: Token) {
    state.withCriticalRegion { state in
      if let existing = state.items[token] {
        // find any existing items that are present and patch them up as cancelled
        let item = existing.cancelling()
        state.items[token] = item
        for (job, var queue) in state.queues {
          var finished = false
          for index in 0..<queue.count {
            if queue[index].token == existing.token {
              queue[index] = item
              finished = true
              break
            }
          }
          state.queues[job] = queue
          if finished {
            break
          }
        }
      } else {
        // emit a tombstone for the enqueue
        state.items[token] = .cancelled(token)
      }
    }
  }

  func enqueue(
    _ job: Job?,
    deadline: AsyncSequenceValidationDiagram.Clock.Instant,
    continuation: UnsafeContinuation<Void, Error>,
    token: Token
  ) {
    state.withCriticalRegion { state in
      if state.queues[job] == nil, let job = job {
        state.jobs.append(job)
      }
      if state.items[token]?.isCancelled == true {
        let item: Item = .work(
          token,
          {
            continuation.resume(throwing: CancellationError())
          }
        )
        state.queues[job, default: []].append(item)
        state.items[token] = item
      } else {
        let item: Item = .blocked(token, deadline, continuation)
        state.queues[job, default: []].append(item)
        state.items[token] = item
      }
    }
  }

  func enqueue(
    _ job: Job?,
    deadline: AsyncSequenceValidationDiagram.Clock.Instant,
    continuation: UnsafeContinuation<String?, Error>,
    _ result: Result<String?, Error>,
    index: Int,
    token: Token
  ) {
    state.withCriticalRegion { state in
      if state.queues[job] == nil, let job = job {
        state.jobs.append(job)
      }
      if state.items[token]?.isCancelled == true {
        let item: Item = .work(
          token,
          {
            // the input sequences should not throw cancellation errors
            continuation.resume(returning: nil)
          }
        )
        state.queues[job, default: []].append(item)
        state.items[token] = item
      } else {
        let item: Item = .emit(token, deadline, continuation, result, index)
        state.queues[job, default: []].append(item)
        state.items[token] = item
      }
    }
  }

  func enqueue(_ job: Job?, work: @Sendable @escaping () -> Void) {
    state.withCriticalRegion { state in
      if state.queues[job] == nil, let job = job {
        state.jobs.append(job)
      }
      let token = Token(generation: state.generation)
      let item: Item = .work(token, work)
      state.queues[job, default: []].append(item)
      state.generation += 1
      state.items[token] = item
    }
  }

  func configure(
    _ strategy: AsyncSequenceValidationDiagram.SchedulingStrategy,
    trace: Bool,
    observer: (@Sendable (String) -> Void)?
  ) {
    state.withCriticalRegion { state in
      state.observer = observer
      switch strategy.kind {
      case .fifo:
        state.random = nil
        state.traceLabel = nil
      case .seeded(let seed):
        state.random = SplitMix64(seed: seed)
        state.traceLabel = trace ? "schedule seed \(seed)" : nil
      }
    }
  }

  var isSeeded: Bool {
    state.withCriticalRegion { $0.random != nil }
  }

  var observer: (@Sendable (String) -> Void)? {
    state.withCriticalRegion { $0.observer }
  }

  func drainSeeded() {
    let observer = self.observer
    while let item = state.withCriticalRegion({ $0.takeRandomReady() }) {
      observer?(item.description)
      item.run()
    }
  }

  func drain() {
    if isSeeded {
      drainSeeded()
      return
    }
    let observer = self.observer
    // keep draining until there is no recursive work to do
    while true {
      var items: [Item] = state.withCriticalRegion { $0.drain() }
      if items.count == 0 {
        break
      }
      // ensure deterministic order of execution
      // first by source order, then by enqueue order
      items.sort()
      for item in items {
        observer?(item.description)
        item.run()
      }
    }
  }

  func advance() {
    if isSeeded {
      state.withCriticalRegion { state in
        state.now = state.now.advanced(by: .steps(1))
      }
      drainSeeded()
      return
    }
    let observer = self.observer
    // drain off the advancement
    var items: [Item] = state.withCriticalRegion { state in
      state.now = state.now.advanced(by: .steps(1))
      return state.drain()
    }
    // ensure deterministic order of execution
    // first by source order, then by enqueue order
    items.sort()
    for item in items {
      observer?(item.description)
      item.run()
    }

    // and cleanup any additional recursive items
    drain()
  }
}
