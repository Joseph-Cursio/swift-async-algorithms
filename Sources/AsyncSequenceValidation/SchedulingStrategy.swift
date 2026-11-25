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

#if canImport(Darwin)
import Darwin
#elseif canImport(Glibc)
import Glibc
#elseif canImport(Musl)
import Musl
#elseif canImport(Bionic)
import Bionic
#elseif canImport(wasi_pthread)
import wasi_pthread
#endif

@available(AsyncAlgorithms 1.0, *)
extension AsyncSequenceValidationDiagram {
  /// How the validation scheduler orders work that is ready at the same point in virtual time.
  ///
  /// The default, ``fifo``, is the scheduler's historical order: it collects ready work in rounds and
  /// runs each round sorted by input side and then by enqueue order.
  ///
  /// ``seeded(_:)`` instead runs one ready item at a time, chosen by a SplitMix64 generator, from
  /// every item that is ready: pending task jobs, and clock wake-ups and input emissions whose
  /// deadline has passed. Virtual time still only advances once nothing is ready, so a seed explores
  /// an interleaving that the real concurrent runtime could also produce. The FIFO order is one of
  /// the orders a seed can produce.
  ///
  /// A seed replays the same order only when the code under test is itself deterministic. Code that
  /// iterates a `Dictionary` or `Set` needs `SWIFT_DETERMINISTIC_HASHING=1` in the environment.
  public struct SchedulingStrategy: Sendable, Hashable, CustomStringConvertible {
    enum Kind: Sendable, Hashable {
      case fifo
      case seeded(UInt64)
    }

    let kind: Kind

    /// The historical first-in, first-out order.
    public static var fifo: SchedulingStrategy { SchedulingStrategy(kind: .fifo) }

    /// A pseudo-random order among ready work, reproducible from `seed`.
    public static func seeded(_ seed: UInt64) -> SchedulingStrategy {
      SchedulingStrategy(kind: .seeded(seed))
    }

    /// The environment variable that selects a seed for every validation diagram in the process.
    public static let seedEnvironmentVariable = "ASYNC_VALIDATION_SCHEDULE_SEED"

    /// When this environment variable is set to `1`, each scheduling decision of a seeded run is
    /// printed to standard output.
    public static let traceEnvironmentVariable = "ASYNC_VALIDATION_SCHEDULE_TRACE"

    /// The strategy selected by the environment: ``seeded(_:)`` when ``seedEnvironmentVariable``
    /// holds an unsigned integer, ``fifo`` otherwise.
    public static var current: SchedulingStrategy {
      guard let raw = getenv(seedEnvironmentVariable) else {
        return .fifo
      }
      guard let seed = UInt64(String(cString: raw)) else {
        return .fifo
      }
      return .seeded(seed)
    }

    /// The seed of a ``seeded(_:)`` strategy, or nil for ``fifo``.
    public var seed: UInt64? {
      switch kind {
      case .fifo: return nil
      case .seeded(let seed): return seed
      }
    }

    public var description: String {
      switch kind {
      case .fifo: return "fifo"
      case .seeded(let seed): return "seeded(\(seed))"
      }
    }

    static var traceEnabled: Bool {
      guard let raw = getenv(traceEnvironmentVariable) else {
        return false
      }
      return String(cString: raw) == "1"
    }
  }
}

/// SplitMix64 (Steele, Lea and Flood, 2014). Small, fast, and specified bit for bit, so a seed means
/// the same sequence on every platform and toolchain.
struct SplitMix64: Sendable {
  var state: UInt64

  init(seed: UInt64) {
    state = seed
  }

  mutating func next() -> UInt64 {
    state &+= 0x9E37_79B9_7F4A_7C15
    var z = state
    z = (z ^ (z >> 30)) &* 0xBF58_476D_1CE4_E5B9
    z = (z ^ (z >> 27)) &* 0x94D0_49BB_1331_11EB
    return z ^ (z >> 31)
  }

  /// A value in `0..<upperBound`. Uses a plain modulo; the bias is negligible for the handful of
  /// candidates a scheduling decision has, and the mapping stays trivially portable.
  mutating func next(upperBound: Int) -> Int {
    precondition(upperBound > 0)
    return Int(next() % UInt64(upperBound))
  }
}
