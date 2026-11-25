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
@_spi(Testing) @testable import AsyncSequenceValidation

@available(AsyncAlgorithms 1.0, *)
final class TestSeededSchedule: XCTestCase {
  typealias Diagram = AsyncSequenceValidationDiagram
  typealias Strategy = AsyncSequenceValidationDiagram.SchedulingStrategy

  final class Recorder: @unchecked Sendable {
    let lock = NSLock()
    var steps = [String]()
    func append(_ step: String) {
      lock.lock()
      steps.append(step)
      lock.unlock()
    }
  }

  /// Runs one of the diagrams below and returns every scheduler item in the order it ran, plus the
  /// actual output reconstituted as a diagram.
  func run(_ name: String, _ strategy: Strategy) throws -> (steps: [String], actual: String, failures: [String]) {
    let recorder = Recorder()
    let observer: @Sendable (String) -> Void = { recorder.append($0) }
    let (result, failures): (Diagram.ExpectationResult, [Diagram.ExpectationFailure])
    switch name {
    case "merge3":
      (result, failures) = try Diagram.test(theme: .ascii, scheduling: strategy, observer: observer) {
        "a---e---|"
        "-b-d-f-h|"
        "--c---g-|"
        merge($0.inputs[0], $0.inputs[1], $0.inputs[2])
        "abcdefgh|"
      }
    case "mergeSameTick":
      (result, failures) = try Diagram.test(theme: .ascii, scheduling: strategy, observer: observer) {
        "a-c-|"
        "b-d-|"
        merge($0.inputs[0], $0.inputs[1])
        "[ab]-[cd]-|"
      }
    case "debounce":
      (result, failures) = try Diagram.test(theme: .ascii, scheduling: strategy, observer: observer) {
        "abcd----e---f-g----|"
        $0.inputs[0].debounce(for: .steps(3), clock: $0.clock)
        "------d----e-----g-|"
      }
    case "mapThroughTask":
      (result, failures) = try Diagram.test(theme: .ascii, scheduling: strategy, observer: observer) {
        "a--b--c|"
        $0.inputs[0].map { item in await Task { item.capitalized }.value }
        "A--B--C|"
      }
    case "cancel":
      (result, failures) = try Diagram.test(theme: .ascii, scheduling: strategy, observer: observer) {
        "a--b--c--d|"
        $0.inputs[0]
        "a--b-[;|]"
      }
    case "zip":
      (result, failures) = try Diagram.test(theme: .ascii, scheduling: strategy, observer: observer) {
        "a-b-c-|"
        "-x-y-z|"
        zip($0.inputs[0], $0.inputs[1]).map { "\($0)\($1)" }
        "-'ax'-'by'-'cz'|"
      }
    default:
      fatalError("unknown diagram \(name)")
    }
    return (recorder.steps, result.reconstituteActual(theme: .ascii), failures.map(\.description))
  }

  static let diagrams = ["merge3", "mergeSameTick", "debounce", "mapThroughTask", "cancel", "zip"]

  // MARK: - SplitMix64

  func test_splitmix64_matches_reference_vector() {
    // Reference outputs for seed 1234567 from the published SplitMix64 algorithm.
    var generator = SplitMix64(seed: 1_234_567)
    XCTAssertEqual(
      (0..<5).map { _ in generator.next() },
      [
        6_457_827_717_110_365_317,
        3_203_168_211_198_807_973,
        9_817_491_932_198_370_423,
        4_593_380_528_125_082_431,
        16_408_922_859_458_223_821,
      ]
    )
  }

  // MARK: - Strategy selection

  func test_strategy_descriptions_and_seeds() {
    XCTAssertEqual(Strategy.fifo.description, "fifo")
    XCTAssertNil(Strategy.fifo.seed)
    XCTAssertEqual(Strategy.seeded(42).description, "seeded(42)")
    XCTAssertEqual(Strategy.seeded(42).seed, 42)
    XCTAssertNotEqual(Strategy.seeded(1), Strategy.seeded(2))
  }

  func test_unset_environment_selects_fifo() throws {
    try XCTSkipIf(getenv(Strategy.seedEnvironmentVariable) != nil, "a seed is set for this process")
    XCTAssertEqual(Strategy.current, .fifo)
  }

  // MARK: - FIFO is unchanged

  // Golden schedules: the description of every item the scheduler ran, in order, recorded from the
  // FIFO scheduler as it was before seeded scheduling existed (commit 963eecf, instrumented only
  // with a print before each `item.run()`). If any of these change, FIFO behaviour changed.
  static let fifoGolden: [String: [String]] = [
    "merge3": [
      "work #0",
      "work #1",
      "work #2",
      "work #3",
      "work #4",
      "work #5",
      "work #6",
      "work #7",
      "work #8",
      "work #9",
      "work #10",
      "work #11",
      "work #12",
      "work #13",
      "work #14",
      "work #15",
      "work #16",
      "work #17",
      "work #18",
      "work #19",
      "work #20",
      "work #21",
      "work #22",
      "work #23",
      "work #24",
      "work #25",
      "emit #26 @tick 0 result success(Optional(\"a\")) side 0",
      "work #29",
      "work #30",
      "work #31",
      "work #32",
      "work #33",
      "work #34",
      "work #35",
      "work #36",
      "work #37",
      "work #38",
      "work #39",
      "work #40",
      "work #41",
      "work #42",
      "work #43",
      "emit #27 @tick 1 result success(Optional(\"b\")) side 1",
      "work #45",
      "work #46",
      "work #47",
      "work #48",
      "work #49",
      "work #50",
      "work #51",
      "work #52",
      "work #53",
      "work #54",
      "work #55",
      "work #56",
      "work #57",
      "work #58",
      "work #59",
      "emit #28 @tick 2 result success(Optional(\"c\")) side 2",
      "work #61",
      "work #62",
      "work #63",
      "work #64",
      "work #65",
      "work #66",
      "work #67",
      "work #68",
      "work #69",
      "work #70",
      "work #71",
      "work #72",
      "work #73",
      "work #74",
      "work #75",
      "emit #60 @tick 3 result success(Optional(\"d\")) side 1",
      "work #77",
      "work #78",
      "work #79",
      "work #80",
      "work #81",
      "work #82",
      "work #83",
      "work #84",
      "work #85",
      "work #86",
      "work #87",
      "work #88",
      "work #89",
      "work #90",
      "work #91",
      "emit #44 @tick 4 result success(Optional(\"e\")) side 0",
      "work #93",
      "work #94",
      "work #95",
      "work #96",
      "work #97",
      "work #98",
      "work #99",
      "work #100",
      "work #101",
      "work #102",
      "work #103",
      "work #104",
      "work #105",
      "work #106",
      "work #107",
      "emit #92 @tick 5 result success(Optional(\"f\")) side 1",
      "work #109",
      "work #110",
      "work #111",
      "work #112",
      "work #113",
      "work #114",
      "work #115",
      "work #116",
      "work #117",
      "work #118",
      "work #119",
      "work #120",
      "work #121",
      "work #122",
      "work #123",
      "emit #76 @tick 6 result success(Optional(\"g\")) side 2",
      "work #125",
      "work #126",
      "work #127",
      "work #128",
      "work #129",
      "work #130",
      "work #131",
      "work #132",
      "work #133",
      "work #134",
      "work #135",
      "work #136",
      "work #137",
      "work #138",
      "work #139",
      "emit #124 @tick 7 result success(Optional(\"h\")) side 1",
      "work #141",
      "work #142",
      "work #143",
      "work #144",
      "work #145",
      "work #146",
      "work #147",
      "work #148",
      "work #149",
      "work #150",
      "work #151",
      "work #152",
      "work #153",
      "work #154",
      "work #155",
      "emit #108 @tick 8 result success(nil) side 0",
      "emit #156 @tick 8 result success(nil) side 1",
      "emit #140 @tick 8 result success(nil) side 2",
      "work #157",
      "work #158",
      "work #159",
      "work #160",
      "work #161",
      "work #162",
      "work #163",
      "work #164",
      "work #165",
      "work #166",
      "work #167",
      "work #168",
      "work #169",
      "work #170",
      "work #171",
      "work #172",
      "work #173",
      "work #174",
      "work #175",
      "work #176",
      "work #177",
      "work #178",
      "work #179",
      "work #180",
      "work #181",
      "work #182",
      "work #183",
      "work #184",
      "work #185",
      "work #186",
    ],
    "mergeSameTick": [
      "work #0",
      "work #1",
      "work #2",
      "work #3",
      "work #4",
      "work #5",
      "work #6",
      "work #7",
      "work #8",
      "work #9",
      "work #10",
      "work #11",
      "work #12",
      "work #13",
      "work #14",
      "work #15",
      "work #16",
      "work #17",
      "work #18",
      "work #19",
      "work #20",
      "emit #21 @tick 0 result success(Optional(\"a\")) side 0",
      "emit #22 @tick 0 result success(Optional(\"b\")) side 1",
      "work #23",
      "work #24",
      "work #25",
      "work #26",
      "work #27",
      "work #28",
      "work #29",
      "work #30",
      "work #31",
      "work #32",
      "work #33",
      "work #34",
      "work #35",
      "work #36",
      "work #37",
      "work #38",
      "work #39",
      "work #40",
      "work #41",
      "work #42",
      "work #43",
      "work #44",
      "work #45",
      "work #46",
      "work #47",
      "work #48",
      "work #49",
      "emit #50 @tick 2 result success(Optional(\"c\")) side 0",
      "emit #51 @tick 2 result success(Optional(\"d\")) side 1",
      "work #52",
      "work #53",
      "work #54",
      "work #55",
      "work #56",
      "work #57",
      "work #58",
      "work #59",
      "work #60",
      "work #61",
      "work #62",
      "work #63",
      "work #64",
      "work #65",
      "work #66",
      "work #67",
      "work #68",
      "work #69",
      "work #70",
      "work #71",
      "work #72",
      "work #73",
      "work #74",
      "work #75",
      "work #76",
      "work #77",
      "work #78",
      "emit #79 @tick 4 result success(nil) side 0",
      "emit #80 @tick 4 result success(nil) side 1",
      "work #81",
      "work #82",
      "work #83",
      "work #84",
      "work #85",
      "work #86",
      "work #87",
      "work #88",
      "work #89",
      "work #90",
      "work #91",
      "work #92",
      "work #93",
      "work #94",
      "work #95",
      "work #96",
      "work #97",
      "work #98",
      "work #99",
      "work #100",
      "work #101",
      "work #102",
      "work #103",
      "work #104",
    ],
    "debounce": [
      "work #0",
      "work #1",
      "work #2",
      "work #3",
      "work #4",
      "work #5",
      "work #6",
      "work #7",
      "work #8",
      "work #9",
      "work #10",
      "work #11",
      "work #12",
      "work #13",
      "work #14",
      "work #15",
      "work #16",
      "work #17",
      "emit #18 @tick 0 result success(Optional(\"a\")) side 0",
      "work #19",
      "work #20",
      "work #21",
      "work #22",
      "work #23",
      "work #24",
      "work #25",
      "work #26",
      "work #27",
      "work #28",
      "emit #30 @tick 1 result success(Optional(\"b\")) side 0",
      "work #31",
      "work #32",
      "work #33",
      "work #34",
      "work #35",
      "work #36",
      "work #37",
      "emit #38 @tick 2 result success(Optional(\"c\")) side 0",
      "work #39",
      "work #40",
      "work #41",
      "work #42",
      "work #43",
      "work #44",
      "work #45",
      "wakeup #29 @tick 3",
      "emit #46 @tick 3 result success(Optional(\"d\")) side 0",
      "work #47",
      "work #48",
      "work #49",
      "work #50",
      "work #51",
      "work #52",
      "work #53",
      "work #54",
      "work #55",
      "work #56",
      "work #58",
      "work #59",
      "wakeup #57 @tick 5",
      "work #61",
      "work #62",
      "work #63",
      "work #64",
      "work #65",
      "wakeup #66 @tick 6",
      "work #67",
      "work #68",
      "work #69",
      "work #70",
      "work #71",
      "work #72",
      "work #73",
      "work #74",
      "work #75",
      "work #76",
      "emit #60 @tick 8 result success(Optional(\"e\")) side 0",
      "work #77",
      "work #78",
      "work #79",
      "work #80",
      "work #81",
      "work #82",
      "work #83",
      "work #84",
      "work #85",
      "work #86",
      "wakeup #87 @tick 11",
      "work #89",
      "work #90",
      "work #91",
      "work #92",
      "work #93",
      "work #94",
      "work #95",
      "work #96",
      "work #97",
      "work #98",
      "emit #88 @tick 12 result success(Optional(\"f\")) side 0",
      "work #99",
      "work #100",
      "work #101",
      "work #102",
      "work #103",
      "work #104",
      "work #105",
      "work #106",
      "work #107",
      "work #108",
      "emit #110 @tick 14 result success(Optional(\"g\")) side 0",
      "work #111",
      "work #112",
      "work #113",
      "work #114",
      "work #115",
      "work #116",
      "work #117",
      "wakeup #109 @tick 15",
      "work #119",
      "work #120",
      "work #121",
      "work #122",
      "work #123",
      "wakeup #124 @tick 17",
      "work #125",
      "work #126",
      "work #127",
      "work #128",
      "work #129",
      "work #130",
      "work #131",
      "work #132",
      "work #133",
      "work #134",
      "emit #118 @tick 19 result success(nil) side 0",
      "work #135",
      "work #136",
      "work #137",
      "work #138",
      "work #139",
      "work #140",
      "work #141",
      "work #142",
      "work #143",
      "work #144",
      "work #145",
      "work #146",
      "work #147",
      "work #148",
      "work #149",
      "work #150",
      "work #151",
      "work #152",
      "work #153",
      "work #154",
      "work #155",
      "work #156",
      "work #157",
      "work #158",
    ],
    "mapThroughTask": [
      "work #0",
      "work #1",
      "work #2",
      "work #3",
      "work #4",
      "emit #5 @tick 0 result success(Optional(\"a\")) side 0",
      "work #6",
      "work #7",
      "work #8",
      "work #9",
      "work #10",
      "work #11",
      "work #12",
      "work #13",
      "work #14",
      "work #15",
      "work #16",
      "work #17",
      "work #18",
      "emit #19 @tick 3 result success(Optional(\"b\")) side 0",
      "work #20",
      "work #21",
      "work #22",
      "work #23",
      "work #24",
      "work #25",
      "work #26",
      "work #27",
      "work #28",
      "work #29",
      "work #30",
      "work #31",
      "work #32",
      "emit #33 @tick 6 result success(Optional(\"c\")) side 0",
      "work #34",
      "work #35",
      "work #36",
      "work #37",
      "work #38",
      "work #39",
      "work #40",
      "work #41",
      "work #42",
      "work #43",
      "work #44",
      "work #45",
      "work #46",
      "emit #47 @tick 7 result success(nil) side 0",
      "work #48",
      "work #49",
      "work #50",
      "work #51",
      "work #52",
      "work #53",
      "work #54",
      "work #55",
      "work #56",
    ],
    "cancel": [
      "work #0",
      "work #1",
      "work #2",
      "work #3",
      "work #4",
      "emit #5 @tick 0 result success(Optional(\"a\")) side 0",
      "work #6",
      "work #7",
      "work #8",
      "work #9",
      "work #10",
      "work #11",
      "emit #12 @tick 3 result success(Optional(\"b\")) side 0",
      "work #13",
      "work #14",
      "work #15",
      "work #16",
      "work #17",
      "work #18",
      "work #19",
      "work #20",
      "work #21",
      "work #22",
      "work #23",
      "work #24",
      "work #25",
      "work #26",
      "work #27",
      "work #28",
      "work #29",
      "work #30",
      "work #31",
    ],
    "zip": [
      "work #0",
      "work #1",
      "work #2",
      "work #3",
      "work #4",
      "work #5",
      "work #6",
      "work #7",
      "work #8",
      "work #9",
      "work #10",
      "work #11",
      "work #12",
      "work #13",
      "work #14",
      "work #15",
      "work #16",
      "work #17",
      "work #18",
      "work #19",
      "work #20",
      "emit #21 @tick 0 result success(Optional(\"a\")) side 0",
      "work #23",
      "work #24",
      "work #25",
      "work #26",
      "emit #22 @tick 1 result success(Optional(\"x\")) side 1",
      "work #27",
      "work #28",
      "work #29",
      "work #30",
      "work #31",
      "work #32",
      "work #33",
      "work #34",
      "work #35",
      "work #36",
      "work #37",
      "work #38",
      "work #39",
      "work #40",
      "work #41",
      "work #42",
      "work #43",
      "work #44",
      "work #45",
      "work #46",
      "work #47",
      "work #48",
      "work #49",
      "emit #50 @tick 2 result success(Optional(\"b\")) side 0",
      "work #52",
      "work #53",
      "work #54",
      "work #55",
      "emit #51 @tick 3 result success(Optional(\"y\")) side 1",
      "work #56",
      "work #57",
      "work #58",
      "work #59",
      "work #60",
      "work #61",
      "work #62",
      "work #63",
      "work #64",
      "work #65",
      "work #66",
      "work #67",
      "work #68",
      "work #69",
      "work #70",
      "work #71",
      "work #72",
      "work #73",
      "work #74",
      "work #75",
      "work #76",
      "work #77",
      "work #78",
      "emit #79 @tick 4 result success(Optional(\"c\")) side 0",
      "work #81",
      "work #82",
      "work #83",
      "work #84",
      "emit #80 @tick 5 result success(Optional(\"z\")) side 1",
      "work #85",
      "work #86",
      "work #87",
      "work #88",
      "work #89",
      "work #90",
      "work #91",
      "work #92",
      "work #93",
      "work #94",
      "work #95",
      "work #96",
      "work #97",
      "work #98",
      "work #99",
      "work #100",
      "work #101",
      "work #102",
      "work #103",
      "work #104",
      "work #105",
      "work #106",
      "work #107",
      "emit #108 @tick 6 result success(nil) side 0",
      "emit #109 @tick 6 result success(nil) side 1",
      "work #110",
      "work #111",
      "work #112",
      "work #113",
      "work #114",
      "work #115",
      "work #116",
      "work #117",
      "work #118",
      "work #119",
      "work #120",
      "work #121",
      "work #122",
      "work #123",
      "work #124",
      "work #125",
      "work #126",
      "work #127",
      "work #128",
      "work #129",
      "work #130",
      "work #131",
      "work #132",
      "work #133",
      "work #134",
      "work #135",
      "work #136",
      "work #137",
      "work #138",
      "work #139",
      "work #140",
      "work #141",
    ],
  ]

  static func trimmed(_ step: String) -> String {
    var step = Substring(step)
    while step.last == " " { step.removeLast() }
    return String(step)
  }

  func test_fifo_schedule_matches_pre_seeding_golden() throws {
    for name in Self.diagrams {
      let golden = try XCTUnwrap(Self.fifoGolden[name], "no golden schedule for \(name)")
      let (steps, _, failures) = try run(name, .fifo)
      XCTAssertEqual(failures, [], name)
      // the golden copies drop the trailing space that wake-up descriptions end with
      XCTAssertEqual(steps.map(Self.trimmed), golden.map(Self.trimmed), "FIFO schedule of \(name) changed")
    }
  }

  func test_default_strategy_is_fifo() throws {
    try XCTSkipIf(getenv(Strategy.seedEnvironmentVariable) != nil, "a seed is set for this process")
    let recorder = Recorder()
    let observer: @Sendable (String) -> Void = { recorder.append($0) }
    // With no seed in the environment, `.current` must schedule exactly as `.fifo` does.
    let (result, _) = try Diagram.test(theme: .ascii, scheduling: .current, observer: observer) {
      "a-c-|"
      "b-d-|"
      merge($0.inputs[0], $0.inputs[1])
      "[ab]-[cd]-|"
    }
    XCTAssertEqual(result.reconstituteActual(theme: .ascii), "[ab]-[cd]-|")
    XCTAssertEqual(recorder.steps, try run("mergeSameTick", .fifo).steps)
  }

  // MARK: - Seeded schedules

  func test_seed_replays_identically() throws {
    for name in Self.diagrams {
      for seed: UInt64 in [1, 2, 3, 17, 99] {
        let first = try run(name, .seeded(seed))
        let second = try run(name, .seeded(seed))
        XCTAssertEqual(first.steps, second.steps, "\(name) seed \(seed)")
        XCTAssertEqual(first.actual, second.actual, "\(name) seed \(seed)")
      }
    }
  }

  func test_seeds_explore_orders_fifo_never_takes() throws {
    // Two inputs emit at the same tick: FIFO always delivers input 0 first. Some seed must deliver
    // input 1 first, which the real concurrent runtime can also do.
    let fifo = try run("mergeSameTick", .fifo)
    XCTAssertEqual(fifo.actual, "[ab]-[cd]-|")
    var outputs = Set<String>()
    for seed: UInt64 in 1...50 {
      outputs.insert(try run("mergeSameTick", .seeded(seed)).actual)
    }
    XCTAssertTrue(outputs.contains("[ab]-[cd]-|"), "\(outputs)")
    XCTAssertTrue(outputs.contains { $0.hasPrefix("[ba]") }, "\(outputs)")
  }

  func test_seeded_schedule_keeps_unambiguous_outputs() throws {
    // These diagrams have no two events at the same tick, so every interleaving must produce the
    // FIFO output. (The number of items run may differ: operators do more or less internal work
    // depending on the interleaving.)
    for name in ["merge3", "debounce", "zip", "mapThroughTask"] {
      let fifo = try run(name, .fifo)
      for seed: UInt64 in 1...20 {
        let seeded = try run(name, .seeded(seed))
        XCTAssertEqual(seeded.actual, fifo.actual, "\(name) seed \(seed)")
        XCTAssertEqual(seeded.failures, [], "\(name) seed \(seed)")
      }
    }
  }
}
#endif
