# GTFS-RT Matching CPU Investigation — Design

**Date:** 2026-07-10
**Branch:** `perf/gtfsrt-matching-investigation`
**Status:** Design — approved for implementation

## Problem

A production OBA deployment (2 GB RAM / 1 vCPU) runs with one GTFS-realtime feed
wired (~840 trips, polled every ~30 s). Memory is flat (~50%) but CPU is pegged
near 85% of the single core. A prior assessment claimed this is "mostly
legitimate work" — that matching 840 trips every 30 s is inherently enough to
saturate a core.

We doubt that claim. 840 trips every 30 s is ~28 trips/sec of matching work;
saturating a modern core with that implies large per-trip constant factors, i.e.
algorithmic waste, not inherent load.

## Goal

Produce **evidence**, not a fix:

1. A committed, repeatable performance harness that reproduces the production
   poll path at scale.
2. A CPU flamegraph + a wall-clock baseline (ms/refresh, refreshes/sec) captured
   with King County Metro (KCM) GTFS + GTFS-RT data.
3. An attribution of where the cycles actually go.
4. A prioritized list of next-step optimizations ranked by cost / risk / reward.
5. A regression safety net so any *future* optimization is provably
   behavior-preserving.

**Out of scope this session:** changing the matching engine itself. The
deliverable ends at the prioritized task list.

## Findings that motivate the design (from code exploration)

Pipeline: `GtfsRealtimeSource.refresh()` (scheduled every `_refreshInterval`,
default 30 s) → `handleUpdates()` → `GtfsRealtimeTripLibrary` matching →
`BlockLocationServiceImpl` / `ScheduledBlockLocationServiceImpl` position
inference → caches.

Prime CPU suspects (O(feed) × large per-trip constants, re-run every poll):

- `GtfsRealtimeTripLibrary.applyTripUpdatesToRecord` — outer loop over all
  `blockTrips`; for each `stopTimeUpdate`, an inner loop over all
  `blockTrip.getStopTimes()` (≈ O(stops²) per trip); and
  `getBlockStopTimeForStopTimeUpdate` rebuilds a
  `MappingLibrary.mapToValue(stopTimes, "stopTime.gtfsSequence")` index on
  *every* stop-time update.
- `GtfsRealtimeSource.handleCombinedUpdates` — calls
  `createVehicleLocationRecordForUpdate` for all ~840 updates every poll
  **before** the `_lastVehicleUpdate` timestamp dedup, so the full match cost is
  paid even when nothing changed.
- `AbstractBlockLocationServiceImpl.getBlockLocation` — nested loop over
  timepoint predictions × `blockConfig.getStopTimes()`.

What is *not* a full-bundle rescan: `getActiveBlocks` / `getBlockInstance` are
per-blockId and `@Cacheable`; `BlockFinder` is cached (30-min TTL); shape points
and block indices are prebuilt. So the suspects above are the target.

## Design

### Principle: decouple the slow/fragile build from the fast profiled loop

The existing `onebusaway-gtfsrt-integration-tests` module is slow and fragile
because it rebuilds bundles and stands up Spring contexts inside a parallel-fork
test lifecycle. We reuse its proven building blocks
(`FederatedTransitDataBundleConventionMain` bundle builder; the
`GtfsRealtimeSource` DI wiring pattern from `BundleLoader`) but run them from a
**standalone harness**, never as tests in that module.

### Component 1 — Bundle build (slow, once, off to the side)

- Download KCM GTFS (`google_transit.zip`) + `tripupdates.pb`,
  `vehiclepositions.pb`, `alerts.pb` into the scratchpad.
- Build the KCM bundle **once** into a persisted scratchpad directory via the
  federation bundle-builder convention main.
- This step is slow, runs outside the measured loop, and never touches CI.

### Component 2 — Perf harness (new committed module `onebusaway-gtfsrt-perf`)

- New isolated Maven module depending on `onebusaway-transit-data-federation`.
- **Kept out of the default reactor test run** (not listed as a normal reactor
  test, or its heavy driver guarded so `mvn install` at the root never runs it).
  KCM data + the built bundle live in scratchpad / are git-ignored — no large
  binaries committed.
- A driver (`GtfsRtRefreshBenchmark`) that:
  1. Loads the **pre-built** bundle (Spring context + `GtfsRealtimeSource`
     wired like `BundleLoader`), pointed at the local `.pb` files.
  2. Warmup: ~20 `source.refresh()` calls (JIT + cache warm).
  3. Measure: ~100 `source.refresh()` calls, recording per-call ms →
     mean / p50 / p95, refreshes/sec.
  4. Runs under **async-profiler** (`event=cpu`) → CPU flamegraph HTML.
     Fallback: JDK-builtin **JFR** (`-XX:StartFlightRecording`) if
     async-profiler won't attach on macOS.
  5. Optionally dumps the produced `VehicleLocationRecord`/prediction output to
     a local KCM snapshot for extra (non-committed) confidence.

Measuring `refresh()` in a loop is faithful: production does the identical
`refresh()` → `handleUpdates` → matching work; we just remove the 30 s sleep.
Feeding the same `.pb` repeatedly is representative because the matching +
schedule-deviation cost (the suspect) is paid per poll regardless of whether the
payload changed — that is precisely the waste under investigation.

### Component 3 — Regression golden test (committed, CI-friendly)

- A characterization test using a **small existing checked-in fixture**
  (an NYCT/WMATA `cut.zip` already in `onebusaway-gtfsrt-integration-tests`
  resources), **not** KCM (KCM is a network download + slow build — unfit for
  CI, and the fragility we were warned about).
- Runs one `source.refresh()` and snapshots the produced
  `VehicleLocationRecord`s / predictions to a committed golden fixture.
- Regeneratable via a flag (e.g. `-Dupdate.golden=true`).
- Purpose: any *future* optimization of the hot methods must reproduce identical
  output. This is the behavior-preserving safety net for step 1.
- Lives where it runs fast and deterministically (co-located with the matching
  unit tests in `onebusaway-transit-data-federation`, or a small fast test in
  the harness module — chosen during implementation to avoid the fragile
  integration lifecycle).

### Components 4–5 — Analysis

Read the flamegraph; attribute self-time to `applyTripUpdatesToRecord`,
`getBlockStopTimeForStopTimeUpdate` (map rebuild), `getBlockLocation`,
`handleCombinedUpdates`, and feed parsing. Quantify the O(stops²) and
before-dedup waste against the baseline ms/refresh.

### Component 6 — Deliverable report

Written findings: baseline numbers, flamegraph attribution, and a ranked
fix list (each: expected reward, implementation cost, regression risk, and which
existing/added test covers it). Committed under `docs/`.

## Testing strategy

- **Golden/characterization test** (above) is the regression net for the hot
  path — committed, fast, deterministic.
- Existing coverage relied on as-is: `GtfsRealtimeTripLibraryTest` (12),
  `ScheduledBlockLocationServiceImplTest` (14), 16 end-to-end integration tests.
- The harness itself is validated by: it builds, it completes N refreshes
  without error, and it emits a non-empty flamegraph + timing numbers.

## Risks & mitigations

- **async-profiler attach on macOS may fail** → JFR fallback (ships with JDK 11).
- **KCM bundle build is slow/heavy** → done once, persisted, excluded from CI;
  the measured loop reuses the built bundle.
- **New module slowing the default build** → excluded from the default reactor
  test run; heavy driver guarded so root `mvn install` never triggers it.
- **KCM feed doesn't match the built bundle's service dates** → harness logs
  matched-vs-unmatched counts each refresh so we confirm real matching is
  happening before trusting the flamegraph.

## Non-goals

- No changes to `GtfsRealtimeTripLibrary` / `BlockLocationServiceImpl` / any
  matching code this session.
- No committing of KCM data or built bundles (large binaries).
- No modification to the fragile `onebusaway-gtfsrt-integration-tests` lifecycle.
