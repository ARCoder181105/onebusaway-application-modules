# GTFS-RT Matching CPU Investigation — Design

**Date:** 2026-07-10
**Branch:** `perf/gtfsrt-matching-investigation`
**Status:** Design — approved, revised after adversarial spec review

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
   **poll path** at scale.
2. A CPU flamegraph + an allocation profile + a wall-clock baseline
   (ms/refresh, refreshes/sec) captured with King County Metro (KCM) GTFS +
   GTFS-RT data.
3. An attribution of where the cycles actually go, and evidence (not just
   assertion) for the quadratic-cost hypothesis.
4. A prioritized list of next-step optimizations ranked by cost / risk / reward.
5. A regression safety net so any *future* optimization is provably
   behavior-preserving.

**Out of scope this session:** changing the matching engine itself. The
deliverable ends at the prioritized task list.

## Findings that motivate the design (from code exploration, verified in review)

Pipeline: `GtfsRealtimeSource.refresh()` (public; scheduled every
`_refreshInterval`, default 30 s) → `handleUpdates()` (`synchronized`) →
`GtfsRealtimeTripLibrary` matching → (gated by dedup) → `VehicleLocationListener`
→ `BlockLocationServiceImpl` position inference → caches.

**Two distinct cost stages, gated differently — this distinction is load-bearing
for the harness:**

- **Matching stage (paid every poll):**
  `handleCombinedUpdates` calls `createVehicleLocationRecordForUpdate` for all
  ~840 updates (`GtfsRealtimeSource.java:711`) **before** the `_lastVehicleUpdate`
  timestamp dedup (`GtfsRealtimeSource.java:741-742`). So the full O(feed) match
  cost is paid on every poll even when nothing changed.
  - `GtfsRealtimeTripLibrary.applyTripUpdatesToRecord` — outer loop over
    `stopTimeUpdateList` (line 958); `getBlockStopTimeForStopTimeUpdate` rebuilds
    a `MappingLibrary.mapToValue(stopTimes, "stopTime.gtfsSequence")` index on
    *every* stop-time update (line 1132); plus a second inner loop over all
    `blockTrip.getStopTimes()` per update (lines 964-970) → ≈ O(stops²) per trip.

- **Block-location write stage (gated by dedup):**
  the downstream `_vehicleLocationListener.handleVehicleLocationRecord(record)`
  (`GtfsRealtimeSource.java:744`) only runs when
  `prev.before(timestamp)` — i.e. when the feed timestamp has advanced. This is
  the entry to `VehicleStatusServiceImpl.handleVehicleLocationRecord` →
  `BlockLocationServiceImpl.handleVehicleLocationRecord` (`:228`) →
  `ScheduledBlockLocationServiceImpl` interpolation. In production the feed
  timestamp advances every 30 s, so this **also runs every poll**. A naïve
  harness that replays one static `.pb` would run it only on iteration 1 — see
  Component 2, which corrects for this.

**Read-path, NOT on the poll path:**
`AbstractBlockLocationServiceImpl.getBlockLocation` is invoked on API
arrivals/departures *queries*, not by `refresh()`. The poll path calls
`handleVehicleLocationRecord` (a cache *write*). The primary harness measures the
poll path; read-path cost is a **secondary, optional** measurement (see
Component 2b) and is not conflated with the poll suspects.

What is *not* a full-bundle rescan: `getActiveBlocks` / `getBlockInstance` are
per-blockId and `@Cacheable`; `BlockFinder` is cached (30-min TTL); shape points
and block indices are prebuilt. So the matching-stage suspects above are the
target.

## Design

### Principle: decouple the slow/fragile build from the fast profiled loop

The existing `onebusaway-gtfsrt-integration-tests` module is slow and fragile
because it rebuilds bundles and stands up Spring contexts inside a parallel-fork
test lifecycle. We reuse its proven building blocks
(`FederatedTransitDataBundleConventionMain` bundle builder; the
`GtfsRealtimeSource` DI wiring from `BundleLoader` **and its test subclasses**)
but run them from a **standalone `main()`**, never as tests in that module.

### Component 1 — Bundle build + pinned feed snapshot (slow, once, off to the side)

- Download KCM GTFS (`google_transit.zip`) into scratchpad and build the KCM
  bundle **once** into a persisted scratchpad directory via
  `FederatedTransitDataBundleConventionMain`.
- Download the 3 KCM `.pb` feeds **once** into scratchpad and **pin that
  snapshot** (do not re-download per run). Rationale: live KCM feeds drift —
  matched counts and service dates change run-to-run and eventually expire
  against a fixed bundle, destroying comparability. One pinned snapshot + the
  fixed bundle = reproducible numbers.
- This step is slow, runs outside the measured loop, and never touches CI. KCM
  data + the built bundle are git-ignored (no large binaries committed).

### Component 2 — Perf harness (new committed module `onebusaway-gtfsrt-perf`)

- New isolated Maven module depending on `onebusaway-transit-data-federation`
  (+ the federation-builder for bundle build if needed).
- **Reactor + test-run mechanics (concrete):** the module IS added to root
  `<modules>` so CI compiles it, but the driver is a `public static void main`
  class (**not** a `*Test`), so root `mvn install` compiles it and never
  executes it. (The root surefire `<excludes>` only covers
  `org/onebusaway/integration/**`; adding a `*Test` would otherwise run in the
  default unit phase.) No reliance on implicit exclusion.
- The driver `GtfsRtRefreshBenchmark` (main class):
  1. **Stand up the source with the full collaborator set** the poll path needs
     (replicating `BundleLoader` **plus** what
     `AbstractGtfsRealtimeIntegrationTest` wires — `BundleLoader` alone is
     insufficient):
     - blocking bundle load via `ContainerLibrary.createContext(...)`, waiting
       for `BundleManagementService.bundleIsReady()`, with the pre-built bundle
       mounted via the `bundle.root`/`bundlePath` system properties;
     - `setRefreshInterval(0)` so **no background `RefreshTask` is scheduled** —
       we drive `refresh()` synchronously and avoid the `synchronized
       handleUpdates` interleaving that a live scheduler + manual loop would
       cause;
     - a **non-null `VehicleLocationListener`** that delegates to the real
       `VehicleStatusServiceImpl` bean (mirroring
       `TestVehicleLocationListener`) — without it, `refresh()` NPEs at
       `cacheVehicleLocations` (line 662) and the block-location write path is
       never measured;
     - the cancel service and feed `file://` URLs
       (`setTripUpdatesUrl`/`setVehiclePositionsUrl`/`setAlertsUrl`) pointed at
       the pinned local `.pb` files.
  2. **Reset dedup per measured iteration.** Call `source.reset()` (public,
     `GtfsRealtimeSource.java:574` — clears `_lastVehicleUpdate`) at the top of
     every iteration so **both** stages run every iteration, matching
     production where the feed timestamp advances each poll. Without this, only
     iteration 1 exercises the block-location write path and the baseline
     understates real cost. (Log matched/unmatched + per-refresh ms — the source
     already logs these at lines 785-791 — to confirm both stages fire each
     iteration.)
  3. **Warmup:** ~20 `source.refresh()` calls (each with `reset()`), so **both**
     the matching and block-location paths are JIT-warm before measurement.
  4. **Measure:** ~100 `source.refresh()` calls, recording per-call ms →
     mean / p50 / p95, refreshes/sec.
  5. **Profile only the measure phase** (start the profiler after warmup, stop
     before teardown):
     - async-profiler `event=cpu` → CPU flamegraph HTML (primary);
     - async-profiler `event=alloc` → allocation flamegraph — directly tests the
       "index rebuilt on every stop-time update" hypothesis, which a CPU-only
       graph can mis-attribute to GC threads;
     - JVM flags for accurate stacks:
       `-XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints` (both
       async-profiler and JFR are safepoint-biased without it); pinned
       `-Xmx`/GC; `-Xlog:gc` for GC overhead.
     - **Fallback:** JDK-builtin **JFR** (`-XX:StartFlightRecording`) if
       async-profiler won't attach on macOS.
  6. Optionally dump the produced records to a local KCM snapshot for
     (non-committed) confidence.

Measuring `refresh()` in a `reset()`-per-iteration loop is faithful: production
does the identical `refresh()` → `handleUpdates` → match → listener work each
poll; we remove only the 30 s sleep.

### Component 2b — Optional read-path measurement (secondary)

The production symptom described (CPU pegged "importing and matching") points at
the poll path, which is the primary target. If poll-path cost does **not**
account for the observed saturation, add a secondary measured step that drives
API reads (`BlockLocationService.getScheduledLocationForBlockInstance` /
arrivals-and-departures) to exercise `getBlockLocation`, and profile that
separately. Kept out of the primary baseline so the two paths are never
conflated.

### Component 3 — Regression golden test (committed, unit-level, CI-friendly)

- A characterization test at the **unit level** on
  `GtfsRealtimeTripLibrary.createVehicleLocationRecordForUpdate` — constructed
  inputs, snapshot of the produced `VehicleLocationRecord` / predictions —
  consistent with the existing `GtfsRealtimeTripLibraryTest` (Mockito-based, no
  bundle/Spring lifecycle).
- Rationale (revised from the first draft): a *real* `source.refresh()` golden
  needs the same bundle build + Spring context + `BundleManagementService` as
  the fragile integration module, and the `cut.zip` fixtures live in that
  module's `src/test/resources` (not visible elsewhere without a test-jar or
  duplication). A unit-level golden covers the exact refactor target
  deterministically, fast, in CI, with zero lifecycle entanglement.
- Regeneratable via a flag (e.g. `-Dupdate.golden=true`).
- Purpose: any *future* optimization of the hot methods must reproduce identical
  output. This is the behavior-preserving safety net for step 1.
- Full-`refresh()` characterization is deferred to the integration module if ever
  wanted; not built this session.

### Components 4–5 — Analysis

- Attribute CPU **self-time** (poll path) to `applyTripUpdatesToRecord`,
  `getBlockStopTimeForStopTimeUpdate` (the map rebuild),
  `handleCombinedUpdates`, `handleVehicleLocationRecord`/
  `ScheduledBlockLocationServiceImpl` (block-location write), and feed parsing.
- Corroborate with the **allocation** profile and GC logs.
- **Demonstrate the complexity class, not just self-time:** capture per-trip
  stop-count distribution from the KCM bundle so the O(stops²) claim is
  evidence-backed (self-time correlated with stop count), rather than asserted
  from the flamegraph alone.

### Component 6 — Deliverable report

Written findings under `docs/`: baseline numbers, CPU + allocation attribution,
the quadratic-scaling evidence, and a ranked fix list (each entry: expected
reward, implementation cost, regression risk, and which existing/added test
covers it).

## Testing strategy

- **Unit-level golden** (Component 3) is the regression net for the hot path —
  committed, fast, deterministic, CI-safe.
- Existing coverage relied on as-is: `GtfsRealtimeTripLibraryTest` (12),
  `ScheduledBlockLocationServiceImplTest` (14), 16 end-to-end integration tests.
- The harness itself is validated by: it builds, it completes N refreshes
  without error, both stages fire each iteration (matched-count logging), and it
  emits non-empty CPU + alloc flamegraphs plus timing numbers.

## Risks & mitigations

- **Static-feed dedup hides half the pipeline** → `source.reset()` per measured
  iteration (Blocker fix); confirm via per-iteration matched-count logging.
- **Poll-path vs read-path confusion** → primary harness measures the poll path
  only; `getBlockLocation` (read-path) is a separate optional measurement.
- **async-profiler attach on macOS may fail** → JFR fallback (ships with JDK 11).
- **KCM bundle build is slow/heavy; live feed drifts** → build once, pin one
  `.pb` snapshot, persist, exclude from CI.
- **New module slowing/altering the default build** → added to the reactor as a
  `main()` class (no `*Test`), so `mvn install` compiles but never runs it.
- **`refresh()` NPE / missing collaborators** → enumerate and wire the full set
  (listener delegating to real `VehicleStatusServiceImpl`, cancel service, feed
  URLs, `refreshInterval=0`, blocking bundle load) per
  `AbstractGtfsRealtimeIntegrationTest`, not just `BundleLoader`.
- **Safepoint-biased stacks / single-run variance** → `DebugNonSafepoints`;
  report mean/p50/p95; profile only the measure phase.

## Non-goals

- No changes to `GtfsRealtimeTripLibrary` / `BlockLocationServiceImpl` / any
  matching code this session.
- No committing of KCM data or built bundles (large binaries).
- No modification to the fragile `onebusaway-gtfsrt-integration-tests` lifecycle.
