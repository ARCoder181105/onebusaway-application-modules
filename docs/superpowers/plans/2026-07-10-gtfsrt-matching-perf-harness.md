# GTFS-RT Matching Perf Harness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a committed, repeatable performance harness that reproduces OBA's GTFS-RT poll path at King County Metro scale, capture a CPU + allocation profile and a wall-clock baseline, and produce a ranked list of optimization tasks — without changing the matching engine.

**Architecture:** A new isolated Maven module `onebusaway-gtfsrt-perf` holds a standalone `main()` harness that (1) loads a pre-built KCM bundle by replicating the integration tests' `BundleLoader` + `AbstractGtfsRealtimeIntegrationTest` wiring, (2) drives `GtfsRealtimeSource.reset()` + `refresh()` in a timed loop so both the matching and block-location write stages run every iteration, and (3) profiles only the measure phase. A separate unit-level golden test on `GtfsRealtimeTripLibrary.createVehicleLocationRecordForUpdate` is the behavior-preserving regression net. KCM data and the built bundle live in the scratchpad and are never committed.

**Tech Stack:** Java 11, Spring 5.2 (non-Boot), Maven multi-module, async-profiler (primary, macOS build) with programmatic JDK Flight Recorder (JFR) fallback, existing OBA federation stack.

## Global Constraints

- Java 11; project version `2.7.1` — new module inherits parent `onebusaway-application-modules` `2.7.1`.
- Apache 2.0 license header required on every new `.java` file (run `mvn verify` checks license headers).
- No matching-engine source changes (`GtfsRealtimeTripLibrary`, `GtfsRealtimeSource`, `BlockLocationServiceImpl`, `ScheduledBlockLocationServiceImpl`, etc. are read-only this session).
- No large binaries committed: KCM GTFS zip, the built bundle, `.pb` snapshots, and generated flamegraphs stay under the scratchpad `/private/tmp/claude-501/-Users-aaron-repos-onebusaway-app-modules/37b98a36-1a28-480c-b199-a1b58c7b3a18/scratchpad` and are git-ignored.
- The harness driver is a `public static void main` class (NOT named `*Test`) so root `mvn install` compiles but never executes it. Root surefire only excludes `org/onebusaway/integration/**`.
- Reference paths for the wiring being replicated:
  - `onebusaway-gtfsrt-integration-tests/.../BundleLoader.java` (`create`, `load`, `setupEnvironment`).
  - `onebusaway-gtfsrt-integration-tests/.../AbstractGtfsRealtimeIntegrationTest.java` lines 129-143, 346-368 (listener + cancel service + feed URL + refresh).
  - `onebusaway-gtfsrt-integration-tests/src/test/resources/test-data-sources.xml` (Spring context).
  - `GtfsRealtimeSource.reset()` = line 574, `refresh()` = line 586, dedup gate = lines 741-748.

---

### Task 1: Scaffold the `onebusaway-gtfsrt-perf` module

**Files:**
- Create: `onebusaway-gtfsrt-perf/pom.xml`
- Modify: `pom.xml` (root `<modules>`, after line 125 `onebusaway-gtfsrt-integration-tests`)
- Create: `onebusaway-gtfsrt-perf/src/main/java/org/onebusaway/perf/gtfsrt/Placeholder.java`

**Interfaces:**
- Produces: a compilable module in the reactor named `onebusaway-gtfsrt-perf`.

- [ ] **Step 1: Create the module POM**

`onebusaway-gtfsrt-perf/pom.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <parent>
    <groupId>org.onebusaway</groupId>
    <artifactId>onebusaway-application-modules</artifactId>
    <version>2.7.1</version>
  </parent>
  <artifactId>onebusaway-gtfsrt-perf</artifactId>
  <packaging>jar</packaging>
  <name>onebusaway-gtfsrt-perf</name>
  <description>Standalone performance harness for the GTFS-RT poll/matching path. Not part of the default test run.</description>

  <dependencies>
    <dependency>
      <groupId>org.onebusaway</groupId>
      <artifactId>onebusaway-transit-data-federation</artifactId>
      <version>${project.version}</version>
    </dependency>
    <dependency>
      <groupId>org.onebusaway</groupId>
      <artifactId>onebusaway-transit-data-federation-builder</artifactId>
      <version>${project.version}</version>
    </dependency>
    <dependency>
      <groupId>org.apache.logging.log4j</groupId>
      <artifactId>log4j-api</artifactId>
      <version>${log4j.version}</version>
    </dependency>
    <dependency>
      <groupId>org.apache.logging.log4j</groupId>
      <artifactId>log4j-core</artifactId>
      <version>${log4j.version}</version>
    </dependency>
    <dependency>
      <groupId>org.apache.logging.log4j</groupId>
      <artifactId>log4j-slf4j-impl</artifactId>
      <version>${log4j.version}</version>
    </dependency>
  </dependencies>
</project>
```

- [ ] **Step 2: Register the module in the root reactor**

In root `pom.xml`, add immediately after `<module>onebusaway-gtfsrt-integration-tests</module>` (line 125):

```xml
        <module>onebusaway-gtfsrt-perf</module>
```

- [ ] **Step 3: Add a placeholder class with license header**

`onebusaway-gtfsrt-perf/src/main/java/org/onebusaway/perf/gtfsrt/Placeholder.java`:

```java
/**
 * Copyright (C) 2026 Open Transit Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.onebusaway.perf.gtfsrt;

/** Placeholder so the module compiles before the harness lands. Removed in Task 4. */
final class Placeholder {
  private Placeholder() {}
}
```

- [ ] **Step 4: Verify it compiles and inherits the license plugin**

Run: `mvn -q -pl onebusaway-gtfsrt-perf -am -DskipTests compile`
Expected: `BUILD SUCCESS`, module `onebusaway-gtfsrt-perf` compiles.

- [ ] **Step 5: Verify the default test phase runs nothing in this module**

Run: `mvn -q -pl onebusaway-gtfsrt-perf test`
Expected: `BUILD SUCCESS` with "No tests to run" / zero tests (module has no `*Test`).

- [ ] **Step 6: Commit**

```bash
git add onebusaway-gtfsrt-perf/pom.xml pom.xml onebusaway-gtfsrt-perf/src/main/java/org/onebusaway/perf/gtfsrt/Placeholder.java
git commit -m "Add onebusaway-gtfsrt-perf module scaffold"
```

---

### Task 2: Bundle-load harness (replicate BundleLoader + integration-test wiring)

**Files:**
- Create: `onebusaway-gtfsrt-perf/src/main/resources/perf-data-sources.xml`
- Create: `onebusaway-gtfsrt-perf/src/main/java/org/onebusaway/perf/gtfsrt/PerfBundleHarness.java`

**Interfaces:**
- Produces: `PerfBundleHarness` with
  - `PerfBundleHarness(String bundleRootDir, String tripUpdatesUrl, String vehiclePositionsUrl, String alertsUrl)`
  - `void open() throws Exception` — builds Spring context, wires and starts the source.
  - `void refreshOnce() throws Exception` — `source.reset()` then `source.refresh()`.
  - `GtfsRealtimeSource getSource()`
  - `int lastRecordCount()` — number of `VehicleLocationRecord`s produced by the most recent refresh (via the capturing listener).
  - `void close()`
- Consumes (Task 4/5 rely on these exact signatures).

- [ ] **Step 1: Copy the Spring context config**

Create `onebusaway-gtfsrt-perf/src/main/resources/perf-data-sources.xml` with the exact contents of `onebusaway-gtfsrt-integration-tests/src/test/resources/test-data-sources.xml` (imports `application-context.xml` + `application-context-services.xml`, component-scans `org.onebusaway.transit_data_federation.impl`, defines `bundleManagementService` with `bundleStoreRoot=${bundle.root}` / `standaloneMode=true` / `remoteSourceURI=${bundle.remote.source}`, and the placeholder `httpServiceClient`). Read that file and reproduce it verbatim under the new path.

- [ ] **Step 2: Write the harness class**

`PerfBundleHarness.java` (license header as in Task 1). The `open()` body replicates `BundleLoader.create`+`load` and the listener/feed wiring from `AbstractGtfsRealtimeIntegrationTest` (lines 129-143). A capturing listener that delegates to the real `VehicleStatusServiceImpl` is required — without it `refresh()` NPEs in `cacheVehicleLocations`.

```java
package org.onebusaway.perf.gtfsrt;

// license header above
import org.onebusaway.container.ContainerLibrary;
import org.onebusaway.gtfs.model.AgencyAndId;
import org.onebusaway.realtime.api.VehicleLocationListener;
import org.onebusaway.realtime.api.VehicleLocationRecord;
import org.onebusaway.transit_data_federation.impl.realtime.DynamicBlockIndexServiceImpl;
import org.onebusaway.transit_data_federation.impl.realtime.VehicleStatusServiceImpl;
import org.onebusaway.transit_data_federation.impl.realtime.gtfs_realtime.GtfsRealtimeCancelServiceImpl;
import org.onebusaway.transit_data_federation.impl.realtime.gtfs_realtime.GtfsRealtimeSource;
import org.onebusaway.transit_data_federation.impl.transit_graph.StopTimeEntriesFactory;
import org.onebusaway.transit_data_federation.services.AgencyService;
import org.onebusaway.transit_data_federation.services.ConsolidatedStopsService;
import org.onebusaway.transit_data_federation.services.StopSwapService;
import org.onebusaway.transit_data_federation.services.blocks.BlockCalendarService;
import org.onebusaway.transit_data_federation.services.blocks.BlockIndexService;
import org.onebusaway.transit_data_federation.services.bundle.BundleManagementService;
import org.onebusaway.transit_data_federation.services.narrative.NarrativeService;
import org.onebusaway.transit_data_federation.services.realtime.BlockLocationService;
import org.onebusaway.transit_data_federation.services.shapes.ShapePointService;
import org.onebusaway.transit_data_federation.services.transit_graph.TransitGraphDao;
import org.springframework.context.ConfigurableApplicationContext;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

public class PerfBundleHarness {

  private final String _bundleRootDir;
  private final String _tripUpdatesUrl;
  private final String _vehiclePositionsUrl;
  private final String _alertsUrl;

  private ConfigurableApplicationContext _context;
  private GtfsRealtimeSource _source;
  private final AtomicInteger _lastCount = new AtomicInteger();

  public PerfBundleHarness(String bundleRootDir, String tripUpdatesUrl,
                           String vehiclePositionsUrl, String alertsUrl) {
    _bundleRootDir = bundleRootDir;
    _tripUpdatesUrl = tripUpdatesUrl;
    _vehiclePositionsUrl = vehiclePositionsUrl;
    _alertsUrl = alertsUrl;
  }

  public void open() throws Exception {
    System.setProperty("bundle.root", _bundleRootDir);
    System.setProperty("bundlePath", _bundleRootDir);
    System.setProperty("bundle.remote.source", _bundleRootDir + File.separator + "index.json");

    _context = ContainerLibrary.createContext(Collections.singletonList("perf-data-sources.xml"));

    _source = new GtfsRealtimeSource();
    _source.setRefreshInterval(0); // no background scheduler; we drive refresh() synchronously
    _source.setTransitGraphDao(_context.getBean(TransitGraphDao.class));
    _source.setAgencyService(_context.getBean(AgencyService.class));
    _source.setBlockCalendarService(_context.getBean(BlockCalendarService.class));
    _source.setBlockLocationService(_context.getBean(BlockLocationService.class));
    _source.setScheduledExecutorService(_context.getBean(ScheduledExecutorService.class));
    _source.setConsolidatedStopsService(_context.getBean(ConsolidatedStopsService.class));
    _source.setDynamicBlockIndexService(_context.getBean(DynamicBlockIndexServiceImpl.class));
    _source.setStopTimeEntriesFactory(_context.getBean(StopTimeEntriesFactory.class));
    _source.setNarrativeService(_context.getBean(NarrativeService.class));
    _source.setShapePointService(_context.getBean(ShapePointService.class));
    _source.setStopSwapService(_context.getBean(StopSwapService.class));
    _source.setBlockIndexService(_context.getBean(BlockIndexService.class));

    BundleManagementService bundleManagementService = _context.getBean(BundleManagementService.class);
    int i = 0;
    while (!bundleManagementService.bundleIsReady()) {
      Thread.sleep(1000);
      if (i++ % 10 == 0) System.out.println("waiting on bundle...");
    }
    _source.markBundleReady();
    _source.setFilterUnassigned(false);

    // Capturing listener that delegates to the real block-location write path.
    VehicleLocationListener real = _context.getBean(VehicleStatusServiceImpl.class);
    _source.setVehicleLocationListener(new CountingListener(real, _lastCount));
    _source.setGtfsRealtimeCancelService(_context.getBean(GtfsRealtimeCancelServiceImpl.class));

    _source.setTripUpdatesUrl(new java.net.URL(_tripUpdatesUrl));
    if (_vehiclePositionsUrl != null)
      _source.setVehiclePositionsUrl(new java.net.URL(_vehiclePositionsUrl));
    if (_alertsUrl != null)
      _source.setAlertsUrl(new java.net.URL(_alertsUrl));

    _source.start();
  }

  public void refreshOnce() throws Exception {
    _lastCount.set(0);
    _source.reset();      // clear _lastVehicleUpdate so BOTH stages run this iteration
    _source.refresh();
  }

  public int lastRecordCount() { return _lastCount.get(); }
  public GtfsRealtimeSource getSource() { return _source; }

  public void close() {
    if (_context != null) { _context.stop(); _context.close(); }
  }

  private static final class CountingListener implements VehicleLocationListener {
    private final VehicleLocationListener _delegate;
    private final AtomicInteger _counter;
    CountingListener(VehicleLocationListener delegate, AtomicInteger counter) {
      _delegate = delegate; _counter = counter;
    }
    @Override public void handleVehicleLocationRecord(VehicleLocationRecord record) {
      _counter.incrementAndGet();
      _delegate.handleVehicleLocationRecord(record);
    }
    @Override public void handleVehicleLocationRecords(List<VehicleLocationRecord> records) {
      _counter.addAndGet(records.size());
      _delegate.handleVehicleLocationRecords(records);
    }
    @Override public void resetVehicleLocation(AgencyAndId vehicleId) { _delegate.resetVehicleLocation(vehicleId); }
    @Override public void handleRawPosition(AgencyAndId vehicleId, double lat, double lon, long timestamp) { }
  }
}
```

- [ ] **Step 3: Delete the placeholder**

```bash
git rm onebusaway-gtfsrt-perf/src/main/java/org/onebusaway/perf/gtfsrt/Placeholder.java
```

- [ ] **Step 4: Verify compilation**

Run: `mvn -q -pl onebusaway-gtfsrt-perf -am -DskipTests compile`
Expected: `BUILD SUCCESS`. If a setter name (e.g. `setVehiclePositionsUrl`, `setGtfsRealtimeCancelService`) does not resolve, grep `GtfsRealtimeSource.java` for the exact setter and correct the call — do not invent names.

- [ ] **Step 5: Commit**

```bash
git add onebusaway-gtfsrt-perf/src/main/resources/perf-data-sources.xml onebusaway-gtfsrt-perf/src/main/java/org/onebusaway/perf/gtfsrt/PerfBundleHarness.java
git commit -m "Add PerfBundleHarness: standalone GtfsRealtimeSource wiring"
```

---

### Task 3: One-time KCM bundle build (scratchpad; not committed)

**Files:**
- Create: `onebusaway-gtfsrt-perf/src/main/java/org/onebusaway/perf/gtfsrt/BuildKcmBundle.java`

**Interfaces:**
- Consumes: nothing from prior tasks (uses `FederatedTransitDataBundleConventionMain` from the builder dep).
- Produces: a built bundle directory + `index.json` on disk at a scratchpad path (input to Task 4).

- [ ] **Step 1: Write the bundle-builder main**

`BuildKcmBundle.java` (license header). Mirrors `BundleBuilder.buildBundle(inputDir, outputDir, name)` + `createIndexJson`.

```java
package org.onebusaway.perf.gtfsrt;

// license header above
import org.onebusaway.transit_data_federation.bundle.FederatedTransitDataBundleConventionMain;

import java.io.File;
import java.io.FileWriter;

/** Builds a bundle from an already-unzipped GTFS directory. Run once. Args: <gtfsInputDir> <bundleOutputDir> <name> */
public class BuildKcmBundle {
  public static void main(String[] args) throws Exception {
    if (args.length != 3) {
      System.err.println("usage: BuildKcmBundle <gtfsInputDir> <bundleOutputDir> <name>");
      System.exit(2);
    }
    String inputDir = args[0], outputDir = args[1], name = args[2];
    new File(outputDir).mkdirs();
    String gzipUri = new FederatedTransitDataBundleConventionMain().run(new String[]{inputDir, outputDir, name});
    try (FileWriter fw = new FileWriter(outputDir + File.separator + "index.json")) {
      fw.write("{\"latest\":\"" + gzipUri + "\"}");
    }
    System.out.println("BUNDLE_BUILT gzip=" + gzipUri + " root=" + outputDir);
  }
}
```

- [ ] **Step 2: Compile**

Run: `mvn -q -pl onebusaway-gtfsrt-perf -am -DskipTests compile`
Expected: `BUILD SUCCESS`.

- [ ] **Step 3: Download + unzip KCM GTFS into the scratchpad**

```bash
SCRATCH=/private/tmp/claude-501/-Users-aaron-repos-onebusaway-app-modules/37b98a36-1a28-480c-b199-a1b58c7b3a18/scratchpad
mkdir -p "$SCRATCH/kcm/gtfs" "$SCRATCH/kcm/bundle" "$SCRATCH/kcm/rt"
curl -sSL -o "$SCRATCH/kcm/google_transit.zip" https://metro.kingcounty.gov/GTFS/google_transit.zip
unzip -o -q "$SCRATCH/kcm/google_transit.zip" -d "$SCRATCH/kcm/gtfs"
ls -la "$SCRATCH/kcm/gtfs"
```
Expected: standard GTFS `.txt` files (`stops.txt`, `trips.txt`, `stop_times.txt`, `shapes.txt`, etc.).

- [ ] **Step 4: Pin one snapshot of the 3 realtime feeds**

```bash
SCRATCH=/private/tmp/claude-501/-Users-aaron-repos-onebusaway-app-modules/37b98a36-1a28-480c-b199-a1b58c7b3a18/scratchpad
curl -sSL -o "$SCRATCH/kcm/rt/tripupdates.pb" https://s3.amazonaws.com/kcm-alerts-realtime-prod/tripupdates.pb
curl -sSL -o "$SCRATCH/kcm/rt/vehiclepositions.pb" https://s3.amazonaws.com/kcm-alerts-realtime-prod/vehiclepositions.pb
curl -sSL -o "$SCRATCH/kcm/rt/alerts.pb" https://s3.amazonaws.com/kcm-alerts-realtime-prod/alerts.pb
ls -la "$SCRATCH/kcm/rt"
```
Expected: three non-empty `.pb` files. These are pinned; do not re-download during measurement.

- [ ] **Step 5: Build the bundle (slow, once)**

```bash
SCRATCH=/private/tmp/claude-501/-Users-aaron-repos-onebusaway-app-modules/37b98a36-1a28-480c-b199-a1b58c7b3a18/scratchpad
mvn -q -pl onebusaway-gtfsrt-perf exec:java \
  -Dexec.mainClass=org.onebusaway.perf.gtfsrt.BuildKcmBundle \
  -Dexec.args="$SCRATCH/kcm/gtfs $SCRATCH/kcm/bundle kcm" 2>&1 | tail -20
```
Note: if `exec-maven-plugin` is not configured, run via explicit classpath instead:
```bash
CP=$(mvn -q -pl onebusaway-gtfsrt-perf dependency:build-classpath -Dmdep.outputFile=/dev/stdout 2>/dev/null | tail -1)
java -cp "onebusaway-gtfsrt-perf/target/classes:$CP" org.onebusaway.perf.gtfsrt.BuildKcmBundle "$SCRATCH/kcm/gtfs" "$SCRATCH/kcm/bundle" kcm
```
Expected: `BUNDLE_BUILT ...` line; `$SCRATCH/kcm/bundle` populated with bundle artifacts + `index.json`.

- [ ] **Step 6: Verify the bundle looks complete**

```bash
SCRATCH=/private/tmp/claude-501/-Users-aaron-repos-onebusaway-app-modules/37b98a36-1a28-480c-b199-a1b58c7b3a18/scratchpad
find "$SCRATCH/kcm/bundle" -maxdepth 2 -type f | head -40
cat "$SCRATCH/kcm/bundle/index.json"
```
Expected: bundle metadata/graph files present; `index.json` references the built gzip. No commit (scratchpad only).

---

### Task 4: The timed refresh loop (`GtfsRtRefreshBenchmark`)

**Files:**
- Create: `onebusaway-gtfsrt-perf/src/main/java/org/onebusaway/perf/gtfsrt/GtfsRtRefreshBenchmark.java`

**Interfaces:**
- Consumes: `PerfBundleHarness(open/refreshOnce/lastRecordCount/close)` from Task 2.
- Produces: console baseline stats (mean/p50/p95 ms, refreshes/sec) + per-iteration matched-record counts.

- [ ] **Step 1: Write the benchmark main**

`GtfsRtRefreshBenchmark.java` (license header). Reads bundle root + feed paths from system properties; warms up, then measures.

```java
package org.onebusaway.perf.gtfsrt;

// license header above
import java.io.File;
import java.util.Arrays;

/**
 * Drives GtfsRealtimeSource.refresh() in a timed loop. System properties:
 *   perf.bundleRoot (required), perf.rtDir (required, holds tripupdates.pb/vehiclepositions.pb/alerts.pb),
 *   perf.warmup (default 20), perf.measure (default 100).
 */
public class GtfsRtRefreshBenchmark {
  public static void main(String[] args) throws Exception {
    String bundleRoot = required("perf.bundleRoot");
    String rtDir = required("perf.rtDir");
    int warmup = Integer.getInteger("perf.warmup", 20);
    int measure = Integer.getInteger("perf.measure", 100);

    String tu = new File(rtDir, "tripupdates.pb").toURI().toString();
    String vp = new File(rtDir, "vehiclepositions.pb").toURI().toString();
    String al = new File(rtDir, "alerts.pb").toURI().toString();

    PerfBundleHarness harness = new PerfBundleHarness(bundleRoot, tu, vp, al);
    harness.open();
    try {
      System.out.println("== warmup " + warmup + " ==");
      for (int i = 0; i < warmup; i++) {
        harness.refreshOnce();
        if (i == 0) System.out.println("first refresh records=" + harness.lastRecordCount());
      }

      System.out.println("== measure " + measure + " ==");
      long[] ns = new long[measure];
      Profiling.start();       // no-op until Task 5 wires the profiler
      for (int i = 0; i < measure; i++) {
        long t0 = System.nanoTime();
        harness.refreshOnce();
        ns[i] = System.nanoTime() - t0;
        if (i % 10 == 0)
          System.out.println("iter " + i + " ms=" + (ns[i] / 1_000_000.0) + " records=" + harness.lastRecordCount());
      }
      Profiling.stop();
      report(ns);
    } finally {
      harness.close();
    }
  }

  private static void report(long[] ns) {
    long[] sorted = ns.clone();
    Arrays.sort(sorted);
    double mean = Arrays.stream(ns).average().orElse(0) / 1_000_000.0;
    double p50 = sorted[sorted.length / 2] / 1_000_000.0;
    double p95 = sorted[(int) (sorted.length * 0.95)] / 1_000_000.0;
    double total = Arrays.stream(ns).sum() / 1_000_000_000.0;
    System.out.printf("BASELINE n=%d mean=%.2fms p50=%.2fms p95=%.2fms throughput=%.2f refreshes/sec%n",
        ns.length, mean, p50, p95, ns.length / total);
  }

  private static String required(String key) {
    String v = System.getProperty(key);
    if (v == null) throw new IllegalStateException("missing -D" + key);
    return v;
  }
}
```

- [ ] **Step 2: Add a temporary no-op `Profiling` shim (replaced in Task 5)**

`onebusaway-gtfsrt-perf/src/main/java/org/onebusaway/perf/gtfsrt/Profiling.java`:

```java
package org.onebusaway.perf.gtfsrt;

// license header above
/** Measure-phase profiler control. Task 4: no-op. Task 5: real async-profiler / JFR. */
final class Profiling {
  private Profiling() {}
  static void start() {}
  static void stop() {}
}
```

- [ ] **Step 3: Compile**

Run: `mvn -q -pl onebusaway-gtfsrt-perf -am -DskipTests compile`
Expected: `BUILD SUCCESS`.

- [ ] **Step 4: Run the baseline and confirm BOTH stages fire every iteration**

```bash
SCRATCH=/private/tmp/claude-501/-Users-aaron-repos-onebusaway-app-modules/37b98a36-1a28-480c-b199-a1b58c7b3a18/scratchpad
CP=$(mvn -q -pl onebusaway-gtfsrt-perf dependency:build-classpath -Dmdep.outputFile=/dev/stdout 2>/dev/null | tail -1)
java -cp "onebusaway-gtfsrt-perf/target/classes:$CP" \
  -Dperf.bundleRoot="$SCRATCH/kcm/bundle" -Dperf.rtDir="$SCRATCH/kcm/rt" \
  org.onebusaway.perf.gtfsrt.GtfsRtRefreshBenchmark 2>&1 | tail -40
```
Expected: a `BASELINE ...` line, and **`records=` > 0 on every logged iteration** (not just iteration 0) — this proves `reset()` makes the block-location write path run each refresh. If `records` is 0 after iteration 0, the dedup fix is not working — stop and investigate before trusting numbers.

- [ ] **Step 5: Commit (code only; no scratchpad data)**

```bash
git add onebusaway-gtfsrt-perf/src/main/java/org/onebusaway/perf/gtfsrt/GtfsRtRefreshBenchmark.java onebusaway-gtfsrt-perf/src/main/java/org/onebusaway/perf/gtfsrt/Profiling.java
git commit -m "Add GtfsRtRefreshBenchmark timed refresh loop"
```

---

### Task 5: Profile only the measure phase (async-profiler primary, JFR fallback)

**Files:**
- Modify: `onebusaway-gtfsrt-perf/src/main/java/org/onebusaway/perf/gtfsrt/Profiling.java`

**Interfaces:**
- Consumes: called by `GtfsRtRefreshBenchmark` (`Profiling.start()`/`stop()` already in place).
- Produces: `cpu-flamegraph.html` + `alloc-flamegraph.html` (async-profiler) OR `perf-recording.jfr` (JFR fallback) covering only the measure loop.

- [ ] **Step 1: Implement measure-phase profiler control**

Replace `Profiling.java` body. async-profiler is controlled via its agent command interface loaded with `-agentpath`; JFR via `jdk.jfr.Recording`. Selection by system property `perf.profiler` (`async` | `jfr` | `none`, default `async`).

```java
package org.onebusaway.perf.gtfsrt;

// license header above
import java.lang.reflect.Method;
import java.nio.file.Paths;

/** Starts/stops a profiler around the measure loop only. */
final class Profiling {
  private Profiling() {}
  private static Object jfrRecording;

  static void start() {
    String mode = System.getProperty("perf.profiler", "async");
    String out = System.getProperty("perf.out", ".");
    try {
      if ("async".equals(mode)) {
        // async-profiler must be loaded via -agentpath:libasyncProfiler.dylib=... (see run cmd).
        // Control it through its dynamic-attach command channel exposed on the loaded lib.
        String event = System.getProperty("perf.event", "cpu"); // cpu | alloc
        execAsync("start,event=" + event + ",file=" + out + "/" + event + "-flamegraph.html");
        System.out.println("async-profiler " + event + " started");
      } else if ("jfr".equals(mode)) {
        Class<?> rec = Class.forName("jdk.jfr.Recording");
        Object r = rec.getConstructor().newInstance();
        rec.getMethod("enable", String.class).invoke(r, "jdk.ObjectAllocationSample");
        rec.getMethod("enable", String.class).invoke(r, "jdk.ExecutionSample");
        rec.getMethod("start").invoke(r);
        jfrRecording = r;
        System.out.println("JFR recording started");
      }
    } catch (Throwable t) {
      System.out.println("Profiling.start skipped: " + t);
    }
  }

  static void stop() {
    String mode = System.getProperty("perf.profiler", "async");
    String out = System.getProperty("perf.out", ".");
    try {
      if ("async".equals(mode)) {
        execAsync("stop");
        // second pass for allocation profile
        System.out.println("async-profiler cpu written");
      } else if ("jfr".equals(mode) && jfrRecording != null) {
        Class<?> rec = jfrRecording.getClass();
        rec.getMethod("stop").invoke(jfrRecording);
        rec.getMethod("dump", java.nio.file.Path.class)
           .invoke(jfrRecording, Paths.get(out, "perf-recording.jfr"));
        System.out.println("JFR recording written to " + out + "/perf-recording.jfr");
      }
    } catch (Throwable t) {
      System.out.println("Profiling.stop skipped: " + t);
    }
  }

  // Calls one.profiler.AsyncProfiler.execute(cmd) via reflection so the harness
  // compiles even when the async-profiler jar is absent.
  private static void execAsync(String cmd) throws Exception {
    Class<?> ap = Class.forName("one.profiler.AsyncProfiler");
    Object instance = ap.getMethod("getInstance").invoke(null);
    Method execute = ap.getMethod("execute", String.class);
    execute.invoke(instance, cmd);
  }
}
```

- [ ] **Step 2: Download async-profiler (macOS) into the scratchpad**

```bash
SCRATCH=/private/tmp/claude-501/-Users-aaron-repos-onebusaway-app-modules/37b98a36-1a28-480c-b199-a1b58c7b3a18/scratchpad
cd "$SCRATCH"
curl -sSL -o async-profiler.zip https://github.com/async-profiler/async-profiler/releases/download/v3.0/async-profiler-3.0-macos.zip
unzip -o -q async-profiler.zip
ls async-profiler-3.0-macos/lib async-profiler-3.0-macos/bin 2>/dev/null || ls async-profiler-3.0-macos
```
Expected: `libasyncProfiler.dylib` and an `async-profiler.jar` (provides `one.profiler.AsyncProfiler`). If the URL 404s, list releases and pick the latest macOS asset.

- [ ] **Step 3: Run the CPU profile over the measure phase**

```bash
SCRATCH=/private/tmp/claude-501/-Users-aaron-repos-onebusaway-app-modules/37b98a36-1a28-480c-b199-a1b58c7b3a18/scratchpad
AP="$SCRATCH/async-profiler-3.0-macos"
CP=$(mvn -q -pl onebusaway-gtfsrt-perf dependency:build-classpath -Dmdep.outputFile=/dev/stdout 2>/dev/null | tail -1)
java -cp "onebusaway-gtfsrt-perf/target/classes:$AP/lib/async-profiler.jar:$CP" \
  -agentpath:"$AP/lib/libasyncProfiler.dylib" \
  -XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints -Xmx1500m -Xlog:gc \
  -Dperf.profiler=async -Dperf.out="$SCRATCH/kcm" \
  -Dperf.bundleRoot="$SCRATCH/kcm/bundle" -Dperf.rtDir="$SCRATCH/kcm/rt" \
  org.onebusaway.perf.gtfsrt.GtfsRtRefreshBenchmark 2>&1 | tail -40
ls -la "$SCRATCH/kcm/cpu-flamegraph.html"
```
Expected: `BASELINE ...` line and a non-empty `cpu-flamegraph.html`. Note: the jar path may be `$AP/async-profiler.jar` or `$AP/lib/async-profiler.jar` depending on the release layout — adjust from Step 2's `ls`. `-Xmx1500m` approximates the 2 GB prod box.

- [ ] **Step 4: Capture the allocation profile**

Re-run Step 3 adding `-Dperf.event=alloc` (the `Profiling` class reads `perf.event`, default `cpu`). This writes `alloc-flamegraph.html` alongside the CPU one.
Expected: non-empty `alloc-flamegraph.html`.

- [ ] **Step 5: Verify the JFR fallback works (so the harness is usable without async-profiler)**

```bash
SCRATCH=/private/tmp/claude-501/-Users-aaron-repos-onebusaway-app-modules/37b98a36-1a28-480c-b199-a1b58c7b3a18/scratchpad
CP=$(mvn -q -pl onebusaway-gtfsrt-perf dependency:build-classpath -Dmdep.outputFile=/dev/stdout 2>/dev/null | tail -1)
java -cp "onebusaway-gtfsrt-perf/target/classes:$CP" \
  -XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints -Xmx1500m \
  -Dperf.profiler=jfr -Dperf.out="$SCRATCH/kcm" \
  -Dperf.bundleRoot="$SCRATCH/kcm/bundle" -Dperf.rtDir="$SCRATCH/kcm/rt" \
  org.onebusaway.perf.gtfsrt.GtfsRtRefreshBenchmark 2>&1 | tail -10
ls -la "$SCRATCH/kcm/perf-recording.jfr"
```
Expected: a non-empty `perf-recording.jfr`.

- [ ] **Step 6: Commit (code only)**

```bash
git add onebusaway-gtfsrt-perf/src/main/java/org/onebusaway/perf/gtfsrt/Profiling.java
git commit -m "Wire measure-phase profiling (async-profiler + JFR fallback)"
```

---

### Task 6: Unit-level golden/characterization test for the matching hot path

**Files:**
- Test: `onebusaway-transit-data-federation/src/test/java/org/onebusaway/transit_data_federation/impl/realtime/gtfs_realtime/GtfsRealtimeTripLibraryCharacterizationTest.java`
- Create (golden fixture): `onebusaway-transit-data-federation/src/test/resources/org/onebusaway/transit_data_federation/impl/realtime/gtfs_realtime/characterization-golden.txt`

**Interfaces:**
- Consumes: existing test scaffolding in `GtfsRealtimeTripLibraryTest.java` (same package) — reuse its mock setup and any `@Before`-built `GtfsRealtimeTripLibrary` / entity-source helpers.
- Produces: a committed golden that locks `createVehicleLocationRecordForUpdate` output; future optimization must reproduce it byte-for-byte.

- [ ] **Step 1: Read the existing test to reuse its fixtures**

Read `onebusaway-transit-data-federation/src/test/java/org/onebusaway/transit_data_federation/impl/realtime/gtfs_realtime/GtfsRealtimeTripLibraryTest.java`. Identify: how it constructs a `GtfsRealtimeTripLibrary`, how it builds a `TripUpdate`/`CombinedTripUpdatesAndVehiclePosition`, and how it calls `createVehicleLocationRecordForUpdate`. The characterization test reuses the same construction so it exercises the real matching path with fixed inputs.

- [ ] **Step 2: Write the failing characterization test**

Model construction on the existing test's most representative "createVehicleLocationRecordForUpdate with stop-time updates + timepoint predictions" case. Serialize the deterministic fields of the resulting `VehicleLocationRecord` (blockId, tripId, serviceDate, scheduleDeviation, and each `TimepointPredictionRecord`'s stopId/gtfsSequence/predicted arrival+departure) into a stable, sorted string, and compare to the golden.

```java
// license header
package org.onebusaway.transit_data_federation.impl.realtime.gtfs_realtime;

import static org.junit.Assert.assertEquals;
import org.junit.Test;
import org.onebusaway.realtime.api.TimepointPredictionRecord;
import org.onebusaway.realtime.api.VehicleLocationRecord;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.StringJoiner;

public class GtfsRealtimeTripLibraryCharacterizationTest {

  private static final String GOLDEN =
      "org/onebusaway/transit_data_federation/impl/realtime/gtfs_realtime/characterization-golden.txt";

  @Test
  public void createVehicleLocationRecord_matchesGolden() throws Exception {
    // Build library + a CombinedTripUpdatesAndVehiclePosition exactly as
    // GtfsRealtimeTripLibraryTest does for its stop-time-update + predictions case.
    // VehicleLocationRecord record = library.createVehicleLocationRecordForUpdate(update);
    VehicleLocationRecord record = buildRecordUnderTest();

    String actual = serialize(record);
    if (Boolean.getBoolean("update.golden")) {
      Path p = Paths.get("src/test/resources", GOLDEN);
      Files.createDirectories(p.getParent());
      Files.write(p, actual.getBytes());
    }
    String expected = new String(Files.readAllBytes(
        Paths.get(Thread.currentThread().getContextClassLoader().getResource(GOLDEN).toURI())));
    assertEquals(expected, actual);
  }

  private static String serialize(VehicleLocationRecord r) {
    StringJoiner sj = new StringJoiner("\n");
    sj.add("blockId=" + r.getBlockId());
    sj.add("tripId=" + r.getTripId());
    sj.add("serviceDate=" + r.getServiceDate());
    sj.add("scheduleDeviation=" + r.getScheduleDeviation());
    if (r.getTimepointPredictions() != null) {
      r.getTimepointPredictions().stream()
        .sorted(java.util.Comparator.comparingInt(TimepointPredictionRecord::getStopSequence))
        .forEach(t -> sj.add("tpr stop=" + t.getStopId() + " seq=" + t.getStopSequence()
            + " arr=" + t.getTimepointPredictedArrivalTime()
            + " dep=" + t.getTimepointPredictedDepartureTime()));
    }
    return sj.toString();
  }

  // Extracted so the construction mirrors GtfsRealtimeTripLibraryTest's fixture.
  private VehicleLocationRecord buildRecordUnderTest() {
    throw new UnsupportedOperationException("port fixture from GtfsRealtimeTripLibraryTest");
  }
}
```

- [ ] **Step 3: Run it and confirm it fails for the right reason**

Run: `mvn -q -pl onebusaway-transit-data-federation test -Dtest=GtfsRealtimeTripLibraryCharacterizationTest`
Expected: FAIL — first because `buildRecordUnderTest` throws, then (after porting the fixture) because the golden file does not exist yet.

- [ ] **Step 4: Port the fixture and generate the golden**

Replace `buildRecordUnderTest()` with the real construction copied from `GtfsRealtimeTripLibraryTest`'s representative case (verify field getters exist on `VehicleLocationRecord` / `TimepointPredictionRecord`; adjust `serialize` to the actual getter names). Generate the golden:

Run: `mvn -q -pl onebusaway-transit-data-federation test -Dtest=GtfsRealtimeTripLibraryCharacterizationTest -Dupdate.golden=true`
Expected: writes `characterization-golden.txt`; test passes on the write pass.

- [ ] **Step 5: Run again without the flag to confirm it locks**

Run: `mvn -q -pl onebusaway-transit-data-federation test -Dtest=GtfsRealtimeTripLibraryCharacterizationTest`
Expected: PASS reading the committed golden.

- [ ] **Step 6: Commit**

```bash
git add onebusaway-transit-data-federation/src/test/java/org/onebusaway/transit_data_federation/impl/realtime/gtfs_realtime/GtfsRealtimeTripLibraryCharacterizationTest.java \
        onebusaway-transit-data-federation/src/test/resources/org/onebusaway/transit_data_federation/impl/realtime/gtfs_realtime/characterization-golden.txt
git commit -m "Add golden characterization test for createVehicleLocationRecordForUpdate"
```

---

### Task 7: Capture baseline + profiles and evidence the quadratic hypothesis

**Files:**
- Create: `onebusaway-gtfsrt-perf/src/main/java/org/onebusaway/perf/gtfsrt/StopCountHistogram.java`

**Interfaces:**
- Consumes: `PerfBundleHarness` (to reach `TransitGraphDao` via a new getter) OR reads the bundle directly. Produces: a distribution of stop-times-per-trip for the KCM bundle (evidence that hot-method self-time scales with stop count).

- [ ] **Step 1: Add a getter to reach the graph for the histogram**

In `PerfBundleHarness`, expose the context: add `public org.springframework.context.ApplicationContext getContext() { return _context; }`. Recompile.

- [ ] **Step 2: Write the histogram main**

`StopCountHistogram.java` (license header): loads the harness, gets `TransitGraphDao`, iterates `getAllTrips()`, buckets `trip.getStopTimes().size()`, prints a histogram + min/median/max/total-trips. This quantifies "stops per trip" so the flamegraph's `applyTripUpdatesToRecord` self-time can be correlated with stop count (O(stops²) evidence).

```java
package org.onebusaway.perf.gtfsrt;
// license header above
import org.onebusaway.transit_data_federation.services.transit_graph.TransitGraphDao;
import org.onebusaway.transit_data_federation.services.transit_graph.TripEntry;
import java.util.TreeMap;

public class StopCountHistogram {
  public static void main(String[] args) throws Exception {
    String bundleRoot = System.getProperty("perf.bundleRoot");
    PerfBundleHarness h = new PerfBundleHarness(bundleRoot, "file:///dev/null", null, null);
    // open() needs feeds only for refresh; for the histogram we just need the graph,
    // so guard: catch feed-URL failures after context is up, or add an openGraphOnly().
    h.open();
    try {
      TransitGraphDao dao = h.getContext().getBean(TransitGraphDao.class);
      TreeMap<Integer, Integer> hist = new TreeMap<>();
      int n = 0; long sum = 0;
      for (TripEntry t : dao.getAllTrips()) {
        int s = t.getStopTimes().size();
        hist.merge(s / 10 * 10, 1, Integer::sum);
        n++; sum += s;
      }
      System.out.println("trips=" + n + " avgStops=" + (n == 0 ? 0 : sum / n));
      hist.forEach((bucket, count) -> System.out.println("stops[" + bucket + "-" + (bucket + 9) + "]=" + count));
    } finally {
      h.close();
    }
  }
}
```
Note: if `open()` cannot tolerate a `/dev/null` trip-updates URL, add `PerfBundleHarness.openGraphOnly()` that does everything except feed-URL wiring and `start()`. Prefer that if `start()` requires a valid feed.

- [ ] **Step 3: Compile and run the histogram**

```bash
SCRATCH=/private/tmp/claude-501/-Users-aaron-repos-onebusaway-app-modules/37b98a36-1a28-480c-b199-a1b58c7b3a18/scratchpad
mvn -q -pl onebusaway-gtfsrt-perf -am -DskipTests compile
CP=$(mvn -q -pl onebusaway-gtfsrt-perf dependency:build-classpath -Dmdep.outputFile=/dev/stdout 2>/dev/null | tail -1)
java -cp "onebusaway-gtfsrt-perf/target/classes:$CP" -Xmx1500m \
  -Dperf.bundleRoot="$SCRATCH/kcm/bundle" \
  org.onebusaway.perf.gtfsrt.StopCountHistogram 2>&1 | tail -30
```
Expected: `trips=... avgStops=...` and a stop-count histogram.

- [ ] **Step 4: Run the full profiled baseline (from Task 5) and save all artifacts**

Re-run Task 5 Step 3 (CPU) and Step 4 (alloc). Copy the `BASELINE` line, `cpu-flamegraph.html`, `alloc-flamegraph.html`, GC log summary, and the histogram output into a scratchpad results folder for Task 8. Record: feed trip-update count (log line at `GtfsRealtimeSource.java:785-791`), matched vs unmatched, ms/refresh, and the top self-time frames from the CPU flamegraph.

- [ ] **Step 5: Commit the histogram tool (code only)**

```bash
git add onebusaway-gtfsrt-perf/src/main/java/org/onebusaway/perf/gtfsrt/StopCountHistogram.java onebusaway-gtfsrt-perf/src/main/java/org/onebusaway/perf/gtfsrt/PerfBundleHarness.java
git commit -m "Add stop-count histogram for quadratic-cost evidence"
```

---

### Task 8: Findings report + ranked fix list

**Files:**
- Create: `docs/perf/2026-07-10-gtfsrt-matching-findings.md`

**Interfaces:**
- Consumes: all artifacts from Task 7. Produces: the deliverable report.

- [ ] **Step 1: Write the report**

`docs/perf/2026-07-10-gtfsrt-matching-findings.md` with sections:
1. **Setup** — KCM bundle stats (trips, avg/median/max stops per trip from Task 7), pinned feed snapshot (trip-update count), JVM flags, `-Xmx1500m`, hardware.
2. **Baseline** — mean/p50/p95 ms per refresh, refreshes/sec, matched vs unmatched counts, extrapolation to the 30 s poll / one-core budget (is the poll path actually ~85% of a core at ~840 trips?).
3. **CPU attribution** — top self-time frames from the flamegraph, explicitly quantifying `applyTripUpdatesToRecord`, `getBlockStopTimeForStopTimeUpdate` (map rebuild), `handleCombinedUpdates`, block-location write, and feed parsing.
4. **Allocation attribution** — top alloc sites; confirm/deny the "index rebuilt per stop-time update" churn.
5. **Quadratic evidence** — correlate self-time with the stop-count histogram; state whether the O(stops²) hypothesis holds.
6. **Verdict on the prior claim** — is "mostly legitimate work" accurate, or is it algorithmic waste?
7. **Ranked fix list** — each entry: description, expected reward (est. % of poll-path CPU), implementation cost, regression risk, and the covering test (existing or the Task 6 golden). Rank lowest-cost/lowest-risk/highest-reward first. Seed candidates (validate against the flamegraph before asserting):
   - hoist the `MappingLibrary.mapToValue(stopTimes, ...)` index out of the per-stop-time-update loop (`GtfsRealtimeTripLibrary.java:1132`) — build once per block-trip;
   - remove the O(stops) `lastStopScheduleTime` inner rescan (`:964-970`);
   - move `createVehicleLocationRecordForUpdate` to run only for changed vehicles (dedup before match, `GtfsRealtimeSource.java:711` vs `:741`), if semantics allow.

- [ ] **Step 2: Commit**

```bash
git add docs/perf/2026-07-10-gtfsrt-matching-findings.md
git commit -m "Add GTFS-RT matching perf findings and ranked fix list"
```

- [ ] **Step 3: Update .gitignore if any scratchpad path leaked**

Run: `git status --porcelain`
Expected: no `.pb`, `.zip`, bundle, or flamegraph artifacts staged. If any appear, add them to `.gitignore` and unstage.

---

## Self-Review Notes

- **Spec coverage:** Component 1 → Task 3; Component 2 → Tasks 1,2,4,5; Component 2b (read-path) → deferred/optional, noted in report §2 verdict (acceptable — spec marks it secondary); Component 3 → Task 6; Components 4-5 → Task 7; Component 6 → Task 8. Golden safety net (spec §5) → Task 6.
- **Known executor decision points (not placeholders — real "read the neighbor and match it" steps):** exact `GtfsRealtimeSource` setter names (Task 2 Step 4), async-profiler jar path/layout (Task 5), and porting the `GtfsRealtimeTripLibraryTest` fixture (Task 6 Step 4). Each step says explicitly what to read and how to adjust.
- **Non-goal guard:** no task edits matching-engine source; the seed fix list in Task 8 is described, not implemented.
