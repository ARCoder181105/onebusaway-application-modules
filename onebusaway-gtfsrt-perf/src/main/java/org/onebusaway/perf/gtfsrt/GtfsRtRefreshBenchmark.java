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
