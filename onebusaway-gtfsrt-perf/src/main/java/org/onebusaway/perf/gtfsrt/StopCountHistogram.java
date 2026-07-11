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

import org.onebusaway.transit_data_federation.services.transit_graph.TransitGraphDao;
import org.onebusaway.transit_data_federation.services.transit_graph.TripEntry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;

/**
 * Quantifies stops-per-trip for the loaded bundle, to correlate against the
 * CPU/allocation flamegraph hotspot in
 * {@code BlockConfigurationEntryImpl$BlockStopTimeList.get()} (see task 5's
 * report): if that method is called roughly once per stop-time per trip per
 * refresh, a large stops-per-trip count is direct evidence for an O(stops^2)
 * (or at least O(stops)-per-call-with-large-constant) matching cost.
 *
 * <p>Loads a real federation Spring context via {@link PerfBundleHarness},
 * then walks every {@link TripEntry} in the {@link TransitGraphDao}, bucketing
 * trips by stop-time count (in buckets of 10) and reporting min/median/max
 * along with the count of trips whose stop-time count exceeds notable
 * thresholds.
 */
public class StopCountHistogram {

  public static void main(String[] args) throws Exception {
    String bundleRoot = System.getProperty("perf.bundleRoot");
    if (bundleRoot == null || bundleRoot.isEmpty()) {
      System.err.println("usage: -Dperf.bundleRoot=<path to bundle root>");
      System.exit(1);
    }

    // open() with refreshInterval=0 never schedules or performs a feed fetch
    // (see GtfsRealtimeSource.start(), which only schedules a RefreshTask when
    // _refreshInterval > 0), so a harmless placeholder trip-updates URL is
    // safe here -- we only need the wired TransitGraphDao, not a live feed.
    PerfBundleHarness h = new PerfBundleHarness(bundleRoot, "file:///dev/null", null, null);
    h.open();
    try {
      TransitGraphDao dao = h.getContext().getBean(TransitGraphDao.class);

      TreeMap<Integer, Integer> hist = new TreeMap<>();
      List<Integer> stopCounts = new ArrayList<>();
      int n = 0;
      long sum = 0;
      int min = Integer.MAX_VALUE;
      int max = Integer.MIN_VALUE;
      int over50 = 0;
      int over100 = 0;

      for (TripEntry t : dao.getAllTrips()) {
        int s = t.getStopTimes().size();
        hist.merge(s / 10 * 10, 1, Integer::sum);
        stopCounts.add(s);
        n++;
        sum += s;
        if (s < min) min = s;
        if (s > max) max = s;
        if (s > 50) over50++;
        if (s > 100) over100++;
      }

      Collections.sort(stopCounts);
      int median = n == 0 ? 0 : stopCounts.get(n / 2);
      long avg = n == 0 ? 0 : sum / n;

      System.out.println("trips=" + n + " avgStops=" + avg
          + " minStops=" + (n == 0 ? 0 : min)
          + " medianStops=" + median
          + " maxStops=" + (n == 0 ? 0 : max));
      System.out.println("tripsOver50Stops=" + over50 + " tripsOver100Stops=" + over100);
      System.out.println("--- histogram (bucketed by 10 stops) ---");
      hist.forEach((bucket, count) ->
          System.out.println("stops[" + bucket + "-" + (bucket + 9) + "]=" + count));
    } finally {
      h.close();
    }
  }
}
