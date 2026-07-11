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

/**
 * Stands up a real Spring federation context and a fully-wired
 * {@link GtfsRealtimeSource}, replicating the wiring used by
 * {@code BundleLoader} and {@code AbstractGtfsRealtimeIntegrationTest} in
 * onebusaway-gtfsrt-integration-tests, so that {@code source.reset()} and
 * {@code source.refresh()} can be driven synchronously in a perf loop.
 */
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
