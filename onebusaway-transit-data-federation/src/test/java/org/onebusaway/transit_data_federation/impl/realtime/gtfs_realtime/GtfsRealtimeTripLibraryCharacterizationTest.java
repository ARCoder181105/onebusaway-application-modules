/**
 * Copyright (C) 2024 OneBusAway
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
package org.onebusaway.transit_data_federation.impl.realtime.gtfs_realtime;

import static org.junit.Assert.assertEquals;
import static org.onebusaway.transit_data_federation.testing.UnitTestingSupport.block;
import static org.onebusaway.transit_data_federation.testing.UnitTestingSupport.blockConfiguration;
import static org.onebusaway.transit_data_federation.testing.UnitTestingSupport.serviceIds;
import static org.onebusaway.transit_data_federation.testing.UnitTestingSupport.stop;
import static org.onebusaway.transit_data_federation.testing.UnitTestingSupport.stopTime;
import static org.onebusaway.transit_data_federation.testing.UnitTestingSupport.time;
import static org.onebusaway.transit_data_federation.testing.UnitTestingSupport.trip;

import com.google.transit.realtime.GtfsRealtime.FeedEntity;
import com.google.transit.realtime.GtfsRealtime.FeedHeader;
import com.google.transit.realtime.GtfsRealtime.FeedMessage;
import com.google.transit.realtime.GtfsRealtime.TripDescriptor;
import com.google.transit.realtime.GtfsRealtime.TripUpdate;
import com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeEvent;
import com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.onebusaway.api.model.transit.realtime.GtfsRealtimeConstantsV2;
import org.onebusaway.realtime.api.TimepointPredictionRecord;
import org.onebusaway.realtime.api.VehicleLocationRecord;
import org.onebusaway.transit_data_federation.impl.transit_graph.BlockEntryImpl;
import org.onebusaway.transit_data_federation.impl.transit_graph.StopEntryImpl;
import org.onebusaway.transit_data_federation.impl.transit_graph.TripEntryImpl;
import org.onebusaway.transit_data_federation.services.blocks.BlockCalendarService;
import org.onebusaway.transit_data_federation.services.blocks.BlockInstance;
import org.onebusaway.transit_data_federation.services.transit_graph.BlockConfigurationEntry;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.StringJoiner;

/**
 * Golden/characterization test for
 * {@link GtfsRealtimeTripLibrary#createVehicleLocationRecordForUpdate(CombinedTripUpdatesAndVehiclePosition)},
 * the matching hot path that the perf investigation identified as heavily
 * allocating (e.g. BlockStopTimeList.get() rebuilding objects per call).
 *
 * This test locks the observable output of that method (as a serialized,
 * committed golden string) so that future optimizations of the matching hot
 * path can be verified as behavior-preserving. It deliberately does not
 * assert on individual fields the way {@link GtfsRealtimeTripLibraryTest}
 * does -- instead it serializes the whole record and diffs it against a
 * fixture on disk.
 *
 * The fixture construction below mirrors
 * {@code GtfsRealtimeTripLibraryTest#testTprInterpolation_0()} exactly,
 * since that case exercises TPR interpolation across multiple stops
 * (producing more than one {@link TimepointPredictionRecord}), which is the
 * most representative "real matching" scenario among the existing tests.
 *
 * To regenerate the golden after an intentional behavior change, run:
 * <pre>
 * mvn -pl onebusaway-transit-data-federation test \
 *     -Dtest=GtfsRealtimeTripLibraryCharacterizationTest -Dupdate.golden=true
 * </pre>
 */
public class GtfsRealtimeTripLibraryCharacterizationTest {

  private static final String GOLDEN =
      "org/onebusaway/transit_data_federation/impl/realtime/gtfs_realtime/characterization-golden.txt";

  private GtfsRealtimeTripLibrary _library;
  private GtfsRealtimeEntitySource _entitySource;
  private GtfsRealtimeServiceSource _serviceSource;
  private BlockCalendarService _blockCalendarService;

  @Before
  public void before() {
    _library = new GtfsRealtimeTripLibrary();
    _library.setCurrentTime(8 * 60 * 60 * 1000);
    _library.setValidateCurrentTime(false); // tell library it's a test
    _entitySource = Mockito.mock(GtfsRealtimeEntitySource.class);
    _serviceSource = Mockito.mock(GtfsRealtimeServiceSource.class);
    _blockCalendarService = Mockito.mock(BlockCalendarService.class);
    Mockito.when(_serviceSource.getBlockCalendarService()).thenReturn(_blockCalendarService);
    Mockito.when(_serviceSource.getBlockFinder()).thenReturn(new BlockFinder(_serviceSource));
    _serviceSource.setBlockCalendarService(_blockCalendarService);
    _library.setEntitySource(_entitySource);
    _library.setServiceSource(_serviceSource);

    _serviceSource.setBlockCalendarService(_blockCalendarService);
  }

  @Test
  public void createVehicleLocationRecord_matchesGolden() throws Exception {
    VehicleLocationRecord record = buildRecordUnderTest();

    String actual = serialize(record);
    Path goldenSourcePath = Paths.get("src/test/resources", GOLDEN);
    if (Boolean.getBoolean("update.golden")) {
      Files.createDirectories(goldenSourcePath.getParent());
      Files.write(goldenSourcePath, actual.getBytes());
    }
    // Read from the module-relative source path rather than the test
    // classpath: when -Dupdate.golden=true just wrote the file above, the
    // classpath (target/test-classes) won't have picked it up yet within
    // the same Maven test-resources phase.
    String expected = new String(Files.readAllBytes(goldenSourcePath));
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
          .sorted(Comparator.comparingInt(TimepointPredictionRecord::getStopSequence))
          .forEach(t -> sj.add("tpr timepointId=" + t.getTimepointId()
              + " tripId=" + t.getTripId()
              + " seq=" + t.getStopSequence()
              + " arr=" + t.getTimepointPredictedArrivalTime()
              + " dep=" + t.getTimepointPredictedDepartureTime()));
    }
    return sj.toString();
  }

  /**
   * Ported verbatim (construction-wise) from
   * {@code GtfsRealtimeTripLibraryTest#testTprInterpolation_0()}: current
   * time 7:31, a two-stop trip (stopA @ 7:30, stopB @ 7:40), a stop time
   * update carrying a 180s departure delay at stopB, and an overall trip
   * delay of 120s. This produces two timepoint predictions: one interpolated
   * (stopA) and one directly from the feed (stopB), so the golden captures
   * real matching output, not an empty/degenerate record.
   */
  private VehicleLocationRecord buildRecordUnderTest() {
    _library.setCurrentTime(time(7, 31) * 1000);

    TripEntryImpl tripA = trip("tripA");
    stopTime(0, stop("stopA", 0, 0), tripA, time(7, 30), 0.0);
    stopTime(1, stop("stopB", 0, 0), tripA, time(7, 40), 10.0);
    BlockEntryImpl blockA = block("blockA");
    BlockConfigurationEntry blockConfigA = blockConfiguration(blockA,
        serviceIds("s1"), tripA);
    BlockInstance blockInstanceA = new BlockInstance(blockConfigA, 0L);

    StopTimeUpdate.Builder stopTimeUpdate = stopTimeUpdateWithDepartureDelay("stopB", 180);
    TripUpdate.Builder tripUpdate = tripUpdate("tripA", "07:30:00",
        _library.getCurrentTime() / 1000, 120, stopTimeUpdate);

    Mockito.when(_entitySource.getTrip("tripA")).thenReturn(tripA);

    Mockito.when(
        _serviceSource.getBlockCalendarService().getActiveBlocks(Mockito.eq(blockA.getId()),
            Mockito.anyLong(), Mockito.anyLong())).thenReturn(Arrays.asList(blockInstanceA));
    Mockito.when(
        _serviceSource.getBlockCalendarService().getBlockInstance(Mockito.eq(tripA.getBlock().getId()),
            Mockito.anyLong())).thenReturn(blockInstanceA);

    return vehicleLocationRecord(tripUpdate);
  }

  // --- helpers ported from GtfsRealtimeTripLibraryTest ---

  private static FeedMessage.Builder createFeed() {
    FeedMessage.Builder builder = FeedMessage.newBuilder();
    FeedHeader.Builder header = FeedHeader.newBuilder();
    header.setGtfsRealtimeVersion(GtfsRealtimeConstantsV2.VERSION);
    builder.setHeader(header);
    return builder;
  }

  private static FeedEntity feed(TripUpdate.Builder tripUpdate) {
    FeedEntity.Builder tripUpdateEntity = FeedEntity.newBuilder();
    tripUpdateEntity.setId(tripUpdate.getTrip().getTripId());
    tripUpdateEntity.setTripUpdate(tripUpdate);
    return tripUpdateEntity.build();
  }

  private static StopTimeUpdate.Builder stopTimeUpdateWithDepartureDelay(String stopId, int delay) {
    StopTimeUpdate.Builder stopTimeUpdate = StopTimeUpdate.newBuilder();
    stopTimeUpdate.setStopId(stopId);
    StopTimeEvent.Builder stopTimeEvent = StopTimeEvent.newBuilder();
    stopTimeEvent.setDelay(delay);
    stopTimeUpdate.setDeparture(stopTimeEvent);
    return stopTimeUpdate;
  }

  private static TripUpdate.Builder tripUpdate(String tripId, String startTime, long timestamp, int delay,
      StopTimeUpdate.Builder... stopTimeUpdates) {
    TripUpdate.Builder tu = TripUpdate.newBuilder()
        .setTrip(
            TripDescriptor.newBuilder()
                .setTripId(tripId)
                .setStartTime(startTime)
        )
        .setDelay(delay)
        .setTimestamp(timestamp);

    for (StopTimeUpdate.Builder stu : stopTimeUpdates) {
      tu.addStopTimeUpdate(stu);
    }

    return tu;
  }

  private VehicleLocationRecord vehicleLocationRecord(TripUpdate.Builder... tripUpdates) {
    FeedMessage.Builder TU = createFeed();

    for (TripUpdate.Builder tu : tripUpdates) {
      TU.addEntity(feed(tu));
    }

    FeedMessage.Builder VP = createFeed();

    List<CombinedTripUpdatesAndVehiclePosition> updates =
        _library.groupTripUpdatesAndVehiclePositions(TU.build(), VP.build());

    CombinedTripUpdatesAndVehiclePosition update = updates.get(0);
    return _library.createVehicleLocationRecordForUpdate(update);
  }
}
