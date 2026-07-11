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

import org.onebusaway.transit_data_federation.bundle.FederatedTransitDataBundleConventionMain;

import java.io.File;
import java.io.FileWriter;

/**
 * Builds a bundle from an already-unzipped GTFS directory. Run once.
 * Args: {@code <gtfsInputDir> <bundleOutputDir> <name>}
 */
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
