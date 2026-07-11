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
        // async-profiler requires the output file to be repeated on the stop
        // command; specifying it only on "start" is silently ignored ("No
        // dump options specified") and no flamegraph is written.
        String event = System.getProperty("perf.event", "cpu");
        execAsync("stop,file=" + out + "/" + event + "-flamegraph.html");
        System.out.println("async-profiler " + event + " written");
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
