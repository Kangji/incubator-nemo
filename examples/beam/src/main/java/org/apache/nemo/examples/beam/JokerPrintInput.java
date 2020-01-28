/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nemo.examples.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.util.Vector;

/**
 * Print input data.
 */
public final class JokerPrintInput {
  /**
   * Private Constructor.
   */
  private JokerPrintInput() {
  }

  /**
   * Main function.
   *
   * @param args arguments.
   */
  public static void main(final String[] args) {
    final String inputFilePath = args[0];
    final int chainLength = Integer.parseInt(args[1]);
    final boolean isNative = Boolean.parseBoolean(args[2]);

    final PipelineOptions options = NemoPipelineOptionsFactory.create();
    options.setJobName("WordCount");

    final Pipeline p = Pipeline.create(options);
    PCollection<String> data = GenericSourceSink.read(p, inputFilePath);

    for (int i = 0; i < chainLength; i++) {
      data = data.apply(MapElements.<String, String>via(new SimpleFunction<String, String>() {
        @Override
        public String apply(final String line) {
          if (isNative) {
            try {
              // Thread.sleep(1000 * 60);

              System.loadLibrary("NativeFunctions");

              final NativeFunctions nfs = new NativeFunctions();


              // System.loadLibrary("VUVUVUVU"); <-- this fails, which means the above line works

              /*
              java.lang.reflect.Field LIBRARIES;

              LIBRARIES = ClassLoader.class.getDeclaredField("loadedLibraryNames");
              LIBRARIES.setAccessible(true);


              final Vector<String> libraries = (Vector<String>) LIBRARIES.get(ClassLoader.getSystemClassLoader());
              final String[] libraryStrings = libraries.toArray(new String[] {});

              for (String str : libraryStrings) {
                System.out.println(str);
              }

              System.out.println("NOW CONCAT");
               */

              return nfs.concatNatively(line);
            } catch (Exception e) {
              throw new RuntimeException(e);
            }

          } else {
            return line + "a";
          }
        }
      }));
    }

    data.apply(MapElements.<String, String>via(new SimpleFunction<String, String>() {
      @Override
      public String apply(final String line) {
        System.out.println(isNative + ": " + line);
        return line;
      }
    }));

    p.run();
  }
}
