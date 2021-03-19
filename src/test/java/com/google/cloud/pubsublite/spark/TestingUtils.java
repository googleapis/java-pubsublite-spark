/*
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.pubsublite.spark;

import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.Partition;
import java.util.HashMap;
import java.util.Map;

public class TestingUtils {
  public static PslSourceOffset createPslSourceOffset(long... offsets) {
    Map<Partition, Offset> map = new HashMap<>();
    int idx = 0;
    for (long offset : offsets) {
      map.put(Partition.of(idx++), Offset.of(offset));
    }
    return PslSourceOffset.builder().partitionOffsetMap(map).build();
  }

  public static SparkSourceOffset createSparkSourceOffset(long... offsets) {
    Map<Partition, SparkPartitionOffset> map = new HashMap<>();
    int idx = 0;
    for (long offset : offsets) {
      map.put(Partition.of(idx), SparkPartitionOffset.create(Partition.of(idx), offset));
      idx++;
    }
    return new SparkSourceOffset(map);
  }
}
