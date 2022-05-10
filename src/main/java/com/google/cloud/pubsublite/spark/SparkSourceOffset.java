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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.cloud.pubsublite.Partition;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Collectors;

public final class SparkSourceOffset extends org.apache.spark.sql.connector.read.streaming.Offset {
  private static final Gson gson = new Gson();

  // Using a map to ensure unique partitions.
  private final ImmutableMap<Partition, SparkPartitionOffset> partitionOffsetMap;

  public SparkSourceOffset(Map<Partition, SparkPartitionOffset> map) {
    validateMap(map);
    this.partitionOffsetMap = ImmutableMap.copyOf(map);
  }

  private static void validateMap(Map<Partition, SparkPartitionOffset> map) {
    map.forEach(
        (k, v) ->
            checkArgument(
                Objects.equals(k, v.partition()),
                "Key(Partition) and value(SparkPartitionOffset)'s partition don't match."));
  }

  public static SparkSourceOffset merge(SparkSourceOffset o1, SparkSourceOffset o2) {
    Map<Partition, SparkPartitionOffset> result = new HashMap<>(o1.partitionOffsetMap);
    o2.partitionOffsetMap.forEach(
        (k, v) ->
            result.merge(
                k,
                v,
                (v1, v2) ->
                    SparkPartitionOffset.builder()
                        .partition(Partition.of(k.value()))
                        .offset(Collections.max(ImmutableList.of(v1.offset(), v2.offset())))
                        .build()));
    return new SparkSourceOffset(result);
  }

  public static SparkSourceOffset merge(SparkPartitionOffset[] offsets) {
    Map<Partition, SparkPartitionOffset> map = new HashMap<>();
    for (SparkPartitionOffset po : offsets) {
      checkArgument(
          !map.containsKey(po.partition()), "Multiple PslPartitionOffset has same partition.");
      map.put(
          po.partition(),
          SparkPartitionOffset.builder().partition(po.partition()).offset(po.offset()).build());
    }
    return new SparkSourceOffset(map);
  }

  public static SparkSourceOffset fromJson(String json) {
    Map<Long, Long> map = gson.fromJson(json, new TypeToken<Map<Long, Long>>() {}.getType());
    Map<Partition, SparkPartitionOffset> partitionOffsetMap =
        map.entrySet().stream()
            .collect(
                Collectors.toMap(
                    e -> Partition.of(e.getKey()),
                    e ->
                        SparkPartitionOffset.builder()
                            .partition(Partition.of(e.getKey()))
                            .offset(e.getValue())
                            .build()));
    return new SparkSourceOffset(partitionOffsetMap);
  }

  public Map<Partition, SparkPartitionOffset> getPartitionOffsetMap() {
    return this.partitionOffsetMap;
  }

  @Override
  public String json() {
    Map<Long, Long> map =
        partitionOffsetMap.entrySet().stream()
            .collect(Collectors.toMap(e -> e.getKey().value(), e -> e.getValue().offset()));
    return gson.toJson(new TreeMap<>(map));
  }
}
