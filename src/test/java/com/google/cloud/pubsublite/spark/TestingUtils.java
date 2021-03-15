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
