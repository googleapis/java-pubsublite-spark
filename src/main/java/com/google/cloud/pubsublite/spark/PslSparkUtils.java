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
import static scala.collection.JavaConverters.asScalaBufferConverter;

import com.google.cloud.pubsublite.Message;
import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.SequencedMessage;
import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.internal.CursorClient;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.math.LongMath;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.Timestamps;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.ByteArray;
import org.apache.spark.unsafe.types.UTF8String;
import scala.compat.java8.functionConverterImpls.FromJavaBiConsumer;

public class PslSparkUtils {
  @VisibleForTesting
  public static ArrayBasedMapData convertAttributesToSparkMap(
      ListMultimap<String, ByteString> attributeMap) {

    List<UTF8String> keyList = new ArrayList<>();
    List<GenericArrayData> valueList = new ArrayList<>();

    attributeMap
        .asMap()
        .forEach(
            (key, value) -> {
              keyList.add(UTF8String.fromString(key));
              List<byte[]> attributeVals =
                  value.stream()
                      .map(v -> ByteArray.concat(v.toByteArray()))
                      .collect(Collectors.toList());
              valueList.add(new GenericArrayData(asScalaBufferConverter(attributeVals).asScala()));
            });

    return new ArrayBasedMapData(
        new GenericArrayData(asScalaBufferConverter(keyList).asScala()),
        new GenericArrayData(asScalaBufferConverter(valueList).asScala()));
  }

  public static InternalRow toInternalRow(
      SequencedMessage msg, SubscriptionPath subscription, Partition partition) {
    List<Object> list =
        new ArrayList<>(
            Arrays.asList(
                UTF8String.fromString(subscription.toString()),
                partition.value(),
                msg.offset().value(),
                ByteArray.concat(msg.message().key().toByteArray()),
                ByteArray.concat(msg.message().data().toByteArray()),
                Timestamps.toMicros(msg.publishTime()),
                msg.message().eventTime().isPresent()
                    ? Timestamps.toMicros(msg.message().eventTime().get())
                    : null,
                convertAttributesToSparkMap(msg.message().attributes())));
    return InternalRow.apply(asScalaBufferConverter(list).asScala());
  }

  private static void extractVal(
      StructType inputSchema,
      InternalRow row,
      String fieldName,
      DataType expectedDataType,
      Consumer<Object> consumer) {
    if (!inputSchema.getFieldIndex(fieldName).isEmpty()) {
      Integer idx = (Integer) inputSchema.getFieldIndex(fieldName).get();
      try {
        consumer.accept(row.get(idx, expectedDataType));
      } catch (ClassCastException e) {
        // This means the field has a wrong class type.
      }
    }
  }

  public static Message toPubSubMessage(StructType inputSchema, InternalRow row) {
    Message.Builder builder = Message.builder();
    extractVal(
        inputSchema,
        row,
        "key",
        DataTypes.BinaryType,
        o -> builder.setKey(ByteString.copyFrom((byte[]) o)));
    extractVal(
        inputSchema,
        row,
        "data",
        DataTypes.BinaryType,
        o -> builder.setData(ByteString.copyFrom((byte[]) o)));
    extractVal(
        inputSchema,
        row,
        "event_timestamp",
        DataTypes.TimestampType,
        o -> builder.setEventTime(Timestamps.fromMicros((long) o)));
    extractVal(
        inputSchema,
        row,
        "attributes",
        SparkStructs.ATTRIBUTES_DATATYPE,
        o -> {
          MapData mapData = (MapData) o;
          ListMultimap<String, ByteString> attributeMap = ArrayListMultimap.create();
          mapData.foreach(
              DataTypes.StringType,
              SparkStructs.ATTRIBUTES_PER_KEY_DATATYPE,
              new FromJavaBiConsumer<>(
                  (k, v) -> {
                    String key = ((UTF8String) k).toString();
                    ArrayData values = (ArrayData) v;
                    values.foreach(
                        DataTypes.BinaryType,
                        new FromJavaBiConsumer<>(
                            (idx, a) -> {
                              attributeMap.put(key, ByteString.copyFrom((byte[]) a));
                            }));
                  }));
          builder.setAttributes(ImmutableListMultimap.copyOf(attributeMap));
        });
    return builder.build();
  }

  public static SparkSourceOffset toSparkSourceOffset(PslSourceOffset pslSourceOffset) {
    return new SparkSourceOffset(
        pslSourceOffset.partitionOffsetMap().entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    e ->
                        SparkPartitionOffset.builder()
                            .partition(Partition.of(e.getKey().value()))
                            .offset(e.getValue().value() - 1)
                            .build())));
  }

  public static PslSourceOffset toPslSourceOffset(SparkSourceOffset sparkSourceOffset) {
    long partitionCount = sparkSourceOffset.getPartitionOffsetMap().size();
    Map<Partition, Offset> pslSourceOffsetMap = new HashMap<>();
    for (long i = 0; i < partitionCount; i++) {
      Partition p = Partition.of(i);
      checkArgument(sparkSourceOffset.getPartitionOffsetMap().containsKey(p));
      pslSourceOffsetMap.put(
          p, Offset.of(sparkSourceOffset.getPartitionOffsetMap().get(p).offset() + 1));
    }
    return PslSourceOffset.builder().partitionOffsetMap(pslSourceOffsetMap).build();
  }

  public static PslPartitionOffset toPslPartitionOffset(SparkPartitionOffset sparkPartitionOffset) {
    return PslPartitionOffset.builder()
        .partition(sparkPartitionOffset.partition())
        .offset(Offset.of(sparkPartitionOffset.offset() + 1))
        .build();
  }

  public static SparkSourceOffset getSparkStartOffset(
      CursorClient cursorClient, SubscriptionPath subscriptionPath, long topicPartitionCount) {
    try {
      Map<Partition, com.google.cloud.pubsublite.Offset> pslSourceOffsetMap = new HashMap<>();
      for (int i = 0; i < topicPartitionCount; i++) {
        pslSourceOffsetMap.put(Partition.of(i), com.google.cloud.pubsublite.Offset.of(0));
      }
      cursorClient
          .listPartitionCursors(subscriptionPath)
          .get()
          .forEach(pslSourceOffsetMap::replace);
      return PslSparkUtils.toSparkSourceOffset(
          PslSourceOffset.builder().partitionOffsetMap(pslSourceOffsetMap).build());
    } catch (InterruptedException | ExecutionException e) {
      throw new IllegalStateException(
          "Failed to get information from PSL and construct startOffset", e);
    }
  }

  // EndOffset = min(startOffset + batchOffsetRange, headOffset)
  public static SparkSourceOffset getSparkEndOffset(
      SparkSourceOffset headOffset,
      SparkSourceOffset startOffset,
      long maxMessagesPerBatch,
      long topicPartitionCount) {
    Map<Partition, SparkPartitionOffset> map = new HashMap<>();
    for (int i = 0; i < topicPartitionCount; i++) {
      Partition p = Partition.of(i);
      SparkPartitionOffset emptyPartition = SparkPartitionOffset.create(p, -1L);
      long head = headOffset.getPartitionOffsetMap().getOrDefault(p, emptyPartition).offset();
      long start = startOffset.getPartitionOffsetMap().getOrDefault(p, emptyPartition).offset();
      map.put(
          p,
          SparkPartitionOffset.create(
              p, Math.min(LongMath.saturatedAdd(start, maxMessagesPerBatch), head)));
    }
    return new SparkSourceOffset(map);
  }
}
