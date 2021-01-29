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

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.pubsublite.Message;
import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.SequencedMessage;
import com.google.cloud.pubsublite.internal.testing.UnitTestExamples;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import java.nio.charset.StandardCharsets;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.types.DataTypes;
import org.junit.Test;

public class PslSparkUtilsTest {

  @Test
  public void testToInternalRow() {
    Timestamp publishTimestamp = Timestamp.newBuilder().setSeconds(20000000L).setNanos(20).build();
    Timestamp eventTimestamp = Timestamp.newBuilder().setSeconds(10000000L).setNanos(10).build();
    Message message =
        Message.builder()
            .setKey(ByteString.copyFromUtf8("key"))
            .setData(ByteString.copyFromUtf8("data"))
            .setEventTime(eventTimestamp)
            .setAttributes(
                ImmutableListMultimap.of(
                    "key1", ByteString.copyFromUtf8("val1"),
                    "key1", ByteString.copyFromUtf8("val2"),
                    "key2", ByteString.copyFromUtf8("val3")))
            .build();
    SequencedMessage sequencedMessage =
        SequencedMessage.of(message, publishTimestamp, UnitTestExamples.exampleOffset(), 10L);
    InternalRow row =
        PslSparkUtils.toInternalRow(
            sequencedMessage,
            UnitTestExamples.exampleSubscriptionPath(),
            UnitTestExamples.examplePartition());
    assertThat(row.getString(0)).isEqualTo(UnitTestExamples.exampleSubscriptionPath().toString());
    assertThat(row.getLong(1)).isEqualTo(UnitTestExamples.examplePartition().value());
    assertThat(row.getLong(2)).isEqualTo(UnitTestExamples.exampleOffset().value());
    assertThat(row.getBinary(3)).isEqualTo("key".getBytes(StandardCharsets.UTF_8));
    assertThat(row.getBinary(4)).isEqualTo("data".getBytes(StandardCharsets.UTF_8));
    assertThat(row.getLong(5)).isEqualTo(Timestamps.toMicros(publishTimestamp));
    assertThat(row.getLong(6)).isEqualTo(Timestamps.toMicros(eventTimestamp));
    ArrayData keys = row.getMap(7).keyArray();
    ArrayData values = row.getMap(7).valueArray();
    assertThat(keys.get(0, DataTypes.StringType).toString()).isEqualTo("key1");
    assertThat(keys.get(1, DataTypes.StringType).toString()).isEqualTo("key2");
    GenericArrayData valueOfKey1 =
        (GenericArrayData) values.get(0, DataTypes.createArrayType(DataTypes.BinaryType));
    GenericArrayData valueOfKey2 =
        (GenericArrayData) values.get(1, DataTypes.createArrayType(DataTypes.BinaryType));
    assertThat(valueOfKey1.getBinary(0)).isEqualTo("val1".getBytes(StandardCharsets.UTF_8));
    assertThat(valueOfKey1.getBinary(1)).isEqualTo("val2".getBytes(StandardCharsets.UTF_8));
    assertThat(valueOfKey2.getBinary(0)).isEqualTo("val3".getBytes(StandardCharsets.UTF_8));
  }

  @Test
  public void testSourceOffsetConversion() {
    PslSourceOffset pslSourceOffset =
        PslSourceOffset.builder()
            .partitionOffsetMap(
                ImmutableMap.of(Partition.of(0L), Offset.of(10), Partition.of(1L), Offset.of(50)))
            .build();

    SparkSourceOffset sparkSourceOffset = PslSparkUtils.toSparkSourceOffset(pslSourceOffset);
    assertThat(sparkSourceOffset.getPartitionOffsetMap().get(Partition.of(0L)).offset())
        .isEqualTo(9L);
    assertThat(sparkSourceOffset.getPartitionOffsetMap().get(Partition.of(1L)).offset())
        .isEqualTo(49L);

    assertThat(PslSparkUtils.toPslSourceOffset(sparkSourceOffset)).isEqualTo(pslSourceOffset);
  }

  @Test
  public void testToPslPartitionOffset() {
    SparkPartitionOffset sparkPartitionOffset =
        SparkPartitionOffset.builder().partition(Partition.of(1L)).offset(10L).build();
    PslPartitionOffset pslPartitionOffset =
        PslPartitionOffset.builder().partition(Partition.of(1L)).offset(Offset.of(11L)).build();
    assertThat(PslSparkUtils.toPslPartitionOffset(sparkPartitionOffset))
        .isEqualTo(pslPartitionOffset);
  }
}
