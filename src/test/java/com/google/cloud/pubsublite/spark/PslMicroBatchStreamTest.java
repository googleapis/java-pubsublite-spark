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

import static com.google.cloud.pubsublite.spark.TestingUtils.createPslSourceOffset;
import static com.google.cloud.pubsublite.spark.TestingUtils.createSparkSourceOffset;
import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.times;

import com.google.api.core.ApiFutures;
import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.internal.CursorClient;
import com.google.cloud.pubsublite.internal.testing.UnitTestExamples;
import com.google.cloud.pubsublite.spark.internal.MultiPartitionCommitter;
import com.google.cloud.pubsublite.spark.internal.PartitionCountReader;
import com.google.cloud.pubsublite.spark.internal.PartitionSubscriberFactory;
import com.google.cloud.pubsublite.spark.internal.PerTopicHeadOffsetReader;
import com.google.common.collect.ImmutableMap;
import java.util.Optional;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.junit.Test;

public class PslMicroBatchStreamTest {
  private static final PslReadDataSourceOptions OPTIONS =
      PslReadDataSourceOptions.builder()
          .setSubscriptionPath(UnitTestExamples.exampleSubscriptionPath())
          .build();
  private final CursorClient cursorClient = mock(CursorClient.class);
  private final MultiPartitionCommitter committer = mock(MultiPartitionCommitter.class);
  private final PerTopicHeadOffsetReader headOffsetReader = mock(PerTopicHeadOffsetReader.class);
  private final PartitionCountReader partitionCountReader = mock(PartitionCountReader.class);
  private final MicroBatchStream stream =
      new PslMicroBatchStream(
          cursorClient,
          committer,
          headOffsetReader,
          UnitTestExamples.exampleSubscriptionPath(),
          partitionCountReader,
          OPTIONS);

  @Test
  public void testLatestOffset() {
    when(cursorClient.listPartitionCursors(UnitTestExamples.exampleSubscriptionPath()))
        .thenReturn(ApiFutures.immediateFuture(ImmutableMap.of()));
    when(headOffsetReader.getHeadOffset()).thenReturn(createPslSourceOffset(301L, 200L));
    assertThat(((SparkSourceOffset) stream.latestOffset()).getPartitionOffsetMap())
        .containsExactly(
            Partition.of(0L),
            SparkPartitionOffset.create(Partition.of(0L), 300L),
            Partition.of(1L),
            SparkPartitionOffset.create(Partition.of(1L), 199L));
  }

  @Test
  public void testPlanInputPartitionNoMessage() {
    SparkSourceOffset startOffset = createSparkSourceOffset(10L, 100L);
    SparkSourceOffset endOffset = createSparkSourceOffset(20L, 100L);
    assertThat(stream.planInputPartitions(startOffset, endOffset)).asList().hasSize(1);
  }

  @Test
  public void testPartitionIncreasedRetry() {
    SparkSourceOffset startOffset = createSparkSourceOffset(10L, 100L);
    SparkSourceOffset endOffset = createSparkSourceOffset(20L, 300L, 100L);
    assertThat(stream.planInputPartitions(startOffset, endOffset)).asList().hasSize(3);
  }

  @Test
  public void testPartitionIncreasedNewQuery() {
    SparkSourceOffset startOffset = createSparkSourceOffset(99L, -1L);
    SparkSourceOffset endOffset = createSparkSourceOffset(301L, 200L);
    assertThat(stream.planInputPartitions(startOffset, endOffset)).asList().hasSize(2);
  }
}
