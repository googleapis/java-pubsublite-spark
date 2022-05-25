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
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.mock;

import com.google.cloud.pubsublite.*;
import com.google.cloud.pubsublite.internal.CursorClient;
import com.google.cloud.pubsublite.internal.testing.UnitTestExamples;
import com.google.cloud.pubsublite.spark.internal.MultiPartitionCommitter;
import com.google.cloud.pubsublite.spark.internal.PartitionCountReader;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;

public class PslContinuousStreamTest {

  private static final PslReadDataSourceOptions OPTIONS =
      PslReadDataSourceOptions.builder()
          .setSubscriptionPath(UnitTestExamples.exampleSubscriptionPath())
          .build();
  private final CursorClient cursorClient = mock(CursorClient.class);
  private final MultiPartitionCommitter committer = mock(MultiPartitionCommitter.class);
  private final PartitionCountReader partitionCountReader = mock(PartitionCountReader.class);

  {
    when(partitionCountReader.getPartitionCount()).thenReturn(2);
  }

  private final PslContinuousStream stream =
      new PslContinuousStream(
          cursorClient,
          committer,
          UnitTestExamples.exampleSubscriptionPath(),
          partitionCountReader,
          OPTIONS);

  @Test
  public void testMergeOffsets() {
    SparkPartitionOffset po1 =
        SparkPartitionOffset.builder().partition(Partition.of(1L)).offset(10L).build();
    SparkPartitionOffset po2 =
        SparkPartitionOffset.builder().partition(Partition.of(2L)).offset(5L).build();
    assertThat(stream.mergeOffsets(new SparkPartitionOffset[] {po1, po2}))
        .isEqualTo(SparkSourceOffset.merge(new SparkPartitionOffset[] {po1, po2}));
  }

  @Test
  public void testDeserializeOffset() {
    SparkSourceOffset offset =
        new SparkSourceOffset(
            ImmutableMap.of(
                Partition.of(1L),
                SparkPartitionOffset.builder().partition(Partition.of(1L)).offset(10L).build()));
    assertThat(stream.deserializeOffset(offset.json())).isEqualTo(offset);
  }

  @Test
  public void testCommit() {
    SparkSourceOffset offset =
        new SparkSourceOffset(
            ImmutableMap.of(
                Partition.of(0L),
                    SparkPartitionOffset.builder().partition(Partition.of(0L)).offset(10L).build(),
                Partition.of(1L),
                    SparkPartitionOffset.builder()
                        .partition(Partition.of(1L))
                        .offset(50L)
                        .build()));
    PslSourceOffset expectedCommitOffset =
        PslSourceOffset.builder()
            .partitionOffsetMap(
                ImmutableMap.of(
                    Partition.of(0L), Offset.of(11L),
                    Partition.of(1L), Offset.of(51L)))
            .build();
    stream.commit(offset);
    verify(committer, times(1)).commit(eq(expectedCommitOffset));
  }

  @Test
  public void testPartitionIncrease() {
    when(partitionCountReader.getPartitionCount()).thenReturn(4);
    assertThat(stream.needsReconfiguration()).isTrue();
  }
}
