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

import static com.google.cloud.pubsublite.internal.testing.UnitTestExamples.example;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode.Code;
import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.internal.CheckedApiException;
import com.google.cloud.pubsublite.internal.CursorClient;
import com.google.cloud.pubsublite.spark.internal.MultiPartitionCommitter;
import com.google.cloud.pubsublite.spark.internal.PartitionCountReader;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;

public class BaseDataStreamTest {

  private static final ApiException UNCHECKED = new CheckedApiException(Code.INTERNAL).underlying;

  private final CursorClient cursorClient = mock(CursorClient.class);
  private final MultiPartitionCommitter committer = mock(MultiPartitionCommitter.class);
  private final PartitionCountReader countReader = mock(PartitionCountReader.class);
  private final BaseDataStream stream =
      new BaseDataStream(cursorClient, committer, countReader, example(SubscriptionPath.class));

  @Test
  public void testReadCountFailure() {
    when(countReader.getPartitionCount()).thenThrow(UNCHECKED);
    assertThrows(ApiException.class, stream::initialOffset);
  }

  @Test
  public void testListOffsetsFailure() {
    when(countReader.getPartitionCount()).thenReturn(2);
    when(cursorClient.listPartitionCursors(example(SubscriptionPath.class)))
        .thenReturn(ApiFutures.immediateFailedFuture(UNCHECKED));
    assertThrows(IllegalStateException.class, stream::initialOffset);
  }

  @Test
  public void testInitialOffsetSuccess() {
    when(countReader.getPartitionCount()).thenReturn(2);
    when(cursorClient.listPartitionCursors(example(SubscriptionPath.class)))
        .thenReturn(
            ApiFutures.immediateFuture(
                ImmutableMap.of(Partition.of(0), Offset.of(10), Partition.of(2), Offset.of(30))));
    PslSourceOffset offset = PslSparkUtils.toPslSourceOffset(stream.initialOffset());
    // Missing offset for partition 1 set to 0.
    // Extra offset for partition 2 not added.
    assertThat(offset.partitionOffsetMap())
        .containsExactlyEntriesIn(
            ImmutableMap.of(Partition.of(0), Offset.of(10), Partition.of(1), Offset.of(0)));
  }
}
