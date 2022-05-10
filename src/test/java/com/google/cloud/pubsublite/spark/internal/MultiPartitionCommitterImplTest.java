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

package com.google.cloud.pubsublite.spark.internal;

import static com.google.cloud.pubsublite.internal.testing.UnitTestExamples.example;
import static com.google.cloud.pubsublite.spark.TestingUtils.createPslSourceOffset;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.*;

import com.google.api.core.ApiFutures;
import com.google.api.core.SettableApiFuture;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode.Code;
import com.google.cloud.pubsublite.*;
import com.google.cloud.pubsublite.internal.CheckedApiException;
import com.google.cloud.pubsublite.internal.CursorClient;
import com.google.cloud.pubsublite.spark.PslSourceOffset;
import org.junit.Test;

public class MultiPartitionCommitterImplTest {

  private final CursorClient client = mock(CursorClient.class);
  private final MultiPartitionCommitter committer = new MultiPartitionCommitterImpl(
      example(SubscriptionPath.class), client);

  @Test
  public void testCommit() {
    PslSourceOffset offset = createPslSourceOffset(10L, 8L);
    when(client.commitCursor(example(SubscriptionPath.class), Partition.of(0),
        Offset.of(10))).thenReturn(
        ApiFutures.immediateFuture(null));
    when(client.commitCursor(example(SubscriptionPath.class), Partition.of(1),
        Offset.of(8))).thenReturn(ApiFutures.immediateFuture(null));
    committer.commit(offset);
    verify(client).commitCursor(example(SubscriptionPath.class), Partition.of(0), Offset.of(10));
    verify(client).commitCursor(example(SubscriptionPath.class), Partition.of(1), Offset.of(8));
  }

  @Test
  public void testCommitFailure() {
    PslSourceOffset offset = createPslSourceOffset(10L, 8L);
    when(client.commitCursor(example(SubscriptionPath.class), Partition.of(0),
        Offset.of(10))).thenReturn(
        ApiFutures.immediateFailedFuture(new CheckedApiException(
            Code.INTERNAL)));
    when(client.commitCursor(example(SubscriptionPath.class), Partition.of(1),
        Offset.of(8))).thenReturn(ApiFutures.immediateFuture(null));
    assertThrows(ApiException.class, () -> committer.commit(offset));
    verify(client).commitCursor(example(SubscriptionPath.class), Partition.of(0), Offset.of(10));
    verify(client).commitCursor(example(SubscriptionPath.class), Partition.of(1), Offset.of(8));
  }

  @Test
  public void testClose() {
    committer.close();
    verify(committer).close();
  }
}
