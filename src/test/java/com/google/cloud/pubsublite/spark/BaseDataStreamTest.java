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
  private final BaseDataStream stream = new BaseDataStream(
      cursorClient, committer, countReader, example(SubscriptionPath.class));


  @Test
  public void testReadCountFailure() {
    when(countReader.getPartitionCount()).thenThrow(UNCHECKED);
    assertThrows(ApiException.class, stream::initialOffset);
  }

  @Test
  public void testListOffsetsFailure() {
    when(countReader.getPartitionCount()).thenReturn(2);
    when(cursorClient.listPartitionCursors(example(SubscriptionPath.class))).thenReturn(
        ApiFutures.immediateFailedFuture(UNCHECKED));
    assertThrows(ApiException.class, stream::initialOffset);
  }

  @Test
  public void testInitialOffsetSuccess() {
    when(countReader.getPartitionCount()).thenReturn(2);
    when(cursorClient.listPartitionCursors(example(SubscriptionPath.class))).thenReturn(
        ApiFutures.immediateFuture(
            ImmutableMap.of(Partition.of(0), Offset.of(10), Partition.of(2), Offset.of(30))));
    SparkSourceOffset offset = stream.initialOffset();
    // Missing offset for partition 1 set to 0.
    // Extra offset for partition 2 added even though it was not in the count.
    assertThat(offset.getPartitionOffsetMap()).containsExactlyEntriesIn(
        ImmutableMap.of(Partition.of(0), Offset.of(10), Partition.of(1), Offset.of(0),
            Partition.of(2), Offset.of(30)));
  }
}
