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

import static com.google.cloud.pubsublite.spark.TestingUtils.createPslSourceOffset;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import com.google.api.core.SettableApiFuture;
import com.google.cloud.pubsublite.*;
import com.google.cloud.pubsublite.internal.wire.Committer;
import com.google.cloud.pubsublite.spark.PslSourceOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class MultiPartitionCommitterImplTest {

  private Runnable task;
  private List<Committer> committerList;

  private MultiPartitionCommitterImpl createCommitter(int initialPartitions, int available) {
    committerList = new ArrayList<>();
    for (int i = 0; i < available; i++) {
      Committer committer = mock(Committer.class);
      when(committer.startAsync())
          .thenReturn(committer)
          .thenThrow(new IllegalStateException("should only init once"));
      when(committer.commitOffset(eq(Offset.of(10L)))).thenReturn(SettableApiFuture.create());
      committerList.add(committer);
    }
    ScheduledExecutorService mockExecutor = mock(ScheduledExecutorService.class);
    ArgumentCaptor<Runnable> taskCaptor = ArgumentCaptor.forClass(Runnable.class);
    when(mockExecutor.scheduleWithFixedDelay(
            taskCaptor.capture(), anyLong(), anyLong(), any(TimeUnit.class)))
        .thenReturn(null);
    MultiPartitionCommitterImpl multiCommitter =
        new MultiPartitionCommitterImpl(
            initialPartitions, p -> committerList.get((int) p.value()), mockExecutor);
    task = taskCaptor.getValue();
    return multiCommitter;
  }

  private MultiPartitionCommitterImpl createCommitter(int initialPartitions) {
    return createCommitter(initialPartitions, initialPartitions);
  }

  @Test
  public void testCommit() {
    MultiPartitionCommitterImpl multiCommitter = createCommitter(2);

    verify(committerList.get(0)).startAsync();
    verify(committerList.get(1)).startAsync();

    PslSourceOffset offset = createPslSourceOffset(10L, 8L);
    SettableApiFuture<Void> future1 = SettableApiFuture.create();
    SettableApiFuture<Void> future2 = SettableApiFuture.create();
    when(committerList.get(0).commitOffset(eq(Offset.of(10L)))).thenReturn(future1);
    when(committerList.get(1).commitOffset(eq(Offset.of(8L)))).thenReturn(future2);
    multiCommitter.commit(offset);
    verify(committerList.get(0)).commitOffset(eq(Offset.of(10L)));
    verify(committerList.get(1)).commitOffset(eq(Offset.of(8L)));
  }

  @Test
  public void testClose() {
    MultiPartitionCommitterImpl multiCommitter = createCommitter(1);

    PslSourceOffset offset = createPslSourceOffset(10L);
    SettableApiFuture<Void> future1 = SettableApiFuture.create();
    when(committerList.get(0).commitOffset(eq(Offset.of(10L)))).thenReturn(future1);
    multiCommitter.commit(offset);
    when(committerList.get(0).stopAsync()).thenReturn(committerList.get(0));

    multiCommitter.close();
    verify(committerList.get(0)).stopAsync();
  }

  @Test
  public void testPartitionChange() {
    // Creates committer with 2 partitions
    MultiPartitionCommitterImpl multiCommitter = createCommitter(2, 4);
    for (int i = 0; i < 2; i++) {
      verify(committerList.get(i)).startAsync();
    }
    for (int i = 2; i < 4; i++) {
      verify(committerList.get(i), times(0)).startAsync();
    }

    // Partitions increased to 4.
    multiCommitter.commit(createPslSourceOffset(10L, 10L, 10L, 10L));
    for (int i = 0; i < 2; i++) {
      verify(committerList.get(i)).commitOffset(eq(Offset.of(10L)));
    }
    for (int i = 2; i < 4; i++) {
      verify(committerList.get(i)).startAsync();
      verify(committerList.get(i)).commitOffset(eq(Offset.of(10L)));
    }

    // Partitions decreased to 2
    multiCommitter.commit(createPslSourceOffset(10L, 10L));
    for (int i = 0; i < 2; i++) {
      verify(committerList.get(i), times(2)).commitOffset(eq(Offset.of(10L)));
    }
    task.run();
    for (int i = 2; i < 4; i++) {
      verify(committerList.get(i)).stopAsync();
    }
  }

  @Test
  public void testDelayedPartitionRemoval() {
    // Creates committer with 4 partitions, then decrease to 2, then increase to 3.
    MultiPartitionCommitterImpl multiCommitter = createCommitter(4);
    multiCommitter.commit(createPslSourceOffset(10L, 10L));
    multiCommitter.commit(createPslSourceOffset(10L, 10L, 10L));
    task.run();
    verify(committerList.get(2)).startAsync();
    verify(committerList.get(2), times(0)).stopAsync();
    verify(committerList.get(3)).startAsync();
    verify(committerList.get(3)).stopAsync();
  }
}
