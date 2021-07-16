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

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.internal.wire.Committer;
import com.google.cloud.pubsublite.spark.PslSourceOffset;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.flogger.GoogleLogger;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.GuardedBy;

/**
 * A {@link MultiPartitionCommitter} that lazily adjusts for partition changes when {@link
 * MultiPartitionCommitter#commit(PslSourceOffset)} is called.
 */
public class MultiPartitionCommitterImpl implements MultiPartitionCommitter {
  private static final GoogleLogger log = GoogleLogger.forEnclosingClass();

  private final CommitterFactory committerFactory;

  @GuardedBy("this")
  private final Map<Partition, Committer> committerMap = new HashMap<>();

  @GuardedBy("this")
  private final Set<Partition> partitionsCleanUp = new HashSet<>();

  public MultiPartitionCommitterImpl(long topicPartitionCount, CommitterFactory committerFactory) {
    this(
        topicPartitionCount,
        committerFactory,
        MoreExecutors.getExitingScheduledExecutorService(new ScheduledThreadPoolExecutor(1)));
  }

  @VisibleForTesting
  MultiPartitionCommitterImpl(
      long topicPartitionCount,
      CommitterFactory committerFactory,
      ScheduledExecutorService executorService) {
    this.committerFactory = committerFactory;
    for (int i = 0; i < topicPartitionCount; i++) {
      Partition p = Partition.of(i);
      committerMap.put(p, createCommitter(p));
    }
    executorService.scheduleWithFixedDelay(this::cleanUpCommitterMap, 10, 10, TimeUnit.MINUTES);
  }

  @Override
  public synchronized void close() {
    committerMap.values().forEach(c -> c.stopAsync().awaitTerminated());
  }

  /** Adjust committerMap based on the partitions that needs to be committed. */
  private synchronized void updateCommitterMap(PslSourceOffset offset) {
    int currentPartitions = committerMap.size();
    int newPartitions = offset.partitionOffsetMap().size();

    if (currentPartitions == newPartitions) {
      return;
    }
    if (currentPartitions < newPartitions) {
      for (int i = currentPartitions; i < newPartitions; i++) {
        Partition p = Partition.of(i);
        if (!committerMap.containsKey(p)) {
          committerMap.put(p, createCommitter(p));
        }
        partitionsCleanUp.remove(p);
      }
      return;
    }
    partitionsCleanUp.clear();
    for (int i = newPartitions; i < currentPartitions; i++) {
      partitionsCleanUp.add(Partition.of(i));
    }
  }

  private synchronized Committer createCommitter(Partition p) {
    Committer committer = committerFactory.newCommitter(p);
    committer.startAsync().awaitRunning();
    return committer;
  }

  private synchronized void cleanUpCommitterMap() {
    for (Partition p : partitionsCleanUp) {
      committerMap.get(p).stopAsync();
      committerMap.remove(p);
    }
    partitionsCleanUp.clear();
  }

  @Override
  public synchronized void commit(PslSourceOffset offset) {
    updateCommitterMap(offset);
    for (Map.Entry<Partition, Offset> entry : offset.partitionOffsetMap().entrySet()) {
      // Note we don't need to worry about commit offset disorder here since Committer
      // guarantees the ordering. Once commitOffset() returns, it's either already
      // sent to stream, or waiting for next connection to open to be sent in order.
      ApiFuture<Void> future = committerMap.get(entry.getKey()).commitOffset(entry.getValue());
      ApiFutures.addCallback(
          future,
          new ApiFutureCallback<Void>() {
            @Override
            public void onFailure(Throwable t) {
              if (!future.isCancelled()) {
                log.atWarning().withCause(t).log(
                    "Failed to commit %s,%s.", entry.getKey().value(), entry.getValue().value());
              }
            }

            @Override
            public void onSuccess(Void result) {
              log.atInfo().log(
                  "Committed %s,%s.", entry.getKey().value(), entry.getValue().value());
            }
          },
          MoreExecutors.directExecutor());
    }
  }
}
