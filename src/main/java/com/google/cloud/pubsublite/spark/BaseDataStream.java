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

import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.internal.CursorClient;
import com.google.cloud.pubsublite.spark.internal.MultiPartitionCommitter;
import com.google.cloud.pubsublite.spark.internal.PartitionCountReader;
import com.google.common.flogger.GoogleLogger;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.spark.sql.connector.read.streaming.SparkDataStream;

public class BaseDataStream implements SparkDataStream {
  private static final GoogleLogger log = GoogleLogger.forEnclosingClass();
  private final CursorClient cursorClient;
  private final MultiPartitionCommitter committer;
  private final PartitionCountReader countReader;
  private final SubscriptionPath subscriptionPath;

  BaseDataStream(
      CursorClient cursorClient,
      MultiPartitionCommitter committer,
      PartitionCountReader countReader,
      SubscriptionPath subscriptionPath) {
    this.cursorClient = cursorClient;
    this.committer = committer;
    this.countReader = countReader;
    this.subscriptionPath = subscriptionPath;
  }

  @Override
  public SparkSourceOffset initialOffset() {
    int partitionCount = countReader.getPartitionCount();
    try {
      Map<Partition, Offset> pslSourceOffsetMap = new HashMap<>();
      for (int i = 0; i < partitionCount; i++) {
        pslSourceOffsetMap.put(Partition.of(i), Offset.of(0));
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

  @Override
  public SparkSourceOffset deserializeOffset(String json) {
    return SparkSourceOffset.fromJson(json);
  }

  @Override
  public void commit(org.apache.spark.sql.connector.read.streaming.Offset end) {
    checkArgument(
        end instanceof SparkSourceOffset, "end offset is not instance of SparkSourceOffset.");
    committer.commit(PslSparkUtils.toPslSourceOffset((SparkSourceOffset) end));
  }

  @Override
  public void stop() {
    try (AutoCloseable a = committer;
        AutoCloseable b = cursorClient;
        AutoCloseable c = countReader) {
    } catch (Exception e) {
      log.atWarning().withCause(e).log("Unable to close LimitingHeadOffsetReader.");
    }
  }
}
