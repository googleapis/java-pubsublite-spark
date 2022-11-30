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

import static com.google.common.base.Preconditions.checkState;

import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.internal.CursorClient;
import com.google.cloud.pubsublite.spark.internal.MultiPartitionCommitter;
import com.google.cloud.pubsublite.spark.internal.PartitionCountReader;
import com.google.cloud.pubsublite.spark.internal.PerTopicHeadOffsetReader;
import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.concurrent.GuardedBy;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.connector.read.streaming.Offset;

public class PslMicroBatchStream extends BaseDataStream implements MicroBatchStream {

  private final PerTopicHeadOffsetReader headOffsetReader;
  private final PslReadDataSourceOptions options;

  @GuardedBy("this")
  private SparkSourceOffset lastEndOffset = null;

  @VisibleForTesting
  PslMicroBatchStream(
      CursorClient cursorClient,
      MultiPartitionCommitter committer,
      PerTopicHeadOffsetReader headOffsetReader,
      SubscriptionPath subscriptionPath,
      PartitionCountReader countReader,
      PslReadDataSourceOptions options) {
    super(cursorClient, committer, countReader, subscriptionPath);
    this.headOffsetReader = headOffsetReader;
    this.options = options;
  }

  PslMicroBatchStream(PslReadDataSourceOptions options) {
    this(
        options.newCursorClient(),
        options.newMultiPartitionCommitter(),
        options.newHeadOffsetReader(),
        options.subscriptionPath(),
        options.newPartitionCountReader(),
        options);
  }

  @Override
  public synchronized SparkSourceOffset latestOffset() {
    SparkSourceOffset newStartingOffset = (lastEndOffset == null) ? initialOffset() : lastEndOffset;
    SparkSourceOffset headOffset =
        PslSparkUtils.toSparkSourceOffset(headOffsetReader.getHeadOffset());
    lastEndOffset =
        PslSparkUtils.getSparkEndOffset(
            headOffset,
            newStartingOffset,
            options.maxMessagesPerBatch(),
            headOffset.getPartitionOffsetMap().size());
    return lastEndOffset;
  }

  @Override
  public void stop() {
    super.stop();
    headOffsetReader.close();
  }

  @Override
  public InputPartition[] planInputPartitions(Offset startOffset, Offset endOffset) {
    checkState(startOffset != null && endOffset != null);
    SparkSourceOffset startSourceOffset = (SparkSourceOffset) startOffset;
    SparkSourceOffset endSourceOffset = (SparkSourceOffset) endOffset;

    List<InputPartition> list = new ArrayList<>();
    for (SparkPartitionOffset endPartitionOffset :
        endSourceOffset.getPartitionOffsetMap().values()) {
      Partition p = endPartitionOffset.partition();
      SparkPartitionOffset startPartitionOffset =
          startSourceOffset
              .getPartitionOffsetMap()
              .getOrDefault(p, SparkPartitionOffset.create(p, -1L));
      if (startPartitionOffset.equals(endPartitionOffset)) {
        // There is no message to pull for this partition.
        continue;
      }
      list.add(new PslMicroBatchInputPartition(startPartitionOffset, endPartitionOffset, options));
    }
    return list.toArray(new InputPartition[0]);
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    return new PslMicroBatchPartitionReaderFactory();
  }
}
