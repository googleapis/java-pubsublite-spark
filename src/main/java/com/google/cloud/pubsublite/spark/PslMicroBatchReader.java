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
import static com.google.common.base.Preconditions.checkState;

import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.cloudpubsub.FlowControlSettings;
import com.google.cloud.pubsublite.internal.CursorClient;
import com.google.cloud.pubsublite.internal.wire.SubscriberFactory;
import com.google.cloud.pubsublite.spark.internal.MultiPartitionCommitter;
import com.google.cloud.pubsublite.spark.internal.PartitionSubscriberFactory;
import com.google.cloud.pubsublite.spark.internal.PerTopicHeadOffsetReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.streaming.MicroBatchReader;
import org.apache.spark.sql.sources.v2.reader.streaming.Offset;
import org.apache.spark.sql.types.StructType;

public class PslMicroBatchReader implements MicroBatchReader {
  private final CursorClient cursorClient;
  private final MultiPartitionCommitter committer;
  private final PartitionSubscriberFactory partitionSubscriberFactory;
  private final PerTopicHeadOffsetReader headOffsetReader;
  private final SubscriptionPath subscriptionPath;
  private final FlowControlSettings flowControlSettings;
  private final long maxMessagesPerBatch;
  @Nullable private SparkSourceOffset startOffset = null;
  private SparkSourceOffset endOffset;

  public PslMicroBatchReader(
      CursorClient cursorClient,
      MultiPartitionCommitter committer,
      PartitionSubscriberFactory partitionSubscriberFactory,
      PerTopicHeadOffsetReader headOffsetReader,
      SubscriptionPath subscriptionPath,
      FlowControlSettings flowControlSettings,
      long maxMessagesPerBatch) {
    this.cursorClient = cursorClient;
    this.committer = committer;
    this.partitionSubscriberFactory = partitionSubscriberFactory;
    this.headOffsetReader = headOffsetReader;
    this.subscriptionPath = subscriptionPath;
    this.flowControlSettings = flowControlSettings;
    this.maxMessagesPerBatch = maxMessagesPerBatch;
  }

  @Override
  public void setOffsetRange(Optional<Offset> start, Optional<Offset> end) {
    int currentTopicPartitionCount;
    if (end.isPresent()) {
      checkArgument(
          end.get() instanceof SparkSourceOffset,
          "end offset is not instance of SparkSourceOffset.");
      endOffset = (SparkSourceOffset) end.get();
      currentTopicPartitionCount = ((SparkSourceOffset) end.get()).getPartitionOffsetMap().size();
    } else {
      endOffset = PslSparkUtils.toSparkSourceOffset(headOffsetReader.getHeadOffset());
      currentTopicPartitionCount = endOffset.getPartitionOffsetMap().size();
    }

    if (start.isPresent()) {
      checkArgument(
          start.get() instanceof SparkSourceOffset,
          "start offset is not instance of SparkSourceOffset.");
      startOffset = (SparkSourceOffset) start.get();
    } else {
      startOffset =
          PslSparkUtils.getSparkStartOffset(
              cursorClient, subscriptionPath, currentTopicPartitionCount);
    }

    // Limit endOffset by maxMessagesPerBatch.
    endOffset =
        PslSparkUtils.getSparkEndOffset(
            endOffset, startOffset, maxMessagesPerBatch, currentTopicPartitionCount);
  }

  @Override
  public Offset getStartOffset() {
    return startOffset;
  }

  @Override
  public Offset getEndOffset() {
    return endOffset;
  }

  @Override
  public Offset deserializeOffset(String json) {
    return SparkSourceOffset.fromJson(json);
  }

  @Override
  public void commit(Offset end) {
    checkArgument(
        end instanceof SparkSourceOffset, "end offset is not instance of SparkSourceOffset.");
    committer.commit(PslSparkUtils.toPslSourceOffset((SparkSourceOffset) end));
  }

  @Override
  public void stop() {
    committer.close();
    cursorClient.close();
    headOffsetReader.close();
  }

  @Override
  public StructType readSchema() {
    return SparkStucts.DEFAULT_SCHEMA;
  }

  @Override
  public List<InputPartition<InternalRow>> planInputPartitions() {
    checkState(startOffset != null && endOffset != null);

    List<InputPartition<InternalRow>> list = new ArrayList<>();
    // Since this is called right after setOffsetRange, we could use partitions in endOffset as
    // current partition count.
    for (SparkPartitionOffset endPartitionOffset : endOffset.getPartitionOffsetMap().values()) {
      Partition p = endPartitionOffset.partition();
      SparkPartitionOffset startPartitionOffset =
          startOffset.getPartitionOffsetMap().getOrDefault(p, SparkPartitionOffset.create(p, -1L));
      if (startPartitionOffset.equals(endPartitionOffset)) {
        // There is no message to pull for this partition.
        continue;
      }
      PartitionSubscriberFactory partitionSubscriberFactory = this.partitionSubscriberFactory;
      SubscriberFactory subscriberFactory =
          (consumer) ->
              partitionSubscriberFactory.newSubscriber(endPartitionOffset.partition(), consumer);
      list.add(
          new PslMicroBatchInputPartition(
              subscriptionPath,
              flowControlSettings,
              startPartitionOffset,
              endPartitionOffset,
              subscriberFactory));
    }
    return list;
  }
}
