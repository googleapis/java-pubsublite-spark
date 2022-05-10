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

import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.internal.CursorClient;
import com.google.cloud.pubsublite.spark.internal.MultiPartitionCommitter;
import com.google.cloud.pubsublite.spark.internal.PartitionCountReader;
import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.streaming.ContinuousPartitionReaderFactory;
import org.apache.spark.sql.connector.read.streaming.ContinuousStream;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.connector.read.streaming.PartitionOffset;

public class PslContinuousStream extends BaseDataStream implements ContinuousStream {
  private final PartitionCountReader partitionCountReader;
  private final long topicPartitionCount;
  private final PslReadDataSourceOptions options;

  @VisibleForTesting
  PslContinuousStream(
      CursorClient cursorClient,
      MultiPartitionCommitter committer,
      SubscriptionPath subscriptionPath,
      PartitionCountReader partitionCountReader,
      PslReadDataSourceOptions options) {
    super(cursorClient, committer, partitionCountReader, subscriptionPath);
    this.partitionCountReader = partitionCountReader;
    this.topicPartitionCount = partitionCountReader.getPartitionCount();
    this.options = options;
  }

  PslContinuousStream(PslReadDataSourceOptions options) {
    this(options.newCursorClient(), options.newMultiPartitionCommitter(), options.subscriptionPath(), options.newPartitionCountReader(), options);
  }

  @Override
  public Offset mergeOffsets(PartitionOffset[] offsets) {
    return SparkSourceOffset.merge(
        Arrays.copyOf(offsets, offsets.length, SparkPartitionOffset[].class));
  }

  @Override
  public InputPartition[] planInputPartitions(Offset start) {
    SparkSourceOffset startOffset = (SparkSourceOffset) start;
    List<InputPartition> list = new ArrayList<>();
    for (SparkPartitionOffset offset : startOffset.getPartitionOffsetMap().values()) {
      list.add(new PslContinuousInputPartition(offset, options));
    }
    return list.toArray(new InputPartition[0]);
  }

  @Override
  public boolean needsReconfiguration() {
    return partitionCountReader.getPartitionCount() != topicPartitionCount;
  }

  @Override
  public ContinuousPartitionReaderFactory createContinuousReaderFactory() {
    return new PslContinuousPartitionReaderFactory();
  }
}
