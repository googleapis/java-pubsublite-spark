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

import com.google.cloud.pubsublite.internal.BlockingPullSubscriber;
import com.google.cloud.pubsublite.internal.BlockingPullSubscriberImpl;
import com.google.cloud.pubsublite.internal.CheckedApiException;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;

public class PslMicroBatchPartitionReaderFactory implements PartitionReaderFactory {
  @Override
  public PartitionReader<InternalRow> createReader(InputPartition inputPartition) {
    checkArgument(inputPartition instanceof PslMicroBatchInputPartition);
    PslMicroBatchInputPartition partition = (PslMicroBatchInputPartition) inputPartition;
    PslReadDataSourceOptions options = partition.options;
    BlockingPullSubscriber subscriber;
    try {
      PslPartitionOffset pslStartOffset = PslSparkUtils.toPslPartitionOffset(partition.startOffset);
      subscriber =
          new BlockingPullSubscriberImpl(
              (consumer) ->
                  options.getSubscriberFactory().newSubscriber(
                      pslStartOffset.partition(), pslStartOffset.offset(), consumer),
              options.flowControlSettings());
    } catch (CheckedApiException e) {
      throw new IllegalStateException(
          "Unable to create PSL subscriber for " + partition.startOffset.partition(), e);
    }
    return new PslMicroBatchInputPartitionReader(options.subscriptionPath(), partition.endOffset, subscriber);
  }
}
