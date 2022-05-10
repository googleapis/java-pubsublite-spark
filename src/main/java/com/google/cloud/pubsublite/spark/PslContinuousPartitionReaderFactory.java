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

import com.google.cloud.pubsublite.internal.BlockingPullSubscriberImpl;
import com.google.cloud.pubsublite.internal.CheckedApiException;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.streaming.ContinuousPartitionReader;
import org.apache.spark.sql.connector.read.streaming.ContinuousPartitionReaderFactory;

public class PslContinuousPartitionReaderFactory implements ContinuousPartitionReaderFactory {
  @Override
  public ContinuousPartitionReader<InternalRow> createReader(InputPartition inputPartition) {
    checkArgument(inputPartition instanceof PslContinuousInputPartition);
    PslContinuousInputPartition partition = (PslContinuousInputPartition) inputPartition;
    PslReadDataSourceOptions options = partition.options;
    PslPartitionOffset pslPartitionOffset = PslSparkUtils.toPslPartitionOffset(partition.startOffset);

    BlockingPullSubscriberImpl subscriber;
    try {
      subscriber =
          new BlockingPullSubscriberImpl(
              (consumer) -> options.getSubscriberFactory().newSubscriber(
                      pslPartitionOffset.partition(), pslPartitionOffset.offset(), consumer),
              options.flowControlSettings());
    } catch (CheckedApiException e) {
      throw new IllegalStateException(
          "Unable to create PSL subscriber for " + pslPartitionOffset.toString(), e);
    }
    return new PslContinuousPartitionReader(
        options.subscriptionPath(), partition.startOffset, subscriber);
  }
}
