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

import static com.google.cloud.pubsublite.internal.ExtractStatus.toCanonical;

import com.github.benmanes.caffeine.cache.Ticker;
import com.google.auto.service.AutoService;
import com.google.cloud.pubsublite.AdminClient;
import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.TopicPath;
import com.google.cloud.pubsublite.spark.internal.CachedPartitionCountReader;
import com.google.cloud.pubsublite.spark.internal.LimitingHeadOffsetReader;
import com.google.cloud.pubsublite.spark.internal.PartitionCountReader;
import java.util.Objects;
import java.util.Optional;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.sources.v2.ContinuousReadSupport;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.MicroBatchReadSupport;
import org.apache.spark.sql.sources.v2.StreamWriteSupport;
import org.apache.spark.sql.sources.v2.reader.streaming.ContinuousReader;
import org.apache.spark.sql.sources.v2.reader.streaming.MicroBatchReader;
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.types.StructType;

@AutoService(DataSourceRegister.class)
public final class PslDataSource
    implements DataSourceV2,
        ContinuousReadSupport,
        MicroBatchReadSupport,
        StreamWriteSupport,
        DataSourceRegister {

  @Override
  public String shortName() {
    return "pubsublite";
  }

  @Override
  public ContinuousReader createContinuousReader(
      Optional<StructType> schema, String checkpointLocation, DataSourceOptions options) {
    if (schema.isPresent()) {
      throw new IllegalArgumentException(
          "PubSub Lite uses fixed schema and custom schema is not allowed");
    }

    PslReadDataSourceOptions pslReadDataSourceOptions =
        PslReadDataSourceOptions.fromSparkDataSourceOptions(options);
    SubscriptionPath subscriptionPath = pslReadDataSourceOptions.subscriptionPath();
    TopicPath topicPath;
    try (AdminClient adminClient = pslReadDataSourceOptions.newAdminClient()) {
      topicPath = TopicPath.parse(adminClient.getSubscription(subscriptionPath).get().getTopic());
    } catch (Throwable t) {
      throw toCanonical(t).underlying;
    }
    PartitionCountReader partitionCountReader =
        new CachedPartitionCountReader(pslReadDataSourceOptions.newAdminClient(), topicPath);
    return new PslContinuousReader(
        pslReadDataSourceOptions.newCursorClient(),
        pslReadDataSourceOptions.newMultiPartitionCommitter(
            partitionCountReader.getPartitionCount()),
        pslReadDataSourceOptions.getSubscriberFactory(),
        subscriptionPath,
        Objects.requireNonNull(pslReadDataSourceOptions.flowControlSettings()),
        partitionCountReader);
  }

  @Override
  public MicroBatchReader createMicroBatchReader(
      Optional<StructType> schema, String checkpointLocation, DataSourceOptions options) {
    if (schema.isPresent()) {
      throw new IllegalArgumentException(
          "PubSub Lite uses fixed schema and custom schema is not allowed");
    }

    PslReadDataSourceOptions pslReadDataSourceOptions =
        PslReadDataSourceOptions.fromSparkDataSourceOptions(options);
    SubscriptionPath subscriptionPath = pslReadDataSourceOptions.subscriptionPath();
    TopicPath topicPath;
    try (AdminClient adminClient = pslReadDataSourceOptions.newAdminClient()) {
      topicPath = TopicPath.parse(adminClient.getSubscription(subscriptionPath).get().getTopic());
    } catch (Throwable t) {
      throw toCanonical(t).underlying;
    }
    PartitionCountReader partitionCountReader =
        new CachedPartitionCountReader(pslReadDataSourceOptions.newAdminClient(), topicPath);
    return new PslMicroBatchReader(
        pslReadDataSourceOptions.newCursorClient(),
        pslReadDataSourceOptions.newMultiPartitionCommitter(
            partitionCountReader.getPartitionCount()),
        pslReadDataSourceOptions.getSubscriberFactory(),
        new LimitingHeadOffsetReader(
            pslReadDataSourceOptions.newTopicStatsClient(),
            topicPath,
            partitionCountReader,
            Ticker.systemTicker()),
        subscriptionPath,
        Objects.requireNonNull(pslReadDataSourceOptions.flowControlSettings()),
        pslReadDataSourceOptions.maxMessagesPerBatch());
  }

  @Override
  public StreamWriter createStreamWriter(
      String queryId, StructType schema, OutputMode mode, DataSourceOptions options) {
    PslSparkUtils.verifyWriteInputSchema(schema);
    PslWriteDataSourceOptions pslWriteDataSourceOptions =
        PslWriteDataSourceOptions.fromSparkDataSourceOptions(options);
    return new PslStreamWriter(schema, pslWriteDataSourceOptions);
  }
}
