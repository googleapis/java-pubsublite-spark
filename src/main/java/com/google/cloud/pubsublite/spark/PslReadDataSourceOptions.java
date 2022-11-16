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

import static com.google.cloud.pubsublite.internal.wire.ServiceClients.addDefaultSettings;
import static com.google.cloud.pubsublite.internal.wire.ServiceClients.getCallContext;
import static com.google.common.base.Preconditions.checkNotNull;

import com.github.benmanes.caffeine.cache.Ticker;
import com.google.api.gax.rpc.ApiCallContext;
import com.google.auto.value.AutoValue;
import com.google.cloud.pubsublite.AdminClient;
import com.google.cloud.pubsublite.AdminClientSettings;
import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.TopicPath;
import com.google.cloud.pubsublite.cloudpubsub.FlowControlSettings;
import com.google.cloud.pubsublite.internal.CursorClient;
import com.google.cloud.pubsublite.internal.CursorClientSettings;
import com.google.cloud.pubsublite.internal.TopicStatsClient;
import com.google.cloud.pubsublite.internal.TopicStatsClientSettings;
import com.google.cloud.pubsublite.internal.wire.PubsubContext;
import com.google.cloud.pubsublite.internal.wire.RoutingMetadata;
import com.google.cloud.pubsublite.internal.wire.SubscriberBuilder;
import com.google.cloud.pubsublite.proto.Cursor;
import com.google.cloud.pubsublite.proto.SeekRequest;
import com.google.cloud.pubsublite.spark.internal.CachedPartitionCountReader;
import com.google.cloud.pubsublite.spark.internal.LimitingHeadOffsetReader;
import com.google.cloud.pubsublite.spark.internal.MultiPartitionCommitter;
import com.google.cloud.pubsublite.spark.internal.MultiPartitionCommitterImpl;
import com.google.cloud.pubsublite.spark.internal.PartitionCountReader;
import com.google.cloud.pubsublite.spark.internal.PartitionSubscriberFactory;
import com.google.cloud.pubsublite.spark.internal.PerTopicHeadOffsetReader;
import com.google.cloud.pubsublite.spark.internal.PslCredentialsProvider;
import com.google.cloud.pubsublite.v1.AdminServiceClient;
import com.google.cloud.pubsublite.v1.AdminServiceSettings;
import com.google.cloud.pubsublite.v1.CursorServiceClient;
import com.google.cloud.pubsublite.v1.CursorServiceSettings;
import com.google.cloud.pubsublite.v1.SubscriberServiceClient;
import com.google.cloud.pubsublite.v1.SubscriberServiceSettings;
import com.google.cloud.pubsublite.v1.TopicStatsServiceClient;
import com.google.cloud.pubsublite.v1.TopicStatsServiceSettings;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;

@AutoValue
public abstract class PslReadDataSourceOptions implements Serializable {
  private static final long serialVersionUID = 2680059304693561607L;

  @Nullable
  public abstract String credentialsKey();

  public abstract SubscriptionPath subscriptionPath();

  public abstract FlowControlSettings flowControlSettings();

  public abstract long maxMessagesPerBatch();

  public static Builder builder() {
    return new AutoValue_PslReadDataSourceOptions.Builder()
        .setCredentialsKey(null)
        .setMaxMessagesPerBatch(Constants.DEFAULT_MAX_MESSAGES_PER_BATCH)
        .setFlowControlSettings(
            FlowControlSettings.builder()
                .setMessagesOutstanding(Constants.DEFAULT_MESSAGES_OUTSTANDING)
                .setBytesOutstanding(Constants.DEFAULT_BYTES_OUTSTANDING)
                .build());
  }

  public static PslReadDataSourceOptions fromProperties(Map<String, String> properties) {
    Builder builder = builder();
    String pathVal =
        checkNotNull(
            properties.get(Constants.SUBSCRIPTION_CONFIG_KEY),
            Constants.SUBSCRIPTION_CONFIG_KEY + " is required.");
    builder.setSubscriptionPath(SubscriptionPath.parse(pathVal));
    Optional.ofNullable(properties.get(Constants.CREDENTIALS_KEY_CONFIG_KEY))
        .ifPresent(builder::setCredentialsKey);
    Optional.ofNullable(properties.get(Constants.MAX_MESSAGE_PER_BATCH_CONFIG_KEY))
        .ifPresent(mmpb -> builder.setMaxMessagesPerBatch(Long.parseLong(mmpb)));
    FlowControlSettings.Builder flowControl = FlowControlSettings.builder();
    flowControl.setMessagesOutstanding(
        Optional.ofNullable(properties.get(Constants.MESSAGES_OUTSTANDING_CONFIG_KEY))
            .map(Long::parseLong)
            .orElse(Constants.DEFAULT_MESSAGES_OUTSTANDING));
    flowControl.setBytesOutstanding(
        Optional.ofNullable(properties.get(Constants.BYTES_OUTSTANDING_CONFIG_KEY))
            .map(Long::parseLong)
            .orElse(Constants.DEFAULT_BYTES_OUTSTANDING));
    builder.setFlowControlSettings(flowControl.build());
    return builder.build();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setCredentialsKey(String credentialsKey);

    public abstract Builder setSubscriptionPath(SubscriptionPath subscriptionPath);

    public abstract Builder setMaxMessagesPerBatch(long maxMessagesPerBatch);

    public abstract Builder setFlowControlSettings(FlowControlSettings flowControlSettings);

    public abstract PslReadDataSourceOptions build();
  }

  MultiPartitionCommitter newMultiPartitionCommitter() {
    return new MultiPartitionCommitterImpl(subscriptionPath(), newCursorClient());
  }

  @SuppressWarnings("CheckReturnValue")
  PartitionSubscriberFactory getSubscriberFactory() {
    return (partition, offset, consumer) -> {
      SubscriberServiceSettings.Builder settingsBuilder =
          SubscriberServiceSettings.newBuilder()
              .setCredentialsProvider(new PslCredentialsProvider(credentialsKey()));
      try {
        SubscriberServiceClient serviceClient =
            SubscriberServiceClient.create(
                addDefaultSettings(
                    this.subscriptionPath().location().extractRegion(), settingsBuilder));
        return SubscriberBuilder.newBuilder()
            .setSubscriptionPath(this.subscriptionPath())
            .setPartition(partition)
            .setMessageConsumer(consumer)
            .setStreamFactory(
                responseStream -> {
                  ApiCallContext context =
                      getCallContext(
                          PubsubContext.of(Constants.FRAMEWORK),
                          RoutingMetadata.of(subscriptionPath(), partition));
                  return serviceClient.subscribeCallable().splitCall(responseStream, context);
                })
            .setInitialLocation(
                SeekRequest.newBuilder()
                    .setCursor(Cursor.newBuilder().setOffset(offset.value()))
                    .build())
            .build();
      } catch (IOException e) {
        throw new IllegalStateException("Failed to create subscriber service.", e);
      }
    };
  }

  // TODO(b/jiangmichael): Make XXXClientSettings accept creds so we could simplify below methods.
  private CursorServiceClient newCursorServiceClient() {
    try {
      return CursorServiceClient.create(
          addDefaultSettings(
              this.subscriptionPath().location().extractRegion(),
              CursorServiceSettings.newBuilder()
                  .setCredentialsProvider(new PslCredentialsProvider(credentialsKey()))));
    } catch (IOException e) {
      throw new IllegalStateException("Unable to create CursorServiceClient.");
    }
  }

  CursorClient newCursorClient() {
    return CursorClient.create(
        CursorClientSettings.newBuilder()
            .setRegion(this.subscriptionPath().location().extractRegion())
            .setServiceClient(newCursorServiceClient())
            .build());
  }

  private AdminServiceClient newAdminServiceClient() {
    try {
      return AdminServiceClient.create(
          addDefaultSettings(
              this.subscriptionPath().location().extractRegion(),
              AdminServiceSettings.newBuilder()
                  .setCredentialsProvider(new PslCredentialsProvider(credentialsKey()))));
    } catch (IOException e) {
      throw new IllegalStateException("Unable to create AdminServiceClient.");
    }
  }

  AdminClient newAdminClient() {
    return AdminClient.create(
        AdminClientSettings.newBuilder()
            .setRegion(this.subscriptionPath().location().extractRegion())
            .setServiceClient(newAdminServiceClient())
            .build());
  }

  private TopicStatsServiceClient newTopicStatsServiceClient() {
    try {
      return TopicStatsServiceClient.create(
          addDefaultSettings(
              this.subscriptionPath().location().extractRegion(),
              TopicStatsServiceSettings.newBuilder()
                  .setCredentialsProvider(new PslCredentialsProvider(credentialsKey()))));
    } catch (IOException e) {
      throw new IllegalStateException("Unable to create TopicStatsServiceClient.");
    }
  }

  TopicPath getTopicPath() {
    try (AdminClient adminClient = newAdminClient()) {
      return TopicPath.parse(adminClient.getSubscription(subscriptionPath()).get().getTopic());
    } catch (Exception e) {
      throw new IllegalStateException("Unable to fetch topic.", e);
    }
  }

  TopicStatsClient newTopicStatsClient() {
    return TopicStatsClient.create(
        TopicStatsClientSettings.newBuilder()
            .setRegion(this.subscriptionPath().location().extractRegion())
            .setServiceClient(newTopicStatsServiceClient())
            .build());
  }

  PartitionCountReader newPartitionCountReader() {
    return new CachedPartitionCountReader(newAdminClient(), getTopicPath());
  }

  PerTopicHeadOffsetReader newHeadOffsetReader() {
    return new LimitingHeadOffsetReader(
        newTopicStatsClient(), getTopicPath(), newPartitionCountReader(), Ticker.systemTicker());
  }
}
