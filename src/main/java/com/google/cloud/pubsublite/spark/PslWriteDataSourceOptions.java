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
import static com.google.cloud.pubsublite.internal.wire.ServiceClients.addDefaultSettings;
import static com.google.cloud.pubsublite.internal.wire.ServiceClients.getCallContext;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.gax.rpc.ApiCallContext;
import com.google.api.gax.rpc.ApiException;
import com.google.auto.value.AutoValue;
import com.google.cloud.pubsublite.AdminClient;
import com.google.cloud.pubsublite.AdminClientSettings;
import com.google.cloud.pubsublite.MessageMetadata;
import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.TopicPath;
import com.google.cloud.pubsublite.cloudpubsub.PublisherSettings;
import com.google.cloud.pubsublite.internal.Publisher;
import com.google.cloud.pubsublite.internal.wire.PartitionCountWatchingPublisherSettings;
import com.google.cloud.pubsublite.internal.wire.PartitionPublisherFactory;
import com.google.cloud.pubsublite.internal.wire.PubsubContext;
import com.google.cloud.pubsublite.internal.wire.RoutingMetadata;
import com.google.cloud.pubsublite.internal.wire.SinglePartitionPublisherBuilder;
import com.google.cloud.pubsublite.spark.internal.PslCredentialsProvider;
import com.google.cloud.pubsublite.v1.AdminServiceClient;
import com.google.cloud.pubsublite.v1.AdminServiceSettings;
import com.google.cloud.pubsublite.v1.PublisherServiceClient;
import com.google.cloud.pubsublite.v1.PublisherServiceSettings;
import java.io.Serializable;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;

@AutoValue
public abstract class PslWriteDataSourceOptions implements Serializable {

  @Nullable
  public abstract String credentialsKey();

  public abstract TopicPath topicPath();

  public static Builder builder() {
    return new AutoValue_PslWriteDataSourceOptions.Builder().setCredentialsKey(null);
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract PslWriteDataSourceOptions.Builder setCredentialsKey(String credentialsKey);

    public abstract PslWriteDataSourceOptions.Builder setTopicPath(TopicPath topicPath);

    public abstract PslWriteDataSourceOptions build();
  }

  public static PslWriteDataSourceOptions fromProperties(Map<String, String> properties) {
    Builder builder = builder();
    String pathVal =
        checkNotNull(
            properties.get(Constants.TOPIC_CONFIG_KEY),
            Constants.TOPIC_CONFIG_KEY + " is required.");
    builder.setTopicPath(TopicPath.parse(pathVal));
    Optional.ofNullable(properties.get(Constants.CREDENTIALS_KEY_CONFIG_KEY))
        .ifPresent(builder::setCredentialsKey);
    return builder.build();
  }

  public PslCredentialsProvider getCredentialProvider() {
    return new PslCredentialsProvider(credentialsKey());
  }

  private PartitionPublisherFactory getPartitionPublisherFactory() {
    PublisherServiceClient client = newServiceClient();
    return new PartitionPublisherFactory() {
      @Override
      public Publisher<MessageMetadata> newPublisher(Partition partition) throws ApiException {
        SinglePartitionPublisherBuilder.Builder singlePartitionBuilder =
            SinglePartitionPublisherBuilder.newBuilder()
                .setTopic(topicPath())
                .setPartition(partition)
                .setBatchingSettings(PublisherSettings.DEFAULT_BATCHING_SETTINGS)
                .setStreamFactory(
                    responseStream -> {
                      ApiCallContext context =
                          getCallContext(
                              PubsubContext.of(Constants.FRAMEWORK),
                              RoutingMetadata.of(topicPath(), partition));
                      return client.publishCallable().splitCall(responseStream, context);
                    });
        return singlePartitionBuilder.build();
      }

      @Override
      public void close() {
        client.close();
      }
    };
  }

  public Publisher<MessageMetadata> createNewPublisher() {
    return PartitionCountWatchingPublisherSettings.newBuilder()
        .setTopic(topicPath())
        .setPublisherFactory(getPartitionPublisherFactory())
        .setAdminClient(getAdminClient())
        .build()
        .instantiate();
  }

  private PublisherServiceClient newServiceClient() throws ApiException {
    PublisherServiceSettings.Builder settingsBuilder = PublisherServiceSettings.newBuilder();
    settingsBuilder = settingsBuilder.setCredentialsProvider(getCredentialProvider());
    try {
      return PublisherServiceClient.create(
          addDefaultSettings(topicPath().location().extractRegion(), settingsBuilder));
    } catch (Throwable t) {
      throw toCanonical(t).underlying;
    }
  }

  private AdminClient getAdminClient() throws ApiException {
    try {
      return AdminClient.create(
          AdminClientSettings.newBuilder()
              .setServiceClient(
                  AdminServiceClient.create(
                      addDefaultSettings(
                          topicPath().location().extractRegion(),
                          AdminServiceSettings.newBuilder()
                              .setCredentialsProvider(getCredentialProvider()))))
              .setRegion(topicPath().location().extractRegion())
              .build());
    } catch (Throwable t) {
      throw toCanonical(t).underlying;
    }
  }
}
