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
import static com.google.cloud.pubsublite.internal.wire.ServiceClients.addDefaultMetadata;
import static com.google.cloud.pubsublite.internal.wire.ServiceClients.addDefaultSettings;

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
import com.google.cloud.pubsublite.internal.wire.PubsubContext;
import com.google.cloud.pubsublite.internal.wire.RoutingMetadata;
import com.google.cloud.pubsublite.internal.wire.SinglePartitionPublisherBuilder;
import com.google.cloud.pubsublite.spark.internal.PslCredentialsProvider;
import com.google.cloud.pubsublite.v1.AdminServiceClient;
import com.google.cloud.pubsublite.v1.AdminServiceSettings;
import com.google.cloud.pubsublite.v1.PublisherServiceClient;
import com.google.cloud.pubsublite.v1.PublisherServiceSettings;
import java.io.Serializable;
import javax.annotation.Nullable;
import org.apache.spark.sql.sources.v2.DataSourceOptions;

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

  public static PslWriteDataSourceOptions fromSparkDataSourceOptions(DataSourceOptions options) {
    if (!options.get(Constants.TOPIC_CONFIG_KEY).isPresent()) {
      throw new IllegalArgumentException(Constants.TOPIC_CONFIG_KEY + " is required.");
    }

    Builder builder = builder();
    String topicPathVal = options.get(Constants.TOPIC_CONFIG_KEY).get();
    try {
      builder.setTopicPath(TopicPath.parse(topicPathVal));
    } catch (ApiException e) {
      throw new IllegalArgumentException("Unable to parse topic path " + topicPathVal, e);
    }
    options.get(Constants.CREDENTIALS_KEY_CONFIG_KEY).ifPresent(builder::setCredentialsKey);
    return builder.build();
  }

  public PslCredentialsProvider getCredentialProvider() {
    return new PslCredentialsProvider(credentialsKey());
  }

  public Publisher<MessageMetadata> createNewPublisher() {
    return PartitionCountWatchingPublisherSettings.newBuilder()
        .setTopic(topicPath())
        .setPublisherFactory(
            partition ->
                SinglePartitionPublisherBuilder.newBuilder()
                    .setTopic(topicPath())
                    .setPartition(partition)
                    .setServiceClient(newServiceClient(partition))
                    .setBatchingSettings(PublisherSettings.DEFAULT_BATCHING_SETTINGS)
                    .build())
        .setAdminClient(getAdminClient())
        .build()
        .instantiate();
  }

  private PublisherServiceClient newServiceClient(Partition partition) throws ApiException {
    PublisherServiceSettings.Builder settingsBuilder = PublisherServiceSettings.newBuilder();
    settingsBuilder = settingsBuilder.setCredentialsProvider(getCredentialProvider());
    settingsBuilder =
        addDefaultMetadata(
            PubsubContext.of(Constants.FRAMEWORK),
            RoutingMetadata.of(topicPath(), partition),
            settingsBuilder);
    try {
      return PublisherServiceClient.create(
          addDefaultSettings(topicPath().location().region(), settingsBuilder));
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
                          topicPath().location().region(),
                          AdminServiceSettings.newBuilder()
                              .setCredentialsProvider(getCredentialProvider()))))
              .setRegion(topicPath().location().region())
              .build());
    } catch (Throwable t) {
      throw toCanonical(t).underlying;
    }
  }
}
