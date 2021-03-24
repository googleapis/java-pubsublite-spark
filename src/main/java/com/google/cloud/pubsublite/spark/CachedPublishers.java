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

import com.google.api.core.ApiService;
import com.google.api.gax.rpc.ApiException;
import com.google.cloud.pubsublite.AdminClient;
import com.google.cloud.pubsublite.AdminClientSettings;
import com.google.cloud.pubsublite.MessageMetadata;
import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.internal.CloseableMonitor;
import com.google.cloud.pubsublite.internal.Publisher;
import com.google.cloud.pubsublite.internal.wire.PartitionCountWatchingPublisherSettings;
import com.google.cloud.pubsublite.internal.wire.PubsubContext;
import com.google.cloud.pubsublite.internal.wire.RoutingMetadata;
import com.google.cloud.pubsublite.internal.wire.SinglePartitionPublisherBuilder;
import com.google.cloud.pubsublite.v1.AdminServiceClient;
import com.google.cloud.pubsublite.v1.AdminServiceSettings;
import com.google.cloud.pubsublite.v1.PublisherServiceClient;
import com.google.cloud.pubsublite.v1.PublisherServiceSettings;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import javax.annotation.concurrent.GuardedBy;

/** Cached {@link Publisher}s to reuse publisher of same settings in the same task. */
public class CachedPublishers {

  private final CloseableMonitor monitor = new CloseableMonitor();

  private final Executor listenerExecutor = Executors.newSingleThreadExecutor();

  @GuardedBy("monitor.monitor")
  private static final Map<PslWriteDataSourceOptions, Publisher<MessageMetadata>> publishers =
      new HashMap<>();

  public Publisher<MessageMetadata> getOrCreate(PslWriteDataSourceOptions writeOptions) {
    try (CloseableMonitor.Hold h = monitor.enter()) {
      Publisher<MessageMetadata> publisher = publishers.get(writeOptions);
      if (publisher != null) {
        return publisher;
      }

      publisher = createPublisherInternal(writeOptions);
      publishers.put(writeOptions, publisher);
      publisher.addListener(
          new ApiService.Listener() {
            @Override
            public void failed(ApiService.State s, Throwable t) {
              try (CloseableMonitor.Hold h = monitor.enter()) {
                publishers.remove(writeOptions);
              }
            }
          },
          listenerExecutor);
      publisher.startAsync().awaitRunning();
      return publisher;
    }
  }

  private PublisherServiceClient newServiceClient(
      PslWriteDataSourceOptions writeOptions, Partition partition) throws ApiException {
    PublisherServiceSettings.Builder settingsBuilder = PublisherServiceSettings.newBuilder();
    settingsBuilder = settingsBuilder.setCredentialsProvider(writeOptions.getCredentialProvider());
    settingsBuilder =
        addDefaultMetadata(
            PubsubContext.of(Constants.FRAMEWORK),
            RoutingMetadata.of(writeOptions.topicPath(), partition),
            settingsBuilder);
    try {
      return PublisherServiceClient.create(
          addDefaultSettings(writeOptions.topicPath().location().region(), settingsBuilder));
    } catch (Throwable t) {
      throw toCanonical(t).underlying;
    }
  }

  private AdminClient getAdminClient(PslWriteDataSourceOptions writeOptions) throws ApiException {
    try {
      return AdminClient.create(
          AdminClientSettings.newBuilder()
              .setServiceClient(
                  AdminServiceClient.create(
                      addDefaultSettings(
                          writeOptions.topicPath().location().region(),
                          AdminServiceSettings.newBuilder()
                              .setCredentialsProvider(writeOptions.getCredentialProvider()))))
              .setRegion(writeOptions.topicPath().location().region())
              .build());
    } catch (Throwable t) {
      throw toCanonical(t).underlying;
    }
  }

  private Publisher<MessageMetadata> createPublisherInternal(
      PslWriteDataSourceOptions writeOptions) {
    return PartitionCountWatchingPublisherSettings.newBuilder()
        .setTopic(writeOptions.topicPath())
        .setPublisherFactory(
            partition ->
                SinglePartitionPublisherBuilder.newBuilder()
                    .setTopic(writeOptions.topicPath())
                    .setPartition(partition)
                    .setServiceClient(newServiceClient(writeOptions, partition))
                    .build())
        .setAdminClient(getAdminClient(writeOptions))
        .build()
        .instantiate();
  }
}
