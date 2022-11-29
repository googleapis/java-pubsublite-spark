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

package com.google.cloud.pubsublite.spark.internal;

import static com.google.cloud.pubsublite.internal.wire.ServiceClients.addDefaultSettings;

import com.google.cloud.pubsublite.spark.PslReadDataSourceOptions;
import com.google.cloud.pubsublite.v1.SubscriberServiceClient;
import com.google.cloud.pubsublite.v1.SubscriberServiceSettings;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Singleton class that caches subscriber service clients to be re-used by the same task. */
public class CachedSubscriberClients {
  private static CachedSubscriberClients instance = null;

  private final Map<PslReadDataSourceOptions, SubscriberServiceClient> subscriberClients =
      new ConcurrentHashMap<>();

  private static synchronized CachedSubscriberClients getInstance() {
    if (instance == null) {
      instance = new CachedSubscriberClients();
    }
    return instance;
  }

  public static SubscriberServiceClient getOrCreate(PslReadDataSourceOptions readOptions) {
    return getInstance()
        .subscriberClients
        .computeIfAbsent(readOptions, CachedSubscriberClients::newSubscriberClient);
  }

  private static SubscriberServiceClient newSubscriberClient(PslReadDataSourceOptions readOptions) {
    try {
      SubscriberServiceSettings.Builder settingsBuilder =
          SubscriberServiceSettings.newBuilder()
              .setCredentialsProvider(new PslCredentialsProvider(readOptions.credentialsKey()));
      return SubscriberServiceClient.create(
          addDefaultSettings(
              readOptions.subscriptionPath().location().extractRegion(), settingsBuilder));
    } catch (IOException e) {
      throw new IllegalStateException("Unable to create SubscriberServiceClient.");
    }
  }
}
