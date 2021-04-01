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

import com.google.api.core.ApiService;
import com.google.cloud.pubsublite.MessageMetadata;
import com.google.cloud.pubsublite.internal.Publisher;
import com.google.cloud.pubsublite.spark.PslWriteDataSourceOptions;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import javax.annotation.concurrent.GuardedBy;

/** Cached {@link Publisher}s to reuse publisher of same settings in the same task. */
public class CachedPublishers {

  // TODO(jiangmichaellll): Use com.google.cloud.pubsublite.internal.wire.SystemExecutors
  // once new PSL client library is released.
  private final Executor listenerExecutor = Executors.newSingleThreadExecutor();

  @GuardedBy("this")
  private static final Map<PslWriteDataSourceOptions, Publisher<MessageMetadata>> publishers =
      new HashMap<>();

  public synchronized Publisher<MessageMetadata> getOrCreate(
      PslWriteDataSourceOptions writeOptions) {
    Publisher<MessageMetadata> publisher = publishers.get(writeOptions);
    if (publisher != null) {
      return publisher;
    }

    publisher = writeOptions.createNewPublisher();
    publishers.put(writeOptions, publisher);
    publisher.addListener(
        new ApiService.Listener() {
          @Override
          public void failed(ApiService.State s, Throwable t) {
            removePublisher(writeOptions);
          }
        },
        listenerExecutor);
    publisher.startAsync().awaitRunning();
    return publisher;
  }

  private synchronized void removePublisher(PslWriteDataSourceOptions writeOptions) {
    publishers.remove(writeOptions);
  }
}
