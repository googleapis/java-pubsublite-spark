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

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.internal.CursorClient;
import com.google.cloud.pubsublite.internal.ExtractStatus;
import com.google.cloud.pubsublite.spark.PslSourceOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A {@link MultiPartitionCommitter} that lazily adjusts for partition changes when {@link
 * MultiPartitionCommitter#commit(PslSourceOffset)} is called.
 */
public class MultiPartitionCommitterImpl implements MultiPartitionCommitter {
  private final SubscriptionPath subscription;
  private final CursorClient client;

  public MultiPartitionCommitterImpl(SubscriptionPath subscription, CursorClient client) {
    this.subscription = subscription;
    this.client = client;
  }

  @Override
  public void close() {
    client.close();
  }

  @Override
  public void commit(PslSourceOffset offset) {
    List<ApiFuture<Void>> futures = new ArrayList<>();
    for (Map.Entry<Partition, Offset> entry : offset.partitionOffsetMap().entrySet()) {
      futures.add(client.commitCursor(subscription, entry.getKey(), entry.getValue()));
    }
    try {
      ApiFutures.allAsList(futures).get();
    } catch (Exception e) {
      throw ExtractStatus.toCanonical(e).underlying;
    }
  }
}
