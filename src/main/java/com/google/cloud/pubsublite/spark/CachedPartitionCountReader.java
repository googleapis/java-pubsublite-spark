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

import com.google.cloud.pubsublite.AdminClient;
import com.google.cloud.pubsublite.PartitionLookupUtils;
import com.google.cloud.pubsublite.TopicPath;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class CachedPartitionCountReader implements PartitionCountReader {
  private final AdminClient adminClient;
  private final Supplier<Integer> supplier;

  public CachedPartitionCountReader(AdminClient adminClient, TopicPath topicPath) {
    this.adminClient = adminClient;
    this.supplier =
        Suppliers.memoizeWithExpiration(
            () -> PartitionLookupUtils.numPartitions(topicPath, adminClient), 1, TimeUnit.MINUTES);
  }

  @Override
  public void close() {
    adminClient.close();
  }

  public int getPartitionCount() {
    return supplier.get();
  }
}
