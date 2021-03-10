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
            () -> PartitionLookupUtils.numPartitions(topicPath, adminClient), 10, TimeUnit.SECONDS);
  }

  @Override
  public void close() {
    adminClient.close();
  }

  public int getPartitionCount() {
    return supplier.get();
  }
}
