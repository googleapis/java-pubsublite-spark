package com.google.cloud.pubsublite.spark;

import java.io.Closeable;

public interface PartitionCountReader extends Closeable {
  int getPartitionCount();

  @Override
  void close();
}
