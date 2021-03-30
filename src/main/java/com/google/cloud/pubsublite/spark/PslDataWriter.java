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

import com.google.api.core.ApiFuture;
import com.google.cloud.pubsublite.MessageMetadata;
import com.google.cloud.pubsublite.TopicPath;
import com.google.cloud.pubsublite.spark.internal.CachedPublishers;
import com.google.cloud.pubsublite.spark.internal.PublisherFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.flogger.GoogleLogger;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import javax.annotation.concurrent.GuardedBy;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;

public class PslDataWriter implements DataWriter<InternalRow> {

  private static final GoogleLogger log = GoogleLogger.forEnclosingClass();

  private static final CachedPublishers CACHED_PUBLISHERS = new CachedPublishers();

  private final long partitionId, taskId, epochId;
  private final StructType inputSchema;
  private final PslWriteDataSourceOptions writeOptions;
  private final CachedPublishers cachedPublishers; // just a reference

  @GuardedBy("this")
  private final List<ApiFuture<MessageMetadata>> futures = new ArrayList<>();

  public PslDataWriter(
      long partitionId,
      long taskId,
      long epochId,
      StructType schema,
      PslWriteDataSourceOptions writeOptions) {
    this(partitionId, taskId, epochId, schema, writeOptions, CACHED_PUBLISHERS);
  }

  @VisibleForTesting
  public PslDataWriter(
      long partitionId,
      long taskId,
      long epochId,
      StructType schema,
      PslWriteDataSourceOptions writeOptions,
      CachedPublishers cachedPublishers) {
    this.partitionId = partitionId;
    this.taskId = taskId;
    this.epochId = epochId;
    this.inputSchema = schema;
    this.writeOptions = writeOptions;
    this.cachedPublishers = cachedPublishers;
  }

  @Override
  public synchronized void write(InternalRow record) {
    futures.add(
        cachedPublishers
            .getOrCreate(writeOptions)
            .publish(Objects.requireNonNull(PslSparkUtils.toPubSubMessage(inputSchema, record))));
  }

  @Override
  public synchronized WriterCommitMessage commit() throws IOException {
    for (ApiFuture<MessageMetadata> f : futures) {
      try {
        f.get();
      } catch (InterruptedException | ExecutionException e) {
        throw new IOException(e);
      }
    }
    log.atInfo().log(
        "All writes for partitionId:%d, taskId:%d, epochId:%d succeeded, committing...",
        partitionId, taskId, epochId);
    return PslWriterCommitMessage.create(futures.size());
  }

  @Override
  public synchronized void abort() {
    log.atWarning().log(
        "One or more writes for partitionId:%d, taskId:%d, epochId:%d failed, aborted.",
        partitionId, taskId, epochId);
  }
}
