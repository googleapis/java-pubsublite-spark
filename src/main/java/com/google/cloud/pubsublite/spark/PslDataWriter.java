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
import com.google.api.core.ApiService;
import com.google.cloud.pubsublite.MessageMetadata;
import com.google.cloud.pubsublite.internal.Publisher;
import com.google.cloud.pubsublite.spark.internal.PublisherFactory;
import com.google.common.flogger.GoogleLogger;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import javax.annotation.concurrent.GuardedBy;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;

public class PslDataWriter implements DataWriter<InternalRow> {
  private static final GoogleLogger log = GoogleLogger.forEnclosingClass();

  private final long partitionId, taskId, epochId;
  private final StructType inputSchema;
  private final PublisherFactory publisherFactory;

  @GuardedBy("this")
  private Optional<Publisher<MessageMetadata>> publisher = Optional.empty();

  @GuardedBy("this")
  private final List<ApiFuture<MessageMetadata>> futures = new ArrayList<>();

  public PslDataWriter(
      long partitionId,
      long taskId,
      long epochId,
      StructType schema,
      PublisherFactory publisherFactory) {
    this.partitionId = partitionId;
    this.taskId = taskId;
    this.epochId = epochId;
    this.inputSchema = schema;
    this.publisherFactory = publisherFactory;
  }

  @Override
  public synchronized void write(InternalRow record) {
    if (!publisher.isPresent() || publisher.get().state() != ApiService.State.RUNNING) {
      publisher = Optional.of(publisherFactory.newPublisher());
    }
    futures.add(
        publisher
            .get()
            .publish(Objects.requireNonNull(PslSparkUtils.toPubSubMessage(inputSchema, record))));
  }

  @Override
  public synchronized WriterCommitMessage commit() throws IOException {
    for (ApiFuture<MessageMetadata> f : futures) {
      try {
        f.get();
      } catch (InterruptedException | ExecutionException e) {
        publisher = Optional.empty();
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

  @Override
  public void close() throws IOException {}
}
