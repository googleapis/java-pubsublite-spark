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

import com.google.common.flogger.GoogleLogger;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;
import org.apache.spark.sql.internal.connector.SupportsStreamingUpdateAsAppend;
import org.apache.spark.sql.types.StructType;

/**
 * Pub/Sub Lite class for writing.
 *
 * <p>Note that SupportsStreamingUpdateAsAppend is the same hack that <a
 * href="https://github.com/apache/spark/commit/db89b0e1b8bb98db6672f2b89e42e8a14e06e745">kafka</a>
 * uses to opt-in to writing aggregates without requiring windowing.
 */
public class PslWrite
    implements WriteBuilder, SupportsStreamingUpdateAsAppend, BatchWrite, StreamingWrite {
  private static final GoogleLogger log = GoogleLogger.forEnclosingClass();

  private final StructType inputSchema;
  private final PslWriteDataSourceOptions writeOptions;

  public PslWrite(StructType inputSchema, PslWriteDataSourceOptions writeOptions) {
    this.inputSchema = inputSchema;
    this.writeOptions = writeOptions;
  }

  @Override
  public void commit(long epochId, WriterCommitMessage[] messages) {
    log.atInfo().log("Committed %d messages for epochId:%d.", countMessages(messages), epochId);
  }

  @Override
  public void commit(WriterCommitMessage[] messages) {
    commit(-1, messages);
  }

  @Override
  public void abort(long epochId, WriterCommitMessage[] messages) {
    log.atWarning().log(
        "Epoch id: %d is aborted, %d messages might have been published.",
        epochId, countMessages(messages));
  }

  @Override
  public void abort(WriterCommitMessage[] messages) {
    abort(-1, messages);
  }

  private long countMessages(WriterCommitMessage[] messages) {
    long cnt = 0;
    for (WriterCommitMessage m : messages) {
      // It's not guaranteed to be typed PslWriterCommitMessage when abort.
      if (m instanceof PslWriterCommitMessage) {
        cnt += ((PslWriterCommitMessage) m).numMessages();
      }
    }
    return cnt;
  }

  private PslDataWriterFactory newWriterFactory() {
    return new PslDataWriterFactory(inputSchema, writeOptions);
  }

  @Override
  public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
    return newWriterFactory();
  }

  @Override
  public StreamingDataWriterFactory createStreamingWriterFactory(PhysicalWriteInfo info) {
    return newWriterFactory();
  }

  @Override
  public BatchWrite buildForBatch() {
    return this;
  }

  @Override
  public StreamingWrite buildForStreaming() {
    return this;
  }

  @Override
public  boolean useCommitCoordinator() {
    return false;
}
