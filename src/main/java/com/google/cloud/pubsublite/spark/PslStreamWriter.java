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
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter;
import org.apache.spark.sql.types.StructType;

public class PslStreamWriter implements StreamWriter {

  private static final GoogleLogger log = GoogleLogger.forEnclosingClass();

  private final StructType inputSchema;
  private final PslWriteDataSourceOptions writeOptions;

  public PslStreamWriter(StructType inputSchema, PslWriteDataSourceOptions writeOptions) {
    this.inputSchema = inputSchema;
    this.writeOptions = writeOptions;
  }

  @Override
  public void commit(long epochId, WriterCommitMessage[] messages) {
    log.atInfo().log("Committed %d messages for epochId:%d.", countMessages(messages), epochId);
  }

  @Override
  public void abort(long epochId, WriterCommitMessage[] messages) {
    log.atWarning().log(
        "Epoch id: %d is aborted, %d messages might have been published.",
        epochId, countMessages(messages));
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

  @Override
  public DataWriterFactory<InternalRow> createWriterFactory() {
    return new PslDataWriterFactory(inputSchema, writeOptions);
  }
}
