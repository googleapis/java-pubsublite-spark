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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.cloud.pubsublite.TopicPath;
import com.google.common.flogger.GoogleLogger;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter;
import org.apache.spark.sql.types.StructType;

public class PslStreamWriter implements StreamWriter {

  private static final GoogleLogger log = GoogleLogger.forEnclosingClass();

  private final StructType inputSchema;
  private final TopicPath topicPath;
  private final PublisherFactory publisherFactory;

  public PslStreamWriter(
      StructType schema, TopicPath topicPath, PublisherFactory publisherFactory) {
    this.inputSchema = schema;
    this.topicPath = topicPath;
    this.publisherFactory = publisherFactory;
  }

  @Override
  public void commit(long epochId, WriterCommitMessage[] messages) {
    log.atInfo().log("Committed %d messages for epochId:%d.", countMessages(messages), epochId);
  }

  @Override
  public void abort(long epochId, WriterCommitMessage[] messages) {}

  private long countMessages(WriterCommitMessage[] messages) {
    long cnt = 0;
    for (WriterCommitMessage m : messages) {
      checkArgument(
          m instanceof PslWriterCommitMessage, "commit message not typed PslWriterCommitMessage");
      cnt += ((PslWriterCommitMessage) m).numMessages();
    }
    return cnt;
  }

  @Override
  public DataWriterFactory<InternalRow> createWriterFactory() {
    return new PslDataWriterFactory(inputSchema, topicPath, publisherFactory);
  }
}
