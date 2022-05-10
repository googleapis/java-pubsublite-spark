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

import com.google.cloud.pubsublite.internal.testing.UnitTestExamples;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.PhysicalWriteInfoImpl;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.junit.Test;

public class PslWriteTest {

  private final PslWrite writer =
      new PslWrite(
          SparkStructs.DEFAULT_SCHEMA,
          PslWriteDataSourceOptions.builder()
              .setTopicPath(UnitTestExamples.exampleTopicPath())
              .build());
  private final PslWriterCommitMessage message1 = PslWriterCommitMessage.create(10);
  private final PslWriterCommitMessage message2 = PslWriterCommitMessage.create(5);

  private static class AbortCommitMessage implements WriterCommitMessage {}

  @Test
  public void testCommit() {
    writer.commit(100, new WriterCommitMessage[] {message1, message2});
  }

  @Test
  public void testAbort() {
    writer.abort(100, new WriterCommitMessage[] {message1, message2, new AbortCommitMessage()});
  }

  @Test
  public void testCreateFactory() {
    PhysicalWriteInfo info = new PhysicalWriteInfoImpl(42);
    writer.toBatch().createBatchWriterFactory(info);
    writer.toStreaming().createStreamingWriterFactory(info);
  }
}
