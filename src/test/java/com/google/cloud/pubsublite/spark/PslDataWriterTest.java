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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.api.core.ApiFutures;
import com.google.cloud.pubsublite.MessageMetadata;
import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.internal.Publisher;
import com.google.cloud.pubsublite.internal.testing.UnitTestExamples;
import java.io.IOException;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataType;
import org.junit.Test;

public class PslDataWriterTest {

  private final InternalRow row = mock(InternalRow.class);

  @SuppressWarnings("unchecked")
  private final Publisher<MessageMetadata> publisher = mock(Publisher.class);

  private final CachedPublishers cachedPublishers = mock(CachedPublishers.class);
  private final PslDataWriter writer =
      new PslDataWriter(
          1L,
          2L,
          3L,
          Constants.DEFAULT_SCHEMA,
          PslWriteDataSourceOptions.builder()
              .setTopicPath(UnitTestExamples.exampleTopicPath())
              .build(),
          cachedPublishers);

  @Test
  public void testAllSuccess() throws IOException {
    when(cachedPublishers.getOrCreate(any())).thenReturn(publisher);
    when(publisher.publish(any()))
        .thenReturn(
            ApiFutures.immediateFuture(MessageMetadata.of(Partition.of(0L), Offset.of(0L))));
    when(row.get(anyInt(), any(DataType.class))).thenReturn(0);
    writer.write(row);
    writer.write(row);
    assertThat(writer.commit()).isEqualTo(PslWriterCommitMessage.create(2));
  }

  @Test
  public void testPartialFail() {
    when(cachedPublishers.getOrCreate(any())).thenReturn(publisher);
    when(publisher.publish(any()))
        .thenReturn(ApiFutures.immediateFuture(MessageMetadata.of(Partition.of(0L), Offset.of(0L))))
        .thenReturn(ApiFutures.immediateFailedFuture(new InternalError("")));
    when(row.get(anyInt(), any(DataType.class))).thenReturn(0);
    writer.write(row);
    writer.write(row);
    assertThrows(IOException.class, writer::commit);
  }
}
