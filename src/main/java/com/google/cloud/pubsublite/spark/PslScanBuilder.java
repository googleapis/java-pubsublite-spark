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

import static com.google.cloud.pubsublite.internal.ExtractStatus.toCanonical;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.pubsublite.AdminClient;
import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.TopicPath;
import com.google.cloud.pubsublite.internal.CursorClient;
import com.google.cloud.pubsublite.internal.ExtractStatus;
import com.google.cloud.pubsublite.internal.TopicStatsClient;
import com.google.cloud.pubsublite.proto.ComputeMessageStatsResponse;
import com.google.cloud.pubsublite.proto.Cursor;
import com.google.cloud.pubsublite.spark.internal.PartitionCountReader;
import com.google.cloud.pubsublite.spark.internal.PerTopicHeadOffsetReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.Statistics;
import org.apache.spark.sql.connector.read.SupportsReportStatistics;
import org.apache.spark.sql.connector.read.streaming.ContinuousStream;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.types.StructType;

/**
 * Both Scan and ScanBuilder implementation, otherwise estimateStatistics() is not called due to bug
 * in DataSourceV2Relation.
 *
 * <p>Information from https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/b0b2a8add37bd044e5a9976ce635bb4230957f29/spark-bigquery-dsv2/spark-3.1-bigquery/src/main/java/com/google/cloud/spark/bigquery/v2/BigQueryScanBuilder.java#L30
 */
public class PslScanBuilder implements ScanBuilder, Scan, SupportsReportStatistics {
  private final PslReadDataSourceOptions options;

  public PslScanBuilder(PslReadDataSourceOptions options) {
    this.options = options;
  }

  @Override
  public Statistics estimateStatistics() {
    try (TopicStatsClient statsClient = options.newTopicStatsClient();
        CursorClient cursorClient = options.newCursorClient();
        PerTopicHeadOffsetReader headOffsetReader = options.newHeadOffsetReader()){
      Map<Partition, Offset> cursors = cursorClient.listPartitionCursors(options.subscriptionPath()).get();
      PslSourceOffset headOffset = headOffsetReader.getHeadOffset();
      List<ApiFuture<ComputeMessageStatsResponse>> stats = new ArrayList<>();
      TopicPath topicPath = options.getTopicPath();
      cursors.forEach((partition, offset) -> stats.add(statsClient.computeMessageStats(topicPath, partition, offset, headOffset.partitionOffsetMap().get(partition))));
      List<ComputeMessageStatsResponse> responses = ApiFutures.allAsList(stats).get();
      long bytes = 0;
      long messages = 0;
      for (ComputeMessageStatsResponse response : responses) {
        bytes += response.getMessageBytes();
        messages += response.getMessageCount();
      }
      final long finalBytes = bytes;
      final long finalMessages = messages;
      return new Statistics() {
        @Override
        public OptionalLong sizeInBytes() {
          return OptionalLong.of(finalBytes);
        }

        @Override
        public OptionalLong numRows() {
          return OptionalLong.of(finalMessages);
        }
      };
    } catch (Exception e) {
      throw ExtractStatus.toCanonical(e).underlying;
    }
  }

  @Override
  public StructType readSchema() {
    return SparkStructs.DEFAULT_SCHEMA;
  }

  @Override
  public String description() {
    return "Source for reading from Pub/Sub Lite";
  }

  @Override
  public MicroBatchStream toMicroBatchStream(String checkpointLocation) {
    return new PslMicroBatchStream(options);
  }

  @Override
  public ContinuousStream toContinuousStream(String checkpointLocation) {
    return new PslContinuousStream(options);
  }

  @Override
  public Scan build() {
    return this;
  }
}
