/*
 * Copyright 2021 Google LLC
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

package pubsublite.spark;

import com.google.common.collect.ImmutableList;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.ByteArray;

public class SimpleWrite {
  private static final StructType TABLE_SCHEMA =
      new StructType(
          new StructField[] {
            new StructField("key", DataTypes.BinaryType, false, Metadata.empty()),
            new StructField("data", DataTypes.BinaryType, false, Metadata.empty())
          });
  private static final String DESTINATION_TOPIC_PATH = "DESTINATION_TOPIC_PATH";

  public static void main(String[] args) throws Exception {

    Map<String, String> env = CommonUtils.getAndValidateEnvVars(DESTINATION_TOPIC_PATH);
    final String appId = UUID.randomUUID().toString();
    SparkSession spark =
        SparkSession.builder()
            .appName(String.format("Simple write (ID: %s)", appId))
            .master("yarn")
            .getOrCreate();

    // Read messages from Pub/Sub Lite
    Dataset<Row> df =
        spark.createDataFrame(
            ImmutableList.of(
                createRow("key1", "foo"), createRow("key1", "bar"), createRow("key2", "foobar")),
            TABLE_SCHEMA);

    // Write word count results to Pub/Sub Lite
    StreamingQuery query =
        df.writeStream()
            .format("pubsublite")
            .option("pubsublite.topic", Objects.requireNonNull(env.get(DESTINATION_TOPIC_PATH)))
            .option("checkpointLocation", String.format("/tmp/checkpoint-%s", appId))
            .outputMode(OutputMode.Complete())
            .trigger(Trigger.ProcessingTime(1, TimeUnit.SECONDS))
            .start();

    // Wait enough time to execute query
    query.awaitTermination(60 * 1000); // 60s
    query.stop();
  }

  private static GenericRow createRow(String key, String data) {
    Object[] list = {
      ByteArray.concat(key.getBytes(StandardCharsets.UTF_8)),
      ByteArray.concat(data.getBytes(StandardCharsets.UTF_8))
    };
    return new GenericRow(list);
  }
}
