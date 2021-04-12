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

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;

public class SimpleWrite {
  private static final String DESTINATION_TOPIC_PATH = "DESTINATION_TOPIC_PATH";

  public static void main(String[] args) throws Exception {
    Map<String, String> env = CommonUtils.getAndValidateEnvVars(DESTINATION_TOPIC_PATH);
    simpleWrite(Objects.requireNonNull(env.get(DESTINATION_TOPIC_PATH)));
  }

  private static void simpleWrite(String destinationTopicPath) throws Exception {
    final String appId = UUID.randomUUID().toString();
    SparkSession spark =
        SparkSession.builder()
            .appName(String.format("Simple write (ID: %s)", appId))
            .master("yarn")
            .getOrCreate();

    // Generate rate source, 1 row per second
    Dataset<Row> df = spark.readStream().format("rate").load();
    df =
        df.withColumn("key", lit("testkey").cast(DataTypes.BinaryType))
            .withColumn("data", concat(lit("data_"), df.col("value")).cast(DataTypes.BinaryType));

    // Write word count results to Pub/Sub Lite
    StreamingQuery query =
        df.writeStream()
            .format("pubsublite")
            .option("pubsublite.topic", destinationTopicPath)
            .option("checkpointLocation", String.format("/tmp/checkpoint-%s", appId))
            .outputMode(OutputMode.Append())
            .trigger(Trigger.ProcessingTime(1, TimeUnit.SECONDS))
            .start();

    // Wait enough time to execute query
    query.awaitTermination(60 * 1000); // 60s
    query.stop();
  }
}
