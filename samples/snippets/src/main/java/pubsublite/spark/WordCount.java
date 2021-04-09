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
import static org.apache.spark.sql.functions.split;

import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;

public class WordCount {

  private static final String SOURCE_SUBSCRIPTION_PATH = "SOURCE_SUBSCRIPTION_PATH";
  private static final String DESTINATION_TOPIC_PATH = "DESTINATION_TOPIC_PATH";

  public static void main(String[] args) throws Exception {
    Map<String, String> env =
        CommonUtils.getAndValidateEnvVars(SOURCE_SUBSCRIPTION_PATH, DESTINATION_TOPIC_PATH);
    wordCount(
        Objects.requireNonNull(env.get(SOURCE_SUBSCRIPTION_PATH)),
        Objects.requireNonNull(env.get(DESTINATION_TOPIC_PATH)));
  }

  private static void wordCount(String sourceSubscriptionPath, String destinationTopicPath)
      throws Exception {
    final String appId = UUID.randomUUID().toString();

    SparkSession spark =
        SparkSession.builder()
            .appName(String.format("Word count (ID: %s)", appId))
            .master("yarn")
            .getOrCreate();

    // Read messages from Pub/Sub Lite
    Dataset<Row> df =
        spark
            .readStream()
            .format("pubsublite")
            .option("pubsublite.subscription", sourceSubscriptionPath)
            .load();

    // Aggregate word counts
    Column splitCol = split(df.col("data"), "_");
    df =
        df.withColumn("word", splitCol.getItem(0))
            .withColumn("word_count", splitCol.getItem(1).cast(DataTypes.LongType));
    df = df.groupBy("word").sum("word_count");
    df = df.orderBy(df.col("sum(word_count)").desc(), df.col("word").asc());

    // Add Pub/Sub Lite message data field
    df =
        df.withColumn(
            "data",
            concat(df.col("word"), lit("_"), df.col("sum(word_count)")).cast(DataTypes.BinaryType));

    // Write word count results to Pub/Sub Lite
    StreamingQuery query =
        df.writeStream()
            .format("pubsublite")
            .option("pubsublite.topic", destinationTopicPath)
            .option("checkpointLocation", String.format("/tmp/checkpoint-%s", appId))
            .outputMode(OutputMode.Complete())
            .trigger(Trigger.ProcessingTime(1, TimeUnit.SECONDS))
            .start();

    // Wait enough time to execute query
    query.awaitTermination(60 * 1000); // 60s
    query.stop();
  }
}
