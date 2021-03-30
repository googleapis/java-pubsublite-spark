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

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;

import java.util.concurrent.TimeUnit;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.split;

public class WordCount {

  public static void main(String[] args) throws Exception {
    String subscription_path_raw = args[0];
    String topic_path_result = args[1];

    SparkSession spark = SparkSession.builder().appName("Word count").master("yarn").getOrCreate();

    // Reads messages from Pub/Sub Lite
    Dataset<Row> df =
        spark.readStream().format("pubsublite").option("pubsublite.subscription", subscription_path_raw).load();

    // Aggregate word counts
    Column splitCol = split(df.col("data"), "_");
    df =
        df.withColumn("word", splitCol.getItem(0))
            .withColumn("word_count", splitCol.getItem(1).cast(DataTypes.LongType));
    df = df.groupBy("word").sum("word_count");
    df = df.orderBy(df.col("sum(word_count)").desc(), df.col("word").asc());

    // Adds Pub/Sub Lite message data field
    df = df.withColumn("data", concat(df.col("word"),
            lit("_"), df.col("sum(word_count)")));

    // Write word count results to Pub/Sub Lite
    StreamingQuery query =
        df.writeStream()
            .format("pubsublite")
            .option("pubsublite.topic", topic_path_result)
            .option("checkpointLocation", "/tmp/checkpoint")
            .outputMode(OutputMode.Complete())
            .trigger(Trigger.ProcessingTime(1, TimeUnit.SECONDS))
            .start();
    query.awaitTermination(60 * 1000); // 60s
    query.stop();
  }
}
