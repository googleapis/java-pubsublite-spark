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

public class SimpleRead {

  private static final String SOURCE_SUBSCRIPTION_PATH = "SOURCE_SUBSCRIPTION_PATH";

  public static void main(String[] args) throws Exception {
    Map<String, String> env = CommonUtils.getAndValidateEnvVars(SOURCE_SUBSCRIPTION_PATH);
    final String appId = UUID.randomUUID().toString();

    SparkSession spark =
        SparkSession.builder()
            .appName(String.format("Simple read (ID: %s)", appId))
            .master("yarn")
            .getOrCreate();

    // Read messages from Pub/Sub Lite
    Dataset<Row> df =
        spark
            .readStream()
            .format("pubsublite")
            .option(
                "pubsublite.subscription",
                Objects.requireNonNull(env.get(SOURCE_SUBSCRIPTION_PATH)))
            .load();

    // Write messages to Console Output
    StreamingQuery query =
        df.writeStream()
            .format("console")
            .outputMode(OutputMode.Append())
            .trigger(Trigger.ProcessingTime(1, TimeUnit.SECONDS))
            .start();

    // Wait enough time to execute query
    query.awaitTermination(60 * 1000); // 60s
    query.stop();
  }
}
