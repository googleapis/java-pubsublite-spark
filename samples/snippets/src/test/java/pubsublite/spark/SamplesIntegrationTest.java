/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package pubsublite.spark;

import static com.google.common.truth.Truth.assertThat;
import static pubsublite.spark.AdminUtils.createSubscriptionExample;
import static pubsublite.spark.AdminUtils.createTopicExample;
import static pubsublite.spark.AdminUtils.deleteSubscriptionExample;
import static pubsublite.spark.AdminUtils.deleteTopicExample;
import static pubsublite.spark.AdminUtils.subscriberExample;

import com.google.cloud.dataproc.v1.Job;
import com.google.cloud.dataproc.v1.SparkJob;
import com.google.cloud.pubsublite.SubscriptionName;
import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.TopicName;
import com.google.cloud.pubsublite.TopicPath;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.flogger.GoogleLogger;
import com.google.pubsub.v1.PubsubMessage;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.Before;
import org.junit.Test;

public class SamplesIntegrationTest extends SampleTestBase {

  private static final GoogleLogger log = GoogleLogger.forEnclosingClass();

  private SubscriptionName sourceSubscriptionName;
  private SubscriptionPath sourceSubscriptionPath;
  private TopicName destinationTopicId;
  private TopicPath destinationTopicPath;
  private SubscriptionName destinationSubscriptionName;
  private SubscriptionPath destinationSubscriptionPath;
  private Boolean initialized = false;

  @Before
  public void beforeClass() throws Exception {
    if (initialized) {
      return;
    }
    log.atInfo().log("RunId is: %s", runId);
    setupEnvVars();
    findMavenHome();

    // Maven package into jars
    mavenPackage(workingDir);
    mavenPackage(workingDir + "/samples");

    // Upload to GCS
    Storage storage =
        StorageOptions.newBuilder().setProjectId(projectId.value()).build().getService();
    uploadGCS(storage, sampleJarNameInGCS, sampleJarLoc);
    uploadGCS(storage, connectorJarNameInGCS, connectorJarLoc);
    initialized = true;
  }

  /** Note that source single word messages have been published to a permanent topic. */
  @Test
  public void testWordCount() throws Exception {
    UUID testId = UUID.randomUUID();
    setupSourceWithTestId(testId);
    setupDestinationWithTestId(testId);
    try {
      // Run Dataproc job, block until it finishes
      SparkJob.Builder sparkJobBuilder =
          SparkJob.newBuilder()
              .setMainClass("pubsublite.spark.WordCount")
              .putProperties("spark.submit.deployMode", "cluster")
              .putProperties(
                  "spark.yarn.appMasterEnv.SOURCE_SUBSCRIPTION_PATH",
                  sourceSubscriptionPath.toString())
              .putProperties(
                  "spark.yarn.appMasterEnv.DESTINATION_TOPIC_PATH",
                  destinationTopicPath.toString());
      runDataprocJob(sparkJobBuilder);
      // Verify final destination messages in Pub/Sub Lite
      verifyWordCountResultViaPSL();
    } finally {
      deleteSubscriptionExample(cloudRegion.value(), sourceSubscriptionPath);
      deleteSubscriptionExample(cloudRegion.value(), destinationSubscriptionPath);
      deleteTopicExample(cloudRegion.value(), destinationTopicPath);
    }
  }

  @Test
  public void testSimpleRead() throws Exception {
    UUID testId = UUID.randomUUID();
    setupSourceWithTestId(testId);
    try {
      // Run Dataproc job, block until it finishes
      SparkJob.Builder sparkJobBuilder =
          SparkJob.newBuilder()
              .setMainClass("pubsublite.spark.SimpleRead")
              .addArgs(sourceSubscriptionPath.toString());
      Job job = runDataprocJob(sparkJobBuilder);
      // Verify results in console
      verifyConsoleOutput(job);
    } finally {
      deleteSubscriptionExample(cloudRegion.value(), sourceSubscriptionPath);
    }
  }

  @Test
  public void testSimpleWrite() throws Exception {
    UUID testId = UUID.randomUUID();
    setupDestinationWithTestId(testId);
    try {
      // Run Dataproc job, block until it finishes
      SparkJob.Builder sparkJobBuilder =
          SparkJob.newBuilder()
              .setMainClass("pubsublite.spark.SimpleWrite")
              .putProperties("spark.submit.deployMode", "cluster")
              .putProperties(
                  "spark.yarn.appMasterEnv.DESTINATION_TOPIC_PATH",
                  destinationTopicPath.toString());
      runDataprocJob(sparkJobBuilder);
      // Verify write results in PSL
      verifySimpleWriteResultViaPSL();
    } finally {
      deleteSubscriptionExample(cloudRegion.value(), destinationSubscriptionPath);
      deleteTopicExample(cloudRegion.value(), destinationTopicPath);
    }
  }

  private void setupSourceWithTestId(UUID testId) throws Exception {
    sourceSubscriptionName = SubscriptionName.of("sample-integration-sub-source-" + testId);
    sourceSubscriptionPath =
        SubscriptionPath.newBuilder()
            .setProject(projectId)
            .setLocation(cloudZone)
            .setName(sourceSubscriptionName)
            .build();
    createSubscriptionExample(
        cloudRegion.value(),
        cloudZone.zoneId(),
        projectNumber.value(),
        sourceTopicId.value(),
        sourceSubscriptionName.value());
  }

  private void setupDestinationWithTestId(UUID testId) throws Exception {
    destinationTopicId = TopicName.of("sample-integration-topic-destination-" + testId);
    destinationTopicPath =
        TopicPath.newBuilder()
            .setProject(projectId)
            .setLocation(cloudZone)
            .setName(destinationTopicId)
            .build();
    destinationSubscriptionName =
        SubscriptionName.of("sample-integration-sub-destination-" + runId);
    destinationSubscriptionPath =
        SubscriptionPath.newBuilder()
            .setProject(projectId)
            .setLocation(cloudZone)
            .setName(destinationSubscriptionName)
            .build();
    createTopicExample(
        cloudRegion.value(),
        cloudZone.zoneId(),
        projectNumber.value(),
        destinationTopicId.value(),
        /*partitions=*/ 1);
    createSubscriptionExample(
        cloudRegion.value(),
        cloudZone.zoneId(),
        projectNumber.value(),
        destinationTopicId.value(),
        destinationSubscriptionName.value());
  }

  private void verifyWordCountResultViaPSL() {
    Map<String, Integer> expected = new HashMap<>();
    expected.put("the", 24);
    expected.put("of", 16);
    expected.put("and", 14);
    expected.put("i", 13);
    expected.put("my", 10);
    expected.put("a", 6);
    expected.put("in", 5);
    expected.put("that", 5);
    expected.put("soul", 4);
    expected.put("with", 4);
    expected.put("as", 3);
    expected.put("feel", 3);
    expected.put("like", 3);
    expected.put("me", 3);
    expected.put("so", 3);
    expected.put("then", 3);
    expected.put("us", 3);
    expected.put("when", 3);
    expected.put("which", 3);
    expected.put("am", 2);
    Map<String, Integer> actual = new HashMap<>();
    Queue<PubsubMessage> results =
        subscriberExample(
            cloudRegion.value(),
            cloudZone.zoneId(),
            projectNumber.value(),
            destinationSubscriptionName.value());
    for (PubsubMessage m : results) {
      String[] pair = m.getData().toStringUtf8().split("_");
      actual.put(pair[0], Integer.parseInt(pair[1]));
    }
    assertThat(actual).containsAtLeastEntriesIn(expected);
  }

  private void verifySimpleWriteResultViaPSL() {
    Queue<PubsubMessage> results =
        subscriberExample(
            cloudRegion.value(),
            cloudZone.zoneId(),
            projectNumber.value(),
            destinationSubscriptionName.value());
    // The streaming query runs for 60s, and rate source generate one row per sec.
    assertThat(results.size()).isGreaterThan(10);
    for (PubsubMessage m : results) {
      assertThat(m.getOrderingKey()).isEqualTo("testkey");
      assertThat(m.getData().toStringUtf8()).startsWith("data_");
    }
  }

  private void verifyConsoleOutput(Job job) {
    Storage storage =
        StorageOptions.newBuilder().setProjectId(projectId.value()).build().getService();
    Matcher matches = Pattern.compile("gs://(.*?)/(.*)").matcher(job.getDriverOutputResourceUri());
    assertThat(matches.matches()).isTrue();

    Blob blob = storage.get(matches.group(1), String.format("%s.000000000", matches.group(2)));
    String sparkJobOutput = new String(blob.getContent());
    log.atInfo().log(sparkJobOutput);
    String expectedWordCountResult =
        "-------------------------------------------\n"
            + "Batch: 0\n"
            + "-------------------------------------------\n"
            + "+--------------------+---------+------+---+--------------------+"
            + "--------------------+---------------+----------+\n"
            + "|        subscription|partition|offset|key|                data|"
            + "   publish_timestamp|event_timestamp|attributes|\n"
            + "+--------------------+---------+------+---+--------------------+"
            + "--------------------+---------------+----------+\n"
            + "|projects/java-doc...|        0|     0| []|          [61 5F 31]|"
            + "2021-02-01 23:26:...|           null|        []|\n"
            + "|projects/java-doc...|        0|     1| []|[77 6F 6E 64 65 7...|"
            + "2021-02-01 23:26:...|           null|        []|\n"
            + "|projects/java-doc...|        0|     2| []|[73 65 72 65 6E 6...|"
            + "2021-02-01 23:26:...|           null|        []|\n"
            + "|projects/java-doc...|        0|     3| []|    [68 61 73 5F 31]|"
            + "2021-02-01 23:26:...|           null|        []|\n"
            + "|projects/java-doc...|        0|     4| []|[74 61 6B 65 6E 5...|"
            + "2021-02-01 23:26:...|           null|        []|\n"
            + "|projects/java-doc...|        0|     5| []|[70 6F 73 73 65 7...|"
            + "2021-02-01 23:26:...|           null|        []|\n"
            + "|projects/java-doc...|        0|     6| []|       [6F 66 5F 31]|"
            + "2021-02-01 23:26:...|           null|        []|\n"
            + "|projects/java-doc...|        0|     7| []|       [6D 79 5F 31]|"
            + "2021-02-01 23:26:...|           null|        []|\n"
            + "|projects/java-doc...|        0|     8| []|[65 6E 74 69 72 6...|"
            + "2021-02-01 23:26:...|           null|        []|\n"
            + "|projects/java-doc...|        0|     9| []| [73 6F 75 6C 5F 31]|"
            + "2021-02-01 23:26:...|           null|        []|\n"
            + "|projects/java-doc...|        0|    10| []| [6C 69 6B 65 5F 31]|"
            + "2021-02-01 23:26:...|           null|        []|\n"
            + "|projects/java-doc...|        0|    11| []|[74 68 65 73 65 5...|"
            + "2021-02-01 23:26:...|           null|        []|\n"
            + "|projects/java-doc...|        0|    12| []|[73 77 65 65 74 5...|"
            + "2021-02-01 23:26:...|           null|        []|\n"
            + "|projects/java-doc...|        0|    13| []|[6D 6F 72 6E 69 6...|"
            + "2021-02-01 23:26:...|           null|        []|\n"
            + "|projects/java-doc...|        0|    14| []|       [6F 66 5F 31]|"
            + "2021-02-01 23:26:...|           null|        []|\n"
            + "|projects/java-doc...|        0|    15| []|[73 70 72 69 6E 6...|"
            + "2021-02-01 23:26:...|           null|        []|\n"
            + "|projects/java-doc...|        0|    16| []|[77 68 69 63 68 5...|"
            + "2021-02-01 23:26:...|           null|        []|\n"
            + "|projects/java-doc...|        0|    17| []|          [69 5F 31]|"
            + "2021-02-01 23:26:...|           null|        []|\n"
            + "|projects/java-doc...|        0|    18| []|[65 6E 6A 6F 79 5...|"
            + "2021-02-01 23:26:...|           null|        []|\n"
            + "|projects/java-doc...|        0|    19| []| [77 69 74 68 5F 31]|"
            + "2021-02-01 23:26:...|           null|        []|\n"
            + "+--------------------+---------+------+---+--------------------+"
            + "--------------------+"
            + "---------------+----------+\n"
            + "only showing top 20 rows";
    assertThat(sparkJobOutput).contains(expectedWordCountResult);
  }
}
