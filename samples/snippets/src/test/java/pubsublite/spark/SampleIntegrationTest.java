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
import static pubsublite.spark.AdminUtils.deleteSubscriptionExample;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.dataproc.v1.Job;
import com.google.cloud.dataproc.v1.JobControllerClient;
import com.google.cloud.dataproc.v1.JobControllerSettings;
import com.google.cloud.dataproc.v1.JobMetadata;
import com.google.cloud.dataproc.v1.JobPlacement;
import com.google.cloud.dataproc.v1.SparkJob;
import com.google.cloud.pubsublite.CloudRegion;
import com.google.cloud.pubsublite.CloudZone;
import com.google.cloud.pubsublite.ProjectId;
import com.google.cloud.pubsublite.SubscriptionName;
import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.TopicName;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.maven.shared.invoker.DefaultInvocationRequest;
import org.apache.maven.shared.invoker.DefaultInvoker;
import org.apache.maven.shared.invoker.InvocationRequest;
import org.apache.maven.shared.invoker.Invoker;
import org.apache.maven.shared.invoker.MavenInvocationException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.spark_project.guava.collect.ImmutableList;

public class SampleIntegrationTest {

  private static final CloudRegion CLOUD_REGION = CloudRegion.of("us-central1");
  CloudZone CLOUD_ZONE = CloudZone.of(CLOUD_REGION, 'b');
  ProjectId PROEJCT_ID = ProjectId.of("pubsub-lite-load-tests");
  TopicName TOPIC_NAME = TopicName.of("jiangmichael-sample-integration");
  SubscriptionName SUBSCRIPTION_NAME =
      SubscriptionName.of("sample-integration-sub-" + System.currentTimeMillis());
  SubscriptionPath SUBSCIPTION_PATH =
      SubscriptionPath.newBuilder()
          .setProject(PROEJCT_ID)
          .setLocation(CLOUD_ZONE)
          .setName(SUBSCRIPTION_NAME)
          .build();
  String CLUSTER_NAME = "jiangmichael-psl-spark-connector-test-cluster-2";
  String BUCKET_NAME = "jiangmichael-spark-sample-integration";
  String WORKING_DIR = System.getProperty("user.dir").replace("/samples/snapshot", "");
  String SAMPLE_JAR_NAME = "pubsublite-spark-snapshot-1.0.21.jar";
  String CONNECTOR_JAR_NAME = "pubsublite-spark-sql-streaming-with-dependencies-0.1.0-SNAPSHOT.jar";
  String SAMPLE_JAR_LOC =
      String.format("%s/samples/snapshot/target/%s", WORKING_DIR, SAMPLE_JAR_NAME);
  String CONNECTOR_JAR_LOC = String.format("%s/target/%s", WORKING_DIR, CONNECTOR_JAR_NAME);

  private static void mavenPackage(String workingDir) throws MavenInvocationException {
    InvocationRequest request = new DefaultInvocationRequest();
    request.setPomFile(new File(workingDir + "/pom.xml"));
    request.setGoals(ImmutableList.of("clean", "package", "-Dmaven.test.skip=true"));
    Invoker invoker = new DefaultInvoker();
    invoker.setMavenHome(new File("/opt/apache-maven-3.5.3"));
    assertThat(invoker.execute(request).getExitCode()).isEqualTo(0);
  }

  @Before
  public void setUp() throws Exception {
    // Create a subscription
    createSubscriptionExample(
        CLOUD_REGION.value(),
        CLOUD_ZONE.zoneId(),
        PROEJCT_ID.value(),
        TOPIC_NAME.value(),
        SUBSCRIPTION_NAME.value());
  }

  @After
  public void tearDown() throws Exception {
    // Cleanup the subscription
    deleteSubscriptionExample(
        CLOUD_REGION.value(), CLOUD_ZONE.zoneId(), PROEJCT_ID.value(), SUBSCRIPTION_NAME.value());
  }

  @Test
  public void test() throws Exception {
    // Maven package into jars
    mavenPackage(WORKING_DIR);
    mavenPackage(WORKING_DIR + "/samples");

    // Upload to GCS
    Storage storage =
        StorageOptions.newBuilder().setProjectId(PROEJCT_ID.value()).build().getService();
    BlobId blobId = BlobId.of(BUCKET_NAME, SAMPLE_JAR_NAME);
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
    storage.create(blobInfo, Files.readAllBytes(Paths.get(SAMPLE_JAR_LOC)));
    blobId = BlobId.of(BUCKET_NAME, CONNECTOR_JAR_NAME);
    blobInfo = BlobInfo.newBuilder(blobId).build();
    storage.create(blobInfo, Files.readAllBytes(Paths.get(CONNECTOR_JAR_LOC)));

    // Run Dataproc job
    String myEndpoint = String.format("%s-dataproc.googleapis.com:443", CLOUD_REGION.value());
    JobControllerSettings jobControllerSettings =
        JobControllerSettings.newBuilder().setEndpoint(myEndpoint).build();

    try (JobControllerClient jobControllerClient =
        JobControllerClient.create(jobControllerSettings)) {
      JobPlacement jobPlacement = JobPlacement.newBuilder().setClusterName(CLUSTER_NAME).build();
      SparkJob sparkJob =
          SparkJob.newBuilder()
              .addJarFileUris(String.format("gs://%s/%s", BUCKET_NAME, SAMPLE_JAR_NAME))
              .addJarFileUris(String.format("gs://%s/%s", BUCKET_NAME, CONNECTOR_JAR_NAME))
              .setMainClass("pubsublite.spark.WordCount")
              .addArgs(SUBSCIPTION_PATH.toString())
              .build();
      Job job = Job.newBuilder().setPlacement(jobPlacement).setSparkJob(sparkJob).build();
      OperationFuture<Job, JobMetadata> submitJobAsOperationAsyncRequest =
          jobControllerClient.submitJobAsOperationAsync(
              PROEJCT_ID.value(), CLOUD_REGION.value(), job);
      Job jobResponse = submitJobAsOperationAsyncRequest.get();

      // Check Dataproc job output from GCS
      Matcher matches =
          Pattern.compile("gs://(.*?)/(.*)").matcher(jobResponse.getDriverOutputResourceUri());
      assertThat(matches.matches()).isTrue();

      Blob blob = storage.get(matches.group(1), String.format("%s.000000000", matches.group(2)));
      String sparkJobOutput = new String(blob.getContent());
      String expectedWordCountResult =
          "+-----+---------------+\n"
              + "| word|sum(word_count)|\n"
              + "+-----+---------------+\n"
              + "|  the|             24|\n"
              + "|   of|             16|\n"
              + "|  and|             14|\n"
              + "|    i|             13|\n"
              + "|   my|             10|\n"
              + "|    a|              6|\n"
              + "|   in|              5|\n"
              + "| that|              5|\n"
              + "| with|              4|\n"
              + "| soul|              4|\n"
              + "|   us|              3|\n"
              + "|   me|              3|\n"
              + "| when|              3|\n"
              + "| feel|              3|\n"
              + "| like|              3|\n"
              + "|   so|              3|\n"
              + "|   as|              3|\n"
              + "| then|              3|\n"
              + "|which|              3|\n"
              + "|among|              2|\n"
              + "+-----+---------------+\n"
              + "only showing top 20 rows";
      assertThat(sparkJobOutput).contains(expectedWordCountResult);
    }
  }
}
