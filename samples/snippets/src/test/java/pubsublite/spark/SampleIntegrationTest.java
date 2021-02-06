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
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Preconditions;
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

  private static final String CLOUD_REGION = "CLOUD_REGION";
  private static final String CLOUD_ZONE = "CLOUD_ZONE";
  private static final String PROJECT_ID = "PROJECT_ID";
  private static final String TOPIC_NAME = "TOPIC_NAME";
  private static final String CLUSTER_NAME = "CLUSTER_NAME";
  private static final String BUCKET_NAME = "BUCKET_NAME";
  private static final String SAMPLE_VERSION = "SAMPLE_VERSION";
  private static final String CONNECTOR_VERSION = "CONNECTOR_VERSION";
  private static final String MAVEN_HOME = "MAVEN_HOME";

  private CloudRegion cloudRegion;
  private CloudZone cloudZone;
  private ProjectId projectId;
  private TopicName topicName;
  private SubscriptionName subscriptionName;
  private SubscriptionPath subscriptionPath;
  private String clusterName;
  private String bucketName;
  private String workingDir;
  private String mavenHome;
  private String sampleVersion;
  private String connectorVersion;
  private String sampleJarName;
  private String connectorJarName;
  private String sampleJarLoc;
  private String connectorJarLoc;


  private void mavenPackage(String workingDir) throws MavenInvocationException {
    InvocationRequest request = new DefaultInvocationRequest();
    request.setPomFile(new File(workingDir + "/pom.xml"));
    request.setGoals(ImmutableList.of("clean", "package", "-Dmaven.test.skip=true"));
    Invoker invoker = new DefaultInvoker();
    invoker.setMavenHome(new File(mavenHome));
    assertThat(invoker.execute(request).getExitCode()).isEqualTo(0);
  }


  private void setUpVariables() {
    Map<String, String> env = System.getenv();
    Preconditions.checkState(env.keySet().containsAll(ImmutableList.of(
            CLOUD_REGION,
            CLOUD_ZONE,
            PROJECT_ID,
            TOPIC_NAME,
            CLUSTER_NAME,
            BUCKET_NAME,
            SAMPLE_VERSION,
            CONNECTOR_VERSION,
            MAVEN_HOME
    )));
    cloudRegion = CloudRegion.of(env.get(CLOUD_REGION));
    cloudZone =
            CloudZone.of(cloudRegion, env.get(CLOUD_ZONE).charAt(0));
    projectId = ProjectId.of(env.get(PROJECT_ID));
    topicName = TopicName.of(env.get(TOPIC_NAME));
    subscriptionName =
            SubscriptionName.of("sample-integration-sub-" + UUID.randomUUID());
    subscriptionPath =
            SubscriptionPath.newBuilder()
                    .setProject(projectId)
                    .setLocation(cloudZone)
                    .setName(subscriptionName)
                    .build();
    clusterName = env.get(CLUSTER_NAME);
    bucketName = env.get(BUCKET_NAME);
    workingDir =
            System.getProperty("user.dir").replace("/samples/snippets", "");
    sampleVersion = env.get(SAMPLE_VERSION);
    connectorVersion = env.get(CONNECTOR_VERSION);
    sampleJarName =
            String.format("pubsublite-spark-snippets-%s.jar", sampleVersion);
    connectorJarName =
            String.format(
                    "pubsublite-spark-sql-streaming-with-dependencies-%s.jar",
                    connectorVersion);
    sampleJarLoc =
            String.format("%s/samples/snippets/target/%s", workingDir, sampleJarName);
    connectorJarLoc =
            String.format("%s/target/%s", workingDir, connectorJarName);
  }

  @Before
  public void setUp() throws Exception {
    setUpVariables();

    // Create a subscription
    createSubscriptionExample(
        cloudRegion.value(),
        cloudZone.zoneId(),
        projectId.value(),
        topicName.value(),
        subscriptionName.value());
  }

  @After
  public void tearDown() throws Exception {
    // Cleanup the subscription
    deleteSubscriptionExample(
        cloudRegion.value(), cloudZone.zoneId(), projectId.value(), subscriptionName.value());
  }

  @Test
  public void test() throws Exception {
    // Maven package into jars
    mavenPackage(workingDir);
    mavenPackage(workingDir + "/samples");

    // Upload to GCS
    Storage storage =
        StorageOptions.newBuilder().setProjectId(projectId.value()).build().getService();
    BlobId blobId = BlobId.of(bucketName, sampleJarName);
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
    storage.create(blobInfo, Files.readAllBytes(Paths.get(sampleJarLoc)));
    blobId = BlobId.of(bucketName, connectorJarName);
    blobInfo = BlobInfo.newBuilder(blobId).build();
    storage.create(blobInfo, Files.readAllBytes(Paths.get(connectorJarLoc)));

    // Run Dataproc job
    String myEndpoint = String.format("%s-dataproc.googleapis.com:443", cloudRegion.value());
    JobControllerSettings jobControllerSettings =
        JobControllerSettings.newBuilder().setEndpoint(myEndpoint).build();

    try (JobControllerClient jobControllerClient =
        JobControllerClient.create(jobControllerSettings)) {
      JobPlacement jobPlacement = JobPlacement.newBuilder().setClusterName(clusterName).build();
      SparkJob sparkJob =
          SparkJob.newBuilder()
              .addJarFileUris(String.format("gs://%s/%s", bucketName, sampleJarName))
              .addJarFileUris(String.format("gs://%s/%s", bucketName, connectorJarName))
              .setMainClass("pubsublite.spark.WordCount")
              .addArgs(subscriptionPath.toString())
              .build();
      Job job = Job.newBuilder().setPlacement(jobPlacement).setSparkJob(sparkJob).build();
      OperationFuture<Job, JobMetadata> submitJobAsOperationAsyncRequest =
          jobControllerClient.submitJobAsOperationAsync(
              projectId.value(), cloudRegion.value(), job);
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
