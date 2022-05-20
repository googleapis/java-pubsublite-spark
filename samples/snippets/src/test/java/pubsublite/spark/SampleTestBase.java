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
import com.google.cloud.pubsublite.ProjectNumber;
import com.google.cloud.pubsublite.TopicName;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.common.flogger.GoogleLogger;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.commons.lang.StringUtils;
import org.apache.maven.shared.invoker.DefaultInvocationRequest;
import org.apache.maven.shared.invoker.DefaultInvoker;
import org.apache.maven.shared.invoker.InvocationOutputHandler;
import org.apache.maven.shared.invoker.InvocationRequest;
import org.apache.maven.shared.invoker.InvocationResult;
import org.apache.maven.shared.invoker.Invoker;
import org.apache.maven.shared.invoker.MavenInvocationException;
import org.apache.maven.shared.utils.cli.CommandLineException;

public abstract class SampleTestBase {

  private static final GoogleLogger log = GoogleLogger.forEnclosingClass();

  private static final String CLOUD_REGION = "CLOUD_REGION";
  private static final String CLOUD_ZONE = "CLOUD_ZONE";
  private static final String PROJECT_NUMBER = "GOOGLE_CLOUD_PROJECT_NUMBER";
  private static final String PROJECT_ID = "PROJECT_ID";
  private static final String TOPIC_ID = "TOPIC_ID";
  private static final String CLUSTER_NAME = "CLUSTER_NAME";
  private static final String BUCKET_NAME = "BUCKET_NAME";

  protected final String runId = UUID.randomUUID().toString();
  protected CloudRegion cloudRegion;
  protected CloudZone cloudZone;
  protected ProjectNumber projectNumber;
  protected ProjectId projectId;
  protected TopicName sourceTopicId;
  protected String clusterName;
  protected String bucketName;
  protected String workingDir;
  protected String mavenHome;
  protected String sampleVersion;
  protected String connectorVersion;
  protected String sampleJarName;
  protected String connectorJarName;
  protected String sampleJarNameInGCS;
  protected String connectorJarNameInGCS;
  protected String sampleJarLoc;
  protected String connectorJarLoc;

  protected void setupEnvVars() {
    Map<String, String> env =
        CommonUtils.getAndValidateEnvVars(
            CLOUD_REGION,
            CLOUD_REGION,
            CLOUD_ZONE,
            PROJECT_ID,
            PROJECT_NUMBER,
            TOPIC_ID,
            CLUSTER_NAME,
            BUCKET_NAME);
    cloudRegion = CloudRegion.of(env.get(CLOUD_REGION));
    cloudZone = CloudZone.of(cloudRegion, env.get(CLOUD_ZONE).charAt(0));
    projectId = ProjectId.of(env.get(PROJECT_ID));
    projectNumber = ProjectNumber.of(Long.parseLong(env.get(PROJECT_NUMBER)));
    sourceTopicId = TopicName.of(env.get(TOPIC_ID));
    clusterName = env.get(CLUSTER_NAME) + "-" + runId.substring(0,16);
    bucketName = env.get(BUCKET_NAME);
  }

  protected void findMavenHome() throws Exception {
    Process p = Runtime.getRuntime().exec("mvn --version");
    BufferedReader stdOut = new BufferedReader(new InputStreamReader(p.getInputStream()));
    assertThat(p.waitFor()).isEqualTo(0);
    String s;
    while ((s = stdOut.readLine()) != null) {
      if (StringUtils.startsWith(s, "Maven home: ")) {
        mavenHome = s.replace("Maven home: ", "");
      }
    }
  }

  private void runMavenCommand(
      String workingDir, Optional<InvocationOutputHandler> outputHandler, String... goals)
      throws MavenInvocationException, CommandLineException {
    InvocationRequest request = new DefaultInvocationRequest();
    request.setPomFile(new File(workingDir + "/pom.xml"));
    request.setGoals(Arrays.asList(goals.clone()));
    Invoker invoker = new DefaultInvoker();
    outputHandler.ifPresent(invoker::setOutputHandler);
    invoker.setMavenHome(new File(mavenHome));
    InvocationResult result = invoker.execute(request);
    if (result.getExecutionException() != null) {
      throw result.getExecutionException();
    }
    assertThat(result.getExitCode()).isEqualTo(0);
  }

  protected void mavenPackage(String workingDir)
      throws MavenInvocationException, CommandLineException {
    runMavenCommand(workingDir, Optional.empty(), "clean", "package", "-Dmaven.test.skip=true");
  }

  private void getVersion(String workingDir, InvocationOutputHandler outputHandler)
      throws MavenInvocationException, CommandLineException {
    runMavenCommand(
        workingDir,
        Optional.of(outputHandler),
        "-q",
        "-Dexec.executable=echo",
        "-Dexec.args='${project.version}'",
        "--non-recursive",
        "exec:exec");
  }

  protected void setupVersions() throws MavenInvocationException, CommandLineException {
    workingDir =
        System.getProperty("user.dir")
            .replace("/samples/snapshot", "")
            .replace("/samples/snippets", "");
    getVersion(workingDir, (l) -> connectorVersion = l);
    log.atInfo().log("Connector version is: %s", connectorVersion);
    getVersion(workingDir + "/samples", (l) -> sampleVersion = l);
    log.atInfo().log("Sample version is: %s", sampleVersion);
    sampleJarName = String.format("pubsublite-spark-snippets-%s.jar", sampleVersion);
    connectorJarName =
        String.format("pubsublite-spark-sql-streaming-%s-with-dependencies.jar", connectorVersion);
    sampleJarNameInGCS = String.format("pubsublite-spark-snippets-%s-%s.jar", sampleVersion, runId);
    connectorJarNameInGCS =
        String.format(
            "pubsublite-spark-sql-streaming-%s-with-dependencies-%s.jar", connectorVersion, runId);
    sampleJarLoc = String.format("%s/samples/snippets/target/%s", workingDir, sampleJarName);
    connectorJarLoc = String.format("%s/target/%s", workingDir, connectorJarName);
  }

  protected void uploadGCS(Storage storage, String fileNameInGCS, String fileLoc) throws Exception {
    BlobId blobId = BlobId.of(bucketName, fileNameInGCS);
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
    storage.create(blobInfo, Files.readAllBytes(Paths.get(fileLoc)));
  }

  protected Job runDataprocJob(SparkJob.Builder sparkJobBuilder) throws Exception {
    String myEndpoint = String.format("%s-dataproc.googleapis.com:443", cloudRegion.value());
    JobControllerSettings jobControllerSettings =
        JobControllerSettings.newBuilder().setEndpoint(myEndpoint).build();

    try (JobControllerClient jobControllerClient =
        JobControllerClient.create(jobControllerSettings)) {
      JobPlacement jobPlacement = JobPlacement.newBuilder().setClusterName(clusterName).build();
      sparkJobBuilder
          .addJarFileUris(String.format("gs://%s/%s", bucketName, sampleJarNameInGCS))
          .addJarFileUris(String.format("gs://%s/%s", bucketName, connectorJarNameInGCS));
      Job job =
          Job.newBuilder().setPlacement(jobPlacement).setSparkJob(sparkJobBuilder.build()).build();
      OperationFuture<Job, JobMetadata> submitJobAsOperationAsyncRequest =
          jobControllerClient.submitJobAsOperationAsync(
              projectId.value(), cloudRegion.value(), job);
      return submitJobAsOperationAsyncRequest.get();
    }
  }
}
