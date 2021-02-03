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
import com.google.common.flogger.GoogleLogger;
import org.apache.maven.cli.MavenCli;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static pubsublite.spark.AdminUtils.createSubscriptionExample;

public class SampleIntegrationTest {

    private static final GoogleLogger log = GoogleLogger.forEnclosingClass();

    @Test
    public void test() throws Exception {
        String workingDir = System.getProperty("user.dir").replace("/samples/snapshot", "");

        CloudRegion cloudRegion = CloudRegion.of("us-central1");
        CloudZone cloudZone = CloudZone.of(cloudRegion, 'b');
        ProjectId projectId = ProjectId.of("pubsublite-load-tests");
        TopicName topicName = TopicName.of("test-topic");
        SubscriptionName subscriptionName = SubscriptionName.of("test-sub-" + System.currentTimeMillis());
        SubscriptionPath subscriptionPath = SubscriptionPath.newBuilder()
                .setProject(projectId)
                .setLocation(cloudZone)
                .setName(subscriptionName)
                .build();
        String clusterName = "";
        String bucketName = "";
        String sampleJarName = "pubsublite-spark-snapshot-1.0.21.jar";
        String connectorJarName = "pubsublite-spark-sql-streaming-with-dependencies-0.1.0-SNAPSHOT.jar";
        String sampleJarLoc = String.format("%s/samples/snapshot/%s", workingDir, sampleJarName);
        String connectorJarLoc = String.format("%s/target/%s", workingDir, connectorJarName);

        // Create a subscription
        createSubscriptionExample(cloudRegion.value(), cloudZone.zoneId(), projectId.value(),
                topicName.value(), subscriptionName.value());

        MavenCli mavenCli = new MavenCli();
        mavenCli.doMain(new String[]{"clean", "package", "-Dmaven.test.skip=true"}, workingDir, System.out, System.out);

        Storage storage = StorageOptions.newBuilder().setProjectId(projectId.value()).build().getService();
        BlobId blobId = BlobId.of(bucketName, sampleJarName);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
        storage.create(blobInfo, Files.readAllBytes(Paths.get(sampleJarLoc)));
        blobId = BlobId.of(bucketName, connectorJarName);
        blobInfo = BlobInfo.newBuilder(blobId).build();
        storage.create(blobInfo, Files.readAllBytes(Paths.get(connectorJarLoc)));

        String myEndpoint = String.format("%s-dataproc.googleapis.com:443", cloudRegion.value());

        // Configure the settings for the job controller client.
        JobControllerSettings jobControllerSettings =
                JobControllerSettings.newBuilder().setEndpoint(myEndpoint).build();

        try (JobControllerClient jobControllerClient =
                     JobControllerClient.create(jobControllerSettings)) {
            JobPlacement jobPlacement = JobPlacement.newBuilder().setClusterName(clusterName).build();
            SparkJob sparkJob = SparkJob.newBuilder()
                    .addJarFileUris(String.format("%s/%s", bucketName, sampleJarName))
                    .addJarFileUris(String.format("%s/%s", bucketName, connectorJarName))
                    .setMainClass("pubsublite.spark.WordCount")
                    .addArgs(subscriptionPath.toString())
                    .build();
            Job job = Job.newBuilder().setPlacement(jobPlacement).setSparkJob(sparkJob).build();
            // Submit an asynchronous request to execute the job.
            OperationFuture<Job, JobMetadata> submitJobAsOperationAsyncRequest =
                    jobControllerClient.submitJobAsOperationAsync(projectId.value(), cloudRegion.value(), job);
            Job jobResponse = submitJobAsOperationAsyncRequest.get();

            // Print output from Google Cloud Storage.
            Matcher matches =
                    Pattern.compile("gs://(.*?)/(.*)").matcher(jobResponse.getDriverOutputResourceUri());
            matches.matches();

            Blob blob = storage.get(matches.group(1), String.format("%s.000000000", matches.group(2)));

            System.out.println(
                    String.format("Job finished successfully: %s", new String(blob.getContent())));

        }


    }

}
