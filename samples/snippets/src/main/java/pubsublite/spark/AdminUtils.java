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

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.gax.longrunning.OperationFuture;
import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.api.gax.rpc.ApiException;
import com.google.cloud.dataproc.v1.Cluster;
import com.google.cloud.dataproc.v1.ClusterConfig;
import com.google.cloud.dataproc.v1.ClusterControllerClient;
import com.google.cloud.dataproc.v1.ClusterControllerSettings;
import com.google.cloud.dataproc.v1.ClusterOperationMetadata;
import com.google.cloud.dataproc.v1.GceClusterConfig;
import com.google.cloud.dataproc.v1.InstanceGroupConfig;
import com.google.cloud.dataproc.v1.SoftwareConfig;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsublite.AdminClient;
import com.google.cloud.pubsublite.AdminClientSettings;
import com.google.cloud.pubsublite.BacklogLocation;
import com.google.cloud.pubsublite.CloudRegion;
import com.google.cloud.pubsublite.CloudZone;
import com.google.cloud.pubsublite.MessageMetadata;
import com.google.cloud.pubsublite.ProjectNumber;
import com.google.cloud.pubsublite.SubscriptionName;
import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.TopicName;
import com.google.cloud.pubsublite.TopicPath;
import com.google.cloud.pubsublite.cloudpubsub.FlowControlSettings;
import com.google.cloud.pubsublite.cloudpubsub.Publisher;
import com.google.cloud.pubsublite.cloudpubsub.PublisherSettings;
import com.google.cloud.pubsublite.cloudpubsub.Subscriber;
import com.google.cloud.pubsublite.cloudpubsub.SubscriberSettings;
import com.google.cloud.pubsublite.proto.Subscription;
import com.google.cloud.pubsublite.proto.Topic;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.Durations;
import com.google.pubsub.v1.PubsubMessage;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class AdminUtils {

  public static void createTopicExample(
      String cloudRegion, char zoneId, long projectNumber, String topicId, int partitions)
      throws Exception {

    TopicPath topicPath =
        TopicPath.newBuilder()
            .setProject(ProjectNumber.of(projectNumber))
            .setLocation(CloudZone.of(CloudRegion.of(cloudRegion), zoneId))
            .setName(TopicName.of(topicId))
            .build();

    Topic topic =
        Topic.newBuilder()
            .setPartitionConfig(
                Topic.PartitionConfig.newBuilder()
                    // Set throughput capacity per partition in MiB/s.
                    .setCapacity(
                        Topic.PartitionConfig.Capacity.newBuilder()
                            // Must be 4-16 MiB/s.
                            .setPublishMibPerSec(4)
                            // Must be 4-32 MiB/s.
                            .setSubscribeMibPerSec(8)
                            .build())
                    .setCount(partitions))
            .setRetentionConfig(
                Topic.RetentionConfig.newBuilder()
                    // How long messages are retained.
                    .setPeriod(Durations.fromDays(1))
                    // Set storage per partition to 30 GiB. This must be 30 GiB-10 TiB.
                    // If the number of bytes stored in any of the topic's partitions grows
                    // beyond this value, older messages will be dropped to make room for
                    // newer ones, regardless of the value of `period`.
                    .setPerPartitionBytes(30 * 1024 * 1024 * 1024L))
            .setName(topicPath.toString())
            .build();

    AdminClientSettings adminClientSettings =
        AdminClientSettings.newBuilder().setRegion(CloudRegion.of(cloudRegion)).build();

    try (AdminClient adminClient = AdminClient.create(adminClientSettings)) {
      Topic response = adminClient.createTopic(topic).get();
      System.out.println(response.getAllFields() + "created successfully.");
    } catch (ExecutionException e) {
      if (e.getCause() instanceof AlreadyExistsException) {
        System.out.println(topicPath + " already exists");
      } else {
        throw e;
      }
    }
  }

  public static void createSubscriptionExample(
      String cloudRegion, char zoneId, long projectNumber, String topicId, String subscriptionId)
      throws Exception {

    TopicPath topicPath =
        TopicPath.newBuilder()
            .setProject(ProjectNumber.of(projectNumber))
            .setLocation(CloudZone.of(CloudRegion.of(cloudRegion), zoneId))
            .setName(TopicName.of(topicId))
            .build();

    SubscriptionPath subscriptionPath =
        SubscriptionPath.newBuilder()
            .setLocation(CloudZone.of(CloudRegion.of(cloudRegion), zoneId))
            .setProject(ProjectNumber.of(projectNumber))
            .setName(SubscriptionName.of(subscriptionId))
            .build();

    Subscription subscription =
        Subscription.newBuilder()
            .setDeliveryConfig(
                // Possible values for DeliveryRequirement:
                // - `DELIVER_IMMEDIATELY`
                // - `DELIVER_AFTER_STORED`
                // You may choose whether to wait for a published message to be successfully written
                // to storage before the server delivers it to subscribers. `DELIVER_IMMEDIATELY` is
                // suitable for applications that need higher throughput.
                Subscription.DeliveryConfig.newBuilder()
                    .setDeliveryRequirement(
                        Subscription.DeliveryConfig.DeliveryRequirement.DELIVER_IMMEDIATELY))
            .setName(subscriptionPath.toString())
            .setTopic(topicPath.toString())
            .build();

    AdminClientSettings adminClientSettings =
        AdminClientSettings.newBuilder().setRegion(CloudRegion.of(cloudRegion)).build();

    try (AdminClient adminClient = AdminClient.create(adminClientSettings)) {
      Subscription response =
          adminClient.createSubscription(subscription, BacklogLocation.BEGINNING).get();
      System.out.println(response.getAllFields() + "created successfully.");
    } catch (ExecutionException e) {
      if (e.getCause() instanceof AlreadyExistsException) {
        System.out.println(topicPath + " already exists");
      } else {
        throw e;
      }
    }
  }

  public static void deleteSubscriptionExample(
      String cloudRegion, SubscriptionPath subscriptionPath) throws Exception {
    AdminClientSettings adminClientSettings =
        AdminClientSettings.newBuilder().setRegion(CloudRegion.of(cloudRegion)).build();

    try (AdminClient adminClient = AdminClient.create(adminClientSettings)) {
      adminClient.deleteSubscription(subscriptionPath).get();
      System.out.println(subscriptionPath + " deleted successfully.");
    }
  }

  public static void deleteTopicExample(String cloudRegion, TopicPath topicPath) throws Exception {
    AdminClientSettings adminClientSettings =
        AdminClientSettings.newBuilder().setRegion(CloudRegion.of(cloudRegion)).build();

    try (AdminClient adminClient = AdminClient.create(adminClientSettings)) {
      adminClient.deleteTopic(topicPath).get();
      System.out.println(topicPath + " deleted successfully.");
    }
  }

  public static void publisherExample(
      String cloudRegion, char zoneId, long projectNumber, String topicId, List<String> words)
      throws ApiException, ExecutionException, InterruptedException {

    TopicPath topicPath =
        TopicPath.newBuilder()
            .setProject(ProjectNumber.of(projectNumber))
            .setLocation(CloudZone.of(CloudRegion.of(cloudRegion), zoneId))
            .setName(TopicName.of(topicId))
            .build();
    Publisher publisher = null;
    List<ApiFuture<String>> futures = new ArrayList<>();

    try {
      PublisherSettings publisherSettings =
          PublisherSettings.newBuilder().setTopicPath(topicPath).build();

      publisher = Publisher.create(publisherSettings);

      // Start the publisher. Upon successful starting, its state will become RUNNING.
      publisher.startAsync().awaitRunning();

      for (String word : words) {
        // Include the count in message, and convert the message to a byte string
        ByteString data = ByteString.copyFromUtf8(word + "_1");
        PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();

        // Publish a message. Messages are automatically batched.
        ApiFuture<String> future = publisher.publish(pubsubMessage);
        futures.add(future);
      }
    } finally {
      ArrayList<MessageMetadata> metadata = new ArrayList<>();
      List<String> ackIds = ApiFutures.allAsList(futures).get();
      for (String id : ackIds) {
        // Decoded metadata contains partition and offset.
        metadata.add(MessageMetadata.decode(id));
      }
      System.out.println(metadata + "\nPublished " + ackIds.size() + " messages.");

      if (publisher != null) {
        // Shut down the publisher.
        publisher.stopAsync().awaitTerminated();
        System.out.println("Publisher is shut down.");
      }
    }
  }

  public static Queue<PubsubMessage> subscriberExample(
      String cloudRegion, char zoneId, long projectNumber, String subscriptionId)
      throws ApiException {
    // Sample has at most 200 messages.
    Queue<PubsubMessage> result = new ArrayBlockingQueue<>(1000);

    SubscriptionPath subscriptionPath =
        SubscriptionPath.newBuilder()
            .setLocation(CloudZone.of(CloudRegion.of(cloudRegion), zoneId))
            .setProject(ProjectNumber.of(projectNumber))
            .setName(SubscriptionName.of(subscriptionId))
            .build();

    MessageReceiver receiver =
        (PubsubMessage message, AckReplyConsumer consumer) -> {
          result.add(message);
          consumer.ack();
        };
    FlowControlSettings flowControlSettings =
        FlowControlSettings.builder()
            .setBytesOutstanding(10 * 1024 * 1024L)
            .setMessagesOutstanding(1000L)
            .build();

    SubscriberSettings subscriberSettings =
        SubscriberSettings.newBuilder()
            .setSubscriptionPath(subscriptionPath)
            .setReceiver(receiver)
            .setPerPartitionFlowControlSettings(flowControlSettings)
            .build();

    Subscriber subscriber = Subscriber.create(subscriberSettings);

    // Start the subscriber. Upon successful starting, its state will become RUNNING.
    subscriber.startAsync().awaitRunning();

    try {
      System.out.println(subscriber.state());
      // Wait 90 seconds for the subscriber to reach TERMINATED state. If it encounters
      // unrecoverable errors before then, its state will change to FAILED and an
      // IllegalStateException will be thrown.
      subscriber.awaitTerminated(90, TimeUnit.SECONDS);
    } catch (TimeoutException t) {
      // Shut down the subscriber. This will change the state of the subscriber to TERMINATED.
      subscriber.stopAsync().awaitTerminated();
      System.out.println("Subscriber is shut down: " + subscriber.state());
    }

    return result;
  }

  public static void createCluster(
      String projectId, String region, String clusterName, String imageVersion)
      throws IOException, InterruptedException {
    String myEndpoint = String.format("%s-dataproc.googleapis.com:443", region);

    // Configure the settings for the cluster controller client.
    ClusterControllerSettings clusterControllerSettings =
        ClusterControllerSettings.newBuilder().setEndpoint(myEndpoint).build();

    // Create a cluster controller client with the configured settings. The client only needs to be
    // created once and can be reused for multiple requests. Using a try-with-resources
    // closes the client, but this can also be done manually with the .close() method.
    try (ClusterControllerClient clusterControllerClient =
        ClusterControllerClient.create(clusterControllerSettings)) {
      // Configure the settings for our cluster.
      InstanceGroupConfig masterConfig =
          InstanceGroupConfig.newBuilder()
              .setMachineTypeUri("n1-standard-2")
              .setNumInstances(1)
              .build();
      InstanceGroupConfig workerConfig =
          InstanceGroupConfig.newBuilder()
              .setMachineTypeUri("n1-standard-2")
              .setNumInstances(2)
              .build();
      SoftwareConfig softwareConfig =
          SoftwareConfig.newBuilder().setImageVersion(imageVersion).build();
      GceClusterConfig gceClusterConfig =
          GceClusterConfig.newBuilder()
              .addServiceAccountScopes("https://www.googleapis.com/auth/cloud-platform")
              .build();
      ClusterConfig clusterConfig =
          ClusterConfig.newBuilder()
              .setMasterConfig(masterConfig)
              .setWorkerConfig(workerConfig)
              .setSoftwareConfig(softwareConfig)
              .setGceClusterConfig(gceClusterConfig)
              .build();
      // Create the cluster object with the desired cluster config.
      Cluster cluster =
          Cluster.newBuilder().setClusterName(clusterName).setConfig(clusterConfig).build();

      // Create the Cloud Dataproc cluster.
      OperationFuture<Cluster, ClusterOperationMetadata> createClusterAsyncRequest =
          clusterControllerClient.createClusterAsync(projectId, region, cluster);
      Cluster response = createClusterAsyncRequest.get();

      // Print out a success message.
      System.out.printf("Cluster created successfully: %s", response.getClusterName());

    } catch (ExecutionException e) {
      System.err.println(String.format("Error executing createCluster: %s ", e.getMessage()));
    }
  }

  public static void deleteCluster(String projectId, String region, String clusterName)
      throws IOException {
    String myEndpoint = String.format("%s-dataproc.googleapis.com:443", region);
    ClusterControllerSettings clusterControllerSettings =
        ClusterControllerSettings.newBuilder().setEndpoint(myEndpoint).build();
    ClusterControllerClient clusterControllerClient =
        ClusterControllerClient.create(clusterControllerSettings);
    clusterControllerClient.deleteClusterAsync(projectId, region, clusterName);
  }
}
