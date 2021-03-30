# Pub/Sub Lite Spark Connector Word Count Samples

This directory contains a word count sample for Pub/Sub Lite Spark Connector. The sample will read 
single word count messages from Pub/Sub Lite, do the aggregation (count words) in Spark, and finally
write back to Pub/Sub Lite. Note the topic/subscription to read is different from the topic/subscription
to write and verify the final word count results.

## Authentication

Please see the [Google cloud authentication guide](https://cloud.google.com/docs/authentication/). 
The recommended approach is to use Application Default Credentials by setting `GOOGLE_APPLICATION_CREDENTIALS`.

## Environment Variables
Set the following environment variables. <br>
Note `TOPIC_ID_RAW` and `SUBSCRIPTION_ID_RAW` are used to read _raw_ single word count messages; 
while `TOPIC_ID_RESULT` and `SUBSCRIPTION_ID_RESULT` are used for final word counts results. They must 
be different, otherwise it will be a cycle.
```
PROJECT_NUMBER=12345 # or your project number
REGION=us-central1 # or your region
ZONE_ID=b # or your zone id
TOPIC_ID_RAW=test-topic # or your topic id to create
SUBSCRIPTION_ID_RAW=test-subscription # or your subscription to create
TOPIC_ID_RESULT=test-topic-2 # or your topic id to create, this is different from TOPIC_ID_RAW!
SUBSCRIPTION_ID_RESULT=test-subscription-2 # or your subscription to create, this is different from SUBSCRIPTION_ID_RAW!
CLUSTER_NAME=waprin-spark7 # or your Dataproc cluster name to create
BUCKET=gs://your-gcs-bucket
CONNECTOR_VERSION= # latest pubsublite-spark-sql-streaming release version
PUBSUBLITE_SPARK_SQL_STREAMING_JAR_LOCATION= # downloaded pubsublite-spark-sql-streaming-$CONNECTOR_VERSION-with-dependencies jar location
```

## Running word count sample

To run the word count sample in Dataproc cluster, follow the steps:

1. `cd samples/snippets` 
2. Set the current sample version.
   ```sh
   SAMPLE_VERSION=$(mvn -q \
    -Dexec.executable=echo \
    -Dexec.args='${project.version}' \
    --non-recursive \
    exec:exec)
   ```
3. Create all the needed topics and subscriptions, and publish word count messages to the _raw_ topic.
   ```sh
   PROJECT_NUMBER=$PROJECT_NUMBER \
   REGION=$REGION \
   ZONE_ID=$ZONE_ID \
   TOPIC_ID_RAW=$TOPIC_ID_RAW \
   SUBSCRIPTION_ID_RAW=$SUBSCRIPTION_ID_RAW \
   TOPIC_ID_RESULT=$TOPIC_ID_RESULT \
   SUBSCRIPTION_ID_RESULT=$SUBSCRIPTION_ID_RESULT \
   mvn compile exec:java -Dexec.mainClass=pubsublite.spark.PublishWords
   ```
4. Create a Dataproc cluster
   ```sh
   gcloud dataproc clusters create $CLUSTER_NAME --region=$REGION --zone=$REGION-$ZONE_ID --image-version=1.5-debian10 --scopes=cloud-platform
   ```
5. Package sample jar
   ```sh
   mvn clean package -Dmaven.test.skip=true
   ```
6. Download `pubsublite-spark-sql-streaming-$CONNECTOR_VERSION-with-dependencies.jar` from [Maven Central](https://search.maven.org/artifact/com.google.cloud/pubsublite-spark-sql-streaming) and set `PUBSUBLITE_SPARK_SQL_STREAMING_JAR_LOCATION` environment variable.
7. Create GCS bucket and upload both `pubsublite-spark-sql-streaming-$CONNECTOR_VERSION-with-dependencies.jar` and the sample jar onto GCS
   ```sh
   gsutil mb $BUCKET
   gsutil cp target/pubsublite-spark-snippets-$SAMPLE_VERSION.jar $BUCKET
   gsutil cp $PUBSUBLITE_SPARK_SQL_STREAMING_JAR_LOCATION $BUCKET
   ```
8. Set Dataproc region
   ```sh
   gcloud config set dataproc/region $REGION
   ```
9. Run the sample in Dataproc. This will perform word count aggregation and publish word count results to Pub/Sub Lite.
   ```sh
   gcloud dataproc jobs submit spark --cluster=$CLUSTER_NAME \
      --jars=$BUCKET/pubsublite-spark-snippets-$SAMPLE_VERSION.jar,$BUCKET/pubsublite-spark-sql-streaming-$CONNECTOR_VERSION-with-dependencies.jar \
      --class=pubsublite.spark.WordCount -- \
      projects/$PROJECT_NUMBER/locations/$REGION-$ZONE_ID/subscriptions/$SUBSCRIPTION_ID_RAW \
      projects/$PROJECT_NUMBER/locations/$REGION-$ZONE_ID/topics/$TOPIC_ID_RESULT
   ```
10. Read word count results from Pub/Sub Lite, you should see the result in console output.
    ```sh
    PROJECT_NUMBER=$PROJECT_NUMBER \
    REGION=$REGION \
    ZONE_ID=$ZONE_ID \
    SUBSCRIPTION_ID_RESULT=$SUBSCRIPTION_ID_RESULT \
    mvn compile exec:java -Dexec.mainClass=pubsublite.spark.ReadResults
    ```

## Cleaning up
1. Delete Pub/Sub Lite topic and subscription.
   ```sh
   gcloud pubsub lite-subscriptions delete $SUBSCRIPTION_ID_RAW --zone=$REGION-$ZONE_ID
   gcloud pubsub lite-topics delete $TOPIC_ID_RAW --zone=$REGION-$ZONE_ID
   gcloud pubsub lite-subscriptions delete $SUBSCRIPTION_ID_RESULT --zone=$REGION-$ZONE_ID
   gcloud pubsub lite-topics delete $TOPIC_ID_RESULT --zone=$REGION-$ZONE_ID
   ```
2. Delete GCS bucket.
   ```sh
   gsutil -m rm -rf $BUCKET
   ```
3. Delete Dataproc cluster.
   ```sh
   gcloud dataproc clusters delete $CLUSTER_NAME --region=$REGION
   ```

## Common issues
1. Permission not granted. <br>
   This could happen when creating a topic and a subscription, or submitting a job to your Dataproc cluster.
   Make sure your service account has at least `Editor` permissions for Pub/Sub Lite and Dataproc. 
   Your Dataproc cluster needs `scope=cloud-platform` to access other services and resources within the same project.
   Your `gcloud` and `GOOGLE_APPLICATION_CREDENTIALS` should access the same project. Check out which project your `gcloud` and `gstuil` commands use with `gcloud config get-value project`.

2. Your Dataproc job fails with `ClassNotFound` or similar exceptions. <br>
   Make sure your Dataproc cluster uses images of [supported Spark versions](https://github.com/googleapis/java-pubsublite-spark#compatibility). 
