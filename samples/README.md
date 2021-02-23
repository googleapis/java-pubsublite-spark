# Pub/Sub Lite Spark Connector Word Count Samples

This directory contains a word count sample for Pub/Sub Lite Spark Connector.

## Authentication

Please see the [Google cloud authentication guide](https://cloud.google.com/docs/authentication/). 
The recommended approach is to use Application Default Credentials by setting `GOOGLE_APPLICATION_CREDENTIALS`.

## Environment Variables
Set the following environment variables:
```
PROJECT_NUMBER=12345 # or your project number
REGION=us-central1 # or your region
ZONE_ID=b # or your zone id
TOPIC_ID=test-topic # or your topic id to create
SUBSCRIPTION_ID=test-subscrciption # or your subscription to create
PARTITIONS=1 # or your number of partitions to create
CLUSTER_NAME=waprin-spark7 # or your Dataproc cluster name to create
BUCKET=gs://your-gcs-bucket
SUBSCRIPTION_PATH=projects/$PROJECT_NUMBER/locations/$REGION-$ZONE_ID/subscriptions/$SUBSCRIPTION_ID
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
3. Create the topic and subscription, and publish word count messages to the topic.
   ```sh
   PROJECT_NUMBER=$PROJECT_NUMBER \
   REGION=$REGION \
   ZONE_ID=$ZONE_ID \
   TOPIC_ID=$TOPIC_ID \
   SUBSCRIPTION_ID=$SUBSCRIPTION_ID \
   PARTITIONS=$PARTITIONS \
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
<!-- TODO: provide link to maven central --> 
6. Download `pubsublite-spark-sql-streaming-$CONNECTOR_VERSION-with-dependencies.jar` from Maven Central and set `PUBSUBLITE_SPARK_SQL_STREAMING_JAR_LOCATION` environment variable.
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
<!-- TODO: set up bots to update jar version -->
9. Run the sample in Dataproc. You would see the word count result show up in the console output.
   ```sh
   gcloud dataproc jobs submit spark --cluster=$CLUSTER_NAME \
      --jars=$BUCKET/pubsublite-spark-snippets-$SAMPLE_VERSION.jar,$BUCKET/pubsublite-spark-sql-streaming-$CONNECTOR_VERSION-with-dependencies.jar \
      --class=pubsublite.spark.WordCount -- $SUBSCRIPTION_PATH
   ```

## Cleaning up
1. Delete Pub/Sub Lite topic and subscription.
   ```sh
   gcloud pubsub lite-subscriptions delete $SUBSCRIPTION_ID --zone=$REGION-$ZONE_ID
   gcloud pubsub lite-topics delete $TOPIC_ID --zone=$REGION-$ZONE_ID
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
   This could happen in when creating topic and subscription, or submitting job to Dataproc cluster.
   Make sure the account/service account has `editor` permission for Pub/Sub Lite and Dataproc. 
   Dataproc cluster should have `scope=cloud-platform` to be able to access other cloud resources within the same
   project such as Pub/Sub Lite.

2. Dataproc job shows ClassNotFound or similar exceptions. <br>
   Make sure the Dataproc cluster you created has images with [supported spark versions](https://github.com/googleapis/java-pubsublite-spark#compatibility). 
