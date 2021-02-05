# Pub/Sub Lite Spark Connector Word Count Samples

This directory contains a word count sample for Pub/Sub Lite Spark Connector.

## Authentication

Please see the [Google cloud authentication guide](https://cloud.google.com/docs/authentication/). 
The recommended approach is to use Application Default Credentials by setting `GOOGLE_APPLICATION_CREDENTIALS`.

## Environment Variables
Set the following environment variables:
```
PROJECT_ID=your-project-id
REGION=us-central1 # or your region
ZONE_ID=b # or your zone id
TOPIC_ID=test-topic # or your topic id to create
SUBSCRIPTION_ID=test-subscrciption # or your subscription to create
PARTITIONS=1 # or your number of partitions to create
CLUSTER_NAME=waprin-spark7 # or your Dataproc cluster name to create
BUCKET=gs://your-gcs-bucket
SUBSCRIPTION_PATH=projects/$PROJECT_NUMBER/locations/$REGION-$ZONE_ID/subscriptions/$SUBSCRIPTION_ID
```

## Running word count sample

To run the word count sample in Dataproc cluster, follow the steps:

1. `cd samples/` 
2. Create the topic and subscription, and publish word count messages to the topic.
   ```
   PROJECT_ID=$PROJECT_ID \
   REGION=$REGION \
   ZONE_ID=$ZONE_ID \
   TOPIC_ID=$TOPIC_ID \
   SUBSCRIPTION_ID=$SUBSCRIPTION_ID \
   PARTITIONS=$PARTITIONS \
   mvn compile exec:java -Dexec.mainClass=pubsublite.spark.PublishWords
   ```
3. Create a Dataproc cluster
   ```
   gcloud dataproc clusters create $CLUSTER_NAME --region=$REGION --zone="$REGION-$ZONE_ID" --image-version=1.5-debian10 --scopes=cloud-platform
   ```
4. Package sample jar
   ```
   mvn clean package -Dmaven.test.skip=true
   ```
<!-- TODO: set up bots to update jar version, also provide link to maven central --> 
5. Download pubsublite-spark-sql-streaming-0.1.0.jar from Maven Central
<!-- TODO: set up bots to update jar version -->
6. Create GCS bucket and upload both pubsublite-spark-sql-streaming jar and the sample jar onto GCS
   ```
   gsutil mb $BUCKET
   gsutil cp snapshot/target/pubsublite-spark-snapshot-1.0.21.jar $BUCKET
   gsutil cp $PUBSUBLITE_SPARK_SQL_STREAMING_JAR_LOCATION $BUCKET
   ```
<!-- TODO: set up bots to update jar version -->
7. Run the sample in Dataproc
   ```
   gcloud dataproc jobs submit spark --cluster="$CLUSTER_NAME" \
      --jars="$BUCKET/pubsublite-spark-snapshot-1.0.21.jar,$BUCKET/pubsublite-spark-sql-streaming-0.1.0.jar" \
      --class=pubsublite.spark.WordCount -- $SUBSCRIPTION_PATH
   ```



