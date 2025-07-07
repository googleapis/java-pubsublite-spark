# Pub/Sub Lite Spark Connector Samples

This directory contains 3 samples for Pub/Sub Lite Spark Connector:
1. [Word count sample](#word-count-sample). The sample reads single word count messages from Pub/Sub Lite,
   does the aggregation (count words) in Spark, and finally writes back to Pub/Sub Lite. 
   Note the topic/subscription to read is different from the topic/subscription to write 
   and verify the final word count results.
2. [Simple read sample](#simple-read-sample). The sample reads messages from Pub/Sub Lite, and outputs to console sink.
3. [Simple write sample](#simple-write-sample). The sample creates DataFrame inside spark and writes to Pub/Sub Lite.

### Authentication

Please see the [Google cloud authentication guide](https://cloud.google.com/docs/authentication/). 
The recommended approach is to use Application Default Credentials by setting `GOOGLE_APPLICATION_CREDENTIALS`.

## Word Count Sample

### Environment Variables
Set the following environment variables. <br>
Note `SOURCE_TOPIC_ID` and `SOURCE_SUBSCRIPTION_ID` are used to read _raw_ single word count messages; 
while `DESTINATION_TOPIC_ID` and `DESTINATION_SUBSCRIPTION_ID` are used for the final word counts results. They must 
be different.
```
export PROJECT_NUMBER=12345 # or your project number
export REGION=us-central1 # or your region
export ZONE_ID=b # or your zone id
export SOURCE_TOPIC_ID=test-topic # or your topic id to create
export SOURCE_SUBSCRIPTION_ID=test-subscription # or your subscription to create
export DESTINATION_TOPIC_ID=test-topic-2 # or your topic id to create, this is different from SOURCE_TOPIC_ID!
export DESTINATION_SUBSCRIPTION_ID=test-subscription-2 # or your subscription to create, this is different from SOURCE_SUBSCRIPTION_ID!
export CLUSTER_NAME=waprin-spark7 # or your Dataproc cluster name to create
export BUCKET=gs://your-gcs-bucket
export CONNECTOR_VERSION= # latest pubsublite-spark-sql-streaming release version
```

### Running word count sample

To run the word count sample in Dataproc cluster, either use provided bash script `word_count_sample.sh run` or 
follow the steps:

1. `cd samples/snippets`
2. Set extra environment variables.
   ```sh
   export SAMPLE_VERSION=$(mvn -q \
    -Dexec.executable=echo \
    -Dexec.args='${project.version}' \
    --non-recursive \
    exec:exec)
   export SOURCE_SUBSCRIPTION_PATH=projects/$PROJECT_NUMBER/locations/$REGION-$ZONE_ID/subscriptions/$SOURCE_SUBSCRIPTION_ID
   export DESTINATION_TOPIC_PATH=projects/$PROJECT_NUMBER/locations/$REGION-$ZONE_ID/topics/$DESTINATION_TOPIC_ID
   ```
3. Create both the source and destination topics and subscriptions, and publish word count messages to the _source_
   topic.
   ```sh
   mvn compile exec:java -Dexec.mainClass=pubsublite.spark.PublishWords
   ```
4. Create a Dataproc cluster
   ```sh
   gcloud dataproc clusters create $CLUSTER_NAME --region=$REGION --zone=$REGION-$ZONE_ID --image-version=2.3-debian12 --scopes=cloud-platform
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
      --class=pubsublite.spark.WordCount \
      --properties=spark.submit.deployMode=cluster,spark.yarn.appMasterEnv.SOURCE_SUBSCRIPTION_PATH=$SOURCE_SUBSCRIPTION_PATH,spark.yarn.appMasterEnv.DESTINATION_TOPIC_PATH=$DESTINATION_TOPIC_PATH
   ```
10. Read word count results from Pub/Sub Lite, you should see the result in console output.
    ```sh
    mvn compile exec:java -Dexec.mainClass=pubsublite.spark.ReadResults
    ```

## Cleaning up

To clean up, either use provided bash script `word_count_sample.sh clean` or follow the steps:

1. Delete Pub/Sub Lite topic and subscription.
   ```sh
   gcloud pubsub lite-subscriptions delete $SOURCE_SUBSCRIPTION_ID --zone=$REGION-$ZONE_ID
   gcloud pubsub lite-topics delete $SOURCE_TOPIC_ID --zone=$REGION-$ZONE_ID
   gcloud pubsub lite-subscriptions delete $DESTINATION_SUBSCRIPTION_ID --zone=$REGION-$ZONE_ID
   gcloud pubsub lite-topics delete $DESTINATION_TOPIC_ID --zone=$REGION-$ZONE_ID
   ```
2. Delete GCS bucket.
   ```sh
   gsutil -m rm -rf $BUCKET
   ```
3. Delete Dataproc cluster.
   ```sh
   gcloud dataproc clusters delete $CLUSTER_NAME --region=$REGION
   ```

## Simple Read Sample

### Environment Variables
Set the following environment variables. <br>
```
export PROJECT_NUMBER=12345 # or your project number
export REGION=us-central1 # or your region
export ZONE_ID=b # or your zone id
export SOURCE_TOPIC_ID=test-topic # or your topic id to create
export SOURCE_SUBSCRIPTION_ID=test-subscription # or your subscription to create
export CLUSTER_NAME=waprin-spark7 # or your Dataproc cluster name to create
export BUCKET=gs://your-gcs-bucket
export CONNECTOR_VERSION= # latest pubsublite-spark-sql-streaming release version
```

### Running simple read sample

To run the simple read sample in Dataproc cluster, either use provided bash script `simple_read_sample.sh run` or
follow the steps:

1. `cd samples/snippets`
2. Set extra environment variables.
   ```sh
   export SAMPLE_VERSION=$(mvn -q \
    -Dexec.executable=echo \
    -Dexec.args='${project.version}' \
    --non-recursive \
    exec:exec)
   export SOURCE_SUBSCRIPTION_PATH=projects/$PROJECT_NUMBER/locations/$REGION-$ZONE_ID/subscriptions/$SOURCE_SUBSCRIPTION_ID
   ```
3. Create both the source and destination topics and subscriptions, and publish word count messages to the _source_
   topic.
   ```sh
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
   ```sh  
9. Run the sample in Dataproc. You would see the messages show up in the console output.
   ```sh
   gcloud dataproc jobs submit spark --cluster=$CLUSTER_NAME \
      --jars=$BUCKET/pubsublite-spark-snippets-$SAMPLE_VERSION.jar,$BUCKET/pubsublite-spark-sql-streaming-$CONNECTOR_VERSION-with-dependencies.jar \
      --class=pubsublite.spark.SimpleRead -- $SOURCE_SUBSCRIPTION_PATH
   ```

### Cleaning up

To clean up, either use provided bash script `simple_read_sample.sh clean` or follow the steps:

1. Delete Pub/Sub Lite topic and subscription.
   ```sh
   gcloud pubsub lite-subscriptions delete $SOURCE_SUBSCRIPTION_ID --zone=$REGION-$ZONE_ID
   gcloud pubsub lite-topics delete $SOURCE_TOPIC_ID --zone=$REGION-$ZONE_ID
   ```
2. Delete GCS bucket.
   ```sh
   gsutil -m rm -rf $BUCKET
   ```
3. Delete Dataproc cluster.
   ```sh
   gcloud dataproc clusters delete $CLUSTER_NAME --region=$REGION
   ```

## Simple Write Sample

### Environment Variables
Set the following environment variables. <br>
```
export PROJECT_NUMBER=12345 # or your project number
export REGION=us-central1 # or your region
export ZONE_ID=b # or your zone id
export DESTINATION_TOPIC_ID=test-topic # or your topic id to create
export DESTINATION_SUBSCRIPTION_ID=test-subscription # or your subscription to create
export CLUSTER_NAME=waprin-spark7 # or your Dataproc cluster name to create
export BUCKET=gs://your-gcs-bucket
export CONNECTOR_VERSION= # latest pubsublite-spark-sql-streaming release version
```

### Running simple write sample

To run the simple read sample in Dataproc cluster, either use provided bash script `simple_write_sample.sh run` or
follow the steps:

1. `cd samples/snippets`
2. Set extra environment variables.
   ```sh
   export SAMPLE_VERSION=$(mvn -q \
    -Dexec.executable=echo \
    -Dexec.args='${project.version}' \
    --non-recursive \
    exec:exec)
   export DESTINATION_TOPIC_PATH=projects/$PROJECT_NUMBER/locations/$REGION-$ZONE_ID/topics/$DESTINATION_TOPIC_ID
   ```
3. Create both the source and destination topics and subscriptions, and publish word count messages to the _source_
   topic.
   ```sh
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
   ```sh  
9. Run the sample in Dataproc. You would see the messages show up in the console output.
   ```sh
   gcloud dataproc jobs submit spark --cluster=$CLUSTER_NAME \
      --jars=$BUCKET/pubsublite-spark-snippets-$SAMPLE_VERSION.jar,$BUCKET/pubsublite-spark-sql-streaming-$CONNECTOR_VERSION-with-dependencies.jar \
      --class=pubsublite.spark.SimpleWrite \
      --properties=spark.submit.deployMode=cluster,spark.yarn.appMasterEnv.DESTINATION_TOPIC_PATH=$DESTINATION_TOPIC_PATH
   ```

### Cleaning up

To clean up, either use provided bash script `simple_write_sample.sh clean` or follow the steps:

1. Delete Pub/Sub Lite topic and subscription.
   ```sh
   gcloud pubsub lite-subscriptions delete DESTINATION_SUBSCRIPTION_ID --zone=$REGION-$ZONE_ID
   gcloud pubsub lite-topics delete $DESTINATION_TOPIC_ID --zone=$REGION-$ZONE_ID
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
