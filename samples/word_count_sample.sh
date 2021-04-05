#!/bin/bash
# Bash script that runs word count sample.

cd samples/snippets

# Set the current sample version.
export SAMPLE_VERSION=$(mvn -q \
  -Dexec.executable=echo \
  -Dexec.args="${project.version}" \
  --non-recursive \
  exec:exec)

# Create both the source and destination topics and subscriptions,
# and publish word count messages to the _source_ topic.
mvn compile exec:java -Dexec.mainClass=pubsublite.spark.PublishWords

# Create a Dataproc cluster
gcloud dataproc clusters create $CLUSTER_NAME \
  --region=$REGION \
  --zone=$REGION-$ZONE_ID \
  --image-version=1.5-debian10 \
  --scopes=cloud-platform

# Package sample jar
mvn clean package -Dmaven.test.skip=true

# Create GCS bucket and upload sample jar onto GCS
gsutil mb $BUCKET
gsutil cp target/pubsublite-spark-snippets-$SAMPLE_VERSION.jar $BUCKET

# Set Dataproc region
gcloud config set dataproc/region $REGION

# Run the sample in Dataproc. This will perform word count aggregation
# and publish word count results to Pub/Sub Lite.
gcloud dataproc jobs submit spark --cluster=$CLUSTER_NAME \
  --jars=$BUCKET/pubsublite-spark-snippets-$SAMPLE_VERSION.jar,gs://spark-lib/pubsublite/pubsublite-spark-sql-streaming-$CONNECTOR_VERSION-with-dependencies.jar \
  --class=pubsublite.spark.WordCount -- \
  projects/$PROJECT_NUMBER/locations/$REGION-$ZONE_ID/subscriptions/$SOURCE_SUBSCRIPTION_ID \
  projects/$PROJECT_NUMBER/locations/$REGION-$ZONE_ID/topics/$DESTINATION_TOPIC_ID

# Read word count results from Pub/Sub Lite, you should see the result in console output.
mvn compile exec:java -Dexec.mainClass=pubsublite.spark.ReadResults