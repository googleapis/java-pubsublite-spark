#!/bin/bash
# Bash script that runs simple read sample.
set -e

if [ "$1" == "run" ]; then
  echo "Running simple read sample..."

  cd samples/snippets

  # Set the current sample version.
  export SAMPLE_VERSION=$(mvn -q \
    -Dexec.executable=echo \
    -Dexec.args="${project.version}" \
    --non-recursive \
    exec:exec)

  # Set extra environment variables.
  export SOURCE_SUBSCRIPTION_PATH=projects/$PROJECT_NUMBER/locations/$REGION-$ZONE_ID/subscriptions/$SOURCE_SUBSCRIPTION_ID

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

  # Run the sample in Dataproc. You would see the messages show up in the console output.
  gcloud dataproc jobs submit spark --cluster=$CLUSTER_NAME \
    --jars=$BUCKET/pubsublite-spark-snippets-$SAMPLE_VERSION.jar,gs://spark-lib/pubsublite/pubsublite-spark-sql-streaming-$CONNECTOR_VERSION-with-dependencies.jar \
    --class=pubsublite.spark.SimpleRead \
    --properties=spark.yarn.appMasterEnv.SOURCE_SUBSCRIPTION_PATH=$SOURCE_SUBSCRIPTION_PATH

  echo "Simple read sample finished."
elif [ "$1" == "clean" ]; then
  echo "Cleaning up..."

  # Delete Pub/Sub Lite topic and subscription.
  gcloud pubsub lite-subscriptions delete $SOURCE_SUBSCRIPTION_ID --zone=$REGION-$ZONE_ID
  gcloud pubsub lite-topics delete $SOURCE_TOPIC_ID --zone=$REGION-$ZONE_ID

  # Delete GCS bucket.
  gsutil -m rm -rf $BUCKET

  # Delete Dataproc cluster.
  gcloud dataproc clusters delete $CLUSTER_NAME --region=$REGION
  echo "Clean up finished."
else
  echo "Invalid arguments, should be either run or clean."
  exit 1
