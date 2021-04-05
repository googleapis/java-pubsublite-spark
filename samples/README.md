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

To run the word count sample in Dataproc cluster:
```sh
word_count_sample.sh run
```

### Cleaning up
```sh
word_count_sample.sh clean
```

### Common issues
1. Permission not granted. <br>
   This could happen when creating a topic and a subscription, or submitting a job to your Dataproc cluster.
   Make sure your service account has at least `Editor` permissions for Pub/Sub Lite and Dataproc. 
   Your Dataproc cluster needs `scope=cloud-platform` to access other services and resources within the same project.
   Your `gcloud` and `GOOGLE_APPLICATION_CREDENTIALS` should access the same project. Check out which project your `gcloud` and `gstuil` commands use with `gcloud config get-value project`.

2. Your Dataproc job fails with `ClassNotFound` or similar exceptions. <br>
   Make sure your Dataproc cluster uses images of [supported Spark versions](https://github.com/googleapis/java-pubsublite-spark#compatibility). 

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

To run the simple read sample in Dataproc cluster:
```sh
simple_read_sample.sh run
```

### Cleaning up
```sh
simple_read_sample.sh clean
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

To run the simple write sample in Dataproc cluster:
```sh
simple_write_sample.sh run
```

### Cleaning up
```sh
simple_write_sample.sh clean
```