# Google Pub/Sub Lite Spark Connector Client for Java

Java idiomatic client for [Pub/Sub Lite Spark Connector][product-docs].

[![Maven][maven-version-image]][maven-version-link]
![Stability][stability-image]

- [Product Documentation][product-docs]
- [Client Library Documentation][javadocs]

> Note: This client is a work-in-progress, and may occasionally
> make backwards-incompatible changes.


## Quickstart


If you are using Maven, add this to your pom.xml file:


```xml
<dependency>
  <groupId>com.google.cloud</groupId>
  <artifactId>pubsublite-spark-sql-streaming</artifactId>
  <version>1.0.0</version>
</dependency>
```

If you are using Gradle without BOM, add this to your dependencies:

```Groovy
implementation 'com.google.cloud:pubsublite-spark-sql-streaming:1.0.0'
```

If you are using SBT, add this to your dependencies:

```Scala
libraryDependencies += "com.google.cloud" % "pubsublite-spark-sql-streaming" % "1.0.0"
```

## Authentication

See the [Authentication][authentication] section in the base directory's README.

## Authorization

The client application making API calls must be granted [authorization scopes][auth-scopes] required for the desired Pub/Sub Lite Spark Connector APIs, and the authenticated principal must have the [IAM role(s)][predefined-iam-roles] required to access GCP resources using the Pub/Sub Lite Spark Connector API calls.

## Getting Started

### Prerequisites

You will need a [Google Cloud Platform Console][developer-console] project with the Pub/Sub Lite Spark Connector [API enabled][enable-api].
You will need to [enable billing][enable-billing] to use Google Pub/Sub Lite Spark Connector.
[Follow these instructions][create-project] to get your project set up. You will also need to set up the local development environment by
[installing the Google Cloud SDK][cloud-sdk] and running the following commands in command line:
`gcloud auth login` and `gcloud config set project [YOUR PROJECT ID]`.

### Installation and setup

You'll need to obtain the `pubsublite-spark-sql-streaming` library.  See the [Quickstart](#quickstart) section
to add `pubsublite-spark-sql-streaming` as a dependency in your code.

## About Pub/Sub Lite Spark Connector

[Google Cloud Pub/Sub Lite][product-docs] is a zonal, real-time messaging
service that lets you send and receive messages between independent
applications. You can manually configure the throughput and storage capacity
for Pub/Sub Lite systems.

The Pub/Sub Lite Spark connector supports Pub/Sub Lite as an input source to
Apache Spark Structured Streaming in both the default micro-batch processing
mode and the _experimental_ continous processing mode. The connector works in
all Apache Spark distributions, including [Google Cloud Dataproc](https://cloud.google.com/dataproc/docs/)
and manual Spark installations.



## Requirements

### Creating a new subscription or using an existing subscription

 Follow [the instruction](https://cloud.google.com/pubsub/lite/docs/quickstart#create_a_lite_subscription) to create a new subscription or use an existing subscription. If using an existing subscription, the connector will read from the oldest unacknowledged message in the subscription.

### Creating a Google Cloud Dataproc cluster (Optional)

 If you do not have an Apache Spark environment, you can create a [Cloud Dataproc](https://cloud.google.com/dataproc/docs) cluster with pre-configured auth. The following examples assume you are using Cloud Dataproc, but you can use `spark-submit` on any cluster.

  ```
  MY_CLUSTER=...
  gcloud dataproc clusters create "$MY_CLUSTER"
  ```

## Downloading and Using the Connector
  The latest version of the connector is publicly available in the following link:
  | Connector version | Link |
  | --- | --- |
  | 1.0.0 | `gs://spark-lib/pubsublite/pubsublite-spark-sql-streaming-LATEST-with-dependencies.jar`([HTTP link](https://storage.googleapis.com/spark-lib/pubsublite/pubsublite-spark-sql-streaming-LATEST-with-dependencies.jar)) |

  The connector is also available from the [Maven Central repository](https://repo1.maven.org/maven2/com/google/cloud/pubsublite-spark-sql-streaming/). You can download and pass it in the `--jars` option when using the `spark-submit` command.

## Compatibility
| Connector version | Spark version |
| --- | --- |
| ≤0.3.4 | 2.4.X |
| Current | 3.X.X |

## Usage

### Samples

  There are 3 java samples (word count, simple write, simple read) under [samples](https://github.com/googleapis/java-pubsublite-spark/tree/master/samples) that shows using the connector inside Dataproc.

### Reading data from Pub/Sub Lite

  Here is an example in Python:
  ```python
  df = spark.readStream \
    .format("pubsublite") \
    .option("pubsublite.subscription", "projects/$PROJECT_NUMBER/locations/$LOCATION/subscriptions/$SUBSCRIPTION_ID") \
    .load
  ```
  Here is an example in Java:
  ```java
  Dataset<Row> df = spark
    .readStream()
    .format("pubsublite")
    .option("pubsublite.subscription", "projects/$PROJECT_NUMBER/locations/$LOCATION/subscriptions/$SUBSCRIPTION_ID")
    .load();
  ```

  Note that the connector supports both MicroBatch Processing and [Continuous Processing](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#continuous-processing).

### Writing data to Pub/Sub Lite

  Here is an example in Python:
  ```python
  df.writeStream \
    .format("pubsublite") \
    .option("pubsublite.topic", "projects/$PROJECT_NUMBER/locations/$LOCATION/topics/$TOPIC_ID") \
    .option("checkpointLocation", "path/to/HDFS/dir")
    .outputMode("complete") \
    .trigger(processingTime="2 seconds") \
    .start()
  ```
  Here is an example in Java:
  ```java
  df.writeStream()
    .format("pubsublite")
    .option("pubsublite.topic", "projects/$PROJECT_NUMBER/locations/$LOCATION/topics/$TOPIC_ID")
    .option("checkpointLocation", "path/to/HDFS/dir")
    .outputMode(OutputMode.Complete())
    .trigger(Trigger.ProcessingTime(2, TimeUnit.SECONDS))
    .start();
  ```

### Properties

When reading from Pub/Sub Lite, the connector supports a number of configuration options:

  | Option | Type | Required | Default Value | Meaning |
  | ------ | ---- | -------- | ------------- | ------- |
  | pubsublite.subscription | String | Y | | Full subscription path that the connector will read from. |
  | pubsublite.flowcontrol.byteoutstandingperpartition | Long | N | 50_000_000 | Max number of bytes per partition that will be cached in workers before Spark processes the messages. |
  | pubsublite.flowcontrol.messageoutstandingperpartition | Long | N | Long.MAX | Max number of messages per partition that will be cached in workers before Spark processes the messages. |
  | pubsublite.flowcontrol.maxmessagesperbatch | Long | N | Long.MAX | Max number of messages in micro batch. |
  | gcp.credentials.key | String | N | [Application Default Credentials](https://cloud.google.com/docs/authentication/production#automatically) | Service account JSON in base64. |

When writing to Pub/Sub Lite, the connector supports a number of configuration options:

  | Option | Type | Required | Default Value | Meaning |
  | ------ | ---- | -------- | ------------- | ------- |
  | pubsublite.topic | String | Y | | Full topic path that the connector will write to. |
  | gcp.credentials.key | String | N | [Application Default Credentials](https://cloud.google.com/docs/authentication/production#automatically) | Service account JSON in base64. |

### Data Schema

When reading from Pub/Sub Lite, the connector has a fixed data schema as follows:

  | Data Field | Spark Data Type | Notes |
  | ---------- | --------------- | ----- |
  | subscription | StringType | Full subscription path |
  | partition | LongType | |
  | offset | LongType | |
  | key | BinaryType | |
  | data | BinaryType | |
  | attributes | MapType\[StringType, ArrayType\[BinaryType\]\] | |
  | publish_timestamp | TimestampType | |
  | event_timestamp | TimestampType | Nullable |

When writing to Pub/Sub Lite, the connetor matches the following data field and data types as follows:

  | Data Field | Spark Data Type | Required |
  | ---------- | --------------- | ----- |
  | key | BinaryType | N |
  | data | BinaryType | N |
  | attributes | MapType\[StringType, ArrayType\[BinaryType\]\] | N |
  | event_timestamp | TimestampType | N |

Note that when a data field is present in the table but the data type mismatches, the connector will throw IllegalArgumentException that terminates the query.

## Building the Connector

The connector is built using Maven. Following command creates a JAR file with shaded dependencies:

  ```sh
  mvn package
  ```

## FAQ

### What is the cost for the Pub/Sub Lite?

See the [Pub/Sub Lite pricing documentation](https://cloud.google.com/pubsub/lite/pricing).

### Can I configure the number of Spark partitions?

No, the number of Spark partitions is set to be the number of Pub/Sub Lite partitions of the topic that the subscription is attached to.

### How do I authenticate outside Cloud Compute Engine / Cloud Dataproc?

Use a service account JSON key and `GOOGLE_APPLICATION_CREDENTIALS` as described [here](https://cloud.google.com/docs/authentication/getting-started).

Credentials can be provided with `gcp.credentials.key` option, it needs to be passed in as a base64-encoded string.

Example:
  ```java
  spark.readStream.format("pubsublite").option("gcp.credentials.key", "<SERVICE_ACCOUNT_JSON_IN_BASE64>")
  ```




## Samples

Samples are in the [`samples/`](https://github.com/googleapis/java-pubsublite-spark/tree/main/samples) directory.

| Sample                      | Source Code                       | Try it |
| --------------------------- | --------------------------------- | ------ |
| Admin Utils | [source code](https://github.com/googleapis/java-pubsublite-spark/blob/main/samples/snippets/src/main/java/pubsublite/spark/AdminUtils.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-pubsublite-spark&page=editor&open_in_editor=samples/snippets/src/main/java/pubsublite/spark/AdminUtils.java) |
| Common Utils | [source code](https://github.com/googleapis/java-pubsublite-spark/blob/main/samples/snippets/src/main/java/pubsublite/spark/CommonUtils.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-pubsublite-spark&page=editor&open_in_editor=samples/snippets/src/main/java/pubsublite/spark/CommonUtils.java) |
| Publish Words | [source code](https://github.com/googleapis/java-pubsublite-spark/blob/main/samples/snippets/src/main/java/pubsublite/spark/PublishWords.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-pubsublite-spark&page=editor&open_in_editor=samples/snippets/src/main/java/pubsublite/spark/PublishWords.java) |
| Read Results | [source code](https://github.com/googleapis/java-pubsublite-spark/blob/main/samples/snippets/src/main/java/pubsublite/spark/ReadResults.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-pubsublite-spark&page=editor&open_in_editor=samples/snippets/src/main/java/pubsublite/spark/ReadResults.java) |
| Simple Read | [source code](https://github.com/googleapis/java-pubsublite-spark/blob/main/samples/snippets/src/main/java/pubsublite/spark/SimpleRead.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-pubsublite-spark&page=editor&open_in_editor=samples/snippets/src/main/java/pubsublite/spark/SimpleRead.java) |
| Simple Write | [source code](https://github.com/googleapis/java-pubsublite-spark/blob/main/samples/snippets/src/main/java/pubsublite/spark/SimpleWrite.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-pubsublite-spark&page=editor&open_in_editor=samples/snippets/src/main/java/pubsublite/spark/SimpleWrite.java) |
| Word Count | [source code](https://github.com/googleapis/java-pubsublite-spark/blob/main/samples/snippets/src/main/java/pubsublite/spark/WordCount.java) | [![Open in Cloud Shell][shell_img]](https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-pubsublite-spark&page=editor&open_in_editor=samples/snippets/src/main/java/pubsublite/spark/WordCount.java) |



## Troubleshooting

To get help, follow the instructions in the [shared Troubleshooting document][troubleshooting].

## Transport

Pub/Sub Lite Spark Connector uses gRPC for the transport layer.

## Supported Java Versions

Java 8 or above is required for using this client.

Google's Java client libraries,
[Google Cloud Client Libraries][cloudlibs]
and
[Google Cloud API Libraries][apilibs],
follow the
[Oracle Java SE support roadmap][oracle]
(see the Oracle Java SE Product Releases section).

### For new development

In general, new feature development occurs with support for the lowest Java
LTS version covered by  Oracle's Premier Support (which typically lasts 5 years
from initial General Availability). If the minimum required JVM for a given
library is changed, it is accompanied by a [semver][semver] major release.

Java 11 and (in September 2021) Java 17 are the best choices for new
development.

### Keeping production systems current

Google tests its client libraries with all current LTS versions covered by
Oracle's Extended Support (which typically lasts 8 years from initial
General Availability).

#### Legacy support

Google's client libraries support legacy versions of Java runtimes with long
term stable libraries that don't receive feature updates on a best efforts basis
as it may not be possible to backport all patches.

Google provides updates on a best efforts basis to apps that continue to use
Java 7, though apps might need to upgrade to current versions of the library
that supports their JVM.

#### Where to find specific information

The latest versions and the supported Java versions are identified on
the individual GitHub repository `github.com/GoogleAPIs/java-SERVICENAME`
and on [google-cloud-java][g-c-j].

## Versioning


This library follows [Semantic Versioning](http://semver.org/).


It is currently in major version zero (``0.y.z``), which means that anything may change at any time
and the public API should not be considered stable.


## Contributing


Contributions to this library are always welcome and highly encouraged.

See [CONTRIBUTING][contributing] for more information how to get started.

Please note that this project is released with a Contributor Code of Conduct. By participating in
this project you agree to abide by its terms. See [Code of Conduct][code-of-conduct] for more
information.


## License

Apache 2.0 - See [LICENSE][license] for more information.

## CI Status

Java Version | Status
------------ | ------
Java 8 | [![Kokoro CI][kokoro-badge-image-2]][kokoro-badge-link-2]
Java 8 OSX | [![Kokoro CI][kokoro-badge-image-3]][kokoro-badge-link-3]
Java 8 Windows | [![Kokoro CI][kokoro-badge-image-4]][kokoro-badge-link-4]
Java 11 | [![Kokoro CI][kokoro-badge-image-5]][kokoro-badge-link-5]

Java is a registered trademark of Oracle and/or its affiliates.

[product-docs]: https://cloud.google.com/pubsub/lite/docs
[javadocs]: https://cloud.google.com/java/docs/reference/pubsublite-spark-sql-streaming/latest/history
[kokoro-badge-image-1]: http://storage.googleapis.com/cloud-devrel-public/java/badges/java-pubsublite-spark/java7.svg
[kokoro-badge-link-1]: http://storage.googleapis.com/cloud-devrel-public/java/badges/java-pubsublite-spark/java7.html
[kokoro-badge-image-2]: http://storage.googleapis.com/cloud-devrel-public/java/badges/java-pubsublite-spark/java8.svg
[kokoro-badge-link-2]: http://storage.googleapis.com/cloud-devrel-public/java/badges/java-pubsublite-spark/java8.html
[kokoro-badge-image-3]: http://storage.googleapis.com/cloud-devrel-public/java/badges/java-pubsublite-spark/java8-osx.svg
[kokoro-badge-link-3]: http://storage.googleapis.com/cloud-devrel-public/java/badges/java-pubsublite-spark/java8-osx.html
[kokoro-badge-image-4]: http://storage.googleapis.com/cloud-devrel-public/java/badges/java-pubsublite-spark/java8-win.svg
[kokoro-badge-link-4]: http://storage.googleapis.com/cloud-devrel-public/java/badges/java-pubsublite-spark/java8-win.html
[kokoro-badge-image-5]: http://storage.googleapis.com/cloud-devrel-public/java/badges/java-pubsublite-spark/java11.svg
[kokoro-badge-link-5]: http://storage.googleapis.com/cloud-devrel-public/java/badges/java-pubsublite-spark/java11.html
[stability-image]: https://img.shields.io/badge/stability-preview-yellow
[maven-version-image]: https://img.shields.io/maven-central/v/com.google.cloud/pubsublite-spark-sql-streaming.svg
[maven-version-link]: https://search.maven.org/search?q=g:com.google.cloud%20AND%20a:pubsublite-spark-sql-streaming&core=gav
[authentication]: https://github.com/googleapis/google-cloud-java#authentication
[auth-scopes]: https://developers.google.com/identity/protocols/oauth2/scopes
[predefined-iam-roles]: https://cloud.google.com/iam/docs/understanding-roles#predefined_roles
[iam-policy]: https://cloud.google.com/iam/docs/overview#cloud-iam-policy
[developer-console]: https://console.developers.google.com/
[create-project]: https://cloud.google.com/resource-manager/docs/creating-managing-projects
[cloud-sdk]: https://cloud.google.com/sdk/
[troubleshooting]: https://github.com/googleapis/google-cloud-common/blob/main/troubleshooting/readme.md#troubleshooting
[contributing]: https://github.com/googleapis/java-pubsublite-spark/blob/main/CONTRIBUTING.md
[code-of-conduct]: https://github.com/googleapis/java-pubsublite-spark/blob/main/CODE_OF_CONDUCT.md#contributor-code-of-conduct
[license]: https://github.com/googleapis/java-pubsublite-spark/blob/main/LICENSE
[enable-billing]: https://cloud.google.com/apis/docs/getting-started#enabling_billing
[enable-api]: https://console.cloud.google.com/flows/enableapi?apiid=pubsublite.googleapis.com
[libraries-bom]: https://github.com/GoogleCloudPlatform/cloud-opensource-java/wiki/The-Google-Cloud-Platform-Libraries-BOM
[shell_img]: https://gstatic.com/cloudssh/images/open-btn.png

[semver]: https://semver.org/
[cloudlibs]: https://cloud.google.com/apis/docs/client-libraries-explained
[apilibs]: https://cloud.google.com/apis/docs/client-libraries-explained#google_api_client_libraries
[oracle]: https://www.oracle.com/java/technologies/java-se-support-roadmap.html
[g-c-j]: http://github.com/googleapis/google-cloud-java
