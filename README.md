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
  <artifactId>pubsublite-spark</artifactId>
  <version>0.0.0</version>
</dependency>
```

If you are using Gradle without BOM, add this to your dependencies
```Groovy
compile 'com.google.cloud:pubsublite-spark:0.0.0'
```

If you are using SBT, add this to your dependencies
```Scala
libraryDependencies += "com.google.cloud" % "pubsublite-spark" % "0.0.0"
```

## Authentication

See the [Authentication][authentication] section in the base directory's README.

## Getting Started

### Prerequisites

You will need a [Google Cloud Platform Console][developer-console] project with the Pub/Sub Lite Spark Connector [API enabled][enable-api].
You will need to [enable billing][enable-billing] to use Google Pub/Sub Lite Spark Connector.
[Follow these instructions][create-project] to get your project set up. You will also need to set up the local development environment by
[installing the Google Cloud SDK][cloud-sdk] and running the following commands in command line:
`gcloud auth login` and `gcloud config set project [YOUR PROJECT ID]`.

### Installation and setup

You'll need to obtain the `pubsublite-spark` library.  See the [Quickstart](#quickstart) section
to add `pubsublite-spark` as a dependency in your code.

## About Pub/Sub Lite Spark Connector


[Pub/Sub Lite Spark Connector][product-docs] is designed to provide reliable,
many-to-many, asynchronous messaging between applications. Publisher
applications can send messages to a topic and other applications can
subscribe to that topic to receive the messages. By decoupling senders and
receivers, Google Cloud Pub/Sub allows developers to communicate between
independently written applications.

Compared to Google Pub/Sub, Pub/Sub Lite provides partitioned zonal data
storage with predefined capacity. Both products present a similar API, but
Pub/Sub Lite has more usage caveats.

See the [Google Pub/Sub Lite docs](https://cloud.google.com/pubsub/quickstart-console#before-you-begin) for more details on how to activate
Pub/Sub Lite for your project, as well as guidance on how to choose between
Cloud Pub/Sub and Pub/Sub Lite.

See the [Pub/Sub Lite Spark Connector client library docs][javadocs] to learn how to
use this Pub/Sub Lite Spark Connector Client Library.


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

<!--- TODO(jiangmichael): Add jar link for spark-pubsublite-latest.jar -->
  The latest version connector of the connector (Scala 2.11) will be publicly available in `gs://spark-lib/pubsublite/spark-pubsublite-latest.jar`.

<!--- TODO(jiangmichael): Release on Maven Central and add Maven Central link -->
  The connector will also be available from the Maven Central repository. It can be used using the `--packages` option or the `spark.jars.packages` configuration property.

<!--
  | Scala version | Connector Artifact |
  | --- | --- |
  | Scala 2.11 | `com.google.cloud.pubsublite.spark:pubsublite-spark-sql-streaming-with-dependencies_2.11:0.1.0` |
-->

<!--- TODO(jiangmichael): Add exmaple code and brief description here -->

## Usage

### Reading data from Pub/Sub Lite

  ```python
  df = spark.readStream \
    .option("pubsublite.subscription", "projects/$PROJECT_NUMBER/locations/$LOCATION/subscriptions/$SUBSCRIPTION_ID")
    .format("pubsublite") \
    .load
  ```

  Note that the connector supports both MicroBatch Processing and [Continuous Processing](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#continuous-processing).

### Properties

The connector supports a number of options to configure the read:

  | Option | Type | Required | Meaning |
  | ------ | ---- | -------- | ------- |
  | pubsublite.subscription | String | Y | Full subscription path that the connector will read from. |
  | pubsublite.flowcontrol.byteoutstandingperpartition | Long | N | Max number of bytes per partition that will be cached in workers before Spark processes the messages. Default to 50000000 bytes. |
  | pubsublite.flowcontrol.messageoutstandingperpartition | Long | N | Max number of messages per partition that will be cached in workers before Spark processes the messages. Default to Long.MAX_VALUE. |
  | gcp.credentials.key | String | N | Service account JSON in base64. Default to [Application Default Credentials](https://cloud.google.com/docs/authentication/production#automatically). |

### Data Schema

The connector has fixed data schema as follows:

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





## Troubleshooting

To get help, follow the instructions in the [shared Troubleshooting document][troubleshooting].

## Transport

Pub/Sub Lite Spark Connector uses gRPC for the transport layer.

## Java Versions

Java 8 or above is required for using this client.

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
[javadocs]: https://googleapis.dev/java/google-cloud-pubsublite/latest/index.html
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
[stability-image]: https://img.shields.io/badge/stability-alpha-orange
[maven-version-image]: https://img.shields.io/maven-central/v/com.google.cloud/pubsublite-spark.svg
[maven-version-link]: https://search.maven.org/search?q=g:com.google.cloud%20AND%20a:pubsublite-spark&core=gav
[authentication]: https://github.com/googleapis/google-cloud-java#authentication
[developer-console]: https://console.developers.google.com/
[create-project]: https://cloud.google.com/resource-manager/docs/creating-managing-projects
[cloud-sdk]: https://cloud.google.com/sdk/
[troubleshooting]: https://github.com/googleapis/google-cloud-common/blob/master/troubleshooting/readme.md#troubleshooting
[contributing]: https://github.com/googleapis/java-pubsublite-spark/blob/master/CONTRIBUTING.md
[code-of-conduct]: https://github.com/googleapis/java-pubsublite-spark/blob/master/CODE_OF_CONDUCT.md#contributor-code-of-conduct
[license]: https://github.com/googleapis/java-pubsublite-spark/blob/master/LICENSE
[enable-billing]: https://cloud.google.com/apis/docs/getting-started#enabling_billing
[enable-api]: https://console.cloud.google.com/flows/enableapi?apiid=pubsublite.googleapis.com
[libraries-bom]: https://github.com/GoogleCloudPlatform/cloud-opensource-java/wiki/The-Google-Cloud-Platform-Libraries-BOM
[shell_img]: https://gstatic.com/cloudssh/images/open-btn.png
