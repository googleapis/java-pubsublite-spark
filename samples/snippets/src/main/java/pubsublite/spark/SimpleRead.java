package pubsublite.spark;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;

import java.util.concurrent.TimeUnit;

public class SimpleRead {

    public static void main(String[] args) throws Exception {

        final String sourceSubscriptionPath = args[0];

        SparkSession spark = SparkSession.builder().appName("Simple read").master("yarn").getOrCreate();

        // Read messages from Pub/Sub Lite
        Dataset<Row> df =
                spark
                        .readStream()
                        .format("pubsublite")
                        .option("pubsublite.subscription", sourceSubscriptionPath)
                        .load();

        // Write messages to Console Output
        StreamingQuery query =
                df.writeStream()
                        .format("console")
                        .outputMode(OutputMode.Append())
                        .trigger(Trigger.ProcessingTime(1, TimeUnit.SECONDS))
                        .start();
        query.awaitTermination(60 * 1000); // 60s
        query.stop();
    }
}
