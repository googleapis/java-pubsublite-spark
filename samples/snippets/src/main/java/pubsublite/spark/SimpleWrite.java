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

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.split;

public class SimpleWrite {

    public static void main(String[] args) throws Exception {

        final String destinationTopicPath = args[1];

        SparkSession spark = SparkSession.builder().appName("Simple write").master("yarn").getOrCreate();

        // Read messages from Pub/Sub Lite
        List<Row>
        Dataset<Row> df = spark.createDataFrame();

        // Write word count results to Pub/Sub Lite
        StreamingQuery query =
                df.writeStream()
                        .format("pubsublite")
                        .option("pubsublite.topic", destinationTopicPath)
                        .option("checkpointLocation", "/tmp/checkpoint")
                        .outputMode(OutputMode.Complete())
                        .trigger(Trigger.ProcessingTime(1, TimeUnit.SECONDS))
                        .start();
        query.awaitTermination(60 * 1000); // 60s
        query.stop();
    }
}
