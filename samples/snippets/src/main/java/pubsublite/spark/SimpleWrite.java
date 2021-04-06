package pubsublite.spark;

import com.google.common.collect.ImmutableList;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.ByteArray;

public class SimpleWrite {
  private static final StructType TABLE_SCHEMA =
          new StructType(
                  new StructField[] {
                          new StructField("key", DataTypes.BinaryType, false, Metadata.empty()),
                          new StructField("data", DataTypes.BinaryType, false, Metadata.empty())
                  });

  public static void main(String[] args) throws Exception {

    final String destinationTopicPath = args[1];

    SparkSession spark =
        SparkSession.builder().appName("Simple write").master("yarn").getOrCreate();

    // Read messages from Pub/Sub Lite
    Dataset<Row> df =
        spark.createDataFrame(
            ImmutableList.of(
                createRow("key1", "foo"), createRow("key1", "bar"), createRow("key2", "foobar")),
            TABLE_SCHEMA);

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

  private static GenericRow createRow(String key, String data) {
    Object[] list = {
      ByteArray.concat(key.getBytes(StandardCharsets.UTF_8)),
      ByteArray.concat(data.getBytes(StandardCharsets.UTF_8))
    };
    return new GenericRow(list);
  }
}
