package jobs.stream;

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;

public class SparkKafkaConsumerActivityNDemographic {

    public static void main(String[] args) throws Exception {

        SparkSession spark = SparkSession.builder()
                .appName("Spark Kafka Consumer Activity + Demographic")
                .master("local[2]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        // -------------------------------
        // 1. Read from Kafka (JSON)
        // -------------------------------

        Dataset<Row> kafkaStream = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "consumer_activity")
                .option("startingOffsets", "earliest")
                .load();

        Dataset<Row> activityStream = kafkaStream
                .selectExpr("CAST(value AS STRING)")
                .select(from_json(
                        col("value"),
                        getSchema()
                ).as("data"))
                .select("data.*");

        // -------------------------------
        // 2. Load MySQL demographic table
        // -------------------------------

        Dataset<Row> demographicDF = spark.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://127.0.0.1:3306/users?useSSL=false&allowPublicKeyRetrieval=true")
                .option("dbtable", "users.userdemographics")
                .option("user", "root")
                .option("password", "example")
                .load();

        // -------------------------------
        // 3. Join
        // -------------------------------

        Dataset<Row> enrichedStream = activityStream
                .join(demographicDF,
                        activityStream.col("id")
                                .equalTo(demographicDF.col("i")))
                .select(
                        activityStream.col("id"),
                        activityStream.col("campaignid"),
                        activityStream.col("orderid"),
                        activityStream.col("total_amount"),
                        activityStream.col("units"),
                        activityStream.col("activity"),
                        demographicDF.col("country")
                );

        // -------------------------------
        // 4. Aggregation
        // -------------------------------

        Dataset<Row> purchaseAgg = enrichedStream
                .filter("activity = 'purchase'")
                .groupBy("country")
                .agg(
                        count("*").alias("purchase_count"),
                        sum("total_amount").alias("total_revenue")
                );

        // -------------------------------
        // 5. Write to MySQL
        // -------------------------------

        StreamingQuery mysqlQuery = purchaseAgg
                .writeStream()
                .outputMode("complete")
                .foreachBatch((batchDF, batchId) -> {
                    batchDF.write()
                            .format("jdbc")
                            .option("url", "jdbc:mysql://127.0.0.1:3306/users?useSSL=false&allowPublicKeyRetrieval=true")
                            .option("dbtable", "users.countryAgg")
                            .option("user", "root")
                            .option("password", "example")
                            .mode("overwrite")
                            .save();
                })
                .option("checkpointLocation", "C:/spark-checkpoints/mysql")
                .start();

        mysqlQuery.awaitTermination();
    }

    private static StructType getSchema() {
        return new StructType()
                .add("id", "integer")
                .add("campaignid", "integer")
                .add("orderid", "integer")
                .add("total_amount", "integer")
                .add("units", "integer")
                .add("activity", "string");
    }
}