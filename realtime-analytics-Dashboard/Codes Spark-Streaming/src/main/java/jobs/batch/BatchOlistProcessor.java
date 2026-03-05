package jobs.batch;

import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;

public class BatchOlistProcessor {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("Olist Batch Processing")
                .master("local[*]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        // Load CSV files
        Dataset<Row> orders = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("olist-dataset/olist_orders_dataset.csv");

        Dataset<Row> customers = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("olist-dataset/olist_customers_dataset.csv");

        Dataset<Row> payments = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("olist-dataset/olist_order_payments_dataset.csv");

        // Join orders + customers
        Dataset<Row> ordersWithState = orders
                .join(customers,
                        orders.col("customer_id")
                                .equalTo(customers.col("customer_id")));

        // Join payments
        Dataset<Row> fullData = ordersWithState
                .join(payments,
                        ordersWithState.col("order_id")
                                .equalTo(payments.col("order_id")));

        // ---------------------------
        // 1️⃣ Revenue by State
        // ---------------------------
        Dataset<Row> stateRevenue = fullData
                .groupBy("customer_state")
                .agg(sum("payment_value").alias("total_revenue"));

        stateRevenue.write()
                .format("jdbc")
                .option("url", "jdbc:mysql://127.0.0.1:3306/users?useSSL=false")
                .option("dbtable", "users.batch_state_revenue")
                .option("user", "root")
                .option("password", "example")
                .mode("overwrite")
                .save();

        // ---------------------------
        // 2️⃣ Monthly Revenue
        // ---------------------------
        Dataset<Row> monthlyRevenue = fullData
                .withColumn("month",
                        date_format(col("order_purchase_timestamp"), "yyyy-MM"))
                .groupBy("month")
                .agg(sum("payment_value").alias("total_revenue"));

        monthlyRevenue.write()
                .format("jdbc")
                .option("url", "jdbc:mysql://127.0.0.1:3306/users?useSSL=false")
                .option("dbtable", "users.batch_monthly_revenue")
                .option("user", "root")
                .option("password", "example")
                .mode("overwrite")
                .save();

        System.out.println("Batch Processing Completed Successfully");

        spark.stop();
    }
}