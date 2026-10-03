# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # ⚡ Delta Live Tables (DLT) Pipeline
# MAGIC ## 🍕 FoodRush India | Medallion Architecture
# MAGIC ---
# MAGIC ### Tables defined in this notebook:
# MAGIC | Layer | Table Name | Type | Source |
# MAGIC |-------|-----------|------|--------|
# MAGIC | 🥉 Bronze | `bronze_orders` | Streaming | Auto Loader (CSV landing zone) |
# MAGIC | 🥈 Silver | `silver_orders` | Streaming | `bronze_orders` |
# MAGIC | 🥇 Gold | `gold_city_revenue` | Batch | `silver_orders` |
# MAGIC | 🥇 Gold | `gold_restaurant_performance` | Batch | `silver_orders` |
# MAGIC | 🥇 Gold | `gold_payment_insights` | Batch | `silver_orders` |
# MAGIC
# MAGIC ---
# MAGIC ### ⚠️ Important — How DLT notebooks are different:
# MAGIC | Regular Streaming Notebook | DLT Notebook |
# MAGIC |---------------------------|-------------|
# MAGIC | You write `writeStream`, checkpoints, `.start()` | DLT manages all of that automatically |
# MAGIC | You call `awaitTermination()` or `.stop()` | DLT manages the lifecycle |
# MAGIC | Each cell runs top to bottom | DLT reads ALL cells, builds a DAG, then runs |
# MAGIC | You run the notebook directly | You **never** run this notebook directly — attach it to a Pipeline in the UI |
# MAGIC
# MAGIC > 🔑 **Run this notebook by creating a DLT Pipeline in the UI and pointing it here.**
# MAGIC > See the Setup Guide document for step-by-step instructions.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Step 1: Imports and Path Configuration

# COMMAND ----------

import dlt

from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import (
    col, to_date, to_timestamp, when, round,
    current_timestamp, lit, concat, trim,
    count, sum, avg, max, min
)
from pyspark.sql.types import IntegerType, DoubleType

# Landing zone path — where the Data Generator writes CSV files
RAW_LANDING_PATH   = "dbfs:/FileStore/foodrush/landing/orders/"

# Schema location for Auto Loader — DLT needs a stable path to store schema info
BRONZE_SCHEMA_PATH = "dbfs:/FileStore/foodrush/schema/dlt/bronze/"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🥉 Step 2: Bronze Table — Raw Ingestion via Auto Loader
# MAGIC
# MAGIC - Reads CSV files continuously from the landing zone
# MAGIC - All columns kept as `StringType` — raw, no transformation
# MAGIC - Adds `ingestion_timestamp` and `source_file` metadata columns
# MAGIC - DLT automatically handles checkpointing and exactly-once ingestion

# COMMAND ----------

@dlt.table(
    name    = "bronze_orders",
    comment = "Raw food delivery orders ingested from CSV landing zone using Auto Loader. All columns are StringType."
)
def bronze_orders():

    bronze_schema = StructType([
        StructField("order_id",         StringType(), True),
        StructField("customer_id",      StringType(), True),
        StructField("restaurant_name",  StringType(), True),
        StructField("cuisine_type",     StringType(), True),
        StructField("city",             StringType(), True),
        StructField("order_date",       StringType(), True),
        StructField("order_time",       StringType(), True),
        StructField("food_item",        StringType(), True),
        StructField("quantity",         StringType(), True),
        StructField("item_price",       StringType(), True),
        StructField("delivery_fee",     StringType(), True),
        StructField("payment_mode",     StringType(), True),
        StructField("delivery_status",  StringType(), True),
        StructField("delivery_minutes", StringType(), True),
        StructField("customer_rating",  StringType(), True)
    ])

    return (
        spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format",         "csv")
             .option("header",                    "true")
             .option("cloudFiles.schemaLocation", BRONZE_SCHEMA_PATH)
             .schema(bronze_schema)
             .load(RAW_LANDING_PATH)
             .withColumn("ingestion_timestamp", current_timestamp())
             .withColumn("source_file",         col("_metadata.file_path"))
             .withColumn("layer",               lit("bronze"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🥈 Step 3: Silver Table — Cleaning, Typing & Enrichment
# MAGIC
# MAGIC - Reads from `bronze_orders` using `dlt.read_stream()` — preserves streaming end-to-end
# MAGIC - Casts all string columns to correct types
# MAGIC - Handles nulls for cancelled orders
# MAGIC - Computes `subtotal`, `gst_amount` (18%), `total_amount`
# MAGIC - Adds `is_cancelled`, `is_late_delivery`, `order_size` flags
# MAGIC
# MAGIC ### Data Quality Rules (Expectations):
# MAGIC | Rule | Action | Condition |
# MAGIC |------|--------|-----------|
# MAGIC | `valid_order_id` | Drop bad rows | `order_id IS NOT NULL` |
# MAGIC | `valid_delivery_status` | Drop bad rows | Only Delivered or Cancelled |
# MAGIC | `valid_city` | Drop bad rows | `city IS NOT NULL` |

# COMMAND ----------

@dlt.table(
    name    = "silver_orders",
    comment = "Cleaned, typed, and enriched food delivery orders. Includes GST, revenue, and delivery flag columns."
)
@dlt.expect_or_drop("valid_order_id",        "order_id IS NOT NULL")
@dlt.expect_or_drop("valid_delivery_status", "delivery_status IN ('Delivered', 'Cancelled')")
@dlt.expect_or_drop("valid_city",            "city IS NOT NULL")
def silver_orders():

    return (
        dlt.read_stream("bronze_orders")

        # --------------------------------------------------
        # STEP A: Build order_timestamp BEFORE casting order_date
        # order_date is still a String here — needed for concat
        # --------------------------------------------------
        .withColumn(
            "order_timestamp",
            to_timestamp(
                concat(col("order_date"), lit(" "), col("order_time")),
                "yyyy-MM-dd HH:mm:ss"
            )
        )

        # --------------------------------------------------
        # STEP B: Type Casting
        # --------------------------------------------------
        .withColumn("order_date",        to_date(col("order_date"), "yyyy-MM-dd"))
        .withColumn("quantity",          col("quantity").cast(IntegerType()))
        .withColumn("item_price",        col("item_price").cast(DoubleType()))
        .withColumn("delivery_fee",      col("delivery_fee").cast(DoubleType()))
        .withColumn("delivery_minutes",  col("delivery_minutes").cast(IntegerType()))
        .withColumn("customer_rating",   col("customer_rating").cast(DoubleType()))

        # --------------------------------------------------
        # STEP C: Null Handling for Cancelled Orders
        # --------------------------------------------------
        .withColumn(
            "delivery_minutes",
            when(col("delivery_minutes").isNull(), 0)
            .otherwise(col("delivery_minutes"))
        )
        .withColumn(
            "customer_rating",
            when(col("customer_rating").isNull(), 0.0)
            .otherwise(col("customer_rating"))
        )
        .withColumn(
            "city",
            when(col("city").isNull(), "Unknown")
            .otherwise(trim(col("city")))
        )

        # --------------------------------------------------
        # STEP D: Business Revenue Columns
        # GST at 18% applies to food subtotal only
        # --------------------------------------------------
        .withColumn("subtotal",     round(col("quantity") * col("item_price"), 2))
        .withColumn("gst_amount",   round(col("quantity") * col("item_price") * 0.18, 2))
        .withColumn("total_amount", round(
            col("quantity") * col("item_price")
            + col("delivery_fee")
            + (col("quantity") * col("item_price") * 0.18),
            2
        ))

        # --------------------------------------------------
        # STEP E: Derived Flag Columns
        # --------------------------------------------------
        .withColumn(
            "is_late_delivery",
            when(col("delivery_status") == "Cancelled", False)
            .when(col("delivery_minutes") > 45, True)
            .otherwise(False)
        )
        .withColumn(
            "is_cancelled",
            when(col("delivery_status") == "Cancelled", True)
            .otherwise(False)
        )
        .withColumn(
            "order_size",
            when(col("subtotal") < 150,  "Small")
            .when(col("subtotal") < 300, "Medium")
            .otherwise("Large")
        )

        # --------------------------------------------------
        # STEP F: Drop Bronze-only columns
        # --------------------------------------------------
        .drop("order_time")
        .drop("layer")

        # --------------------------------------------------
        # STEP G: Silver audit metadata
        # --------------------------------------------------
        .withColumn("silver_processed_at", current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🥇 Step 4: Gold Table 1 — City Revenue Summary
# MAGIC
# MAGIC - Uses `dlt.read("silver_orders")` — **batch read** for aggregations
# MAGIC - Groups by city, computes revenue, order counts, delivery stats
# MAGIC - `cancellation_rate_pct` shows operational health per city

# COMMAND ----------

@dlt.table(
    name    = "gold_city_revenue",
    comment = "City-level revenue summary: total orders, delivered, cancelled, revenue, avg delivery time."
)
def gold_city_revenue():

    return (
        dlt.read("silver_orders")
        .groupBy("city")
        .agg(
            count("order_id").alias("total_orders"),
            count(when(col("is_cancelled") == False, col("order_id"))).alias("delivered_orders"),
            count(when(col("is_cancelled") == True,  col("order_id"))).alias("cancelled_orders"),
            round(sum(when(col("is_cancelled") == False, col("total_amount"))), 2).alias("total_revenue_inr"),
            round(avg(when(col("is_cancelled") == False, col("total_amount"))), 2).alias("avg_order_value_inr"),
            round(avg(when(col("delivery_minutes") > 0,  col("delivery_minutes"))), 1).alias("avg_delivery_minutes"),
            count(when(col("is_late_delivery") == True,  col("order_id"))).alias("late_delivery_count")
        )
        .withColumn(
            "cancellation_rate_pct",
            round((col("cancelled_orders") / col("total_orders")) * 100, 1)
        )
        .withColumn("gold_created_at", current_timestamp())
        .orderBy(col("total_revenue_inr").desc())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🥇 Step 5: Gold Table 2 — Restaurant Performance
# MAGIC
# MAGIC - Groups by restaurant and cuisine type
# MAGIC - Computes revenue, avg rating, delivery speed
# MAGIC - Assigns `performance_tier` label based on avg rating

# COMMAND ----------

@dlt.table(
    name    = "gold_restaurant_performance",
    comment = "Restaurant-level performance: revenue, ratings, delivery speed, and performance tier."
)
def gold_restaurant_performance():

    return (
        dlt.read("silver_orders")
        .groupBy("restaurant_name", "cuisine_type")
        .agg(
            count("order_id").alias("total_orders"),
            count(when(col("is_cancelled") == False, col("order_id"))).alias("delivered_orders"),
            count(when(col("is_cancelled") == True,  col("order_id"))).alias("cancelled_orders"),
            round(sum(when(col("is_cancelled") == False, col("total_amount"))), 2).alias("total_revenue_inr"),
            round(avg(when(col("is_cancelled") == False, col("total_amount"))), 2).alias("avg_order_value_inr"),
            round(avg(when(col("customer_rating") > 0,   col("customer_rating"))), 2).alias("avg_rating"),
            round(avg(when(col("delivery_minutes") > 0,  col("delivery_minutes"))), 1).alias("avg_delivery_minutes"),
            count(when(col("is_late_delivery") == True,  col("order_id"))).alias("late_deliveries")
        )
        .withColumn(
            "performance_tier",
            when(col("avg_rating") >= 4.5, "Top Rated")
            .when(col("avg_rating") >= 4.0, "Good")
            .when(col("avg_rating") >= 3.5, "Average")
            .otherwise("Needs Improvement")
        )
        .withColumn("gold_created_at", current_timestamp())
        .orderBy(col("total_revenue_inr").desc())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🥇 Step 6: Gold Table 3 — Payment Mode Insights
# MAGIC
# MAGIC - Groups by payment mode (UPI, Card, Cash, Wallet)
# MAGIC - Shows total revenue, order count, and revenue share per payment mode

# COMMAND ----------

@dlt.table(
    name    = "gold_payment_insights",
    comment = "Payment mode breakdown: order count, revenue, avg order value per payment method."
)
def gold_payment_insights():

    silver = dlt.read("silver_orders")

    # Compute total delivered revenue for share calculation
    total_revenue = (
        silver
        .filter(col("is_cancelled") == False)
        .agg(sum("total_amount").alias("tr"))
        .collect()[0]["tr"]
    )

    return (
        silver
        .groupBy("payment_mode")
        .agg(
            count("order_id").alias("total_orders"),
            count(when(col("is_cancelled") == False, col("order_id"))).alias("delivered_orders"),
            round(sum(when(col("is_cancelled") == False, col("total_amount"))), 2).alias("total_revenue_inr"),
            round(avg(when(col("is_cancelled") == False, col("total_amount"))), 2).alias("avg_order_value_inr"),
            round(max(when(col("is_cancelled") == False, col("total_amount"))), 2).alias("max_order_inr"),
            round(min(when(col("is_cancelled") == False, col("total_amount"))), 2).alias("min_order_inr")
        )
        .withColumn(
            "revenue_share_pct",
            round((col("total_revenue_inr") / total_revenue) * 100, 1)
        )
        .withColumn("gold_created_at", current_timestamp())
        .orderBy(col("total_revenue_inr").desc())
    )
