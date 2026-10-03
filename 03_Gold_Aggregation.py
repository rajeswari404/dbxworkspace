# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC # 🥇 Gold Layer — Business Aggregations
# MAGIC ## 🍕 FoodRush India | Food Delivery Orders Pipeline
# MAGIC ---
# MAGIC ### What this notebook does:
# MAGIC | Step | Action |
# MAGIC |------|--------|
# MAGIC | 1 | Read from Silver Delta table (batch read) |
# MAGIC | 2 | Build **Gold Table 1** — City Revenue Summary |
# MAGIC | 3 | Build **Gold Table 2** — Restaurant Performance |
# MAGIC | 4 | Build **Gold Table 3** — Payment Mode Insights |
# MAGIC | 5 | Write all 3 Gold tables as Delta tables |
# MAGIC | 6 | Display final business dashboards |
# MAGIC
# MAGIC ### Gold Tables Created:
# MAGIC | Table | Purpose |
# MAGIC |-------|---------|
# MAGIC | `gold_city_revenue` | Revenue, order count, avg delivery time per city |
# MAGIC | `gold_restaurant_performance` | Revenue, ratings, cancellations per restaurant |
# MAGIC | `gold_payment_insights` | Which payment mode drives the most orders & revenue |
# MAGIC
# MAGIC > 🔑 **Gold Rule:** Aggregated, business-ready tables. These feed dashboards and BI reports.
# MAGIC > Gold uses **batch read** from Silver — aggregations do not need streaming.
# MAGIC
# MAGIC > ⚠️ **Run Bronze → Silver notebooks first** before executing this notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📁 Step 1: Define All Paths

# COMMAND ----------

# Silver source path
silver_delta_path = "/Volumes/streaming/schema1/streaming-storage/output_delta/delta/silver/orders/"

# Gold destination paths — one Delta table per aggregation
gold_city_path        = "/Volumes/streaming/schema1/streaming-storage/output_delta/delta/gold/city_revenue/"
gold_restaurant_path  = "/Volumes/streaming/schema1/streaming-storage/output_delta/delta/gold/restaurant_performance/"
gold_payment_path     = "/Volumes/streaming/schema1/streaming-storage/output_delta/delta/gold/payment_insights/"

print("=" * 60)
print("        FoodRush India — Gold Layer Paths")
print("=" * 60)
print(f"  🥈 Silver Source          : {silver_delta_path}")
print(f"  🥇 Gold — City Revenue    : {gold_city_path}")
print(f"  🥇 Gold — Restaurant Perf : {gold_restaurant_path}")
print(f"  🥇 Gold — Payment Insight : {gold_payment_path}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔄 Step 2: Reset Gold Paths (Run ONLY when starting fresh)

# COMMAND ----------

# Uncomment ALL lines below ONLY to reset Gold tables

dbutils.fs.rm(gold_city_path,       recurse=True)
dbutils.fs.rm(gold_restaurant_path, recurse=True)
dbutils.fs.rm(gold_payment_path,    recurse=True)
print("🧹 All Gold paths cleared — ready for fresh run")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📖 Step 3: Read from Silver Delta (Batch)
# MAGIC
# MAGIC > 💡 Gold uses **batch read** (`spark.read`) instead of streaming.
# MAGIC > Aggregations like `sum`, `avg`, `count` work naturally in batch mode.

# COMMAND ----------

from pyspark.sql.functions import (
    col, count, sum, avg, round,
    when, current_timestamp, max, min
)

# Batch read from Silver Delta
silver_df = spark.read.format("delta").load(silver_delta_path)

print(f"✅ Silver Delta loaded — {silver_df.count()} records")
print(f"📋 Available columns: {silver_df.columns}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏙️ Step 4: Gold Table 1 — City Revenue Summary
# MAGIC
# MAGIC **Business Question:** Which cities are generating the most orders and revenue?
# MAGIC
# MAGIC > Only **Delivered** orders are counted in revenue.
# MAGIC > Cancelled orders are reported separately for operational insight.

# COMMAND ----------

# Filter for delivered orders only (for revenue metrics)
delivered_df = silver_df.filter(col("delivery_status") == "Delivered")

# Build City Revenue aggregation
gold_city_df = (
    silver_df
    .groupBy("city")
    .agg(
        count("order_id").alias("total_orders"),
        count(when(col("is_cancelled") == False, col("order_id"))).alias("delivered_orders"),
        count(when(col("is_cancelled") == True,  col("order_id"))).alias("cancelled_orders"),
        round(sum(when(col("is_cancelled") == False, col("total_amount"))), 2).alias("total_revenue_inr"),
        round(avg(when(col("is_cancelled") == False, col("total_amount"))), 2).alias("avg_order_value_inr"),
        round(avg(when(col("delivery_minutes") > 0,  col("delivery_minutes"))), 1).alias("avg_delivery_minutes"),
        count(when(col("is_late_delivery") == True, col("order_id"))).alias("late_delivery_count")
    )
    .withColumn(
        "cancellation_rate_pct",
        round((col("cancelled_orders") / col("total_orders")) * 100, 1)
    )
    .withColumn("gold_created_at", current_timestamp())
    .orderBy(col("total_revenue_inr").desc())
)

print("✅ Gold Table 1 built — City Revenue Summary")
print(f"   Cities covered: {gold_city_df.count()}")
display(gold_city_df)

# COMMAND ----------

# Write Gold Table 1 to Delta
(
    gold_city_df
    .write
    .format("parquet")
    .mode("overwrite")
    .save(gold_city_path)
)

print("✅ Gold Table 1 written  →  gold_city_revenue")
print(f"   📍 {gold_city_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🍽️ Step 5: Gold Table 2 — Restaurant Performance
# MAGIC
# MAGIC **Business Question:** Which restaurants are performing best in terms of revenue, ratings, and delivery speed?

# COMMAND ----------

# Build Restaurant Performance aggregation
gold_restaurant_df = (
    silver_df
    .groupBy("restaurant_name", "cuisine_type")
    .agg(
        count("order_id").alias("total_orders"),
        count(when(col("is_cancelled") == False, col("order_id"))).alias("delivered_orders"),
        count(when(col("is_cancelled") == True,  col("order_id"))).alias("cancelled_orders"),
        round(sum(when(col("is_cancelled") == False, col("total_amount"))), 2).alias("total_revenue_inr"),
        round(avg(when(col("is_cancelled") == False, col("total_amount"))), 2).alias("avg_order_value_inr"),
        # avg rating — only for delivered orders with a rating (rating > 0)
        round(avg(when(col("customer_rating") > 0, col("customer_rating"))), 2).alias("avg_rating"),
        # avg delivery time — only for delivered orders
        round(avg(when(col("delivery_minutes") > 0, col("delivery_minutes"))), 1).alias("avg_delivery_minutes"),
        count(when(col("is_late_delivery") == True, col("order_id"))).alias("late_deliveries")
    )
    .withColumn(
        "performance_tier",
        when(col("avg_rating") >= 4.5, "⭐ Top Rated")
        .when(col("avg_rating") >= 4.0, "✅ Good")
        .when(col("avg_rating") >= 3.5, "⚠️ Average")
        .otherwise("❌ Needs Improvement")
    )
    .withColumn("gold_created_at", current_timestamp())
    .orderBy(col("total_revenue_inr").desc())
)

print("✅ Gold Table 2 built — Restaurant Performance")
print(f"   Restaurants covered: {gold_restaurant_df.count()}")
display(gold_restaurant_df)

# COMMAND ----------

# Write Gold Table 2 to Delta
(
    gold_restaurant_df
    .write
    .format("parquet")
    .mode("overwrite")
    .save(gold_restaurant_path)
)

print("✅ Gold Table 2 written  →  gold_restaurant_performance")
print(f"   📍 {gold_restaurant_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💳 Step 6: Gold Table 3 — Payment Mode Insights
# MAGIC
# MAGIC **Business Question:** Which payment methods are customers preferring, and what revenue do they contribute?

# COMMAND ----------

# Total revenue across all delivered orders (used for percentage calculation)
total_revenue = silver_df.filter(col("is_cancelled") == False).agg(sum("total_amount")).collect()[0][0]

# Build Payment Insights aggregation
gold_payment_df = (
    silver_df
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

print("✅ Gold Table 3 built — Payment Mode Insights")
print(f"   Payment modes covered: {gold_payment_df.count()}")
display(gold_payment_df)

# COMMAND ----------

# Write Gold Table 3 to Delta
(
    gold_payment_df
    .write
    .format("parquet")
    .mode("overwrite")
    .save(gold_payment_path)
)

print("✅ Gold Table 3 written  →  gold_payment_insights")
print(f"   📍 {gold_payment_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Step 7: Verify All 3 Gold Tables

# COMMAND ----------

print("=" * 55)
print("        🥇 GOLD LAYER — VERIFICATION")
print("=" * 55)

gold_city_check       = spark.read.format("delta").load(gold_city_path)
gold_restaurant_check = spark.read.format("delta").load(gold_restaurant_path)
gold_payment_check    = spark.read.format("delta").load(gold_payment_path)

print(f"  🏙️  gold_city_revenue          : {gold_city_check.count()} rows, {len(gold_city_check.columns)} cols")
print(f"  🍽️  gold_restaurant_performance : {gold_restaurant_check.count()} rows, {len(gold_restaurant_check.columns)} cols")
print(f"  💳  gold_payment_insights       : {gold_payment_check.count()} rows, {len(gold_payment_check.columns)} cols")
print("=" * 55)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏆 Step 8: Final Dashboard — Top Insights

# COMMAND ----------

print("━" * 55)
print("   🏙️  TOP CITIES BY REVENUE")
print("━" * 55)
display(
    spark.read.format("delta").load(gold_city_path)
    .select("city", "total_orders", "delivered_orders", "total_revenue_inr", "avg_delivery_minutes")
    .orderBy(col("total_revenue_inr").desc())
)

# COMMAND ----------

print("━" * 55)
print("   🍽️  TOP RESTAURANTS BY REVENUE")
print("━" * 55)
display(
    spark.read.format("delta").load(gold_restaurant_path)
    .select("restaurant_name", "cuisine_type", "total_orders", "total_revenue_inr", "avg_rating", "performance_tier")
    .orderBy(col("total_revenue_inr").desc())
)

# COMMAND ----------

print("━" * 55)
print("   💳  PAYMENT MODE BREAKDOWN")
print("━" * 55)
display(
    spark.read.format("delta").load(gold_payment_path)
    .select("payment_mode", "total_orders", "total_revenue_inr", "avg_order_value_inr", "revenue_share_pct")
    .orderBy(col("total_revenue_inr").desc())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Step 9: Full Medallion Architecture Summary

# COMMAND ----------

print("=" * 60)
print("    🏅 FOODRUSH INDIA — MEDALLION PIPELINE COMPLETE")
print("=" * 60)
print()
print("  Layer   │ Table / Path                   │ Records")
print("  ────────┼────────────────────────────────┼────────")

bronze_count     = spark.read.format("delta").load("dbfs:/FileStore/foodrush/delta/bronze/orders/").count()
silver_count     = spark.read.format("delta").load("dbfs:/FileStore/foodrush/delta/silver/orders/").count()
city_count       = spark.read.format("delta").load(gold_city_path).count()
restaurant_count = spark.read.format("delta").load(gold_restaurant_path).count()
payment_count    = spark.read.format("delta").load(gold_payment_path).count()

print(f"  🥉 Bronze │ bronze/orders                   │ {bronze_count} rows")
print(f"  🥈 Silver │ silver/orders                   │ {silver_count} rows")
print(f"  🥇 Gold   │ gold/city_revenue               │ {city_count} rows")
print(f"  🥇 Gold   │ gold/restaurant_performance     │ {restaurant_count} rows")
print(f"  🥇 Gold   │ gold/payment_insights           │ {payment_count} rows")
print()
print("  ✅ All 3 layers complete!")
print("=" * 60)