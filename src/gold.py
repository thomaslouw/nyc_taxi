import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, sum as _sum, count as _count, desc

spark = SparkSession.builder.getOrCreate()

catalog = "workspace"  # Default catalog for Databricks SQL
schema = sys.argv[1] if len(sys.argv) > 1 else "dev"
source_table = f"{catalog}.{schema}.silver_taxi_trips"

print(f"Reading cleaned silver data from {source_table}...")
df_silver = spark.table(source_table)

target_revenue_table = f"{catalog}.{schema}.gold_daily_revenue"
print(f"Calculating daily revenue metrics and writing to {target_revenue_table}...")

# Cast datetime to date, group by date, and aggregate sum of fares and count of trips
df_daily_revenue = df_silver.withColumn("pickup_date", to_date(col("tpep_pickup_datetime"))) \
    .groupBy("pickup_date") \
    .agg(
        _sum("fare_amount").alias("total_daily_revenue"),
        _count("*").alias("total_trips")
    ) \
    .orderBy(desc("pickup_date"))

df_daily_revenue.write.format("delta").mode("overwrite").saveAsTable(target_revenue_table)

target_zones_table = f"{catalog}.{schema}.gold_top_zones"
print(f"Calculating busiest pickup zones and writing to {target_zones_table}...")

# Group by zip code to find the most popular pickup locations
df_top_zones = df_silver.groupBy("pickup_zip") \
    .agg(_count("*").alias("trip_count")) \
    .orderBy(desc("trip_count"))

df_top_zones.write.format("delta").mode("overwrite").saveAsTable(target_zones_table)

print("Gold aggregations completed successfully.")