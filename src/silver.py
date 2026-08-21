import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

catalog = "workspace"  # Default catalog for Databricks SQL
schema = sys.argv[1] if len(sys.argv) > 1 else "dev"

source_table = f"{catalog}.{schema}.bronze_taxi_trips"
target_table = f"{catalog}.{schema}.silver_taxi_trips"

print(f"Reading bronze data from {source_table}...")
df_bronze = spark.table(source_table)

print("Applying data quality rules: enforcing positive trip distance and fare amounts...")
# Drop records with logical errors (negative distances or fares)
df_silver = df_bronze.filter((col("trip_distance") > 0) & (col("fare_amount") >= 0))

print(f"Writing cleaned data to Silver table: {target_table}...")
df_silver.write.format("delta").mode("overwrite").saveAsTable(target_table)

print("Silver transformation completed successfully.")