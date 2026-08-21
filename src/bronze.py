import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

catalog = "workspace"  # Default catalog for Databricks SQL
# Accept schema as a command-line argument (passed via DAB task parameters), defaulting to 'dev'
schema = sys.argv[1] if len(sys.argv) > 1 else "dev"

# Ensure the target schema exists
print(f"Ensuring schema {catalog}.{schema} exists...")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

source_table = "samples.nyctaxi.trips"
target_table = f"{catalog}.{schema}.bronze_taxi_trips"

print(f"Reading raw data from {source_table}...")
df_raw = spark.table(source_table)

# Add auditing metadata: Ingestion timestamp
df_bronze = df_raw.withColumn("_ingest_ts", current_timestamp())

print(f"Writing data to Bronze table: {target_table}...")
# Using overwrite mode for idempotency during development/testing
df_bronze.write.format("delta").mode("overwrite").saveAsTable(target_table)

print("Bronze ingestion completed successfully.")