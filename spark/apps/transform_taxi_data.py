from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# --- Spark Session Initialization ---
# Configured for high-performance S3A connectivity to MinIO Data Lake
spark = SparkSession.builder \
    .appName("NYCTaxiDistributedTransformation") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    .config("spark.hadoop.fs.s3a.path.style.access", True) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# 1. DATA INGESTION (From MinIO Data Lake)
print("📂 Loading raw Parquet data from MinIO Object Storage...")
df = spark.read.parquet("s3a://nyc-taxi/yellow_tripdata_2024-01.parquet")

# 2. DATA TRANSFORMATION (Cleaning & Feature Engineering)
# Filtering anomalous data: Ensuring positive trip distance and passenger counts
print("🧹 Performing data cleansing and filtering...")
df_cleaned = df.filter((df.trip_distance > 0) & (df.passenger_count > 0))

# Time-series Feature Engineering: Extracting pickup month for temporal analysis
df_with_month = df_cleaned.withColumn("pickup_month", F.date_format("tpep_pickup_datetime", "yyyy-MM"))

# Aggregating metrics for Business Intelligence (BI) consumption
print("📊 Executing analytical aggregations (Grouped by Vendor & Period)...")
summary_df = df_with_month.groupBy("VendorID", "pickup_month").agg(
    F.count("VendorID").alias("total_trips"),
    F.avg("total_amount").alias("avg_fare"),
    F.avg("trip_distance").alias("avg_distance"),
    F.sum("total_amount").alias("total_revenue")
)

# 3. CONSOLE VALIDATION
# Displaying a preview of the processed summary table
summary_df.show()

# 4. DATA LOADING (To PostgreSQL Warehouse)
# Persisting the aggregated results into the relational Data Warehouse via JDBC
print("💾 Writing processed insights to PostgreSQL Data Warehouse...")
summary_df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/nyc_warehouse") \
    .option("dbtable", "monthly_taxi_summary") \
    .option("user", "caelan") \
    .option("password", "password123") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

print("✅ Pipeline execution successful: Data Warehouse updated.")