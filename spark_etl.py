from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, to_timestamp

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Ecommerce_ETL_Pipeline") \
    .getOrCreate()

# --------------------------
# Extract (From S3)
# --------------------------
input_path = "s3://your-bucket/raw-data/orders.csv"

orders_df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(input_path)

# --------------------------
# Transform
# --------------------------

# Remove null values
orders_cleaned = orders_df.dropna()

# Convert timestamp column
orders_transformed = orders_cleaned.withColumn(
    "order_timestamp",
    to_timestamp(col("order_date"), "yyyy-MM-dd HH:mm:ss")
)

# Aggregate revenue per customer
customer_revenue = orders_transformed.groupBy("customer_id") \
    .agg(_sum("amount").alias("total_revenue"))

# --------------------------
# Load (Back to S3 for Redshift COPY)
# --------------------------
output_path = "s3://your-bucket/processed-data/customer_revenue/"

customer_revenue.write \
    .mode("overwrite") \
    .parquet(output_path)

print("ETL Pipeline Completed Successfully")

spark.stop()
