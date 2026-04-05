from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Phase4_Business_Pipeline") \
    .getOrCreate()

# Load Data
customers = spark.read.csv("/samples/customers.csv", header=True, inferSchema=True)
sales = spark.read.csv("/samples/sales.csv", header=True, inferSchema=True)

# Data Cleaning
customers = customers.dropna(subset=["customer_id"]) \
                     .dropDuplicates(["customer_id"])

sales = sales.dropna(subset=["customer_id"]) \
             .dropDuplicates(["sale_id"]) \
             .filter((F.col("total_amount") > 0) & (F.col("quantity") > 0))

# Create Customer Name
customers = customers.withColumn(
    "customer_name",
    F.concat_ws(" ", F.col("first_name"), F.col("last_name"))
)

# Join Data
df = sales.join(customers, "customer_id")

# Task 1: Daily Sales
daily_sales = df.groupBy("sale_date") \
    .agg(
        F.sum("total_amount").alias("total_sales"),
        F.avg("total_amount").alias("avg_order_value")
    )

# Task 2: City-wise Revenue
city_revenue = df.groupBy("city") \
    .agg(
        F.sum("total_amount").alias("total_revenue"),
        F.count("sale_id").alias("total_orders")
    )

# Task 3: Customer Summary
customer_summary = df.groupBy("customer_id", "customer_name", "city") \
    .agg(
        F.sum("total_amount").alias("total_spend"),
        F.count("sale_id").alias("order_count")
    )

# Task 4: Top Customers
top_customers = customer_summary.orderBy(F.desc("total_spend")).limit(5)

# Task 5: Customer Segmentation
segmentation = customer_summary.withColumn(
    "segment",
    F.when(F.col("total_spend") > 10000, "Gold")
     .when(F.col("total_spend") >= 5000, "Silver")
     .otherwise("Bronze")
)

# Task 6: Customer Type
final_df = segmentation.withColumn(
    "customer_type",
    F.when(F.col("order_count") > 5, "Frequent")
     .when(F.col("order_count") > 1, "Regular")
     .otherwise("Occasional")
)

# Task 7: Save Output
final_df.write.mode("overwrite").parquet("/tmp/final_report")

# Display Outputs
daily_sales.show()
city_revenue.show()
top_customers.show()
final_df.show()
