from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum, col, avg, coalesce, lit


# Initialize Spark Session
spark = SparkSession.builder.appName("Customer Sales Analysis").getOrCreate()

# Load Data
customers = spark.read.format("csv").option("header", "true").load("/samples/customers.csv")
orders = spark.read.format("csv").option("header", "true").load("/samples/sales.csv")

# Data Cleaning
# Remove records with missing customer_id
customers = customers.dropna(subset=["customer_id"])
orders = orders.dropna(subset=["customer_id"])

# Cast total_amount to numeric
orders = orders.withColumn("total_amount", col("total_amount").cast("double"))

# Join Data
df = customers.join(orders, on="customer_id", how="left")


# 1. Total Order Amount per Customer

df_total = df.groupBy("customer_id", "first_name", "last_name").agg(
    coalesce(sum("total_amount"), lit(0)).alias("total_spent")
)

print("1. Total order amount for each customer")
df_total.show()


# 2. Top 3 Customers by Total Spend

print("2. Top 3 customers by total spend")
df_total.orderBy(col("total_spent").desc()).limit(3).show()


# 3. Customers with No Orders

df_no_orders = df.groupBy("customer_id", "first_name", "last_name").agg(
    count("sale_id").alias("order_count")
).filter(col("order_count") == 0)

print("3. Customers with no orders")
df_no_orders.show()


# 4. City-wise Total Revenue

df_city = df.groupBy("city").agg(
    coalesce(sum("total_amount"), lit(0)).alias("city_revenue")
)

print("4. City-wise total revenue")
df_city.show()


# 5. Average Order Value per Customer

df_avg = df.groupBy("customer_id", "first_name").agg(
    avg(coalesce(col("total_amount"), lit(0))).alias("avg_order_value")
)

print("5. Average order amount per customer")
df_avg.show()


# 6. Customers with More Than One Order

df_multi = orders.groupBy("customer_id").agg(
    count("*").alias("order_count")
).filter(col("order_count") > 1)

print("6. Customers with more than one order")
df_multi.show()


# 7. Sort Customers by Total Spend

print("7. Customers sorted by total spend (desc)")
df_total.orderBy(col("total_spent").desc()).show()
