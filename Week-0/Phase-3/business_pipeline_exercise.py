#from pyspark.sql import SparkSession
from pyspark.sql.functions import sum,col
spark = SparkSession.builder.appName("Phase2").getOrCreate()
------------------------------------------------------------
First Pipeline : Read sales data -> clean nulls -> calculate daily sales
#Read sales,customers data
orders = spark.read.format('csv').option('header', 'true').load('/samples/sales.csv')
customers = spark.read.format('csv').option('header', 'true').load('/samples/customers.csv')
# Clean nulls
orders_clean = orders.dropna(subset=["customer_id", "quantity", "total_amount", "sale_date"]) \
    .filter((col("quantity") > 0) & (col("total_amount") > 0))
customers_clean = customers.dropna(subset=["customer_id"])
# Join with customer details
joined = orders_clean.join(customers_clean, "customer_id", "left")
# Daily sales with customer details
daily_sales = joined.groupBy("sale_date", "customer_id", "first_name", "city") \
    .agg(sum("total_amount").alias("daily_sales"))
daily_sales.show()
--------------------------------------------------------------------------------------------------
