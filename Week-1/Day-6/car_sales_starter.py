
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()

customers = spark.createDataFrame([
    (1,"John ","Hyderabad"),
    (2,"Alice","Chennai"),
    (3,None,"Bangalore")
],["customer_id","name","city"])

cars = spark.createDataFrame([
    (101,"Toyota","Camry",30000),
    (102,"Honda","Civic",-20000),
    (103,"Hyundai","i20",15000)
],["car_id","brand","model","price"])

sales = spark.createDataFrame([
    (1,1,101,"2024-01-01",1),
    (2,2,102,"2024-01-02",2),
    (3,99,103,"2024-01-03",1)
],["sale_id","customer_id","car_id","sale_date","quantity"])

sales = sales.withColumn("sale_date", to_date(col("sale_date")))

df = sales.join(customers,"customer_id").join(cars,"car_id")
df.groupBy("customer_id").sum("price").show()
