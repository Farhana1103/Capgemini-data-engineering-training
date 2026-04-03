from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# 1. Initialize Spark Session

spark = SparkSession.builder \
    .appName("Phase3A_DataCleaning") \
    .getOrCreate()
# 2. Create DataFrame
data = [
    (1, "Ravi", "Hyderabad", 25),
    (2, None, "Chennai", 32),
    (None, "Arun", "Hyderabad", 28),
    (4, "Meena", None, 30),
    (4, "Meena", None, 30),
    (5, "John", "Bangalore", -5)
]

columns = ["customer_id", "name", "city", "age"]

df = spark.createDataFrame(data, columns)

print("RAW DATA")
df.show()

print("Schema")
df.printSchema()

print("Row Count Before Cleaning:", df.count())


# 2. TRANSFORM (Data Cleaning Pipeline)

# Step 1: Remove records with missing primary key
df_clean = df.dropna(subset=["customer_id"])

# Step 2: Remove duplicate records
df_clean = df_clean.dropDuplicates()

# Step 3: Handle missing values (business defaults)
df_clean = df_clean.fillna({
    "name": "Unknown",
    "city": "Unknown"
})

# Step 4: Remove invalid values (age must be > 0)
df_clean = df_clean.filter(col("age") > 0)


# 3. VALIDATION (Check Results)
print("CLEANED DATA")
df_clean.show()

print("Row Count After Cleaning:", df_clean.count())

print("Customers per City")
df_clean.groupBy("city").count().show()


# 4. LOAD (End of Pipeline)
spark.stop()
