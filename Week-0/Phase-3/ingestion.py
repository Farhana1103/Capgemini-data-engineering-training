# Read CSV file from /samples/
df = (
    spark.read.format("csv")
    .option("header", "true")
    .load("/samples/customers.csv")
)

# Inspect Data
df.show()
df.printSchema()

# Identify Missing Values
from pyspark.sql.functions import col

# Check nulls in a specific column
df.filter(col("column").isNull()).show()

# Check nulls in multiple columns
df.filter(col("age").isNull() | col("name").isNull()).show()


# Handle Missing Data using dropna()

# Remove rows with any null values
df_clean = df.dropna()

# Remove rows only if all values are null
df_clean = df.dropna(how="all")

# Remove rows based on specific columns
df_clean = df.dropna(subset=["age", "name"])

# Replace NULL Values
# -----------------------------------------

# Fill all null values with 0
df_filled = df.fillna(0)

# Fill null values in specific column
df_filled = df.fillna({"age": 0})

# Read JSON and Parquet Files
# -----------------------------------------

# Read JSON file
df_json = (
    spark.read
    .option("multiline", "true")
    .json("/samples/file.json")
)
df_json.show()

# Read Parquet file
df_parquet = spark.read.parquet("/samples/file.parquet")
df_parquet.show()
df_parquet.printSchema()
