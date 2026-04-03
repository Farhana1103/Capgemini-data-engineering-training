# Phase 3A – Data Quality & Cleaning using PySpark

## 🔹 Objective

In this phase, I worked with messy data and focused on cleaning it using PySpark while following a proper ETL approach.

---

## 🔹 Problem Overview

Dataset contained:
- Missing values (name, city, customer_id)
- Duplicate records
- Invalid data (negative age)
- Null primary keys

---

## 🔹 Approach

- Loaded data into a DataFrame  
- Checked schema and row count before cleaning  
- Applied step-by-step cleaning:
  - Removed null `customer_id`
  - Filled missing values using `fillna()`
  - Removed duplicates
  - Filtered invalid age values (`age > 0`)  
- Validated results by comparing before/after counts  
- Performed aggregation (customers per city)

---

## 🔹 Key Operations Used

`createDataFrame()` • `dropna()` • `fillna()` • `dropDuplicates()` • `filter()` • `groupBy()` • `count()` • `printSchema()`

---

## 🔹 Challenges

- Handling null values after joins  
- Understanding correct order of cleaning steps  
- Identifying invalid vs acceptable data  
- Writing clean and readable transformations  

---

## 🔹 Learnings

- Real-world data is always messy  
- Data cleaning is essential before analysis  
- Order of operations affects results  
- Validation is important to ensure correctness  
- Thinking in ETL steps improves clarity  

---

## 🔹 Conclusion

This phase helped me understand how to clean and prepare raw data using PySpark and build a simple ETL pipeline that produces reliable results.
