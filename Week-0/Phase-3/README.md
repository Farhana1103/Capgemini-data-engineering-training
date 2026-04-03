# 🚀 Phase 3 – ETL Pipeline using PySpark

---

## 🔹 Objective

In this phase, I built an ETL pipeline using PySpark to clean data, apply transformations, and generate meaningful insights.

---

## 🔹 Problem Overview

Worked with:
* `customers.csv`
* `sales.csv`

Issues in data:
- Missing values  
- Invalid records  
- Incorrect data types  

---

## 🔹 Approach

- Loaded data into DataFrames  
- Cleaned data by removing null and invalid values  
- Converted columns to correct data types  
- Joined datasets using `customer_id`  
- Applied transformations using aggregations and filtering  
- Generated outputs for analysis  

---

## 🔹 Key Operations Used

| Category            | Operations                      |
|--------------------|-------------------------------|
| Data Ingestion      | `read()`                      |
| Data Cleaning       | `dropna()`, `filter()`        |
| Data Transformation | `withColumn()`, `cast()`      |
| Data Integration    | `join()`                      |
| Aggregation         | `groupBy()`, `sum()`, `count()` |
| Advanced Analytics  | `Window`, `row_number()`      |
| Output              | `write().parquet()`           |

## 🔹 Output / Insights

- Customer spending analysis  
- City-wise revenue  
- Repeat customers  
- Top customers per city  
- Daily sales trends  

---

## 🔹 Best Practices Followed

- Cleaned data before applying logic  
- Maintained proper transformation order  
- Ensured correct schema and data types  
- Used efficient storage format (Parquet)  

---

## 🔹 Challenges

| Challenge               | Solution                          |
|------------------------|----------------------------------|
| Missing / null values  | Used `dropna()`                  |
| Incorrect data types   | Applied `cast()`                 |
| Duplicate / invalid data | Used `filter()`                |
| Ranking logic          | Used `row_number()` (window function) |

---

## 🔹 Learnings

- Real-world data is always messy  
- Cleaning is essential before analysis  
- ETL is a step-by-step process  
- PySpark helps handle large-scale data efficiently  
- Proper pipeline design makes code easier to manage  

---

## 🔹 Conclusion

This phase helped me understand how to build a structured ETL pipeline using PySpark and prepare data for real-world analysis.
