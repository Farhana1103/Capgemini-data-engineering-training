# Phase 4 – Business Pipeline using PySpark

## 🔹 Objective

In this phase, I built an end-to-end ETL pipeline using PySpark to clean data, perform transformations, and generate business insights.

---

## 🔹 Problem Overview

Worked with:
- `customers` dataset  
- `sales` dataset  

Issues faced:
- Missing values (`customer_id`)
- Duplicate records  
- Invalid data (negative sales)  
- Need for business-level insights  

---

## 🔹 Approach

- Loaded data from CSV files  
- Cleaned data using `dropna()`, `dropDuplicates()`, and filtering  
- Created `customer_name` column  
- Joined datasets using `customer_id`  
- Applied aggregations and business logic  
- Generated final reporting dataset  

---

## 🔹 Tasks Performed

1. Daily sales calculation  
2. City-wise revenue  
3. Top 5 customers by spend  
4. Repeat customers identification  
5. Customer segmentation (Gold, Silver, Bronze)  
6. Final reporting dataset creation  

---

## 🔹 Key Operations Used

`read.csv()` • `dropna()` • `dropDuplicates()` • `filter()` • `withColumn()` • `groupBy()` • `agg()` • `join()` • `orderBy()` • `limit()` • `when()` • `coalesce()` • `write()`

---

## 🔹 Best Practices Followed

- Cleaned data before transformations  
- Removed invalid and duplicate records  
- Used proper joins for accuracy  
- Applied business logic on aggregated data  
- Saved output efficiently  

---

## 🔹 Challenges

- Handling missing and invalid data  
- Managing duplicates  
- Applying correct joins  
- Generating single output file  

---

## 🔹 Learnings

- End-to-end pipelines are important in real-world projects  
- Data cleaning directly affects results  
- Aggregations and joins are key skills  
- Business logic adds real value  
- Proper pipeline structure improves clarity  

---

## 🔹 Conclusion

This phase helped me understand how to build a complete ETL pipeline using PySpark and convert raw data into meaningful business insights.
