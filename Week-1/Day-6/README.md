# PySpark – Analytical Transformations & Output

## 🔹 Objective

In this phase, I performed analytical transformations on cleaned data and generated business insights, and saved the results for further use.

---

## 🔹 Problem Overview

Tasks included:
- Identifying top customers  
- Finding repeat customers  
- Analyzing monthly sales trends  
- Saving processed data for reporting  

---

## 🔹 Approach

- Used cleaned DataFrames from previous phases  
- Joined datasets (customers, cars, sales)  
- Created revenue column using transformations  
- Applied aggregations and window functions  
- Generated analytical outputs  
- Saved results in required formats  

---

## 🔹 Key Operations Used

`join()` • `withColumn()` • `groupBy()` • `agg()` • `row_number()` • `filter()` • `date_format()`

---

## 🔹 Analysis Performed

- Top 3 customers per city using ranking  
- Repeat customers using count  
- Monthly sales trend using date functions  

---

## 🔹 Output / Insights

- Top customers per city  
- Repeat customer list  
- Monthly revenue trends  

Formats used:
- Parquet (optimized)  
- CSV (readable)  

---

## 🔹 Challenges Faced

- Implementing window functions correctly  
- Handling joins across multiple datasets  
- Ensuring correct revenue calculation  
- Managing date formats  

---

## 🔹 Learnings

- Performing analytical queries using PySpark  
- Importance of window functions  
- Difference between aggregation and window functions  
- Generating business insights from data  
- Best practices for saving outputs  

---

## 🔹 Conclusion

This phase helped me understand how to perform analysis on processed data and generate insights using PySpark.
