# Phase 5 – Databricks + Olist Pipeline

## 🔹 Objective

In this phase, I worked with a real-world dataset in Databricks and built an end-to-end pipeline using PySpark.

---

## 🔹 Problem Overview

Worked with multiple datasets:
- orders  
- customers  
- order_items  
- payments  
- products  

Tasks:
- Combine multiple tables  
- Perform aggregations and analysis  
- Apply window functions  
- Generate final reporting dataset  

---

## 🔹 Approach

- Set up Databricks environment (cluster + notebook)  
- Loaded CSV files into DataFrames  
- Validated data using schema checks  
- Created `fact_orders` using joins  
- Applied transformations and aggregations  
- Used window functions for analysis  
- Built final reporting dataset  

---

## 🔹 Key Operations Used

`join()` • `groupBy()` • `agg()` • `withColumn()` • `when()` • `Window()` • `dense_rank()` • `sum()` • `count()`

---

## 🔹 Tasks Performed

- Top 3 customers per city  
- Running total of daily sales  
- Top products per category  
- Customer Lifetime Value (CLV)  
- Customer segmentation (Gold, Silver, Bronze)  
- Final reporting dataset  

---

## 🔹 Output / Insights

- Customer spending and segmentation  
- Product-level performance  
- Daily and cumulative sales trends  
- Final combined dataset for analysis  

---

## 🔹 Challenges

- Understanding relationships between multiple tables  
- Handling missing columns  
- Managing Databricks file paths  
- Writing window functions correctly  

---

## 🔹 Learnings

- Worked with real-world multi-table data  
- Understood importance of fact table (`fact_orders`)  
- Learned window functions for advanced analytics  
- Built complete end-to-end pipeline  
- Applied business logic on data  

---

## 🔹 Conclusion

This phase helped me build a complete data pipeline in Databricks and generate business insights from real-world data.
