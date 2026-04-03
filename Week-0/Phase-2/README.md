# Phase 2 – Data Transformation with PySpark

## 🔹 Objective

In this phase, I applied SQL concepts in PySpark to work with multiple datasets, perform transformations, and generate useful insights.

---

## 🔹 Problem Overview

Worked with:
* `customers`
* `sales / orders`

Tasks included:
* Joining datasets using `customer_id`
* Performing aggregations like total spend and average order value
* Identifying top customers and customers with no orders
* Analyzing data at both customer and city level

---

## 🔹 Approach

* Loaded datasets into DataFrames  
* Cleaned data by handling null values (`dropna()`, `coalesce()`)  
* Joined datasets using `customer_id`  
* Applied transformations using `groupBy()`, `agg()`, `filter()`, and `orderBy()`  
* Generated outputs for each required query  

---

## 🔹 Key Transformations

* `join()` for combining datasets  
* `groupBy()` and `agg()` for calculations  
* `filter()` for conditions  
* `orderBy()` for sorting  
* `coalesce()` → Replace null values with defaults
* `dropna()` → Remove rows with null values
* `left_anti join` → Identify customers with no orders

---

## 🔹 Data Engineering Practices

* Used **LEFT JOIN** to include customers with no orders  
* Handled null values to avoid incorrect results  
* Ensured proper grouping to maintain accuracy  
* Kept code clean and readable  

---

## 🔹 Challenges

* Understanding how `groupBy()` works in PySpark  
* Handling null values after joins  
* Fixing column-related errors  
* Writing clean multi-line queries  

---

## 🔹 Learnings

* Better understanding of SQL to PySpark mapping  
* Importance of data cleaning before transformations  
* Confidence in using joins and aggregations  
* Improved ability to write clean and structured PySpark code  

---
