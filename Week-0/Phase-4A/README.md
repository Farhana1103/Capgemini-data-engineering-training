# Phase 4A – Segmentation using PySpark

## 🔹 Objective

In this phase, I learned how to convert continuous data into meaningful customer segments using different techniques in PySpark.

---

## 🔹 Problem Overview

Worked with customer spending data (`total_spend`) and categorized customers into:

- Gold  
- Silver  
- Bronze  

Challenges:
- Continuous data is hard to interpret  
- Fixed rules may not fit all data  
- Needed to compare multiple segmentation methods  

---

## 🔹 Approach

- Loaded and cleaned datasets  
- Calculated `total_spend` using `groupBy()`  
- Joined with customer data  
- Created `customer_name` column  
- Applied different segmentation techniques  
- Compared results across methods  

---

## 🔹 Segmentation Methods Used

- Conditional logic (`when()`)  
- Quantile-based (`approxQuantile()`)  
- Bucketizer (`Bucketizer`)  
- Window-based (`percent_rank()`)  

---

## 🔹 Key Operations Used

`groupBy()` • `agg()` • `join()` • `withColumn()` • `when()` • `approxQuantile()` • `Bucketizer()` • `Window()` • `percent_rank()`

---

## 🔹 Output / Insights

- Created customer segments using multiple methods  
- Compared distribution of customers across segments  
- Understood difference between rule-based and data-driven approaches  

---

## 🔹 Challenges

- Handling continuous data  
- Choosing correct thresholds  
- Understanding ranking logic  
- Comparing multiple segmentation results  

---

## 🔹 Learnings

- Segmentation makes data easier to analyze  
- Different methods give different results  
- Fixed rules are simple but not always accurate  
- Quantile and ranking methods adapt better to data  
- Combination of methods works best in real-world  

---

## 🔹 Conclusion

This phase helped me understand how to segment customers using PySpark and compare different approaches to generate better insights.
