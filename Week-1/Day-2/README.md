# SQL Practice: Joins & Group By (Databricks)

This repository contains my hands-on practice of SQL concepts using Databricks.  
It includes solutions for **JOIN operations** and **GROUP BY queries** based on structured datasets.

---

## What I Did

- Executed SQL queries in **Databricks**
- Practiced real-world scenarios using:
  - Employee data
  - Department data
  - Sales data
- Solved **60 SQL questions**:
  - 30 JOIN-based queries
  - 30 GROUP BY queries

---

## Topics Covered

### 🔗 Joins
- Inner Join
- Left Join
- Right Join
- Full Outer Join
- Self Join
- Joins with conditions
- Handling NULL values

### 📊 Group By
- Aggregate functions:
  - SUM()
  - AVG()
  - COUNT()
  - MAX()
  - MIN()
- GROUP BY with conditions
- HAVING clause
- GROUP BY with JOINs
- Real-world business queries

---

## 🗂️ Dataset Used

### Employee Table
- emp_id
- emp_name
- department
- salary
- joining_date

### Sales Table
- sales_id
- emp_id
- product
- amount
- sale_date

---

## 💡 Key Learnings

- Difference between `WHERE` and `HAVING`
- How joins impact data (row duplication issues)
- Using `LEFT JOIN + NULL` to find missing records
- Writing optimized GROUP BY queries
- Handling real-world data scenarios

---

## 🛠️ Tools Used

- Databricks
- SQL

---

## 📌 Example Query

```sql
select e.emp_name, sum(s.amount) as total_sales
from employee e
join sales s
on e.emp_id = s.emp_id
group by e.emp_name;
