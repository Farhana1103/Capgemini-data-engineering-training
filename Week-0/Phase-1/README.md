
# SQL to PySpark – Phase 1: Simple Foundation Pack

## 🔹 Objective

Build confidence in converting basic SQL queries into PySpark DataFrame operations using simple datasets. This phase focuses on understanding how SQL concepts translate into PySpark.

---

## 🔹 Concepts Covered

* `show()` → Display data
* `select()` → Select specific columns
* `filter()` → Apply conditions
* `count()` → Count number of records 
* `groupBy()` → Perform aggregation
* Understanding **DataFrame as a table**
* Difference between **`=` in SQL** and **`==` in PySpark**

---

## 🔹 Dataset Used

### SQL Table

```sql
CREATE TABLE customers (
    customer_id INT,
    customer_name VARCHAR(50),
    city VARCHAR(50),
    age INT
);

INSERT INTO customers VALUES
(1, 'Ravi', 'Hyderabad', 25),
(2, 'Sita', 'Chennai', 32),
(3, 'Arun', 'Hyderabad', 28);
```

### PySpark DataFrame

```python
customers = spark.createDataFrame(
    [
        (1, "Ravi", "Hyderabad", 25),
        (2, "Sita", "Chennai", 32),
        (3, "Arun", "Hyderabad", 28)
    ],
    ["customer_id", "customer_name", "city", "age"]
)
```

---

## 🔹 Exercises Completed

### 1. Show all customers

**SQL**

```sql
SELECT * FROM customers;
```

**PySpark**

```python
customers.show()
```

---

### 2. Show customers from Chennai

**SQL**

```sql
SELECT * FROM customers WHERE city = 'Chennai';
```

**PySpark**

```python
customers.filter(customers.city == "Chennai").show()
```

---

### 3. Show customers with age > 25

**SQL**

```sql
SELECT * FROM customers WHERE age > 25;
```

**PySpark**

```python
customers.filter(customers.age > 25).show()
```

---

### 4. Show only customer_name and city

**SQL**

```sql
SELECT customer_name, city FROM customers;
```

**PySpark**

```python
customers.select("customer_name", "city").show()
```

---

### 5. Count customers city-wise

**SQL**

```sql
SELECT city, COUNT(*) FROM customers GROUP BY city;
```

**PySpark**

```python
customers.groupBy("city").count().show()
```

---

## 🔹 Key Difference: SQL vs PySpark Conditions

| SQL | PySpark |
| --- | ------- |
| `=` | `==`    |

* In **SQL**, `=` is used for comparison
* In **PySpark (Python)**, `==` is used for comparison
* `=` in PySpark is used for **assignment**, not comparison

---

## 🔹 Learning Outcome

* Understood how SQL queries map to PySpark DataFrame operations
* Learned the difference between `=` and `==`
* Practiced filtering, selection, and aggregation
* Built a strong foundation in PySpark basics
* Gained clarity on how DataFrames behave like SQL tables

---

## 🔹 Author

**Farhana**
