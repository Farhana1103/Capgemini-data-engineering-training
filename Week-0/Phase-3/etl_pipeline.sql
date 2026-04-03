--  PHASE 3: SQL DATA PIPELINE 

-- -----------------------------------------
-- 1. EXTRACT – Load Source Data
-- -----------------------------------------

CREATE OR REPLACE TEMP VIEW raw_customers
USING csv
OPTIONS (
  path "/samples/customers.csv",
  header "true",
  inferSchema "true"
);

CREATE OR REPLACE TEMP VIEW raw_sales
USING csv
OPTIONS (
  path "/samples/sales.csv",
  header "true",
  inferSchema "true"
);

-- -----------------------------------------
-- 2. TRANSFORM – Data Cleaning Layer
-- -----------------------------------------

-- Clean sales data (remove invalid & null records)
CREATE OR REPLACE TEMP VIEW sales_clean AS
SELECT *
FROM raw_sales
WHERE customer_id IS NOT NULL
  AND quantity > 0
  AND total_amount > 0;

-- Clean customer data (ensure valid keys)
CREATE OR REPLACE TEMP VIEW customers_clean AS
SELECT *
FROM raw_customers
WHERE customer_id IS NOT NULL;

-- -----------------------------------------
-- 3. TRANSFORM – Business Logic Layer
-- -----------------------------------------

-- Daily sales trend
CREATE OR REPLACE TEMP VIEW daily_sales AS
SELECT 
    sale_date,
    SUM(total_amount) AS total_daily_sales
FROM sales_clean
GROUP BY sale_date;

-- Revenue by city
CREATE OR REPLACE TEMP VIEW city_revenue AS
SELECT 
    c.city,
    SUM(s.total_amount) AS total_revenue
FROM sales_clean s
JOIN customers_clean c
ON s.customer_id = c.customer_id
GROUP BY c.city;

-- Repeat customers (more than 2 orders)
CREATE OR REPLACE TEMP VIEW repeat_customers AS
SELECT 
    customer_id,
    COUNT(*) AS order_count
FROM sales_clean
GROUP BY customer_id
HAVING COUNT(*) > 2;

-- Top customer per city (window function)
CREATE OR REPLACE TEMP VIEW top_customers AS
SELECT city, customer_id, total_spent
FROM (
    SELECT 
        c.city,
        s.customer_id,
        SUM(s.total_amount) AS total_spent,
        ROW_NUMBER() OVER (
            PARTITION BY c.city 
            ORDER BY SUM(s.total_amount) DESC
        ) AS rank
    FROM sales_clean s
    JOIN customers_clean c
    ON s.customer_id = c.customer_id
    GROUP BY c.city, s.customer_id
) ranked_data
WHERE rank = 1;

-- -----------------------------------------
-- 4. FINAL AGGREGATED OUTPUT
-- -----------------------------------------

CREATE OR REPLACE TEMP VIEW final_report AS
SELECT 
    c.customer_id,
    c.city,
    SUM(s.total_amount) AS total_spend,
    COUNT(*) AS total_orders
FROM sales_clean s
JOIN customers_clean c
ON s.customer_id = c.customer_id
GROUP BY c.customer_id, c.city;

-- -----------------------------------------
-- 5. LOAD – Persist Final Data
-- -----------------------------------------

CREATE TABLE IF NOT EXISTS final_report_table
USING parquet
AS
SELECT * FROM final_report;

-- -----------------------------------------
-- 6. VALIDATION – Quick Checks
-- -----------------------------------------

SELECT * FROM daily_sales;
SELECT * FROM city_revenue;
SELECT * FROM repeat_customers;
SELECT * FROM top_customers;
SELECT * FROM final_report;
