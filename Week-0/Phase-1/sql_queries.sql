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

--queries
--1
SELECT * FROM customers;
--2
SELECT * FROM customers
WHERE city = 'Chennai';
--3
SELECT * FROM customers
WHERE age > 25;
--4
SELECT customer_name, city FROM customers;
--5
SELECT city, COUNT(*) AS total_customers
FROM customers
GROUP BY city;
