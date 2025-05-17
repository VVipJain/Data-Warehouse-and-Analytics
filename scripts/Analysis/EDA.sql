-----------Exploring the Database--------------
SELECT * FROM INFORMATION_SCHEMA.TABLES;

------------Exploring Columns in the Sales Table------------
SELECT * FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME='fact_sales';

--------------------------------------------------------
----------------Dimesions Exploration-------------------
---------------------------------------------------------

----------Checking Values in Marital Status-------------
SELECT DISTINCT marital_status
FROM gold.dim_customers;

----------Checking Values in gender-------------
SELECT DISTINCT gender
FROM gold.dim_customers;

----------Checking Values in country-------------
SELECT DISTINCT country
FROM gold.dim_customers;

----------Checking category hierarchary-------------
SELECT DISTINCT category,subcategory, product_name
FROM gold.dim_products; ------> 4 categories -> 36 subcategories -> 295 products

--------------------------------------------------------
----------------DATES Exploration-----------------------
--------------------------------------------------------

----------Oldest and Youngest Customer Age-------------
SELECT DATEDIFF(year,MIN(birthdate),GETDATE()) AS oldest_customer,
DATEDIFF(year,MAX(birthdate),GETDATE()) AS youngest_customer,
FROM gold.dim_customers;

----------First and last order date-------------
SELECT MIN(order_date) AS first_order_date, MAX(order_date) AS last_order_date,
DATEDIFF(year,MIN(order_date),MAX(order_date)) AS order_range_years
FROM gold.fact_sales;

--------------------------------------------------------
----------------Measures Exploration--------------------
--------------------------------------------------------

---------Total Sales-------------
SELECT SUM(sales_amount) AS total_sales
FROM gold.fact_sales;

---------Total quantity-------------
SELECT SUM(quantity) AS total_items_sold
FROM gold.fact_sales;

---------Average price-------------
SELECT AVG(price) AS avg_selling_price
FROM gold.fact_sales;

---------Total Orders-------------
SELECT COUNT(DISTINCT order_number) AS total_orders
FROM gold.fact_sales;

---------Total products-------------
SELECT COUNT(DISTINCT product_name) AS total_products
FROM gold.dim_products;

---------Total customers-------------
SELECT COUNT(DISTINCT customer_id) AS total_customers
FROM gold.dim_customers;

---------Total customers ordered-------------
SELECT COUNT(DISTINCT cust.customer_id) AS customers_ordered
FROM gold.dim_customers AS cust RIGHT JOIN gold.fact_sales AS sales
ON cust.customer_id = sales.customer_id;

---------All key measures and values-------------
SELECT 'Total Sales' AS MeasureNAME, SUM(sales_amount) AS MeasureValue FROM gold.fact_sales
UNION ALL
SELECT 'Total Qty' AS MeasureNAME, SUM(quantity) AS MeasureValue FROM gold.fact_sales
UNION ALL
SELECT 'Avg. Price' AS MeasureNAME, AVG(price) AS MeasureValue FROM gold.fact_sales
UNION ALL
SELECT 'Total Orders' AS MeasureNAME, COUNT(DISTINCT order_number) AS MeasureValue FROM gold.fact_sales
UNION ALL
SELECT 'Total Products' AS MeasureNAME, COUNT(DISTINCT product_name) AS MeasureValue FROM gold.dim_products
UNION ALL
SELECT 'Total Customers' AS MeasureNAME, COUNT(DISTINCT customer_id) AS MeasureValue FROM gold.dim_customers
UNION ALL
SELECT 'Total Customers Who Ordered' AS MeasureNAME, COUNT(DISTINCT customer_id) AS MeasureValue FROM gold.fact_sales;

---------------------------------------------------------------------
----------------Measures by Dimension Exploration--------------------
---------------------------------------------------------------------

---------Customers by country-------------
SELECT country, COUNT(customer_id) AS total_customers
FROM gold.dim_customers
GROUP BY country;

---------Customers by gender-------------
SELECT gender, COUNT(customer_id) AS total_customers
FROM gold.dim_customers
GROUP BY gender;

---------Products In each category-------------
SELECT category, COUNT(product_id) AS total_products
FROM gold.dim_products
GROUP BY category;

---------Average cost of category-------------
SELECT category, AVG(cost) AS avg_cost
FROM gold.dim_products
GROUP BY category;

---------Revenue by category-------------
SELECT p.category,SUM(sales_amount) AS total_revenue
FROM gold.fact_sales AS s LEFT JOIN
gold.dim_products AS p
ON s.product_key = p.product_key
GROUP BY p.category;

---------Revenue by each customer-------------
SELECT s.customer_id,c.customer_firstname,c.customer_lastname,SUM(sales_amount) AS total_revenue
FROM gold.fact_sales AS s LEFT JOIN
gold.dim_customers AS c
ON s.customer_id = c.customer_id
GROUP BY s.customer_id,c.customer_firstname,c.customer_lastname;

---------Items sold in each country-------------
SELECT country, COUNT(s.quantity) AS total_items_sold
FROM gold.fact_sales AS s LEFT JOIN
gold.dim_customers AS c
ON s.customer_id = c.customer_id
GROUP BY c.country; 

---------------------------------------------------------------------
----------------Top and Worst Performers-----------------------------
---------------------------------------------------------------------

---------Top 5 product by reveue-------------
SELECT TOP 5
p.product_name,SUM(sales_amount) AS total_revenue
FROM gold.fact_sales AS s LEFT JOIN
gold.dim_products AS p
ON s.product_key = p.product_key
GROUP BY p.product_name
ORDER BY total_revenue DESC;

---------Last 5 product by reveue-------------
SELECT TOP 5
p.product_name,SUM(sales_amount) AS total_revenue
FROM gold.fact_sales AS s LEFT JOIN
gold.dim_products AS p
ON s.product_key = p.product_key
GROUP BY p.product_name
ORDER BY total_revenue;

---------Top 5 customers by reveue-------------
SELECT TOP 10 
s.customer_id,c.customer_firstname,c.customer_lastname,SUM(sales_amount)AS total_revenue
FROM gold.fact_sales AS s LEFT JOIN
gold.dim_customers AS c
ON s.customer_id = c.customer_id
GROUP BY s.customer_id,c.customer_firstname,c.customer_lastname
ORDER BY total_revenue DESC;

---------Last 3 customers by orders-------------
SELECT TOP 3 
s.customer_id,c.customer_firstname,c.customer_lastname,COUNT(DISTINCT s.order_number)AS total_orders
FROM gold.fact_sales AS s LEFT JOIN
gold.dim_customers AS c
ON s.customer_id = c.customer_id
GROUP BY s.customer_id,c.customer_firstname,c.customer_lastname
ORDER BY total_orders;
