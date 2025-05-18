------------------------------------------------
------------Customer Report View----------------
------------------------------------------------
IF OBJECT_ID('gold.report_customers','V') IS NOT NULL
DROP VIEW gold.report_customers;
GO
CREATE VIEW gold.report_customers AS
WITH base_query AS(
SELECT s.order_number,
s.product_key,
s.customer_id,
c.customer_key,
CONCAT(c.customer_firstname,' ',c.customer_lastname) AS customer_fullname,
DATEDIFF(YEAR,c.birthdate,GETDATE()) AS age,
s.order_date,
s.quantity,
s.sales_amount
FROM gold.fact_sales AS s
LEFT JOIN gold.dim_customers AS c
ON s.customer_id = c.customer_id
WHERE order_number IS NOT NULL
),
cust_agg AS(
SELECT customer_id,
customer_key,
customer_fullname,
age,
MAX(order_date) AS last_order_date,
SUM(sales_amount) AS total_sales,
COUNT(DISTINCT order_number) AS total_orders,
COUNT(DISTINCT product_key) AS products_ordered,
SUM(quantity) AS purchase_quantity,
DATEDIFF(MONTH,MIN(order_date),MAX(order_date)) AS order_monthspan
FROM base_query
GROUP BY customer_id,customer_key,customer_fullname,age
)
SELECT customer_id,customer_key,customer_fullname,age,
CASE
	WHEN age<18 THEN 'Under 18'
	WHEN age BETWEEN 19 AND 40 THEN '19-35'
	WHEN age BETWEEN 41 AND 60 THEN '41-60'
	ELSE 'Above 60'
END AS age_range,
CASE
	WHEN order_monthspan>=12 AND total_sales>5000 THEN 'VIP'
	WHEN order_monthspan>=12 AND total_sales<=5000 THEN 'Regular'
	ELSE 'New'
END AS customer_segment,
DATEDIFF(month,last_order_date,GETDATE()) AS 'order_receny(in months)',
order_monthspan,
total_sales,
total_orders,
products_ordered,
purchase_quantity,
CASE
	WHEN total_orders=0 THEN 0
	ELSE total_sales/total_orders
END AS avg_order_value,
CASE
	WHEN order_monthspan=0 THEN 0
	ELSE total_sales/order_monthspan
END AS avg_monthly_spend
FROM cust_agg;

SELECT * FROM gold.report_customers;

------------------------------------------------
------------Product Report View-----------------
------------------------------------------------
IF OBJECT_ID('gold.report_products','V') IS NOT NULL
DROP VIEW gold.report_products;
GO
CREATE VIEW gold.report_products AS
WITH base_query AS(
SELECT p.product_id,
p.product_key,
p.product_line,
p.category,
p.subcategory,
p.product_name,
s.customer_id,
p.cost,
s.order_number,
s.order_date,
s.quantity,
s.sales_amount
FROM gold.fact_sales AS s
LEFT JOIN gold.dim_products AS p
ON s.product_key = p.product_key
WHERE s.order_number IS NOT NULL
)
,prod_agg AS(
SELECT
product_key,
product_line,
category,
subcategory,
product_name,
cost,
MAX(order_date) AS last_order_date,
SUM(sales_amount) AS total_sales,
COUNT(DISTINCT order_number) AS total_orders,
COUNT(DISTINCT customer_id) AS total_customers,
SUM(quantity) AS quantity_sold,
DATEDIFF(MONTH,MIN(order_date),MAX(order_date)) AS order_monthspan
FROM base_query
GROUP BY
product_key,
product_line,
category,
subcategory,
product_name,
cost
)
SELECT 
product_key,
product_line,
category,
subcategory,
product_name,
cost,
last_order_date,
DATEDIFF(month,last_order_date,GETDATE()) AS 'order_receny(in months)',
order_monthspan,
total_sales,
CASE
	WHEN total_sales>60000 THEN 'High Revenue'
	WHEN total_sales BETWEEN 10000 AND 60000 THEN 'Mid Revenue'
	ELSE 'Low Revenue'
END AS revenue_category,
total_customers,
total_orders,
quantity_sold,
CASE
	WHEN total_orders=0 THEN 0
	ELSE total_sales/total_orders
END AS avg_order_value,
CASE
	WHEN order_monthspan=0 THEN 0
	ELSE total_sales/order_monthspan
END AS avg_monthly_spend
FROM prod_agg;

SELECT * FROM gold.report_products;
