------------------------------------------------
-------Meaures Over Time Analysis---------------
------------------------------------------------
SELECT YEAR(order_date) AS order_year,
MONTH(order_date) AS order_month,
SUM(sales_amount) AS total_sales, 
COUNT(DISTINCT customer_id) AS total_customers,
COUNT(DISTINCT order_number) AS total_orders
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY YEAR(order_date),MONTH(order_date)
ORDER BY order_year,order_month;

------------------------------------------------
----------Cumulative Analysis-------------------
------------------------------------------------
SELECT
	order_date,
	total_sales,
	total_quantity,
	avg_price,
	SUM(total_sales) OVER (PARTITION BY YEAR(order_date) ORDER BY order_date) AS running_total_sales,
	SUM(total_quantity) OVER(PARTITION BY YEAR(order_date) ORDER BY order_date) AS cumulative_quantity,
	AVG(avg_price) OVER (PARTITION BY YEAR(order_date) ORDER BY order_date) AS moving_average_price
FROM
(
    SELECT 
        DATETRUNC(month, order_date) AS order_date,
        SUM(sales_amount) AS total_sales,
		SUM(quantity) AS total_quantity,
        AVG(price) AS avg_price
    FROM gold.fact_sales
    WHERE order_date IS NOT NULL
    GROUP BY DATETRUNC(month, order_date)
) t

------------------------------------------------
----------Performance Analysis------------------
------------------------------------------------
 WITH yearly_product_sales AS(
 SELECT
 YEAR(order_date) AS order_year,
 p.product_name,
 SUM(sales_amount) AS current_sales
 FROM gold.fact_sales AS s
 LEFT JOIN gold.dim_products AS p
 ON s.product_key = p.product_key
 WHERE  YEAR(order_date) IS NOT NULL
 GROUP BY YEAR(order_date),p.product_name
 )
 SELECT *,
 AVG(current_sales) OVER(PARTITION BY product_name ORDER BY product_name) AS avg_product_sales,
 (current_sales - AVG(current_sales) OVER(PARTITION BY product_name ORDER BY product_name)) AS avg_diff,
 CASE
	WHEN  (current_sales - AVG(current_sales) OVER(PARTITION BY product_name ORDER BY product_name))>0 THEN 'Above Avg.'
	WHEN  (current_sales - AVG(current_sales) OVER(PARTITION BY product_name ORDER BY product_name))<0 THEN 'Below Avg.'
	ELSE 'Avg.'
END AS avg_sales_comparison,
LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_year) AS prev_year_sales,
(current_sales - LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_year)) AS yoy_sales_diff,
CASE
	WHEN (current_sales - LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_year))>0 THEN 'Increase'
	WHEN (current_sales - LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_year))<0 THEN 'Decreased'
	ELSE 'Same'
END AS yoy_sales_comparison
FROM yearly_product_sales
ORDER BY product_name,order_year;

------------------------------------------------
----------Proportion Analysis-------------------
------------------------------------------------
SELECT *,
SUM(total_sales) OVER() AS overall_sales,
CONCAT(ROUND((CAST(total_sales AS float)/SUM(total_sales) OVER())*100,2),'%') AS percentage_of_overall_sales
FROM
(
SELECT
p.category,
SUM(sales_amount) AS total_sales
FROM
gold.fact_sales AS s LEFT JOIN
gold.dim_products AS p
ON s.product_key = p.product_key
GROUP BY p.category
)AS category_sales
ORDER BY CONCAT(ROUND((CAST(total_sales AS float)/SUM(total_sales) OVER())*100,2),'%') DESC

------------------------------------------------
------------Data Segmentation-------------------
------------------------------------------------
WITH cost_segmentation AS(
SELECT product_key,
product_name,
cost,
CASE
	WHEN cost<100 THEN 'Less than 100'
	WHEN cost BETWEEN 100 AND 500 THEN 'Between 100-500'
	WHEN cost BETWEEN 501 AND 1000 THEN 'Between 501-1000'
	ELSE 'More than 1000'
END cost_range
FROM gold.dim_products
)
SELECT cost_range,COUNT(cost_range) AS no_of_products FROM cost_segmentation
GROUP BY cost_range
ORDER BY no_of_products DESC;

WITH customer_segmentation AS(
SELECT customer_id,
SUM(sales_amount) AS order_amount,
MIN(order_date) AS first_order_date,
MAX(order_date) AS latest_order_date,
DATEDIFF(month,MIN(order_date),MAX(order_date)) AS order_span,
CASE
	WHEN DATEDIFF(month,MIN(order_date),MAX(order_date))>=12 AND SUM(sales_amount)>5000 THEN 'VIP'
	WHEN DATEDIFF(month,MIN(order_date),MAX(order_date))>=12 AND SUM(sales_amount)<=5000 THEN 'Regular'
	ELSE 'New'
END AS customer_segment
FROM gold.fact_sales
GROUP BY customer_id
)
SELECT customer_segment,COUNT(customer_segment) AS no_of_customers
FROM customer_segmentation
GROUP BY customer_segment
ORDER BY no_of_customers DESC;
