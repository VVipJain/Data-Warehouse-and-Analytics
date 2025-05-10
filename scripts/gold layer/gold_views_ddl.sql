-- =============================================================================
--Create Fact Table: gold.dim_customers
-- =============================================================================
IF OBJECT_ID('gold.dim_customers','V') IS NOT NULL
	DROP VIEW gold.dim_customers;
GO
CREATE VIEW gold.dim_customers AS
SELECT 
ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_number,
cst_id AS customer_id,
cst_key AS customer_key,
cst_firstname AS customer_firstname,
cst_lastname AS customer_lastname,
BDATE AS birthdate,
CASE
	WHEN cst_gender='Others' AND GEN IS NOT NULL THEN GEN
	WHEN cst_gender='Others' AND GEN IS NULL THEN 'Not Disclosed'
	ELSE cst_gender
END AS gender,
cst_marital_status AS marital_status,
CNTRY AS country,
cst_create_date AS create_date
FROM(
SELECT cst_id,
cst_key,
cst_firstname,
cst_lastname,
cst_marital_status,
cst_gender,
cst_create_date,
CNTRY
FROM silver.crm_cust_info as custinfo
LEFT JOIN silver.erp_loc_a101 as loc
ON custinfo.cst_key=loc.CID
) AS custloc LEFT JOIN silver.erp_cust_az12 AS cstextra
ON custloc.cst_key=cstextra.CID;
GO
-- =============================================================================
-- Create Fact Table: gold.dim_products
-- =============================================================================
IF OBJECT_ID('gold.dim_products','V') IS NOT NULL
	DROP VIEW gold.dim_products;
GO
CREATE VIEW gold.dim_products AS
SELECT 
ROW_NUMBER() OVER(ORDER BY prd_id) AS product_number,
prd_id AS product_id,
prd_key AS product_key,
prd_nm AS product_name,
prd_line AS product_line,
cat_id AS category_id,
CAT AS category,
SUBCAT AS subcategory,
prd_cost AS cost,
MAINTENANCE AS maintenance
FROM silver.crm_prd_info AS prd
LEFT JOIN silver.erp_px_cat_g1v2 AS cat
ON prd.cat_id=cat.ID;
GO
-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
IF OBJECT_ID('gold.fact_sales','V') IS NOT NULL
	DROP VIEW gold.fact_sales;
GO
CREATE VIEW gold.fact_sales AS
SELECT 
sls_ord_num AS order_number,
sls_prd_key AS product_key,
sls_cust_id AS customer_id,
sls_order_dt AS order_date,
sls_ship_dt AS shipping_date,
sls_due_dt AS due_date,
sls_quantity AS quantity,
sls_price AS price,
sls_sales AS sales_amount
FROM silver.crm_sales_details AS sales
LEFT JOIN gold.dim_products AS prd
ON sales.sls_prd_key=prd.product_key
LEFT JOIN gold.dim_customers AS cust
ON sales.sls_cust_id=cust.customer_id;
