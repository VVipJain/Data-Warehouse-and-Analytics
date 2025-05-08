SELECT * FROM bronze.crm_sales_details; 

----Unwanted Spaces in sls_prd_key-----------
SELECT *
FROM  bronze.crm_sales_details
WHERE sls_prd_key!=TRIM((sls_prd_key));

----Invalid Dates------------
SELECT *
FROM bronze.crm_sales_details
WHERE sls_order_dt=0 OR LEN(sls_order_dt)!=8;

SELECT *
FROM bronze.crm_sales_details
WHERE sls_ship_dt=0 OR LEN(sls_ship_dt)!=8;

SELECT *
FROM bronze.crm_sales_details
WHERE sls_due_dt=0 OR LEN(sls_due_dt)!=8;

-------Negative or Incorrect sales---------------
SELECT *
FROM bronze.crm_sales_details
WHERE sls_sales<=0 OR sls_sales!=sls_quantity*abs(sls_price);

-------Negative or 0 quantity---------------
SELECT *
FROM bronze.crm_sales_details
WHERE sls_quantity<=0;

-------Negative or 0 price---------------
SELECT *
FROM bronze.crm_sales_details
WHERE sls_price<=0;
