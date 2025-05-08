SELECT * FROM bronze.crm_prd_info;

----Checking for duplicates---------
SELECT prd_id, COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*)>1

-----Matching cat_id---------------
SELECT * FROM bronze.erp_px_cat_g1v2;

-----Matching prod_key---------------
SELECT * FROM bronze.crm_sales_details;

-----Checking Spaces in prd_nm--------
SELECT *
FROM bronze.crm_prd_info
WHERE prd_nm!=TRIM(prd_nm);

------Negative or Null prd_cost--------
SELECT *
FROM bronze.crm_prd_info
WHERE prd_cost<0 or prd_cost IS NULL;

-------Product Line-----------------
SELECT DISTINCT
prd_line
FROM bronze.crm_prd_info;

--------Start Date & End Date--------
SELECT *
FROM bronze.crm_prd_info
WHERE prd_start_dt>prd_end_dt OR prd_end_dt IS NULL;
