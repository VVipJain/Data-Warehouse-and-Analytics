------Checking for Null Values and Duplicates--------
SELECT cst_id, COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*)!=1 OR cst_id IS NULL;

------First Name Spaces-------------
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname!=TRIM(cst_firstname);

-----Last Name Spaces----------------
SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname!=TRIM(cst_lastname);

------Marital Status-----------------
SELECT DISTINCT cst_marital_status
FROM bronze.crm_cust_info;

-----Gender--------------------------
SELECT DISTINCT cst_gender
FROM bronze.crm_cust_info;
