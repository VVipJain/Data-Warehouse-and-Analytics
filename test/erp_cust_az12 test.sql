SELECT *
FROM bronze.erp_cust_az12;

-------Invalid BirthDate---------------
SELECT *
FROM bronze.erp_cust_az12
WHERE BDATE>=GETDATE();

-----Checking Different Genders--------
SELECT DISTINCT GEN
FROM bronze.erp_cust_az12;
