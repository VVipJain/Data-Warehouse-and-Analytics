SELECT * FROM bronze.erp_px_cat_g1v2;

------Unawanted Spaces------------
SELECT * FROM bronze.erp_px_cat_g1v2
WHERE CAT!=TRIM(CAT);

SELECT * FROM bronze.erp_px_cat_g1v2
WHERE SUBCAT!=TRIM(SUBCAT);

SELECT * FROM bronze.erp_px_cat_g1v2
WHERE MAINTENANCE!=TRIM(MAINTENANCE);

------DISTINCT VALUES----------------
SELECT DISTINCT CAT FROM bronze.erp_px_cat_g1v2;

SELECT DISTINCT SUBCAT FROM bronze.erp_px_cat_g1v2;

SELECT DISTINCT MAINTENANCE FROM bronze.erp_px_cat_g1v2;
