CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	BEGIN TRY
		DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
		PRINT '==============================================================='
		PRINT '           LOADING THE DATA INTO THE SILVER LAYER              '
		PRINT '==============================================================='

		PRINT '---------------------------------------------------------------'
		PRINT '              Loading the CRM Tables Data                      '
		PRINT '---------------------------------------------------------------'
		PRINT '<<---Loading Table: silver.crm_cust_info--->>'
		PRINT '>>>Truncating Table: silver.crm_cust_info'
		SET @start_time=GETDATE();
		SET @batch_start_time=GETDATE();
		TRUNCATE TABLE silver.crm_cust_info
		PRINT '>>>Inserting Data Into: silver.crm_cust_info'
		INSERT INTO silver.crm_cust_info(
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gender,
			cst_create_date
		)
		SELECT cst_id,
		cst_key,
		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,
		CASE
			WHEN UPPER(TRIM(cst_marital_status))='S' THEN 'Single'
			WHEN UPPER(TRIM(cst_marital_status))='M' THEN 'Married'
			ELSE 'Not Declared'
		END AS 'cst_marital_status',
		CASE
			WHEN UPPER(TRIM(cst_gender))='F' THEN 'Female'
			WHEN UPPER(TRIM(cst_gender))='M' THEN 'Male'
			ELSE 'Others'
		END AS 'cst_gender',
		cst_create_date FROM(
			SELECT *,
			RANK() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS latest_data
			FROM bronze.crm_cust_info
		)t
		WHERE latest_data=1 AND cst_id IS NOT NULL;
		SET @end_time=GETDATE();
		PRINT '>>>Loading Duration: '+CAST(DATEDIFF(second,@end_time,@start_time) AS NVARCHAR)+' sec.'
		PRINT '--------------------------'

		PRINT '<<---Loading Table: silver.crm_prd_info--->>'
		PRINT '>>>Truncating Table: silver.crm_prd_info'
		TRUNCATE TABLE silver.crm_prd_info
		SET @start_time=GETDATE();
		PRINT '>>>Inserting Data Into: silver.crm_prd_info'
		INSERT INTO silver.crm_prd_info
		(	prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
		SELECT 
		prd_id,
		REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
		SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
		prd_nm,
		ISNULL(prd_cost,0) AS prd_cost,
		CASE UPPER(TRIM(prd_line))
			WHEN 'M'THEN 'Mountains'
			WHEN 'R'THEN 'Road'
			WHEN 'S'THEN 'Sales'
			WHEN 'T'THEN 'Tour'
			ELSE 'Other'
		END AS prd_line,
		prd_start_dt,
		DATEADD(DAY,15,prd_start_dt) AS prd_end_dt
		FROM bronze.crm_prd_info;
		SET @end_time=GETDATE();
		PRINT '>>>Loading Duration: '+CAST(DATEDIFF(second,@end_time,@start_time) AS NVARCHAR)+' sec.'
		PRINT '--------------------------'

		PRINT '<<---Loading Table: silver.crm_sales_details--->>'
		PRINT '>>>Truncating Table: silver.crm_sales_details'
		TRUNCATE TABLE silver.crm_sales_details
		SET @start_time=GETDATE();
		PRINT '>>>Inserting Data Into: silver.crm_sales_details'
		INSERT INTO silver.crm_sales_details(
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
		)
		SELECT sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE
			WHEN sls_order_dt=0 OR LEN(sls_order_dt)!=8 THEN NULL
			ELSE CAST(CAST(sls_order_dt AS CHAR(8)) AS DATE) 
		END AS sls_order_dt,
		CAST(CAST(sls_ship_dt AS CHAR(8)) AS DATE) AS sls_ship_dt,
		CAST(CAST(sls_due_dt AS CHAR(8)) AS DATE) AS sls_due_dt,
		CASE
			WHEN sls_sales<=0 OR sls_sales!=sls_quantity*abs(sls_price) THEN sls_quantity*abs(sls_price)
			ELSE sls_sales
		END AS sls_sales,
		sls_quantity,
		CASE
			WHEN sls_price<=0 THEN abs(sls_price)
			ELSE sls_price
		END AS sls_price
		FROM bronze.crm_sales_details;
		SET @end_time=GETDATE();
		PRINT '>>>Loading Duration: '+CAST(DATEDIFF(second,@end_time,@start_time) AS NVARCHAR)+' sec.'
		PRINT '--------------------------'

		PRINT '---------------------------------------------------------------'
		PRINT '                Loading the ERP Tables Data                    '
		PRINT '---------------------------------------------------------------'
		PRINT '<<---Loading Table: silver.erp_cust_az12--->>'
		PRINT '>>>Truncating Table: silver.erp_cust_az12'
		TRUNCATE TABLE silver.erp_cust_az12
		SET @start_time=GETDATE();
		PRINT '>>>Inserting Data Into: silver.erp_cust_az12'
		INSERT INTO silver.erp_cust_az12(
			CID,
			BDATE,
			GEN
		)
		SELECT 
		CASE
			WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID,4,LEN(CID))
			ELSE CID
		END AS CID,
		CASE
			WHEN BDATE>=GETDATE() THEN NULL
			ELSE BDATE
		END AS BDATE,
		CASE
			WHEN UPPER(TRIM(GEN))='F' OR UPPER(TRIM(GEN))='Female' THEN 'Female'
			WHEN UPPER(TRIM(GEN))='M' OR UPPER(TRIM(GEN))='Male' THEN 'Male'
			ELSE 'Not Disclosed'
		END AS GEN
		FROM bronze.erp_cust_az12;
		SET @end_time=GETDATE();
		PRINT '>>>Loading Duration: '+CAST(DATEDIFF(second,@end_time,@start_time) AS NVARCHAR)+' sec.'
		PRINT '--------------------------'

		PRINT '<<---Loading Table: silver.erp_loc_a101--->>'
		PRINT '>>>Truncating Table: silver.erp_loc_a101'
		TRUNCATE TABLE silver.erp_loc_a101
		SET @start_time=GETDATE();
		PRINT '>>>Inserting Data Into: silver.erp_loc_a101'
		INSERT INTO silver.erp_loc_a101
		(
			CID,
			CNTRY
		)
		SELECT REPLACE(CID,'-','') AS CID,
		CASE
			WHEN TRIM(CNTRY)='DE' OR TRIM(CNTRY)='Germany' THEN 'Germany'
			WHEN TRIM(CNTRY)='USA' OR TRIM(CNTRY)='US' THEN 'United States'
			WHEN CNTRY='' OR CNTRY IS NULL THEN 'Not Specified'
			ELSE TRIM(CNTRY)
		END AS CNTRY
		FROM bronze.erp_loc_a101;
		SET @end_time=GETDATE();
		PRINT '>>>Loading Duration: '+CAST(DATEDIFF(second,@end_time,@start_time) AS NVARCHAR)+' sec.'
		PRINT '--------------------------'

		PRINT '<<---Loading Table: silver.erp_px_cat_g1v2--->>'
		PRINT '>>>Truncating Table: silver.erp_px_cat_g1v2'
		TRUNCATE TABLE silver.erp_px_cat_g1v2
		SET @start_time=GETDATE();
		PRINT '>>>Inserting Data Into: silver.erp_px_cat_g1v2'
		INSERT INTO silver.erp_px_cat_g1v2
		(
			ID,
			CAT,
			SUBCAT,
			MAINTENANCE
		)
		SELECT ID,	
		CAT,SUBCAT, MAINTENANCE
		FROM bronze.erp_px_cat_g1v2;
		SET @end_time=GETDATE();
		SET @batch_end_time=GETDATE();
		PRINT '>>>Loading Duration: '+CAST(DATEDIFF(second,@end_time,@start_time) AS NVARCHAR)+' sec.'
		PRINT '--------------------------'
		PRINT '/////OVERALL DURATION: '+CAST(DATEDIFF(second,@batch_end_time,@batch_start_time) AS NVARCHAR)+' sec./////'
	END TRY
	BEGIN CATCH
		PRINT '==========================================================='
		PRINT 'ERROR OCCURED DURING LOADING THE DATA IN BRONZE LAYER   '
		PRINT 'Error_Message' + ERROR_MESSAGE();
		PRINT 'Error_Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error_Message' + CAST(ERROR_STATE() AS NVARCHAR);
	END CATCH
END
