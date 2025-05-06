CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		PRINT '==============================================================='
		PRINT '           LOADING THE DATA INTO THE BRONZE LAYER              '
		PRINT '==============================================================='

		PRINT '---------------------------------------------------------------'
		PRINT '              Loading the CRM Tables Data                      '
		PRINT '---------------------------------------------------------------'
		SET @batch_start_time = GETDATE();
		PRINT '>>>Truncating Table:bronze.crm_cust_info'
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT 'Inserting Data Into:bronze.crm_cust_info'
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\VIPUL JAIN\OneDrive\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>>Load Duration: '+CAST(DATEDIFF(second,@end_time,@start_time) AS NVARCHAR)+'sec.'
		PRINT '------------------'

		PRINT '>>>Truncating Table:bronze.crm_prd_info'
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT 'Inserting Data Into:bronze.crm_prd_info'
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\VIPUL JAIN\OneDrive\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>>Load Duration: '+CAST(DATEDIFF(second,@end_time,@start_time) AS NVARCHAR)+'sec.'
		PRINT '------------------'

		PRINT '>>>Truncating Table:bronze.crm_sales_details'
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT 'Inserting Data Into:bronze.crm_sales_details'
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\VIPUL JAIN\OneDrive\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>>Load Duration: '+CAST(DATEDIFF(second,@end_time,@start_time) AS NVARCHAR)+'sec.'
		PRINT '------------------'

		PRINT '---------------------------------------------------------------'
		PRINT '                Loading the ERP Tables Data                    '
		PRINT '---------------------------------------------------------------'

		PRINT '>>>Truncating Table:erp_cust_az12'
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT 'Inserting Data Into:bronze.erp_cust_az12'
		SET @start_time = GETDATE();
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\VIPUL JAIN\OneDrive\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>>Load Duration: '+CAST(DATEDIFF(second,@end_time,@start_time) AS NVARCHAR)+'sec.'
		PRINT '------------------'

		PRINT '>>>Truncating Table:bronze.erp_loc_a101'
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT 'Inserting Data Into:bronze.erp_loc_a101'
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\VIPUL JAIN\OneDrive\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>>Load Duration: '+CAST(DATEDIFF(second,@end_time,@start_time) AS NVARCHAR)+'sec.'
		PRINT '------------------'

		PRINT '>>>Truncating Table:bronze.erp_px_cat_g1v2'
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT 'Inserting Data Into:bronze.erp_px_cat_g1v2'
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\VIPUL JAIN\OneDrive\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		SET @batch_end_time = GETDATE();
		PRINT '>>>Load Duration: '+CAST(DATEDIFF(second,@end_time,@start_time) AS NVARCHAR)+'sec.'
		PRINT '------------------'
		PRINT '/////OVERALL LOAD DURATION:-'+CAST(DATEDIFF(second,@batch_end_time,@batch_start_time) AS NVARCHAR)+ 'sec./////' 

	END TRY
	BEGIN CATCH
		PRINT '==========================================================='
		PRINT '   ERROR OCCURED DURING LOADING THE DATA IN BRONZE LAYER   '
		PRINT 'Error_Message' + ERROR_MESSAGE();
		PRINT 'Error_Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error_Message' + CAST(ERROR_STATE() AS NVARCHAR);
	END CATCH
END
