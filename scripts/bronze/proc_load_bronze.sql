USE [DataWarehouse]
GO
/****** Object:  StoredProcedure [bronze].[load_bronze]    Script Date: 10/08/2025 11:57:56 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER  PROCEDURE [bronze].[load_bronze] AS 
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @start_batch_time DATETIME, @end_batch_time DATETIME;

BEGIN TRY
	
	SET @start_batch_time = GETDATE()
	PRINT '===============================';
	PRINT 'Loading Bronze Layer';
	PRINT '===============================';

	PRINT'--------------------------------';
	PRINT'Loading CRM Tables';
	PRINT'--------------------------------';

	SET @start_time = GETDATE();
	TRUNCATE TABLE [DataWarehouse].[bronze].[crm_cust_info];
	BULK INSERT [bronze].[crm_cust_info]
	FROM 'C:\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
	WITH (
		FIRSTROW = 2
		, FIELDTERMINATOR = ','
		, TABLOCK
		);
	SET @end_time = GETDATE()
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(ss, @start_time, @end_time) AS NVARCHAR) + ' seconds.';

	SET @start_time = GETDATE();
	TRUNCATE TABLE [DataWarehouse].[bronze].[crm_prd_info];
	BULK INSERT [bronze].[crm_prd_info]
	FROM 'C:\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
	WITH (
		FIRSTROW = 2
		, FIELDTERMINATOR = ','
		, TABLOCK
		);

	SET @end_time = GETDATE()
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(ss, @start_time, @end_time) AS NVARCHAR) + ' seconds.';

	SET @start_time = GETDATE()
	TRUNCATE TABLE [DataWarehouse].[bronze].[crm_sales_details];
	BULK INSERT [bronze].[crm_sales_details]
	FROM 'C:\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
	WITH (
		FIRSTROW = 2
		, FIELDTERMINATOR = ','
		, TABLOCK
		);
	SET @end_time = GETDATE()
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(ss, @start_time, @end_time) AS NVARCHAR) + ' seconds.';

	PRINT'--------------------------------';
	PRINT'Loading ERP Tables';
	PRINT'--------------------------------';

	SET @start_time = GETDATE()
	TRUNCATE TABLE [DataWarehouse].[bronze].[erp_cust_az12];
	BULK INSERT [bronze].[erp_cust_az12]
	FROM 'C:\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
	WITH (
		FIRSTROW = 2
		, FIELDTERMINATOR = ','
		, TABLOCK
		);
	SET @end_time = GETDATE()
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(ss, @start_time, @end_time) AS NVARCHAR) + ' seconds.';

	SET @start_time = GETDATE()
	TRUNCATE TABLE [DataWarehouse].[bronze].[erp_loc_a101];
	BULK INSERT [bronze].[erp_loc_a101]
	FROM 'C:\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
	WITH (
		FIRSTROW = 2
		, FIELDTERMINATOR = ','
		, TABLOCK
		);
	SET @end_time = GETDATE()
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(ss, @start_time, @end_time) AS NVARCHAR) + ' seconds.';

	SET @start_time = GETDATE()
	TRUNCATE TABLE [DataWarehouse].[bronze].[erp_px_cat_g1v2];
	BULK INSERT [bronze].[erp_px_cat_g1v2]
	FROM 'C:\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
	WITH (
		FIRSTROW = 2
		, FIELDTERMINATOR = ','
		, TABLOCK
		);
	SET @end_time = GETDATE()
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(ss, @start_time, @end_time) AS NVARCHAR) + ' seconds.';


END TRY
		BEGIN CATCH
			PRINT '============================';
			PRINT 'Error Occured During Bronze Layer';
			PRINT 'Error Message ' + ERROR_MESSAGE();
			PRINT '============================';

		END CATCH
	SET @end_batch_time = GETDATE()
	PRINT '===============================';
	PRINT '>> Batch Duration: ' + CAST(DATEDIFF(dd, @start_batch_time, @end_batch_time) AS NVARCHAR) + ' seconds.';
END
