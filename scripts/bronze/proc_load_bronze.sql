/*
=========================================================================
ETL Script: Load Bronze Layer
=========================================================================

Purpose:
This script populates the Bronze layer tables in the DataWarehouse database.
It performs the following actions for each table in the 'bronze' schema:

1. Truncates the existing table data to ensure a fresh load.
2. Loads raw data from CSV files into the corresponding Bronze tables using BULK INSERT.
3. Measures and prints the time taken for each table load.
4. Handles errors using TRY...CATCH and logs error messages if any occur.

Notes:
- This script assumes that all Bronze tables have been created already (DDL is managed separately).
- CSV files must exist in the specified file paths.
- Designed for repeatable batch loads of raw data.
=========================================================================
*/

/* =========================
   CREATE BRONZE LAYER LOADING PROCEDURE
========================= */

CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN

    DECLARE 
        @start_time DATETIME,
        @end_time DATETIME,
        @batch_start_time DATETIME,
        @batch_end_time DATETIME;

    BEGIN TRY
        
        SET @batch_start_time = GETDATE();

        PRINT '==========================';
        PRINT 'Loading Bronze Layer...';
        PRINT '==========================';

        /* CRM CUSTOMER */
        PRINT 'Loading CRM Customer Info...';
        TRUNCATE TABLE bronze.crm_cust_info;

        BULK INSERT bronze.crm_cust_info
        FROM 'C:\temp\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        /* CRM PRODUCT */
        PRINT 'Loading CRM Product Info...';
        TRUNCATE TABLE bronze.crm_prd_info;

        BULK INSERT bronze.crm_prd_info
        FROM 'C:\temp\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        /* CRM SALES */
        PRINT 'Loading CRM Sales Details...';
        TRUNCATE TABLE bronze.crm_sales_details;

        BULK INSERT bronze.crm_sales_details
        FROM 'C:\temp\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        /* ERP CUSTOMER */
        PRINT 'Loading ERP Customer...';
        TRUNCATE TABLE bronze.erp_cust_az12;

        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\temp\source_erp\CUST_AZ12.CSV'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        /* ERP LOCATION */
        PRINT 'Loading ERP Location...';
        TRUNCATE TABLE bronze.erp_loc_a101;

        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\temp\source_erp\LOC_A101.CSV'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        /* ERP CATEGORY */
        PRINT 'Loading ERP Product Category...';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\temp\source_erp\PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        SET @batch_end_time = GETDATE();

        PRINT '==========================';
        PRINT 'Bronze Layer Loaded OK';
        PRINT 'Total time: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' sec';
        PRINT '==========================';

    END TRY

    BEGIN CATCH
        PRINT 'ERROR: ' + ERROR_MESSAGE();
    END CATCH

END
GO
