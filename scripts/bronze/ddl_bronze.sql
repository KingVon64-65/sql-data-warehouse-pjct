/* 
==================================================================
DDL SCRIPT: Create Bronze Tables
==================================================================
Script Purpose:
this script creates tables in the 'bronze' schema, dropping exsisting tables if they already exist.
Run this script to re-define the DDL structure of 'bronze' Tables
=========================================================================
*/

/* =========================
   CREATE BRONZE LAYER LOADING PROCEDURE
========================= */
CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN

        DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;

    BEGIN TRY
    
    SET @batch_start_time = GETDATE();
        PRINT '==========================';
        PRINT 'Loading Bronze Layer...';
        PRINT '==========================';

        /* =========================
           CRM CUSTOMER INFO
        ========================= */
        SET @start_time = GETDATE();
        PRINT 'Loading CRM Customer Info...';

        IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
            DROP TABLE bronze.crm_cust_info;

        CREATE TABLE bronze.crm_cust_info (
            cst_id INT,
            cst_key VARCHAR(50),
            cst_firstname VARCHAR(50),
            cst_lastname VARCHAR(50),
            cst_material_status VARCHAR(50),
            cst_gendr NVARCHAR(50),
            cst_create_date DATE
        );

        BULK INSERT bronze.crm_cust_info
        FROM 'C:\temp\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'CRM Customer Info loaded in ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' sec';


        /* =========================
           CRM PRODUCT INFO
        ========================= */
        SET @start_time = GETDATE();
        PRINT 'Loading CRM Product Info...';

        IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
            DROP TABLE bronze.crm_prd_info;

        CREATE TABLE bronze.crm_prd_info (
            prd_id INT,
            prd_key NVARCHAR(50),
            prd_nm NVARCHAR(50),
            prd_cost INT,
            prd_line NVARCHAR(50),
            prd_start_dt DATETIME,
            prd_end_dt DATETIME
        );

        BULK INSERT bronze.crm_prd_info
        FROM 'C:\temp\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'CRM Product Info loaded in ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' sec';


        /* =========================
           CRM SALES DETAILS
        ========================= */
        SET @start_time = GETDATE();
        PRINT 'Loading CRM Sales Details...';

        IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
            DROP TABLE bronze.crm_sales_details;

        CREATE TABLE bronze.crm_sales_details (
            sls_ord_num NVARCHAR(50),
            sls_prd_key NVARCHAR(50),
            sls_cust_id INT,
            sls_order_dt INT,
            sls_ship_dt INT,
            sls_due_dt INT,
            sls_sales INT,
            sls_quantity INT,
            sls_price INT
        );

        BULK INSERT bronze.crm_sales_details
        FROM 'C:\temp\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'CRM Sales loaded in ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' sec';


        /* =========================
           ERP CUSTOMER
        ========================= */
        SET @start_time = GETDATE();
        PRINT 'Loading ERP Customer...';

        IF OBJECT_ID('bronze.erp_cust_az12', 'U') IS NOT NULL
            DROP TABLE bronze.erp_cust_az12;

        CREATE TABLE bronze.erp_cust_az12 (
            cid NVARCHAR(50),
            bdate DATE,
            gen NVARCHAR(50)
        );

        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\temp\source_erp\CUST_AZ12.CSV'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'ERP Customer loaded in ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' sec';


        /* =========================
           ERP LOCATION
        ========================= */
        SET @start_time = GETDATE();
        PRINT 'Loading ERP Location...';

        IF OBJECT_ID('bronze.erp_loc_a101', 'U') IS NOT NULL
            DROP TABLE bronze.erp_loc_a101;

        CREATE TABLE bronze.erp_loc_a101 (
            cid NVARCHAR(50),
            cntry NVARCHAR(50)
        );

        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\temp\source_erp\LOC_A101.CSV'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'ERP Location loaded in ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' sec';


        /* =========================
           ERP PRODUCT CATEGORY
        ========================= */
        SET @start_time = GETDATE();
        PRINT 'Loading ERP Product Category...';

        IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
            DROP TABLE bronze.erp_px_cat_g1v2;

        CREATE TABLE bronze.erp_px_cat_g1v2 (
            id NVARCHAR(50),
            cat NVARCHAR(50),
            subcat NVARCHAR(50),
            maintenance NVARCHAR(50)
        );

        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\temp\source_erp\PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'ERP Product Category loaded in ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' sec';

        SET @batch_end_time = GETDATE();
        PRINT '==========================';
        PRINT 'Bronze Layer Loaded OK';
        PRINT 'Total batch time: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' sec';
        PRINT '==========================';

    END TRY

    BEGIN CATCH
        PRINT '❌ ERROR: ' + ERROR_MESSAGE();
        PRINT 'STATE: ' + CAST(ERROR_STATE() AS NVARCHAR(50));
    END CATCH

END
GO


/* =========================
   EXECUTE PROCEDURE
========================= */
EXEC bronze.load_bronze;
GO
