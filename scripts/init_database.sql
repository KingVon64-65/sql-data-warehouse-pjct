
/* =======================================
CREATE DATABASE AND SCHEMAS
=======================================
Script purpose: This script creates the "DataWarehouse" database and the "bronze", "silver", and "gold" schemas within it. The script first checks if the "DataWarehouse" database already exists, and if it does, it sets it to single-user mode and drops it before creating a new one. After creating the database, it switches to it and creates the three schemas.
WARNING: Running this script will delete the existing "DataWarehouse" database and all its contents. Make sure to back up any important data before executing this script.
*/


USE master;
GO

  --drop and recrate the "DataWarehouse" database
  IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
  ALTER DATABASE DataWharehouse SET SINGLE_USER WHIT ROLLBACK IMMEDIATE;
DROP DATABASE DataWarehouse;
GO

  --create the DataWharehouse database
  CREATE DATABASE DataWarehouse;

USE DataWarehouse;
GO

  --create schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO


