-- Create the DataWarehouse database

/*
Create Database And Schemas

Script Purpose:
This script creates a new database named 'DataWarehouse' after checking if it already exists. 
If the database exists, it is dropped and recreated. Additionally, the script sets up 3 schemas within the database: 'bronze', 'silver', 'gold'.

Warning :
Running this script will drop the entire database "DataWarehouse' if it already exist.
All data in the database will be permanantly deleted. Proceed with caution.
*/

--Use Master 
use master;

--Drop and re-create DataWarehouse database 
If exists (select 1 from sys.databases where name ='DataWarehouse')
begin 
  alter database dataWar set sinnle_user with rollback immediate;
  drop database 
end;
go

  -- Create DataWarehouse database
Create database DataWarehouse;
go

use DataWarehouse;
go

-- create schemas
create schema bronze;
go 

create schema silver;
go

create schema gold;
go 
