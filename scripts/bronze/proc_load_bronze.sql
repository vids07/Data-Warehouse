/* 
=======================================================================================================
Stored Procedure: Load Bronze Layer (Source -> bronze)
=======================================================================================================
Script Purpose:
   This stored procedure loads data into the 'bronze' schema from external CSV files.
   It performs the following actions:
      - Truncates the bronze tables before loading data.
      - Uses the 'bulk insert' command to load data from CSV files to bronze tables.

Parameters:
   None.
   This stored procedure does not accept any parameters or return aqny values.

Usage Example:
   EXEC bronze.load_bronze;
=========================================================================================================
*/

create or alter procedure bronze.load_bronze as
begin
    declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime;
    begin try
    set @batch_start_time = getdate();
        print '======================================================';
        print 'Bronze Layer';
        print '======================================================';

        print '======================================================';
        print 'Loading CRM Tables';
        print '======================================================';

        set @start_time = getdate();
        print '------------------------------------------------------';
        print 'Cust_info table';
        print '------------------------------------------------------';

        print 'Step 1 : Empty the table (Truncate table: bronze.crm_cust_info)';
        truncate table bronze.crm_cust_info

        print 'Step 2 : Inserting the data into: bronze.crm_cust_info';
        bulk insert bronze.crm_cust_info 
        from 'C:\Users\USER\Desktop\SQL\sql-projects\dwh\datasets\source_crm\cust_info.csv'
        with (
             firstrow =2,
             fieldterminator =',',
             tablock
             );	
        set @end_time = getdate();
        print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'second';
        print '------------------';
        -- View the table
        select
        count(*)
        from bronze.crm_cust_info

        set @start_time = getdate();
        print '------------------------------------------------------';
        print 'Prd_info table';
        print '------------------------------------------------------';
        print 'Step 1 : Empty the table (Truncate table: bronze.crm_prd_info)';
        truncate table bronze.crm_prd_info

        print 'Step 2 : Inserting the data into: bronze.crm_prd_info';
        bulk insert bronze.crm_prd_info
        from 'C:\Users\USER\Desktop\SQL\sql-projects\dwh\datasets\source_crm\prd_info.csv'
        with (
             firstrow = 2,
             fieldterminator = ',',
             tablock
             );
        set @end_time = getdate();
        print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'second';
        print '------------------';
        
        -- View
        select *
        from bronze.crm_prd_info 

        select count(*)
        from bronze.crm_prd_info

        set @start_time = getdate();
        print '------------------------------------------------------';
        print 'Sales Details table';
        print '------------------------------------------------------';
        print 'Step 1 : Empty the table (Truncate table: bronze.crm_sales_details)';
        truncate table bronze.crm_sales_details

        print 'Step 2 : Inserting the data into: bronze.crm_sales_details';
        bulk insert bronze.crm_sales_details
        from 'C:\Users\USER\Desktop\SQL\sql-projects\dwh\datasets\source_crm\sales_details.csv'
        with (
              firstrow = 2,
              fieldterminator = ',',
              tablock
             );
        set @end_time = getdate();
        print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'second';
        print '------------------';
        -- View 
        select *
        from bronze.crm_sales_details

        select count(*)
        from bronze.crm_sales_details

        print '======================================================';
        print 'Loading ERM Tables';
        print '======================================================';

        set @start_time = getdate();
        print '------------------------------------------------------';
        print 'Cust_Az table';
        print '------------------------------------------------------';
        print 'Step 1 : Empty the table (Truncate table: bronze.erp_cust_az12)';
        truncate table bronze.erp_cust_az12

        print 'Step 2 : Inserting the data into: bronze.erp_cust_az12';
        bulk insert bronze.erp_cust_az12 
        from 'C:\Users\USER\Desktop\SQL\sql-projects\dwh\datasets\source_erp\cust_az12.csv'
        with (
             firstrow = 2,
             fieldterminator = ',',
             tablock
             );
        set @end_time = getdate();
        print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'second';
        print '------------------';

        -- View
        select *
        from bronze.erp_cust_az12

        select count(*)
        from bronze.erp_cust_az12

        set @start_time = getdate();
        print '------------------------------------------------------';
        print 'loc table';
        print '------------------------------------------------------';
        print 'Step 1 : Empty the table (Truncate table: bronze.erp_loc_a101)';
        truncate table bronze.erp_loc_a101

        print 'Step 2 : Inserting the data into: bronze.erp_loc_a101';
        bulk insert bronze.erp_loc_a101 
        from 'C:\Users\USER\Desktop\SQL\sql-projects\dwh\datasets\source_erp\loc_a101.csv'
        with (
              firstrow = 2,
              fieldterminator = ',',
              tablock
             );
        set @end_time = getdate();
        print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'second';
        print '------------------';
        -- View 
        select *
        from bronze.erp_loc_a101

        select count(*)
        from bronze.erp_loc_a101

        set @start_time = getdate();
        print '------------------------------------------------------';
        print 'px_cat table';
        print '------------------------------------------------------';
        print 'Step 1 : Empty the table (Truncate table: bronze.erp_px_cat_g1v2)';
        truncate table bronze.erp_px_cat_g1v2

        print 'Step 2 : Inserting the data into: bronze.erp_px_cat_g1v2';
        bulk insert bronze.erp_px_cat_g1v2 
        from 'C:\Users\USER\Desktop\SQL\sql-projects\dwh\datasets\source_erp\px_cat_g1v2.csv'
        with (
              firstrow = 2,
              fieldterminator = ',',
              tablock
             );
        set @end_time = getdate();
        print '>> Load Duration:' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'second';
        print '------------------';

        -- View 
        select *
        from bronze.erp_px_cat_g1v2

        select count(*)
        from bronze.erp_px_cat_g1v2

    set @batch_end_time = getdate();
    print '------------------'
    print 'loading bronze layer is completed'
    print '>> Load Duration of bronze table: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
    print '------------------'
    end try

    begin catch
     print '======================================================';
     print 'Error Occured during loading Bronze layer';
     print 'Error Message' + Error_message();
     print 'Error Message' + cast (Error_number() as nvarchar);
     print 'Error Message' + cast (Error_state() as nvarchar);
     print '======================================================';
    end catch
end;



