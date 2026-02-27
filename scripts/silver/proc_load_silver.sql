/* 
=======================================================================================================
Stored Procedure: Load Bronze Layer (bronze -> silver)
=======================================================================================================
Script Purpose:
   This stored procedure performs the ETL(Extraction, Truncate, Load) process to populate the 'silver' schema tables from 'bronze' schema.

   It performs the following actions:
      - Truncates the silver tables before loading data.
      - Insert transformed and cleansed data from bronze into silver tables.

Parameters:
   None.
   This stored procedure does not accept any parameters or return aqny values.

Usage Example:
   EXEC silver.load_silver;
=========================================================================================================
*/



-- Stored Procedures : Silver Layer
create or alter procedure silver.load_silver as
begin
	declare @start_time datetime, @end_time datetime,
	@batch_start_time datetime, @batch_end_time datetime;
	begin try
	set @batch_start_time = getdate();
	set @batch_start_time = getdate();
	Print '===========================================================';
	Print 'Loading Silver Layer';
	Print '===========================================================';

	Print '-----------------------------------------------------------';
	Print 'Loading CRM Tables';
	Print '-----------------------------------------------------------';

	-- Silver.crm_cust_info
	set @start_time = getdate();
	Print '>> Table 1';
	Print '>> Truncating Table: Silver.crm_cust_info';
	Truncate Table silver.crm_cust_info
	Print '>> Inserting data into: Silver.crm_cust_info';
	insert into silver.crm_cust_info(
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date
	)
	select 
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date
	from (
	select 
	cst_id,
	rank() over(partition by cst_id order by cst_create_date desc) as latest,
	cst_key,
	trim(cst_firstname) as cst_firstname,
	trim(cst_lastname) as cst_lastname,
	case
		when cst_marital_status = 'm' then trim('Married')
		when cst_marital_status = 's' then trim('Single')
		else 'n/a'
	end as cst_marital_status,
	case
		when cst_gndr = 'f' then trim('Female')
		when cst_gndr = 'm' then trim('Male')
		else 'n/a'
	end as cst_gndr,
	cst_create_date
	from bronze.crm_cust_info
	)t where latest = 1 and latest is not null
	set @end_time = getdate();
	Print 'Load Duration:' +cast(datediff(second, @start_time, @end_time)as nvarchar) + 'Seconds'
	Print '>>>>>>>>>>>';



	-- Silver.crm_prd_info
	set @start_time = getdate();
	Print '>> Table 2';
	Print '>> Truncating Table: Silver.crm_prd_info';
	Truncate Table Silver.crm_prd_info
	Print '>> Inserting data into: Silver.crm_prd_info';
	insert into silver.crm_prd_info (
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
	)
	select
	prd_id,
	replace(substring(prd_key, 1, 5), '-', '_') as cat_id,
	substring(prd_key, 7, len(prd_key)) as prd_key,
	prd_nm,
	isnull(prd_cost, 0) as prd_cost,
	case
		when prd_line = 'm' then trim('Mountain')
		when prd_line = 'r' then trim('Road')
		when prd_line = 's' then trim('Other Sales')
		when prd_line = 't' then trim('Touring')
		else 'n/a'
	end as prd_line,
	least(prd_start_dt, prd_end_dt) as prd_start_dt,
	dateadd(day, -1,
	lead(least(prd_start_dt, prd_end_dt)) 
	over (partition by prd_nm order by prd_id asc)) as prd_end_dt
	from bronze.crm_prd_info
	set @end_time = getdate();
	Print 'Load Duration:' + cast(datediff(second, @start_time, @end_time)as nvarchar) + 'seconds';
	Print '>>>>>>>>>>>>>'



	-- Silver.crm_sales_details
	set @start_time = getdate();
	Print '>>Table 3';
	Print '>> Truncating table : silver.crm_sales_details';
	truncate table silver.crm_sales_details
	Print '>>Inserting into table : silver.crm_sales_details';
	insert into silver.crm_sales_details (
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_ord_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
	)
	select 
	-- Composite primary key is:(sls_ord_num, sls_prd_key)
	sls_ord_num,-- represents 1 order with multiple products
	trim(sls_prd_key) as sls_prd_key,
	sls_cust_id,
	case 
		when len(sls_ord_dt) < 8 then null
		when len(sls_ord_dt) > 8 then null
		else cast(cast(sls_ord_dt as nvarchar(8)) as date) 
	end as sls_ord_dt,
	case 
		when len(sls_ship_dt) < 8 then null
		when len(sls_ship_dt) > 8 then null
		else cast(cast(sls_ship_dt as nvarchar(8)) as date) 
	end as sls_ship_dt,
	case 
		when len(sls_due_dt) < 8 then null
		when len(sls_due_dt) > 8 then null
		else cast(cast(sls_due_dt as nvarchar(8)) as date) 
	end as sls_due_dt,
	case 
		when sls_sales != sls_quantity * sls_price 
		then sls_quantity * abs(sls_price)
		else sls_sales
	end as sls_sales,
	sls_quantity,
	case
		when sls_price is null then abs(sls_sales/nullif(sls_quantity, 0))
		when sls_price < 1 then abs(sls_sales/nullif(sls_quantity, 0))
		else sls_price
	end as sls_price	
	from bronze.crm_sales_details;
	set @end_time = getdate()
	Print 'Load Duration:' + cast(datediff(second, @start_time, @end_time)as nvarchar) + 'seconds';
	Print '>>>>>>>>>>>>>'

	set @batch_end_time = getdate();
	Print '>>>>>>>>>>>>'
	Print 'Loading CRM Tables Completed';
	Print 'Load Duration of CRM Tables:' + cast(datediff(second, @batch_start_time, @batch_end_time) as nvarchar) + 'seconds';
	Print '>>>>>>>>>>>>>'

	-- Erp Tables
	set @batch_start_time = getdate();
	Print '-----------------------------------------------------------';
	Print 'Loading ERP Tables';
	Print '-----------------------------------------------------------';


	-- Silver.erp_cust_az12
	set @start_time = getdate();
	Print '>> Erp Tables';
	Print '>> Table 4: Silver.erp_cust_az12';
	Print '>> Truncating Table : silver.erp_cust_az12';
	truncate table silver.erp_cust_az12
	Print '>> Inserting table : Silver.erp_cust_az12';
	insert into silver.erp_cust_az12 (
	cid,
	bdate,
	gen
	)
	select 
	case	
		when cid like 'NAS%' then substring(cid, 4, len(cid)) 
		else cid
	end as cid,
	case
		when bdate > getdate() then null
		else bdate
	end as bdate,
	case
		when gen is null then 'n/a'
		when gen = ' ' then 'n/a'
		when gen = 'f' then trim('Female')
		when gen = 'm' then trim('Male')
		else gen
	end as gen
	from bronze.erp_cust_az12
	set @end_time = getdate();
	Print 'Load Duration:' + cast(datediff(second, @start_time, @end_time)as nvarchar) + 'seconds';
	Print '>>>>>>>>>>>>>'



	-- silver.erp_loc_a101
	set @start_time = getdate();
	Print '>> Table 5 : silver.erp_loc_a101';
	Print '>> Truncating table: silver.erp_loc_a101';
	truncate table silver.erp_loc_a101
	Print '>> Inserting table: silver.erp_loc_a101';
	insert into silver.erp_loc_a101 (
	cid,
	cntry
	)
	select 
	case 
		when cid like '%-%' then replace(cid, '-', '')
		else cid
	end as cidd,
	case
		when cntry = 'de' then trim('Germany')
		when cntry = 'usa' then trim('United Stated')
		when cntry = 'us' then trim('United States')
		when cntry = ' ' then 'n/a'
		when cntry is null then 'n/a'
		else cntry
	end as cntry
	from bronze.erp_loc_a101;
	set @end_time = getdate();
	Print 'Load Duration:' + cast(datediff(second, @start_time, @end_time)as nvarchar) + 'seconds';
	Print '>>>>>>>>>>>>>'



	-- Silver.erp_px_g1v1
	set @start_time = getdate();
	Print '>> Table 6:silver.erp_px_cat_g1v2';
	Print '>> Truncate table : silver.erp_px_cat_g1v2';
	truncate table silver.erp_px_cat_g1v2
	Print '>> Inserting table : silver.erp_px_cat_g1v2';
	insert into silver.erp_px_cat_g1v2 (
	id,
	cat,
	subcat,
	maintenance
	)
	select *
	from bronze.erp_px_cat_g1v2
	set @start_time = getdate();
	Print 'Load Duration:' + cast(datediff(second, @start_time, @end_time)as nvarchar) + 'seconds';
	Print '>>>>>>>>>>>>>'



	set @batch_end_time = getdate();
	Print '>>>>>>>>>>>>'
	Print 'Loading ERP Tables completed';
	Print 'Load Duration of ERP Tables:' + cast(datediff(second, @batch_start_time, @batch_end_time) as nvarchar) + 'seconds';
	Print '>>>>>>>>>>>>>'

	set @batch_end_time = getdate();
	Print '>>>>>>>>>>>>'
	Print 'Loading Silver Layer is completed'
	Print 'Total Load Duration of Entire Silver Layer:' + cast(datediff(second, @batch_start_time, @batch_end_time) as nvarchar) + 'seconds';
	Print '>>>>>>>>>>>>>'
	end try
	Begin Catch
	Print '================================================================';
	Print 'Error Occured during loading bronze layer'
	Print 'Error Message' + error_message();
	Print 'Error Message' + cast(error_number() as nvarchar);
	Print 'Error Message' + cast(error_state() as nvarchar);
	Print '================================================================';
	end catch
end

exec silver.load_silver
