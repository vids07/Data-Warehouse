/* 
======================================================================================================================
DDL Script: Create Gold Views
======================================================================================================================
Script Purpose:
   This script creates views for the 'gold layer' in the  data warehouse.
   The gold layer represents the final dimenson and fact tables(star schema)

   Each view  performs transformations and combines data from silver layer to produce a clean, enriched and business-ready data. 

Usage:
   These views can be queried directly for analytics and reporting. 
======================================================================================================================   
*/

-- =====================================================================================================================
-- Create Dimension: gold.dim_customers
-- =====================================================================================================================

If object_id('gold.dim_customers', 'v') is not null
drop view gold.dim_customers;
go

/*
-- Step 1: Checking all the tables and ensuring the joining key
-- Always join from the master table

select *
from silver.crm_cust_info

select *
from silver.erp_cust_az12

select *
from silver.erp_loc_a101 

-- Step 2: Joining
select
cu.cst_id,
cu.cst_key,
cu.cst_firstname,
cu.cst_lastname,
cu.cst_marital_status,
cu.cst_gndr,
cu.cst_create_date,
cz.bdate,
cz.gen,
cl.cid,
cl.cntry
from silver.crm_cust_info as cu
left join silver.erp_cust_az12 as cz
on cu.cst_key = cz.cid
left join silver.erp_loc_a101 as cl
on cu.cst_key = cl.cid


-- Step 3:  Checking all the columns
-- 2 are repeated

select distinct
cu.cst_gndr,
cz.gen
from silver.crm_cust_info as cu
left join silver.erp_cust_az12 as cz
on cu.cst_key = cz.cid

-- Step 4: Asking from Experts or owner which one to consider
select *
from (
select
cu.cst_gndr,
cz.gen,
case
	when cu.cst_gndr = 'Male'   then 'Male'
	when cu.cst_gndr = 'Female' then 'Female'
	when cu.cst_gndr = 'n/a'    then isnull(cz.gen, 'n/a')
end as Gender
from silver.crm_cust_info as cu
left join silver.erp_cust_az12 as cz
on cu.cst_key = cz.cid
)t where gen is null


-- Step 5: Final query with friendly name
select
cu.cst_id as Customer_Number,
cu.cst_key as Customer_Key,
cu.cst_firstname as Fitsrname,
cu.cst_lastname as Lastname,
cu.cst_marital_status as Marital_Status,
case
	when cu.cst_gndr = 'Male'   then 'Male'
	when cu.cst_gndr = 'Female' then 'Female'
	when cu.cst_gndr = 'n/a'    then isnull(cz.gen, 'n/a')
end as Gender,
cu.cst_create_date,
cz.bdate as Birthdate,
cl.cntry as Country
from silver.crm_cust_info as cu
left join silver.erp_cust_az12 as cz
on cu.cst_key = cz.cid
left join silver.erp_loc_a101 as cl
on cu.cst_key = cl.cid

-- Step 6: Adding surrogate key
select 
row_number() over(order by cst_id) as Customer_Key,
cu.cst_id as Customer_Id,
cu.cst_key as Customer_Number,
cu.cst_firstname as Firstname,
cu.cst_lastname as Lastname,
cl.cntry as Country,
cu.cst_marital_status as Marital_Status,
case
	when cu.cst_gndr = 'Male'   then 'Male'
	when cu.cst_gndr = 'Female' then 'Female'
	when cu.cst_gndr = 'n/a'    then isnull(cz.gen, 'n/a')
end as Gender,
cz.bdate as Birthdate,
cu.cst_create_date
from silver.crm_cust_info as cu
left join silver.erp_cust_az12 as cz
on cu.cst_key = cz.cid
left join silver.erp_loc_a101 as cl
on cu.cst_key = cl.cid

-- Step 7: Ensuring no duplicates; After Joining Table Check 
select Customer_Key, count(*)
from (
  select 
row_number() over(order by cst_id) as Customer_Key,
cu.cst_id as Customer_Id,
cu.cst_key as Customer_Number,
cu.cst_firstname as Firstname,
cu.cst_lastname as Lastname,
cl.cntry as Country,
cu.cst_marital_status as Marital_Status,
case
	when cu.cst_gndr = 'Male'   then 'Male'
	when cu.cst_gndr = 'Female' then 'Female'
	when cu.cst_gndr = 'n/a'    then isnull(cz.gen, 'n/a')
	else 'n/a'
end as Gender,
cz.bdate as Birthdate,
cu.cst_create_date
from silver.crm_cust_info as cu
left join silver.erp_cust_az12 as cz
on cu.cst_key = cz.cid
left join silver.erp_loc_a101 as cl
on cu.cst_key = cl.cid
  )t 
group by Customer_Key
having count(*) > 1;
*/

-- Step 8: Gold Layer : In view not in table
create view gold.dim_customers as 
select 
row_number() over(order by cst_id) as Customer_Key,
cu.cst_id as Customer_Id,
cu.cst_key as Customer_Number,
cu.cst_firstname as Firstname,
cu.cst_lastname as Lastname,
cl.cntry as Country,
cu.cst_marital_status as Marital_Status,
case
	when cu.cst_gndr = 'Male'   then 'Male'
	when cu.cst_gndr = 'Female' then 'Female'
	when cu.cst_gndr = 'n/a'    then isnull(cz.gen, 'n/a')
	else 'n/a'
end as Gender,
cz.bdate as Birthdate,
cu.cst_create_date
from silver.crm_cust_info as cu
left join silver.erp_cust_az12 as cz
on cu.cst_key = cz.cid
left join silver.erp_loc_a101 as cl
on cu.cst_key = cl.cid
go

/*
-- Step 9: Check
select *
from gold.dim_customers

-- Step 10: Integrity Check : Ensure no null value
-- Table 1
select *
from (
select
cu.cst_id,
cz.cid
from silver.crm_cust_info as cu
left join silver.erp_cust_az12 as cz
on cu.cst_key = cz.cid
)t where cst_id is null

-- Table 2
select *
from (
select
cu.cst_id,
cl.cid
from silver.crm_cust_info as cu
left join silver.erp_loc_a101 as cl
on cu.cst_key = cl.cid
)t where cst_id is null
*/

-- =====================================================================================================================
-- Create Dimension: gold.dim_products
-- =====================================================================================================================

If object_id('gold.dim_products', 'v') is not null
drop view gold.dim_products;
go

/*
-- Step 1: Business object check
select *
from silver.crm_prd_info

select *
from silver.erp_px_cat_g1v2

-- Step 2: Joining
select
pr.prd_id,
pr.cat_id,
pr.prd_key,
pr.prd_nm,
pr.prd_cost,
pr.prd_line,
pr.prd_start_dt,
px.cat,
px.subcat,
px.maintenance
from silver.crm_prd_info as pr
left join silver.erp_px_cat_g1v2 as px
on pr.cat_id = px.id


-- Step 2: Final query with friendly name and re-arrangement
select
pr.prd_id as Product_Id,
pr.prd_key as Product_Number,
pr.prd_nm as Product_Name,
pr.cat_id as Category_Id,
px.cat as Category,
px.subcat as Subcategory,
px.maintenance as Maintenance,
pr.prd_cost as Cost,
pr.prd_line as Product_Line,
pr.prd_start_dt as Start_Date
from silver.crm_prd_info as pr
left join silver.erp_px_cat_g1v2 as px
on pr.cat_id = px.id

-- Step 3: Add the surrogate key
select
row_number() over(order by pr.prd_id) as Product_Key,
pr.prd_id as Product_Id,
pr.prd_key as Product_Number,
pr.prd_nm as Product_Name,
pr.cat_id as Category_Id,
px.cat as Category,
px.subcat as Subcategory,
px.maintenance as Maintenance,
pr.prd_cost as Cost,
pr.prd_line as Product_Line,
pr.prd_start_dt as Start_Date
from silver.crm_prd_info as pr
left join silver.erp_px_cat_g1v2 as px
on pr.cat_id = px.id
*/

-- Step 4: Create view
create view gold.dim_products as
select
row_number() over(order by pr.prd_id) as Product_Key,
pr.prd_id as Product_Id,
pr.prd_key as Product_Number,
pr.prd_nm as Product_Name,
pr.cat_id as Category_Id,
px.cat as Category,
px.subcat as Subcategory,
px.maintenance as Maintenance,
pr.prd_cost as Cost,
pr.prd_line as Product_Line,
pr.prd_start_dt as Start_Date
from silver.crm_prd_info as pr
left join silver.erp_px_cat_g1v2 as px
on pr.cat_id = px.id
go

/*
-- Step 5: Ensuring no duplicates; After Joining Table Check
select product_Key, count(*)
from (
  select 
  row_number() over(order by pr.prd_id) as Product_Key,
  pr.prd_id as Product_Id,
  pr.prd_key as Product_Number,
  pr.prd_nm as Product_Name,
  pr.cat_id as Category_Id,
  px.cat as Category,
  px.subcat as Subcategory,
  px.maintenance as Maintenance,
  pr.prd_cost as Cost,
  pr.prd_line as Product_Line,
  pr.prd_start_dt as Start_Date
  from silver.crm_prd_info as pr
  left join silver.erp_px_cat_g1v2 as px
  on pr.cat_id = px.id
  )t 
group by product_Key
having count(*) > 1;

-- Step 6: Check
select *
from gold.dim_products
*/


-- =====================================================================================================================
-- Create Dimension: gold.fact_sales
-- =====================================================================================================================

If object_id('gold.fact_sales', 'v') is not null
drop view gold.fact_sales;
go

/*
-- Step 1 : Check all the existing table 
-- And see what is joining key 
-- Always join from master table

select *
from silver.crm_sales_details

select *
from gold.fact_sales


-- Step 2: Friendly name and Re-arrangement
select 
sl.sls_ord_num as Order_Number,
sl.sls_prd_key as Product_Key,
sl.sls_cust_id as Customer_key,
sl.sls_ord_dt as Order_Date,
sl.sls_ship_dt as Shipping_Date,
sl.sls_due_dt as Due_Date,
sl.sls_sales as Sales_Amount,
sl.sls_quantity as Quantity,
sl.sls_price as Price
from silver.crm_sales_details as sl

-- Step 3: Adding Foregin Key
select 
sl.sls_ord_num as Order_Number,
pr.Product_Key,
ci.Customer_key,
sl.sls_ord_dt as Order_Date,
sl.sls_ship_dt as Shipping_Date,
sl.sls_due_dt as Due_Date,
sl.sls_sales as Sales_Amount,
sl.sls_quantity as Quantity,
sl.sls_price as Price
from silver.crm_sales_details as sl
left join gold.dim_products as pr
on sl.sls_prd_key = pr.Product_Number
left join gold.dim_customers as ci
on sl.sls_cust_id = ci.Customer_Id
*/

-- Step 4: Create view
create view gold.fact_sales as
select 
sl.sls_ord_num as Order_Number,
pr.Product_Key,
ci.Customer_key,
sl.sls_ord_dt as Order_Date,
sl.sls_ship_dt as Shipping_Date,
sl.sls_due_dt as Due_Date,
sl.sls_sales as Sales_Amount,
sl.sls_quantity as Quantity,
sl.sls_price as Price
from silver.crm_sales_details as sl
left join gold.dim_products as pr
on sl.sls_prd_key = pr.Product_Number
left join gold.dim_customers as ci
on sl.sls_cust_id = ci.Customer_Id
go

/*
-- Step 5: Ensuring no duplicates; After Joining Table Check
No NEED ORDER NUMBER CAN BE SAME THEY APPEAR WHEN A SAME CUSTOMER BOOKS PRODUCTS AT A SAME TIME - ONE TRANSACTION SO THOSE ALL PRODUCTS IN 1 CHECKOUT GET THE SAME ORDER_NUMBER
That's why order_number can never be a primary key

-- Step 6: Check
select *
from gold.fact_sales

-- Step 7: Foreign key integrity check
select *
from gold.fact_sales
where Product_Key not in (
select Product_Key
from gold.dim_products
)
*/
