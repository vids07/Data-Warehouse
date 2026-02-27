/* 
======================================================================================================================
DDL Script: Create Silver Tables
======================================================================================================================
Script Purpose:
   This script creates tables in the 'silver' schema, dropping existing tables if they already exists.
   Run this script to re-define the DDL structure of 'bronze' Tables
======================================================================================================================   
*/


if object_id('silver.crm_cust_info') is not null
   drop table silver.crm_cust_info;
go
  
create table silver.crm_cust_info(
   cst_id                         int,
   cst_key                        varchar(15),
   cst_firstname                  varchar(30),
   cst_lastname                   varchar(30),
   cst_marital_status             varchar(1),
   cst_gndr                       varchar(1),
   cst_create_date                date
);
go

if object_id('silver.crm_prd_info') is not null
   drop table silver.crm_prd_info;
go
  
   create table silver.crm_prd_info (
      prd_id                         int,
      cat_id                         nvarchar(10),
      prd_key                        nvarchar(30),
      prd_nm                         nvarchar(50),
      prd_cost                       int,
      prd_line                       nvarchar(20),
      prd_start_dt                   date,
      prd_end_dt                     date
);
go

 if object_id('silver.crm_sales_details') is not null
    drop table silver.crm_sales_details
 go
  
    create table silver.crm_sales_details (
       sls_ord_num                         nvarchar(20),
       sls_prd_key                         nvarchar(20),
       sls_cust_id                         int,
       sls_ord_dt                          date,
       sls_ship_dt                         date,
       sls_due_dt                          date,
       sls_sales                           int,
       sls_quantity                        int,
       sls_price                           int
);
go

if object_id('silver.erp_cust_az12') is not null  
   drop table silver.erp_cust_az12
 go
  
   create table silver.erp_cust_az12 (
      CID                             nvarchar(20) not null,
      BDATE                           date,
      GEN                             nvarchar(10)
);
go

if object_id('silver.erp_loc_a101') is not null  
   drop table silver.erp_loc_a101
 go
  
   create table silver.erp_loc_a101 (
      CID                            nvarchar(20) not null,
      CNTRY                          nvarchar(20)
);
go  

if object_id('silver.erp_px_cat_g1v2') is not null  
   drop table silver.erp_px_cat_g1v2
go
  
  create table silver.erp_px_cat_g1v2 (
     ID                                nvarchar(20) not null,
     CAT                               nvarchar(20),
     SUBCAT                            nvarchar(40),
     MAINTENANCE                       nvarchar(5)
);
go  
  
