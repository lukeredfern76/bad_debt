--Gap Brand Split 
create or replace table `data-engineering-prod.u_will_rowe.tbl_Gap_brand_split_Jan24` as (

select distinct 
Account_no, 
Payment_Method_Category

from `payg-revenue-assurance.account_gap_model.pa_revenue_reconciliation_master` as r

where 
  Reporting_Month = '2024-01-01'
