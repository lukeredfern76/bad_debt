<<<<<<< HEAD
--Gap Brand Split 

create or replace table `data-engineering-prod.u_will_rowe.tbl_Gap_brand_split_current` as (
>>>>>>> refs/heads/main

select distinct 
reporting_month,
Account_no, 
Payment_Method_Category

from `payg-revenue-assurance.account_gap_model.pa_revenue_reconciliation_master` as r

where 
  Reporting_Month = date_trunc(current_date(), month) - 1
