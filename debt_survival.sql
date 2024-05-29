 -- Stage 3: Debt Survival
--test
CREATE TEMP TABLE Dates AS -- Create a list of the last 12 month ends as this is needed to create an average survival curve using the last 12 month's data.
SELECT
    Calendar_Date as Month_End
    --,
    --date_trunc(Calendar_Date,month) as Month_Start
FROM `data-engineering-prod.reference.tbl_Date`
WHERE Calendar_Date between '2023-04-30' and '2024-04-30'
    and Is_Month_End is true
;
-----------------------------------------------
with Dates2 as (
select d1.Month_End, d2.Month_End as Following_Month_Ends

 from Dates as d1
inner join Dates as d2 on d2.Month_End > d1.Month_End
),
-----------------------------------------------
-- The first section of code calculates any negative rebilling. It creates a view of all Orion statements then identifies the most recent statement and the statement just before 12 months ago. Next, it joins these to the debtor listing from 12 months ago. Next, it identifies all consumption transactions and joins these where the effectiveAt date is older than the statements from 12 months ago but the transaction timestamp is after the invoice date from 12 months ago. Where the sum of these is negative then we count this as negative rebilling.
-----------------------------------------------
Statements as (
select distinct
    d.Month_End,
    B.accountid,
    cast(B.metadata.createdAt as datetime) as Invoice_Created_Date,
    --B.CommDate as Invoice_Date,
    --B.startDate,
    B.endDate,
    --B.closingBalanceInPence/100 as Statement_Closing_Balance,
    'Live' as statement_type

 from `data-engineering-prod.landing_bast_secure.bast_account_billed_v1` B
cross join Dates as d

 UNION ALL

 select distinct
  d.Month_End,
  fs.accountId,
  cast(createdAt as datetime) as Invoice_Created_Date,
  --cast(createdAt as date) as Invoice_Date,
  --cast('1900-01-01' as date) as startDate,
  cast('2099-12-31' as date) as endDate,
  --closingBalance/100 as Statement_Closing_Balance,
  'Final' as statement_type

 from `data-engineering-prod.u_will_rowe.v_Final_Statements` as fs
cross join Dates as d

 where statementArchivedAtTimestamp is null
    or cast(statementArchivedAtTimestamp as date) > d.Month_End

 QUALIFY ROW_NUMBER() OVER (PARTITION BY Month_End, accountId ORDER BY createdAt DESC) = 1 --take most recent final bill
),
--------------------------------------------------------
max_statement1 as ( -- statement just before 12 months ago
select
  s.Month_End,
  s.accountId,
  s.Invoice_Created_Date,
  s.endDate,
  s.statement_type

 from Statements as s
  inner join (
              select
                Month_End,
                accountid,
                max(Invoice_Created_Date) as max_date
             
              from Statements
             
              where Invoice_Created_Date <= Month_End
             
              group by 1,2
              ) as m on m.accountid = s.accountid and m.max_date = s.Invoice_Created_Date and m.Month_End = s.Month_End
),
--------------------------------------------------------
max_statement2 as ( -- most recent statement
select
  d.Month_End,
  d.Following_Month_Ends,
  s.accountId,
  s.Invoice_Created_Date,
  s.endDate,
  s.statement_type

 from Statements as s
  inner join Dates2 as d on d.Month_End = s.Month_End
  inner join (
              select
                d.Month_End,
                d.Following_Month_Ends,
                accountid,
                max(Invoice_Created_Date) as max_date
             
              from Statements as s
                inner join Dates2 as d on d.Month_End = s.Month_End
             
              where Invoice_Created_Date <= Following_Month_Ends
             
              group by 1,2,3
              ) as m on m.accountid = s.accountid and m.max_date = s.Invoice_Created_Date and m.Month_End = s.Month_End and m.Following_Month_Ends = d.Following_Month_Ends
),
--------------------------------------------------------
debt_with_last_statement as (
select
  d.Account_No,
  d2.Month_End,
  d2.Following_Month_Ends,
  s1.Invoice_Created_Date as Invoice_Created_Date1,--statement just before 12 months ago
  s1.endDate as endDate1,
  s1.statement_type as statement_type1,
  s2.Invoice_Created_Date as Invoice_Created_Date2,--most recent statement
  s2.endDate as endDate2,
  s2.statement_type as statement_type2

 from `u_will_rowe.tbl_BD_12Months_Apr24` as d
  inner join Dates2 as d2 on d2.Month_End = d.Month_End
  inner join max_statement1 as s1 on s1.accountid = d.Account_No and s1.Month_End = d.Month_End
  inner join max_statement2 as s2 on s2.accountid = d.Account_No and s2.Month_End = d.Month_End and d2.Following_Month_Ends = s2.Following_Month_Ends

 --where Amount_Excl_VAT > 0
),
--------------------------------------------------------
Transactions as (
select
    accountid,
    effectiveAt,
    transactionTimeStamp,
    transactionType,
    netTransactionAmountInPence/100 as Transaction_Charges_Net

 from
    `kaluza-analytics-prod.reporting_bal.billable_transactions_v1_presentation_layer` PL
    LEFT OUTER JOIN `data-engineering-prod.product_orion_secure.bal_report_transaction_type_v1` TRANS ON PL.transactionType = TRANS.transaction_type

 where
  transactionType in ('consumption_charge','standing_charge','consumption_correction')
  --and effectiveAt <= (select month_end from dates)
  --and cast(transactionTimeStamp as datetime) <= date_add((select month_end from dates),interval 1 year)
),
--------------------------------------------------------
combined as (
select *

 from debt_with_last_statement as d
  inner join Transactions as t on t.accountid = d.account_no and t.effectiveAt <= d.endDate1 and cast(t.transactionTimeStamp as datetime) > d.Invoice_Created_Date1 and t.effectiveAt <= d.endDate2 and cast(t.transactionTimeStamp as datetime) < d.Invoice_Created_Date2
),
--------------------------------------------------------
negative_rebills as (
select
   Month_End,
   Following_Month_Ends,
   account_no,
   sum(Transaction_Charges_Net) as Amount

 from combined as c

 group by 1,2,3

 having Amount < 0
),
------------------------------------------
debt_reduction_excl_EBSS as (
select
  account_no,
  transaction_date,
  sum(Amount) as Amount

 from (
      select
        accountId as account_no,
        effectiveAt as transaction_date,
        netTransactionAmountInPence/100 as Amount,

       from `kaluza-analytics-prod.reporting_bal.billable_transactions_v1_presentation_layer` PL

                      where
        (PL.balanceCategoryAffected in ('Payments','Discounts') or PL.transactionType = 'dd_payment_late_failure_settled')
        and PL.transactionType not like ('%write_off%')
        and PL.transactionType not like ('WO_%')
        and PL.transactionType not like '%_Fund'
        and PL.transactionType not like '%_fund'
        and PL.transactionType not like '%EBSS%'
        and PL.transactionType <> 'consumption_correction'
        and effectiveAt between (select min(Month_End) from Dates) and (select max(Month_End) from Dates)

        
        union all

        
      select
        account_no,
        transaction_date,
        Transaction_Amount_Inc_Vat,

                      from `data-engineering-prod.reporting_finance.v_Transaction` as t
            inner join `data-engineering-prod.u_will_rowe.Transaction_Type_Code_Categories` TCC on TCC.Transaction_Type_Code = T.Transaction_Type_Code and TCC.Transaction_Category in ('Payment','WHD','GSOP - Compliance','GSOP - OFF','Goodwill','Ovo Interest Reward','Failure to supply','Misdirected PAYG Cash','V2G export credit','Discount','Incentives')
     
      where
        billing_system = 'GENTRACK'
        and transaction_date between (select min(Month_End) from Dates) and (select max(Month_End) from Dates)
)
group by 1,2
),
--------------------------------------------------
-- Energy Bill Support Scheme EBSS gives credits of £66 per month to all customers from Oct 22 - Mar 23. For DD customers these credits are refunded shortly afterwards. For On Demand customers the credit is left on the account to pay for bills.
EBSS as (
select
  t.account_no,
  t.transaction_date,
  SUM(t.Amount) as Amount

 from reporting_crm.v_Account_History as a
    inner join
            (
              select
                accountId as account_no,
                effectiveAt as transaction_date,
                netTransactionAmountInPence/100 as Amount
              from `kaluza-analytics-prod.reporting_bal.billable_transactions_v1_presentation_layer` PL
              where transactionType = 'EBSS_C'
              union all
              select
                account_no,
                transaction_date,
                Transaction_Amount_inc_VAT
              from reporting_finance.v_Transaction as t
              where t.transaction_type_code = 'EBSS_C'
                and billing_system <> 'ORION'
            ) as t on t.account_no = a.account_no and a.snapshot_date = t.transaction_date

                where
  a.Snapshot_Date between (select min(Month_End) from Dates) and (select max(Month_End) from Dates)
  and ifnull(a.DD_Status,'No DD') not like 'Active%'

                group by
  account_no,
  transaction_date
),
----------------------------------------------
pay as (
select
  account_no,
  transaction_date,
  sum(Amount) as Amount

                from (
    select account_no, transaction_date, Amount/1.05 as Amount from debt_reduction_excl_EBSS
    union all
    select account_no, transaction_date, Amount/1.05 as Amount from EBSS
    -- union all
    -- select account_no, Amount from negative_rebills
    )

 group by 1,2
),
--------------------------------------------------
Debt_and_Payments as (
select
  d2.Month_End,
  d2.Following_Month_Ends,
  SD.Account_No,
  SD.Ledger_Segment,
  Debt_Age,
  SD.Amount_Excl_VAT as Starting_Debt,
  n.amount as Rebills,
  sum(p.amount) as Total_Payments
 
from `u_will_rowe.tbl_BD_12Months_Apr24` SD
  inner join Dates2 as d2 on d2.Month_End = SD.Month_End
  left join pay p on p.Account_No = SD.Account_No and p.transaction_date between date_add(d2.Month_End, interval 1 day) and d2.Following_Month_Ends
  left join negative_rebills as n on n.account_no = SD.account_no and n.Month_End = d2.Month_End and n.Following_Month_Ends = d2.Following_Month_Ends

 --where Amount_Excl_VAT > 0

 group by 1,2,3,4,5,6,7
),
------------------------------------------------
Debt_Survival as (
select
  *,
  case
    when Starting_Debt + ifnull(Total_Payments,0) + ifnull(Rebills,0) > Starting_Debt then Starting_Debt
    when Starting_Debt + ifnull(Total_Payments,0) + ifnull(Rebills,0) < 0 then 0
    else Starting_Debt + ifnull(Total_Payments,0) + ifnull(Rebills,0)
  end as Surviving_Debt_From_Payments
 
from Debt_and_Payments as DS
)
--,
-----------------------------------------
--survival_rates as (
select
Month_End,
Following_Month_Ends,
date_diff(Following_Month_Ends,Month_End,Month) as Month_Count,
Ledger_Segment,
Debt_Age,
sum(Starting_Debt) as Starting_Debt,
sum(Surviving_Debt_From_Payments)  as Surviving_Debt,
sum(Surviving_Debt_From_Payments) / sum(Starting_Debt) as Debt_Survival_Rate_12M

 from Debt_Survival as DS

 group by 1,2,3,4,5
--)
--,
------------------------------------------
--survival_rates2 as (
-- select
--   SR.*,
--   case
--     when left(SR.Debt_Age,1) in ('i') then SR.Debt_Survival_Rate_12M
--     when left(SR.Debt_Age,1) in ('h') then SR.Debt_Survival_Rate_12M * SR24.Debt_Survival_Rate_12M
--     else SR.Debt_Survival_Rate_12M * SR12.Debt_Survival_Rate_12M
--   end as Debt_Survival_Rate_24M

 --  from survival_rates  SR
--   left join survival_rates SR12 on SR.Ledger_Segment = SR12.Ledger_Segment and left(SR12.Debt_Age,1) = 'h' and SR.Month_End = SR12.Month_End
--   left join survival_rates SR24 on SR.Ledger_Segment = SR24.Ledger_Segment and left(SR24.Debt_Age,1) = 'i' and SR.Month_End = SR24.Month_End
-- --),

--test2
------------------------------------------
-- linreg as (
-- SELECT Bucket,
--        SLOPE,
--        (SUM_OF_Y - SLOPE * SUM_OF_X) / N AS INTERCEPT,
--        CORRELATION
-- FROM (
--     SELECT Bucket,
--            N,
--            SUM_OF_X,
--            SUM_OF_Y,
--            CORRELATION * STDDEV_OF_Y / STDDEV_OF_X AS SLOPE,
--            CORRELATION
--     FROM (
--         SELECT Bucket,
--                COUNT(*) AS N,
--                SUM(X) AS SUM_OF_X,
--                SUM(Y) AS SUM_OF_Y,
--                STDDEV_POP(X) AS STDDEV_OF_X,
--                STDDEV_POP(Y) AS STDDEV_OF_Y,
--                CORR(X,Y) AS CORRELATION
--         FROM (SELECT Ledger_Segment AS Bucket,
--                      Category_No AS X,
--                      Debt_Survival_Rate_24M AS Y
--               FROM survival_rates2
--               WHERE Category_No <= 12
--               )
--         WHERE Bucket IS NOT NULL AND
--               X IS NOT NULL AND
--               Y IS NOT NULL
--         GROUP BY Bucket))
-- ),
------------------------------------------
-- Survival_rates_with_lin_reg as (
-- select
-- SR.Ledger_Segment,
-- SR.Debt_Age,
-- SR.Starting_Debt,
-- SR.Surviving_Debt,
-- SR.Debt_Survival_Rate_12M,
-- SR.Debt_Survival_Rate_24M,
-- SR.Category_No,
-- lr.slope as Slope,
-- lr.intercept as Intercept,
-- SR.Category_no * lr.slope + lr.intercept as  Modelled_Survival_Rate24M

  --         from survival_rates2 as SR
-- left join linreg as lr on SR.Ledger_Segment = lr.Bucket
-- )
------------------------------------------
--------------------------------------------
-- select
--   Ledger_Segment,
--   Debt_Age,
--   Starting_Debt,
--   Surviving_Debt,
--   Debt_Survival_Rate_12M,
--   Debt_Survival_Rate_24M,
--   Modelled_Survival_Rate24M,
--   case
--        when Category_No >= 13 then Debt_Survival_Rate_24M
--        when Modelled_Survival_Rate24M < 0 then Debt_Survival_Rate_24M
--        when Modelled_Survival_Rate24M > 1 then 1
--        else Modelled_Survival_Rate24M
--   end as Provision_Rate

  --  from Survival_rates_with_lin_reg
