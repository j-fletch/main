# Databricks notebook source
## Declining Lab 

## Original Value Hypothesis:
# Goal of this play is to capture customers who are on their way to becoming inactivate and re-activate their sales activity. 
# We know gaining new customers is far more expensive than keeping existing customers and this play is inteded to proactivaly retain business.

# Analysis to additionally seek to control for seasonal YOY changes

# Original business request provided below:

"""
*Must Have*
1. Model provides recommendations for accounts whose non-equipment & instrument spend is declining (following the below criteria as a starting place; can adjust after review if needed):
1a. Last 365 D spend is <50% of preceding 365 D spend
1b. AND Last 90 D spend is <50% of preceding 90 D spend
1c. AND preceding 365 D spend is between $5,000 and $30,000
2. Model provides recommendations at 9digit level
3. Model excludes Equipment & Instrument spend by excluding CMT2 "Equipment & Instruments" as part of model input/ouput
4. Model provides output to be utilized for commercial channel
5. Model output includes "Last 365 day NEI spend" and "Last 90 day NEI spend" per account
6. Ability to add "Last 365 day NEI spend" and "Last 90 day NEI spend" per account as talking points/merge fields
7. Model is refreshed monthly
8. Existing revenue attribution logic for business development is applied (see analytics impact assessment in comments)
9. This model will be broken out as a separate play in RSD operational reporting (see analytics impact assessment in comments)
10. Revenue is captured in the executive dashboard

Nice to Have
1. Model uses TOG spend as part of model (is TOG spend also declining?)
2. Model uses "Generalist" level as part of model (generalist = group of 9digits within a TOG based on territory)
3. Model uses other model output to enhance model (ex: LSG retention model)
4. Model provides recommendation at 9digit AND CMT level (either CMT2 or CMT3)
"""


# COMMAND ----------

## accessing CoE data lake

# account table:      ccgds.d_acct, ccgds.d_sf_acct (SFDC account)
# product table:      ccgds.d_prod
# transaction table:  ccgds.f_sls_invc_model_vw

## Dependencies:
# connect2Databricks 2.0 +
# Python 3.7+ 
# Databricks Runtime 5.4+ Conda

# COMMAND ----------

# MAGIC %run /DS_CCG/RSD_COE/Data_Scientists/CommonDataFunctions

# COMMAND ----------

def get_environment():
  division = 'RSD'
  environment = 'TST'
  return division,environment

# COMMAND ----------

from connect2Databricks.read2Databricks import redshift_ccg_read

# COMMAND ----------

# importing data: ccgds.d_acct, ccgds.f_sls_invc, and ccgds.d_prod
# Only importing subset of columns to descrease import time.

# setting enviornment based on prior command. 
division,environment = get_environment()

### account data
acct = redshift_ccg_read(table = """SELECT
                                      co_cd,
                                      acct_nbr,
                                      acct_nm, 
                                      account_status,
                                      commercial_grp_nm,
                                      top_of_grp_cd,
                                      top_of_grp_nm,
                                      prog_acct_ind,
                                      cust_seg_nm,
                                      acct_key,
                                      sf_acct_id
                                    FROM ccgds.d_acct
                                    WHERE division_code = 'RSD' """,
                                    env = environment, cache = True)   
       
### invoice data   
# ship_to_cust_key used to merge to d_acct
# ltm_period:
  # 2 = most recent year
  # 1 = 13-24 months prior
  # 0 = 25 - 36 months prior
# TO DO: Add in contact level data from invoice history.
ih = redshift_ccg_read(table = """SELECT
                                    ship_to_cust_key,
                                    ord_nbr,
                                    invc_nbr
                                    invc_dt_key,
                                    to_date(CAST(invc_dt_key AS VARCHAR(8)),'yyyyMMdd') AS invc_dt_dt,
                                    ltm_period,
                                    prod_key AS prod_key_ih,
                                    invc_qty,
                                    net_sls_scur,  
                                    ord_entry_src_nm,
                                    ord_entry_type
                                  FROM ccgds.f_sls_invc  """,
                                  env = environment, cache = True)

### product data
# filtering out CMT2 equipment/instruments and services.
prod = redshift_ccg_read(table = """SELECT 
                                      prod_key,
                                      prod_nbr,
                                      cmt_nm_lvl_2_cd,
                                      cmt_nm_lvl_2_nm,
                                      std_unit_cost
                                   FROM ccgds.d_prod
                                   WHERE cmt_nm_lvl_2_nm NOT IN ('Equipment and Instruments','Services') """,
                                   env = environment, cache = True)         


# COMMAND ----------

# create temp view of imported data; to faciliate access via %sql and %r

acct.createOrReplaceTempView("acct")
prod.createOrReplaceTempView("prod")
ih.createOrReplaceTempView("ih")

# COMMAND ----------

display(acct)

# COMMAND ----------

display(prod)

# COMMAND ----------

display(ih)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --- Merging datasets 
# MAGIC   SELECT *
# MAGIC   FROM acct a                       
# MAGIC     INNER JOIN ih f        
# MAGIC       ON f.ship_to_cust_key = a.acct_key  
# MAGIC     INNER JOIN prod p            
# MAGIC       ON p.prod_key = f.prod_key
# MAGIC       

# COMMAND ----------

# MAGIC %sql   
# MAGIC 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW ih_YOY_spend AS
# MAGIC SELECT
# MAGIC   ship_to_cust_key,
# MAGIC   ltm_period,
# MAGIC   SUM(net_sls_scur) as sales
# MAGIC FROM ih
# MAGIC WHERE ltm_period IN ('2', '1')
# MAGIC GROUP BY
# MAGIC   ship_to_cust_key,
# MAGIC   ltm_period;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW prod_no_equip AS
# MAGIC SELECT
# MAGIC   prod_key,
# MAGIC   cmt_nm_lvl_2_nm
# MAGIC FROM prod
# MAGIC WHERE cmt_nm_lvl_2_nm NOT IN ('Equipment and Instruments', 'Services');

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW ih_three_month_comparison_spend AS
# MAGIC SELECT
# MAGIC   ship_to_cust_key,
# MAGIC   SUM(net_sls_scur) as sales,
# MAGIC CASE 
# MAGIC   WHEN DATEDIFF(CURRENT_DATE(), invc_dt_dt) <= 90  THEN 'x1_90'
# MAGIC   WHEN DATEDIFF(CURRENT_DATE(), invc_dt_dt) <=180  THEN 'x91_180'
# MAGIC   ELSE 'NA'
# MAGIC   END AS purchase_window
# MAGIC FROM ih
# MAGIC GROUP BY 
# MAGIC     purchase_window,
# MAGIC     ship_to_cust_key;
# MAGIC   

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW ih_three_month_comparison_spend AS
# MAGIC SELECT
# MAGIC   ship_to_cust_key,
# MAGIC   invc_dt_dt,
# MAGIC   DATEDIFF(CURRENT_DATE(), invc_dt_dt) as diff
# MAGIC FROM ih;
# MAGIC   

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * 
# MAGIC FROM ih_three_month_comparison_spend;

# COMMAND ----------

# MAGIC %
# MAGIC 
# MAGIC test -> ih_three_month_comparison_spend %>%
# MAGIC               

# COMMAND ----------

SELECT 
      f.invc_yr,
      a.acct_key,
      a.acct_nbr, 
      a.acct_nm,
      a.sf_acct_id,
      f.ship_to_cust_key, 
      SUM(f.net_sls_scur) AS sales, 
      COUNT(DISTINCT f.ord_nbr) AS order_ct
    FROM acct a                       
      INNER JOIN invoice f        
        ON f.ship_to_cust_key = a.acct_key  
      INNER JOIN prod p            
        ON p.prod_key = f.prod_key
    GROUP BY
      f.invc_yr,
      a.acct_nbr,
      a.sf_acct_id,
      a.acct_nm,
      a.acct_key,
      f.ship_to_cust_key;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC from f_sls_invc
# MAGIC WHERE ord_nbr = '01322306343';

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC from d_prod
# MAGIC WHERE prod_key = '151551';

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --- d_acct is subdset above; thus, this only returns values which were imported abvove. 
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM d_acct
# MAGIC WHERE 
# MAGIC --acct_nbr = '3250246'
# MAGIC   co_cd = 'US'
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --- merging account and invoice data. 
# MAGIC --- deriving sales by year fields
# MAGIC 
# MAGIC  SELECT 
# MAGIC       f.invc_yr,
# MAGIC       a.acct_key,
# MAGIC       a.acct_nbr, 
# MAGIC       a.acct_nm,
# MAGIC       a.sf_acct_id,
# MAGIC       a.prog_acct_ind,
# MAGIC       f.ship_to_cust_key, 
# MAGIC       SUM(f.net_sls_scur) AS sales, 
# MAGIC       COUNT(DISTINCT f.ord_nbr) AS order_ct  --- invc_nbr is not a unique value, so using ord_nbr instead.
# MAGIC     FROM d_acct a                       
# MAGIC       INNER JOIN f_sls_invc f        
# MAGIC         ON f.ship_to_cust_key = a.acct_key  
# MAGIC     GROUP BY
# MAGIC       f.invc_yr,
# MAGIC       a.prog_acct_ind,
# MAGIC       a.acct_nbr,
# MAGIC       a.sf_acct_id,
# MAGIC       a.acct_nm,
# MAGIC       a.acct_key,
# MAGIC       f.ship_to_cust_key;
# MAGIC       
# MAGIC                  
# MAGIC                  

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- Industrial Play
# MAGIC 
# MAGIC WITH acct AS( 
# MAGIC     SELECT 
# MAGIC       acct_nbr,
# MAGIC       co_cd,
# MAGIC       acct_nm,
# MAGIC       acct_key,
# MAGIC       sf_acct_id,
# MAGIC       cust_seg_nm,
# MAGIC       commercial_grp_nm,
# MAGIC       prog_acct_ind,
# MAGIC       cust_seg_nm
# MAGIC     FROM d_acct
# MAGIC     WHERE 
# MAGIC       commercial_grp_nm  =  'Inside' AND         -- also found in d_sls_terr (no join key)
# MAGIC       prog_acct_ind      =  'N'      AND         -- exclude program accounts
# MAGIC       cust_seg_nm       IN ('Food/Agriculture',  -- US-only  
# MAGIC                             'Chemical',
# MAGIC                             'Dealer / Small Business', -- US-only
# MAGIC                             'Electronics',
# MAGIC                             'Environmental',    -- US-only
# MAGIC                             'Industrial',
# MAGIC                             'Medical Device',   -- US-only
# MAGIC                             'Other')
# MAGIC           ), 
# MAGIC            
# MAGIC    invoice AS(
# MAGIC     SELECT 
# MAGIC       ship_to_cust_key,      
# MAGIC       prod_key,
# MAGIC       invc_yr,
# MAGIC       ord_nbr,
# MAGIC       net_sls_scur
# MAGIC     FROM f_sls_invc
# MAGIC    --- WHERE invc_yr IN ('2016', '2017', '2018', '2019') 
# MAGIC        
# MAGIC          ),
# MAGIC   
# MAGIC   prod AS( 
# MAGIC     SELECT 
# MAGIC       prod_key,
# MAGIC       cmt_nm_lvl_2_nm
# MAGIC     FROM d_prod
# MAGIC     WHERE cmt_nm_lvl_2_nm NOT IN ('Equipment and Instruments',
# MAGIC                                   'Services')
# MAGIC           )
# MAGIC 
# MAGIC   SELECT 
# MAGIC       f.invc_yr,
# MAGIC       a.acct_key,
# MAGIC       a.acct_nbr, 
# MAGIC       a.acct_nm,
# MAGIC       a.sf_acct_id,
# MAGIC       f.ship_to_cust_key, 
# MAGIC       SUM(f.net_sls_scur) AS sales, 
# MAGIC       COUNT(DISTINCT f.ord_nbr) AS order_ct
# MAGIC     FROM acct a                       
# MAGIC       INNER JOIN invoice f        
# MAGIC         ON f.ship_to_cust_key = a.acct_key  
# MAGIC       INNER JOIN prod p            
# MAGIC         ON p.prod_key = f.prod_key
# MAGIC     GROUP BY
# MAGIC       f.invc_yr,
# MAGIC       a.acct_nbr,
# MAGIC       a.sf_acct_id,
# MAGIC       a.acct_nm,
# MAGIC       a.acct_key,
# MAGIC       f.ship_to_cust_key;
# MAGIC       
# MAGIC       

# COMMAND ----------

# Placeholder

# COMMAND ----------

# MAGIC %md
# MAGIC Let's break that down line by line:
# MAGIC 
# MAGIC `df_sales_with_acct.alias('ih')\` - This gives us a "nickname" for our Dataframe that we can use for the remainder of this code block.<br>
# MAGIC `.filter((col('ih.invc_dt_key') >= 20190101)&(col('ih.invc_dt_key') >= 20190131))\` - This filters our Dataframe for sales occurring in January 2019.<br>
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Note we can have multiple filter conditions, joined with a boolean operator.<br>
# MAGIC `.groupby('ship_to_cust_key')\` - This groups the data to the acct_key level, similar to GROUP BY in SQL.<br>
# MAGIC `.withColumnRenamed('SUM(ext_sls_pmar_amt)','sales_january')\` - This renames the automatically named sum column to something more "output-friendly".<br>
# MAGIC `.orderBy('sales_january',ascending=False)\` - Orders the Dataframe by our aggregated sales column, in descending order.<br>
# MAGIC `.head(10)` - Returns only the top 10 rows.

# COMMAND ----------




# COMMAND ----------

# You can use a redshift SQL query when you pull data from data lake using redshift_cdw_read or redshift_ccg_read.
d_prod = redshift_ccg_read(table = 'SELECT prod_key, cmt_nm_lvl_2_nm, std_uom_list_prc FROM ccgds.d_prod WHERE cmt_nm_lvl_2_nm NOT IN ('Equipment and Instruments', 'Services'),
                           env = 'PRD',
                           cache=True)

# COMMAND ----------

# You can use a redshift SQL query when you pull data from data lake using redshift_cdw_read or redshift_ccg_read.
df_sales = redshift_cdw_read(table = 'SELECT * FROM cdwds.lsg_f_sls_invc WHERE invc_dt_key >= 20190101', 
                             env = 'PRD',
                             cache=True)
display(df_sales)

# COMMAND ----------

##df = spark.table("d_acct")

# COMMAND ----------

# product data
d_prod = redshift_ccg_read(table = """SELECT 
                                          prod_key,
                                          cmt_nm_lvl_2_nm,
                                          std_uom_list_prc
                                      FROM  ccgds.d_prod""",
                           env   = 'PRD',
                           cache = True)         

# to eliminate large lab equipment line items.
# WHERE cmt_nm_lvl_2_nm NOT IN ('Equipment and Instruments','Services')

d_prod.createOrReplaceTempView("d_prod")
