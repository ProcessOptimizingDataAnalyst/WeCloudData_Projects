
from pyspark.sql import SparkSession
import sys

date_str = sys.argv[1]


# creating spark session
# change to yarn for EMR cluster
spark = SparkSession.builder.master("yarn").appName("demo").getOrCreate()


# Read files from s3 bucket
# change to s3 bucket when running in EMR
sales_df = spark.read.option("header","true").option("delimiter",",").csv("s3://de-midterm-raw/sales_2023-07-30.csv")
inventory_df = spark.read.option("header","true").option("delimiter",",").csv("s3://de-midterm-raw/inventory_2023-07-30.csv")
product_df = spark.read.option("header","true").option("delimiter",",").csv("s3://de-midterm-raw/product_2023-07-30.csv")
store_df = spark.read.option("header","true").option("delimiter",",").csv("s3://de-midterm-raw/store_2023-07-30.csv")
calendar_df = spark.read.option("header","true").option("delimiter",",").csv("s3://de-midterm-raw/calendar_2023-07-30.csv")

# total sales quantity by week, by store and by product
sales_df.createOrReplaceTempView("sales")
inventory_df.createOrReplaceTempView("inventory")
product_df.createOrReplaceTempView("product")
store_df.createOrReplaceTempView("store")
calendar_df.createOrReplaceTempView("calendar")

df_agg_transformations = spark.sql("""
SELECT CAST(WK_NUM AS INT),
       CAST(STORE_KEY AS INT),
       CAST(PROD_KEY AS INT), 
       CAST(sum_sales_qty AS DECIMAL(18,2)),
       CAST(sum_sales_amt AS DECIMAL(18,2)),
       CAST(avg_price AS DECIMAL(18,2)),
       stock_level,
	   order_level,
       CAST(sales_cost AS DECIMAL(18,2)),
       in_stock_store_percentage,
       LOW_STOCK_LEVEL,
       potential_low_stock_level,
       no_stock_impact,
       COUNT(low_stock_level) AS low_stock_instance,
       COUNT(CASE WHEN OUT_OF_STOCK_FLG = 1 THEN OUT_OF_STOCK_FLG ELSE 0 END) AS no_stock_instance,
       SUM(inventory_on_hand_qty)/SUM(sales_qty) AS on_hand_stock_weekly_supply
FROM
(SELECT calendar.WK_NUM AS WK_NUM,
       sales.store_key AS STORE_KEY,
       sales.prod_key AS PROD_KEY,
       sales.sales_qty AS sales_qty,
       inventory.inventory_on_hand_qty AS inventory_on_hand_qty,
       inventory.OUT_OF_STOCK_FLG  AS OUT_OF_STOCK_FLG,
       sum(sales.sales_qty) as sum_sales_qty,
	   sum(sales.sales_amt) as sum_sales_amt,
       sum(sales.sales_amt)/sum(sales.sales_qty) as avg_price,
       sum(inventory.inventory_on_hand_qty) as stock_level,
       sum(inventory.inventory_on_order_qty) as order_level,
       sum(sales.sales_cost) as sales_cost,
       (1 - sum(out_of_stock_flg)/count(out_of_stock_flg)) * 100 as in_stock_store_percentage,
       sum(CASE WHEN inventory.inventory_on_hand_qty < sales.sales_qty THEN 1 ELSE 0 END) as low_stock_level,
       SUM(CASE WHEN inventory.OUT_OF_STOCK_FLG = 1 THEN sales.sales_amt - inventory.inventory_on_hand_qty ELSE 0 END) as potential_low_stock_level,
       SUM(CASE WHEN inventory.OUT_OF_STOCK_FLG = 1 THEN sales.sales_amt ELSE 0 END) as no_stock_impact     
FROM sales
INNER JOIN inventory
ON sales.prod_key = inventory.prod_key
AND 
sales.store_key = inventory.store_key
AND
CAST(sales.trans_dt AS DATE) = CAST(inventory.cal_dt AS DATE)
INNER JOIN calendar
ON CAST(inventory.cal_dt AS DATE) = CAST(calendar.cal_dt AS DATE)
GROUP BY 1,2,3,4,5,6
ORDER BY 1,2,3,4,5,6
) AS SUBQUERY
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
""")


df_agg_transformations.show()


# write the file back to s3
# change this to s3 bucket for EMR
df_agg_transformations.repartition(1).write.mode("overwrite").option("compression","gzip").parquet(f"s3://midterm-data-result-b7/date={date_str}")




