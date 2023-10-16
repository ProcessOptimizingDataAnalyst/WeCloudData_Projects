--data warehouse + database + schema creations

CREATE OR REPLACE WAREHOUSE MIDTERM_WH with warehouse_size = 'X-SMALL' AUTO_SUSPEND = 60;
USE WAREHOUSE MIDTERM_WH;

CREATE OR REPLACE DATABASE MIDTERM_DB;
CREATE OR REPLACE SCHEMA RAW;

USE DATABASE MIDTERM_DB;
USE SCHEMA RAW;


---Create a stage table using data provided by WCD

create or replace file format csv_comma_skip1_format
type = 'CSV'
field_delimiter = ','
skip_header = 1;

create or replace stage wcd_de_midterm_s3_stage
file_format = csv_comma_skip1_format
url = 's3://weclouddata/data/de_midterm_raw/';


list @wcd_de_midterm_s3_stage;

---tables creations

CREATE OR REPLACE TABLE MIDTERM_DB.RAW.store
(
    store_key   INTEGER,
    store_num   varchar(30),
    store_desc  varchar(150),
    addr    varchar(500),
    city    varchar(50),
    region varchar(100),
    cntry_cd    varchar(30),
    cntry_nm    varchar(150),
    postal_zip_cd   varchar(10),
    prov_state_desc varchar(30),
    prov_state_cd   varchar(30),
    store_type_cd varchar(30),
    store_type_desc varchar(150),
    frnchs_flg  boolean,
    store_size numeric(19,3),
    market_key  integer,
    market_name varchar(150),
    submarket_key   integer,
    submarket_name  varchar(150),
    latitude    NUMERIC(19, 6),
    longitude   NUMERIC(19, 6)
);

COPY INTO MIDTERM_DB.RAW.store FROM @wcd_de_midterm_s3_stage/store_mid.csv;


CREATE OR REPLACE TABLE sales(
trans_id int,
prod_key int,
store_key int,
trans_dt date,
trans_time int,
sales_qty numeric(38,2),
sales_price numeric(38,2),
sales_amt NUMERIC(38,2),
discount numeric(38,2),
sales_cost numeric(38,2),
sales_mgrn numeric(38,2),
ship_cost numeric(38,2)
);

COPY INTO MIDTERM_DB.RAW.sales FROM @wcd_de_midterm_s3_stage/sales_mid.csv;


CREATE OR REPLACE TABLE MIDTERM_DB.RAW.calendar
(   
    cal_dt  date NOT NULL,
    cal_type_desc   varchar(20),
    day_of_wk_num    varchar(30),
    day_of_wk_desc varchar,
    yr_num  integer,
    wk_num  integer,
    yr_wk_num   integer,
    mnth_num    integer,
    yr_mnth_num integer,
    qtr_num integer,
    yr_qtr_num  integer
);

COPY INTO MIDTERM_DB.RAW.calendar FROM @wcd_de_midterm_s3_stage/calendar_mid.csv;


CREATE OR REPLACE TABLE product 
(
    prod_key int ,
    prod_name varchar,
    vol NUMERIC (38,2),
    wgt NUMERIC (38,2),
    brand_name varchar, 
    status_code int,
    status_code_name varchar,
    category_key int,
    category_name varchar,
    subcategory_key int,
    subcategory_name varchar
);

COPY INTO MIDTERM_DB.RAW.product FROM @wcd_de_midterm_s3_stage/product_mid.csv;


CREATE OR REPLACE TABLE RAW.inventory (
cal_dt date,
store_key int,
prod_key int,
inventory_on_hand_qty NUMERIC(38,2),
inventory_on_order_qty NUMERIC(38,2),
out_of_stock_flg int,
waste_qty number(38,2),
promotion_flg boolean,
next_delivery_dt date
);

COPY INTO MIDTERM_DB.RAW.inventory FROM @wcd_de_midterm_s3_stage/inventory_mid.csv;


--Create My stage table

---My Stage integration

----data integration

-----Create integration

CREATE OR REPLACE STORAGE INTEGRATION S3_DE_MIDTERM
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::445175902094:role/snowflake-de-midterm-stage-role'
STORAGE_ALLOWED_LOCATIONS = ('s3://de-midterm-raw');


-----Describe bucket and copy STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID in AWS Policy

DESC STORAGE INTEGRATION S3_DE_MIDTERM;

---Create format

USE DATABASE MIDTERM_DB;
USE SCHEMA RAW;

create or replace file format csv_comma_skip1_format
type = 'CSV'
field_delimiter = ','
skip_header = 1;

---Grant necessary permissions to integration and file format

GRANT CREATE STAGE ON SCHEMA RAW to ROLE accountadmin;
GRANT USAGE ON INTEGRATION S3_DE_MIDTERM to ROLE accountadmin;

---Create stage table

create or replace stage S3_MIDTERM_STAGE
STORAGE_INTEGRATION = S3_DE_MIDTERM
file_format = csv_comma_skip1_format
url = 's3://de-midterm-raw';

list @S3_MIDTERM_STAGE;

---Data loading

----Step 0. adding 1 file in S3 bucket for testing purposes

-----Load today's data

copy into '@S3_MIDTERM_STAGE/sales_20230725.csv' from (select * from midterm_db.raw.sales where trans_dt <= current_date())
file_format=(TYPE=CSV, COMPRESSION='None')
single = true
MAX_FILE_SIZE=107772160
OVERWRITE=TRUE
HEADER = TRUE
;

copy into '@S3_MIDTERM_STAGE/inventory_20230725.csv' from (select * from midterm_db.raw.inventory where cal_dt <= current_date())
file_format=(TYPE=CSV, COMPRESSION='None')
single = true
MAX_FILE_SIZE=107772160
OVERWRITE=TRUE
HEADER = TRUE
;

copy into '@S3_MIDTERM_STAGE/store_20230725.csv' from (select * from midterm_db.raw.store)
file_format=(TYPE=CSV, COMPRESSION='None')
single = true
MAX_FILE_SIZE=107772160
OVERWRITE=TRUE
HEADER = TRUE
;

copy into '@S3_MIDTERM_STAGE/product_20230725.csv' from (select * from midterm_db.raw.product)
file_format=(TYPE=CSV, COMPRESSION='None')
single = true
MAX_FILE_SIZE=107772160
OVERWRITE=TRUE
HEADER = TRUE
;

copy into '@S3_MIDTERM_STAGE/calendar_20230725.csv' from (select * from midterm_db.raw.calendar)
file_format=(TYPE=CSV, COMPRESSION='None')
single = true
MAX_FILE_SIZE=107772160
OVERWRITE=TRUE
HEADER = TRUE
;

-----Load tomorrow's data

copy into '@S3_MIDTERM_STAGE/sales_20230726.csv' from (select * from midterm_db.raw.sales where trans_dt <= current_date()+1)
file_format=(TYPE=CSV, COMPRESSION='None')
single = true
MAX_FILE_SIZE=107772160
OVERWRITE=TRUE
HEADER = TRUE
;

copy into '@S3_MIDTERM_STAGE/inventory_20230726.csv' from (select * from midterm_db.raw.inventory where cal_dt <= current_date()+1)
file_format=(TYPE=CSV, COMPRESSION='None')
single = true
MAX_FILE_SIZE=107772160
OVERWRITE=TRUE
HEADER = TRUE
;

copy into '@S3_MIDTERM_STAGE/store_20230726.csv' from (select * from midterm_db.raw.store)
file_format=(TYPE=CSV, COMPRESSION='None')
single = true
MAX_FILE_SIZE=107772160
OVERWRITE=TRUE
HEADER = TRUE
;

copy into '@S3_MIDTERM_STAGE/product_20230726.csv' from (select * from midterm_db.raw.product)
file_format=(TYPE=CSV, COMPRESSION='None')
single = true
MAX_FILE_SIZE=107772160
OVERWRITE=TRUE
HEADER = TRUE
;

copy into '@S3_MIDTERM_STAGE/calendar_20230726.csv' from (select * from midterm_db.raw.calendar)
file_format=(TYPE=CSV, COMPRESSION='None')
single = true
MAX_FILE_SIZE=107772160
OVERWRITE=TRUE
HEADER = TRUE
;

---Step 1. Create a procedure to load data from Snowflake table to S3

CREATE OR REPLACE PROCEDURE COPY_INTO_S3()
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    var rows = [];

    var n = new Date();
    // May need refinement to zero-pad some values or achieve a specific format
    var date = `${n.getFullYear()}-${("0" + (n.getMonth() + 1)).slice(-2)}-${n.getDate()}`;

    var st_inv = snowflake.createStatement({
        sqlText: `COPY INTO '@S3_MIDTERM_STAGE/inventory_${date}.csv' FROM (select * from midterm_db.raw.inventory where cal_dt <= current_date()) file_format=(TYPE=CSV, COMPRESSION='None') SINGLE=TRUE HEADER=TRUE MAX_FILE_SIZE=107772160 OVERWRITE=TRUE;`
    });
    var st_sales = snowflake.createStatement({
        sqlText: `COPY INTO '@S3_MIDTERM_STAGE/sales_${date}.csv' FROM (select * from midterm_db.raw.sales where trans_dt <= current_date()) file_format=(TYPE=CSV, COMPRESSION='None') SINGLE=TRUE HEADER=TRUE MAX_FILE_SIZE=107772160 OVERWRITE=TRUE;`
    });
    var st_store = snowflake.createStatement({
        sqlText: `COPY INTO '@S3_MIDTERM_STAGE/store_${date}.csv' FROM (select * from midterm_db.raw.store) file_format=(TYPE=CSV, COMPRESSION='None') SINGLE=TRUE HEADER=TRUE MAX_FILE_SIZE=107772160 OVERWRITE=TRUE;`
    });
    var st_product = snowflake.createStatement({
        sqlText: `COPY INTO '@S3_MIDTERM_STAGE/product_${date}.csv' FROM (select * from midterm_db.raw.product) file_format=(TYPE=CSV, COMPRESSION='None') SINGLE=TRUE HEADER=TRUE MAX_FILE_SIZE=107772160 OVERWRITE=TRUE;`
    });
    var st_calendar = snowflake.createStatement({
        sqlText: `COPY INTO '@S3_MIDTERM_STAGE/calendar_${date}.csv' FROM (select * from midterm_db.raw.calendar) file_format=(TYPE=CSV, COMPRESSION='None') SINGLE=TRUE HEADER=TRUE MAX_FILE_SIZE=107772160 OVERWRITE=TRUE;`
    });

    var result_inv = st_inv.execute();
    var result_sales = st_sales.execute();
    var result_store = st_store.execute();
    var result_product = st_product.execute();
    var result_calendar = st_calendar.execute();


    result_inv.next();
    result_sales.next();
    result_store.next();
    result_product.next();
    result_calendar.next();

    rows.push(result_inv.getColumnValue(1))
    rows.push(result_sales.getColumnValue(1))
    rows.push(result_store.getColumnValue(1))
    rows.push(result_product.getColumnValue(1))
    rows.push(result_calendar.getColumnValue(1))


    return rows;
$$;


---Step 2. Create a task to run the job. Here we use cron to set job at 2am EST everyday

CREATE OR REPLACE TASK load_data_to_s3
WAREHOUSE = MIDTERM_WH 
SCHEDULE = 'USING CRON 0 2 * * * America/New_York'
AS
CALL COPY_INTO_S3();

---Step 3. Activate the task

ALTER TASK load_data_to_s3 resume;

---Step 4. Check if the task state is 'started'

DESCRIBE TASK load_data_to_s3;