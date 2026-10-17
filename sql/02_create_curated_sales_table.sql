CREATE EXTERNAL TABLE IF NOT EXISTS thornquist_db.curated_sales (
    order_id string,
    sale_date string,
    customer_id string,
    product_id string,
    product_name string,
    unit_price double,
    quantity int
)
PARTITIONED BY (category string)
STORED AS PARQUET
LOCATION 's3://thornquist-outfitters-am-rob-alx/curated/conformed_sales/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');