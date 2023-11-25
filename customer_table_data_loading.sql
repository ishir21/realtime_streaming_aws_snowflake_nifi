create database if not exists scd

create schema scd2

create or replace table customer (
    customer_id number,
    first_name varchar,
    last_name varchar,
    email varchar,
    street varchar,
    city varchar,
    state varchar,
    country varchar,
    update_timestamp timestamp_ntz default current_timestamp()
);

create or replace table customer_history(
    customer_id number,
    first_name varchar,
    last_name varchar,
    email varchar,
    street varchar,
    city varchar,
    state varchar,
    country varchar,
    start_time timestamp_ntz default current_timestamp(),
    end_time timestamp_ntz default current_timestamp(),
    is_current boolean
);

create or replace table customer_raw(
     customer_id number,
    first_name varchar,
    last_name varchar,
    email varchar,
    street varchar,
    city varchar,
    state varchar,
    country varchar
);
show tables


create storage integration s3_customer_data
    type= external_stage
    storage_provider = 's3'
    enabled = true
    storage_aws_role_arn = 'arn:aws:iam::835395167012:role/snowflake_s3_customer_data'
    storage_allowed_locations = ('s3://snowflake-ecommerce-db/stream_data/')

desc storage integration s3_customer_data

create or replace stage scd.scd2.customer_stage
    url='s3://snowflake-ecommerce-db/stream_data/'
    storage_integration = s3_customer_data

create or replace file format scd.scd2.csv
type = csv
field_delimiter = ","
skip_header = 1;

list @customer_stage

create or replace pipe customer_s3_pipe
auto_ingest = TRUE
as
copy into customer_raw
from @customer_stage
file_format = csv

show pipes

select count(*) from customer_raw