create database if not exists tpcds_db;

use tpcds_db;

drop table if exists catalog_sales;

create table catalog_sales
(
    cs_sold_date_sk           int,
    cs_sold_time_sk           int,
    cs_ship_date_sk           int,
    cs_bill_customer_sk       int,
    cs_bill_cdemo_sk          int,
    cs_bill_hdemo_sk          int,
    cs_bill_addr_sk           int,
    cs_ship_customer_sk       int,
    cs_ship_cdemo_sk          int,
    cs_ship_hdemo_sk          int,
    cs_ship_addr_sk           int,
    cs_call_center_sk         int,
    cs_catalog_page_sk        int,
    cs_ship_mode_sk           int,
    cs_warehouse_sk           int,
    cs_item_sk                int,
    cs_promo_sk               int,
    cs_order_number           bigint,
    cs_quantity               int,
    cs_wholesale_cost         float,
    cs_list_price             float,
    cs_sales_price            float,
    cs_ext_discount_amt       float,
    cs_ext_sales_price        float,
    cs_ext_wholesale_cost     float,
    cs_ext_list_price         float,
    cs_ext_tax                float,
    cs_coupon_amt             float,
    cs_ext_ship_cost          float,
    cs_net_paid               float,
    cs_net_paid_inc_tax       float,
    cs_net_paid_inc_ship      float,
    cs_net_paid_inc_ship_tax  float,
    cs_net_profit             float
)
row format delimited fields terminated by '|';

load data local inpath 'src/main/resources/inputFiles/catalog_sales.dat' overwrite into table catalog_sales;

drop table if exists customer;

create table customer
(
    c_customer_sk             int,
    c_customer_id             string,
    c_current_cdemo_sk        int,
    c_current_hdemo_sk        int,
    c_current_addr_sk         int,
    c_first_shipto_date_sk    int,
    c_first_sales_date_sk     int,
    c_salutation              string,
    c_first_name              string,
    c_last_name               string,
    c_preferred_cust_flag     string,
    c_birth_day               int,
    c_birth_month             int,
    c_birth_year              int,
    c_birth_country           string,
    c_login                   string,
    c_email_address           string,
    c_last_review_date        string
)
row format delimited fields terminated by '|';

load data local inpath 'src/main/resources/inputFiles/customer.dat' overwrite into table customer;

 drop table if exists customer_address;

 create table customer_address
 (
     ca_address_sk             int,
     ca_address_id             string,
     ca_street_number          string,
     ca_street_name            string,
     ca_street_type            string,
     ca_suite_number           string,
     ca_city                   string,
     ca_county                 string,
     ca_state                  string,
     ca_zip                    string,
     ca_country                string,
     ca_gmt_offset             float,
     ca_location_type          string
 )
 row format delimited fields terminated by '|';

 load data local inpath 'src/main/resources/inputFiles/customer_address.dat' overwrite into table customer_address;

 drop table if exists customer_demographics;

 create table customer_demographics
 (
     cd_demo_sk                int,
     cd_gender                 string,
     cd_marital_status         string,
     cd_education_status       string,
     cd_purchase_estimate      int,
     cd_credit_rating          string,
     cd_dep_count              int,
     cd_dep_employed_count     int,
     cd_dep_college_count      int
 )
 row format delimited fields terminated by '|';

 load data local inpath 'src/main/resources/inputFiles/customer_demographics.dat' overwrite into table customer_demographics;

drop table if exists cd_partitioned;

create table cd_partitioned
(
     cd_demo_sk                int,
     cd_marital_status         string,
     cd_education_status       string,
     cd_purchase_estimate      int,
     cd_credit_rating          string,
     cd_dep_count              int,
     cd_dep_employed_count     int,
     cd_dep_college_count      int
)
partitioned by (cd_gender string)
row format delimited fields terminated by '|';

insert into table cd_partitioned
partition (cd_gender)
select
     cd_demo_sk                int,
     cd_gender                 string,
     cd_marital_status         string,
     cd_education_status       string,
     cd_purchase_estimate      int,
     cd_credit_rating          string,
     cd_dep_count              int,
     cd_dep_employed_count     int,
     cd_dep_college_count      int
from customer_demographics;

show partitions cd_partitioned;

 drop table if exists date_dim;

 create table date_dim
 (
     d_date_sk                 int,
     d_date_id                 string,
     d_date                    string,
     d_month_seq               int,
     d_week_seq                int,
     d_quarter_seq             int,
     d_year                    int,
     d_dow                     int,
     d_moy                     int,
     d_dom                     int,
     d_qoy                     int,
     d_fy_year                 int,
     d_fy_quarter_seq          int,
     d_fy_week_seq             int,
     d_day_name                string,
     d_quarter_name            string,
     d_holiday                 string,
     d_weekend                 string,
     d_following_holiday       string,
     d_first_dom               int,
     d_last_dom                int,
     d_same_day_ly             int,
     d_same_day_lq             int,
     d_current_day             string,
     d_current_week            string,
     d_current_month           string,
     d_current_quarter         string,
     d_current_year            string
 )
 row format delimited fields terminated by '|';

 load data local inpath 'src/main/resources/inputFiles/date_dim.dat' overwrite into table date_dim;

 drop table if exists item;

 create table item
 (
     i_item_sk                 int,
     i_item_id                 string,
     i_rec_start_date          string,
     i_rec_end_date            string,
     i_item_desc               string,
     i_current_price           float,
     i_wholesale_cost          float,
     i_brand_id                int,
     i_brand                   string,
     i_class_id                int,
     i_class                   string,
     i_category_id             int,
     i_category                string,
     i_manufact_id             int,
     i_manufact                string,
     i_size                    string,
     i_formulation             string,
     i_color                   string,
     i_units                   string,
     i_container               string,
     i_manager_id              int,
     i_product_name            string
 )
 row format delimited fields terminated by '|';

 load data local inpath 'src/main/resources/inputFiles/item.dat' overwrite into table item;