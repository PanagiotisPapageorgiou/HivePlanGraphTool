create database if not exists tpcds_db;

use tpcds_db;

drop database tpcds_db;

drop table if exists catalogDateCombo;

explain create table catalogDateCombo as
select
	cast(cs_quantity as decimal(12,2)) agg1,
    cast(cs_list_price as decimal(12,2)) agg2,
    cast(cs_coupon_amt as decimal(12,2)) agg3,
    cast(cs_sales_price as decimal(12,2)) agg4,
    cast(cs_net_profit as decimal(12,2)) agg5,
	cs_item_sk,
	cs_bill_cdemo_sk,
	cs_bill_customer_sk
from catalog_sales inner join date_dim on catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
where date_dim.d_year = 2001;

create table catalogDateCombo as
select
	cast(cs_quantity as decimal(12,2)) agg1,
    cast(cs_list_price as decimal(12,2)) agg2,
    cast(cs_coupon_amt as decimal(12,2)) agg3,
    cast(cs_sales_price as decimal(12,2)) agg4,
    cast(cs_net_profit as decimal(12,2)) agg5,
	cs_item_sk,
	cs_bill_cdemo_sk,
	cs_bill_customer_sk
from catalog_sales inner join date_dim on catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
where date_dim.d_year = 2001;

drop table if exists catalogDateItemCombo;

explain create table catalogDateItemCombo as
select
	i_item_id,
	agg1,
	agg2,
	agg3,
	agg4,
	agg5,
	cs_bill_cdemo_sk,
	cs_bill_customer_sk	
from catalogDateCombo inner join item on catalogDateCombo.cs_item_sk = item.i_item_sk;

create table catalogDateItemCombo as
select
	i_item_id,
	agg1,
	agg2,
	agg3,
	agg4,
	agg5,
	cs_bill_cdemo_sk,
	cs_bill_customer_sk	
from catalogDateCombo inner join item on catalogDateCombo.cs_item_sk = item.i_item_sk;

drop table if exists catalogDateItemDemoCombo;

explain create table catalogDateItemDemoCombo as
select
	i_item_id,
	agg1,
	agg2,
	agg3,
	agg4,
	agg5,
	cast(cd.cd_dep_count as decimal(12,2)) agg7,
	cs_bill_customer_sk	
from catalogDateItemCombo inner join customer_demographics cd  on catalogDateItemCombo.cs_bill_cdemo_sk = cd.cd_demo_sk
where cd.cd_gender = 'M' and cd.cd_education_status = 'College';

create table catalogDateItemDemoCombo as
select
	i_item_id,
	agg1,
	agg2,
	agg3,
	agg4,
	agg5,
	cast(cd.cd_dep_count as decimal(12,2)) agg7,
	cs_bill_customer_sk	
from catalogDateItemCombo inner join customer_demographics cd  on catalogDateItemCombo.cs_bill_cdemo_sk = cd.cd_demo_sk
where cd.cd_gender = 'M' and cd.cd_education_status = 'College';

drop table if exists catalogDateItemDemoCust;

explain create table catalogDateItemDemoCust as
select
	i_item_id,
	agg1,
	agg2,
	agg3,
	agg4,
	agg5,
	cast(c_birth_year as decimal(12,2)) agg6,
	agg7,
	c_current_cdemo_sk,
	c_current_addr_sk
from catalogDateItemDemoCombo inner join customer on catalogDateItemDemoCombo.cs_bill_customer_sk = customer.c_customer_sk
where c_birth_month in (9,5,12,4,1,10);

create table catalogDateItemDemoCust as
select
	i_item_id,
	agg1,
	agg2,
	agg3,
	agg4,
	agg5,
	cast(c_birth_year as decimal(12,2)) agg6,
	agg7,
	c_current_cdemo_sk,
	c_current_addr_sk
from catalogDateItemDemoCombo inner join customer on catalogDateItemDemoCombo.cs_bill_customer_sk = customer.c_customer_sk
where c_birth_month in (9,5,12,4,1,10);

drop table if exists catDateItemDemCust2;

explain create table catDateItemDemCust2 as
select
	i_item_id,
	agg1,
	agg2,
	agg3,
	agg4,
	agg5,
	agg6,
	agg7,
	c_current_addr_sk	
from catalogDateItemDemoCust inner join customer_demographics cd2 on catalogDateItemDemoCust.c_current_cdemo_sk = cd2.cd_demo_sk;

create table catDateItemDemCust2 as
select
	i_item_id,
	agg1,
	agg2,
	agg3,
	agg4,
	agg5,
	agg6,
	agg7,
	c_current_addr_sk	
from catalogDateItemDemoCust inner join customer_demographics cd2 on catalogDateItemDemoCust.c_current_cdemo_sk = cd2.cd_demo_sk;

drop table if exists results;

explain create table results as
select
	i_item_id,
	ca_country,
	ca_state,
	ca_county,
    agg1,
    agg2,
    agg3,
    agg4,
    agg5,
    agg6,
    agg7
from catDateItemDemCust2 inner join customer_address on catDateItemDemCust2.c_current_addr_sk = customer_address.ca_address_sk
where customer_address.ca_state in ('ND','WI','AL','NC','OK','MS','TN');

create table results as
select
	i_item_id,
	ca_country,
	ca_state,
	ca_county,
    agg1,
    agg2,
    agg3,
    agg4,
    agg5,
    agg6,
    agg7
from catDateItemDemCust2 inner join customer_address on catDateItemDemCust2.c_current_addr_sk = customer_address.ca_address_sk
where customer_address.ca_state in ('ND','WI','AL','NC','OK','MS','TN');

drop table if exists query18results;

explain create table query18results as
 select i_item_id, ca_country, ca_state, ca_county, agg1, agg2, agg3, agg4, agg5, agg6, agg7
 from (
 	select i_item_id, ca_country, ca_state, ca_county, avg(agg1) agg1, 
 		avg(agg2) agg2, avg(agg3) agg3, avg(agg4) agg4, avg(agg5) agg5, avg(agg6) agg6, avg(agg7) agg7 
 	from results
	group by i_item_id, ca_country, ca_state, ca_county
 	union all
 	select i_item_id, ca_country, ca_state, NULL as county, avg(agg1) agg1, avg(agg2) agg2, avg(agg3) agg3,
		avg(agg4) agg4, avg(agg5) agg5, avg(agg6) agg6, avg(agg7) agg7 
	from results
	group by i_item_id, ca_country, ca_state
 	union all
	select i_item_id, ca_country, NULL as ca_state, NULL as county, avg(agg1) agg1, avg(agg2) agg2, avg(agg3) agg3,
		avg(agg4) agg4, avg(agg5) agg5, avg(agg6) agg6, avg(agg7) agg7 
	from results
 	group by i_item_id, ca_country
 	union all
 	select i_item_id, NULL as ca_country, NULL as ca_state, NULL as county, avg(agg1) agg1, avg(agg2) agg2, avg(agg3) agg3,
		avg(agg4) agg4, avg(agg5) agg5, avg(agg6) agg6, avg(agg7) agg7 
	from results
	group by i_item_id
	union all
	select NULL AS i_item_id, NULL as ca_country, NULL as ca_state, NULL as county, avg(agg1) agg1, avg(agg2) agg2, avg(agg3) agg3,
		avg(agg4) agg4, avg(agg5) agg5, avg(agg6) agg6, avg(agg7) agg7 
	from results
 ) foo
 order by ca_country, ca_state, ca_county, i_item_id
limit 100;

create table query18results as
 select i_item_id, ca_country, ca_state, ca_county, agg1, agg2, agg3, agg4, agg5, agg6, agg7
 from (
 	select i_item_id, ca_country, ca_state, ca_county, avg(agg1) agg1, 
 		avg(agg2) agg2, avg(agg3) agg3, avg(agg4) agg4, avg(agg5) agg5, avg(agg6) agg6, avg(agg7) agg7 
 	from results
	group by i_item_id, ca_country, ca_state, ca_county
 	union all
 	select i_item_id, ca_country, ca_state, NULL as county, avg(agg1) agg1, avg(agg2) agg2, avg(agg3) agg3,
		avg(agg4) agg4, avg(agg5) agg5, avg(agg6) agg6, avg(agg7) agg7 
	from results
	group by i_item_id, ca_country, ca_state
 	union all
	select i_item_id, ca_country, NULL as ca_state, NULL as county, avg(agg1) agg1, avg(agg2) agg2, avg(agg3) agg3,
		avg(agg4) agg4, avg(agg5) agg5, avg(agg6) agg6, avg(agg7) agg7 
	from results
 	group by i_item_id, ca_country
 	union all
 	select i_item_id, NULL as ca_country, NULL as ca_state, NULL as county, avg(agg1) agg1, avg(agg2) agg2, avg(agg3) agg3,
		avg(agg4) agg4, avg(agg5) agg5, avg(agg6) agg6, avg(agg7) agg7 
	from results
	group by i_item_id
	union all
	select NULL AS i_item_id, NULL as ca_country, NULL as ca_state, NULL as county, avg(agg1) agg1, avg(agg2) agg2, avg(agg3) agg3,
		avg(agg4) agg4, avg(agg5) agg5, avg(agg6) agg6, avg(agg7) agg7 
	from results
 ) foo
 order by ca_country, ca_state, ca_county, i_item_id
limit 100;