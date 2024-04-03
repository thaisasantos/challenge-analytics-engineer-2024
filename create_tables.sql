-- Challenge - Analytics Engineer Abr/2024
-- Script DDL para criação das tabelas DER do modelo de negócio Fluxo de Compras
-- (https://github.com/thaisasantos/challenge-analytics-engineer-2024/blob/main/DER%20-%20Modelo%20de%20Neg%C3%B3cio%20Fluxo%20de%20Compras.pdf)
---------------------------------------------------------------------------------------------------------------------------------------------------
-- Customer - Create
create table if not exists `esoteric-sled-419201.public_bigquery_data.mercado_libre_ecommerce_customer` (
			cust_id INT64 not null,
			cust_email STRING,
			cust_name STRING,
			cust_nickname STRING,
			cust_gender STRING,
			cust_birthday DATE,
			cust_phone STRING,
			cust_cellphone STRING,
			cust_status STRING,
			cust_seller_flag BOOL,
			cust_insert_dt DATETIME,
			cust_last_update_dt DATETIME
);
-- Customer - Insert Values Example
insert into `esoteric-sled-419201.public_bigquery_data.mercado_libre_ecommerce_customer`
select		users.id as cust_id,
			users.email as cust_email,
			concat(concat(users.first_name,' '),users.last_name) as cust_name,
			users.first_name as cust_nickname,
			users.gender as cust_gender,
			safe_cast(concat(concat(format_datetime('%Y-',date_add(current_date(), interval -age year)),format_datetime('%m',created_at)),format_datetime('-%d',created_at)) as date) as cust_birthday,
			'(11) 9999-9999' as cust_phone,
			'(11) 9 9999-9999' as cust_cellphone,
			'active' as cust_status,
			cast(cast(round(0 + rand() * (1 - 0)) as int) as bool) as cust_seller_flag,
			created_at as cust_insert_dt,
			current_datetime() as cust_last_update_dt
from		`bigquery-public-data.thelook_ecommerce.users` users
---------------------------------------------------------------------------------------------------------------------------------------------------
-- Seller - Create
create table if not exists `esoteric-sled-419201.public_bigquery_data.mercado_libre_ecommerce_customer_seller` (
			seller_id INT64 not null,
			cust_id INT64,
			seller_company_name STRING,
			seller_type STRING,
			seller_status STRING,
			seller_creation_dt DATETIME,
			seller_cancel_dt DATETIME,
			seller_contact_name STRING,
			seller_income NUMERIC,
			seller_insert_dt DATETIME,
			seller_last_update_date DATETIME
);
---------------------------------------------------------------------------------------------------------------------------------------------------
-- Address - Create
create table if not exists `esoteric-sled-419201.public_bigquery_data.mercado_libre_ecommerce_customer_address` (
			ads_address_id INT64 not null,
			cust_id INT64,
			ads_address_type STRING,
			ads_address_status STRING,
			ads_address_name STRING,
			ads_address_city STRING,
			ads_address_country STRING,
			ads_address_state STRING,
			ads_address_zipcode STRING,
			ads_address_principal_flag BOOL,
			ads_address_insert_dt DATETIME,
			ads_address_last_update_dt DATETIME
);
---------------------------------------------------------------------------------------------------------------------------------------------------
-- Order - Create
create table if not exists `esoteric-sled-419201.public_bigquery_data.mercado_libre_ecommerce_order` (
			order_id INT64 not null,
			cust_id INT64,
			seller_id INT64,
			order_status STRING,
			order_creation_dt DATETIME,
			order_cancel_dt DATETIME,
			order_total_item_qty INT64,
			order_total_amount NUMERIC,
			order_cancel_amount NUMERIC,
			ads_address_id INT64,
			order_shipping_dt DATETIME,
			order_insert_dt DATETIME,
			order_last_update_dt DATETIME
);
-- Order - Insert Values Example
insert into `esoteric-sled-419201.public_bigquery_data.mercado_libre_ecommerce_order`
select		e_ord.order_id,
			e_ord.user_id as cust_id,
			e_ord.user_id as seller_id,
			lower(e_ord.status) as order_status,
			e_ord.created_at as order_creation_dt,
			e_ord.returned_at as order_cancel_dt,
			e_ord.num_of_item as order_total_item_qty,
			cast(round(0.1 + rand() * (999999.9 - 0.1),2) as numeric) as order_total_amount,
			0 as order_cancel_amount,
			11111111 as ads_address_id,
			e_ord.shipped_at as order_shipping_dt,
			current_datetime() as order_insert_dt,
			current_datetime() as order_last_update_dt
from 		`bigquery-public-data.thelook_ecommerce.orders` e_ord
---------------------------------------------------------------------------------------------------------------------------------------------------
-- Category - Create
create table if not exists `esoteric-sled-419201.public_bigquery_data.mercado_libre_ecommerce_category` (
			cat_category_id INT64 not null,
			cat_category_status STRING,
			cat_category_name STRING,
			cat_category_creation_dt DATETIME,
			cat_category_cancel_dt DATETIME,
			cat_category_description STRING,
			cat_category_insert_dt DATETIME,
			cat_category_last_update_dt DATETIME
);
---------------------------------------------------------------------------------------------------------------------------------------------------
-- Item - Create
create table if not exists `esoteric-sled-419201.public_bigquery_data.mercado_libre_ecommerce_order_item` (
			item_id INT64 not null,
			order_id INT64,
			cat_category_id INT64,
			item_status STRING,
			item_name STRING,
			item_creation_dt DATETIME,
			item_cancel_dt DATETIME,
			item_description STRING,
			item_amount NUMERIC,
			item_qty INT64,
			item_available_flag BOOL,
			item_insert_dt DATETIME,
			item_last_update_dt DATETIME
);
