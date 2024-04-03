-- Challenge - Analytics Engineer Abr/2024
-- Código Bigquery - SQL para responder cada uma das situações mencionadas no formulário, utilizando as tabelas criadas
-- (https://github.com/thaisasantos/challenge-analytics-engineer-2024/blob/main/create_tables.sql)
---------------------------------------------------------------------------------------------------------------------------------------------------
-- 1. Listar los usuarios que cumplan años el día de hoy cuya cantidad de ventas realizadas en enero 2020 sea superior a 1500.
---------------------------------------------------------------------------------------------------------------------------------------------------
-- Usuários que fazem aniversário no dia de hoje
create or replace table `esoteric-sled-419201.public_bigquery_data.tmp_mercado_libre_birthday_customers`
options(expiration_timestamp = timestamp_add(current_timestamp(),interval 24 hour))
as 			(
select		cust_id,
			cust_name as nome_aniversariante
from 		`esoteric-sled-419201.public_bigquery_data.mercado_libre_ecommerce_customer` cust
where 		format_datetime('%m-%d',cust_birthday) = format_datetime('%m-%d',current_date()) -- aniversáio hoje
and  		cust_seller_flag = true -- customer é vendedor
and 		cust_status = 'active' -- ativo na plataforma, para não friccionar customers desativados
order by 	2
);

-- Quantidade de Vendas dos Usuários que fazem aniversário no dia de hoje
create or replace table `esoteric-sled-419201.public_bigquery_data.tmp_mercado_libre_birthday_sellers`
options(expiration_timestamp = timestamp_add(current_timestamp(),interval 24 hour))
as 			(
select 		cust_id,
			count(1) as qtde_vendas 
from 		`esoteric-sled-419201.public_bigquery_data.mercado_libre_ecommerce_order` e_ord
inner join 	`esoteric-sled-419201.public_bigquery_data.tmp_mercado_libre_birthday_customers` cust
on 			e_ord.cust_id = cust.cust_id
where		date(e_ord.order_creation_dt) >= '2020-01-01' -- vendas em janeiro
and  		date(e_ord.order_creation_dt) < '2020-02-01'
and  		order_status = 'approved' -- vendas aprovadas, removendo possíveis cancelamentos / reembolsos
group by 	1
having		qtde_vendas > 1500
);

-- Resultado maiores vendedores aniversariantes em ordem de vendas
select		cust.nome_aniversariante
from  		`esoteric-sled-419201.public_bigquery_data.tmp_mercado_libre_birthday_customers` cust
inner join 	`esoteric-sled-419201.public_bigquery_data.tmp_mercado_libre_birthday_sellers` sel
on  		cust.cust_id = sel.cust_id
order by 	sel.qtde_vendas desc
---------------------------------------------------------------------------------------------------------------------------------------------------
-- 2. Por cada mes del 2020, se solicita el top 5 de usuarios que más vendieron($) en la categoría Celulares. Se requiere el mes y año de análisis, nombre y apellido del vendedor, cantidad de ventas realizadas, cantidad de productos vendidos y el monto total transaccionado.
---------------------------------------------------------------------------------------------------------------------------------------------------
-- Histórico de Vendas de Celulares 2020
create or replace table `esoteric-sled-419201.public_bigquery_data.tmp_mercado_libre_cellphones_orders_2020`
options(expiration_timestamp = timestamp_add(current_timestamp(),interval 24 hour))
as 			(
select		e_ord.seller_id,
			date_trunc(date(e_ord.order_creation_dt),month) as ano_mes_venda,
			count(distinct e_ord.order_id) as qtde_vendas,
			count(1) as qtde_produtos_celulares_vendidos,
			sum(itm.item_amount) as valor_total_transacionado
from  		`esoteric-sled-419201.public_bigquery_data.mercado_libre_ecommerce_order` e_ord
inner join 	`esoteric-sled-419201.public_bigquery_data.mercado_libre_ecommerce_order_item` itm
on 			e_ord.order_id = itm.order_id
and  		itm.item_status = 'approved' -- itens aprovados, removendo possíveis cancelamentos / reembolsos
inner join 	`esoteric-sled-419201.public_bigquery_data.mercado_libre_ecommerce_category` cat
on  		cat.cat_category_id = itm.cat_category_id
and 		lower(cat.cat_category_name) = 'celulares'
where		date(e_ord.order_creation_dt) >= '2020-01-01' -- vendas no ano inteiro de 2020
and  		date(e_ord.order_creation_dt) < '2021-01-01'
and  		order_status = 'approved' -- vendas aprovadas, removendo possíveis cancelamentos / reembolsos
group by 	1,2
);

-- Definição de ranking dos vendedores ativos
create or replace table `esoteric-sled-419201.public_bigquery_data.tmp_mercado_libre_ranking_sellers_cellphones_2020`
options(expiration_timestamp = timestamp_add(current_timestamp(),interval 24 hour))
as 			(
select		cell_order.*,
			dense_rank()over(partition by ano_mes_venda order by valor_total_transacionado desc) as ranking
from 		`esoteric-sled-419201.public_bigquery_data.tmp_mercado_libre_cellphones_orders_2020` cell_order
inner join 	`esoteric-sled-419201.public_bigquery_data.mercado_libre_ecommerce_customer_seller` cust_seller
on 			cell_order.seller_id = cust_seller.seller_id
and  		cust_seller.seller_status = 'active' -- somente vendedores ativos
);

-- Resultado Top 5 Vendedores por mês
select		sel.ano_mes_venda,
			cust.cust_name,
			cust.cust_nickname,
			sel.qtde_vendas,
			sel.qtde_produtos_celulares_vendidos,
			concat('BRL ', translate(format("%'.2f", cast((ifnull(sel.valor_total_transacionado,0)) as numeric)), ',.', '.,')) as valor_total_transacionado
from  		`esoteric-sled-419201.public_bigquery_data.tmp_mercado_libre_ranking_sellers_cellphones_2020` sel
inner join 	`esoteric-sled-419201.public_bigquery_data.mercado_libre_ecommerce_customer` cust
on  		sel.cust_id = cust.cust_id
where 		sel.ranking < 6 -- apenas os top 5 vendedores por mês
order by 	sel.ano_mes_venda asc, sel.valor_total_transacionado desc
---------------------------------------------------------------------------------------------------------------------------------------------------
-- 3. Se solicita poblar una nueva tabla con el precio y estado de los Ítems a fin del día. Tener en cuenta que debe ser reprocesable. Vale resaltar que en la tabla Item, vamos a tener únicamente el último estado informado por la PK definida. (Se puede resolver a través de StoredProcedure).
---------------------------------------------------------------------------------------------------------------------------------------------------
-- Criação de tabela stagging para armazenamento de histórico e prévia de atualizações
create or replace table `esoteric-sled-419201.public_bigquery_data.mercado_libre_ecommerce_order_item_stagging`
as 			(
select * from `esoteric-sled-419201.public_bigquery_data.mercado_libre_ecommerce_order_item`
);

-- Atualização para produtos reativados
update 		`esoteric-sled-419201.public_bigquery_data.mercado_libre_ecommerce_order_item_stagging` itm
set 		itm.item_status = 'active'
from 		`esoteric-sled-419201.public_bigquery_data.mercado_libre_ecommerce_order_item_stagging` itm_stg
where 		itm_stg.item_name in ('item1','item2','item3','item4')

-- Atualização para produtos desativados
update 		`esoteric-sled-419201.public_bigquery_data.mercado_libre_ecommerce_order_item_stagging` itm
set 		itm.item_status = 'deactive'
from 		`esoteric-sled-419201.public_bigquery_data.mercado_libre_ecommerce_order_item_stagging` itm_stg
where 		itm_stg.item_name in ('item4','item5','item6','item7')

-- Atualização de preço
update 		`esoteric-sled-419201.public_bigquery_data.mercado_libre_ecommerce_order_item_stagging` itm
set 		itm.item_amount = 43.77
from 		`esoteric-sled-419201.public_bigquery_data.mercado_libre_ecommerce_order_item_stagging` itm_stg
where 		itm_stg.item_name = 'item1'