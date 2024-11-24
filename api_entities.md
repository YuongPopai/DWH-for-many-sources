Здравствуйте! 
У меня появились небольшие проблемы с виртуальным окружением, которые не возникали раньше, поэтому могут появиться проблемы с запуском дагов
но я отработал даги локально на БД и все работает как по маслу
Если это важно, отправляйте на передлку и я продолжу фиксить проблему

Для создания витрины понадобятся поля из следующих таблиц:
year и month из dds.dm_timestamps
courier_id и courier_name из dds.dm_couriers (новая таблица)
sum и rate из dds.dm_delivers (новая таблица)

DDL создания новых таблиц для STG:
для object_value установлен тип text, так как уже написаны парсеры для json

CREATE TABLE stg.api_couriers (
	id serial4 NOT NULL,
	object_id varchar(50) NOT NULL,
	object_value text NOT NULL,
	CONSTRAINT api_user_unique_id UNIQUE (object_id)
);
CREATE TABLE stg.api_delivers (
	id serial4 NOT NULL,
	object_id varchar(50) NOT NULL,
	object_value text NOT NULL,
	CONSTRAINT api_delivers_unique_id UNIQUE (object_id)
);
CREATE TABLE stg.api_restaurants (
	id serial4 NOT NULL,
	object_id varchar(50) NOT NULL,
	object_value text NOT NULL,
	CONSTRAINT api_restaurant_unique_id UNIQUE (object_id)
);



DDL создания новых таблиц для DDS:

CREATE TABLE dds.dm_couriers (
	id serial4 NOT NULL,
	courier_id varchar NOT NULL,
	courier_name varchar NOT NULL,
	CONSTRAINT aa1a UNIQUE (id),
	CONSTRAINT inique_cur_id UNIQUE (courier_id)
);
CREATE TABLE dds.dm_delivers (
	id serial4 NOT NULL,
	delivery_id varchar NOT NULL,
	order_id int4 NOT NULL,
	courier_id int4 NOT NULL,
	address varchar NOT NULL,
	order_ts timestamp NOT NULL,
	delivery_ts timestamp NOT NULL,
	rate int4 NOT NULL,
	sum int4 NOT NULL,
	tip_sum int4 NOT NULL,
	CONSTRAINT dm_delivers_pkey PRIMARY KEY (id),
	CONSTRAINT un_deliver_id UNIQUE (delivery_id)
);
ALTER TABLE dds.dm_delivers ADD CONSTRAINT fk_1 FOREIGN KEY (order_id) REFERENCES dds.dm_orders(id);
ALTER TABLE dds.dm_delivers ADD CONSTRAINT fk_2 FOREIGN KEY (courier_id) REFERENCES dds.dm_couriers(id);




DDL для витрины:

CREATE TABLE cdm.dm_courier_ledger (
	id serial4 NOT NULL,
	courier_id varchar NOT NULL,
	courier_name varchar NOT NULL,
	settlement_year int4 NOT NULL,
	settlement_month int4 NOT NULL,
	orders_count int4 NOT NULL,
	orders_total_sum numeric(14, 2) NOT NULL,
	rate_avg numeric(14, 2) NOT NULL,
	order_processing_fee numeric(14, 2) NOT NULL,
	courier_order_sum numeric(14, 2) NOT NULL,
	courier_tips_sum numeric(14, 2) NOT NULL,
	courier_reward_sum numeric(14, 2) NOT NULL,
	CONSTRAINT dm_courier_ledger_pkey PRIMARY KEY (id),
	CONSTRAINT unique_cur_report UNIQUE (courier_id, settlement_year, settlement_month)
);