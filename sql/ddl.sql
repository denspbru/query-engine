CREATE TABLE IF NOT EXISTS public.client(
	client_id int,
	client_name varchar(128),
	client_segment_id int,
	city varchar(128),
	_created_ts timestamp
);

CREATE TABLE IF NOT EXISTS public.client_segment(
	client_segment_id int,
	client_segment_name varchar(128),
	_created_ts timestamp
);

CREATE TABLE IF NOT EXISTS public.product(
	product_id int,
	product_type_id int,
	product_name varchar(128),
	product_cost decimal(10,2),
	_created_ts timestamp
);

CREATE TABLE IF NOT EXISTS public.product_type(
	product_type_id int,
	product_type_name varchar(128),
	_created_ts timestamp
);

CREATE TABLE IF NOT EXISTS public.sales(
	sales_id int,
	sales_date date,
	product_id int,
	client_id int,
	cnt int,
	ammount decimal(10,2),
	_created_ts timestamp
);