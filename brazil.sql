/*
Kultigin Bozdemir
DBS2 exam, 2020
source of data: https://www.kaggle.com/andresionek/geospatial-analysis-of-brazilian-e-commerce/notebook?
A png picture shows the relations betweenn tables.
The project is stored in a Github repo.
https://github.com/KultiginX/geo_Brazil
*/

drop table if exists customers;
drop table if exists geolocation;
drop table if exists orders;
drop table if exists order_items;
drop table if exists order_payments;
drop table if exists order_reviews;
drop table if exists sellers;
drop table if exists products;
drop table if exists product_category_name_translation;

-- ---------
create table customers
(
	customer_id varchar(50),
	customer_unique_id varchar(50),
	customer_zip_code_prefix integer,
	customer_city varchar(50),
	customer_state varchar(20)
);

COPY customers(customer_id,customer_unique_id,customer_zip_code_prefix,customer_city,customer_state) 
FROM '/Users/kultiginbozdemir/Documents/GitHub/geo_Brazil/brazil_geo_data/olist_customers_dataset.csv' 
DELIMITER ',' CSV HEADER;

-- ----------####################
create table geolocation
(
	geolocation_zip_code_prefix integer,
	geolocation_lat float,
	geolocation_lng float, 
	geolocation_city varchar(50),
	geolocation_state varchar(20)
);


COPY geolocation(
geolocation_zip_code_prefix,
	geolocation_lat ,
	geolocation_lng , 
	geolocation_city ,
	geolocation_state 	
) 
FROM '/Users/kultiginbozdemir/Documents/GitHub/geo_Brazil/brazil_geo_data/olist_geolocation_dataset.csv' 
DELIMITER ',' CSV HEADER;


-- ----------##################
create table order_items
(
	order_id varchar(50),
	order_item_id integer,
	product_id varchar(50),
	seller_id varchar(50),
	shipping_limit_date date,
	price money,
	freight_value money
);

COPY order_items(
order_id,	order_item_id,	product_id, seller_id,	shipping_limit_date,	price, freight_value) 
FROM '/Users/kultiginbozdemir/Documents/GitHub/geo_Brazil/brazil_geo_data/olist_order_items_dataset.csv' 
DELIMITER ',' CSV HEADER;

-- ----------########################
create table order_payments
(
	order_id varchar(50),
	payment_sequential integer,
	payment_type varchar(50),
	payment_installments integer,
	payment_value money
);

COPY order_payments(
order_id, payment_sequential,	payment_type,	payment_installments,	payment_value) 
FROM '/Users/kultiginbozdemir/Documents/GitHub/geo_Brazil/brazil_geo_data/olist_order_payments_dataset.csv' 
DELIMITER ',' CSV HEADER;

-- ----------#########################
create table order_reviews
(
	review_id varchar(50),
	order_id varchar(50),
	review_score integer,
	review_comment_title varchar(100),	
	review_comment_message text,
	review_creation_date date,
	review_answer_timestamp date
);

COPY order_reviews(
	review_id,
	order_id,	review_score,
	review_comment_title,
	review_comment_message,
	review_creation_date,
	review_answer_timestamp) 
FROM '/Users/kultiginbozdemir/Documents/GitHub/geo_Brazil/brazil_geo_data/olist_order_reviews_dataset.csv' 
DELIMITER ',' CSV HEADER;


--------=====##########################
create table orders
(
	order_id varchar(50),
	customer_id varchar(50),
	order_status varchar(20),
	order_purchase_timestamp date,
	order_approved_at date,
	order_delivered_carrier_date date,
	order_delivered_customer_date date,
	order_estimated_delivery_date date
);

COPY orders(
	order_id,
	customer_id,
	order_status,
	order_purchase_timestamp,
	order_approved_at,
	order_delivered_carrier_date,
	order_delivered_customer_date,
	order_estimated_delivery_date) 
FROM '/Users/kultiginbozdemir/Documents/GitHub/geo_Brazil/brazil_geo_data/olist_orders_dataset.csv' 
DELIMITER ',' CSV HEADER;

-----############################
create table products
(
	product_id varchar(50),
	product_category_name varchar(100),
	product_name_lenght integer,
	product_description_lenght integer,
	product_photos_qty integer,
	product_weight_g integer,
	product_length_cm integer,
	product_height_cm integer,
	product_width_cm integer
);

COPY products(
	product_id,
	product_category_name,
	product_name_lenght,
	product_description_lenght,
	product_photos_qty,
	product_weight_g,
	product_length_cm,
	product_height_cm,
	product_width_cm) 
FROM '/Users/kultiginbozdemir/Documents/GitHub/geo_Brazil/brazil_geo_data/olist_products_dataset.csv' 
DELIMITER ',' CSV HEADER;

-----############################
create table sellers
(
	seller_id varchar(50),	
	seller_zip_code_prefix integer,
	seller_city varchar(50),
	seller_state varchar(20)
);

COPY sellers(
	seller_id,
	seller_zip_code_prefix,
	seller_city,
	seller_state) 
FROM '/Users/kultiginbozdemir/Documents/GitHub/geo_Brazil/brazil_geo_data/olist_sellers_dataset.csv' 
DELIMITER ',' CSV HEADER;

-----############################
create table product_category_name_translation
(
	product_category_name varchar,
	product_category_name_english varchar
);

COPY product_category_name_translation(
	product_category_name,
	product_category_name_english) 
FROM '/Users/kultiginbozdemir/Documents/GitHub/geo_Brazil/brazil_geo_data/product_category_name_translation.csv' 
DELIMITER ',' CSV HEADER;


--- Data Cleaning
-- geolocation table is the core of the model.
--We have multiple zip codes.

SELECT DISTINCT geolocation_zip_code_prefix, geolocation_city , geolocation_state 
FROM geolocation;
SELECT
	geolocation_zip_code_prefix,
	COUNT (geolocation_zip_code_prefix)
FROM
	GEOLOCATION
GROUP BY
	geolocation_zip_code_prefix;

--- We remove abundant zip_code. 
-- Different states might have same zip codes. In this contex, I ignored them.
drop table  if exists geolocation2;
SELECT * INTO geolocation2 FROM geolocation WHERE 1 = 0;
insert into geolocation2 select * from geolocation;

ALTER TABLE geolocation2 ADD COLUMN id SERIAL;

DELETE FROM geolocation2
WHERE id IN
    (SELECT id
    FROM 
        (SELECT id,
         ROW_NUMBER() OVER( PARTITION BY geolocation_zip_code_prefix
        ORDER BY  id ) AS row_num
        FROM geolocation2 ) t
        WHERE t.row_num > 1 );
		
-- test the result, and compare it with previous unconnsistennt table.		
select count(*) from geolocation2 where geolocation_zip_code_prefix=13023 ;
select count(*) from geolocation where geolocation_zip_code_prefix=13023 ;

-- seller 
select * from sellers;
select count(*) from sellers;

delete from sellers where seller_zip_code_prefix not in 
(select geolocation_zip_code_prefix from geolocation2);

--customer
select * from customers;
select count (*) from customers;

delete from customers where customer_zip_code_prefix not in 
(select geolocation_zip_code_prefix from geolocation2);


-- product ~ order_items
select * from products; 
select count(*) from products;

-- orders
select * from orders;
select count(*) from orders;

alter table orders add column id serial;
-- size of data is too much. I keep first 1000 rows.
delete from orders where id>1000;

delete from orders where customer_id not in
(select customer_id from customers);


-- order items
select * from order_items;

delete from order_items where seller_id not in
(select seller_id from sellers);

delete from order_items where product_id not in 
(select product_id from products);

delete from order_items where order_id not in 
(select order_id from orders);


-- order payments
select * from order_payments;

delete from order_payments where order_id not in 
(select order_id from orders);
 
 
 --- order reviews
 select * from order_reviews;
 
 delete from order_reviews where order_id not in 
 (select order_id from orders);
 
 
 --
 --time to create relations
alter table geolocation2 add primary key (geolocation_zip_code_prefix);

ALTER TABLE sellers 
ADD CONSTRAINT fk_seller_geolocation FOREIGN KEY (seller_zip_code_prefix) 
REFERENCES geolocation2 (geolocation_zip_code_prefix);

alter table sellers add primary key (seller_id);

ALTER TABLE customers 
ADD CONSTRAINT fk_customer_on_geolocation FOREIGN KEY (customer_zip_code_prefix) 
REFERENCES geolocation2 (geolocation_zip_code_prefix);

alter table customers add primary key(customer_id);

alter table products add primary key(product_id);

ALTER TABLE orders 
ADD CONSTRAINT fk_orders_customers FOREIGN KEY (customer_id) 
REFERENCES customers (customer_id);

alter table orders add primary key(order_id);

ALTER TABLE order_items 
ADD CONSTRAINT fk_orderitems_seller FOREIGN KEY (seller_id) 
REFERENCES sellers (seller_id);

ALTER TABLE order_items 
ADD CONSTRAINT fk_orderitems_products FOREIGN KEY (product_id) 
REFERENCES products (product_id);

ALTER TABLE order_items 
ADD CONSTRAINT fk_orderitems_order FOREIGN KEY (order_id) 
REFERENCES orders (order_id);

alter table order_payments 
 add CONSTRAINT fk_orderpayments_orders FOREIGN key (order_id)
 references orders(order_id);
 
 ALTER TABLE order_reviews 
ADD CONSTRAINT fk_orderreviews_order FOREIGN KEY (order_id) 
REFERENCES orders (order_id);

alter table order_reviews add primary key(review_id);
 
 
 