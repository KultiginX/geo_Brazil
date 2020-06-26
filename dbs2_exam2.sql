-- Kultigin Bozdemir
-- DBS2 exam, 24-25.06.2020
--Task 1
-- rollup
select * from orders limit 5;
-- Rollup is a subclause of the GROUP BY clause.
-- Rollup offers a multiple grouping sets.
-- Below, we roll up orders per (year, month, day). 
--With the help of count function, it returns numer of orders on each day.
SELECT
    EXTRACT (YEAR FROM order_purchase_timestamp) y,
    EXTRACT (MONTH FROM order_purchase_timestamp) M,
    EXTRACT (DAY FROM order_purchase_timestamp) d,
    COUNT (order_id)
FROM
    orders
GROUP BY
    ROLLUP (
        EXTRACT (YEAR FROM order_purchase_timestamp),
        EXTRACT (MONTH FROM order_purchase_timestamp),
        EXTRACT (DAY FROM order_purchase_timestamp)
    ) limit 10;





-- Task 2 
-- Query Processing and Optimization 
select * from order_items;
select * from sellers;
select count(*) from products where product_weight_g<100;
select count(*) from sellers where seller_zip_code_prefix<1050;


explain analyze select sellers.seller_id 
from order_items, sellers, products
where seller_zip_code_prefix < 1550
and products.product_weight_g < 100 
and order_items.seller_id=sellers.seller_id 
and order_items.product_id=products.product_id;
/*
QUERY PLAN
text
1 Nested Loop (cost=73.17..2349.89 rows=1 width=33) (actual time=1.704..2.386 rows=3 loops=1)
2 -> Hash Join (cost=72.75..2254.13 rows=34 width=66) (actual time=0.432..2.168 rows=39 loops=1)
3 Hash Cond: ((order_items.seller_id)::text = (sellers.seller_id)::text)
4 -> Seq Scan on order_items (cost=0.00..2178.39 rows=1139 width=66) (actual time=0.005..1.463 rows=1139 loops=1)
5 -> Hash (cost=71.60..71.60 rows=92 width=33) (actual time=0.373..0.373 rows=93 loops=1)
6 Buckets: 1024 Batches: 1 Memory Usage: 14kB
7 -> Seq Scan on sellers (cost=0.00..71.60 rows=92 width=33) (actual time=0.044..0.352 rows=93 loops=1)
8 Filter: (seller_zip_code_prefix < 1550)
9 Rows Removed by Filter: 2995
10 -> Index Scan using products_pkey on products (cost=0.41..2.82 rows=1 width=33) (actual time=0.005..0.005 rows=0 loops=39)
11 Index Cond: ((product_id)::text = (order_items.product_id)::text)
12 Filter: (product_weight_g < 100)
13 Rows Removed by Filter: 1
14 Planning Time: 0.429 ms
15 Execution Time: 2.441 ms
*/

explain analyze select sellers.seller_id 
from  sellers, order_items, products
where seller_zip_code_prefix < 1550
and products.product_weight_g < 100 
and order_items.seller_id=sellers.seller_id 
and order_items.product_id=products.product_id;

/*
QUERY PLAN
text
1 Nested Loop (cost=73.17..2349.89 rows=1 width=33) (actual time=2.088..3.383 rows=3 loops=1)
2 -> Hash Join (cost=72.75..2254.13 rows=34 width=66) (actual time=0.435..3.005 rows=39 loops=1)
3 Hash Cond: ((order_items.seller_id)::text = (sellers.seller_id)::text)
4 -> Seq Scan on order_items (cost=0.00..2178.39 rows=1139 width=66) (actual time=0.009..2.111 rows=1139 loops=1)
5 -> Hash (cost=71.60..71.60 rows=92 width=33) (actual time=0.401..0.401 rows=93 loops=1)
6 Buckets: 1024 Batches: 1 Memory Usage: 14kB
7 -> Seq Scan on sellers (cost=0.00..71.60 rows=92 width=33) (actual time=0.010..0.375 rows=93 loops=1)
8 Filter: (seller_zip_code_prefix < 1550)
9 Rows Removed by Filter: 2995
10 -> Index Scan using products_pkey on products (cost=0.41..2.82 rows=1 width=33) (actual time=0.008..0.008 rows=0 loops=39)
11 Index Cond: ((product_id)::text = (order_items.product_id)::text)
12 Filter: (product_weight_g < 100)
13 Rows Removed by Filter: 1
14 Planning Time: 0.418 ms
15 Execution Time: 3.436 ms
*/





--Task 3
-- Transaction processing

--Transaction 1
BEGIN;
UPDATE order_items SET price = price - '1' where product_id='1c0c0093a48f13ba70d0c6b0a9157cb7';
UPDATE order_items SET freight_value = freight_value + '1' where product_id='1c0c0093a48f13ba70d0c6b0a9157cb7';
COMMIT;
--rollback ;

-- Transaction2 
BEGIN;
UPDATE order_items SET price= price*0.5 where shipping_limit_date='2017-03-29' ;
UPDATE order_items SET freight_value = freight_value*0.5 where shipping_limit_date='2017-03-29';
COMMIT;



/* Serilizable transactions have the advantage of concurancy control of having like seperate transactions,
and allowing multiple operations at the same time unless it violates concurency conntrols.
*/



--SESSION-1
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;
UPDATE order_items SET price= price*1.1;
COMMIT;
ROLLBACK;

/* Do the followinng sessions on two different query with the given order.
-- SESSION 1 AND SESSION 2
SESSION-1:	BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;
SESSION2:	BEGIN;
SESSION2:	UPDATE order_items SET price= price+'2' ;
SESSION1:	UPDATE order_items SET price= price+'2' ;
SESSION2:	COMMIT;
ERROR:  could not serialize access due to concurrent update
SQL state: 40001
SESSION2:	COMMIT;
*/


-- Task 5
-- Indexing
select * from customers where customer_zip_code_prefix=1151;

create index zip_code_index on customers (customer_zip_code_prefix);

select min(customer_zip_code_prefix), avg(customer_zip_code_prefix), 
max(customer_zip_code_prefix), count(*)  from customers;

explain analyze select customer_id, customer_unique_id 
from customers where customer_zip_code_prefix<1500;

/*
QUERY PLANQUERY PLAN
text
1 Bitmap Heap Scan on customers (cost=25.43..1476.68 rows=1163 width=66) (actual time=6.704..41.108 rows=1416 loops=1)
2 Recheck Cond: (customer_zip_code_prefix < 1500)
3 Heap Blocks: exact=920
4 -> Bitmap Index Scan on zip_code_index (cost=0.00..25.14 rows=1163 width=0) (actual time=4.508..4.508 rows=1416 loops=1)
5 Index Cond: (customer_zip_code_prefix < 1500)
6 Planning Time: 0.335 ms
7 Execution Time: 41.383 ms
*/

/* That query tells that we have 1163 matching rows ((customer_zip_code_prefix < 1500)),
and they are located in 920 different blocks.
*/
