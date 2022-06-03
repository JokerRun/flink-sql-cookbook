CREATE TABLE orders (
  id INT,
  order_time AS CURRENT_TIMESTAMP,
  WATERMARK FOR order_time AS order_time - INTERVAL '5' SECONDS
)
WITH (
  'connector' = 'datagen',
  'rows-per-second'='10',
  'fields.id.kind'='random',
  'fields.id.min'='1',
  'fields.id.max'='100'
);

with de_seq as (select id
                     , order_time
                     , row_number() as over(partition by id order by order_time desc ) as rn
                from orders)
select *
from de_seq
where rn = 1;

/*
--Check for duplicates in the `orders` table
SELECT id AS order_id,
       COUNT(*) AS order_cnt
FROM orders o
GROUP BY id
HAVING COUNT(*) > 1;

--Use deduplication to keep only the latest record for each `order_id`
SELECT
  order_id,
  order_time
FROM (
  SELECT id AS order_id,
         order_time,
         ROW_NUMBER() OVER (PARTITION BY id ORDER BY order_time) AS rownum
  FROM orders
     )
WHERE rownum = 1;
*/