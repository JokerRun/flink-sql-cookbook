CREATE TABLE orders (
  id INT,
  order_time AS TIMESTAMPADD(DAY, CAST(FLOOR(RAND()*(1-5+1)+5)*(-1) AS INT), CURRENT_TIMESTAMP)
)
WITH (
  'connector' = 'datagen',
  'rows-per-second'='10',
  'fields.id.kind'='sequence',
  'fields.id.start'='1',
  'fields.id.end'='1000'
);


CREATE TABLE shipments (
  id INT,
  order_id INT,
  shipment_time AS TIMESTAMPADD(DAY, CAST(FLOOR(RAND()*(1-5+1)) AS INT), CURRENT_TIMESTAMP)
)
WITH (
  'connector' = 'datagen',
  'rows-per-second'='5',
  'fields.id.kind'='random',
  'fields.id.min'='0',
  'fields.order_id.kind'='sequence',
  'fields.order_id.start'='1',
  'fields.order_id.end'='1000'
);

SELECT
  o.id AS order_id,
  o.order_time,
  s.shipment_time,
  TIMESTAMPDIFF(DAY,o.order_time,s.shipment_time) AS day_diff
FROM orders o
JOIN shipments s ON o.id = s.order_id
WHERE
    o.order_time BETWEEN s.shipment_time - INTERVAL '3' DAY AND s.shipment_time;