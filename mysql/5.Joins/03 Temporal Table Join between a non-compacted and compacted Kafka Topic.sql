CREATE TEMPORARY TABLE currency_rates (
  `currency_code` STRING,
  `eur_rate` DECIMAL(6,4),
  `rate_time` TIMESTAMP(3),
  WATERMARK FOR `rate_time` AS rate_time - INTERVAL '15' SECONDS,
  PRIMARY KEY (currency_code) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'currency_rates',
  'properties.bootstrap.servers' = 'localhost:9092',
  'key.format' = 'raw',
  'value.format' = 'json'
);

CREATE TEMPORARY TABLE transactions (
  `id` STRING,
  `currency_code` STRING,
  `total` DECIMAL(10,2),
  `transaction_time` TIMESTAMP(3),
  WATERMARK FOR `transaction_time` AS transaction_time - INTERVAL '30' SECONDS
) WITH (
  'connector' = 'kafka',
  'topic' = 'transactions',
  'properties.bootstrap.servers' = 'localhost:9092',
  'key.format' = 'raw',
  'key.fields' = 'id',
  'value.format' = 'json',
  'value.fields-include' = 'ALL'
);

SELECT
  t.id,
  t.total * c.eur_rate AS total_eur,
  t.total,
  c.currency_code,
  t.transaction_time
FROM transactions t
JOIN currency_rates FOR SYSTEM_TIME AS OF t.transaction_time AS c
ON t.currency_code = c.currency_code;