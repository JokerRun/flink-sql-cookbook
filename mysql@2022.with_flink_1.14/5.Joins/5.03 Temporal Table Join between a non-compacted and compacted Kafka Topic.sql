/*
Temporal Join
为流水表与动态维度表间的join，动态维表记录存在更新时间，流水表需要准确关联到流水发生时间点的确切状态。
*/
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





CREATE TEMPORARY TABLE currency_rates_faker
WITH (
  'connector' = 'faker',
  'fields.currency_code.expression' = '#{Currency.code}',
  'fields.eur_rate.expression' = '#{Number.randomDouble ''4'',''0'',''10''}',
  'fields.rate_time.expression' = '#{date.past ''15'',''SECONDS''}',
  'rows-per-second' = '100'
) LIKE currency_rates (EXCLUDING OPTIONS);

INSERT INTO currency_rates SELECT * FROM currency_rates_faker;

CREATE TEMPORARY TABLE transactions_faker
WITH (
  'connector' = 'faker',
  'fields.id.expression' = '#{Internet.UUID}',
  'fields.currency_code.expression' = '#{Currency.code}',
  'fields.total.expression' = '#{Number.randomDouble ''2'',''10'',''1000''}',
  'fields.transaction_time.expression' = '#{date.past ''30'',''SECONDS''}',
  'rows-per-second' = '100'
) LIKE transactions (EXCLUDING OPTIONS);

INSERT INTO transactions SELECT * FROM transactions_faker;