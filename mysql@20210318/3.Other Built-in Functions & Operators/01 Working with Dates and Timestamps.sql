CREATE TABLE subscriptions (
    id STRING,
    start_date INT,
    end_date INT,
    payment_expiration TIMESTAMP(3)
) WITH (
  'connector' = 'faker',
  'fields.id.expression' = '#{Internet.uuid}',
  'fields.start_date.expression' = '#{number.numberBetween ''1576141834'',''1607764234''}',
  'fields.end_date.expression' = '#{number.numberBetween ''1609060234'',''1639300234''}',
  'fields.payment_expiration.expression' = '#{date.future ''365'',''DAYS''}'
);

SELECT
  id,
  TO_TIMESTAMP(FROM_UNIXTIME(start_date)) AS start_date,
  TO_TIMESTAMP(FROM_UNIXTIME(end_date)) AS end_date,
  DATE_FORMAT(payment_expiration,'YYYYww') AS exp_yweek,
  EXTRACT(DAY FROM payment_expiration) AS exp_day,     --same as DAYOFMONTH(ts)
  EXTRACT(MONTH FROM payment_expiration) AS exp_month, --same as MONTH(ts)
  EXTRACT(YEAR FROM payment_expiration) AS exp_year    --same as YEAR(ts)
FROM subscriptions
WHERE
  TIMESTAMPDIFF(DAY,CURRENT_TIMESTAMP,payment_expiration) < 30;