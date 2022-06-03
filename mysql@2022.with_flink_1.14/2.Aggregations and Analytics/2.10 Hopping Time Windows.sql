CREATE TABLE bids (
    bid_id STRING,
    currency_code STRING,
    bid_price DOUBLE,
    transaction_time TIMESTAMP(3),
    WATERMARK FOR transaction_time AS transaction_time - INTERVAL '5' SECONDS
) WITH (
  'connector' = 'faker',
  'fields.bid_id.expression' = '#{Internet.UUID}',
  'fields.currency_code.expression' = '#{regexify ''(EUR|USD|CNY)''}',
  'fields.bid_price.expression' = '#{Number.randomDouble ''2'',''1'',''150''}',
  'fields.transaction_time.expression' = '#{date.past ''30'',''SECONDS''}',
  'rows-per-second' = '100'
);
select window_start
     , window_time
     , currency_code
     , avg(bid_price) as avg_price
from table(hop(table bids, descriptor(transaction_time),interval '30' second, interval '1' minute))
group by window_start, window_end,window_time, currency_code;



/*
SELECT window_start, window_end, currency_code, ROUND(AVG(bid_price),2) AS MovingAverageBidPrice
  FROM TABLE(
    HOP(TABLE bids, DESCRIPTOR(transaction_time), INTERVAL '30' SECONDS, INTERVAL '1' MINUTE))
  GROUP BY window_start, window_end, currency_code;
*/