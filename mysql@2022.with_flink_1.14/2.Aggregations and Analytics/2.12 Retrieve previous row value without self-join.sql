CREATE TABLE fake_stocks (
    stock_name STRING,
    stock_value double,
    log_time AS PROCTIME()
) WITH (
  'connector' = 'faker',
  'fields.stock_name.expression' = '#{regexify ''(Deja\ Brew|Jurassic\ Pork|Lawn\ \&\ Order|Pita\ Pan|Bread\ Pitt|Indiana\ Jeans|Thai\ Tanic){1}''}',
  'fields.stock_value.expression' =  '#{number.randomDouble ''2'',''10'',''20''}',
  'fields.log_time.expression' =  '#{date.past ''15'',''5'',''SECONDS''}',
  'rows-per-second' = '10'
);
/*
we're going to create a table which contains stock ticker updates for which
we want to determine if the new stock price has gone up or down compared to its previous value.
*/
with src as (select stock_name
                  , stock_value
                  , log_time
                  , lag(stock_value) over(partition by stock_name order by log_time) as previous
             from fake_stocks)
select stock_name
     , log_time
     , stock_value
     , previous
     , stock_value - previous as diff
from src
;


/*
WITH current_and_previous as (
    select
        stock_name,
        log_time,
        stock_value,
        lag(stock_value, 1) over (partition by stock_name order by log_time) previous_value
    from fake_stocks
)
select *,
    case
        when stock_value > previous_value then '▲'
        when stock_value < previous_value then '▼'
        else '='
    end as trend
from current_and_previous;*/