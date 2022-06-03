CREATE TEMPORARY TABLE temperature_measurements
(
    measurement_time TIMESTAMP(3),
    city             STRING,
    temperature      FLOAT,
    WATERMARK FOR measurement_time AS measurement_time - INTERVAL '15' SECONDS
)
    WITH ( 'connector' = 'faker', 'fields.measurement_time.expression' = '#{date.past ''15'',''SECONDS''}', 'fields.temperature.expression' = '#{number.numberBetween ''0'',''50''}', 'fields.city.expression' = '#{regexify ''(Chicago|Munich|Berlin|Portland|Hangzhou|Seatle|Beijing|New York){1}''}' );


select measurement_time
     , city
     , temperature
     , avg(temperature) over last_minute as avg_temp
from temperature_measurements window last_minute as
(partition by city
        order by measurement_time
    range between interval '1' minute preceding and current row)
;


/*
SELECT
  measurement_time,
  city,
  temperature,
  AVG(CAST(temperature AS FLOAT)) OVER last_minute AS avg_temperature_minute,
  MAX(temperature) OVER last_minute AS min_temperature_minute,
  MIN(temperature) OVER last_minute AS max_temperature_minute,
  STDDEV(CAST(temperature AS FLOAT)) OVER last_minute AS stdev_temperature_minute
FROM temperature_measurements WINDOW last_minute AS (
  PARTITION BY city
  ORDER BY measurement_time
  RANGE BETWEEN INTERVAL '1' MINUTE PRECEDING AND CURRENT ROW
);
*/
