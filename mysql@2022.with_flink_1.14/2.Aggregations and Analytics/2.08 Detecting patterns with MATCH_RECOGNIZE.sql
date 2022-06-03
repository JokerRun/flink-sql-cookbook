CREATE TABLE subscriptions (
    id STRING,
    user_id INT,
    type STRING,
    start_date TIMESTAMP(3),
    end_date TIMESTAMP(3),
    payment_expiration TIMESTAMP(3),
    proc_time AS PROCTIME()
) WITH (
  'connector' = 'faker',
  'fields.id.expression' = '#{Internet.uuid}',
  'fields.user_id.expression' = '#{number.numberBetween ''1'',''50''}',
  'fields.type.expression'= '#{regexify ''(basic|premium|platinum){1}''}',
  'fields.start_date.expression' = '#{date.past ''30'',''DAYS''}',
  'fields.end_date.expression' = '#{date.future ''15'',''DAYS''}',
  'fields.payment_expiration.expression' = '#{date.future ''365'',''DAYS''}'
);

SELECT *
FROM subscriptions
     MATCH_RECOGNIZE (PARTITION BY user_id
                      ORDER BY proc_time
                      MEASURES
                        LAST(PREMIUM.type) AS premium_type,
                        AVG(TIMESTAMPDIFF(DAY,PREMIUM.start_date,PREMIUM.end_date)) AS premium_avg_duration,
                        BASIC.start_date AS downgrade_date
                      AFTER MATCH SKIP PAST LAST ROW
                      --Pattern: one or more 'premium' or 'platinum' subscription events (PREMIUM)
                      --followed by a 'basic' subscription event (BASIC) for the same `user_id`
                      PATTERN (PREMIUM+ BASIC)
                      DEFINE PREMIUM AS PREMIUM.type IN ('premium','platinum'),
                             BASIC AS BASIC.type = 'basic');