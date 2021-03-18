CREATE TEMPORARY TABLE server_logs (
    client_ip       STRING,
    client_identity STRING,
    userid          STRING,
    user_agent      STRING,
    log_time        TIMESTAMP(3),
    request_line    STRING,
    status_code     STRING,
    size            INT,
    WATERMARK FOR log_time AS log_time - INTERVAL '30' SECONDS
) WITH (
  'connector' = 'faker',
  'fields.client_ip.expression' = '#{Internet.publicIpV4Address}',
  'fields.client_identity.expression' =  '-',
  'fields.userid.expression' =  '-',
  'fields.user_agent.expression' = '#{Internet.userAgentAny}',
  'fields.log_time.expression' =  '#{date.past ''15'',''5'',''SECONDS''}',
  'fields.request_line.expression' = '#{regexify ''(GET|POST|PUT|PATCH){1}''} #{regexify ''(/search\.html|/login\.html|/prod\.html|cart\.html|/order\.html){1}''} #{regexify ''(HTTP/1\.1|HTTP/2|/HTTP/1\.0){1}''}',
  'fields.status_code.expression' = '#{regexify ''(200|201|204|400|401|403|301){1}''}',
  'fields.size.expression' = '#{number.numberBetween ''100'',''10000000''}'
);

CREATE TEMPORARY TABLE realtime_aggregations (
  `browser`     STRING,
  `status_code` STRING,
  `end_time`    TIMESTAMP(3),
  `requests`    BIGINT NOT NULL
) WITH (
  'connector' = 'kafka',
  'topic' = 'browser-status-codes',
  'properties.bootstrap.servers' = 'localhost:9092',
  'properties.group.id' = 'browser-countds',
  'format' = 'avro'
);


CREATE TEMPORARY TABLE offline_datawarehouse (
    `browser`     STRING,
    `status_code` STRING,
    `dt`          STRING,
    `hour`        STRING,
    `requests`    BIGINT NOT NULL
) PARTITIONED BY (`dt`, `hour`) WITH (
  'connector' = 'filesystem',
  'path' = 's3://my-bucket/browser-into',
  'sink.partition-commit.trigger' = 'partition-time',
  'format' = 'parquet'
);

-- This is a shared view that will be used by both
-- insert into statements
CREATE TEMPORARY VIEW browsers AS
SELECT
  REGEXP_EXTRACT(user_agent,'[^\/]+') AS browser,
  status_code,
  log_time
FROM server_logs;

BEGIN STATEMENT SET;
INSERT INTO realtime_aggregations
SELECT
    browser,
    status_code,
    TUMBLE_ROWTIME(log_time, INTERVAL '5' MINUTE) AS end_time,
    COUNT(*) requests
FROM browsers
GROUP BY
    browser,
    status_code,
    TUMBLE(log_time, INTERVAL '5' MINUTE);
INSERT INTO offline_datawarehouse
SELECT
    browser,
    status_code,
    DATE_FORMAT(TUMBLE_ROWTIME(log_time, INTERVAL '1' HOUR), 'yyyy-MM-dd') AS `dt`,
    DATE_FORMAT(TUMBLE_ROWTIME(log_time, INTERVAL '1' HOUR), 'HH') AS `hour`,
    COUNT(*) requests
FROM browsers
GROUP BY
    browser,
    status_code,
    TUMBLE(log_time, INTERVAL '1' HOUR);
END;