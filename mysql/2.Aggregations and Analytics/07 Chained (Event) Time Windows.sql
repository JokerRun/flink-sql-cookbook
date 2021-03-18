CREATE TEMPORARY TABLE server_logs (
    log_time TIMESTAMP(3),
    client_ip STRING,
    client_identity STRING,
    userid STRING,
    request_line STRING,
    status_code STRING,
    size INT,
    WATERMARK FOR log_time AS log_time - INTERVAL '15' SECONDS
) WITH (
  'connector' = 'faker',
  'fields.client_ip.expression' = '#{Internet.publicIpV4Address}',
  'fields.client_identity.expression' =  '-',
  'fields.log_time.expression' =  '#{date.past ''15'',''5'',''SECONDS''}',
  'fields.request_line.expression' = '#{regexify ''(GET|POST|PUT|PATCH){1}''} #{regexify ''(/search\.html|/login\.html|/prod\.html|cart\.html|/order\.html){1}''} #{regexify ''(HTTP/1\.1|HTTP/2|/HTTP/1\.0){1}''}',
  'fields.status_code.expression' = '#{regexify ''(200|201|204|400|401|403|301){1}''}',
  'fields.size.expression' = '#{number.numberBetween ''100'',''10000000''}'
);

CREATE TEMPORARY TABLE avg_request_size_1m (
  window_start TIMESTAMP(3),
  window_end TIMESTAMP(3),
  avg_size BIGINT
)
WITH (
  'connector' = 'blackhole'
);

CREATE TEMPORARY TABLE avg_request_size_5m (
  window_start TIMESTAMP(3),
  window_end TIMESTAMP(3),
  avg_size BIGINT
)
WITH (
  'connector' = 'blackhole'
);

CREATE TEMPORARY VIEW server_logs_window_1m AS
SELECT
  TUMBLE_START(log_time, INTERVAL '1' MINUTE) AS window_start,
  TUMBLE_ROWTIME(log_time, INTERVAL '1' MINUTE) AS window_end,
  SUM(size) AS total_size,
  COUNT(*) AS num_requests
FROM server_logs
GROUP BY
  TUMBLE(log_time, INTERVAL '1' MINUTE);


CREATE TEMPORARY VIEW server_logs_window_5m AS
SELECT
  TUMBLE_START(window_end, INTERVAL '5' MINUTE) AS window_start,
  TUMBLE_ROWTIME(window_end, INTERVAL '5' MINUTE) AS window_end,
  SUM(total_size) AS total_size,
  SUM(num_requests) AS num_requests
FROM server_logs_window_1m
GROUP BY
  TUMBLE(window_end, INTERVAL '5' MINUTE);

BEGIN STATEMENT SET;

INSERT INTO avg_request_size_1m SELECT
  window_start,
  window_end,
  total_size/num_requests AS avg_size
FROM server_logs_window_1m;

INSERT INTO avg_request_size_5m SELECT
  window_start,
  window_end,
  total_size/num_requests AS avg_size
FROM server_logs_window_5m;

END;