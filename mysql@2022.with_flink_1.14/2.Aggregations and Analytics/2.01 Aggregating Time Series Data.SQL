CREATE TEMPORARY TABLE server_logs (
    client_ip STRING,
    client_identity STRING,
    userid STRING,
    request_line STRING,
    status_code STRING,
    log_time AS PROCTIME()
) WITH (
  'connector' = 'faker',
  'fields.client_ip.expression' = '#{Internet.publicIpV4Address}',
  'fields.client_identity.expression' =  '-',
  'fields.userid.expression' =  '-',
  'fields.log_time.expression' =  '#{date.past ''15'',''5'',''SECONDS''}',
  'fields.request_line.expression' = '#{regexify ''(GET|POST|PUT|PATCH){1}''} #{regexify ''(/search\.html|/login\.html|/prod\.html|cart\.html|/order\.html){1}''} #{regexify ''(HTTP/1\.1|HTTP/2|/HTTP/1\.0){1}''}',
  'fields.status_code.expression' = '#{regexify ''(200|201|204|400|401|403|301){1}''}'
);
-- group by window
SELECT
  COUNT(DISTINCT client_ip) AS ip_addresses,
  TUMBLE_PROCTIME(log_time, INTERVAL '1' MINUTE) AS window_interval
FROM server_logs
GROUP BY
  TUMBLE(log_time, INTERVAL '1' MINUTE);

-- group by window tvf
SELECT window_start
     , window_end
     , COUNT(DISTINCT client_ip) AS ip_addresses
FROM TABLE(TUMBLE(TABLE server_logs, DESCRIPTOR(log_time), INTERVAL '1' MINUTE))
GROUP BY window_start, window_end;

