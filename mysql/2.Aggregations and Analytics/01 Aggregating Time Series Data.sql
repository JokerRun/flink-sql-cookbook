CREATE TABLE server_logs (
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
  'fields.status_code.expression' = '#{regexify ''(200|201|204|400|401|403|301){1}''}',
);

SELECT
  COUNT(DISTINCT client_ip) AS ip_addresses,
  TUMBLE_PROCTIME(log_time, INTERVAL '1' MINUTE) AS window_interval
FROM server_logs
GROUP BY
  TUMBLE(log_time, INTERVAL '1' MINUTE)