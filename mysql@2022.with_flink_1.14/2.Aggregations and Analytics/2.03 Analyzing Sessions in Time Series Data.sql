CREATE TABLE server_logs (
    client_ip STRING,
    client_identity STRING,
    userid STRING,
    log_time TIMESTAMP(3),
    request_line STRING,
    status_code STRING,
    WATERMARK FOR log_time AS log_time - INTERVAL '5' SECONDS
) WITH (
  'connector' = 'faker',
  'rows-per-second' = '5',
  'fields.client_ip.expression' = '#{Internet.publicIpV4Address}',
  'fields.client_identity.expression' =  '-',
  'fields.userid.expression' =  '#{regexify ''(morsapaes|knauf|sjwiesman){1}''}',
  'fields.log_time.expression' =  '#{date.past ''5'',''SECONDS''}',
  'fields.request_line.expression' = '#{regexify ''(GET|POST|PUT|PATCH){1}''} #{regexify ''(/search\.html|/login\.html|/prod\.html|cart\.html|/order\.html){1}''} #{regexify ''(HTTP/1\.1|HTTP/2|/HTTP/1\.0){1}''}',
  'fields.status_code.expression' = '#{regexify ''(200|201|204|400|401|403|301){1}''}'
);

SELECT
  userid,
  SESSION_START(log_time, INTERVAL '10' SECOND) AS session_beg,
  SESSION_ROWTIME(log_time, INTERVAL '10' SECOND) AS session_end,
  COUNT(request_line) AS request_cnt
FROM server_logs
WHERE status_code = '403'
GROUP BY
  userid,
  SESSION(log_time, INTERVAL '10' SECOND);

