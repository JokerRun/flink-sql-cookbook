CREATE TABLE server_logs (
    client_ip STRING,
    client_identity STRING,
    userid STRING,
    user_agent STRING,
    log_time TIMESTAMP(3),
    request_line STRING,
    status_code STRING,
    size INT
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

-- Sample user_agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/7046A194A
-- Regex pattern: '[^\/]+' (Match everything before '/')
SELECT
  REGEXP_EXTRACT(user_agent,'[^\/]+') AS browser,
  status_code,
  COUNT(*) AS cnt_status
FROM server_logs
GROUP BY
  REGEXP_EXTRACT(user_agent,'[^\/]+'),
  status_code;