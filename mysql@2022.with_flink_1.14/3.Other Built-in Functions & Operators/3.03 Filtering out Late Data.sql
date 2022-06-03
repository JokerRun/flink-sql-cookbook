-- Create source table
CREATE TABLE `mobile_usage` (
    `activity` STRING,
    `client_ip` STRING,
    `ingest_time` AS PROCTIME(),
    `log_time` TIMESTAMP_LTZ(3),
    WATERMARK FOR log_time AS log_time - INTERVAL '15' SECONDS
) WITH (
  'connector' = 'faker',
  'rows-per-second' = '50',
  'fields.activity.expression' = '#{regexify ''(open_push_message|discard_push_message|open_app|display_overview|change_settings)''}',
  'fields.client_ip.expression' = '#{Internet.publicIpV4Address}',
  'fields.log_time.expression' =  '#{date.past ''45'',''10'',''SECONDS''}'
);

-- Create sink table for rows that are non-late
CREATE TABLE `unique_users_per_window` (
    `window_start` TIMESTAMP(3),
    `window_end` TIMESTAMP(3),
    `ip_addresses` BIGINT
) WITH (
  'connector' = 'blackhole'
);

-- Create sink table for rows that are late
CREATE TABLE `late_usage_events` (
    `activity` STRING,
    `client_ip` STRING,
    `ingest_time` TIMESTAMP_LTZ(3),
    `log_time` TIMESTAMP_LTZ(3),
    `current_watermark` TIMESTAMP_LTZ(3)
) WITH (
  'connector' = 'blackhole'
);

-- Create a view with non-late data
CREATE TEMPORARY VIEW `mobile_data` AS
    SELECT * FROM mobile_usage
    WHERE CURRENT_WATERMARK(log_time) IS NULL
          OR log_time > CURRENT_WATERMARK(log_time);

-- Create a view with late data
CREATE TEMPORARY VIEW `late_mobile_data` AS
    SELECT * FROM mobile_usage
        WHERE CURRENT_WATERMARK(log_time) IS NOT NULL
              AND log_time <= CURRENT_WATERMARK(log_time);

BEGIN STATEMENT SET;

-- Send all rows that are non-late to the sink for data that's on time
INSERT INTO `unique_users_per_window`
    SELECT `window_start`, `window_end`, COUNT(DISTINCT client_ip) AS `ip_addresses`
      FROM TABLE(
        TUMBLE(TABLE mobile_data, DESCRIPTOR(log_time), INTERVAL '1' MINUTE))
      GROUP BY window_start, window_end;

-- Send all rows that are late to the sink for late data
INSERT INTO `late_usage_events`
    SELECT *, CURRENT_WATERMARK(log_time) as `current_watermark` from `late_mobile_data`;

END;