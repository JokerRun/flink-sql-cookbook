CREATE TEMPORARY TABLE passengers (
  passenger_key STRING,
  first_name STRING,
  last_name STRING,
  update_time TIMESTAMP(3),
  WATERMARK FOR update_time AS update_time - INTERVAL '10' SECONDS,
  PRIMARY KEY (passenger_key) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'passengers',
  'properties.bootstrap.servers' = 'localhost:9092',
  'key.format' = 'raw',
  'value.format' = 'json'
);

CREATE TEMPORARY TABLE stations (
  station_key STRING,
  update_time TIMESTAMP(3),
  city STRING,
  WATERMARK FOR update_time AS update_time - INTERVAL '10' SECONDS,
  PRIMARY KEY (station_key) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'stations',
  'properties.bootstrap.servers' = 'localhost:9092',
  'key.format' = 'raw',
  'value.format' = 'json'
);

CREATE TEMPORARY TABLE booking_channels (
  booking_channel_key STRING,
  update_time TIMESTAMP(3),
  channel STRING,
  WATERMARK FOR update_time AS update_time - INTERVAL '10' SECONDS,
  PRIMARY KEY (booking_channel_key) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'booking_channels',
  'properties.bootstrap.servers' = 'localhost:9092',
  'key.format' = 'raw',
  'value.format' = 'json'
);

CREATE TEMPORARY TABLE train_activities (
  scheduled_departure_time TIMESTAMP(3),
  actual_departure_date TIMESTAMP(3),
  passenger_key STRING,
  origin_station_key STRING,
  destination_station_key STRING,
  booking_channel_key STRING,
  WATERMARK FOR actual_departure_date AS actual_departure_date - INTERVAL '10' SECONDS
) WITH (
  'connector' = 'kafka',
  'topic' = 'train_activities',
  'properties.bootstrap.servers' = 'localhost:9092',
  'value.format' = 'json',
  'value.fields-include' = 'ALL'
);

SELECT
  t.actual_departure_date,
  p.first_name,
  p.last_name,
  b.channel,
  os.city AS origin_station,
  ds.city AS destination_station
FROM train_activities t
LEFT JOIN booking_channels FOR SYSTEM_TIME AS OF t.actual_departure_date AS b
ON t.booking_channel_key = b.booking_channel_key;
LEFT JOIN passengers FOR SYSTEM_TIME AS OF t.actual_departure_date AS p
ON t.passenger_key = p.passenger_key
LEFT JOIN stations FOR SYSTEM_TIME AS OF t.actual_departure_date AS os
ON t.origin_station_key = os.station_key
LEFT JOIN stations FOR SYSTEM_TIME AS OF t.actual_departure_date AS ds
ON t.destination_station_key = ds.station_key