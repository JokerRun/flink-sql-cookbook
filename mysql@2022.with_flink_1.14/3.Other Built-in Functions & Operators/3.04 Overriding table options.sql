CREATE TABLE `airports` (
  `IATA_CODE` CHAR(3),
  `AIRPORT` STRING,
  `CITY` STRING,
  `STATE` CHAR(2),
  `COUNTRY` CHAR(3),
  `LATITUDE` DOUBLE NULL,
  `LONGITUDE` DOUBLE NULL,
  PRIMARY KEY (`IATA_CODE`) NOT ENFORCED
) WITH (
  'connector' = 'filesystem',
  'path' = 'file:///flink-sql-cookbook/other-builtin-functions/04_override_table_options/airports.csv',
  'format' = 'csv'
);


SELECT * FROM `airports` /*+ OPTIONS('csv.ignore-parse-errors'='true') */;

SELECT * FROM `airports` /*+ OPTIONS('csv.ignore-parse-errors'='true') */ WHERE `LATITUDE` IS NULL;

SELECT * FROM `your_kafka_topic` /*+ OPTIONS('scan.startup.mode'='group-offsets') */;