# Apache Flink SQL Cookbook

 The [Apache Flink SQL](https://docs.ververica.com/user_guide/sql_development/index.html) Cookbook is a curated collection of examples, patterns, and use cases of Apache Flink SQL. 
 Many of the recipes are completely self-contained and can be run in [Ververica Platform](https://docs.ververica.com/index.html) as is.

The cookbook is a living document. :seedling: 

## Table of Contents

### Foundations

1. [Creating Tables](foundations/01_create_table/01_create_table.md)
2. [Inserting Into Tables](foundations/02_insert_into/02_insert_into.md)
3. [Working with Temporary Tables](foundations/03_temporary_table/03_temporary_table.md)
4. [Filtering Data](foundations/04_where/04_where.md)
5. [Aggregating Data](foundations/05_group_by/05_group_by.md)
6. [Sorting Tables](foundations/06_order_by/06_order_by.md)
7. [Encapsulating Logic with (Temporary) Views](foundations/07_views/07_views.md)
8. [Writing Results into Multiple Tables](foundations/08_statement_sets/08_statement_sets.md)
9. [Convert timestamps with timezones](foundations/09_convert_timezones/09_convert_timezones.md)

### Aggregations and Analytics

1. [Aggregating Time Series Data](aggregations-and-analytics/01_group_by_window/01_group_by_window_tvf.md)
2. [Watermarks](aggregations-and-analytics/02_watermarks/02_watermarks.md)
3. [Analyzing Sessions in Time Series Data](aggregations-and-analytics/03_group_by_session_window/03_group_by_session_window.md)
4. [Rolling Aggregations on Time Series Data](aggregations-and-analytics/04_over/04_over.md)
5. [Continuous Top-N](aggregations-and-analytics/05_top_n/05_top_n.md)
6. [Deduplication](aggregations-and-analytics/06_dedup/06_dedup.md)
7. [Chained (Event) Time Windows](aggregations-and-analytics/07_chained_windows/07_chained_windows.md)
8. [Detecting Patterns with MATCH_RECOGNIZE](aggregations-and-analytics/08_match_recognize/08_match_recognize.md)
9. [Maintaining Materialized Views with Change Data Capture (CDC) and Debezium](aggregations-and-analytics/09_cdc_materialized_view/09_cdc_materialized_view.md)
10. [Hopping Time Windows](aggregations-and-analytics/10_hopping_time_windows/10_hopping_time_windows.md)
11. [Window Top-N](aggregations-and-analytics/11_window_top_n/11_window_top_n.md)
12. [Retrieve previous row value without self-join](aggregations-and-analytics/12_lag/12_lag.md)

### Other Built-in Functions & Operators

1. [Working with Dates and Timestamps](other-builtin-functions/01_date_time/01_date_time.md)
2. [Building the Union of Multiple Streams](other-builtin-functions/02_union-all/02_union-all.md)
3. [Filtering out Late Data](other-builtin-functions/03_current_watermark/03_current_watermark.md)
4. [Overriding table options](other-builtin-functions/04_override_table_options/04_override_table_options.md)
5. [Expanding arrays into new rows](other-builtin-functions/05_expanding_arrays/05_expanding_arrays.md)
6. [Split strings into maps](other-builtin-functions/06_split_strings_into_maps/06_split_strings_into_maps.md)

### User-Defined Functions (UDFs)

1. [Extending SQL with Python UDFs](udfs/01_python_udfs/01_python_udfs.md)

### Joins

1. [Regular Joins](joins/01_regular_joins/01_regular_joins.md)
2. [Interval Joins](joins/02_interval_joins/02_interval_joins.md)
3. [Temporal Table Join between a non-compacted and compacted Kafka Topic](joins/03_kafka_join/03_kafka_join.md)
4. [Lookup Joins](joins/04_lookup_joins/04_lookup_joins.md)
5. [Star Schema Denormalization (N-Way Join)](joins/05_star_schema/05_star_schema.md)
6. [Lateral Table Join](joins/06_lateral_join/06_lateral_join.md)

### Former Recipes

1. [Aggregating Time Series Data (Before Flink 1.13)](aggregations-and-analytics/01_group_by_window/01_group_by_window.md)

## About Apache Flink

Apache Flink is an open source stream processing framework with powerful stream- and batch-processing capabilities.

Learn more about Flink at https://flink.apache.org/.

## License 

Copyright © 2020-2022 Ververica GmbH

Distributed under Apache License, Version 2.0.



# Foundations

# 01 Creating Tables

> :bulb: This example will show how to create a table using SQL DDL.

Flink SQL operates against logical tables, just like a traditional database.
However, it does not maintain tables internally but always operates against external systems.

Table definitions are in two parts; the logical schema and connector configuration. The logical schema defines the columns and types in the table and is what queries operate against. 
The connector configuration is contained in the `WITH` clause and defines the physical system that backs this table. 
This example uses the `datagen` connector which generates rows in memory and is convenient for testing queries.

You can test the table is properly created by running a simple `SELECT` statement. 
In Ververica Platform you will see the results printed to the UI in the query preview.

## Script

```sql
CREATE TABLE orders (
    order_uid  BIGINT,
    product_id BIGINT,
    price      DECIMAL(32, 2),
    order_time TIMESTAMP(3)
) WITH (
    'connector' = 'datagen'
);

SELECT * FROM orders;
```

## Example Output

![01_create_table](https://user-images.githubusercontent.com/23521087/105504017-913eee80-5cc7-11eb-868c-7b78b1b95b71.png)



# 02 Inserting Into Tables

> :bulb: This recipe shows how to insert rows into a table so that downstream applications can read them.

The source table (`server_logs`) is backed by the [`faker` connector](https://flink-packages.org/packages/flink-faker), which continuously generates rows in memory based on Java Faker expressions.

As outlined in [the first recipe](../01_create_table/01_create_table.md) Flink SQL operates on tables, that are stored in external systems.
To publish results of a query for consumption by downstream applications, you write the results of a query into a table. 
This table can be read by Flink SQL, or directly by connecting to the external system that is storing the data (e.g. an ElasticSearch index.)

This example takes the `server_logs` tables, filters for client errors, and writes these logs into another table called `client_errors`.
Any number of external systems could back the result table, including Apache Kafka, Apache Hive, ElasticSearch, JDBC, among many others. 
To keep this example self-contained, `client_errors` is of type `blackhole`: instead of actually writing the data to an external system, the table discards any rows written to it.

## Script

```sql
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

CREATE TABLE client_errors (
  log_time TIMESTAMP(3),
  request_line STRING,
  status_code STRING,
  size INT
)
WITH (
  'connector' = 'blackhole'
);

INSERT INTO client_errors
SELECT 
  log_time,
  request_line,
  status_code,
  size
FROM server_logs
WHERE 
  status_code SIMILAR TO '4[0-9][0-9]';
```

## Example Output

An INSERT INTO query that reads from an unbounded table (like `server_logs`) is a long-running application. 
When you run such a statement in Apache Flink's [SQL Client](https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/sqlClient.html) a Flink Job will be submitted to the configured cluster. 
In Ververica Platform a so called Deployment will be created to manage the execution of the statement.

![Screenshot GIF](https://user-images.githubusercontent.com/11538663/101192280-22480080-365b-11eb-97e9-35f151027c6e.gif)



# 03 Working with Temporary Tables

![Twitter Badge](https://img.shields.io/badge/Flink%20Version-1.11%2B-lightgrey)

> :bulb: This example will show how and why to create a temporary table using SQL DDL.

Non-temporary tables in Flink SQL are stored in a catalog, while temporary tables only live within the current session (Apache Flink CLI) or script (Ververica Platform). 
You can use a temporary table instead of a regular (catalog) table, if it is only meant to be used within the current session or script.

This example is exactly the same as [Inserting Into Tables](../02_insert_into/02_insert_into.md) except that both `server_logs` and `client_errors` are created as temporary tables.

### Why Temporary Tables?

For result tables like `client_errors` that no one can ever read from (because of its type `blackhole`) it makes a lot of sense to use a temporary table instead of publishing its metadata in a catalog. 

Furthermore, temporary tables allow you to create fully self-contained scripts, which is why we will mostly use those in the Flink SQL Cookbook.

## Script

```sql

CREATE TEMPORARY TABLE server_logs ( 
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

CREATE TEMPORARY TABLE client_errors (
  log_time TIMESTAMP(3),
  request_line STRING,
  status_code STRING,
  size INT
)
WITH (
  'connector' = 'blackhole'
);

INSERT INTO client_errors
SELECT 
  log_time,
  request_line,
  status_code,
  size
FROM server_logs
WHERE 
  status_code SIMILAR TO '4[0-9][0-9]';
```

## Example Output

In comparison to [Inserting Into Tables](../02_insert_into/02_insert_into.md), you can see that the two temporary tables do not appear in the catalog browser on the left. 
The table definitions never make it into the catalog, but are just submitted as part of the script that contains the `INSERT INTO` statement.

![Screencast GIF](https://user-images.githubusercontent.com/11538663/101192652-aac6a100-365b-11eb-82a3-5b86522e772c.gif)


# 04 Filtering Data

> :bulb: This example will show how to filter server logs in real-time using a standard `WHERE` clause.

The table it uses, `server_logs`,  is backed by the [`faker` connector](https://flink-packages.org/packages/flink-faker) which continuously generates rows in memory based on Java Faker expressions and is convenient for testing queries. 
As such, it is an alternative to the built-in `datagen` connector used for example in [the first recipe](../01_create_table/01_create_table.md).

You can continuously filter these logs for those requests that experience authx issues with a simple `SELECT` statement with a `WHERE` clause filtering on the auth related HTTP status codes. 
In Ververica Platform you  will see the results printed to the UI in the query preview.

## Script

```sql
CREATE TABLE server_logs ( 
    client_ip STRING,
    client_identity STRING, 
    userid STRING, 
    log_time TIMESTAMP(3),
    request_line STRING, 
    status_code STRING, 
    size INT
) WITH (
  'connector' = 'faker', 
  'fields.client_ip.expression' = '#{Internet.publicIpV4Address}',
  'fields.client_identity.expression' =  '-',
  'fields.userid.expression' =  '-',
  'fields.log_time.expression' =  '#{date.past ''15'',''5'',''SECONDS''}',
  'fields.request_line.expression' = '#{regexify ''(GET|POST|PUT|PATCH){1}''} #{regexify ''(/search\.html|/login\.html|/prod\.html|cart\.html|/order\.html){1}''} #{regexify ''(HTTP/1\.1|HTTP/2|/HTTP/1\.0){1}''}',
  'fields.status_code.expression' = '#{regexify ''(200|201|204|400|401|403|301){1}''}',
  'fields.size.expression' = '#{number.numberBetween ''100'',''10000000''}'
);

SELECT 
  log_time, 
  request_line,
  status_code 
FROM server_logs
WHERE
  status_code IN ('403', '401');
```

## Example Output

![04_where](https://user-images.githubusercontent.com/23521087/105504095-a6b41880-5cc7-11eb-9606-978e86add144.png)

# 05 Aggregating Data

> :bulb: This example will show how to aggregate server logs in real-time using the standard `GROUP BY` clause.

The source table (`server_logs`) is backed by the [`faker` connector](https://flink-packages.org/packages/flink-faker), which continuously generates rows in memory based on Java Faker expressions.

To count the number of logs received per browser for each status code _over time_, you can combine the `COUNT` aggregate function with a `GROUP BY` clause. Because the `user_agent` field contains a lot of information, you can extract the browser using the built-in [string function](https://ci.apache.org/projects/flink/flink-docs-stable/docs/dev/table/functions/systemfunctions/#string-functions) `REGEXP_EXTRACT`.

A `GROUP BY` on a streaming table produces an updating result, so you will see the aggregated count for each browser continuously changing as new rows flow in.

> As an exercise, you can play around with other standard SQL aggregate functions (e.g. `SUM`,`AVG`,`MIN`,`MAX`).

## Script

```sql
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
```

## Example Output

This example can be run in the [SQL Client](https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/sqlClient.html), a command line tool to develop and execute Flink SQL queries that is bundled in Flink.

![03_group_by](https://user-images.githubusercontent.com/23521087/101014385-19293780-3566-11eb-9d81-9c99d6ffa7e4.gif)

# 06 Sorting Tables 

> :bulb: This example will show how you can sort a table, particularly unbounded tables. 

Flink SQL supports `ORDER BY`. 
Bounded Tables can be sorted by any column, descending or ascending. 

To use `ORDER BY` on unbounded tables like `server_logs` the primary sorting key needs to be a [time attribute](https://docs.ververica.com/user_guide/sql_development/table_view.html#time-attributes) like `log_time`.

In first example below, we are sorting the `server_logs` by `log_time`. 
The second example is a bit more advanced: 
It sorts the number of logs per minute and browser by the `window_time` (a time attribute) and the `cnt_browser` (descending), so that the browser with the highest number of logs is at the top of each window.

## Script

```sql
CREATE TEMPORARY TABLE server_logs ( 
    client_ip STRING,
    client_identity STRING, 
    userid STRING, 
    user_agent STRING,
    log_time TIMESTAMP(3),
    request_line STRING, 
    status_code STRING, 
    size INT, 
    WATERMARK FOR log_time AS log_time - INTERVAL '15' SECONDS
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

SELECT * FROM server_logs 
ORDER BY log_time;
```

## Example Output

![06_order_by](https://user-images.githubusercontent.com/23521087/105504299-e24ee280-5cc7-11eb-8935-ed203e604f8d.png)

## Advanced Example

<details>
    <summary>Advanced Example </summary>

### Script

```sql
CREATE TEMPORARY TABLE server_logs ( 
    client_ip STRING,
    client_identity STRING, 
    userid STRING, 
    user_agent STRING,
    log_time TIMESTAMP(3),
    request_line STRING, 
    status_code STRING, 
    size INT, 
    WATERMARK FOR log_time AS log_time - INTERVAL '15' SECONDS
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

SELECT 
  TUMBLE_ROWTIME(log_time, INTERVAL '1' MINUTE) AS window_time,
  REGEXP_EXTRACT(user_agent,'[^\/]+') AS browser,
  COUNT(*) AS cnt_browser
FROM server_logs
GROUP BY 
  REGEXP_EXTRACT(user_agent,'[^\/]+'),
  TUMBLE(log_time, INTERVAL '1' MINUTE)
ORDER BY
  window_time,
  cnt_browser DESC;
```

### Example Output

![06_order_by_advanced](https://user-images.githubusercontent.com/23521087/105504249-d5ca8a00-5cc7-11eb-984a-e1eaf6622995.png)

</details>

# 07 Encapsulating Logic with (Temporary) Views

![Twitter Badge](https://img.shields.io/badge/Flink%20Version-1.11%2B-lightgrey)

> :bulb: This example will show how you can use (temporary) views to reuse code and to structure long queries and scripts.

`CREATE (TEMPORARY) VIEW` defines a view from a query.
**A view is not physically materialized.**
Instead, the query is run every time the view is referenced in a query.

Temporary views are very useful to structure and decompose more complicated queries and to re-use queries within a longer script.
Non-temporary views - stored in a persistent [catalog](https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/catalogs.html) - can also be used to share common queries within your organization, e.g. common
filters or pre-processing steps.

Here, we create a view on the `server_logs` that only contains successful requests.
This view encapsulates the logic of filtering the logs based on certain `status_code`s.
This logic can subsequently be used by any query or script that has access to the catalog.

## Script

```sql
/*
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
*/

CREATE VIEW successful_requests AS
SELECT *
FROM server_logs
WHERE status_code SIMILAR TO '[2,3][0-9][0-9]';

SELECT *
FROM successful_requests;
```

## Example Output

![views](https://user-images.githubusercontent.com/11538663/102009292-c5250c80-3d36-11eb-85b3-05b8faf8df5a.gif)


# 08 Writing Results into Multiple Tables

![Twitter Badge](https://img.shields.io/badge/Flink%20Version-1.13%2B-lightgrey)

> :bulb: In this recipe, you will learn how to use [Statement Sets](https://docs.ververica.com/user_guide/sql_development/sql_scripts.html#sql-statements) to run multiple `INSERT INTO` statements in a single, optimized Flink Job. 

Many product requirements involve outputting the results of a streaming application to two or more sinks, such as [Apache Kafka](https://docs.ververica.com/user_guide/sql_development/connectors.html#apache-kafka) for real-time use cases, or a [Filesystem](https://docs.ververica.com/user_guide/sql_development/connectors.html#file-system) for offline ones.
Other times, two queries are not the same but share some extensive intermediate operations.

When working with server logs, the support team would like to see the number of status codes per browser every 5 minutes to have real-time insights into a web pages' status.
Additionally, they would like the same information on an hourly basis made available as partitioned [Apache Parquet](https://docs.ververica.com/user_guide/sql_development/connectors.html#apache-parquet) files so they can perform historical analysis. 

We could quickly write two Flink SQL queries to solve both these requirements, but that would not be efficient. 
These queries have a lot of duplicated work, like reading the source logs Kafka topic and cleansing the data. 

Ververica Platform includes a feature called `STATEMENT SET`s, that allows for multiplexing `INSERT INTO` statements into a single query holistically optimized by Apache Flink and deployed as a single application. 

```sql
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
```

Looking at the deployed Job Graph, we can see Flink SQL only performs the shared computation once to achieve the most cost and resource-efficient execution of our query!

![08_jobgraph](https://user-images.githubusercontent.com/23521087/105504375-fb579380-5cc7-11eb-888e-12a1ce7d6f50.png)

# 09 Convert timestamps with timezones

![Twitter Badge](https://img.shields.io/badge/Flink%20Version-1.19%2B-lightgrey)

> :bulb: In this recipe, you will learn how to consolidate timestamps with different time zones to UTC. 

Timestamps in incoming data can refer to different time zones and consolidating them to the same time zone (e.g. UTC) is a prerequisite to ensure correctness in temporal analysis.

The source table (`iot_status`) is backed by the [`faker` connector](https://flink-packages.org/packages/flink-faker), which continuously generates fake IoT status messages in memory based on Java Faker expressions.

In this recipe we create a table which contains IoT devices status updates including timestamp and device time zone, which we'll convert to UTC. 

We create the table first, then use the [`CONVERT_TZ`](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/functions/systemfunctions/#temporal-functions) function to convert the timestamp to UTC. The `CONVERT_TZ` function requires the input timestamp to be passed as string, thus we apply the cast function to `iot_timestamp`.

```sql
CREATE TABLE iot_status ( 
    device_ip       STRING,
    device_timezone STRING,
    iot_timestamp   TIMESTAMP(3),
    status_code     STRING
) WITH (
  'connector' = 'faker', 
  'fields.device_ip.expression' = '#{Internet.publicIpV4Address}',
  'fields.device_timezone.expression' =  '#{regexify ''(America\/Los_Angeles|Europe\/Rome|Europe\/London|Australia\/Sydney){1}''}',
  'fields.iot_timestamp.expression' =  '#{date.past ''15'',''5'',''SECONDS''}',
  'fields.status_code.expression' = '#{regexify ''(OK|KO|WARNING){1}''}',
  'rows-per-second' = '3'
);

SELECT 
  device_ip, 
  device_timezone,
  iot_timestamp,
  convert_tz(cast(iot_timestamp as string), device_timezone, 'UTC') iot_timestamp_utc,
  status_code
FROM iot_status;
```

The 

## Example Output

![09_convert_timezones](https://bucket-jerry-1.oss-cn-shanghai.aliyuncs.com/uPic/09_convert_timezones2022%2006.gif)



# Aggregations and Analytics

# 01 Aggregating Time Series Data

![Twitter Badge](https://img.shields.io/badge/Flink%20Version-1.13%2B-lightgrey)

> :bulb: This example will show how to aggregate time series data in real-time using a `TUMBLE` window.

The source table (`server_logs`) is backed by the [`faker` connector](https://flink-packages.org/packages/flink-faker), which continuously generates rows in memory based on Java Faker expressions.

Many streaming applications work with time series data.
To count the number of `DISTINCT` IP addresses seen each minute, rows need to be grouped based on a [time attribute](https://docs.ververica.com/user_guide/sql_development/table_view.html#time-attributes).
Grouping based on time is special, because time always moves forward, which means Flink can generate final results after the minute is completed.

`TUMBLE` is a [built-in function](https://ci.apache.org/projects/flink/flink-docs-stable/docs/dev/table/sql/queries/window-agg/) for grouping timestamps into time intervals called windows. Windows split the stream into “buckets” of finite size, over which we can apply computations.
Unlike other aggregations such as `HOP` or `CUMULATE`, it will only produce a single final result for each key when the interval is completed.

If the logs do not have a timestamp, one can be generated using a [computed column](https://docs.ververica.com/user_guide/sql_development/table_view.html#computed-column).
`log_time AS PROCTIME()` will append a column to the table with the current system time.

## Script

```sql
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
  'fields.request_line.expression' = '#{regexify ''(GET|POST|PUT|PATCH){1}''} #{regexify ''(/search\.html|/login\.html|/prod\.html|cart\.html|/order\.html){1}''} #{regexify ''(HTTP/1\.1|HTTP/2|/HTTP/1\.0){1}''}',
  'fields.status_code.expression' = '#{regexify ''(200|201|204|400|401|403|301){1}''}'
);

SELECT window_start, window_end, COUNT(DISTINCT client_ip) AS ip_addresses
  FROM TABLE(
    TUMBLE(TABLE server_logs, DESCRIPTOR(log_time), INTERVAL '1' MINUTE))
  GROUP BY window_start, window_end;
```

## Example Output

![01_group_by_window](https://bucket-jerry-1.oss-cn-shanghai.aliyuncs.com/uPic/01_group_by_window_tvf_result2022%2006.png)

# 02 Watermarks

![Twitter Badge](https://img.shields.io/badge/Flink%20Version-1.10%2B-lightgrey)

> :bulb: This example will show how to use `WATERMARK`s to work with timestamps in records. 

The source table (`doctor_sightings`) is backed by the [`faker` connector](https://flink-packages.org/packages/flink-faker), which continuously generates rows in memory based on Java Faker expressions.

The [previous recipe](../01_group_by_window/01_group_by_window.md) showed how a `TUMBLE` group window makes it simple to aggregate time-series data.	 

[The Doctor](https://tardis.fandom.com/wiki/The_Doctor) is a renegade time lord who travels through space and time in a [TARDIS](https://tardis.fandom.com/wiki/The_Doctor%27s_TARDIS).
As different versions of the Doctor travel through time, various people log their sightings.
We want to track how many times each version of the Doctor is seen each minute. 	 
Unlike the previous recipe, these records have an embedded timestamp we need to use to perform our calculation. 	 

More often than not, most data will come with embedded timestamps that we want to use for our time series calculations.	We call this timestamp an [event-time attribute](https://ci.apache.org/projects/flink/flink-docs-stable/docs/learn-flink/streaming_analytics/#event-time-and-watermarks).	 

Event time represents when something actually happened in the real world.
And it is unique because it is quasi-monotonically increasing; we generally see things that happened earlier before seeing things that happen later. Of course, data will never be perfectly ordered (systems go down, networks are laggy, doctor sighting take time to postmark and mail), and there will be some out-of-orderness in our data. 	 

Flink can account for all these variabilities using a [WATERMARK](https://docs.ververica.com/user_guide/sql_development/table_view.html#event-time) attribute in the tables DDL. The watermark signifies a column as the table's event time attribute and tells Flink how out of order we expect our data. 	 

In the Doctor's case, we expect all records to arrive within 15 seconds when the sighting occurs.

## Script

```sql
CREATE TABLE doctor_sightings (
  doctor        STRING,
  sighting_time TIMESTAMP(3),
  WATERMARK FOR sighting_time AS sighting_time - INTERVAL '15' SECONDS
)
WITH (
  'connector' = 'faker', 
  'fields.doctor.expression' = '#{dr_who.the_doctors}',
  'fields.sighting_time.expression' = '#{date.past ''15'',''SECONDS''}'
);

SELECT 
    doctor,
    TUMBLE_ROWTIME(sighting_time, INTERVAL '1' MINUTE) AS sighting_time,
    COUNT(*) AS sightings
FROM doctor_sightings
GROUP BY 
    TUMBLE(sighting_time, INTERVAL '1' MINUTE),
    doctor;
```

## Example Output

![02_watermarks](https://user-images.githubusercontent.com/23521087/105503592-12e24c80-5cc7-11eb-9155-243cc9c314f0.png)

# 03 Analyzing Sessions in Time Series Data

> :bulb: This example will show how to aggregate time-series data in real-time using a `SESSION` window.

The source table (`server_logs`) is backed by the [`faker` connector](https://flink-packages.org/packages/flink-faker), which continuously generates rows in memory based on Java Faker expressions.

#### What are Session Windows?

In a [previous recipe](../01_group_by_window/01_group_by_window.md), you learned about _tumbling windows_. Another way to group time-series data is using [_session windows_](https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/sql/queries.html#group-windows), which aggregate records into _sessions_ that represent periods of activity followed by gaps of idleness. Think, for example, of user sessions on a website: a user will be active for a given period of time, then leave the website; and each user will be active at different times. To analyze user behaviour, it's useful to aggregate their actions on the website for each period of activity (i.e. _session_).

Unlike tumbling windows, session windows don't have a fixed duration and are tracked independenlty across keys (i.e. windows of different keys will have different durations).

#### Using Session Windows

To count the number of "Forbidden" (403) requests per user over the duration of a session, you can use the `SESSION` built-in group window function. In this example, a session is bounded by a gap of idleness of 10 seconds (`INTERVAL '10' SECOND`). This means that requests that occur within 10 seconds of the last seen request for each user will be merged into the same session window; and any request that occurs outside of this gap will trigger the creation of a new session window.

> Tip: You can use the `SESSION_START` and `SESSION_ROWTIME` [auxiliary functions](https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/sql/queries.html#selecting-group-window-start-and-end-timestamps) to check the lower and upper bounds of session windows.


## Script

```sql
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
```

## Example Output

![03_session_windows](https://user-images.githubusercontent.com/23521087/101628701-7ae31900-3a20-11eb-89c2-231649b7d99f.png)

# 04 Rolling Aggregations on Time Series Data

> :bulb: This example will show how to calculate an aggregate or cumulative value based on a group of rows using an `OVER` window. A typical use case are rolling aggregations.

The source table (`temperature_measurements`) is backed by the [`faker` connector](https://flink-packages.org/packages/flink-faker), which continuously generates rows in memory based on Java Faker expressions.

OVER window aggregates compute an aggregated value for every input row over a range of ordered rows. 
In contrast to GROUP BY aggregates, OVER aggregates do not reduce the number of result rows to a single row for every group. 
Instead, OVER aggregates produce an aggregated value for every input row.

The order needs to be defined by a [time attribute](https://docs.ververica.com/user_guide/sql_development/table_view.html#time-attributes). 
The range of rows can be defined by a number of rows or a time interval. 

In this example, we are trying to identify outliers in the `temperature_measurements` table. 
For this, we use an `OVER` window to calculate, for each measurement, the maximum (`MAX`), minimum (`MIN`) and average (`AVG`) temperature across all measurements, as well as the standard deviation (`STDDEV`), for the same city over the previous minute. 
> As an exercise, you can try to write another query to filter out any temperature measurement that are higher or lower than the average by more than four standard deviations.

## Script

```sql
CREATE TEMPORARY TABLE temperature_measurements (
  measurement_time TIMESTAMP(3),
  city STRING,
  temperature FLOAT, 
  WATERMARK FOR measurement_time AS measurement_time - INTERVAL '15' SECONDS
)
WITH (
  'connector' = 'faker',
  'fields.measurement_time.expression' = '#{date.past ''15'',''SECONDS''}',
  'fields.temperature.expression' = '#{number.numberBetween ''0'',''50''}',
  'fields.city.expression' = '#{regexify ''(Chicago|Munich|Berlin|Portland|Hangzhou|Seatle|Beijing|New York){1}''}'
);

SELECT 
  measurement_time,
  city, 
  temperature,
  AVG(CAST(temperature AS FLOAT)) OVER last_minute AS avg_temperature_minute,
  MAX(temperature) OVER last_minute AS min_temperature_minute,
  MIN(temperature) OVER last_minute AS max_temperature_minute,
  STDDEV(CAST(temperature AS FLOAT)) OVER last_minute AS stdev_temperature_minute
FROM temperature_measurements 
WINDOW last_minute AS (
  PARTITION BY city
  ORDER BY measurement_time
  RANGE BETWEEN INTERVAL '1' MINUTE PRECEDING AND CURRENT ROW 
);
```
## Example Output

![04_over](https://user-images.githubusercontent.com/23521087/105503670-2beafd80-5cc7-11eb-9e58-7a4ed71b1d7c.png)

# 05 Continuous Top-N

![Twitter Badge](https://img.shields.io/badge/Flink%20Version-1.9%2B-lightgrey)

> :bulb: This example will show how to continuously calculate the "Top-N" rows based on a given attribute, using an `OVER` window and the `ROW_NUMBER()` function.

The source table (`spells_cast`) is backed by the [`faker` connector](https://flink-packages.org/packages/flink-faker), which continuously generates rows in memory based on Java Faker expressions.

The Ministry of Magic tracks every spell a wizard casts throughout Great Britain and wants to know every wizard's Top 2 all-time favorite spells. 

Flink SQL can be used to calculate continuous [aggregations](../../foundations/05_group_by/05_group_by.md), so if we know
each spell a wizard has cast, we can maintain a continuous total of how many times they have cast that spell. 

```sql
SELECT wizard, spell, COUNT(*) AS times_cast
FROM spells_cast
GROUP BY wizard, spell;
```

This result can be used in an `OVER` window to calculate a [Top-N](https://docs.ververica.com/user_guide/sql_development/queries.html#top-n).
The rows are partitioned using the `wizard` column, and are then ordered based on the count of spell casts (`times_cast DESC`). 
The built-in function `ROW_NUMBER()` assigns a unique, sequential number to each row, starting from one, according to the rows' ordering within the partition.
Finally, the results are filtered for only those rows with a `row_num <= 2` to find each wizard's top 2 favorite spells. 

Where Flink is most potent in this query is its ability to issue retractions.
As wizards cast more spells, their top 2 will change. 
When this occurs, Flink will issue a retraction, modifying its output, so the result is always correct and up to date. 


```sql
CREATE TABLE spells_cast (
    wizard STRING,
    spell  STRING
) WITH (
  'connector' = 'faker',
  'fields.wizard.expression' = '#{harry_potter.characters}',
  'fields.spell.expression' = '#{harry_potter.spells}'
);

SELECT wizard, spell, times_cast
FROM (
    SELECT *,
    ROW_NUMBER() OVER (PARTITION BY wizard ORDER BY times_cast DESC) AS row_num
    FROM (SELECT wizard, spell, COUNT(*) AS times_cast FROM spells_cast GROUP BY wizard, spell)
)
WHERE row_num <= 2;  
```

![05_top_n](https://user-images.githubusercontent.com/23521087/105503736-3e653700-5cc7-11eb-9ddf-9a89d93841bc.png)

# 06 Deduplication

> :bulb: This example will show how you can identify and filter out duplicates in a stream of events.

There are different ways that duplicate events can end up in your data sources, from human error to application bugs. Regardless of the origin, unclean data can have a real impact in the quality (and correctness) of your results. Suppose that your order system occasionally generates duplicate events with the same `order_id`, and that you're only interested in keeping the most recent event for downstream processing.

As a first step, you can use a combination of the `COUNT` function and the `HAVING` clause to check if and which orders have more than one event; and then filter out these events using `ROW_NUMBER()`. In practice, deduplication is a special case of [Top-N aggregation](../05_top_n/05_top_n.md), where N is 1 (`rownum = 1`) and the ordering column is either the processing or event time of events.

## Script

The source table `orders` is backed by the built-in [`datagen` connector](https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/connectors/datagen.html), which continuously generates rows in memory.

```sql
CREATE TABLE orders (
  id INT,
  order_time AS CURRENT_TIMESTAMP,
  WATERMARK FOR order_time AS order_time - INTERVAL '5' SECONDS
)
WITH (
  'connector' = 'datagen',
  'rows-per-second'='10',
  'fields.id.kind'='random',
  'fields.id.min'='1',
  'fields.id.max'='100'
);

--Check for duplicates in the `orders` table
SELECT id AS order_id,
       COUNT(*) AS order_cnt
FROM orders o
GROUP BY id
HAVING COUNT(*) > 1;

--Use deduplication to keep only the latest record for each `order_id`
SELECT
  order_id,
  order_time
FROM (
  SELECT id AS order_id,
         order_time,
         ROW_NUMBER() OVER (PARTITION BY id ORDER BY order_time) AS rownum
  FROM orders
     )
WHERE rownum = 1;
```

## Example Output

![20_dedup](https://user-images.githubusercontent.com/23521087/102718503-b87d5700-42e8-11eb-8b45-4f9908e8e14e.gif)

# 07 Chained (Event) Time Windows

> :bulb: This example will show how to efficiently aggregate time series data on two different levels of granularity.

The source table (`server_logs`) is backed by the [`faker` connector](https://flink-packages.org/packages/flink-faker), which continuously generates rows in memory based on Java Faker expressions.

Based on our `server_logs` table we would like to compute the average request size over one minute **as well as five minute (event) windows.** 
For this, you could run two queries, similar to the one in [Aggregating Time Series Data](../01_group_by_window/01_group_by_window.md). 
At the end of the page is the script and resulting JobGraph from this approach. 

In the main part, we will follow a slightly more efficient approach that chains the two aggregations: the one-minute aggregation output serves as the five-minute aggregation input.

We then use a [Statements Set](../../foundations/08_statement_sets/08_statement_sets.md) to write out the two result tables. 
To keep this example self-contained, we use two tables of type `blackhole` instead of `kafka`, `filesystem`, or any other [connectors](https://ci.apache.org/projects/flink/flink-docs-stable/docs/connectors/table/overview/). 

## Script

```sql
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
  'fields.log_time.expression' =  '#{date.past ''15'',''5'',''SECONDS''}',
  'fields.client_ip.expression' = '#{Internet.publicIpV4Address}',
  'fields.client_identity.expression' =  '-',
  'fields.userid.expression' =  '-',
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
```

## Example Output

### JobGraph

![jobgraph_chained](https://user-images.githubusercontent.com/23521087/105503848-5e94f600-5cc7-11eb-9a7f-2944dd4e1faf.png)

### Result 1 Minute Aggregations

![results_1m](https://user-images.githubusercontent.com/23521087/105503896-6c4a7b80-5cc7-11eb-958d-05d48c9921cf.png)

## Non-Chained Windows

<details>
    <summary>Non-Chained Windows</summary>

### Script

```shell script
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
  'fields.userid.expression' =  '-',
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
  TUMBLE_START(log_time, INTERVAL '5' MINUTE) AS window_start,
  TUMBLE_ROWTIME(log_time, INTERVAL '5' MINUTE) AS window_end,
  SUM(size) AS total_size,
  COUNT(*) AS num_requests
FROM server_logs
GROUP BY 
  TUMBLE(log_time, INTERVAL '5' MINUTE);

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
```

### Example Output

#### JobGraph

![jobgraph_non_chained](https://user-images.githubusercontent.com/23521087/105503946-79676a80-5cc7-11eb-9e8e-15d39482fee9.png)

</details>

# 08 Detecting patterns with MATCH_RECOGNIZE

> :bulb: This example will show how you can use Flink SQL to detect patterns in a stream of events with `MATCH_RECOGNIZE`.

A common (but historically complex) task in SQL day-to-day work is to identify meaningful sequences of events in a data set — also known as Complex Event Processing (CEP). This becomes even more relevant when dealing with streaming data, as you want to react quickly to known patterns or changing trends to deliver up-to-date business insights. In Flink SQL, you can easily perform this kind of tasks using the standard SQL clause [`MATCH_RECOGNIZE`](https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/streaming/match_recognize.html).

## Breaking down MATCH_RECOGNIZE

In this example, you want to find users that downgraded their service subscription from one of the premium tiers (`type IN ('premium','platinum')`) to the basic tier. 

#### Input

The input argument of `MATCH_RECOGNIZE` will be a row pattern table based on `subscriptions`. As a first step, logical partitioning and ordering must be applied to the input row pattern table to ensure that event processing is correct and deterministic:

```sql
PARTITION BY user_id 
ORDER BY proc_time
```

#### Output

Row pattern columns are then defined in the `MEASURES` clause, which can be thought of as the `SELECT` of `MATCH_RECOGNIZE`. If you're interested in getting the type of premium subscription associated with the last event before the downgrade, you can fetch it using the [logical offset](https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/streaming/match_recognize.html#logical-offsets) operator `LAST`. The downgrade date can be extrapolated from the `start_date` of the first basic subscription event following any existing premium one(s).

```sql
MEASURES
  LAST(PREMIUM.type) AS premium_type,
  AVG(TIMESTAMPDIFF(DAY,PREMIUM.start_date,PREMIUM.end_date)) AS premium_avg_duration,
  BASIC.start_date AS downgrade_date
AFTER MATCH SKIP PAST LAST ROW
```

#### Pattern Definition

Patterns are specified in the `PATTERN` clause using row-pattern variables (i.e. event types) and regular expressions. These variables must also be associated with the matching conditions that events must meet to be included in the pattern, using the `DEFINE` clause. Here, you are interested in matching one or more premium subscription events (`PREMIUM+`) followed by a basic subscription event (`BASIC`):

```sql
PATTERN (PREMIUM+ BASIC)
DEFINE PREMIUM AS PREMIUM.type IN ('premium','platinum'),
BASIC AS BASIC.type = 'basic');
```

## Script

The source table (`subscriptions`) is backed by the [`faker` connector](https://flink-packages.org/packages/flink-faker), which continuously generates rows in memory based on Java Faker expressions.

```sql
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
```

## Example Output

![23_match_recognize](https://user-images.githubusercontent.com/2392216/108039085-ee665f80-703b-11eb-93f9-f8e3b684f315.png)

# 09 Maintaining Materialized Views with Change Data Capture (CDC) and Debezium

![Twitter Badge](https://img.shields.io/badge/Flink%20Version-1.11%2B-lightgrey)

> :bulb: This example will show how you can use Flink SQL and Debezium to maintain a materialized view based on database changelog streams.

In the world of analytics, databases are still mostly seen as static sources of data — like a collection of business state(s) just sitting there, waiting to be queried. The reality is that most of the data stored in these databases is continuously produced and is continuously changing, so...why not _stream_ it? 

Change Data Capture (CDC) allows you to do just that: track and propagate changes in a database based on its changelog (e.g. the [Write-Ahead-Log](https://www.postgresql.org/docs/current/wal-intro.html) in Postgres) to downstream consumers. [Debezium](https://debezium.io/) is a popular tool for CDC that Flink supports through **1)** the [Kafka SQL Connector](https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/connectors/formats/debezium.html) and **2)** a set of "standalone" [Flink CDC Connectors](https://github.com/ververica/flink-cdc-connectors#flink-cdc-connectors).

#### Let's get to it!

In this example, you'll monitor a table with insurance claim data related to animal attacks in Australia, and use Flink SQL to maintain an aggregated **materialized view** that is **incrementally updated** with the latest claim costs. You can find a different version of this example deploying Debezium, Kafka and Kafka Connect in [this repository](https://github.com/morsapaes/flink-sql-CDC).

## Pre-requisites

You'll need a running Postgres service to follow this example, so we bundled everything up in a `docker-compose` script to keep it self-contained. The only pre-requisite is to have [Docker](https://docs.docker.com/get-docker/) installed on your machine. :whale:

To get the setup up and running, run:

`docker-compose build`

`docker-compose up -d`

Once all the services are up, you can start the Flink SQL client:

`docker-compose exec sql-client ./sql-client.sh`

## How it works

The source table is backed by the [`Flink CDC Postgres` connector](https://github.com/ververica/flink-cdc-connectors/wiki/Postgres-CDC-Connector), which reads the transaction log of the `postgres` database to continuously produce change events. So, whenever there is an `INSERT`, `UPDATE` or `DELETE` operation in the `claims.accident_claims` table, it will be propagated to Flink.

```sql
CREATE TABLE accident_claims (
    claim_id INT,
    claim_total FLOAT,
    claim_total_receipt VARCHAR(50),
    claim_currency VARCHAR(3),
    member_id INT,
    accident_date VARCHAR(20),
    accident_type VARCHAR(20),
    accident_detail VARCHAR(20),
    claim_date VARCHAR(20),
    claim_status VARCHAR(10),
    ts_created VARCHAR(20),
    ts_updated VARCHAR(20)
) WITH (
  'connector' = 'postgres-cdc',
  'hostname' = 'postgres',
  'port' = '5432',
  'username' = 'postgres',
  'password' = 'postgres',
  'database-name' = 'postgres',
  'schema-name' = 'claims',
  'table-name' = 'accident_claims'
 );
```

After creating the changelog table, you can query it to find out the aggregated insurance costs of all cleared claims per animal type (`accident_detail`):

```sql
SELECT accident_detail,
       SUM(claim_total) AS agg_claim_costs
FROM accident_claims
WHERE claim_status <> 'DENIED'
GROUP BY accident_detail;
```

How can you check that the CDC functionality is _actually_ working? The `docker` directory also includes a data generator script with a series of `INSERT` operations with new insurance claims (`postgres_datagen.sql`); if you run it, you can see how the query results update in (near) real-time:

`cat ./postgres_datagen.sql | docker exec -i flink-cdc-postgres psql -U postgres -d postgres`

In contrast to what would happen in a non-streaming SQL engine, using Flink SQL in combination with CDC allows you to get a consistent and continuous view of the state of the world, instead of a snapshot up to a specific point in time (i.e. the query's execution time).

## Example Output

![09_cdc_materialized_view](https://user-images.githubusercontent.com/23521087/109818653-81ee8180-7c33-11eb-9a76-b1004de8fe23.gif)

# 10 Hopping Time Windows

![Twitter Badge](https://img.shields.io/badge/Flink%20Version-1.13%2B-lightgrey)

> :bulb: This example will show how to calculate a moving average in real-time using a `HOP` window.

The source table (`bids`) is backed by the [`faker` connector](https://flink-packages.org/packages/flink-faker), which continuously generates rows in memory based on Java Faker expressions.

In one of our previous recipes we've shown how you can [aggregate time series data](../01_group_by_window/01_group_by_window_tvf.md) using `TUMBLE`. 
To display every 30 seconds the moving average of bidding prices per currency per 1 minute, we will use the built-in `HOP` [function](https://ci.apache.org/projects/flink/flink-docs-stable/docs/dev/table/sql/queries/window-agg/).

The difference between a `HOP` and a `TUMBLE` function is that with a `HOP` you can "jump" forward in time. That's why you have to specify both the length of the window and the interval you want to jump forward. 
When using a `HOP` function, records can be assigned to multiple windows if the interval is smaller than the window length, like in this example. A tumbling window never overlaps and records will only belong to one window.  

## Script

```sql
CREATE TABLE bids ( 
    bid_id STRING,
    currency_code STRING,
    bid_price DOUBLE, 
    transaction_time TIMESTAMP(3),
    WATERMARK FOR transaction_time AS transaction_time - INTERVAL '5' SECONDS
) WITH (
  'connector' = 'faker',
  'fields.bid_id.expression' = '#{Internet.UUID}',
  'fields.currency_code.expression' = '#{regexify ''(EUR|USD|CNY)''}',
  'fields.bid_price.expression' = '#{Number.randomDouble ''2'',''1'',''150''}',
  'fields.transaction_time.expression' = '#{date.past ''30'',''SECONDS''}',
  'rows-per-second' = '100'
);

SELECT window_start, window_end, currency_code, ROUND(AVG(bid_price),2) AS MovingAverageBidPrice
  FROM TABLE(
    HOP(TABLE bids, DESCRIPTOR(transaction_time), INTERVAL '30' SECONDS, INTERVAL '1' MINUTE))
  GROUP BY window_start, window_end, currency_code;
```

## Example Output

![01_group_by_window](https://bucket-jerry-1.oss-cn-shanghai.aliyuncs.com/uPic/10_hopping_time_windows2022%2006.png)

# 11 Window Top-N

![Twitter Badge](https://img.shields.io/badge/Flink%20Version-1.13%2B-lightgrey)

> :bulb: This example will show how to calculate the Top 3 suppliers who have the highest sales for every tumbling 5 minutes window.

The source table (`orders`) is backed by the [`faker` connector](https://flink-packages.org/packages/flink-faker), which continuously generates rows in memory based on Java Faker expressions.

In our previous recipes we've shown how you can [aggregate time series data](../01_group_by_window/01_group_by_window_tvf.md) using the `TUMBLE` function and also how you can get continuous [Top-N results](../05_top_n/05_top_n.md).
In this recipe, you will use the `Window Top-N` [feature](https://ci.apache.org/projects/flink/flink-docs-stable/docs/dev/table/sql/queries/window-topn/) to display the top 3 suppliers with the highest sales every 5 minutes. 

The difference between the regular Top-N and this Window Top-N, is that Window Top-N only emits final results, which is the total top N records at the end of the window. 

```sql
CREATE TABLE orders ( 
    bidtime TIMESTAMP(3),
    price DOUBLE, 
    item STRING,
    supplier STRING,
    WATERMARK FOR bidtime AS bidtime - INTERVAL '5' SECONDS
) WITH (
  'connector' = 'faker',
  'fields.bidtime.expression' = '#{date.past ''30'',''SECONDS''}',
  'fields.price.expression' = '#{Number.randomDouble ''2'',''1'',''150''}',
  'fields.item.expression' = '#{Commerce.productName}',
  'fields.supplier.expression' = '#{regexify ''(Alice|Bob|Carol|Alex|Joe|James|Jane|Jack)''}',
  'rows-per-second' = '100'
);

SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY window_start, window_end ORDER BY price DESC) as rownum
        FROM (
            SELECT window_start, window_end, supplier, SUM(price) as price, COUNT(*) as cnt
            FROM TABLE(
                TUMBLE(TABLE orders, DESCRIPTOR(bidtime), INTERVAL '10' MINUTES))
            GROUP BY window_start, window_end, supplier
        )
    ) WHERE rownum <= 3;
```

## Example Output

![11_window_top_n](https://bucket-jerry-1.oss-cn-shanghai.aliyuncs.com/uPic/11_window_top_n2022%2006.png)

# 12 Retrieve previous row value without self-join

![Twitter Badge](https://img.shields.io/badge/Flink%20Version-1.19%2B-lightgrey)

> :bulb: This example will show how to retrieve the previous value and compute trends for a specific data partition.

The source table (`fake_stocks`) is backed by the [`faker` connector](https://flink-packages.org/packages/flink-faker), which continuously generates fake stock quotation in memory based on Java Faker expressions.

In this recipe we're going to create a table which contains stock ticker updates for which we want to determine if the new stock price has gone up or down compared to its previous value. 

First we create the table, then use a select statement including the [LAG](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/functions/systemfunctions/#aggregate-functions) function to retrieve the previous stock value. Finally using the `case` statement in the final select we compare the current stock price against the previous value to determine the trend.

```sql
CREATE TABLE fake_stocks ( 
    stock_name STRING,
    stock_value double, 
    log_time AS PROCTIME()
) WITH (
  'connector' = 'faker', 
  'fields.stock_name.expression' = '#{regexify ''(Deja\ Brew|Jurassic\ Pork|Lawn\ \&\ Order|Pita\ Pan|Bread\ Pitt|Indiana\ Jeans|Thai\ Tanic){1}''}',
  'fields.stock_value.expression' =  '#{number.randomDouble ''2'',''10'',''20''}',
  'fields.log_time.expression' =  '#{date.past ''15'',''5'',''SECONDS''}',
  'rows-per-second' = '10'
);

WITH current_and_previous as (
    select 
        stock_name,
        log_time, 
        stock_value, 
        lag(stock_value, 1) over (partition by stock_name order by log_time) previous_value 
    from fake_stocks
)
select *, 
    case 
        when stock_value > previous_value then '▲'
        when stock_value < previous_value then '▼'
        else '=' 
    end as trend 
from current_and_previous;
```

## Example Output

![12_lag](https://bucket-jerry-1.oss-cn-shanghai.aliyuncs.com/uPic/12_lag2022%2006.gif)

# Other Built-in Functions & Operators
# 01 Working with Dates and Timestamps

> :bulb: This example will show how to use [built-in date and time functions](https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/functions/systemFunctions.html#temporal-functions) to manipulate temporal fields.

The source table (`subscriptions`) is backed by the [`faker` connector](https://flink-packages.org/packages/flink-faker), which continuously generates rows in memory based on Java Faker expressions.

#### Date and Time Functions

Working with dates and timestamps is commonplace in SQL, but your input may come in different types, formats or even timezones. Flink SQL has multiple built-in functions that are useful to deal with this kind of situation and make it convenient to handle temporal fields.

Assume you have a table with service subscriptions and that you want to continuously [filter](../../foundations/04_where/04_where.md) these subscriptions to find the ones that have associated payment methods expiring in less than 30 days. The `start_date` and `end_date` are [Unix timestamps](https://en.wikipedia.org/wiki/Unix_time) (i.e. epochs) — which are not very human-readable and should be converted. Also, you want to parse the `payment_expiration` timestamp into its corresponding day, month and year parts. What are some functions that would be useful?

* `TO_TIMESTAMP(string[, format])`: converts a `STRING` value to a `TIMESTAMP` using the specified format (default: 'yyyy-MM-dd HH:mm:ss')

* `FROM_UNIXTIME(numeric[, string])`: converts an epoch to a formatted `STRING` (default: 'yyyy-MM-dd HH:mm:ss')

* `DATE_FORMAT(timestamp, string)`: converts a `TIMESTAMP` to a `STRING` using the specified format

* `EXTRACT(timeintervalunit FROM temporal)`: returns a `LONG` extracted from the specified date part of a temporal field (e.g. `DAY`,`MONTH`,`YEAR`)

* `TIMESTAMPDIFF(unit, timepoint1, timepoint2)`: returns the number of time units (`SECOND`, `MINUTE`, `HOUR`, `DAY`, `MONTH` or `YEAR`) between `timepoint1` and `timepoint2`

* `CURRENT_TIMESTAMP`: returns the current SQL timestamp (UTC)

For a complete list of built-in date and time functions, check the Flink [documentation](https://ci.apache.org/projects/flink/flink-docs-stable/docs/dev/table/functions/systemfunctions/#temporal-functions).

> As an exercise, you can try to reproduce the same filtering condition using `TIMESTAMPADD` instead.

## Script

```sql
CREATE TABLE subscriptions ( 
    id STRING,
    start_date INT,
    end_date INT,
    payment_expiration TIMESTAMP(3)
) WITH (
  'connector' = 'faker',
  'fields.id.expression' = '#{Internet.uuid}', 
  'fields.start_date.expression' = '#{number.numberBetween ''1576141834'',''1607764234''}',
  'fields.end_date.expression' = '#{number.numberBetween ''1609060234'',''1639300234''}',
  'fields.payment_expiration.expression' = '#{date.future ''365'',''DAYS''}'
);

SELECT 
  id,
  TO_TIMESTAMP(FROM_UNIXTIME(start_date)) AS start_date,
  TO_TIMESTAMP(FROM_UNIXTIME(end_date)) AS end_date,
  DATE_FORMAT(payment_expiration,'YYYYww') AS exp_yweek,
  EXTRACT(DAY FROM payment_expiration) AS exp_day,     --same as DAYOFMONTH(ts)
  EXTRACT(MONTH FROM payment_expiration) AS exp_month, --same as MONTH(ts)
  EXTRACT(YEAR FROM payment_expiration) AS exp_year    --same as YEAR(ts)
FROM subscriptions
WHERE 
  TIMESTAMPDIFF(DAY,CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)),payment_expiration) < 30;
```

## Example Output

![12_date_time](https://user-images.githubusercontent.com/23521087/101981480-811a0500-3c6d-11eb-9b28-5603d76ba0e6.png)
# 02 Building the Union of Multiple Streams

> :bulb: This example will show how you can use the set operation `UNION ALL` to combine several streams of data.

See [our documentation](https://ci.apache.org/projects/flink/flink-docs-stable/docs/dev/table/sql/queries/set-ops/)
for a full list of fantastic set operations Apache Flink supports.


## The Sources

The examples assumes you are building an application that is tracking visits :fox_face: on foreign planets :chestnut:. 
There are three sources of visits. The universe of Rick and Morty, the very real world of NASA and such, 
and the not so real world of Hitchhikers Guide To The Galaxy.

All three tables are `unbounded` and backed by the [`faker` connector](https://flink-packages.org/packages/flink-faker).

All sources of tracked visits have the `location` and `visit_time` in common. Some have `visitors`, some have
`spacecrafts` and one has both.

```sql
CREATE TEMPORARY TABLE rickandmorty_visits ( 
    visitor STRING,
    location STRING, 
    visit_time TIMESTAMP(3)
) WITH (
  'connector' = 'faker', 
  'fields.visitor.expression' = '#{RickAndMorty.character}',
  'fields.location.expression' =  '#{RickAndMorty.location}',
  'fields.visit_time.expression' =  '#{date.past ''15'',''5'',''SECONDS''}'
);

CREATE TEMPORARY TABLE spaceagency_visits ( 
    spacecraft STRING,
    location STRING, 
    visit_time TIMESTAMP(3)
) WITH (
  'connector' = 'faker', 
  'fields.spacecraft.expression' = '#{Space.nasaSpaceCraft}',
  'fields.location.expression' =  '#{Space.star}',
  'fields.visit_time.expression' =  '#{date.past ''15'',''5'',''SECONDS''}'
);

CREATE TEMPORARY TABLE hitchhiker_visits ( 
    visitor STRING,
    starship STRING,
    location STRING, 
    visit_time TIMESTAMP(3)
) WITH (
  'connector' = 'faker', 
  'fields.visitor.expression' = '#{HitchhikersGuideToTheGalaxy.character}',
  'fields.starship.expression' = '#{HitchhikersGuideToTheGalaxy.starship}',
  'fields.location.expression' =  '#{HitchhikersGuideToTheGalaxy.location}',
  'fields.visit_time.expression' =  '#{date.past ''15'',''5'',''SECONDS''}'
);

```

## The Query

We are using `UNION ALL` because it doesn't try to combine equivalent rows like 
`UNION` would do. That is also the reason why `UNION` can only be used with `bounded` streams.


```sql
SELECT visitor, '' AS spacecraft, location, visit_time FROM rickandmorty_visits
UNION ALL
SELECT '' AS visitor, spacecraft, location, visit_time FROM spaceagency_visits
UNION ALL
SELECT visitor, starship AS spacecraft, location, visit_time FROM hitchhiker_visits;
```

:alien: As we are using `CREATE TEMPORARY TABLE`, you need to run both the `CREATE TABLE` and the `SELECT` statements together.

## The Beauty in VVP

![screeny](https://user-images.githubusercontent.com/68620/108173907-081cab00-70ff-11eb-823a-8a245b390485.png)


The result is a combined stream of people visiting a location in one of those fantastic universes.
We are sure you'll understand why this is one of our favorite queries.

:bird: [Let us know](https://twitter.com/ververicadata) about your favorite streaming SQL Query.


# 03 Filtering out Late Data

![Twitter Badge](https://img.shields.io/badge/Flink%20Version-1.14%2B-lightgrey)

> :bulb: This example will show how to filter out late data using the `CURRENT_WATERMARK` function.

The source table (`mobile_usage`) is backed by the [`faker` connector](https://flink-packages.org/packages/flink-faker), which continuously generates rows in memory based on Java Faker expressions.

As explained before in the [watermarks recipe](../../aggregations-and-analytics/02_watermarks/02_watermarks.md), Flink uses watermarks to measure progress in event time. By using a `WATERMARK` attribute in a table's DDL, we signify a column as the table's event time attribute and tell Flink how out of order we expect our data to arrive.  

There are many cases when rows are arriving even more out of order than anticipated, i.e. after the watermark. This data is called *late*.  An example could be when someone is using a mobile app while being offline because of lack of mobile coverage or flight mode being enabled. When Internet access is restored, previously tracked activities would then be sent.

In this recipe, we'll filter out this late data using the [`CURRENT_WATERMARK`](https://ci.apache.org/projects/flink/flink-docs-release-1.14/docs/dev/table/functions/systemfunctions/) function. In the first statement, we'll use the non-late data combined with the [`TUMBLE`](../../aggregations-and-analytics/01_group_by_window/01_group_by_window_tvf.md) function to send the unique IP addresses per minute to a downstream consumer (like a BI tool). Next to this use case, we're sending the late data to a different sink. For example, you might want to use these rows to change the results of your product recommender for offline mobile app users. 

This table DDL contains both an event time and a processing time definition. `ingest_time` is defined as processing time, while `log_time` is defined as event time and will contain timestamps between 45 and 10 seconds ago.   

## Script

```sql
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
```

## Example Output

![03_current_watermark](https://bucket-jerry-1.oss-cn-shanghai.aliyuncs.com/uPic/03_current_watermark2022%2006.png)

### Late data

![03_late_data](https://bucket-jerry-1.oss-cn-shanghai.aliyuncs.com/uPic/03_late_data2022%2006.png)

# 04 Overriding table options

![Twitter Badge](https://img.shields.io/badge/Flink%20Version-1.11%2B-lightgrey)

> :bulb: This example will show how you can override table options that have been defined via a DDL by using Hints.

This recipe uses the `2015 Flight Delays and Cancellations` dataset which can be found on [Kaggle](https://www.kaggle.com/usdot/flight-delays).

As explained before in the [creating tables recipe](../../foundations/01_create_table/01_create_table.md), you create tables in Flink SQL by using a SQL DDL. For example, you would use the following DDL to create a table `airports` which reads available airports in via the provided CSV file. 

> :warning: Make sure that the value for `path` is correct for your location environment.

```sql
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
```

After creating this table, you would normally query it using something like:

```sql
SELECT * FROM `airports`;
```

However, this currently doesn't work because there is an improperly formatted line in the CSV file. There is an option for CSV files to ignore parsing errors, but that means you need to alter the table. 

You can also override the defined table options using [SQL Hints](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/queries/hints/). Your SQL statement would then look like:

```sql
SELECT * FROM `airports` /*+ OPTIONS('csv.ignore-parse-errors'='true') */;
```

Since the CSV format option `csv.ignore-parse-errors` sets fields to null in case of errors, you can also quickly identify which fields can't be parsed using:

```sql
SELECT * FROM `airports` /*+ OPTIONS('csv.ignore-parse-errors'='true') */ WHERE `LATITUDE` IS NULL;
```

You can apply SQL Hints for all possible table options. For example, if you SQL job which reads from Kafka has crashed, you can override the default reading position:

```sql
SELECT * FROM `your_kafka_topic` /*+ OPTIONS('scan.startup.mode'='group-offsets') */;
```

Tables, views and functions are all registered in the catalog. The catalog is a collection of metadata. Using SQL Hints, you can override any defined metadata.  

## Example Output

![04_override_table_options.screen01](https://bucket-jerry-1.oss-cn-shanghai.aliyuncs.com/uPic/04_override_table_options.screen012022%2006.png)
![04_override_table_options.screen02](https://bucket-jerry-1.oss-cn-shanghai.aliyuncs.com/uPic/04_override_table_options.screen022022%2006.png)

# 05 Expanding arrays into new rows

![Twitter Badge](https://img.shields.io/badge/Flink%20Version-1.3%2B-lightgrey)

> :bulb: This example will show how to create new rows for each element in an array using a `CROSS JOIN UNNEST`.

The source table (`HarryPotter`) is backed by the [`faker` connector](https://flink-packages.org/packages/flink-faker), which continuously generates rows in memory based on Java Faker expressions.

There are many cases where data contains complex data types. Complex data types means that the data is structured in a nested way. Examples of these data types are `ARRAY`, `MAP` or `ROW`.

In this recipe, we'll expand an `ARRAY` into a new row for each element using `CROSS JOIN UNNEST` and join the new rows with a lookup table.

This table DDL creates a `HarryPotter` themed table. It contains a character from Harry Potter and 3 spells that the character used. The `spells` is of data type `ARRAY`.

## Script

```sql
-- Create source table
CREATE TABLE `HarryPotter` (
  `character` STRING,
  `spells` ARRAY<STRING>
) WITH (
  'connector' = 'faker',
  'fields.character.expression' = '#{harry_potter.character}',
  'fields.spells.expression' = '#{harry_potter.spell}',
  'fields.spells.length' = '3'
);
```

When querying this table, your results will look like this:

![05_complex_data_types](https://bucket-jerry-1.oss-cn-shanghai.aliyuncs.com/uPic/05_complex_data_types2022%2006.png)

In order to generate new rows for each element in the `spells` array, we’ll use a `CROSS JOIN UNNEST`. By applying this statement, the `UNNEST` will create one row for each element in `spells`, which we will store in a temporary table `SpellsTable`. Secondly, the `CROSS JOIN` joins each row in the `SpellsTable` with the matching row  of the `HarryPotter` table. The DDL below will create a view that we’ll use to join all the newly generated rows with a lookup table.

```sql
CREATE TEMPORARY VIEW `SpellsPerCharacter` AS
  SELECT `HarryPotter`.`character`, `SpellsTable`.`spell`
  FROM HarryPotter 
  CROSS JOIN UNNEST(HarryPotter.spells) AS SpellsTable (spell);
```

The results of this view will contain each individual spell as a row. The results look like the following:

![05_unnested_data](https://bucket-jerry-1.oss-cn-shanghai.aliyuncs.com/uPic/05_unnested_data2022%2006.png)

You can then use the resulting table like you normally would, for example, for joining with a lookup table. Here, we enrich each `spell` with the `spoken_language` so that you can see in which language the spells were cast.

```sql
CREATE TABLE `Spells_Language` (
  `spells` STRING,
  `spoken_language` STRING, 
  `proctime` AS PROCTIME()
)
WITH (
  'connector' = 'faker', 
  'fields.spells.expression' = '#{harry_potter.spell}',
  'fields.spoken_language.expression' = '#{regexify ''(Parseltongue|Rune|Gobbledegook|Mermish|Troll|English)''}'
);
```

```sql
SELECT 
  `SpellsPerCharacter`.`character`, 
  `SpellsPerCharacter`.`spell`, 
  `Spells_Language`.`spoken_language`
FROM SpellsPerCharacter
JOIN Spells_Language FOR SYSTEM_TIME AS OF proctime AS Spells_Language
ON SpellsPerCharacter.spell = Spells_Language.spells;
```

## Example Output

![05_joined_unnested_data](https://bucket-jerry-1.oss-cn-shanghai.aliyuncs.com/uPic/05_joined_unnested_data2022%2006.png)


# 06 Split strings into maps

![Twitter Badge](https://img.shields.io/badge/Flink%20Version-1.3%2B-lightgrey)

> :bulb: This example will show how you can create a map of key/value pairs by splitting string values using `STR_TO_MAP`.

The source table (`customers`) is backed by the [`faker` connector](https://flink-packages.org/packages/flink-faker), which continuously generates rows in memory based on Java Faker expressions.

There are many different data types in Flink SQL. You can group these in Character Strings, Binary Strings, Exact Numerics, Approximate Numerics, Date and Time, Constructed Data Types, User-Defined Types and Other Data Types.
Some examples are `VARCHAR/STRING`, `CHAR`, `DECIMAL`, `DATE`, `TIME`, `TIMESTAMP`, `ARRAY`, `MAP`, `ROW` and `JSON`. You can find more information about these data types in the [Flink SQL Data Types Reference](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/types/). 

In this recipe, we'll convert two `STRING` columns containing a `postal_address` and a `residential_address` into a `MAP` column. 

This table DDL creates a `customers` table. It contains an identifier, the full name of a customer, the address to which you sent mail and the address where the customer is living.

## Script

```sql
-- Create source table
CREATE TABLE `customers` (
  `identifier` STRING,
  `fullname` STRING,
  `postal_address` STRING,
  `residential_address` STRING
) WITH (
  'connector' = 'faker',
  'fields.identifier.expression' = '#{Internet.uuid}',
  'fields.fullname.expression' = '#{Name.firstName} #{Name.lastName}',
  'fields.postal_address.expression' = '#{Address.fullAddress}',
  'fields.residential_address.expression' = '#{Address.fullAddress}',
  'rows-per-second' = '1'
);
```

After creating this table, we use the [`STR_TO_MAP`](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/functions/systemfunctions/#string-functions) in our SELECT statement.
This function splits a `STRING` value into one or more key/value pair(s) using a delimiter. 
The default pair delimiter is `,` but this can be adjusted by providing a second argument to this function. In this example, we change the pair delimiter to `;` since our addresses can contain `,`.
There is also a default key-value delimiter, which is `=`. In this example, we're changing this to `:` by providing a third argument to the function. 

To create our `MAP` column, we're using `||` to concatenate multiple `STRING` values. 
We're hardcoding the first key to 'postal_address:' to include the key-value delimiter and concatenate the value from the `postal_address` column.
We then continue with hardcoding our second key to ';residential_address:'. That includes the pair delimiter `;` as a prefix and again `:` as our key-value delimiter as a suffix. 
To complete the function, we change the default values for pair delimiter and key-value delimiter to `;` and `:` respectively.

```sql
SELECT 
  `identifier`,
  `fullname`,
  STR_TO_MAP('postal_address:' || postal_address || ';residential_address:' || residential_address,';',':') AS `addresses`
FROM `customers`;
```

## Example Output

![06_create_maps](https://bucket-jerry-1.oss-cn-shanghai.aliyuncs.com/uPic/06_split_strings_into_maps2022%2006.png)


# User-Defined Functions (UDFs)
# 01 Extending SQL with Python UDFs

![Twitter Badge](https://img.shields.io/badge/Flink%20Version-1.11%2B-lightgrey)

> :bulb: This example will show how to extend Flink SQL with custom functions written in Python.

Flink SQL provides a wide range of [built-in functions](https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/functions/systemFunctions.html) that cover most SQL day-to-day work. Sometimes, you need more flexibility to express custom business logic or transformations that aren't easily translatable to SQL: this can be achieved with [User-Defined Functions](https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/functions/udfs.html) (UDFs).

In this example, you'll focus on [Python UDFs](https://ci.apache.org/projects/flink/flink-docs-stable/dev/python/table-api-users-guide/udfs/python_udfs.html) and implement a custom function (`to_fahr`) to convert temperature readings that are continuously generated for different EU and US cities. The Celsius->Fahrenheit conversion should only happen if the city associated with the reading is in the US.

## Scripts

#### Python UDF

The first step is to create a Python file with the UDF implementation (`python_udf.py`), using Flink's [Python Table API](https://ci.apache.org/projects/flink/flink-docs-stable/dev/python/table-api-users-guide/intro_to_table_api.html). If this is new to you, there are examples on how to write [general](https://ci.apache.org/projects/flink/flink-docs-stable/dev/python/table-api-users-guide/udfs/python_udfs.html) and [vectorized](https://ci.apache.org/projects/flink/flink-docs-stable/dev/python/table-api-users-guide/udfs/vectorized_python_udfs.html) Python UDFs in the Flink documentation.

```python
from pyflink.table import DataTypes
from pyflink.table.udf import udf

us_cities = {"Chicago","Portland","Seattle","New York"}

@udf(input_types=[DataTypes.STRING(), DataTypes.FLOAT()],
     result_type=DataTypes.FLOAT())
def to_fahr(city, temperature):

  if city in us_cities:

    fahr = ((temperature * 9.0 / 5.0) + 32.0)

    return fahr
  else:
    return temperature
```

For detailed instructions on how to then make the Python file available as a UDF in the SQL Client, please refer to [this documentation page](https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/sqlClient.html#user-defined-functions).

#### SQL

The source table (`temperature_measurements`) is backed by the [`faker` connector](https://flink-packages.org/packages/flink-faker), which continuously generates rows in memory based on Java Faker expressions. 

```sql
--Register the Python UDF using the fully qualified 
--name of the function ([module name].[object name])
CREATE FUNCTION to_fahr AS 'python_udf.to_fahr' 
LANGUAGE PYTHON;


CREATE TABLE temperature_measurements (
  city STRING,
  temperature FLOAT,
  measurement_time TIMESTAMP(3),
  WATERMARK FOR measurement_time AS measurement_time - INTERVAL '15' SECONDS
)
WITH (
  'connector' = 'faker',
  'fields.temperature.expression' = '#{number.numberBetween ''0'',''42''}',
  'fields.measurement_time.expression' = '#{date.past ''15'',''SECONDS''}',
  'fields.city.expression' = '#{regexify ''(Copenhagen|Berlin|Chicago|Portland|Seattle|New York){1}''}'
);


--Use to_fahr() to convert temperatures in US cities from C to F
SELECT city,
       temperature AS tmp,
       to_fahr(city,temperature) AS tmp_conv,
       measurement_time
FROM temperature_measurements;
```

## Example Output

![01_python_udfs](https://user-images.githubusercontent.com/23521087/106733744-8ca4ff00-6612-11eb-9721-e4a74fb07329.gif)




# Joins
# 01 Regular Joins

> :bulb: This example will show how you can use joins to correlate rows across multiple tables.

Flink SQL supports complex and flexible join operations over continuous tables.
There are several different types of joins to account for the wide variety of semantics queries may require.

Regular joins are the most generic and flexible type of join.
These include the standard `INNER` and `[FULL|LEFT|RIGHT] OUTER` joins that are available in most modern databases. 

Suppose we have a [NOC list](https://en.wikipedia.org/wiki/Non-official_cover) of secret agents all over the world.
Your mission if you choose to accept it, is to join this table with another containin the agents real name.

In Flink SQL, this can be achieved using a simple `INNER JOIN`.
Flink will join the tables using an equi-join predicate on the `agent_id` and output a new row everytime there is a match.

However, there is something to be careful of. 
Flink must retain every input row as part of the join to potentially join it with the other table in the future. 
This means the queries resource requirements will grow indefinitely and will eventually fail.
While this type of join is useful in some scenarios, other joins are more powerful in a streaming context and significantly more space-efficient.

In this example, both tables are bounded to remain space efficient.

```sql
CREATE TABLE NOC (
  agent_id STRING,
  codename STRING
)
WITH (
  'connector' = 'faker',
  'fields.agent_id.expression' = '#{regexify ''(1|2|3|4|5){1}''}',
  'fields.codename.expression' = '#{superhero.name}',
  'number-of-rows' = '10'
);

CREATE TABLE RealNames (
  agent_id STRING,
  name     STRING
)
WITH (
  'connector' = 'faker',
  'fields.agent_id.expression' = '#{regexify ''(1|2|3|4|5){1}''}',
  'fields.name.expression' = '#{Name.full_name}',
  'number-of-rows' = '10'
);

SELECT
    name,
    codename
FROM NOC
INNER JOIN RealNames ON NOC.agent_id = RealNames.agent_id;
```

![01_regular_joins](https://user-images.githubusercontent.com/23521087/105504538-280bab00-5cc8-11eb-962d-6f36432e422b.png)

# 02 Interval Joins

> :bulb: This example will show how you can perform joins between tables with events that are related in a temporal context.

## Why Interval Joins?

In a [previous recipe](../01_regular_joins/01_regular_joins.md), you learned about using _regular joins_ in Flink SQL. This kind of join works well for some scenarios, but for others a more efficient type of join is required to keep resource utilization from growing indefinitely.

One of the ways to optimize joining operations in Flink SQL is to use [_interval joins_](https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/streaming/joins.html#interval-joins). An interval join is defined by a join predicate that checks if the time attributes of the input events are within certain time constraints (i.e. a time window).

## Using Interval Joins

Suppose you want to join events of two tables that correlate to each other in the [order fulfillment lifecycle](https://en.wikipedia.org/wiki/Order_fulfillment) (`orders` and `shipments`) and that are under a Service-level Aggreement (SLA) of **3 days**. To reduce the amount of input rows Flink has to retain and optimize the join operation, you can define a time constraint in the `WHERE` clause to bound the time on both sides to that specific interval using a `BETWEEN` predicate.

## Script

The source tables (`orders` and `shipments`) are backed by the built-in [`datagen` connector](https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/connectors/datagen.html), which continuously generates rows in memory.

```sql
CREATE TABLE orders (
  id INT,
  order_time AS TIMESTAMPADD(DAY, CAST(FLOOR(RAND()*(1-5+1)+5)*(-1) AS INT), CURRENT_TIMESTAMP)
)
WITH (
  'connector' = 'datagen',
  'rows-per-second'='10',
  'fields.id.kind'='sequence',
  'fields.id.start'='1',
  'fields.id.end'='1000'
);


CREATE TABLE shipments (
  id INT,
  order_id INT,
  shipment_time AS TIMESTAMPADD(DAY, CAST(FLOOR(RAND()*(1-5+1)) AS INT), CURRENT_TIMESTAMP)
)
WITH (
  'connector' = 'datagen',
  'rows-per-second'='5',
  'fields.id.kind'='random',
  'fields.id.min'='0',
  'fields.order_id.kind'='sequence',
  'fields.order_id.start'='1',
  'fields.order_id.end'='1000'
);

SELECT
  o.id AS order_id,
  o.order_time,
  s.shipment_time,
  TIMESTAMPDIFF(DAY,o.order_time,s.shipment_time) AS day_diff
FROM orders o
JOIN shipments s ON o.id = s.order_id
WHERE 
    o.order_time BETWEEN s.shipment_time - INTERVAL '3' DAY AND s.shipment_time;
```

## Example Output

![15_interval_joins](https://user-images.githubusercontent.com/23521087/102237138-9ce30c80-3ef4-11eb-969f-8f157b249ebb.png)

# 03 Temporal Table Join between a non-compacted and compacted Kafka Topic

> :bulb: In this recipe, you will see how to correctly enrich records from one Kafka topic with the corresponding records of another Kafka topic when the order of events matters.  

Temporal table joins take an arbitrary table (left input/probe site) and correlate each row to the corresponding row’s relevant version in a versioned table (right input/build side). 
Flink uses the SQL syntax of ``FOR SYSTEM_TIME AS OF`` to perform this operation. 

In this recipe, we want join each transaction (`transactions`) to its correct currency rate (`currency_rates`, a versioned table) **as of the time when the transaction happened**.
A similar example would be to join each order with the customer details as of the time when the order happened.
This is exactly what an event-time temporal table join does.
A temporal table join in Flink SQL provides correct, deterministic results in the presence of out-of-orderness and arbitrary time skew between the two tables. 

Both the `transactions` and `currency_rates` tables are backed by Kafka topics, but in the case of rates this topic is compacted (i.e. only the most recent messages for a given key are kept as updated rates flow in).
Records in `transactions` are interpreted as inserts only, and so the table is backed by the [standard Kafka connector](https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/connectors/kafka.html) (`connector` = `kafka`); while the records in `currency_rates` need to be interpreted as upserts based on a primary key, which requires the [Upsert Kafka connector](https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/connectors/upsert-kafka.html) (`connector` = `upsert-kafka`).

## Script

```sql
CREATE TEMPORARY TABLE currency_rates (
  `currency_code` STRING,
  `eur_rate` DECIMAL(6,4),
  `rate_time` TIMESTAMP(3),
  WATERMARK FOR `rate_time` AS rate_time - INTERVAL '15' SECONDS,
  PRIMARY KEY (currency_code) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'currency_rates',
  'properties.bootstrap.servers' = 'localhost:9092',
  'key.format' = 'raw',
  'value.format' = 'json'
);

CREATE TEMPORARY TABLE transactions (
  `id` STRING,
  `currency_code` STRING,
  `total` DECIMAL(10,2),
  `transaction_time` TIMESTAMP(3),
  WATERMARK FOR `transaction_time` AS transaction_time - INTERVAL '30' SECONDS
) WITH (
  'connector' = 'kafka',
  'topic' = 'transactions',
  'properties.bootstrap.servers' = 'localhost:9092',
  'key.format' = 'raw',
  'key.fields' = 'id',
  'value.format' = 'json',
  'value.fields-include' = 'ALL'
);

SELECT 
  t.id,
  t.total * c.eur_rate AS total_eur,
  t.total, 
  c.currency_code,
  t.transaction_time
FROM transactions t
JOIN currency_rates FOR SYSTEM_TIME AS OF t.transaction_time AS c
ON t.currency_code = c.currency_code;
```

## Example Output

![kafka_join](https://user-images.githubusercontent.com/11538663/102418338-d2bfe800-3ffd-11eb-995a-13fa116b538f.gif)

## Data Generators

<details>
    <summary>Data Generators</summary>

The two topics are populated using a Flink SQL job, too. 
We use the  [`faker` connector](https://flink-packages.org/packages/flink-faker) to generate rows in memory based on Java Faker expressions and write those to the respective Kafka topics.  

### ``currency_rates`` Topic

### Script

```sql
CREATE TEMPORARY TABLE currency_rates_faker
WITH (
  'connector' = 'faker',
  'fields.currency_code.expression' = '#{Currency.code}',
  'fields.eur_rate.expression' = '#{Number.randomDouble ''4'',''0'',''10''}',
  'fields.rate_time.expression' = '#{date.past ''15'',''SECONDS''}', 
  'rows-per-second' = '100'
) LIKE currency_rates (EXCLUDING OPTIONS);

INSERT INTO currency_rates SELECT * FROM currency_rates_faker;
```
#### Kafka Topic

```shell script
➜  bin ./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic currency_rates --property print.key=true --property key.separator=" - "
HTG - {"currency_code":"HTG","eur_rate":0.0136,"rate_time":"2020-12-16 22:22:02"}
BZD - {"currency_code":"BZD","eur_rate":1.6545,"rate_time":"2020-12-16 22:22:03"}
BZD - {"currency_code":"BZD","eur_rate":3.616,"rate_time":"2020-12-16 22:22:10"}
BHD - {"currency_code":"BHD","eur_rate":4.5308,"rate_time":"2020-12-16 22:22:05"}
KHR - {"currency_code":"KHR","eur_rate":1.335,"rate_time":"2020-12-16 22:22:06"}
```

### ``transactions`` Topic

#### Script

```sql
CREATE TEMPORARY TABLE transactions_faker
WITH (
  'connector' = 'faker',
  'fields.id.expression' = '#{Internet.UUID}',
  'fields.currency_code.expression' = '#{Currency.code}',
  'fields.total.expression' = '#{Number.randomDouble ''2'',''10'',''1000''}',
  'fields.transaction_time.expression' = '#{date.past ''30'',''SECONDS''}',
  'rows-per-second' = '100'
) LIKE transactions (EXCLUDING OPTIONS);

INSERT INTO transactions SELECT * FROM transactions_faker;
```

#### Kafka Topic

```shell script
➜  bin ./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic transactions --property print.key=true --property key.separator=" - "
e102e91f-47b9-434e-86e1-34fb1196d91d - {"id":"e102e91f-47b9-434e-86e1-34fb1196d91d","currency_code":"SGD","total":494.07,"transaction_time":"2020-12-16 22:18:46"}
bf028363-5ee4-4a5a-9068-b08392d59f0b - {"id":"bf028363-5ee4-4a5a-9068-b08392d59f0b","currency_code":"EEK","total":906.8,"transaction_time":"2020-12-16 22:18:46"}
e22374b5-82da-4c6d-b4c6-f27a818a58ab - {"id":"e22374b5-82da-4c6d-b4c6-f27a818a58ab","currency_code":"GYD","total":80.66,"transaction_time":"2020-12-16 22:19:02"}
81b2ce89-26c2-4df3-b12a-8ca921902ac4 - {"id":"81b2ce89-26c2-4df3-b12a-8ca921902ac4","currency_code":"EGP","total":521.98,"transaction_time":"2020-12-16 22:18:57"}
53c4fd3f-af6e-41d3-a677-536f4c86e010 - {"id":"53c4fd3f-af6e-41d3-a677-536f4c86e010","currency_code":"UYU","total":936.26,"transaction_time":"2020-12-16 22:18:59"}
```

</details>

# 04 Lookup Joins

> :bulb: This example will show how you can enrich a stream with an external table of reference data (i.e. a _lookup_ table).

## Data Enrichment

Not all data changes frequently, even when working in real-time: in some cases, you might need to enrich streaming data with static — or _reference_ — data that is stored externally.
For example, `user` metadata may be stored in a relational database that Flink needs to join against directly.
Flink SQL allows you to look up reference data and join it with a stream using a _lookup join_. The join requires one table to have a [processing time attribute](https://docs.ververica.com/user_guide/sql_development/table_view.html#processing-time-attributes) and the other table to be backed by a [lookup source connector](https://docs.ververica.com/user_guide/sql_development/connectors.html#id1), like the JDBC connector.

## Using Lookup Joins

In this example, you will look up reference user data stored in MySQL to flag subscription events for users that are minors (`age < 18`). The `FOR SYSTEM_TIME AS OF` clause uses the processing time attribute to ensure that each row of the `subscriptions` table is joined with the `users` rows that match the join predicate at the point in time when the `subscriptions` row is processed by the join operator. The lookup join also requires an equality join predicate based on the `PRIMARY KEY` of the lookup table (`usub.user_id = u.user_id`). Here, the source does not have to read the entire table and can lazily fetch individual values from the external table when necessary.

## Script

The source table (`subscriptions`) is backed by the [`faker` connector](https://flink-packages.org/packages/flink-faker), which continuously generates rows in memory based on Java Faker expressions. The `users` table is backed by an existing MySQL reference table using the [JDBC connector](https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/connectors/jdbc.html).

```sql
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
  'fields.end_date.expression' = '#{date.future ''365'',''DAYS''}',
  'fields.payment_expiration.expression' = '#{date.future ''365'',''DAYS''}'
);

CREATE TABLE users (
 user_id INT PRIMARY KEY,
 user_name VARCHAR(255) NOT NULL, 
 age INT NOT NULL
)
WITH (
  'connector' = 'jdbc', 
  'url' = 'jdbc:mysql://localhost:3306/mysql-database', 
  'table-name' = 'users', 
  'username' = 'mysql-user', 
  'password' = 'mysql-password'
);

SELECT 
  id AS subscription_id,
  type AS subscription_type,
  age AS user_age,
  CASE 
    WHEN age < 18 THEN 1
    ELSE 0
  END AS is_minor
FROM subscriptions usub
JOIN users FOR SYSTEM_TIME AS OF usub.proc_time AS u
  ON usub.user_id = u.user_id;
```

## Example Output

![18_lookup_joins](https://user-images.githubusercontent.com/23521087/102645588-fcdeea80-4162-11eb-8581-55a06ea82518.png)

# 05 Real Time Star Schema Denormalization (N-Way Join)

> :bulb: In this recipe, we will de-normalize a simple star schema with an n-way temporal table join. 	 

[Star schemas](https://en.wikipedia.org/wiki/Star_schema) are a popular way of normalizing data within a data warehouse. 
At the center of a star schema is a **fact table** whose rows contain metrics, measurements, and other facts about the world. 
Surrounding fact tables are one or more **dimension tables** which have metadata useful for enriching facts when computing queries.  
You are running a small data warehouse for a railroad company which consists of a fact table (`train_activity`) and three dimension tables (`stations`, `booking_channels`, and `passengers`). 
All inserts to the fact table, and all updates to the dimension tables, are mirrored to Apache Kafka. 
Records in the fact table are interpreted as inserts only, and so the table is backed by the [standard Kafka connector](https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/connectors/kafka.html) (`connector` = `kafka`);. 
In contrast, the records in the dimensional tables are upserts based on a primary key, which requires the [Upsert Kafka connector](https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/connectors/upsert-kafka.html) (`connector` = `upsert-kafka`).	 

With Flink SQL you can now easily join all dimensions to our fact table using a 5-way temporal table join. 	 
Temporal table joins take an arbitrary table (left input/probe site) and correlate each row to the corresponding row’s relevant version in a versioned table (right input/build side). 	 
Flink uses the SQL syntax of ``FOR SYSTEM_TIME AS OF`` to perform this operation.
Using a temporal table join leads to consistent, reproducible results when joining a fact table with more (slowly) changing dimensional tables.
Every event (row in the fact table) is joined to its corresponding value of each dimension based on when the event occurred in the real world. 

## Script

```sql
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
ON t.destination_station_key = ds.station_key;
```

## Example Output

### SQL Client

![05_output](https://user-images.githubusercontent.com/23521087/105504672-54272c00-5cc8-11eb-88da-901bb0006da1.png)

### JobGraph

![05_jobgraph](https://user-images.githubusercontent.com/23521087/105504615-440f4c80-5cc8-11eb-94f2-d07d0315dec5.png)

## Data Generators

<details>
    <summary>Data Generators</summary>

The four topics are populated with Flink SQL jobs, too.
We use the  [`faker` connector](https://flink-packages.org/packages/flink-faker) to generate rows in memory based on Java Faker expressions and write those to the respective Kafka topics.  

### ``train_activities`` Topic

### Script

```sql
CREATE TEMPORARY TABLE train_activities_faker
WITH (
  'connector' = 'faker', 
  'fields.scheduled_departure_time.expression' = '#{date.past ''10'',''0'',''SECONDS''}',
  'fields.actual_departure_date.expression' = '#{date.past ''10'',''5'',''SECONDS''}',
  'fields.passenger_key.expression' = '#{number.numberBetween ''0'',''10000000''}',
  'fields.origin_station_key.expression' = '#{number.numberBetween ''0'',''1000''}',
  'fields.destination_station_key.expression' = '#{number.numberBetween ''0'',''1000''}',
  'fields.booking_channel_key.expression' = '#{number.numberBetween ''0'',''7''}',
  'rows-per-second' = '1000'
) LIKE train_activities (EXCLUDING OPTIONS);

INSERT INTO train_activities SELECT * FROM train_activities_faker;
```
#### Kafka Topic

```shell script
➜  bin ./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic train_actitivies --property print.key=true --property key.separator=" - "
null - {"scheduled_departure_time":"2020-12-19 13:52:37","actual_departure_date":"2020-12-19 13:52:16","passenger_key":7014937,"origin_station_key":577,"destination_station_key":862,"booking_channel_key":2}
null - {"scheduled_departure_time":"2020-12-19 13:52:38","actual_departure_date":"2020-12-19 13:52:23","passenger_key":2244807,"origin_station_key":735,"destination_station_key":739,"booking_channel_key":2}
null - {"scheduled_departure_time":"2020-12-19 13:52:46","actual_departure_date":"2020-12-19 13:52:18","passenger_key":2605313,"origin_station_key":216,"destination_station_key":453,"booking_channel_key":3}
null - {"scheduled_departure_time":"2020-12-19 13:53:13","actual_departure_date":"2020-12-19 13:52:19","passenger_key":7111654,"origin_station_key":234,"destination_station_key":833,"booking_channel_key":5}
null - {"scheduled_departure_time":"2020-12-19 13:52:22","actual_departure_date":"2020-12-19 13:52:17","passenger_key":2847474,"origin_station_key":763,"destination_station_key":206,"booking_channel_key":3}
```

### ``passengers`` Topic

#### Script

```sql
CREATE TEMPORARY TABLE passengers_faker
WITH (
  'connector' = 'faker',
  'fields.passenger_key.expression' = '#{number.numberBetween ''0'',''10000000''}',
  'fields.update_time.expression' = '#{date.past ''10'',''5'',''SECONDS''}',
  'fields.first_name.expression' = '#{Name.firstName}',
  'fields.last_name.expression' = '#{Name.lastName}',
  'rows-per-second' = '1000'
) LIKE passengers (EXCLUDING OPTIONS);

INSERT INTO passengers SELECT * FROM passengers_faker;
```

#### Kafka Topic

```shell script
➜  bin ./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic passengers --property print.key=true --property key.separator=" - "
749049 - {"passenger_key":"749049","first_name":"Booker","last_name":"Hackett","update_time":"2020-12-19 14:02:32"}
7065702 - {"passenger_key":"7065702","first_name":"Jeramy","last_name":"Breitenberg","update_time":"2020-12-19 14:02:38"}
3690329 - {"passenger_key":"3690329","first_name":"Quiana","last_name":"Macejkovic","update_time":"2020-12-19 14:02:27"}
1212728 - {"passenger_key":"1212728","first_name":"Lawerence","last_name":"Simonis","update_time":"2020-12-19 14:02:27"}
6993699 - {"passenger_key":"6993699","first_name":"Ardelle","last_name":"Frami","update_time":"2020-12-19 14:02:19"}
```

### ``stations`` Topic

#### Script

```sql
CREATE TEMPORARY TABLE stations_faker
WITH (
  'connector' = 'faker',
  'fields.station_key.expression' = '#{number.numberBetween ''0'',''1000''}',
  'fields.city.expression' = '#{Address.city}',
  'fields.update_time.expression' = '#{date.past ''10'',''5'',''SECONDS''}',
  'rows-per-second' = '100'
) LIKE stations (EXCLUDING OPTIONS);

INSERT INTO stations SELECT * FROM stations_faker;
```

#### Kafka Topic

```shell script
➜  bin ./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic stations --property print.key=true --property key.separator=" - "
80 - {"station_key":"80","update_time":"2020-12-19 13:59:20","city":"Harlandport"}
33 - {"station_key":"33","update_time":"2020-12-19 13:59:12","city":"North Georgine"}
369 - {"station_key":"369","update_time":"2020-12-19 13:59:12","city":"Tillmanhaven"}
580 - {"station_key":"580","update_time":"2020-12-19 13:59:12","city":"West Marianabury"}
616 - {"station_key":"616","update_time":"2020-12-19 13:59:09","city":"West Sandytown"}
```

### ``booking_channels`` Topic

#### Script

```sql
CREATE TEMPORARY TABLE booking_channels_faker
WITH (
  'connector' = 'faker',
  'fields.booking_channel_key.expression' = '#{number.numberBetween ''0'',''7''}',
  'fields.channel.expression' = '#{regexify ''(bahn\.de|station|retailer|app|lidl|hotline|joyn){1}''}',
  'fields.update_time.expression' = '#{date.past ''10'',''5'',''SECONDS''}',
  'rows-per-second' = '100'
) LIKE booking_channels (EXCLUDING OPTIONS);

INSERT INTO booking_channels SELECT * FROM booking_channels_faker;
```

#### Kafka Topic

```shell script
➜  bin ./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic booking_channels --property print.key=true --property key.separator=" - "
1 - {"booking_channel_key":"1","update_time":"2020-12-19 13:57:05","channel":"joyn"}
0 - {"booking_channel_key":"0","update_time":"2020-12-19 13:57:17","channel":"station"}
4 - {"booking_channel_key":"4","update_time":"2020-12-19 13:57:15","channel":"joyn"}
2 - {"booking_channel_key":"2","update_time":"2020-12-19 13:57:02","channel":"app"}
1 - {"booking_channel_key":"1","update_time":"2020-12-19 13:57:06","channel":"retailer"}

```

</details>


# 06 Lateral Table Join

> :bulb: This example will show how you can correlate events using a `LATERAL` join.

A recent addition to the SQL standard is the `LATERAL` join, which allows you to combine 
the power of a correlated subquery with the expressiveness of a join. 

Given a table with people's addresses, you need to find the two most populous cities
for each state and continuously update those rankings as people move. The input table
of `People` contains a uid for each person and their address and when they moved there.

The first step is to calculate each city's population using a [continuous aggregation](../../foundations/05_group_by/05_group_by.md).
While this is simple enough, the real power of Flink SQL comes when people move. By using
[deduplication](../../aggregations-and-analytics/06_dedup/06_dedup.md) Flink will automatically issue a retraction for a persons old city when 
they move. So if John moves from New York to Los Angelos, the population for New York will 
automatically go down by 1. This gives us the power change-data-capture without having
to invest in the actual infrastructure of setting it up!

With this dynamic population table at hand, you are ready to solve the original problem using a `LATERAL` table join.
Unlike a normal join, lateral joins allow the subquery to correlate with columns from other arguments in the `FROM` clause. And unlike a regular subquery, as a join, the lateral can return multiple rows.
You can now have a sub-query correlated with every individual state, and for every state it ranks by population and returns the top 2 cities.

## Script

```sql
CREATE TABLE People (
    id           INT,
    city         STRING,
    state        STRING,
    arrival_time TIMESTAMP(3),
    WATERMARK FOR arrival_time AS arrival_time - INTERVAL '1' MINUTE 
) WITH (
    'connector' = 'faker',
    'fields.id.expression'    = '#{number.numberBetween ''1'',''100''}',
    'fields.city.expression'  = '#{regexify ''(Newmouth|Newburgh|Portport|Southfort|Springfield){1}''}',
    'fields.state.expression' = '#{regexify ''(New York|Illinois|California|Washington){1}''}',
    'fields.arrival_time.expression' = '#{date.past ''15'',''SECONDS''}',
    'rows-per-second'          = '10'
); 

CREATE TEMPORARY VIEW CurrentPopulation AS
SELECT 
    city,
    state,
    COUNT(*) as population
FROM (
    SELECT
        city,
        state,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY arrival_time DESC) AS rownum
    FROM People
)
WHERE rownum = 1
GROUP BY city, state;

SELECT
    state,
    city,
    population
FROM 
    (SELECT DISTINCT state FROM CurrentPopulation) States,
    LATERAL (
        SELECT city, population
        FROM CurrentPopulation
        WHERE state = States.state
        ORDER BY population DESC
        LIMIT 2
);
```

## Example Output

![lateral](https://user-images.githubusercontent.com/23521087/105504738-6bfeb000-5cc8-11eb-9517-1242dfa87bb4.gif)



# Former Recipes
# 01 Aggregating Time Series Data

> :warning: This recipe is using a legacy function. We recommend following the [new recipe](01_group_by_window_tvf.md).

The source table (`server_logs`) is backed by the [`faker` connector](https://flink-packages.org/packages/flink-faker), which continuously generates rows in memory based on Java Faker expressions.

Many streaming applications work with time series data.
To count the number of `DISTINCT` IP addresses seen each minute, rows need to be grouped based on a [time attribute](https://docs.ververica.com/user_guide/sql_development/table_view.html#time-attributes).
Grouping based on time is special, because time always moves forward, which means Flink can generate final results after the minute is completed. 

`TUMBLE` is a built-in function for grouping timestamps into time intervals called windows.
Unlike other aggregations, it will only produce a single final result for each key when the interval is completed. 

If the logs do not have a timestamp, one can be generated using a [computed column](https://docs.ververica.com/user_guide/sql_development/table_view.html#computed-column). 
`log_time AS PROCTIME()` will append a column to the table with the current system time. 

## Script

```sql
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
  'fields.request_line.expression' = '#{regexify ''(GET|POST|PUT|PATCH){1}''} #{regexify ''(/search\.html|/login\.html|/prod\.html|cart\.html|/order\.html){1}''} #{regexify ''(HTTP/1\.1|HTTP/2|/HTTP/1\.0){1}''}',
  'fields.status_code.expression' = '#{regexify ''(200|201|204|400|401|403|301){1}''}'
);

SELECT  
  COUNT(DISTINCT client_ip) AS ip_addresses,
  TUMBLE_PROCTIME(log_time, INTERVAL '1' MINUTE) AS window_interval
FROM server_logs
GROUP BY 
  TUMBLE(log_time, INTERVAL '1' MINUTE);
```

## Example Output

![01_group_by_window](https://user-images.githubusercontent.com/23521087/105503522-fe05b900-5cc6-11eb-9c36-bd8dc2a8e9ce.png)

