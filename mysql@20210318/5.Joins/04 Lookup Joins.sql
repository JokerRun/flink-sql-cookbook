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