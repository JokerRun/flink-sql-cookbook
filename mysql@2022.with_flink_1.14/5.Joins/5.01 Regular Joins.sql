/*
there is something to be careful of.
Flink must retain every input row as part of the join to potentially join it with the other table in the future.
This means the queries resource requirements will grow indefinitely and will eventually fail.
While this type of join is useful in some scenarios, other joins are more powerful in a streaming context and significantly more space-efficient.

In this example, both tables are bounded to remain space efficient.
*/
CREATE TABLE NOC (
  agent_id STRING,
  codename STRING
)
WITH (
  'connector' = 'faker',
  'fields.agent_id.expression' = '#{regexify ''(1|2|3|4|5){1}''}',
  'fields.codename.expression' = '#{superhero.name}',
  'number-of-rows' = '10' -- number-of-rows 限定记录数，所以是有限(bounded)表，非无限流表
);

CREATE TABLE RealNames (
  agent_id STRING,
  name     STRING
)
WITH (
  'connector' = 'faker',
  'fields.agent_id.expression' = '#{regexify ''(1|2|3|4|5){1}''}',
  'fields.name.expression' = '#{Name.full_name}',
  'number-of-rows' = '10' -- number-of-rows 限定记录数，所以是有限(bounded)表，非无限流表
);

SELECT
    name,
    codename
FROM NOC
INNER JOIN RealNames ON NOC.agent_id = RealNames.agent_id;