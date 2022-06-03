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