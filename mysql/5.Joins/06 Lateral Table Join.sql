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