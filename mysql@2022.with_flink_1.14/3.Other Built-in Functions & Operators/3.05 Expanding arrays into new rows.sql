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


CREATE TEMPORARY VIEW `SpellsPerCharacter` AS
  SELECT `HarryPotter`.`character`, `SpellsTable`.`spell`
  FROM HarryPotter
  CROSS JOIN UNNEST(HarryPotter.spells) AS SpellsTable (spell);



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


SELECT
  `SpellsPerCharacter`.`character`,
  `SpellsPerCharacter`.`spell`,
  `Spells_Language`.`spoken_language`
FROM SpellsPerCharacter
JOIN Spells_Language FOR SYSTEM_TIME AS OF proctime AS Spells_Language
ON SpellsPerCharacter.spell = Spells_Language.spells;
