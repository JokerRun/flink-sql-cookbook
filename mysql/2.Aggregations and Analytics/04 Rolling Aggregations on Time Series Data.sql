CREATE TABLE spells_cast
(
    wizard STRING,
    spell  STRING
) WITH (
  ''connector'' = ''faker'',
  ''fields.wizard.expression'' = ''#{harry_potter.characters}'',
  ''fields.spell.expression'' = ''#{harry_potter.spells}''
);

SELECT wizard, spell, times_cast
FROM (
         SELECT *,
                ROW_NUMBER() OVER (PARTITION BY wizard ORDER BY times_cast DESC) AS row_num
         FROM (SELECT wizard, spell, COUNT(*) AS times_cast FROM spells_cast GROUP BY wizard, spell)
     )
WHERE row_num <= 2;  