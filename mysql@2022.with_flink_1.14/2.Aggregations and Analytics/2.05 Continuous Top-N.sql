CREATE Temporary TABLE spells_cast (
    wizard STRING,
    spell  STRING
) WITH (
  'connector' = 'faker',
  'fields.wizard.expression' = '#{harry_potter.characters}',
  'fields.spell.expression' = '#{harry_potter.spells}'
);

with wizard_spell_cnt as (select wizard, spell, count(1) as cnt from spells_cast group by wizard, spell)
   , seq              as (select wizard
                               , spell
                               , cnt
                               , ROW_NUMBER()  over(partition by wizard order by cnt desc ) as rn
                          from wizard_spell_cnt)
select * from seq
where rn <=2
;


/*
SELECT wizard, spell, times_cast
FROM (
    SELECT *,
    ROW_NUMBER() OVER (PARTITION BY wizard ORDER BY times_cast DESC) AS row_num
    FROM (SELECT wizard, spell, COUNT(*) AS times_cast FROM spells_cast GROUP BY wizard, spell)
)
WHERE row_num <= 2;
*/
