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



SELECT visitor, '' AS spacecraft, location, visit_time FROM rickandmorty_visits
UNION ALL
SELECT '' AS visitor, spacecraft, location, visit_time FROM spaceagency_visits
UNION ALL
SELECT visitor, starship AS spacecraft, location, visit_time FROM hitchhiker_visits;
