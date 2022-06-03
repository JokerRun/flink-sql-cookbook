CREATE TABLE doctor_sightings
(
    doctor        STRING,
    sighting_time TIMESTAMP(3),
    WATERMARK FOR sighting_time AS sighting_time - INTERVAL '15' SECONDS
)
    WITH ( 'connector' = 'faker', 'fields.doctor.expression' = '#{dr_who.the_doctors}', 'fields.sighting_time.expression' = '#{date.past ''15'',''SECONDS''}' );


SELECT doctor
     , TUMBLE_ROWTIME(sighting_time, INTERVAL '1' MINUTE) AS sighting_time
     , COUNT(*)                                           AS sightings
FROM doctor_sightings
GROUP BY TUMBLE(sighting_time, INTERVAL '1' MINUTE), doctor;

select window_time,doctor, count(*) as sighting_cnt
from table(tumble(table doctor_sightings,descriptor(sighting_time),interval '1' MINUTE))
group by window_start, window_end,window_time, doctor;