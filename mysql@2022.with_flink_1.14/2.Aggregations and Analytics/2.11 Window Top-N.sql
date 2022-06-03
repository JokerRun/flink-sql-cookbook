CREATE TABLE orders
(
    bidtime  TIMESTAMP(3),
    price    DOUBLE,
    item     STRING,
    supplier STRING,
    WATERMARK FOR bidtime AS bidtime - INTERVAL '5' SECONDS
)
    WITH ( 'connector' = 'faker', 'fields.bidtime.expression' = '#{date.past ''30'',''SECONDS''}', 'fields.price.expression' = '#{Number.randomDouble ''2'',''1'',''150''}', 'fields.item.expression' = '#{Commerce.productName}', 'fields.supplier.expression' = '#{regexify ''(Alice|Bob|Carol|Alex|Joe|James|Jane|Jack)''}', 'rows-per-second' = '100' );
/*
 you will use the `Window Top-N` [feature]
 to display the top 3 suppliers with the highest sales every 5 minutes.
 */

with wd     as (select window_start
                     , window_end
                     , supplier
                     , count(*)   as cnt
                     , sum(price) as amount
                from table(tumble(table orders,descriptor(bidtime),interval '30' second))
                group by window_start, window_end, supplier)
   , seq_wd as (select window_start
                     , window_end
                     , supplier
                     , cnt
                     , amount
                     , row_number() over(partition by window_start , window_end order by amount desc) as top_sup
                from wd)
select *
from seq_wd
where top_sup <= 3
;


/*
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
*/