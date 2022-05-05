SHOW max_parallel_workers_per_gather;

SET max_parallel_workers_per_gather = 4;

--explain (analyze, COSTS, verbose, BUFFERS, FORMAT JSON)
create table temp.demographic_area_confidence as
SELECT
    bd_label,
    bd_id,
    round(bd_area_confidence:: NUMERIC, 2) bd_area_confidence_original,
    round( (SUM(bg_bd_confidence) * 100):: NUMERIC, 2) bd_area_confidence
FROM (
    SELECT
        bd.label AS bd_label,
        bd._id AS bd_id,
        demo.confidence AS bd_area_confidence,
        st_area(st_intersection(bg.geom, bd.geom), TRUE) / st_area(bd.geom, TRUE) * st_area(st_intersection(bg.geom, bd.geom), TRUE) / st_area(bg.geom, TRUE) bg_bd_confidence
    FROM (
        SELECT _id, community_type, confidence
        FROM temp.demographics_csv
        WHERE
            community_type IN (
                'neighborhood',
                'microNeighborhood',
                'community',
                'compositeCommunity'
            )
    ) demo
    JOIN (
        SELECT _id, label, geom
        FROM
            liveby.mongo_boundaries
    ) bd
    ON demo._id = bd._id
    LEFT JOIN (
        SELECT geom, pop10
        FROM tiger2019.bg_pophu
        WHERE
            pop10 > 0
    ) bg
    ON st_intersects(bd.geom, bg.geom) AND
    round( (
            st_area(st_intersection(bd.geom, bg.geom), TRUE) / st_area(bg.geom, TRUE)
        ):: NUMERIC,
        2
    ) > 0
    GROUP BY
        bd.label,
        bd._id,
        bd.geom,
        demo.confidence,
        bg.geom
) bg_bd
GROUP BY bd_label, bd_id, bd_area_confidence;
