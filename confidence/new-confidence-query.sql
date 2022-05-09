SHOW max_parallel_workers_per_gather;

SET max_parallel_workers_per_gather = 4;

--explain (analyze, COSTS, verbose, BUFFERS, FORMAT JSON)
CREATE TABLE temp.demographic_area_confidence AS
SELECT
    bd_label,
    bd_community_type,
    round(bd_confidence::numeric,2) bd_confidence,
    round((sum(bd_bg_area_confidence)*100)::numeric, 2) bd_area_confidence,
    round((sum(bd_bg_pop_confidence)*100)::numeric, 2) bd_pop_confidence    
FROM (
	SELECT
        bd_label,
        bd_id,
        bd_community_type,
        bd_confidence,
        bd_area,
        bg_id,
        bg_pop,
        sum(bl_pop_adjusted) bg_pop_adjusted,
        st_area(st_intersection(bd_geom,bg_geom),true) bg_area_intersect,
        st_area(st_intersection(bd_geom,bg_geom),true)/bg_area * st_area(st_intersection(bd_geom,bg_geom),true)/bd_area bd_bg_area_confidence,
        sum(bl_pop_adjusted)/bg_pop * st_area(st_intersection(bd_geom,bg_geom),true)/bd_area  bd_bg_pop_confidence,
        sum(bl_pop_adjusted),
        sum(area_interect) area_interect
    FROM(
    SELECT
            bd.label AS bd_label,
            bd._id AS bd_id,
            demo.community_type AS bd_community_type,
            demo.confidence AS bd_confidence,
            st_area(bd.geom, true) bd_area,
            bd.geom AS bd_geom,
            bg.blockgroupid AS bg_id,
            nullif(bg.pop10,0) AS bg_pop,
            st_area (bg.geom, TRUE) bg_area,
            bg.geom AS bg_geom,
            bl.blockid10 AS bl_id,
            nullif(bl.pop10, 0) bl_pop,
            round( st_area(st_intersection(bl.geom, bd.geom), TRUE) / st_area(bl.geom, TRUE) * nullif(bl.pop10,0)) bl_pop_adjusted,
            st_area(st_intersection(bl.geom, bd.geom), TRUE) area_interect
        FROM ( SELECT _id, community_type, confidence FROM temp.demographics_csv WHERE community_type IN ( 'neighborhood', 'microNeighborhood', 'community', 'compositeCommunity' ) ) demo
        JOIN ( SELECT _id, label, geom FROM liveby.mongo_boundaries ) bd ON demo._id = bd._id
        LEFT JOIN ( SELECT geom, pop10, blockgroupid FROM tiger2019.bg_pophu ) bg ON st_intersects (bg.geom, bd.geom)
        LEFT JOIN ( SELECT geom, pop10, blockid10 FROM tiger2010.tabblock ) bl ON st_intersects (bl.geom, bd.geom)
        WHERE
            bl.blockid10 LIKE concat(bg.blockgroupid,'%') AND
            round( ( st_area (st_intersection (bl.geom, bd.geom), TRUE) / st_area (bd.geom, TRUE) ):: NUMERIC, 3 ) > 0 AND
            round( ( st_area (st_intersection (bg.geom, bd.geom), TRUE) / st_area (bd.geom, TRUE) ):: NUMERIC, 3 ) > 0
        GROUP BY bd.label, bd._id, bd.geom, demo.confidence, demo.community_type, bg.geom, bg.pop10, bg.blockgroupid, bl.geom, bl.blockid10, bl.pop10
        ) bl_bg_bd
    GROUP BY bd_label, bd_id, bd_area, bd_community_type, bd_confidence, bd_area, bd_geom, bg_id, bg_pop, bg_area, bg_geom
    ) bg_bd
GROUP BY bd_label, bd_community_type, bd_confidence, bd_area;