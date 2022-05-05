SELECT *
FROM (
    SELECT
        bd.label AS bd_label,
        bd._id AS bd_id,
        de.community_type AS bd_community_type,
        round(de.confidence:: NUMERIC, 2) bd_confidence,
        round( (
                100 * ( (st_area(st_intersection(bg.geom, bd.geom), TRUE)) / (st_area(bg.geom, TRUE)) -- bg_area_percent
                ) * ( (st_area(st_intersection(bg.geom, bd.geom), TRUE)) / (st_area(bd.geom, TRUE)) -- bd_area_percent
                )
            ):: NUMERIC,
            2
        ) area_confidence,
        round(
            st_area(
                st_intersection(bd.geom, st_intersection(bg.geom, bl.geom)),
                TRUE
            ) / st_area(bl.geom, TRUE) * bl.pop10
        ) / bg.pop10
) bg_bl_pop_percent,
SUM(st_area(st_intersection(bg.geom, bd.geom), TRUE)) / SUM(st_area(bd.geom, TRUE)) bd_area_percent
FROM liveby.mongo_boundaries bd
    JOIN (
        SELECT
            cs._id,
            cs.confidence,
            cs.community_type
        FROM temp.demographics_csv cs
            JOIN liveby.mongo_boundaries mb
            ON cs._id = mb._id
        WHERE
            cs.community_type IN (
                'neighborhood',
                'microNeighborhood',
                'community',
                'compositeCommunity'
            )
    ) de
    ON de._id = bd._id
    LEFT JOIN tiger2019.bg_pophu bg
    ON st_intersects(bd.geom, bg.geom) AND
    NOT st_touches(bd.geom, bg.geom) AND
    st_area(st_intersection(bd.geom, bg.geom), TRUE) > 1
    LEFT JOIN tiger2010.tabblock bl
    ON st_intersects(bd.geom, bl.geom) AND
    NOT st_touches(bd.geom, bl.geom) AND
    st_area(st_intersection(bd.geom, bl.geom), TRUE) > 1
WHERE
    bl.pop10 > 0 AND
    bg.pop10 > 0 AND (
        st_area(st_intersection(bl.geom, bd.geom), TRUE) / st_area(bl.geom, TRUE)
    ) >.005 AND
    round( (
            st_area(
                st_intersection(bl.geom, st_intersection(bd.geom, bg.geom)),
                TRUE
            ) / st_area(bl.geom, TRUE)
        ):: NUMERIC,
        2
    ) > 0
GROUP BY
    bd.label,
    bd._id,
    bd.geom,
    de.community_type,
    de.confidence,
    bg.geom,
    bl.geom,
    bl.pop10,
    bg.pop10
) parts
WHERE
    bd_id IN (
        '56699b21dd4700202391a9a5',
        '56699b21dd4700202391a9aa'
    );

SELECT
    bd_label,
    bd_id,
    bd_community_type,
    SUM(bg_bd_area_confidence) bg_bd_area_confidence
FROM(
    SELECT
        bd.label AS bd_label,
        bd._id AS bd_id,
        de.community_type AS bd_community_type,
        round( (
                100 -- percent conversion
                * ( (st_area(st_intersection(bg.geom, bd.geom), TRUE)) / (st_area(bg.geom, TRUE))
                ) -- bg_area_percent
                * ( (st_area(st_intersection(bg.geom, bd.geom), TRUE)) / (st_area(bd.geom, TRUE))
                ) -- bd_area_percent
            ):: NUMERIC,
            2
        ) bg_bd_area_confidence
    FROM liveby.mongo_boundaries bd
        JOIN (
            SELECT cs._id, cs.community_type
            FROM temp.demographics_csv cs
                JOIN liveby.mongo_boundaries mb
                ON cs._id = mb._id
            WHERE
                cs.community_type IN (
                    'neighborhood',
                    'microNeighborhood',
                    'community',
                    'compositeCommunity'
                )
        ) de
        ON de._id = bd._id
        LEFT JOIN tiger2019.bg_pophu bg
        ON st_intersects(bd.geom, bg.geom) AND
        NOT st_touches(bd.geom, bg.geom) AND
        st_area(st_intersection(bd.geom, bg.geom), TRUE) > 1
        LEFT JOIN tiger2010.tabblock bl
        ON st_intersects(bd.geom, bl.geom) AND
        NOT st_touches(bd.geom, bl.geom) AND
        st_area(st_intersection(bd.geom, bl.geom), TRUE) > 1
    WHERE
        bl.pop10 > 0 AND
        bg.pop10 > 0 AND (
            st_area(st_intersection(bl.geom, bd.geom), TRUE) / st_area(bl.geom, TRUE)
        ) >.005 AND
        round( (
                st_area(
                    st_intersection(bl.geom, st_intersection(bd.geom, bg.geom)),
                    TRUE
                ) / st_area(bl.geom, TRUE)
            ):: NUMERIC,
            2
        ) > 0
    GROUP BY
        bd.label,
        bd._id,
        bd.geom,
        de.community_type,
        bg.geom
) parts
WHERE
    bd_id IN (
        '56699b21dd4700202391a9a5',
        '56699b21dd4700202391a9aa'
    )
GROUP BY bd_label, bd_id, bd_community_type;