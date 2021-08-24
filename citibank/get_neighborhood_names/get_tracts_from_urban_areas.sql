SELECT t.geoid geoid,
  ST_AsGeoJSON(t.geom) geom
FROM tiger2019.tract AS t,
  (
    SELECT *
    from tiger2019.uac
    where name10 = $1
  ) AS s
WHERE st_intersects(ST_centroid(t.geom), s.geom)