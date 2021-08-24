SELECT jsonb_build_object(
    'type',
    'FeatureCollection',
    'features',
    jsonb_agg(features.feature)
  )
FROM (
    SELECT jsonb_build_object(
        'type',
        'Feature',
        'id',
        gid,
        'geometry',
        ST_AsGeoJSON(geom)::jsonb,
        'properties',
        to_jsonb(inputs) - 'gid' - 'geom'
      ) AS feature
    FROM (
        SELECT t.gid,
          t.geoid,
          t.geom
        FROM tiger2019.tract AS t,
          (
            SELECT *
            from tiger2019.uac
            where name10 = 'San Francisco--Oakland, CA'
          ) AS s
        WHERE st_intersects(ST_centroid(t.geom), s.geom)
      ) inputs
  ) features;