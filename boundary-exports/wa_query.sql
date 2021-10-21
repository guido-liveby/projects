with wa as (
  select *
  from liveby.mongo_boundaries mb
  where mb."type" = 'state'
    and mb."id" = 'WA'
)
select distinct on (mb."type") mb."id",
  mb."_id",
  mb."label",
  mb."type",
  st_multi(mb.simple_geom)::geometry(MultiPolygon, 4326) geometry
from liveby.mongo_boundaries mb
  join wa on st_intersects(mb.geom, wa.geom)
where mb."type" in (
    'compositeCommunity',
    'county',
    'countySubdivision',
    'microNeighborhood',
    'neighborhood',
    'urbanArea',
    'schoolAttendanceArea',
    'schoolDistrict',
    'zipcode'
  )
order by mb."type"

