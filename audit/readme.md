# Audit Review Process

- run report
- if issue
  - check geometry
  - check if more subtypes need to be added
  - check initial report and see if the data was input badly

query to select counts by area _id

```sql
select 
    count(1),
    "PropertyType"
from properties.reso_properties_backup as reso 
join liveby.mongo_boundaries as bounds on
    bounds._id = '5c89e62720427c0001889c78'
    and ST_Intersects(reso.geom, bounds.geom)
where
    "PropertyType" in ('CND', 'RES')
    and "StandardStatus" = 'Closed'
    and "PropertySubType" in ('SingleFamilyResidence','Condominium')
    and vendor = 'claw'
    and extract(year from "CloseDate") = 2021
    and extract(quarter from "CloseDate") = 4
-- group by "PropertyType"
;
```
