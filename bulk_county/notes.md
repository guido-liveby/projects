# Notes

## general

```sql
-- get current query status

select pid, age(clock_timestamp(), query_start), usename, query, application_name
from pg_stat_activity
where application_name ~ 'DBeaver' and query !~ 'pg_stat_activity'
order by query_start desc;
```

## 2021-10-26

```sql
select distinct "PropertySubType" from properties.reso_properties where vendor='bright' and "StandardStatus" = 'Closed';
-- 14m 17s
```

| PropertySubType       |
| --------------------- |
| [NULL]                |
|                       |
| Apartment             |
| Business              |
| DeededParking         |
| Duplex                |
| FarmAndRanch          |
| Industrial            |
| Land                  |
| ManufacturedHome      |
| MixedUse              |
| Mobile Home           |
| Office                |
| OfficeSpace           |
| Other                 |
| Quadruplex            |
| Retail                |
| SingleFamilyResidence |
| Townhouse             |
| Triplex               |
| Services              |

**10:45**

```sql
select distinct "PropertySubType" from properties.reso_properties where vendor='bright';
select distinct "StandardStatus" from properties.reso_properties where vendor='bright';
```

**12:46**

```sql
select distinct "StandardStatus" from properties.reso_properties where vendor='bright';
-- 89m 58s
```

| StandardStatus      |
| ------------------- |
| Active              |
| ActiveUnderContract |
| Closed              |
| Delete              |
| Expired             |
| Hold                |
| Pending             |
| Withdrawn           |

### create temporary reso property table

Extract properties from reso table to improve performance on aggregation script

- remove unused fields
- use index to extract data

**15:30**

```sql
drop table if exists public.tmp_reso_properties cascade;

CREATE TABLE public.tmp_reso_properties AS
-- 'Closed'
SELECT "CloseDate", "ClosePrice", "DaysOnMarket", "ListOfficeName", "ListPrice", "ListingId", "MlsStatus", "OnMarketDate", "StandardStatus", "City", "Latitude", "LivingArea", "Longitude", "PostalCode", "PropertySubType", "PropertyType", "ContingentDate", "StateOrProvince", "UnparsedAddress", reso."geom", "vendor", "Media", "url", "PropertySubTypeText"
FROM properties.reso_properties reso, public.tmp_bounds tb
WHERE st_intersects (tb.geom, reso.geom) AND "PropertySubType" IN ('SingleFamilyResidence', 'Townhouse', 'Condominium') AND "StandardStatus" = 'Closed'
UNION ALL
-- 'Active'
SELECT "CloseDate", "ClosePrice", "DaysOnMarket", "ListOfficeName", "ListPrice", "ListingId", "MlsStatus", "OnMarketDate", "StandardStatus", "City", "Latitude", "LivingArea", "Longitude", "PostalCode", "PropertySubType", "PropertyType", "ContingentDate", "StateOrProvince", "UnparsedAddress", reso."geom", "vendor", "Media", "url", "PropertySubTypeText"
FROM properties.reso_properties reso, public.tmp_bounds tb
WHERE st_intersects (tb.geom, reso.geom) AND "PropertySubType" IN ('SingleFamilyResidence', 'Townhouse', 'Condominium') AND "StandardStatus" = 'Active'
UNION ALL
-- 'Pending'
SELECT "CloseDate", "ClosePrice", "DaysOnMarket", "ListOfficeName", "ListPrice", "ListingId", "MlsStatus", "OnMarketDate", "StandardStatus", "City", "Latitude", "LivingArea", "Longitude", "PostalCode", "PropertySubType", "PropertyType", "ContingentDate", "StateOrProvince", "UnparsedAddress", reso."geom", "vendor", "Media", "url", "PropertySubTypeText"
FROM properties.reso_properties reso, public.tmp_bounds tb
WHERE st_intersects (tb.geom, reso.geom) AND "PropertySubType" IN ('SingleFamilyResidence', 'Townhouse', 'Condominium') AND "StandardStatus" = 'Pending'
UNION ALL
-- 'ActiveUnderContract'
SELECT "CloseDate", "ClosePrice", "DaysOnMarket", "ListOfficeName", "ListPrice", "ListingId", "MlsStatus", "OnMarketDate", "StandardStatus", "City", "Latitude", "LivingArea", "Longitude", "PostalCode", "PropertySubType", "PropertyType", "ContingentDate", "StateOrProvince", "UnparsedAddress", reso."geom", "vendor", "Media", "url", "PropertySubTypeText"
FROM properties.reso_properties reso, public.tmp_bounds tb
WHERE st_intersects (tb.geom, reso.geom) AND "PropertySubType" IN ('SingleFamilyResidence', 'Townhouse', 'Condominium') AND "StandardStatus" = 'ActiveUnderContract';
create index on public.tmp_reso_properties using GIST (geom);
create index on public.tmp_reso_properties ("PropertySubType");
-- 23s 380ms
```

**17:09**

### Generate bulk market data table

```sql
SELECT
  tb.properties_label boundary_label,
  avg(
    CASE WHEN "OnMarketDate" BETWEEN '2021-01-01'
      AND '2021-03-31' THEN
      "ListPrice"::float8
    END)::float8::numeric::money average_list_price,
  avg(
    CASE WHEN "CloseDate" BETWEEN '2021-01-01'
      AND '2021-03-31'
      AND "StandardStatus" = 'Closed'
      AND "ClosePrice" > 0 THEN
      "ClosePrice"::float8
    END)::float8::numeric::money average_sales_price,
  sum(
    CASE WHEN "ContingentDate" BETWEEN '2021-01-01'
      AND '2021-03-31' THEN
      1
    END) contracts_count,
  avg(
      CASE WHEN "CloseDate" BETWEEN '2021-01-01'
        AND '2021-03-31'
        AND "StandardStatus" = 'Closed' THEN
        "DaysOnMarket"::float8
      END),  avg_days_on_market,
  median (
    CASE WHEN "CloseDate" BETWEEN '2021-01-01'
      AND '2021-03-31'
      AND "StandardStatus" = 'Closed'
      AND "ClosePrice" > 0 THEN
      "ClosePrice"::float8
    END)::float8::numeric::money median_sales_price,
  sum(
    CASE WHEN "OnMarketDate" BETWEEN '2021-01-01'
      AND '2021-03-31' THEN
      1
    END) new_listings_count,
  avg(
    CASE WHEN "CloseDate" BETWEEN '2021-01-01'
      AND '2021-03-31'
      AND "StandardStatus" = 'Closed'
      AND "ClosePrice" > 0
      AND "LivingArea" > 0 THEN
      ("ClosePrice" / "LivingArea")
    END)::float8::numeric::money avg_price_per_sqft,
  to_char(avg(
      CASE WHEN "CloseDate" BETWEEN '2021-01-01'
        AND '2021-03-31'
        AND "StandardStatus" = 'Closed'
        AND "ClosePrice" > 0
        AND "ListPrice" > 0 THEN
        (100 * ("ClosePrice"::float8 / "ListPrice"::float8))::float8
      END), '999G999D999"%"') sold_to_list_ratio,
  sum(
    CASE WHEN "CloseDate" BETWEEN '2021-01-01'
      AND '2021-03-31'
      AND "StandardStatus" = 'Closed'
      AND "ClosePrice" > 0 THEN
      "ClosePrice"
    END)::float8::numeric::money total_sales
FROM
  public.tmp_reso_properties reso,
  public.tmp_bounds tb
WHERE
  st_intersects (tb.geom, reso.geom)
  AND reso."PropertySubType" IN ('SingleFamilyResidence', 'Townhouse', 'Condominium')
GROUP BY
  tb.properties_label;
```
