-- This query audit records that have a close date within the following parameters
--
--   valid_close_year (e.g. 2017 and 2022)
--   table (e.g. properties.reso_properties_backup)
--   vendor (any (array['bari','ftl','crmls','ccimls','claw','har','mlslistings','sfar','smart','sfe']))

select
  vendor,
  count(1) "RecordCount",
  count(case when "StandardStatus" = 'Active' then 1 end) as "ActiveCount",
  count(case when "StandardStatus" = 'ActiveUnderContract' then 1 end ) as "ActiveUnderContractCount",
  count( case when "StandardStatus" = 'Pending' then 1 end ) as "PendingCount",
  count( case when "StandardStatus" = 'Closed' then 1 end ) as "SoldCount",
  count( case when "LivingArea" not between 200 and 40000 then 1 end ) as "LivingAreaOutliers",
  count("LivingArea") as "LivingAreaTotal",
  (count( case when "LivingArea" not between 200 and 40000 then 1 end ) 
    / nullif (count(*), 0) :: float8 ) * 100.0 as "LivingAreaOutliersPercent",
  count( case when "StandardStatus" = ' Closed' then "CloseDate" end ) as "CloseDateInRange",
  count("CloseDate") as "CloseDateTotal",
  (count( case when "StandardStatus" = 'Closed' then  "CloseDate" end ) 
      / nullif (count( case when "StandardStatus" = ' Closed' then 1 end ), 0) :: float8) * 100.0 as "CloseDateInRangePercent",
--  count(case when "OnMarketDate" between >= '2020-01-01 ' then "OnMarketDate" end) as "ListDateInRange",
--  count("OnMarketDate") as "ListDateTotal",
--  (count(case when "OnMarketDate" >= '2020-01-01 ' then "OnMarketDate" end) 
--    / nullif (count(*), 0) :: float8) * 100 as "ListDateInRangePercent",
--  count(case when "ContingentDate" >= '2020-01-01 ' then "ContingentDate" end) as "ContractDateInRange",
--  count("ContingentDate") as "ContingentDateTotal",
--  (count(case when "ContingentDate" >= '2020-01-01 ' then "ContingentDate" end) 
--    / nullif(count(case when "StandardStatus" = 'Closed' then 1 end),0) :: float8) * 100 as "ContractDateInRangePercent",
  count(case when "ClosePrice" :: float8 not between 15000 and 100000000 then 1 end ) as "ClosePriceOutliers",
  count("ClosePrice") as "ClosePriceTotal",
  (count(case when "ClosePrice" :: float8 not between 15000 and 100000000 then 1 end) 
    / nullif (count(case when "StandardStatus" = ' Closed' then 1 end), 0) :: float8 ) * 100 as "ClosePriceOutliersPercent",
  count(case when "ListPrice" :: float8 not between 15000 and 100000000 then 1 end) as "ListPriceOutliers",
  count("ListPrice") as "ListPriceTotal",
  (count(case when "ListPrice" :: float8 not between 15000 and 100000000 then 1 end) 
    / nullif (count(*), 0) :: float8) * 100 as "ListPriceOutliersPercent",
  count(case when "OriginalListPrice" :: float8 not between 15000 and 100000000 then 1 end) as "OriginalListPriceOutliers",
  count("OriginalListPrice") as "OriginalListPriceTotal",
  (count(case when "OriginalListPrice" :: float8 not between 15000 and 100000000 then 1 end)
    / nullif (count(*), 0) :: float8) * 100 as "OriginalListPriceOutliersPercent",
  count(case when "DaysOnMarket" :: float8 not between 0 and 300 then "DaysOnMarket" end) as "DOMOutliers",
  count("DaysOnMarket") as "DOMTotal",
  (count(case when "DaysOnMarket" :: float8 not between 0 and 300 then "DaysOnMarket" end)
    / nullif (count(*), 0) :: float8) * 100 as "DOMOutliersPercent",
  count("geom") as "HasCoordinates",
  count( case when geom is null then 1 end ) as "MissingGeometry",
  ( count( case when geom is null then 1 end ) / count(*) :: float8 ) * 100 as "MissingGeometryPercent",
  count("PropertyType") "HasPropertyType",
  count(case when "PropertyType" in (' RES', ' CND') then 1 end ) as "HasUsablePropertyType",
  (count("PropertyType") / nullif (count(*), 0) :: float8) * 100 "HasPropertyTypePercent",
  (count(case when "PropertyType" in (' RES', ' CND') then 1 end) / nullif (count(*), 0) :: float8) * 100 as "HasUsablePropertyTypePercent",
  count("PropertySubType") "HasPropertySubType",
  count(case when "PropertySubType" in ('SingleFamilyResidence','Condominium','Townhouse') then 1 end) "HasUsablePropertySubType",
  (count(case when "PropertySubType" in ('SingleFamilyResidence', 'Condominium', 'Townhouse') then 1 end ) 
    / nullif (count(*), 0) :: float) * 100 "HasUsablePropertySubTypePercent",
  count("PostalCode") "PostalCodeTotal",
  count("City") "CityTotal",
  count("StateOrProvince") "StateTotal"
from
  { table }
where
  vendor = { vendor }
  and extract (year from "CloseDate") between {valid_close_years}
group by
  vendor
