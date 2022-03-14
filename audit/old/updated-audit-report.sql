with audit as (select
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
)
select
  audit_field,
  audit_level,
  array_length(Array_agg(missing), 1) as "amount of MLSs",
  array_to_string( Array_agg( missing order by missing ), ',' ) as "MLSs with issue" from
  (
    select 'Living Area' audit_field, 'Missing' as audit_level, vendor as missing from audit -- LivingArea where "LivingAreaOutliersPercent" = 100 
    UNION ALL
    select 'Living Area' audit_field, 'Sparse' as audit_level, vendor as missing from audit where "LivingAreaOutliersPercent" != 100 and "LivingAreaOutliersPercent" > 15 --Close Date 
    UNION ALL
    select 'Close Date' audit_field, 'Missing' as audit_level, vendor as missing from audit where "CloseDateInRangePercent" = 0
    UNION ALL
    select 'Close Date' audit_field, 'Sparse' as audit_level, vendor as missing from audit where "CloseDateInRangePercent" != 0 and "CloseDateInRangePercent" < 85
    UNION ALL
--    select 'List Date' audit_field, 'Missing' as audit_level, vendor as missing from audit where "ListDatePercent" = 0 
--    UNION ALL
--    select 'List Date' audit_field, 'Sparse' as audit_level, vendor as missing from audit where "ListDatePercent" != 0 and "ListDatePercent" < 85
--    UNION ALL
--    select 'Contract Date' audit_field, 'Missing' as audit_level, vendor as missing from audit where "ContractDateInRangePercent" = 0
--    UNION ALL
--    select 'Contract Date' audit_field, 'Sparse' as audit_level, vendor as missing from audit where "ContractDateInRangePercent" != 0 and "ContractDateInRangePercent" < 85
    UNION ALL
    select 'Close Price' audit_field, 'Missing' as audit_level, vendor as missing from audit where "ClosePriceOutliersPercent" = 100 union ALL select 'Close Price' audit_field, 'Sparse' as audit_level, vendor as missing from audit where "ClosePriceOutliersPercent" != 100 and "ClosePriceOutliersPercent" > 15
    UNION ALL
    select 'List Price' audit_field, 'Missing' as audit_level, vendor as missing from audit where "ListPriceOutliersPercent" = 100
    UNION ALL
    select 'List Price' audit_field, 'Sparse' as audit_level, vendor as missing from audit where "ListPriceOutliersPercent" != 100 and "ListPriceOutliersPercent" > 15
    UNION ALL
    select 'Days On Market' audit_field, 'Missing' as audit_level, vendor as missing from audit where "DOMOutliersPercent" = 100
    UNION ALL
    select 'Days On Market' audit_field, 'Sparse' as audit_level, vendor as missing from audit where "DOMOutliersPercent" != 100 and "DOMOutliersPercent" > 15
    UNION ALL
    select 'Coordinates' audit_field, 'Missing' as audit_level, vendor as missing from audit where "MissingGeometryPercent" = 100
    UNION ALL
    select 'Coordinates' audit_field, 'Sparse' as audit_level, vendor as missing from audit where "MissingGeometryPercent" != 100 and "MissingGeometryPercent" > 15
    UNION ALL
    select 'PropertyType' audit_field, 'Missing' as audit_level, vendor as missing from audit where "HasPropertyTypePercent" = 0
    UNION ALL
    select 'PropertyType' audit_field, 'Sparse' as audit_level, vendor as missing from audit where "HasPropertyTypePercent" != 0 and "HasPropertyTypePercent" < 85
    UNION ALL
    select 'Property SubType' audit_field, 'Missing' as audit_level, vendor as missing from audit where "HasUsablePropertySubTypePercent" = 0
    UNION ALL
    select 'Property SubType' audit_field, 'Sparse' as audit_level, vendor as missing from audit where "HasUsablePropertySubTypePercent" != 0 and "HasUsablePropertySubTypePercent" < 85 
    UNION ALL
    select 'Geom' audit_field, 'Missing' as audit_level, vendor as missing from audit where "HasCoordinates" = 0
    UNION ALL
    select 'PostalCode' audit_field, 'Missing' as audit_level, vendor as missing from audit where "PostalCodeTotal" = 0
    UNION ALL
    select 'PostalCode' audit_field, 'Sparse' as audit_level, vendor as missing from audit where "PostalCodeTotal" between 1 and 85
    UNION ALL
    select 'City' audit_field, 'Missing' as audit_level, vendor as missing from audit where "CityTotal" = 0
    UNION ALL
    select 'City' audit_field, 'Sparse' as audit_level, vendor as missing from audit where "CityTotal"  between 1 and 85
    UNION ALL
    select 'StandardStatus' audit_field, 'Missing' as audit_level, vendor as missing from audit where "StandardStatusTotal" = 0
    UNION ALL
    select 'StandardStatus' audit_field, 'Sparse' as audit_level, vendor as missing from audit where "StandardStatusTotal" between 1 and 85
    UNION ALL
    select 'StateOrProvince' audit_field, 'Missing' as audit_level, vendor as missing from audit where "StateTotal" = 0
    UNION ALL
    select 'StateOrProvince' audit_field, 'Sparse' as audit_level, vendor as missing from audit where "StateTotal" between 1 and 85
  ) as audit_vals
group by
  audit_field,
  audit_level
order by "amount of MLSs" DESC