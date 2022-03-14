SELECT
  vendor,
  count( CASE WHEN "StandardStatus" = 'Active' THEN 1 END) AS "ActiveCount", 
  count( CASE WHEN "StandardStatus" = 'ActiveUnderContract' THEN 1 END) AS "ActiveUnderContractCount",
  count( CASE WHEN "StandardStatus" = 'Pending' THEN 1 END) AS "PendingCount",
  count( CASE WHEN "StandardStatus" = 'Closed' THEN 1 END) AS "SoldCount",
  count( CASE WHEN "LivingArea" NOT BETWEEN 200 AND 40000 THEN 1 END) AS "LivingAreaOutliers",
  count("LivingArea") AS "LivingAreaTotal",
  (count( CASE WHEN "LivingArea" BETWEEN 200 AND 40000 THEN NULL ELSE 1 END) / nullif (count(*), 0)::double precision) * 100.0 AS "LivingAreaOutliersPercent",
  count( CASE WHEN "CloseDate" < '2020-01-01' AND "StandardStatus" != 'Closed' THEN NULL ELSE "CloseDate" END) AS "CloseDateInRange",
  count("CloseDate") AS "CloseDateTotal",
  (count( CASE WHEN "CloseDate" < '2020-01-01' AND "StandardStatus" != 'Closed' THEN NULL ELSE "CloseDate" END) / nullif (count( CASE WHEN "StandardStatus" = 'Closed' THEN 1 ELSE NULL END), 0)::double precision) * 100.0 AS "CloseDateInRangePercent",
  count( CASE WHEN "OnMarketDate" < '2020-01-01' THEN NULL ELSE "OnMarketDate" END) AS "ListDateInRange",
  count("OnMarketDate") AS "ListDateTotal",
  (count( CASE WHEN "OnMarketDate" < '2020-01-01' THEN NULL ELSE "OnMarketDate" END) / nullif (count(*), 0)::double precision) * 100 AS "ListDateInRangePercent",
  count( CASE WHEN "ContingentDate" < '2020-01-01' THEN NULL ELSE "ContingentDate" END) AS "ContractDateInRange",
  count("ContingentDate") AS "ContingentDateTotal", (count( CASE WHEN "ContingentDate" < '2020-01-01' THEN NULL ELSE "ContingentDate" END) / nullif (count( CASE WHEN "StandardStatus" = 'Closed' THEN 1 ELSE NULL END), 0)::double precision) * 100 AS "ContractDateInRangePercent",
  count( CASE WHEN "ClosePrice"::float8 BETWEEN 15000 AND 100000000 THEN NULL ELSE 1 END) AS "ClosePriceOutliers",
  count("ClosePrice") AS "ClosePriceTotal", (count( CASE WHEN "ClosePrice"::float8 BETWEEN 15000 AND 100000000 THEN NULL ELSE 1 END) / nullif (count( CASE WHEN "StandardStatus" = 'Closed' THEN 1 ELSE NULL END), 0)::double precision) * 100 AS "ClosePriceOutliersPercent",
  count( CASE WHEN "ListPrice"::float8 BETWEEN 15000 AND 100000000 THEN NULL ELSE 1 END) AS "ListPriceOutliers",
  count("ListPrice") AS "ListPriceTotal",
  (count( CASE WHEN "ListPrice"::float8 BETWEEN 15000 AND 100000000 THEN NULL ELSE 1 END) / nullif (count(*), 0)::double precision) * 100 AS "ListPriceOutliersPercent",
  count( CASE WHEN "OriginalListPrice"::float8 BETWEEN 15000 AND 100000000 THEN NULL ELSE 1 END) AS "OriginalListPriceOutliers",
  count("OriginalListPrice") AS "OriginalListPriceTotal",
  (count( CASE WHEN "OriginalListPrice"::float8 BETWEEN 15000 AND 100000000 THEN NULL ELSE 1 END) / nullif (count(*), 0)::double precision) * 100 AS "OriginalListPriceOutliersPercent",
  count( CASE WHEN "DaysOnMarket"::float8 BETWEEN 0 AND 300 THEN NULL ELSE "DaysOnMarket" END) AS "DOMOutliers",
  count("DaysOnMarket") AS "DOMTotal",
  (count( CASE WHEN "DaysOnMarket"::float8 BETWEEN 0 AND 300 THEN NULL ELSE "DaysOnMarket" END) / nullif (count(*), 0)::double precision) * 100 AS "DOMOutliersPercent",
  count("geom") AS "HasCoordinates",
  count( CASE WHEN geom IS NULL THEN 1 ELSE NULL END) AS "MissingGeometry",
  (count( CASE WHEN geom IS NULL THEN 1 ELSE NULL END) / count(*)::double precision) * 100 AS "MissingGeometryPercent",
  count("PropertyType") "HasPropertyType",
  count( CASE WHEN "PropertyType" IN ('RES', 'CND') THEN 1 ELSE NULL END) AS "HasUsablePropertyType",
  (count("PropertyType") / nullif (count(*), 0)::double precision) * 100 "HasPropertyTypePercent",
  (count( CASE WHEN "PropertyType" IN ('RES', 'CND') THEN 1 ELSE NULL END) / nullif (count(*), 0)::double precision) * 100 AS "HasUsablePropertyTypePercent",
  count("PropertySubType") "HasPropertySubType",
  count( CASE WHEN "PropertySubType" IN ('SingleFamilyResidence', 'Condominium', 'Townhouse') THEN 1 ELSE NULL END) "HasUsablePropertySubType",
  (count( CASE WHEN "PropertySubType" IN ('SingleFamilyResidence', 'Condominium', 'Townhouse') THEN 1 ELSE NULL END) / nullif (count(*), 0)::double precision) * 100 "HasUsablePropertySubTypePercent",
  count("PostalCode") "PostalCodeTotal",
  count("City") "CityTotal",
  count("StateOrProvince") "StateTotal"
FROM
  properties.reso_properties_backup
WHERE
  vendor = any (array['sfar', 'claw', 'crmls'])
  
GROUP BY
  vendor