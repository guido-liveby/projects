-- $1 - start date
-- $2 - end date
SELECT
  tb.properties_label boundary_label,
  avg(
    CASE WHEN "OnMarketDate" BETWEEN $1 AND $2 THEN
      "ListPrice"::float8
    END)::float8::numeric::money average_list_price,
  avg(
    CASE WHEN "CloseDate" BETWEEN $1 AND $2
      AND "StandardStatus" = 'Closed'
      AND "ClosePrice" > 0 THEN
      "ClosePrice"::float8
    END)::float8::numeric::money average_sales_price,
  sum(
    CASE WHEN "ContingentDate" BETWEEN $1 AND $2 THEN
      1
    END) contracts_count,
  to_char(avg(
      CASE WHEN "CloseDate" BETWEEN $1 AND $2
        AND "StandardStatus" = 'Closed' THEN
        "DaysOnMarket"::float8
      END), '999G999G999') avg_days_on_market,
  median (
    CASE WHEN "CloseDate" BETWEEN $1 AND $2
      AND "StandardStatus" = 'Closed'
      AND "ClosePrice" > 0 THEN
      "ClosePrice"::float8
    END)::float8::numeric::money median_sales_price,
  sum(
    CASE WHEN "OnMarketDate" BETWEEN $1 AND $2 THEN
      1
    END) new_listings_count,
  avg(
    CASE WHEN "CloseDate" BETWEEN $1 AND $2
      AND "StandardStatus" = 'Closed'
      AND "ClosePrice" > 0
      AND "LivingArea" > 0 THEN
      ("ClosePrice" / "LivingArea")
    END)::float8::numeric::money avg_price_per_sqft,
  to_char(avg(
      CASE WHEN "CloseDate" BETWEEN $1 AND $2
        AND "StandardStatus" = 'Closed'
        AND "ClosePrice" > 0
        AND "ListPrice" > 0 THEN
        (100 * ("ClosePrice"::float8 / "ListPrice"::float8))::float8
      END), '999G999D999"%"') sold_to_list_ratio,
  sum(
    CASE WHEN "CloseDate" BETWEEN $1 AND $2
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