const debug = require('debug')('marketExport')
const { withPostgres } = require('./util')
const mgConnect = require('@liveby/mongodb-connect')
const { ObjectId } = require('mongodb')
const format = require('pg-format')

module.exports = {
  getBoundaryIds,
  getBoundaries,
  createBoundaryTable,
  buildReport
}

async function getBoundaryIds (params) {
  const { dbName = 'LiveBy', collection, find } = params
  const mgClient = await mgConnect()
  const data = await mgClient
    .db(dbName)
    .collection(collection)
    .find(find, { projection: { boundary: 1, _id: 0 } })
    .toArray()
  mgClient.close()
  return data.map((d) => String(d.boundary))
}

async function getBoundaries (params) {
  const {
    dbName = 'LiveBy',
    collection = 'neighborhoods',
    boundaryIds
  } = params
  const mgClient = await mgConnect()
  const data = await mgClient
    .db(dbName)
    .collection(collection)
    .find(
      { _id: { $in: boundaryIds.map((i) => ObjectId(i)) } },
      { projection: { type: 1, 'properties.label': 1, geometry: 1 } }
    )
    .toArray()
  mgClient.close()

  return data
}

async function createBoundaryTable (params) {
  const {
    boundaryTable = 'public.tmp_boundary',
    boundaries,
    sampleSize
  } = params

  let values = boundaries
    .map((b) => {
      const { _id, properties, geometry } = b
      return [String(_id), properties.label, geometry]
    })
  if (sampleSize) {
    values = values
      .sort(() => 0.5 - Math.random())
      .slice(0, sampleSize)
  }
  await withPostgres(async (pgClient) => {
    const dropTempTableQuery = getDropTempTableQuery({ boundaryTable })
    await pgClient.query(dropTempTableQuery)

    const createTempTableQuery = getCreateTempTableQuery({ boundaryTable })
    await pgClient.query(createTempTableQuery)

    const insertTempTableQuery = getInsertTempTableQuery({
      boundaryTable,
      values
    })
    await pgClient.query(insertTempTableQuery)

    const updateGeomQuery = getUpdateGeomQuery({ boundaryTable })
    await pgClient.query(updateGeomQuery)
  })

  return boundaryTable
}

async function buildReport (params) {
  const {
    dbName = 'LiveBy',
    propertyTable = 'public.tmp_reso_properties',
    collection,
    find,
    sampleSize,
    quarters = [{ quarterNumber: 2, year: 2021 }]
  } = params
  const timeStamp = (new Date()).toISOString().replace(/-|:/gi, '').split('.')[0]
  const reportTable = `public.report_output_${timeStamp}`

  const boundaryIds = await getBoundaryIds({ dbName, collection, find })
  const boundaries = await getBoundaries({ boundaryIds })
  const boundaryTable = await createBoundaryTable({ boundaries, sampleSize })

  await withPostgres(async (pgClient) => {
    const dropTmpPropertiesTableQuery = getDropTmpPropertiesTableQuery({ propertyTable })
    await pgClient.query(dropTmpPropertiesTableQuery)

    const generateTmpPropertiesTableQuery = getGenerateTmpPropertiesTableQuery({
      propertyTable,
      boundaryTable
    })
    await pgClient.query(generateTmpPropertiesTableQuery)

    const createReportTableQuery = getCreateReportTableQuery({ propertyTable, boundaryTable, reportTable })
    const insertReportTableQuery = getInsertReportTableQuery({ propertyTable, boundaryTable, reportTable })
    let firstQuery = true
    for (const q of quarters) {
      const { quarterNumber, year } = q
      if (firstQuery) {
        await pgClient.query(createReportTableQuery, [...getQuarterDates(q), year, quarterNumber])
        firstQuery = false
      }
      await pgClient.query(insertReportTableQuery, [...getQuarterDates(q), year, quarterNumber])
    }
  })
  return reportTable
}

function getCreateTempTableQuery (params) {
  const { boundaryTable } = params
  const sql = `create table IF NOT EXISTS ${boundaryTable} (
  id serial,
  mongo_id varchar(24),
  properties_label varchar,
  geom_obj jsonb,
  geom geometry(Multipolygon, 4326)
);
CREATE INDEX ON ${boundaryTable} USING GIST (geom);`
  return sql
}

function getDropTempTableQuery (params) {
  const { boundaryTable } = params
  const sql = `drop table if exists ${boundaryTable} cascade;`
  return sql
}

function getInsertTempTableQuery (params) {
  const { boundaryTable, values } = params
  const sql = format(
    `insert into ${boundaryTable} (mongo_id, properties_label, geom_obj) VALUES %L`,
    values
  )
  return sql
}

function getUpdateGeomQuery (params) {
  const { boundaryTable } = params
  const sql = `update ${boundaryTable} set geom = ST_SetSRID(ST_Multi(ST_GeomFromGeoJSON(geom_obj::text)),4326)`
  return sql
}

function getDropTmpPropertiesTableQuery (params) {
  const { propertyTable = 'public.tmp_reso_properties' } = params
  const sql = `
  drop table if exists ${propertyTable} cascade;
`
  return sql
}

function getGenerateTmpPropertiesTableQuery (params) {
  const {
    sourceTable = 'properties.reso_properties',
    propertyTable = 'public.tmp_reso_properties',
    boundaryTable = 'public.tmp_boundary',
    fields = '"CloseDate", "ClosePrice", "DaysOnMarket", "ListOfficeName", "ListPrice", "ListingId", "MlsStatus", "OnMarketDate", "StandardStatus", "City", "Latitude", "LivingArea", "Longitude", "PostalCode", "PropertySubType", "PropertyType", "ContingentDate", "StateOrProvince", "UnparsedAddress", reso."geom", "vendor", "Media", "url", "PropertySubTypeText"'
  } = params
  const sql = `
  CREATE TABLE ${propertyTable} AS
  -- 'Closed'
  SELECT ${fields}
  FROM 
    ${sourceTable} reso, 
    ${boundaryTable} tb
  WHERE 
    st_intersects (tb.geom, reso.geom) 
    AND "PropertySubType" IN ('SingleFamilyResidence', 'Townhouse', 'Condominium') 
    AND "StandardStatus" = 'Closed'
  UNION ALL
  -- 'Active'
  SELECT ${fields}
  FROM 
    ${sourceTable} reso, 
    ${boundaryTable} tb
  WHERE 
    st_intersects (tb.geom, reso.geom) 
    AND "PropertySubType" IN ('SingleFamilyResidence', 'Townhouse', 'Condominium') 
    AND "StandardStatus" = 'Active'
  UNION ALL
  -- 'Pending'
  SELECT ${fields}
  FROM 
    ${sourceTable} reso,
    ${boundaryTable} tb
  WHERE 
    st_intersects (tb.geom, reso.geom) 
    AND "PropertySubType" IN ('SingleFamilyResidence', 'Townhouse', 'Condominium') 
    AND "StandardStatus" = 'Pending'
  UNION ALL
  -- 'ActiveUnderContract'
  SELECT ${fields}
  FROM 
    ${sourceTable} reso, 
    ${boundaryTable} tb
  WHERE 
    st_intersects (tb.geom, reso.geom)
    AND "PropertySubType" IN ('SingleFamilyResidence', 'Townhouse', 'Condominium')
    AND "StandardStatus" = 'ActiveUnderContract';
  create index on ${propertyTable} using GIST (geom);
  create index on ${propertyTable} ("PropertySubType");
`
  return sql
}

function getCreateReportTableQuery (params) {
  const { reportTable, propertyTable, boundaryTable } = params
  const sql = `
  -- $1 - start date
  -- $2 - end date
  -- $3 - year
  -- $4 - quarter
  CREATE TABLE IF NOT EXISTS ${reportTable} AS
  SELECT
    tb.properties_label boundary_label,
    $3::numeric quarter_year,
    $4::numeric quarter_number,
    tb.geom geom,
    avg( CASE WHEN "OnMarketDate" BETWEEN $1 AND $2 THEN "ListPrice"::float8 END)::float8::numeric::money average_list_price,
    avg( CASE WHEN "CloseDate" BETWEEN $1 AND $2 AND "StandardStatus" = 'Closed' AND "ClosePrice" > 0 THEN "ClosePrice"::float8 END)::float8::numeric::money average_sales_price,
    sum( CASE WHEN "ContingentDate" BETWEEN $1 AND $2 THEN 1 END) contracts_count,
    to_char(avg( CASE WHEN "CloseDate" BETWEEN $1 AND $2 AND "StandardStatus" = 'Closed' THEN "DaysOnMarket"::float8 END), '999G999G999') avg_days_on_market,
    median ( CASE WHEN "CloseDate" BETWEEN $1 AND $2 AND "StandardStatus" = 'Closed' AND "ClosePrice" > 0 THEN "ClosePrice"::float8 END)::float8::numeric::money median_sales_price,
    sum( CASE WHEN "OnMarketDate" BETWEEN $1 AND $2 THEN 1 END) new_listings_count,
    avg( CASE WHEN "CloseDate" BETWEEN $1 AND $2 AND "StandardStatus" = 'Closed' AND "ClosePrice" > 0 AND "LivingArea" > 0 THEN ("ClosePrice" / "LivingArea") END)::float8::numeric::money avg_price_per_sqft,
    to_char(avg( CASE WHEN "CloseDate" BETWEEN $1 AND $2 AND "StandardStatus" = 'Closed' AND "ClosePrice" > 0 AND "ListPrice" > 0 THEN (100 * ("ClosePrice"::float8 / "ListPrice"::float8))::float8 END), '999G999D999"%"') sold_to_list_ratio,
    sum( CASE WHEN "CloseDate" BETWEEN $1 AND $2 AND "StandardStatus" = 'Closed' AND "ClosePrice" > 0 THEN "ClosePrice" END)::float8::numeric::money total_sales
  FROM ${propertyTable} reso, ${boundaryTable} tb
  WHERE 1=0
  GROUP BY tb.properties_label;
`

  return sql
}

function getInsertReportTableQuery (params) {
  const { reportTable, propertyTable, boundaryTable } = params

  const sql = `
  -- $1 - start date
  -- $2 - end date
  -- $3 - year
  -- $4 - quarter
  INSERT INTO ${reportTable}
  SELECT
    tb.properties_label boundary_label,
    $3::numeric quarter_year,
    $4::numeric quarter_number,
    tb.geom geom,
    avg( CASE WHEN "OnMarketDate" BETWEEN $1 AND $2 THEN "ListPrice"::float8 END)::float8::numeric::money average_list_price,
    avg( CASE WHEN "CloseDate" BETWEEN $1 AND $2 AND "StandardStatus" = 'Closed' AND "ClosePrice" > 0 THEN "ClosePrice"::float8 END)::float8::numeric::money average_sales_price,
    sum( CASE WHEN "ContingentDate" BETWEEN $1 AND $2 THEN 1 END) contracts_count,
    to_char(avg( CASE WHEN "CloseDate" BETWEEN $1 AND $2 AND "StandardStatus" = 'Closed' THEN "DaysOnMarket"::float8 END), '999G999G999') avg_days_on_market,
    median ( CASE WHEN "CloseDate" BETWEEN $1 AND $2 AND "StandardStatus" = 'Closed' AND "ClosePrice" > 0 THEN "ClosePrice"::float8 END)::float8::numeric::money median_sales_price,
    sum( CASE WHEN "OnMarketDate" BETWEEN $1 AND $2 THEN 1 END) new_listings_count,
    avg( CASE WHEN "CloseDate" BETWEEN $1 AND $2 AND "StandardStatus" = 'Closed' AND "ClosePrice" > 0 AND "LivingArea" > 0 THEN ("ClosePrice" / "LivingArea") END)::float8::numeric::money avg_price_per_sqft,
    to_char(avg( CASE WHEN "CloseDate" BETWEEN $1 AND $2 AND "StandardStatus" = 'Closed' AND "ClosePrice" > 0 AND "ListPrice" > 0 THEN (100 * ("ClosePrice"::float8 / "ListPrice"::float8))::float8 END), '999G999D999"%"') sold_to_list_ratio,
    sum( CASE WHEN "CloseDate" BETWEEN $1 AND $2 AND "StandardStatus" = 'Closed' AND "ClosePrice" > 0 THEN "ClosePrice" END)::float8::numeric::money total_sales
  FROM ${propertyTable} reso, ${boundaryTable} tb
  WHERE st_intersects (tb.geom, reso.geom) AND reso."PropertySubType" IN ('SingleFamilyResidence', 'Townhouse', 'Condominium')
  GROUP BY tb.properties_label;
`

  return sql
}

function getQuarterDates (params) {
  const { quarterNumber = 1, year = 2021 } = params
  const q = [
    [`${year}-01-01`, `${year}-03-31`],
    [`${year}-04-01`, `${year}-06-30`],
    [`${year}-07-01`, `${year}-09-30`],
    [`${year}-10-01`, `${year}-12-31`]
  ]
  return q[quarterNumber - 1]
}
