// const debug = require('debug')('marketExport')
const { withPostgres } = require('./util')
const mgConnect = require('@liveby/mongodb-connect')
const { ObjectId } = require('mongodb')
const format = require('pg-format')

module.exports = {
  getBoundaryIds,
  getBoundaries,
  createTempTable
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

async function createTempTable (params) {
  const { tableName = 'public.tmp_bounds', values } = params

  await withPostgres(async (pgClient) => {
    const dropTempTableQuery = getDropTempTableQuery({ tableName })
    await pgClient.query(dropTempTableQuery)

    const createTempTableQuery = getCreateTempTableQuery({ tableName })
    await pgClient.query(createTempTableQuery)

    const insertTempTableQuery = getInsertTempTableQuery({ tableName, values })
    await pgClient.query(insertTempTableQuery)

    const updateGeomQuery = getUpdateGeomQuery({ tableName })
    await pgClient.query(updateGeomQuery)
  })

  return tableName
}

function getCreateTempTableQuery (params) {
  const { tableName } = params
  const sql = `create table IF NOT EXISTS ${tableName} (
  id serial,
  mongo_id varchar(24),
  properties_label varchar,
  geom_obj jsonb,
  geom geometry(Multipolygon, 4326)
);
CREATE INDEX ON ${tableName} USING GIST (geom);`
  return sql
}
function getDropTempTableQuery (params) {
  const { tableName } = params
  const sql = `drop table if exists ${tableName} cascade;`
  return sql
}
function getInsertTempTableQuery (params) {
  const { tableName, values } = params
  const sql = format(
    `insert into ${tableName} (mongo_id, properties_label, geom_obj) VALUES %L`,
    values
  )
  return sql
}

function getUpdateGeomQuery (params) {
  const { tableName } = params
  const sql = `update ${tableName} set geom = ST_SetSRID(ST_Multi(ST_GeomFromGeoJSON(geom_obj::text)),4326)`
  return sql
}
