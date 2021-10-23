require('dotenv')
const debug = require('debug')('marketExport')
const mgConnect = require('@liveby/mongodb-connect')
const pgConnect = require('@liveby/pg-connect')
const { ObjectId } = require('mongodb')
const format = require('pg-format')

module.exports = {
  getBoundaryIds,
  getBoundaries,
  createTempTable
}

async function getBoundaryIds(params) {
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

async function getBoundaries(params) {
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

async function createTempTable(params) {
  const { values, tableName = 'public.tmp_bounds' } = params

  const dropTempTableQuery = getDropTempTableQuery({ tableName })
  const createTempTableQuery = getCreateTempTableQuery({ tableName })
  const insertTempTableQuery = getInsertTempTableQuery({ tableName, values })
  let pgClient
  try {
    pgClient = await pgConnect()
    await pgClient.query(dropTempTableQuery)
    await pgClient.query(createTempTableQuery)
    await pgClient.query(insertTempTableQuery)
    const updateGeom = `update ${tableName} set geom = ST_SetSRID(ST_Multi(ST_GeomFromGeoJSON(geom_obj::text)),4326)`
    await pgClient.query(updateGeom)
  } catch (err) {
    console.error(err)
    throw err
  } finally {
    pgClient.end()
  }
}

function getCreateTempTableQuery(params) {
  const { tableName } = params
  const query = `create table IF NOT EXISTS ${tableName} (
  id serial,
  mongo_id varchar(24),
  properties_label varchar,
  geom_obj jsonb,
  geom geometry(Multipolygon, 4326)
);
CREATE INDEX ON ${tableName} USING GIST (geom);`
  return query
}
function getDropTempTableQuery(params) {
  const { tableName } = params
  const query = `drop table if exists ${tableName} cascade;`
  return query
}
function getInsertTempTableQuery(params) {
  const { tableName, values } = params
  const query = format(
    `insert into ${tableName} (mongo_id, properties_label, geom_obj) VALUES %L`,
    values
  )
  return query
}
