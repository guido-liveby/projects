// const debug = require('debug')('marketExport')
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
  const { boundaryTable = 'public.tmp_boundary', boundaries, sampleSize } = params

  let values = boundaries
    .map((b) => {
      const { _id, properties, geometry } = b
      return [String(_id), properties.label, geometry]
    })
    .sort(() => 0.5 - Math.random())
  if (sampleSize) {
    values = values.slice(0, sampleSize)
  }

  await withPostgres(async (pgClient) => {
    const dropTempTableQuery = getDropTempTableQuery({ boundaryTable })
    await pgClient.query(dropTempTableQuery)

    const createTempTableQuery = getCreateTempTableQuery({ boundaryTable })
    await pgClient.query(createTempTableQuery)

    const insertTempTableQuery = getInsertTempTableQuery({ boundaryTable, values })
    await pgClient.query(insertTempTableQuery)

    const updateGeomQuery = getUpdateGeomQuery({ boundaryTable })
    await pgClient.query(updateGeomQuery)
  })

  return boundaryTable
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

function buildReport (params) {

}
