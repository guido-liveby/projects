require('dotenv')
const debug = require('debug')('marketExport:test')
const { withPostgres } = require('../src/util')
const { getBoundaryIds, getBoundaries, createTempTable } = require('../src')

describe('get reports', () => {
  it('returns boundary ids in an array', async () => {
    expect.hasAssertions()

    const data = await getBoundaryIds({
      collection: 'productboundaries',
      find: { clientid: 'atproperties', boundaryType: 'county' }
    })

    expect(Array.isArray(data)).toBeTruthy()
    expect(data).not.toHaveLength(0)

    data.forEach((d) => expect(d).toContainEqual(expect.any(String)))
    data.forEach((d) => expect(d).toHaveLength(24))
  })

  it('returns boundaries in an array', async () => {
    expect.hasAssertions()

    const boundaryIds = await getBoundaryIds({
      collection: 'productboundaries',
      find: { clientid: 'atproperties', boundaryType: 'county' }
    })

    const data = await getBoundaries({ boundaryIds })
    expect(Array.isArray(data)).toBeTruthy()
    expect(data).not.toHaveLength(0)
  }, 10000)

  it.only('creates and inserts into tmp table', async () => {
    expect.hasAssertions()

    const boundaryIds = await getBoundaryIds({
      collection: 'productboundaries',
      find: { clientid: 'atproperties', boundaryType: 'county' }
    })
    const boundaries = await getBoundaries({ boundaryIds })

    const values = boundaries
      .map((b) => {
        const { _id, properties, geometry } = b
        return [String(_id), properties.label, geometry]
      })
      .sort(() => 0.5 - Math.random())
      .slice(0, 5)
    const tableName = await createTempTable({ values })

    await withPostgres(async (pgClient) => {
      const tableData = await pgClient.query(
        `select properties_label , ST_AsGeoJSON(geom) geom from ${tableName}`
      )
      debug(
        tableData.rows.map((i) => ({
          properties_label: i.properties_label,
          geom: JSON.parse(i.geom)
        }))
      )
    })
  }, 60000)
})
