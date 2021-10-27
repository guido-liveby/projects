require('dotenv')
// const debug = require('debug')('marketExport:test')
const fixtures = require('./fixture/index.json')
const { withPostgres } = require('../src/util')
const {
  getBoundaryIds,
  getBoundaries,
  createBoundaryTable,
  buildReport
} = require('../src')
const { matchers } = require('jest-json-schema')
expect.extend(matchers)
const testParams = {
  reportParams: {
    collection: 'productboundaries',
    find: {
      clientid: 'atproperties',
      boundaryType: 'county'
    },
    quarters: [
      { quarterNumber: 1, year: 2021 },
      { quarterNumber: 2, year: 2021 },
      { quarterNumber: 3, year: 2021 }
    ]
  },
  sampleSize: 3,
  boundaryTable: 'public.tmp_boundary_test'
}

describe('setup for reports', () => {
  afterAll(async () => {
    const { boundaryTable } = testParams
    await withPostgres(async (pgClient) => {
      const query = `drop table if exists ${boundaryTable} cascade`
      await pgClient.query(query)
    })
  })

  it('returns boundary ids in an array', async () => {
    expect.hasAssertions()
    const { reportParams } = testParams

    const data = await getBoundaryIds(reportParams)

    expect(Array.isArray(data)).toBeTruthy()
    expect(data).not.toHaveLength(0)

    data.forEach((d) => expect(d).toContainEqual(expect.any(String)))
    data.forEach((d) => expect(d).toHaveLength(24))
  })

  it('returns boundaries in an array', async () => {
    expect.hasAssertions()

    const { boundaryIds } = fixtures

    const data = await getBoundaries({ boundaryIds })

    expect(Array.isArray(data)).toBeTruthy()
    expect(data.length).toBeGreaterThan(0)
  }, 10000)

  it('creates and inserts into tmp table', async () => {
    expect.hasAssertions()
    const { boundaries } = fixtures
    const { sampleSize, boundaryTable } = testParams

    await withPostgres(async (pgClient) => {
      await pgClient.query(`drop table if exists ${boundaryTable} cascade`)
    })

    const tableName = await createBoundaryTable({
      boundaryTable,
      boundaries,
      sampleSize
    })

    const data = await withPostgres(async (pgClient) => {
      const query = `select * from ${tableName}`
      const { rows } = await pgClient.query(query)
      return rows
    })

    expect(Array.isArray(data)).toBeTruthy()
    expect(data).toHaveLength(sampleSize)
    expect(data).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          properties_label: expect.any(String),
          geom: expect.any(String),
          mongo_id: expect.any(String)
        })
      ])
    )
  }, 10000)
})

describe.only('run reports', () => {
  let { reportParams, sampleSize } = testParams

  beforeAll(async () => {
    reportParams = { ...reportParams, sampleSize }
  }, 60000)

  it('creates a report', async () => {
    expect.hasAssertions()
    const reportTable = await buildReport(reportParams)

    const data = await withPostgres(async (pgClient) => {
      const query = `select * from ${reportTable}`
      const { rows } = await pgClient.query(query)
      return rows
    })

    expect(Array.isArray(data)).toBeTruthy()
    expect(data.length).toBeGreaterThan(0)
    console.log(data)
  }, 30 * 60000)
})
