require('dotenv')
const fixtures = require('./fixture/index.json')
const { withPostgres } = require('../src/util')
const { getBoundaryIds, getBoundaries, createBoundaryTable } = require('../src')
const { matchers } = require('jest-json-schema')
expect.extend(matchers)
let testParams
beforeAll(() => {
  testParams = {
    reportParams: {
      collection: 'productboundaries',
      find: {
        clientid: 'atproperties',
        boundaryType: 'county'
      }
    },
    sampleSize: 5,
    boundaryTable: 'public.tmp_boundary_test'
  }
})

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

  it.only('creates and inserts into tmp table', async () => {
    expect.hasAssertions()
    const { boundaries } = fixtures
    const { sampleSize, boundaryTable } = testParams

    await withPostgres(async (pgClient) => {
      await pgClient.query(`drop table if exists ${boundaryTable} cascade`)
    })

    const tableName = await createBoundaryTable({ boundaryTable, boundaries, sampleSize })

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

describe('run reports', () => {
  beforeAll(async () => {
    const { reportParams, sampleSize } = testParams
    const boundaryIds = await getBoundaryIds(reportParams)
    const boundaries = await getBoundaries({ boundaryIds })
    const boundaryTable = await createBoundaryTable({ values })
    console.log(boundaryTable)
  }, 60000)
  it('creates a report', async () => {
    expect.hasAssertions()
  })
})
