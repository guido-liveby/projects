const fs = require('fs')
const PromisePool = require('@supercharge/promise-pool')
const debug = require('debug')('names:get_tracts')

module.exports = processUrbanArea

async function processUrbanArea (params) {
  const { urbanAreaName, pgConnect } = params

  console.log(`Overlap tracts centroids in urban area "${urbanAreaName}"`)

  const urbanQuery = fs.readFileSync('./get_tracts_from_urban_areas.sql', 'utf8')
  const client = await pgConnect()

  let tracts
  try {
    tracts = await getTracts({ query: urbanQuery, searchString: urbanAreaName, client })
  } catch (error) {
    console.error(error)
  } finally {
    client.release()
  }

  return tracts
}

async function getTracts (params) {
  const { query, searchString, client } = params

  const { rows } = await client.query({
    name: 'getTracts',
    text: query,
    values: [searchString]
  })

  return rows.map(
    ({ geoid, geom }) => {
      geom = JSON.parse(geom)
      return { geoid, geom, urban_area: searchString }
    })
}
