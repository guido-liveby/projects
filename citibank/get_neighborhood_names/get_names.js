const fs = require('fs')
const pgConnect = require('@liveby/pg-connect')
const connect = require('@liveby/mongodb-connect')
const PromisePool = require('@supercharge/promise-pool')
const turf = require('@turf/turf')
const debug = require('debug')('names:get_names')

module.exports = processUrbanArea

async function processUrbanArea (params) {
  const { urbanAreaName, outputPath } = params
  console.log(`Overlap tracts centroids in urban area "${urbanAreaName}"`)
  const urbanQuery = fs.readFileSync('./get_tract.sql', 'utf8')
  const tracts = await getTracts(urbanQuery, urbanAreaName)

  console.log('Get neighborhoods from mongodb that intersect tracts')

  try {
    const connection = await connect()
    const results = await processTracts([tracts[0]], connection)
    debug({ debug_results: results })
  } catch (error) {
    console.error(error)
  } finally {

  }

  // console.log(`write json output: ${outputPath}`)
  // fs.writeFileSync(
  //   outputPath,
  //   JSON.stringify({ type: 'FeatureCollection', features: results }, null, 2),
  //   'utf8'
  // )
}

async function processTracts (tracts, connection) {
  const communityTypeFilter = [
    'neighborhood',
    'compositeCommunity',
    'community',
    'microNeighborhood'
  ]

  const results = await PromisePool.for(tracts).process(async (tract) => {
    const { geoid, geom } = tract
    const tractGeom = turf.toMercator(turf.flatten(geom))
    const tractArea = turf.area(tractGeom)

    const neighborhoods = await connection
      .db('LiveBy')
      .collection('neighborhoods')
      .find({ geometry: { $geoIntersects: { $geometry: geom } } })
      .toArray()

    const outputNeighborhoods = neighborhoods
      .map((m) => {
        const neighborhoodGeom = turf.toMercator(turf.flatten(m.geometry))
        const neighborhoodArea = turf.area(neighborhoodGeom)
        const intersectGeom = turf.intersect(tractGeom, neighborhoodGeom)
        const intersectArea = turf.area(intersectGeom)
        const tract = geoid
        const label = m.properties.label
        const type = m.properties.community_type
        const area = { intersect: intersectArea, neighborhood: neighborhoodArea, percent: intersectArea / tractArea, tract: tractArea }
        const output = {
          tract,
          label,
          type,
          area,
          area_str: JSON.stringify(area),
          archive: m.properties.archive
        }
        debug({ output })
        return output
      })
      .filter((f) => (!!(f.label) && communityTypeFilter.includes(f.type) && !(f.archive)))
      .sort((a, b) => { return a.area.percent > b.area.percent ? -1 : b.area.percent > a.area.percent ? 1 : 0 })
    // .reduce(
    //   (acc, cur) => {
    //     acc.data.push(cur)
    //     debug({ cur })
    //     return acc
    //   },
    //   { data: [], geoid }
    // )

    console.info(`\tProcessed neighborhoods for tract ${tract.geoid}`)

    debug({ debug_outputNeighborhoods: outputNeighborhoods })
    debug({ length: outputNeighborhoods.length })

    return {
      type: 'Feature',
      geometry: geom,
      properties: outputNeighborhoods
    }
  })
  await connection.close()

  // debug({ results: JSON.stringify(results) })

  return results.results
}

async function getTracts (query, searchString) {
  let data
  const client = await pgConnect()

  try {
    const { rows } = await client.query({
      name: 'getTracts',
      text: query,
      values: [searchString]
    })
    data = rows.map(({ geoid, geom }) => {
      geom = JSON.parse(geom)
      return { geoid, geom }
    })
  } catch (error) {
    console.error(error)
    throw error
  } finally {
    client.release()
  }

  return data
}
