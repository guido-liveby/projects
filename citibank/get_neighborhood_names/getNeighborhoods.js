const debug = require('debug')('names:getNeighborhoods')
const connect = require('@liveby/mongodb-connect')
const PromisePool = require('@supercharge/promise-pool')
const fs = require('fs')

module.exports = async function processTracts (params) {
  const { tractFeatures, outputPath } = params
  const connection = await connect()

  await getNeighborhoodsFromTracts({ tractFeatures, connection, outputPath })
}

async function getNeighborhoodsFromTracts (params) {
  const { tractFeatures, connection, outputPath } = params
  const writer = fs.createWriteStream(outputPath)
  writer.write('{\n"type": "FeatureCollection",\n"features": [')

  const communityTypeFilter = [
    'neighborhood',
    'compositeCommunity',
    'community',
    'microNeighborhood'
  ]
  const features = []

  const results = await PromisePool
    .for(tractFeatures)
    .process(async (tract) => {
      const { type, properties, geometry } = tract
      const { place, geoid } = properties

      const neighborhoods = await connection
        .db('LiveBy')
        .collection('neighborhoods')
        .find({ geometry: { $geoIntersects: { $geometry: geometry } } })
        .toArray()

      for (const neighborhood of neighborhoods) {
        const { properties: nproperties, geometry: ngeometry } = neighborhood
        const { label, community_type: communityType, archive, pop10 } = nproperties

        if (archive || !(communityTypeFilter.includes(communityType))) { continue }
        features.push({
          type,
          properties: { place, geoid, label, communityType, archive, pop10 },
          geometry: ngeometry
        })
      }

      console.info(`\tProcessed neighborhoods for tract ${geoid}`)
      features.forEach(feature => {
        debug(feature)
        writer.write(`\n${JSON.stringify(feature)},`)
      })

      await connection.close()
    }
    )
  writer.write(']}')
}
