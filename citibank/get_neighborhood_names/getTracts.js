const debug = require('debug')('names:getTracts')
const fs = require('fs')
let client

module.exports = async function processUrbanAreas (params) {
  let output = []
  let writer

  try {
    const { urbanAreas, pgConnect, outputPath } = params
    client = await pgConnect()
    const query = fs.readFileSync('./get_tracts_from_urban_areas.sql', 'utf8')
    writer = fs.createWriteStream(outputPath)
    writer.write('{\n"type": "FeatureCollection",\n"features": [')

    for (const place of urbanAreas) {
      const { rows } = await client.query({
        name: 'getTracts',
        text: query,
        values: [place]
      })
      output = output.concat(
        rows.map((m) => {
          return {
            type: 'Feature',
            properties: {
              place,
              geoid: m.geoid
            },
            geometry: JSON.parse(m.geom)
          }
        })
      )
    }
  } catch (error) {
    console.error(error)
  } finally {
    client.release()
  }

  output.forEach(e => { writer.write(`\n${JSON.stringify(e)},`) })
  writer.write(']}')
  return output
}
