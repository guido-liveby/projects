const fs = require('fs')
const { features } = require('process')

module.exports = function writeFeatureCollection (params) {
  const { features, outputPath } = params
  const data = JSON.stringify({ type: 'FeatureCollection', features }, null, 2)
  fs.writeFileSync(outputPath, data)
}
