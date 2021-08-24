require('dotenv').config()
const processUrbanArea = require('./get_tracts')
const debug = require('debug')('names')
const pgConnect = require('@liveby/pg-connect')

run()

async function run () {
  console.info('--  Script Start  --')

  const tracts = await processUrbanArea({
    urbanAreaName: 'Lincoln, NE',
    pgConnect
  })

  debug(tracts)

  // await getNames({
  //   urbanAreaName: 'San Francisco--Oakland, CA',
  //   outputPath: './tmp_data/sf_neighborhoods.geojson'
  // })

  // await getNames({
  //   urbanAreaName: 'Detroit, MI',
  //   outputPath: './tmp_data/detroit_neighborhoods.geojson'
  // })

  console.info('-- Script End  --')
}
