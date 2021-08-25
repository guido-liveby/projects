require('dotenv').config()
const debug = require('debug')('names:')
const pgConnect = require('@liveby/pg-connect')

const processUrbanAreas = require('./getTracts')
const processTracts = require('./getNeighborhoods')
const writeFeatureCollection = require('./writeGeoJSON')

run()

async function run () {
  console.info('--  Script Start  --')

  console.info('Getting tracts associated with Lincoln')
  // const tractFeatures = await processUrbanAreas({ urbanAreas: ['Lincoln, NE', 'San Francisco--Oakland, CA', 'Detroit, MI'], pgConnect })
  const lincolnTractFeatures = await processUrbanAreas({ urbanAreas: ['Lincoln, NE'], pgConnect, outputPath: './tmp/lincolnTractFeatures.json' })
  // writeFeatureCollection({ features: lincolnTractFeatures, outputPath: 'lincolnTractFeatures.geojson' })

  console.info('Getting Neighborhoods associated with Lincoln')
  const lincolnNeighborhoodFeatures = await processTracts({ tractFeatures: lincolnTractFeatures, outputPath: './tmp/lincolnNeighborhod.json' })
  // writeFeatureCollection({ features: lincolnNeighborhoodFeatures, outputPath: 'lincolnNeighborhod.geojson' })

  console.info('Getting tracts associated with San Francisco')
  const sanfranTractFeatures = await processUrbanAreas({ urbanAreas: ['San Francisco--Oakland, CA'], pgConnect, outputPath: './tmp/sanfranTractFeatures.json' })
  // writeFeatureCollection({ features: sanfranTractFeatures, outputPath: 'sanfranTractFeatures.geojson' })

  console.info('Getting Neighborhoods associated with San Francisco')
  const sanfranNeighborhoodFeatures = await processTracts({ tractFeatures: sanfranTractFeatures, outputPath: './tmp/sanfranNeighborhod.json' })
  // writeFeatureCollection({ features: sanfranNeighborhoodFeatures, outputPath: 'sanfranNeighborhod.geojson' })

  console.info('Getting tracts associated with Detroit')
  const detroitTractFeatures = await processUrbanAreas({ urbanAreas: ['Detroit, MI'], pgConnect, outputPath: './tmp/detroitTractFeatures.json' })
  // writeFeatureCollection({ features: detroitTractFeatures, outputPath: 'detroitTractFeatures.geojson' })

  console.info('Getting Neighborhoods associated with Detroit')
  const detroitNeighborhoodFeatures = await processTracts({ tractFeatures: detroitTractFeatures, outputPath: './tmp/detroitNeighborhod.json' })
  // writeFeatureCollection({ features: detroitNeighborhoodFeatures, outputPath: 'detroitNeighborhod.geojson' })

  console.info('-- Script End  --')
}
