const { MongoClient } = require('mongodb')
const fs = require('fs')
const ws = fs.createWriteStream('demographics.csv')
const fastcsv = require('fast-csv')
const debug = require('debug')('getData')

const url = 'mongodb+srv://philcollins:Q0AzLMGqUr6SiX6Q@clustercluck-mmdtz.mongodb.net/LiveBy'
const client = new MongoClient(url)

const agg = [
  {
    $match: {
      'properties.demographics.confidence': { $exists: true },
      'properties.banned': null,
      'properties.archived': null
    }
  },
  {
    $project:
    {
      'community_type': '$properties.community_type',
      'area_m': '$properties.area_m',
      'confidence': '$properties.demographics.confidence',
      'population': '$properties.demographics.population',
      'median_age': '$properties.demographics.median_age',
      'median_household_income': '$properties.demographics.median_household_income',
      'total_educated': '$properties.demographics.total_educated',
      'unemployment_rate': '$properties.demographics.unemployment_rate',
      'country': '$properties.address.country'
    }
  }
]

async function main() {
  debug('Connect to server')
  await client.connect()

  debug('Get data from server')
  const collection = client.db('LiveBy').collection('neighborhoods')
  const aggResult = await collection.aggregate(agg).toArray()

  debug('write csv from data')
  fastcsv
    .writeToStream(ws, aggResult, { headers: true })
    .on('finish', function () { debug('done') })

  return 'done.'
}

main().then(console.log).catch(console.error).finally(() => client.close())
