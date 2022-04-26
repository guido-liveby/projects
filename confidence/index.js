const { MongoClient } = require('mongodb')
const fs = require('fs')
const ws = fs.createWriteStream('agg.csv')
const fastcsv = require('fast-csv')
const debug = require('debug')('getData')

const url = "mongodb+srv://philcollins:Q0AzLMGqUr6SiX6Q@clustercluck-mmdtz.mongodb.net/LiveBy"
const client = new MongoClient(url)

const agg = [
  { $match: { 'properties.demographics.confidence': { $exists: true } } },
  { $project: { 'confidence': '$properties.demographics.confidence' } }
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

main()
  .then(console.log)
  .catch(console.error)
  .finally(() => client.close())
