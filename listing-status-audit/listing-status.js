/*
 * Requires the MongoDB Node.js Driver
 * https://mongodb.github.io/node-mongodb-native
 */

const { MongoClient } = require('mongodb')
const fs = require('fs')
const ws = fs.createWriteStream('agg.csv')
const fastcsv = require('fast-csv')

const agg = [
  {
    $match: {
      vendor: {
        $in: [
          'recolorado', 'njmls', 'bright', 'fmls', 'ccimls', 'sjsr', 'gamls', 'ntreis',
          'risw', 'sira', 'cmcmls', 'gsmls', 'lvar', 'momls', 'nne', 'pin', 'armls',
          'cren', 'lou', 'pmar', 'cin', 'las', 'nky', 'sacm', 'hkar', 'mreis', 'nne',
          'recolorado', 'ires', 'bright', 'nne', 'mreis', 'pin', 'triad', 'momls',
          'nmar', 'swmt', 'las'
        ]
      },
      'mls.status': {
        $in: [
          'Active', 'ActiveUnderContract', 'Pending'
        ]
      }
    }
  }, {
    $group: {
      _id: {
        vendor: '$vendor',
        status_simplyrets: '$mls.status',
        status_mls: '$mls.statusText',
        count: {
          $sum: 1
        }
      }
    }
  }
  // , {
  //   $sort: {
  //     count: -1,
  //     vendor: 1,
  //     status_simplyrets: 1,
  //     status_mls: 1
  //   }
  // }
]
const url = "mongodb+srv://philcollins:Q0AzLMGqUr6SiX6Q@clustercluck-mmdtz.mongodb.net/LiveBy'?authSource=admin&replicaSet=ClusterCluck-shard-0&readPreference=primary&appname=MongoDB%20Compass&ssl=true"
const client = new MongoClient(url)

async function main () {
  // Use connect method to connect to the server
  await client.connect()
  console.log('Connected successfully to server')
  const collection = client.db('LiveBy').collection('properties')
  const aggResult = await collection.aggregate(agg).toArray()

  fastcsv
    .writeToStream(aggResult, { headers: true })
    .on('finish', function () {
      console.log('Write to agg.csv successfully!')
    })
    .pipe(ws)

  return 'done.'
}

main()
  .then(console.log)
  .catch(console.error)
  .finally(() => client.close())
