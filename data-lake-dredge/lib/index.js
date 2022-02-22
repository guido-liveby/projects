// Please follow JSDoc standards for commenting.
// https://jsdoc.app/about-getting-started.html#getting-started

// const { S3 } = require('@aws-sdk/client-s3')
// const s3 = new S3({ region: 'us-east-2' })

const fs = require('fs')
const ndjson = require('ndjson')
const { areSchemasEqual, createSchema } = require('genson-js')
const { diff } = require('json-diff')
const internal = require('stream')
const dataSource = '/home/guido/data/s3/liveby-data-lake/property-history/vendor=triad/cacher=simply-rets/year=2021/month=12/day=31'

run({ dataSource })

async function run ({ dataSource }) {
  const ndjsonFile = `${dataSource}/2021-04-15T23:53:09.432Z.json`
  const schemas = []
  let fullSchema = {}
  let first = true

  fs.createReadStream(ndjsonFile)
    .pipe(ndjson.parse())
    .on('data', function (obj) {
      if (first) {
        first = false
        fullSchema = createSchema(obj)
      } else {
        const schemaDiff = diff(fullSchema, createSchema(obj))
        const diffData = diffFinder(schemaDiff)
        for (const { type, path, newValue } of diffData) {
          const updateObject = fullSchema
          const length = path.length - 1
          for (const i in path) {
            const p = path[i]
            if (updateObject[p]) {
              console.log({ p, i })
            } else {
              console.log({ p, i, length })
              if (i !== `${length}`) {
                
              }



              }

              // console.log(JSON.stringify({ fullSchema, path, length, i, p, type, newValue }))
              // break
            }
          }
        }
        process.exit()
      }
    }).on('end', function () { console.log(schemas) })
}

function diffFinder (obj, keyPath = [], data = []) {
  for (const k in obj) {
    if (typeof obj[k] === 'object' && obj[k] !== null) {
      keyPath.push(k)
      diffFinder(obj[k], keyPath, data)
    } else {
      if (k === '__old' && obj[k] === 'null') {
        data.push({ type: 'updateNull', path: keyPath, newValue: obj.__new })
      }
      if (k === 'type' && obj[k] !== 'null') {
        data.push({ type: 'newKey', path: keyPath, newValue: obj[k] })
      }
      keyPath.pop()
    }
  }
  return data
}
// async function runAWS () {
//   const params = {
//     Bucket: 'liveby-data-lake',
//     // Prefix: 'property-history/vendor=bright/cacher=simply-rets/year=2021/month=04/day=15/',
//     Prefix: 'property-history/vendor=bright/cacher=simply-rets/year=2021/month=04/day=15/',
//     Delimiter: '/'
//   }

//   const data = await listObjects(params)
// }

// async function listObjects (args) {
//   const { Bucket, Prefix, Delimiter } = args

//   let StartAfter
//   let returnCount
//   let Contents = []
//   let CommonPrefixes = []
//   let totalCount = 0

//   do {
//     const objects = await s3.listObjectsV2({
//       Bucket,
//       StartAfter,
//       Prefix,
//       Delimiter
//     })
//     if (objects.Contents) {
//       Contents = [...Contents, ...objects.Contents]
//       returnCount = Contents.length
//       StartAfter = Contents.slice(-1)[0].Key
//       totalCount += returnCount
//     }
//     if (objects.CommonPrefixes) CommonPrefixes = [...CommonPrefixes, ...(objects.CommonPrefixes.map(p => p.Prefix))]
//     if (returnCount === 1000) {
//       console.log({ totalCount, StartAfter })
//     }
//   } while (returnCount === 1000)
//   return { Contents: Contents, CommonPrefixes }
// }
