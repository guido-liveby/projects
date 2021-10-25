const pgConnect = require('@liveby/pg-connect')

module.exports = { withPostgres }
async function withPostgres (func) {
  const pgClient = await pgConnect()
  try {
    const response = await func(pgClient)
    return response
  } catch (e) {
    console.error(e)
    // throw e
  } finally {
    pgClient.release(usingJest())
  }
}

function usingJest () {
  return process.env.JEST_WORKER_ID !== undefined
}
