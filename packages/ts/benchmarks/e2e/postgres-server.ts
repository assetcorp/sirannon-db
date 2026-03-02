import type { us_listen_socket } from 'uWebSockets.js'
import uWS from 'uWebSockets.js'
import pg from 'pg'

interface PostgresAppOptions {
  host: string
  port: number
  poolConfig: pg.PoolConfig
}

function convertPlaceholders(sql: string): string {
  let count = 0
  return sql.replace(/\?/g, () => {
    count++
    return `$${count}`
  })
}

const MAX_BODY = 1_048_576

function readBody(res: uWS.HttpResponse): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    let aborted = false
    const chunks: Buffer[] = []
    let totalLength = 0

    res.onAborted(() => {
      aborted = true
      reject(new Error('Request aborted'))
    })

    res.onData((chunk, isLast) => {
      if (aborted) return

      totalLength += chunk.byteLength
      if (totalLength > MAX_BODY) {
        aborted = true
        res.cork(() => {
          res
            .writeStatus('413')
            .writeHeader('Content-Type', 'application/json')
            .end('{"error":{"code":"PAYLOAD_TOO_LARGE","message":"Request body exceeds size limit"}}')
        })
        reject(new Error('Payload too large'))
        return
      }

      chunks.push(Buffer.from(chunk))

      if (isLast) {
        resolve(Buffer.concat(chunks))
      }
    })
  })
}

function sendJson(res: uWS.HttpResponse, status: string, data: unknown) {
  const payload = JSON.stringify(data)
  res.cork(() => {
    res.writeStatus(status).writeHeader('Content-Type', 'application/json').end(payload)
  })
}

function sendError(res: uWS.HttpResponse, status: string, code: string, message: string) {
  sendJson(res, status, { error: { code, message } })
}

export async function createPostgresAppServer(options: PostgresAppOptions) {
  const pool = new pg.Pool(options.poolConfig)
  const app = uWS.App()
  let listenSocket: us_listen_socket | null = null

  app.get('/health', res => {
    sendJson(res, '200 OK', { status: 'ok' })
  })

  app.post('/db/:id/query', (res, req) => {
    const _dbId = req.getParameter(0)
    const bodyPromise = readBody(res)

    bodyPromise
      .then(async raw => {
        const body = JSON.parse(raw.toString('utf-8'))

        if (!body.sql || typeof body.sql !== 'string') {
          sendError(res, '400', 'INVALID_REQUEST', 'sql field is required')
          return
        }

        const result = await pool.query({
          text: convertPlaceholders(body.sql),
          values: Array.isArray(body.params) ? body.params : [],
        })
        sendJson(res, '200 OK', { rows: result.rows })
      })
      .catch(err => {
        if (String(err).includes('aborted') || String(err).includes('Payload too large')) return
        sendError(res, '500', 'QUERY_ERROR', String(err))
      })
  })

  app.post('/db/:id/execute', (res, req) => {
    const _dbId = req.getParameter(0)
    const bodyPromise = readBody(res)

    bodyPromise
      .then(async raw => {
        const body = JSON.parse(raw.toString('utf-8'))

        if (!body.sql || typeof body.sql !== 'string') {
          sendError(res, '400', 'INVALID_REQUEST', 'sql field is required')
          return
        }

        const result = await pool.query({
          text: convertPlaceholders(body.sql),
          values: Array.isArray(body.params) ? body.params : [],
        })
        sendJson(res, '200 OK', { changes: result.rowCount ?? 0, lastInsertRowId: 0 })
      })
      .catch(err => {
        if (String(err).includes('aborted') || String(err).includes('Payload too large')) return
        sendError(res, '500', 'EXECUTE_ERROR', String(err))
      })
  })

  app.post('/db/:id/transaction', (res, req) => {
    const _dbId = req.getParameter(0)
    const bodyPromise = readBody(res)

    bodyPromise
      .then(async raw => {
        const body = JSON.parse(raw.toString('utf-8'))

        if (!Array.isArray(body.statements) || body.statements.length === 0) {
          sendError(res, '400', 'INVALID_REQUEST', 'statements array is required and must not be empty')
          return
        }

        const client = await pool.connect()
        try {
          await client.query('BEGIN')
          const results = []
          for (const stmt of body.statements) {
            const result = await client.query({
              text: convertPlaceholders(stmt.sql),
              values: Array.isArray(stmt.params) ? stmt.params : [],
            })
            results.push({ changes: result.rowCount ?? 0, lastInsertRowId: 0 })
          }
          await client.query('COMMIT')
          sendJson(res, '200 OK', { results })
        } catch (err) {
          await client.query('ROLLBACK').catch(() => {})
          sendError(res, '500', 'TRANSACTION_ERROR', String(err))
        } finally {
          client.release()
        }
      })
      .catch(err => {
        if (String(err).includes('aborted') || String(err).includes('Payload too large')) return
        sendError(res, '500', 'TRANSACTION_ERROR', String(err))
      })
  })

  app.any('/*', res => {
    sendError(res, '404', 'NOT_FOUND', 'Route not found')
  })

  await new Promise<void>((resolve, reject) => {
    app.listen(options.host, options.port, socket => {
      if (socket) {
        listenSocket = socket
        resolve()
      } else {
        reject(new Error(`Failed to listen on ${options.host}:${options.port}`))
      }
    })
  })

  return {
    pool,
    close() {
      if (listenSocket) {
        uWS.us_listen_socket_close(listenSocket)
        listenSocket = null
      }
    },
  }
}
