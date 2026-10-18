import net from 'node:net'
import { SirannonError } from '../../../core/errors.js'
import type { Sirannon } from '../../../core/sirannon.js'
import type { ServerOptions } from '../../../core/types.js'
import { createServer, type SirannonServer } from '../../../server/server.js'

const LISTEN_ATTEMPTS = 5
const LOOPBACK_HOST = '127.0.0.1'

function unusedLoopbackPort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const probe = net.createServer()
    probe.once('error', reject)
    probe.listen(0, LOOPBACK_HOST, () => {
      const address = probe.address()
      probe.close(closeErr => {
        if (closeErr) reject(closeErr)
        else if (typeof address === 'object' && address !== null) resolve(address.port)
        else reject(new Error('The loopback probe reported no port'))
      })
    })
  })
}

export async function listenOnLoopback<I>(sirannon: Sirannon, options: ServerOptions<I>): Promise<SirannonServer<I>> {
  if (options.port !== undefined && options.port !== 0) {
    const server = createServer<I>(sirannon, options)
    await server.listen()
    return server
  }
  let lastError: unknown = new Error(`No server listened on ${LOOPBACK_HOST} after ${LISTEN_ATTEMPTS} attempts`)
  for (let attempt = 0; attempt < LISTEN_ATTEMPTS; attempt++) {
    const server = createServer<I>(sirannon, { ...options, port: await unusedLoopbackPort() })
    try {
      await server.listen()
      return server
    } catch (err) {
      await server.close().catch(() => {})
      if (err instanceof SirannonError) throw err
      lastError = err
    }
  }
  throw lastError
}
