import type { TemplatedApp } from 'uWebSockets.js'
import type { SQLiteDriver } from '../../src/core/driver/types.js'
import { Sirannon } from '../../src/core/sirannon.js'
import type { ServerOptions } from '../../src/core/types.js'
import { SirannonServer } from '../../src/server/server.js'

export interface RegisteredRoute {
  method: string
  path: string
}

const CATCH_ALL_PATH = '/*'
const UNUSED_DRIVER = {} as SQLiteDriver

interface RouteRegistering {
  registerRoutes(app: TemplatedApp): void
}

function recordingApp(found: RegisteredRoute[]): TemplatedApp {
  const app: Record<string, unknown> = {}
  for (const method of ['get', 'post', 'put', 'patch', 'del', 'options', 'head', 'connect', 'trace', 'any', 'ws']) {
    app[method] = (path: string) => {
      found.push({ method, path })
      return app
    }
  }
  return app as unknown as TemplatedApp
}

function settingsToRecord(): ServerOptions<unknown>[] {
  const authenticate = () => ({})
  const encryptionKeys = async () => {
    throw new Error('The route recorder resolves no key')
  }
  return [
    {},
    { cors: true },
    {
      cors: true,
      acceptSql: true,
      acceptDeviceSync: true,
      acceptBackupRestore: true,
      acceptEncryptionChange: true,
      authenticate,
      encryptionKeys,
      operations: {},
      sharedOperations: {},
    },
  ]
}

export function registeredRoutes(): RegisteredRoute[] {
  const sirannon = new Sirannon({ driver: UNUSED_DRIVER })
  const found: RegisteredRoute[] = []
  for (const options of settingsToRecord()) {
    const server = new SirannonServer(sirannon, options) as unknown as RouteRegistering
    server.registerRoutes(recordingApp(found))
  }
  const unique = new Map<string, RegisteredRoute>()
  for (const route of found) {
    if (route.path === CATCH_ALL_PATH && (route.method === 'any' || route.method === 'options')) continue
    unique.set(`${route.method} ${route.path}`, route)
  }
  return [...unique.values()]
}
