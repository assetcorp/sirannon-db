import { existsSync, readFileSync, writeFileSync } from 'node:fs'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { WS_CLIENT_MESSAGE_TYPES } from '../../src/server/ws-message-types.js'
import { buildAsyncApiDocument, describedClientMessageTypes, WEBSOCKET_PATH } from './asyncapi.js'
import { HTTP_ROUTES } from './http-routes.js'
import { buildOpenApiDocument, openApiPath } from './openapi.js'
import { type RegisteredRoute, registeredRoutes } from './registered-routes.js'

const PACKAGE_DIRECTORY = resolve(dirname(fileURLToPath(import.meta.url)), '../..')
const OPENAPI_PATH = join(PACKAGE_DIRECTORY, 'openapi.json')
const ASYNCAPI_PATH = join(PACKAGE_DIRECTORY, 'asyncapi.json')
const WRITE_HINT = 'Run "pnpm nx run sirannon-ts:api-description:write" to regenerate it.'

function routeKey(method: string, path: string): string {
  return `${method.toUpperCase()} ${path}`
}

function assertEveryRouteDescribed(registered: readonly RegisteredRoute[], openApi: Record<string, unknown>): void {
  const paths = (openApi.paths ?? {}) as Record<string, Record<string, unknown>>
  const httpRoutes = registered.filter(route => route.method !== 'ws')
  const registeredKeys = new Set(httpRoutes.map(route => routeKey(route.method, route.path)))
  for (const route of httpRoutes) {
    if (paths[openApiPath(route.path)]?.[route.method] === undefined) {
      throw new Error(`The server registers ${routeKey(route.method, route.path)}, which the OpenAPI file leaves out`)
    }
  }
  for (const route of HTTP_ROUTES) {
    if (!registeredKeys.has(routeKey(route.method, route.path))) {
      throw new Error(
        `The OpenAPI file describes ${routeKey(route.method, route.path)}, which the server never registers`,
      )
    }
  }
}

function assertEveryMessageDescribed(registered: readonly RegisteredRoute[], asyncApi: Record<string, unknown>): void {
  const sockets = registered.filter(route => route.method === 'ws')
  for (const socket of sockets) {
    if (socket.path !== WEBSOCKET_PATH) {
      throw new Error(`The server accepts WebSocket connections at ${socket.path}, which the AsyncAPI file leaves out`)
    }
  }
  if (sockets.length === 0)
    throw new Error(`The AsyncAPI file describes ${WEBSOCKET_PATH}, which the server never opens`)
  const components = (asyncApi.components ?? {}) as { messages?: Record<string, unknown> }
  for (const type of WS_CLIENT_MESSAGE_TYPES) {
    if (components.messages?.[type] === undefined) {
      throw new Error(`The server accepts the '${type}' message, which the AsyncAPI file leaves out`)
    }
  }
  const accepted = new Set<string>(WS_CLIENT_MESSAGE_TYPES)
  for (const type of describedClientMessageTypes()) {
    if (!accepted.has(type)) throw new Error(`The AsyncAPI file describes '${type}', which the server never accepts`)
  }
}

function serialised(document: Record<string, unknown>): string {
  return `${JSON.stringify(document, null, 2)}\n`
}

function compare(path: string, generated: string): boolean {
  const committed = existsSync(path) ? readFileSync(path, 'utf-8') : ''
  if (committed === generated) return true
  console.error(`${path} differs from a fresh generation. ${WRITE_HINT}`)
  return false
}

function run(write: boolean): number {
  const registered = registeredRoutes()
  const openApi = buildOpenApiDocument(HTTP_ROUTES, PACKAGE_DIRECTORY)
  const asyncApi = buildAsyncApiDocument(PACKAGE_DIRECTORY)
  assertEveryRouteDescribed(registered, openApi)
  assertEveryMessageDescribed(registered, asyncApi)
  const files: Array<[string, string]> = [
    [OPENAPI_PATH, serialised(openApi)],
    [ASYNCAPI_PATH, serialised(asyncApi)],
  ]
  if (write) {
    for (const [path, text] of files) writeFileSync(path, text)
    console.log(`Wrote ${OPENAPI_PATH} with ${HTTP_ROUTES.length} operations and ${ASYNCAPI_PATH}`)
    return 0
  }
  const matching = files.map(([path, text]) => compare(path, text))
  if (matching.includes(false)) return 1
  console.log(`openapi.json and asyncapi.json match a fresh generation of ${HTTP_ROUTES.length} operations`)
  return 0
}

process.exitCode = run(process.argv.includes('--write'))
