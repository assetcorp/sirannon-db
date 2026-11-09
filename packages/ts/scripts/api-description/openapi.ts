import type { HttpWireTypeName } from '../../src/server/wire-types.js'
import type { HttpRouteDescription, RouteRequestBody } from './http-routes.js'
import { type JsonSchema, type LoadedRoots, loadRoots, sortedSchemas } from './schema-from-types.js'

const OPENAPI_VERSION = '3.1.0'
const DESCRIPTION_VERSION = '1.0.0'
const DOCUMENT_TITLE = 'Sirannon HTTP API'
const DOCUMENT_DESCRIPTION =
  'The HTTP API of a Sirannon server. Set the base URL of your client to the address where the server listens. Every value that a row, a bind parameter, or a change event carries follows the tagged value encoding: an integer outside the range from -(2^53 - 1) to 2^53 - 1 travels as an IntegerEnvelope and a BLOB as a BlobEnvelope. An operation that carries x-sirannon-sql-statement sends a SQL statement, so a client reads GET /capabilities first and refuses the call with SQL_NOT_ACCEPTED when the capabilities leave out query.sql.'
const SQL_STATEMENT_EXTENSION = 'x-sirannon-sql-statement'
const SQL_STATEMENT_TAG = 'Statements'
const LICENSE = { name: 'Apache-2.0', identifier: 'Apache-2.0' }
const JSON_CONTENT_TYPE = 'application/json'
const WIRE_TYPES_FILE = 'src/server/wire-types.ts'
const WIRE_TYPES_INTERFACE = 'HttpWireTypes'
const ERROR_RESPONSE_REF = '#/components/responses/Error'
const SUCCESS_DESCRIPTION: Readonly<Record<number, string>> = {
  200: 'The server answered the request.',
  202: 'The server accepted the work and answered before it finished.',
}
const PATH_PARAMETER_DESCRIPTION: Readonly<Record<string, string>> = {
  id: 'The identifier of the database, percent-encoded, so that an identifier holding a slash reaches the database stored under it.',
  name: 'The name of the registered operation, percent-encoded.',
}

function jsonContent(name: HttpWireTypeName): Record<string, unknown> {
  return { [JSON_CONTENT_TYPE]: { schema: { $ref: `#/components/schemas/${name}` } } }
}

function requestBodyOf(request: RouteRequestBody): Record<string, unknown> | undefined {
  if (request === null) return undefined
  if (typeof request === 'string') return { required: true, content: jsonContent(request) }
  return { required: false, content: jsonContent(request.optional) }
}

export function openApiPath(path: string): string {
  return path.replace(/:(\w+)/g, '{$1}')
}

function pathParameters(path: string): JsonSchema[] {
  return [...path.matchAll(/:(\w+)/g)].flatMap(match => {
    const name = match[1]
    if (name === undefined) return []
    const description = PATH_PARAMETER_DESCRIPTION[name]
    if (description === undefined) throw new Error(`${path} takes the path parameter ${name}, which has no description`)
    return [{ name, in: 'path', required: true, description, schema: { type: 'string' } }]
  })
}

function operationOf(route: HttpRouteDescription): Record<string, unknown> {
  const parameters = pathParameters(route.path)
  const requestBody = requestBodyOf(route.request)
  const responses: Record<string, unknown> = {}
  for (const [status, body] of Object.entries(route.responses)) {
    responses[status] = {
      description: SUCCESS_DESCRIPTION[Number(status)] ?? `The server answered with status ${status}.`,
      content: jsonContent(body),
    }
  }
  responses.default = { $ref: ERROR_RESPONSE_REF }
  return {
    operationId: route.operationId,
    summary: route.summary,
    tags: [route.tag],
    ...(route.tag === SQL_STATEMENT_TAG ? { [SQL_STATEMENT_EXTENSION]: true } : {}),
    ...(parameters.length > 0 ? { parameters } : {}),
    ...(requestBody === undefined ? {} : { requestBody }),
    responses,
  }
}

export function assertRoutesDistinct(routes: readonly HttpRouteDescription[]): void {
  const operationIds = new Map<string, HttpRouteDescription>()
  const endpoints = new Set<string>()
  for (const route of routes) {
    const endpoint = `${route.method.toUpperCase()} ${route.path}`
    if (route.summary.trim().length === 0) throw new Error(`${endpoint} has an empty summary`)
    if (Object.keys(route.responses).length === 0) throw new Error(`${endpoint} describes no response`)
    if (endpoints.has(endpoint)) throw new Error(`Two route descriptions share ${endpoint}`)
    endpoints.add(endpoint)
    const earlier = operationIds.get(route.operationId)
    if (earlier !== undefined) {
      throw new Error(`${endpoint} and ${earlier.path} share the operation ID ${route.operationId}`)
    }
    operationIds.set(route.operationId, route)
  }
}

export function buildOpenApiDocument(
  routes: readonly HttpRouteDescription[],
  packageDirectory: string,
): Record<string, unknown> {
  assertRoutesDistinct(routes)
  const wire: LoadedRoots<HttpWireTypeName> = loadRoots(packageDirectory, WIRE_TYPES_FILE, WIRE_TYPES_INTERFACE)
  const used = new Set<HttpWireTypeName>()
  const paths: Record<string, Record<string, unknown>> = {}
  for (const route of routes) {
    const path = openApiPath(route.path)
    paths[path] ??= {}
    paths[path][route.method] = operationOf(route)
    if (route.request !== null) used.add(typeof route.request === 'string' ? route.request : route.request.optional)
    for (const body of Object.values(route.responses)) used.add(body)
  }
  for (const name of wire.names) {
    if (!used.has(name) && name !== 'ErrorResponse' && !name.endsWith('Envelope') && name !== 'BulkLoadDurability') {
      throw new Error(`${WIRE_TYPES_INTERFACE} holds ${name}, which no route sends or takes`)
    }
  }
  const tags = [...new Set(routes.map(route => route.tag))].map(name => ({ name }))
  return {
    openapi: OPENAPI_VERSION,
    info: { title: DOCUMENT_TITLE, version: DESCRIPTION_VERSION, description: DOCUMENT_DESCRIPTION, license: LICENSE },
    tags,
    paths,
    components: {
      schemas: sortedSchemas(wire.builder),
      responses: {
        Error: {
          description: 'The server refused the request or failed to answer it, with a code that says why.',
          content: jsonContent('ErrorResponse'),
        },
      },
    },
  }
}
