import type { WebSocketWireTypeName } from '../../src/server/wire-types.js'
import { type JsonSchema, loadRoots, sortedSchemas } from './schema-from-types.js'
import {
  CLIENT_MESSAGES,
  type ClientMessageDescription,
  SERVER_MESSAGES,
  type ServerMessageDescription,
} from './websocket-messages.js'

const ASYNCAPI_VERSION = '3.0.0'
const DESCRIPTION_VERSION = '1.0.0'
const DOCUMENT_TITLE = 'Sirannon WebSocket API'
const DOCUMENT_DESCRIPTION =
  'The WebSocket protocol of a Sirannon server. A client connects to /db/{id}, offering the subprotocol sirannon.v1 when it offers any, and sends JSON text frames. Every client message carries a string id that the server repeats in its reply. Every value that a row, a bind parameter, a filter, or a change event carries follows the tagged value encoding: an integer outside the range from -(2^53 - 1) to 2^53 - 1 travels as an IntegerEnvelope and a BLOB as a BlobEnvelope. The server closes the connection with code 4401 or 4403 when the authenticate hook refuses the caller, and a client must not reconnect after either code.'
const LICENSE = { name: 'Apache-2.0', identifier: 'Apache-2.0' }
const WIRE_TYPES_FILE = 'src/server/wire-types.ts'
const WIRE_TYPES_INTERFACE = 'WebSocketWireTypes'
const CHANNEL_NAME = 'database'
const SUBPROTOCOL = 'sirannon.v1'

export const WEBSOCKET_PATH = '/db/:id'

function channelRef(): JsonSchema {
  return { $ref: `#/channels/${CHANNEL_NAME}` }
}

function channelMessageRef(type: string): JsonSchema {
  return { $ref: `#/channels/${CHANNEL_NAME}/messages/${type}` }
}

function capitalised(type: string): string {
  return type.charAt(0).toUpperCase() + type.slice(1)
}

function assertMessageSchema(
  schemas: Record<string, JsonSchema>,
  type: string,
  description: ClientMessageDescription | ServerMessageDescription,
): void {
  const schema = schemas[description.schema]
  const properties = schema?.properties as Record<string, JsonSchema> | undefined
  if (properties?.type?.const !== type || properties.id === undefined) {
    throw new Error(`${description.schema} must carry the type '${type}' and an id to describe that message`)
  }
}

export function buildAsyncApiDocument(packageDirectory: string): Record<string, unknown> {
  const wire = loadRoots<WebSocketWireTypeName>(packageDirectory, WIRE_TYPES_FILE, WIRE_TYPES_INTERFACE)
  const schemas = sortedSchemas(wire.builder)
  const described = [...Object.entries(CLIENT_MESSAGES), ...Object.entries(SERVER_MESSAGES)]
  const types = new Set<string>()
  const messages: Record<string, unknown> = {}
  for (const [type, description] of described) {
    if (types.has(type)) throw new Error(`A client message and a server message share the type '${type}'`)
    types.add(type)
    assertMessageSchema(schemas, type, description)
    messages[type] = {
      name: type,
      title: description.schema,
      summary: description.summary,
      contentType: 'application/json',
      payload: { $ref: `#/components/schemas/${description.schema}` },
    }
  }
  const usedSchemas = new Set(described.map(([, description]) => description.schema))
  for (const name of wire.names) {
    if (!usedSchemas.has(name) && !name.endsWith('Envelope')) {
      throw new Error(`${WIRE_TYPES_INTERFACE} holds ${name}, which no message carries`)
    }
  }
  const operations: Record<string, unknown> = {}
  for (const [type, description] of Object.entries(CLIENT_MESSAGES)) {
    operations[`send${capitalised(type)}`] = {
      action: 'send',
      channel: channelRef(),
      summary: description.summary,
      messages: [channelMessageRef(type)],
      reply: { channel: channelRef(), messages: description.replies.map(channelMessageRef) },
    }
  }
  for (const [type, description] of Object.entries(SERVER_MESSAGES)) {
    operations[`receive${capitalised(type)}`] = {
      action: 'receive',
      channel: channelRef(),
      summary: description.summary,
      messages: [channelMessageRef(type)],
    }
  }
  return {
    asyncapi: ASYNCAPI_VERSION,
    info: { title: DOCUMENT_TITLE, version: DESCRIPTION_VERSION, description: DOCUMENT_DESCRIPTION, license: LICENSE },
    defaultContentType: 'application/json',
    channels: {
      [CHANNEL_NAME]: {
        address: WEBSOCKET_PATH.replace(/:(\w+)/g, '{$1}'),
        title: 'One database',
        description: `Queries, writes, change subscriptions, and live queries on one database. Offer the subprotocol ${SUBPROTOCOL} ahead of any other subprotocol.`,
        parameters: {
          id: { description: 'The identifier of the database, percent-encoded.' },
        },
        messages: Object.fromEntries(
          [...types].map(type => [type, { $ref: `#/components/messages/${type}` }] as const),
        ),
        bindings: { ws: { bindingVersion: '0.1.0' } },
      },
    },
    operations,
    components: { schemas, messages },
  }
}

export function describedClientMessageTypes(): string[] {
  return Object.keys(CLIENT_MESSAGES)
}
