import type { WSClientMessage } from './ws-protocol.js'

type ClientMessageType = WSClientMessage['type']

const ACCEPTED_CLIENT_MESSAGE_TYPES: Readonly<Record<ClientMessageType, true>> = {
  subscribe: true,
  unsubscribe: true,
  ack: true,
  query: true,
  execute: true,
  transaction: true,
  batch: true,
  load: true,
}

/**
 * The `type` of every message that the server accepts over the WebSocket, which is every member of
 * {@link WSClientMessage}.
 *
 * @internal
 */
export const WS_CLIENT_MESSAGE_TYPES: readonly ClientMessageType[] = Object.freeze(
  Object.keys(ACCEPTED_CLIENT_MESSAGE_TYPES) as ClientMessageType[],
)

/**
 * The message types that hold a SQL statement unless they give the name of a registered operation. A server that
 * accepts no SQL refuses a message of these types that holds a statement.
 *
 * @internal
 */
export const SQL_MESSAGE_TYPES: ReadonlySet<string> = new Set(['query', 'execute', 'transaction', 'batch', 'load'])

/**
 * The message types that can give the name of a registered operation in place of a statement or a table.
 *
 * @internal
 */
export const NAMED_MESSAGE_TYPES: ReadonlySet<string> = new Set(['query', 'execute', 'subscribe'])

/**
 * Returns whether the server accepts a message of this `type`.
 *
 * @param type - The `type` field of a client message.
 * @returns `true` when the type matches a member of {@link WSClientMessage}.
 * @internal
 */
export function isClientMessageType(type: string): type is ClientMessageType {
  return Object.hasOwn(ACCEPTED_CLIENT_MESSAGE_TYPES, type)
}
