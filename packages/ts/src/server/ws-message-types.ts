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
 * The message types that carry a SQL statement unless they name a registered operation, which a server that
 * accepts no SQL refuses.
 *
 * @internal
 */
export const SQL_MESSAGE_TYPES: ReadonlySet<string> = new Set(['query', 'execute', 'transaction', 'batch', 'load'])

/**
 * The message types that may name a registered operation in place of a statement or a table.
 *
 * @internal
 */
export const NAMED_MESSAGE_TYPES: ReadonlySet<string> = new Set(['query', 'execute', 'subscribe'])

/**
 * Returns whether the server accepts a message of this `type`.
 *
 * @param type - The `type` that a client message carries.
 * @returns `true` when the type names a member of {@link WSClientMessage}.
 * @internal
 */
export function isClientMessageType(type: string): type is ClientMessageType {
  return Object.hasOwn(ACCEPTED_CLIENT_MESSAGE_TYPES, type)
}
