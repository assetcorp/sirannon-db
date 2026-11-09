import type { WebSocketWireTypeName } from '../../src/server/wire-types.js'
import type { WSClientMessage } from '../../src/server/ws-protocol.js'
import type { WSServerMessage } from '../../src/server/ws-server-messages.js'

export type ClientMessageType = WSClientMessage['type']
export type ServerMessageType = WSServerMessage['type']

export interface ClientMessageDescription {
  schema: WebSocketWireTypeName
  summary: string
  replies: readonly ServerMessageType[]
}

export interface ServerMessageDescription {
  schema: WebSocketWireTypeName
  summary: string
}

export const CLIENT_MESSAGES: Readonly<Record<ClientMessageType, ClientMessageDescription>> = {
  subscribe: {
    schema: 'WSSubscribeMessage',
    summary:
      'Opens a change subscription on a table, resuming after sinceSeq when the server retains that history, or a live query on the registered read that name gives.',
    replies: ['subscribed', 'error'],
  },
  unsubscribe: {
    schema: 'WSUnsubscribeMessage',
    summary: 'Ends the subscription or the live query that id gives.',
    replies: ['unsubscribed', 'error'],
  },
  ack: {
    schema: 'WSAckMessage',
    summary: 'Records that a device has stored every change up to seq, on a server that accepts device sync.',
    replies: ['result', 'error'],
  },
  query: {
    schema: 'WSQueryMessage',
    summary: 'Runs a read statement, on a server that accepts SQL, or the registered read that name gives.',
    replies: ['result', 'error'],
  },
  execute: {
    schema: 'WSExecuteMessage',
    summary: 'Runs a write statement, on a server that accepts SQL, or the registered write that name gives.',
    replies: ['result', 'error'],
  },
  transaction: {
    schema: 'WSTransactionMessage',
    summary: 'Runs every statement in one transaction, on a server that accepts SQL.',
    replies: ['result', 'error'],
  },
  batch: {
    schema: 'WSBatchMessage',
    summary: 'Runs one statement once for each parameter set, in one transaction, on a server that accepts SQL.',
    replies: ['result', 'error'],
  },
  load: {
    schema: 'WSLoadMessage',
    summary: 'Loads rows while the writer runs at relaxed durability, on a server that accepts SQL.',
    replies: ['result', 'error'],
  },
}

export const SERVER_MESSAGES: Readonly<Record<ServerMessageType, ServerMessageDescription>> = {
  subscribed: {
    schema: 'WSSubscribedMessage',
    summary: 'Confirms a subscription, with the sequence it is live from, or the first rows of a live query.',
  },
  unsubscribed: { schema: 'WSUnsubscribedMessage', summary: 'Confirms that a subscription has ended.' },
  change: { schema: 'WSChangeMessage', summary: 'Delivers one change to a subscribed table.' },
  changes: {
    schema: 'WSChangesMessage',
    summary: 'Delivers several changes in ascending seq order, on a device subscription that asks for stagedStream.',
  },
  live: {
    schema: 'WSLiveMessage',
    summary:
      'Moves a live query to its new result with operations to apply in order, replaces its rows, or states that the server is reading it again.',
  },
  result: { schema: 'WSResultMessage', summary: 'Answers a read, a write, a transaction, a batch, a load, or an ack.' },
  error: {
    schema: 'WSErrorMessage',
    summary: 'Reports that a request failed, with the code that says why, or that the connection cannot open.',
  },
}
