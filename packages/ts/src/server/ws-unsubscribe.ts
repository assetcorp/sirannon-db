import type { WSConnection } from './ws-connection.js'
import type { ConnectionState } from './ws-connection-state.js'
import type { WSUnsubscribedMessage } from './ws-server-messages.js'
import type { WSSubscribeDeps } from './ws-subscribe.js'

/**
 * Ends one subscription or live query of a connection and confirms it with an `unsubscribed` message, or answers
 * `SUBSCRIPTION_NOT_FOUND` when the connection holds none under that id.
 *
 * @param deps - The senders and the change feed registry that the handler shares with its subscriptions.
 * @param conn - The connection that sent the `unsubscribe` message.
 * @param state - The state of that connection.
 * @param id - The id of the subscription to end.
 *
 * @internal
 */
export function handleUnsubscribeMessage(
  deps: WSSubscribeDeps,
  conn: WSConnection,
  state: ConnectionState,
  id: string,
): void {
  const sub = state.subscriptions.get(id)
  if (!sub) {
    deps.sendError(conn, id, 'SUBSCRIPTION_NOT_FOUND', `Subscription '${id}' not found`)
    return
  }

  sub.unsubscribe()
  state.subscriptions.delete(id)
  const confirmed: WSUnsubscribedMessage = { type: 'unsubscribed', id }
  deps.sendText(conn, JSON.stringify(confirmed))
  deps.cdc.maybeCleanup(state.databaseId)
}
