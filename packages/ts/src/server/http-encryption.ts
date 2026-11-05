import * as v from 'valibot'
import { type EncryptionKeySource, resolveEncryptionKey, wipeEncryptionKey } from '../core/encryption/key.js'
import type { EncryptionStatus } from '../core/encryption/types.js'
import { SirannonError } from '../core/errors.js'
import type { ServerOptions } from '../core/server-options.js'
import type { Sirannon } from '../core/sirannon.js'
import { clientMessageFor } from './client-error.js'
import { parseOptionalBody, refusedOnTheFirstTick } from './http-backups.js'
import { sendCaughtError, sendError, sendJson } from './http-common.js'
import type { DbGetRouteHandler, DbRouteHandler } from './http-handler.js'

const MAX_KEY_REF_LENGTH = 256

const ENCRYPTION_CHANGE_REQUEST = v.object({
  keyRef: v.nullable(v.pipe(v.string(), v.minLength(1), v.maxLength(MAX_KEY_REF_LENGTH))),
})

const ENCRYPTION_CHANGE_NOT_ACCEPTED_MESSAGE =
  'This server does not change database keys over the wire; turn on acceptEncryptionChange to open the route'

/**
 * Whether the server opens the key change route, and the function that turns a `keyRef` into a key.
 *
 * @internal
 */
export interface EncryptionChangeSettings {
  readonly accepted: boolean
  readonly keys: ServerOptions['encryptionKeys']
}

/**
 * Returns the server's key change settings, and throws when the operator opens the route without an `authenticate`
 * hook to name the caller or without an `encryptionKeys` function to resolve a `keyRef`.
 *
 * @param options - The server options.
 * @returns The settings that the key change route reads.
 * @throws A `SirannonError` with `INVALID_ENCRYPTION_CHANGE` for a route that the server cannot serve safely.
 * @internal
 */
export function resolveEncryptionChangeSettings<Identity>(
  options: ServerOptions<Identity> | undefined,
): EncryptionChangeSettings {
  const accepted = options?.acceptEncryptionChange === true
  if (accepted && (options?.authenticate === undefined || typeof options.encryptionKeys !== 'function')) {
    throw new SirannonError(
      'ServerOptions.acceptEncryptionChange opens a route that changes the key of a running database, so it needs an authenticate hook to name the caller and an encryptionKeys function to resolve each keyRef',
      'INVALID_ENCRYPTION_CHANGE',
    )
  }
  return { accepted, keys: options?.encryptionKeys }
}

async function checkedKeySource(
  keys: NonNullable<ServerOptions['encryptionKeys']>,
  databaseId: string,
  keyRef: string,
): Promise<EncryptionKeySource> {
  const source: EncryptionKeySource = () => keys(databaseId, keyRef)
  wipeEncryptionKey(await resolveEncryptionKey(source))
  return source
}

function errorForClient(error: { code: string; message: string }): { code: string; message: string } {
  return { code: error.code, message: clientMessageFor(error) }
}

function statusForClient(status: EncryptionStatus): EncryptionStatus {
  const { error, reopenError } = status.change
  return {
    ...status,
    change: {
      ...status.change,
      ...(error === undefined ? {} : { error: errorForClient(error) }),
      ...(reopenError === undefined ? {} : { reopenError: errorForClient(reopenError) }),
    },
  }
}

/**
 * Handles the route that encrypts a database, changes its key, or removes encryption.
 *
 * The caller names the key with a `keyRef`, which the server resolves through `encryptionKeys` before it answers, so
 * no key crosses the wire. The server waits one event-loop tick, so that it can report a change that the database
 * refuses at once, and then answers with 202, because copying a large database can take longer than the timeout of a
 * proxy between the caller and the server. The caller reads the outcome from the status route.
 *
 * @param sirannon - The registry in which the server's databases are open.
 * @param settings - Whether the operator opens this route, and how the server resolves a `keyRef`.
 * @returns The handler that the server registers.
 * @internal
 */
export function handleEncryptionChange(sirannon: Sirannon, settings: EncryptionChangeSettings): DbRouteHandler {
  return async (res, dbId, rawBody, abort) => {
    const keys = settings.keys
    if (!settings.accepted || keys === undefined) {
      sendError(res, 403, 'ENCRYPTION_CHANGE_NOT_ACCEPTED', ENCRYPTION_CHANGE_NOT_ACCEPTED_MESSAGE)
      return
    }
    const body = parseOptionalBody<unknown>(res, rawBody)
    if (body === null) return
    const request = v.safeParse(ENCRYPTION_CHANGE_REQUEST, body)
    if (!request.success) {
      sendError(
        res,
        400,
        'INVALID_REQUEST',
        `keyRef takes a string of 1 to ${MAX_KEY_REF_LENGTH} characters that names the target key, or null to remove encryption`,
      )
      return
    }
    const database = sirannon.get(dbId)
    if (!database) {
      sendError(res, 404, 'DATABASE_NOT_FOUND', `Database '${dbId}' not found`)
      return
    }

    let key: EncryptionKeySource | null = null
    try {
      if (request.output.keyRef !== null) key = await checkedKeySource(keys, dbId, request.output.keyRef)
    } catch (err) {
      sendCaughtError(res, abort, err)
      return
    }
    const refusal = await refusedOnTheFirstTick(database.changeEncryption({ key }))
    if (refusal !== undefined) {
      sendCaughtError(res, abort, refusal)
      return
    }
    if (abort.aborted) return
    sendJson(res, { started: true }, '202 Accepted')
  }
}

/**
 * Handles the route that reports how a database is encrypted and how its current or last key change went. The status
 * holds no key, and an error in it carries the fixed message for its code where the full message names server files.
 *
 * @param sirannon - The registry in which the server's databases are open.
 * @returns The handler that the server registers.
 * @internal
 */
export function handleEncryptionStatus(sirannon: Sirannon): DbGetRouteHandler {
  return async (res, dbId, _ctx, abort) => {
    const database = sirannon.get(dbId)
    if (!database) {
      sendError(res, 404, 'DATABASE_NOT_FOUND', `Database '${dbId}' not found`)
      return
    }
    let status: EncryptionStatus
    try {
      status = await database.encryptionStatus()
    } catch (err) {
      sendCaughtError(res, abort, err)
      return
    }
    if (abort.aborted) return
    sendJson(res, statusForClient(status))
  }
}
