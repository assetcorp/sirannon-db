import type { HttpResponse } from 'uWebSockets.js'
import { SirannonError } from '../core/errors.js'
import type { Sirannon } from '../core/sirannon.js'
import type { ErrorResponse, ExecuteRequest, QueryRequest, TransactionRequest } from './protocol.js'
import { toExecuteResponse } from './protocol.js'

const MAX_BODY_BYTES = 1_048_576

/**
 * Shared abort tracker for a uWS HttpResponse. uWS only allows a single
 * `onAborted` handler per response, so we register once and fan out to
 * multiple listeners.
 */
export interface ResponseAbort {
	readonly aborted: boolean
	onAbort(fn: () => void): void
}

/**
 * Register the abort handler on a response. Must be called exactly once
 * per response before any async work begins.
 */
export function initAbortHandler(res: HttpResponse): ResponseAbort {
	const listeners: (() => void)[] = []
	let aborted = false

	res.onAborted(() => {
		aborted = true
		for (const fn of listeners) fn()
	})

	return {
		get aborted() {
			return aborted
		},
		onAbort(fn) {
			if (aborted) {
				fn()
			} else {
				listeners.push(fn)
			}
		},
	}
}

/**
 * Read the full request body from a uWS HttpResponse.
 *
 * IMPORTANT: This MUST be called synchronously inside the route handler
 * callback (before the handler function returns to uWS), because uWS
 * delivers body data immediately. If `onData` isn't registered before
 * the route callback returns, body data is lost.
 */
export function readBody(res: HttpResponse, maxBytes: number, abort: ResponseAbort): Promise<Buffer> {
	return new Promise((resolve, reject) => {
		if (abort.aborted) {
			reject(new Error('Request aborted'))
			return
		}

		let done = false
		const chunks: Buffer[] = []
		let totalLength = 0

		abort.onAbort(() => {
			if (done) return
			done = true
			reject(new Error('Request aborted'))
		})

		res.onData((chunk, isLast) => {
			if (done || abort.aborted) return

			totalLength += chunk.byteLength
			if (totalLength > maxBytes) {
				done = true
				sendError(res, 413, 'PAYLOAD_TOO_LARGE', 'Request body exceeds size limit')
				reject(new Error('Payload too large'))
				return
			}

			// Copy chunk because uWS neuters the ArrayBuffer after return
			chunks.push(Buffer.from(chunk))

			if (isLast) {
				done = true
				resolve(Buffer.concat(chunks))
			}
		})
	})
}

/**
 * Parse a Buffer as JSON. Returns the parsed object or null if the body
 * is empty or malformed (sends the appropriate error response).
 */
function parseBody<T>(res: HttpResponse, raw: Buffer): T | null {
	if (raw.length === 0) {
		sendError(res, 400, 'EMPTY_BODY', 'Request body is empty')
		return null
	}

	try {
		return JSON.parse(raw.toString('utf-8')) as T
	} catch {
		sendError(res, 400, 'INVALID_JSON', 'Request body is not valid JSON')
		return null
	}
}

function sendJson(res: HttpResponse, data: unknown): void {
	const payload = JSON.stringify(data)
	res.cork(() => {
		res.writeStatus('200 OK').writeHeader('Content-Type', 'application/json').end(payload)
	})
}

export function sendError(res: HttpResponse, status: number, code: string, message: string): void {
	const body: ErrorResponse = { error: { code, message } }
	const payload = JSON.stringify(body)
	res.cork(() => {
		res.writeStatus(`${status}`).writeHeader('Content-Type', 'application/json').end(payload)
	})
}

function httpStatusForError(err: SirannonError): number {
	switch (err.code) {
		case 'DATABASE_NOT_FOUND':
			return 404
		case 'READ_ONLY':
			return 403
		case 'QUERY_ERROR':
		case 'TRANSACTION_ERROR':
			return 400
		case 'HOOK_DENIED':
			return 403
		case 'DATABASE_CLOSED':
		case 'SHUTDOWN':
			return 503
		default:
			return 500
	}
}

function resolveDatabase(res: HttpResponse, sirannon: Sirannon, id: string) {
	const db = sirannon.get(id)
	if (!db) {
		sendError(res, 404, 'DATABASE_NOT_FOUND', `Database '${id}' not found`)
		return null
	}
	return db
}

/**
 * Handler receives a pre-read body Buffer. Body reading happens in the
 * server's wrapDbRoute middleware so that `onData` is always registered
 * synchronously in the uWS route callback.
 */
export type DbRouteHandler = (res: HttpResponse, dbId: string, rawBody: Buffer) => void

export function handleQuery(sirannon: Sirannon): DbRouteHandler {
	return (res, dbId, rawBody) => {
		const body = parseBody<QueryRequest>(res, rawBody)
		if (!body) return

		if (!body.sql || typeof body.sql !== 'string') {
			sendError(res, 400, 'INVALID_REQUEST', 'Field "sql" is required and must be a string')
			return
		}

		const db = resolveDatabase(res, sirannon, dbId)
		if (!db) return

		try {
			const rows = db.query(body.sql, body.params)
			sendJson(res, { rows })
		} catch (err) {
			if (err instanceof SirannonError) {
				sendError(res, httpStatusForError(err), err.code, err.message)
			} else {
				sendError(res, 500, 'INTERNAL_ERROR', 'An unexpected error occurred')
			}
		}
	}
}

export function handleExecute(sirannon: Sirannon): DbRouteHandler {
	return (res, dbId, rawBody) => {
		const body = parseBody<ExecuteRequest>(res, rawBody)
		if (!body) return

		if (!body.sql || typeof body.sql !== 'string') {
			sendError(res, 400, 'INVALID_REQUEST', 'Field "sql" is required and must be a string')
			return
		}

		const db = resolveDatabase(res, sirannon, dbId)
		if (!db) return

		try {
			const result = db.execute(body.sql, body.params)
			sendJson(res, toExecuteResponse(result))
		} catch (err) {
			if (err instanceof SirannonError) {
				sendError(res, httpStatusForError(err), err.code, err.message)
			} else {
				sendError(res, 500, 'INTERNAL_ERROR', 'An unexpected error occurred')
			}
		}
	}
}

export function handleTransaction(sirannon: Sirannon): DbRouteHandler {
	return (res, dbId, rawBody) => {
		const body = parseBody<TransactionRequest>(res, rawBody)
		if (!body) return

		if (!Array.isArray(body.statements)) {
			sendError(res, 400, 'INVALID_REQUEST', 'Field "statements" is required and must be an array')
			return
		}

		if (body.statements.length === 0) {
			sendError(res, 400, 'INVALID_REQUEST', 'Transaction requires at least one statement')
			return
		}

		for (let i = 0; i < body.statements.length; i++) {
			const stmt = body.statements[i]
			if (!stmt.sql || typeof stmt.sql !== 'string') {
				sendError(res, 400, 'INVALID_REQUEST', `Statement at index ${i} is missing a valid "sql" field`)
				return
			}
		}

		const db = resolveDatabase(res, sirannon, dbId)
		if (!db) return

		try {
			const results = db.transaction(tx => {
				return body.statements.map(stmt => tx.execute(stmt.sql, stmt.params))
			})
			sendJson(res, {
				results: results.map(toExecuteResponse),
			})
		} catch (err) {
			if (err instanceof SirannonError) {
				sendError(res, httpStatusForError(err), err.code, err.message)
			} else {
				sendError(res, 500, 'INTERNAL_ERROR', 'An unexpected error occurred')
			}
		}
	}
}
