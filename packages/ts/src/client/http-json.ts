import { RemoteError } from './types.js'

export const DEFAULT_HTTP_REQUEST_TIMEOUT_MS = 30_000

export function unrefTimer(timer: ReturnType<typeof setTimeout> | ReturnType<typeof setInterval>): void {
  const unrefable = timer as unknown as { unref?: () => void }
  unrefable.unref?.()
}

async function requestJson(url: string, init: RequestInit, timeoutMs: number, cancel?: AbortSignal): Promise<unknown> {
  const controller = new AbortController()
  const timer = timeoutMs > 0 ? setTimeout(() => controller.abort(), timeoutMs) : undefined
  if (timer !== undefined) unrefTimer(timer)
  const abort = (): void => controller.abort()
  if (cancel?.aborted) abort()
  cancel?.addEventListener('abort', abort, { once: true })

  let response: Response
  try {
    response = await fetch(url, { ...init, signal: controller.signal })
  } catch (err) {
    clearTimeout(timer)
    cancel?.removeEventListener('abort', abort)
    throw new RemoteError(
      'CONNECTION_ERROR',
      `Failed to connect to ${url}: ${err instanceof Error ? err.message : String(err)}`,
    )
  }

  let data: unknown
  try {
    data = await response.json()
  } catch {
    if (controller.signal.aborted && cancel?.aborted !== true) {
      throw new RemoteError('TIMEOUT', `The response from ${url} took longer than ${timeoutMs}ms to arrive`)
    }
    throw new RemoteError('INVALID_RESPONSE', `Server returned non-JSON response (HTTP ${response.status})`)
  } finally {
    clearTimeout(timer)
    cancel?.removeEventListener('abort', abort)
  }

  if (!response.ok) {
    const errorData = data as { error?: { code?: string; message?: string } }
    throw new RemoteError(
      errorData.error?.code ?? 'UNKNOWN_ERROR',
      errorData.error?.message ?? `HTTP ${response.status}`,
    )
  }
  return data
}

export async function postJson(
  url: string,
  body: unknown,
  headers: Record<string, string> | undefined,
  timeoutMs: number = DEFAULT_HTTP_REQUEST_TIMEOUT_MS,
): Promise<unknown> {
  return requestJson(
    url,
    {
      method: 'POST',
      headers: { 'content-type': 'application/json', ...headers },
      body: JSON.stringify(body),
    },
    timeoutMs,
  )
}

export async function getJson(
  url: string,
  headers: Record<string, string> | undefined,
  timeoutMs: number = DEFAULT_HTTP_REQUEST_TIMEOUT_MS,
  cancel?: AbortSignal,
): Promise<unknown> {
  return requestJson(url, { method: 'GET', headers: { ...headers } }, timeoutMs, cancel)
}
