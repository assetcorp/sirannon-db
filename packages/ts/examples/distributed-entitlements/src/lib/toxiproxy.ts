const TOXIPROXY_REQUEST_TIMEOUT_MS = 3_000

export async function setProxyEnabled(proxyName: string, enabled: boolean): Promise<void> {
  const url = process.env.TOXIPROXY_URL ?? 'http://127.0.0.1:8474'
  let response: Response
  try {
    response = await fetchWithTimeout(
      `${url}/proxies/${encodeURIComponent(proxyName)}`,
      {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
        },
        body: JSON.stringify({ enabled }),
      },
      TOXIPROXY_REQUEST_TIMEOUT_MS,
    )
  } catch (error) {
    if (isAbortError(error)) {
      throw new Error(
        `Toxiproxy failed to update ${proxyName}: request timed out after ${TOXIPROXY_REQUEST_TIMEOUT_MS}ms`,
      )
    }
    throw error
  }

  if (!response.ok) {
    const body = await response.text().catch(() => '')
    throw new Error(`Toxiproxy failed to update ${proxyName}: HTTP ${response.status} ${body}`)
  }
}

async function fetchWithTimeout(url: string, init: RequestInit, timeoutMs: number): Promise<Response> {
  const controller = new AbortController()
  const timeout = setTimeout(() => {
    controller.abort()
  }, timeoutMs)

  try {
    return await fetch(url, { ...init, signal: controller.signal })
  } finally {
    clearTimeout(timeout)
  }
}

function isAbortError(error: unknown): boolean {
  return error instanceof Error && error.name === 'AbortError'
}
