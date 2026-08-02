export const SIRANNON_WS_SUBPROTOCOL = 'sirannon.v1'

export const WS_CLOSE_UNAUTHENTICATED = 4401

export const WS_CLOSE_FORBIDDEN = 4403

const REFUSAL_CLOSE_CODE_MIN = 4000

const REFUSAL_CLOSE_CODE_MAX = 4099

export type SubprotocolSelection = { ok: true; protocol: string } | { ok: false }

export function selectSubprotocol(header: string): SubprotocolSelection {
  const offered = header
    .split(',')
    .map(value => value.trim())
    .filter(value => value.length > 0)

  if (offered.length === 0) return { ok: true, protocol: '' }
  if (offered.includes(SIRANNON_WS_SUBPROTOCOL)) return { ok: true, protocol: SIRANNON_WS_SUBPROTOCOL }
  return { ok: false }
}

export function withSirannonSubprotocol(protocols: string | string[] | undefined): string[] | undefined {
  if (protocols === undefined) return undefined
  const configured = typeof protocols === 'string' ? [protocols] : [...protocols]
  return [SIRANNON_WS_SUBPROTOCOL, ...configured.filter(value => value !== SIRANNON_WS_SUBPROTOCOL)]
}

export function isRefusalCloseCode(code: number): boolean {
  if (code === WS_CLOSE_UNAUTHENTICATED || code === WS_CLOSE_FORBIDDEN) return true
  return code >= REFUSAL_CLOSE_CODE_MIN && code <= REFUSAL_CLOSE_CODE_MAX
}

export function refusalErrorCode(code: number): string {
  return code === WS_CLOSE_FORBIDDEN ? 'FORBIDDEN' : 'UNAUTHORIZED'
}
