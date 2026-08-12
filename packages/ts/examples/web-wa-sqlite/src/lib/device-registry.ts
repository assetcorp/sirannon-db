const DEVICE_NAME_PATTERN = /^[a-z0-9][a-z0-9-]{0,31}$/
const REGISTRY_STORAGE_KEY = 'sirannon-field-devices'
const TAB_LOCK_PREFIX = 'sirannon-device-'

export const DEVICE_NAME_RULE =
  'Lowercase letters, digits, and hyphens, up to 32 characters, starting with a letter or digit.'

export function normaliseDeviceName(raw: string): string {
  return raw.trim().toLowerCase()
}

export function isValidDeviceName(name: string): boolean {
  return DEVICE_NAME_PATTERN.test(name)
}

export function listKnownDevices(): string[] {
  try {
    const raw = localStorage.getItem(REGISTRY_STORAGE_KEY)
    if (raw === null) return []
    const parsed: unknown = JSON.parse(raw)
    if (!Array.isArray(parsed)) return []
    return parsed.filter((entry): entry is string => typeof entry === 'string' && isValidDeviceName(entry))
  } catch {
    return []
  }
}

export function rememberDevice(name: string): void {
  try {
    const devices = listKnownDevices()
    if (devices.includes(name)) return
    localStorage.setItem(REGISTRY_STORAGE_KEY, JSON.stringify([...devices, name].sort()))
  } catch {}
}

export async function listDevicesHeldElsewhere(): Promise<ReadonlySet<string>> {
  const held = new Set<string>()
  try {
    const state = await navigator.locks.query()
    for (const lock of state.held ?? []) {
      if (lock.name?.startsWith(TAB_LOCK_PREFIX)) {
        held.add(lock.name.slice(TAB_LOCK_PREFIX.length))
      }
    }
  } catch {}
  return held
}

export interface DeviceTabLock {
  acquired: boolean
  release: () => void
}

export async function acquireDeviceTabLock(name: string): Promise<DeviceTabLock> {
  if (typeof navigator === 'undefined' || navigator.locks === undefined) {
    return { acquired: true, release: () => {} }
  }

  let release: () => void = () => {}
  const heldUntilReleased = new Promise<void>(resolve => {
    release = resolve
  })

  return new Promise<DeviceTabLock>(resolve => {
    navigator.locks
      .request(`${TAB_LOCK_PREFIX}${name}`, { ifAvailable: true }, lock => {
        if (lock === null) {
          resolve({ acquired: false, release: () => {} })
          return
        }
        resolve({ acquired: true, release })
        return heldUntilReleased
      })
      .catch(() => {
        resolve({ acquired: true, release: () => {} })
      })
  })
}
