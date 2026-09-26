import { type OptionalDependency, optionalDependencyError } from '../core/optional-dependency.js'

export type UWebSockets = typeof import('uWebSockets.js')

const UWEBSOCKETS_DEPENDENCY: OptionalDependency = {
  packageName: 'uWebSockets.js',
  neededBy: 'The Sirannon server',
  installCommand: 'pnpm add -E "uWebSockets.js@github:uNetworking/uWebSockets.js#v20.69.0"',
  code: 'SERVER_DEPENDENCY_MISSING',
}

export async function loadUWebSockets(): Promise<UWebSockets> {
  try {
    return await import('uWebSockets.js')
  } catch (err) {
    throw optionalDependencyError(UWEBSOCKETS_DEPENDENCY, err)
  }
}
