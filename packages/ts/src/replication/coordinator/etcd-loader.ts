import { type OptionalDependency, optionalDependencyError } from '../../core/optional-dependency.js'

export type Etcd3Module = typeof import('etcd3')

const ETCD3_DEPENDENCY: OptionalDependency = {
  packageName: 'etcd3',
  neededBy: 'The etcd coordinator',
  installCommand: 'pnpm add -E etcd3',
  code: 'COORDINATOR_DEPENDENCY_MISSING',
}

export async function loadEtcd3Module(): Promise<Etcd3Module> {
  try {
    return await import('etcd3')
  } catch (err) {
    throw optionalDependencyError(ETCD3_DEPENDENCY, err)
  }
}
