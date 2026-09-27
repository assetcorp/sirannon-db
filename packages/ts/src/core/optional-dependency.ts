import { SirannonError } from './errors.js'

export interface OptionalDependency {
  packageName: string
  neededBy: string
  installCommand: string
  code: string
}

export function optionalDependencyError(dependency: OptionalDependency, loadFailure: unknown): SirannonError {
  const reported = loadFailure instanceof Error ? loadFailure.message : String(loadFailure)
  return new SirannonError(
    `${dependency.neededBy} needs the '${dependency.packageName}' package, which this process could not load. Install it with \`${dependency.installCommand}\`, or fix the failure that the loader reported: ${reported}`,
    dependency.code,
  )
}
