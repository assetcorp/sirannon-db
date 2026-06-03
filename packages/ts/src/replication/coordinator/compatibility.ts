import type { CoordinatorCompatibilityMetadata } from './types.js'

export function compatibilityAllowsPromotion(
  groupCompatibility: CoordinatorCompatibilityMetadata | undefined,
  sessionCompatibility: CoordinatorCompatibilityMetadata | undefined,
): boolean {
  if (!groupCompatibility) return true
  return (
    compatibleMajorVersion(groupCompatibility.packageVersion, sessionCompatibility?.packageVersion) &&
    compatibleMajorVersion(groupCompatibility.protocolVersion, sessionCompatibility?.protocolVersion) &&
    compatibleMajorVersion(groupCompatibility.specVersion, sessionCompatibility?.specVersion)
  )
}

function compatibleMajorVersion(required: string | undefined, candidate: string | undefined): boolean {
  if (!required) return true
  if (!candidate) return false
  const requiredMajor = parseMajorVersion(required)
  const candidateMajor = parseMajorVersion(candidate)
  if (requiredMajor === null || candidateMajor === null) {
    return required === candidate
  }
  return requiredMajor === candidateMajor
}

function parseMajorVersion(version: string): number | null {
  const match = /^v?(\d+)/.exec(version)
  if (!match) return null
  const major = match[1]
  return major === undefined ? null : Number(major)
}
