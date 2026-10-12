import { createRequire } from 'node:module'
import { resolve } from 'node:path'
import type BetterSqlite3 from 'better-sqlite3'
import { SirannonError } from '../../core/errors.js'
import { optionalDependencyError } from '../../core/optional-dependency.js'

const SIRANNON_SQLITE_PACKAGE = '@delali/sirannon-sqlite'

interface SirannonSqliteModule {
  default: typeof BetterSqlite3 & { bindingPath(): string | null }
}

/**
 * The native module that better-sqlite3 loads, as Sirannon inspects it for the methods that a feature needs.
 *
 * @internal
 */
export interface SirannonSqliteAddon {
  Statement?: { prototype?: Record<string, unknown> }
}

/**
 * A driver option that needs Sirannon's SQLite build, with the error code that the driver raises when the build cannot serve it.
 *
 * @internal
 */
export interface SirannonSqliteFeature {
  /** The driver option that asks for the feature, such as `rowCounts`. */
  option: string
  /** The error code that the driver raises when the build is missing or lacks the feature. */
  code: string
  /** What the build lacks when the feature is missing, in words that fit 'has no ...'. */
  description: string
  /** Returns `true` when the loaded native module offers the feature. */
  presentIn(addon: SirannonSqliteAddon): boolean
}

/**
 * The better-sqlite3 constructor from `@delali/sirannon-sqlite`, with the path of the native build that it loads.
 *
 * @internal
 */
export interface SirannonSqlite {
  Database: typeof BetterSqlite3
  bindingPath: string
}

function errorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err)
}

/**
 * Loads better-sqlite3 from `@delali/sirannon-sqlite` together with the native build for this platform, and checks that the build offers every feature that the caller asked for.
 *
 * @param features - The features that the driver options ask for, each with the error code that the driver raises when the build cannot serve it.
 * @param bindingPath - The path of a native build that you compiled, which replaces the build in this platform's package.
 * @returns The constructor and the path of the native build, which the constructor receives as `nativeBinding`.
 * @internal
 */
export async function loadSirannonSqlite(
  features: readonly [SirannonSqliteFeature, ...SirannonSqliteFeature[]],
  bindingPath?: string,
): Promise<SirannonSqlite> {
  const [first] = features
  let Database: SirannonSqliteModule['default']
  try {
    Database = ((await import('@delali/sirannon-sqlite')) as SirannonSqliteModule).default
  } catch (err) {
    throw optionalDependencyError(
      {
        packageName: SIRANNON_SQLITE_PACKAGE,
        neededBy: `The better-sqlite3 driver's ${first.option} option`,
        installCommand: `pnpm add -E ${SIRANNON_SQLITE_PACKAGE}`,
        code: first.code,
      },
      err,
    )
  }
  const target = `${process.platform}-${process.arch}`
  const located = bindingPath === undefined ? Database.bindingPath() : resolve(bindingPath)
  if (located === null) {
    throw new SirannonError(
      `${SIRANNON_SQLITE_PACKAGE} found no native build for ${target}. Install ${SIRANNON_SQLITE_PACKAGE}-${target}, or set sqliteBindingPath to a build that you compile with native/sqlite/build.mjs.`,
      first.code,
    )
  }
  let addon: SirannonSqliteAddon
  try {
    addon = createRequire(import.meta.url)(located)
  } catch (err) {
    throw new SirannonError(`Sirannon's SQLite build at ${located} failed to load: ${errorMessage(err)}`, first.code)
  }
  for (const feature of features) {
    if (!feature.presentIn(addon)) {
      throw new SirannonError(
        `The SQLite build at ${located} has no ${feature.description}, so the ${feature.option} option cannot work. Point sqliteBindingPath at a build from native/sqlite/build.mjs.`,
        feature.code,
      )
    }
  }
  return { Database, bindingPath: located }
}
