import type { Migration } from './types.js'

export const LAZY_DOWN_SQL = Symbol('sirannon.file-migration.lazy-down')

export interface LazyDownMigration extends Migration {
  readonly [LAZY_DOWN_SQL]?: () => string
}
