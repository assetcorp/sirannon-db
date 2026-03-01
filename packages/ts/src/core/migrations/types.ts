/** Row shape stored in the `_sirannon_migrations` tracking table. */
export interface AppliedMigration {
  version: number
  name: string
  applied_at: number
}
