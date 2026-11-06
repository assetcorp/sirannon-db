import type { Sirannon } from '../core/sirannon.js'
import type { BackupRestoreRuns } from './backup-restore-runs.js'
import type { RouteRegistrar } from './connection-drain.js'
import { wrapDbGetRoute, wrapDbRoute } from './db-route.js'
import { handleBackupRestore, handleBackupRestoreStatus } from './http-backup-restore.js'
import {
  handleBackupChain,
  handleBackupSafeToDelete,
  handleBackupStatus,
  handleBackupTrigger,
  handleBackupVerify,
} from './http-backups.js'
import { type EncryptionChangeSettings, handleEncryptionChange, handleEncryptionStatus } from './http-encryption.js'
import type { OperationRouteDeps } from './operation-route.js'

/**
 * What the backup and encryption routes need from the server.
 *
 * @internal
 */
export interface OperatorRouteDeps<Identity> {
  readonly sirannon: Sirannon
  readonly routeDeps: OperationRouteDeps<Identity>
  readonly acceptBackupRestore: boolean
  readonly restoreRuns: BackupRestoreRuns
  readonly encryptionChange: EncryptionChangeSettings
}

/**
 * Registers the routes that operate on a database file, which are the backup routes, the restore routes, and the
 * encryption routes.
 *
 * @param routes - The registrar that the server's routes go through.
 * @param deps - The registry, the route settings, and the switches that open the restore and key change routes.
 * @internal
 */
export function registerOperatorRoutes<Identity>(routes: RouteRegistrar, deps: OperatorRouteDeps<Identity>): void {
  const { sirannon, routeDeps } = deps
  const post = (path: string, handler: Parameters<typeof wrapDbRoute>[1]) =>
    routes.post(path, wrapDbRoute<Identity>(routeDeps, handler))
  const get = (path: string, handler: Parameters<typeof wrapDbGetRoute>[1]) =>
    routes.get(path, wrapDbGetRoute<Identity>(routeDeps, handler))

  post('/db/:id/backup', handleBackupTrigger(sirannon))
  get('/db/:id/backup', handleBackupStatus(sirannon))
  get('/db/:id/backup/chain', handleBackupChain(sirannon))
  post('/db/:id/backup/verify', handleBackupVerify(sirannon))
  post('/db/:id/backup/safe-to-delete', handleBackupSafeToDelete(sirannon))
  post('/db/:id/backup/restore', handleBackupRestore(sirannon, deps.acceptBackupRestore, deps.restoreRuns))
  get('/db/:id/backup/restore', handleBackupRestoreStatus(deps.restoreRuns))
  post('/db/:id/encryption', handleEncryptionChange(sirannon, deps.encryptionChange))
  get('/db/:id/encryption', handleEncryptionStatus(sirannon))
}
