import { defineDriver } from '../../core/driver/define.js'
import { assertHoldsStatement } from '../../core/driver/statement-text.js'
import { synchronousPragmaValue } from '../../core/driver/synchronous.js'
import type { SQLiteConnection, SQLiteDriver, SQLiteStatement } from '../../core/driver/types.js'
import { EncryptionError } from '../../core/encryption/errors.js'
import { reportEncryptedPageFailures } from '../../core/encryption/page-errors.js'
import { ExtensionError } from '../../core/errors.js'
import { expoBindParams } from './bind.js'
import { keyExpoConnection, probeExpoDatabase, readUnkeyedExpoSchema } from './encryption.js'
import { expoDatabaseFiles } from './files.js'

/**
 * Returns a driver that opens SQLite databases on a device through `expo-sqlite`.
 *
 * The driver sets `multipleConnections` to `false`, so Sirannon opens no reader
 * pool and sends every read and write for a database through one connection.
 *
 * @returns The driver, which you can pass to a `Sirannon` registry in a React Native app.
 *
 * @public
 */
export function expoSqlite(): SQLiteDriver {
  return defineDriver({
    capabilities: {
      multipleConnections: false,
      extensions: false,
      steppedCopy: false,
      rowCounts: false,
      encryption: true,
    },
    files: expoDatabaseFiles,
    async open(path, options) {
      const SQLite = await import('expo-sqlite')
      const openHandle = () =>
        SQLite.openDatabaseAsync(path, {
          readOnly: options?.readonly,
          useNewConnection: true,
        } as Record<string, unknown>)
      type SQLiteHandle = Awaited<ReturnType<typeof SQLite.openDatabaseAsync>>
      const rawKey = options?.encryption?.key
      if (rawKey !== undefined && (path === '' || path === ':memory:')) {
        throw new EncryptionError('An in-memory database cannot be encrypted', 'ENCRYPTION_UNSUPPORTED')
      }
      if (rawKey !== undefined && (await probeExpoDatabase(openHandle)) === 'plaintext') {
        throw new EncryptionError(
          `The database ${path} is plaintext, so it opens without a key. Encrypt it through changeEncryption`,
          'DATABASE_NOT_ENCRYPTED',
        )
      }
      const db = await openHandle()

      try {
        if (rawKey !== undefined) await keyExpoConnection(db, path, rawKey)
        else await readUnkeyedExpoSchema(db, path)
        if (options?.walMode !== false) await db.execAsync('PRAGMA journal_mode = WAL')
        await db.execAsync(`PRAGMA synchronous = ${synchronousPragmaValue(options?.synchronous)}`)
        await db.execAsync('PRAGMA foreign_keys = ON')
        if (options?.walAutoCheckpoint !== undefined) {
          await db.execAsync(`PRAGMA wal_autocheckpoint = ${Math.trunc(options.walAutoCheckpoint)}`)
        }
      } catch (err) {
        await db.closeAsync().catch(() => undefined)
        throw err
      }

      const buildConnectionFromHandle = (dbHandle: SQLiteHandle): SQLiteConnection => ({
        async exec(sql: string): Promise<void> {
          await dbHandle.execAsync(sql)
        },

        async prepare(sql: string): Promise<SQLiteStatement> {
          assertHoldsStatement(sql)
          return {
            async all<T = unknown>(...params: unknown[]): Promise<T[]> {
              return dbHandle.getAllAsync<T>(sql, expoBindParams(params))
            },

            async get<T = unknown>(...params: unknown[]): Promise<T | undefined> {
              const row = await dbHandle.getFirstAsync<T>(sql, expoBindParams(params))
              return (row ?? undefined) as T | undefined
            },

            async run(...params: unknown[]) {
              const result = await dbHandle.runAsync(sql, expoBindParams(params))
              return {
                changes: result.changes,
                lastInsertRowId: result.lastInsertRowId,
              }
            },
          }
        },

        async transaction<T>(fn: (c: SQLiteConnection) => Promise<T>): Promise<T> {
          await dbHandle.execAsync('BEGIN')
          try {
            const result = await fn(conn)
            await dbHandle.execAsync('COMMIT')
            return result
          } catch (err) {
            await dbHandle.execAsync('ROLLBACK').catch(() => undefined)
            throw err
          }
        },

        async loadExtension(extensionPath: string): Promise<void> {
          throw new ExtensionError(
            extensionPath,
            'expo-sqlite carries no extension loading call, so a device running Expo loads no compiled extension',
          )
        },

        async close(): Promise<void> {
          await dbHandle.closeAsync()
        },
      })

      const conn = buildConnectionFromHandle(db)
      return rawKey === undefined ? conn : reportEncryptedPageFailures(conn)
    },
  })
}
