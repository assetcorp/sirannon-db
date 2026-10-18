import { basename, resolve } from 'node:path'
import { defineDriver } from '../../../core/driver/define.js'
import type {
  DriverCapabilities,
  OpenOptions,
  SQLiteConnection,
  SQLiteDriver,
  SQLiteStatement,
} from '../../../core/driver/types.js'
import { ExtensionError } from '../../../core/errors.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'

function withoutLoading(conn: SQLiteConnection): SQLiteConnection {
  const { loadExtension: _loadExtension, ...rest } = conn
  return rest
}

export function driverWithoutExtensionSupport(): SQLiteDriver {
  const base = betterSqlite3()
  return defineDriver({
    capabilities: { ...base.capabilities, extensions: false },
    open: async (path, options) => withoutLoading(await base.open(path, options)),
  })
}

export function driverOpeningConnectionsWithoutLoading(): SQLiteDriver {
  const base = betterSqlite3()
  return defineDriver({
    ...base,
    open: async (path, options) => withoutLoading(await base.open(path, options)),
  })
}

export function driverWithoutPathResolver(): SQLiteDriver {
  const base = betterSqlite3()
  return defineDriver({ capabilities: base.capabilities, open: base.open })
}

export function driverResolvingToRelativePath(): SQLiteDriver {
  const base = betterSqlite3()
  return defineDriver({ ...base, resolveExtensionPath: extensionPath => basename(extensionPath) })
}

export function driverFailingToLoad(thrown: unknown): SQLiteDriver {
  const base = betterSqlite3()
  return defineDriver({
    ...base,
    open: async (path, options) => ({
      ...(await base.open(path, options)),
      loadExtension: async () => {
        throw thrown
      },
    }),
  })
}

class ForwardingConnection implements SQLiteConnection {
  constructor(private readonly inner: SQLiteConnection) {}

  exec(sql: string): Promise<void> {
    return this.inner.exec(sql)
  }

  prepare(sql: string): Promise<SQLiteStatement> {
    return this.inner.prepare(sql)
  }

  transaction<T>(fn: (conn: SQLiteConnection) => Promise<T>): Promise<T> {
    return this.inner.transaction(fn)
  }

  async loadExtension(extensionPath: string): Promise<void> {
    if (!this.inner.loadExtension) throw new ExtensionError(extensionPath, 'the wrapped connection cannot load')
    await this.inner.loadExtension(extensionPath)
  }

  close(): Promise<void> {
    return this.inner.close()
  }
}

class ForwardingDriver implements SQLiteDriver {
  private readonly inner = betterSqlite3()
  readonly capabilities: DriverCapabilities = { ...this.inner.capabilities, steppedCopy: false }

  async open(path: string, options?: OpenOptions): Promise<SQLiteConnection> {
    return new ForwardingConnection(await this.inner.open(path, options))
  }

  resolveExtensionPath(extensionPath: string): string {
    return resolve(extensionPath)
  }
}

export function classBasedDriver(): SQLiteDriver {
  return defineDriver(new ForwardingDriver())
}
