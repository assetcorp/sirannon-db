import { readdirSync, readFileSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { defineConfig, type Options } from 'tsup'

const peerDeps = ['better-sqlite3', 'uWebSockets.js', 'wa-sqlite', 'expo-sqlite', 'croner']

async function restoreNodePrefix() {
  const distDir = join(import.meta.dirname, 'dist')
  const files = readdirSync(distDir, { recursive: true, withFileTypes: true })
  for (const entry of files) {
    if (!entry.isFile() || !entry.name.endsWith('.mjs')) continue
    const filePath = join(entry.parentPath, entry.name)
    let content = readFileSync(filePath, 'utf-8')
    let changed = false
    const replacements: [RegExp, string][] = [
      [/import\('sqlite'\)/g, "import('node:sqlite')"],
      [/from 'sqlite'/g, "from 'node:sqlite'"],
      [/import\("sqlite"\)/g, 'import("node:sqlite")'],
      [/from "sqlite"/g, 'from "node:sqlite"'],
    ]
    for (const [pattern, replacement] of replacements) {
      if (pattern.test(content)) {
        content = content.replace(pattern, replacement)
        changed = true
      }
    }
    if (changed) {
      writeFileSync(filePath, content)
    }
  }
}

const sharedOptions: Options = {
  format: ['esm'],
  splitting: true,
  treeshake: true,
  outExtension: () => ({ js: '.mjs' }),
  external: [...peerDeps, /^node:/, /^bun:/],
}

export default defineConfig([
  {
    ...sharedOptions,
    entry: {
      'core/index': 'src/core/index.ts',
      'server/index': 'src/server/index.ts',
      'driver/better-sqlite3': 'src/drivers/better-sqlite3/index.ts',
      'driver/node': 'src/drivers/node/index.ts',
      'file-migrations/index': 'src/utils/file-migrations/index.ts',
      'backup-scheduler/index': 'src/utils/backup-scheduler/index.ts',
      'replication/index': 'src/replication/index.ts',
      'transport/websocket': 'src/transport/websocket/index.ts',
    },
    platform: 'node',
    dts: true,
    clean: false,
    onSuccess: restoreNodePrefix,
  },
  {
    ...sharedOptions,
    entry: {
      'client/index': 'src/client/index.ts',
    },
    platform: 'browser',
    dts: true,
    clean: false,
  },
  {
    ...sharedOptions,
    entry: {
      'driver/wa-sqlite': 'src/drivers/wa-sqlite/index.ts',
    },
    platform: 'browser',
    dts: true,
    clean: false,
  },
  {
    ...sharedOptions,
    entry: {
      'driver/bun': 'src/drivers/bun/index.ts',
      'driver/expo': 'src/drivers/expo/index.ts',
    },
    platform: 'browser',
    dts: false,
    clean: false,
  },
  {
    ...sharedOptions,
    entry: { 'transport/memory': 'src/transport/memory/index.ts' },
    platform: 'neutral',
    dts: true,
    clean: false,
  },
])
