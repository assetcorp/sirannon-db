import { readdirSync, readFileSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { defineConfig, type Options } from 'tsup'

const peerDeps = [
  'better-sqlite3',
  'uWebSockets.js',
  'wa-sqlite',
  'expo-sqlite',
  'etcd3',
  '@grpc/grpc-js',
  'grpc-health-check',
  '@bufbuild/protobuf',
  'react',
]

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

const nodeEntry = {
  'core/index': 'src/core/index.ts',
  'core/writer-worker': 'src/core/worker/entry.ts',
  'codegen/index': 'src/codegen/index.ts',
  'codegen/cli': 'src/codegen/cli.ts',
  'server/index': 'src/server/index.ts',
  'driver/better-sqlite3': 'src/drivers/better-sqlite3/index.ts',
  'driver/node': 'src/drivers/node/index.ts',
  'file-migrations/index': 'src/utils/file-migrations/index.ts',
  'backup/index': 'src/backup/index.ts',
  'backup-scheduler/index': 'src/utils/backup-scheduler/index.ts',
  'replication/index': 'src/replication/index.ts',
  'replication/coordinator/etcd': 'src/replication/coordinator/etcd.ts',
  'transport/grpc': 'src/transport/grpc/index.ts',
}

const clientEntry = {
  'client/index': 'src/client/index.ts',
  'client/topology': 'src/client/topology.ts',
  'react/index': 'src/react/index.ts',
}

const waSqliteEntry = { 'driver/wa-sqlite': 'src/drivers/wa-sqlite/index.ts' }

const untypedEntry = {
  'driver/bun': 'src/drivers/bun/index.ts',
  'driver/expo': 'src/drivers/expo/index.ts',
}

const memoryTransportEntry = { 'transport/memory': 'src/transport/memory/index.ts' }

const declarationEntry = {
  ...nodeEntry,
  ...clientEntry,
  ...waSqliteEntry,
  ...untypedEntry,
  ...memoryTransportEntry,
}

export default defineConfig([
  {
    ...sharedOptions,
    entry: nodeEntry,
    platform: 'node',
    dts: false,
    clean: false,
    onSuccess: restoreNodePrefix,
  },
  {
    ...sharedOptions,
    entry: clientEntry,
    platform: 'browser',
    dts: false,
    clean: false,
  },
  {
    ...sharedOptions,
    entry: waSqliteEntry,
    platform: 'browser',
    dts: false,
    clean: false,
  },
  {
    ...sharedOptions,
    entry: untypedEntry,
    platform: 'browser',
    dts: false,
    clean: false,
  },
  {
    ...sharedOptions,
    entry: memoryTransportEntry,
    platform: 'neutral',
    dts: false,
    clean: false,
  },
  {
    ...sharedOptions,
    entry: declarationEntry,
    platform: 'neutral',
    dts: { only: true },
    clean: false,
  },
])
