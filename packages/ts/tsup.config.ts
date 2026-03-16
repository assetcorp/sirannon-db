import { defineConfig } from 'tsup'

const sharedOptions = {
  format: ['esm'] as const,
  splitting: true,
  treeshake: true,
  outExtension: () => ({ js: '.mjs' }),
  external: [
    'better-sqlite3',
    'uWebSockets.js',
    'wa-sqlite',
    'wa-sqlite/src/examples/IDBBatchAtomicVFS.js',
    'wa-sqlite/src/examples/AccessHandlePoolVFS.js',
    'expo-sqlite',
    'croner',
    'node:sqlite',
    'bun:sqlite',
    'node:fs',
    'node:path',
    'node:os',
  ],
}

export default defineConfig([
  {
    ...sharedOptions,
    entry: {
      'core/index': 'src/core/index.ts',
      'server/index': 'src/server/index.ts',
      'client/index': 'src/client/index.ts',
      'driver/better-sqlite3': 'src/drivers/better-sqlite3/index.ts',
      'driver/node': 'src/drivers/node/index.ts',
      'file-migrations/index': 'src/utils/file-migrations/index.ts',
      'backup-scheduler/index': 'src/utils/backup-scheduler/index.ts',
    },
    dts: true,
    clean: true,
  },
  {
    ...sharedOptions,
    entry: {
      'driver/bun': 'src/drivers/bun/index.ts',
      'driver/wa-sqlite': 'src/drivers/wa-sqlite/index.ts',
      'driver/expo': 'src/drivers/expo/index.ts',
    },
    dts: false,
    clean: false,
  },
])
