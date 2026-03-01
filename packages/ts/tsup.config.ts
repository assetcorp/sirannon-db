import { defineConfig } from 'tsup'

export default defineConfig({
  entry: {
    'core/index': 'src/core/index.ts',
    'server/index': 'src/server/index.ts',
    'client/index': 'src/client/index.ts',
  },
  format: ['esm'],
  dts: true,
  splitting: true,
  clean: true,
  treeshake: true,
  outExtension: () => ({ js: '.mjs' }),
  external: ['better-sqlite3', 'uWebSockets.js'],
})
