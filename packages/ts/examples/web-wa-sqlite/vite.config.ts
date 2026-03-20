import { fileURLToPath } from 'node:url'
import { defineConfig } from 'vite'

export default defineConfig({
  optimizeDeps: {
    exclude: ['wa-sqlite'],
  },
  resolve: {
    alias: {
      'node:fs': fileURLToPath(new URL('./src/shims/empty.ts', import.meta.url)),
      'node:path': fileURLToPath(new URL('./src/shims/empty.ts', import.meta.url)),
      'node:os': fileURLToPath(new URL('./src/shims/empty.ts', import.meta.url)),
      fs: fileURLToPath(new URL('./src/shims/empty.ts', import.meta.url)),
      path: fileURLToPath(new URL('./src/shims/empty.ts', import.meta.url)),
      os: fileURLToPath(new URL('./src/shims/empty.ts', import.meta.url)),
    },
  },
  server: {
    headers: {
      'Cross-Origin-Opener-Policy': 'same-origin',
      'Cross-Origin-Embedder-Policy': 'require-corp',
    },
  },
})
