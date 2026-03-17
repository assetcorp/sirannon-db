import { defineConfig } from 'vite'

export default defineConfig({
  optimizeDeps: {
    exclude: ['wa-sqlite'],
  },
  resolve: {
    alias: {
      'node:fs': new URL('./src/shims/empty.ts', import.meta.url).pathname,
      'node:path': new URL('./src/shims/empty.ts', import.meta.url).pathname,
      'node:os': new URL('./src/shims/empty.ts', import.meta.url).pathname,
      fs: new URL('./src/shims/empty.ts', import.meta.url).pathname,
      path: new URL('./src/shims/empty.ts', import.meta.url).pathname,
      os: new URL('./src/shims/empty.ts', import.meta.url).pathname,
    },
  },
  server: {
    headers: {
      'Cross-Origin-Opener-Policy': 'same-origin',
      'Cross-Origin-Embedder-Policy': 'require-corp',
    },
  },
})
