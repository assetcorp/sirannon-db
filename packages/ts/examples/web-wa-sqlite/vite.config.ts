import tailwindcss from '@tailwindcss/vite'
import { tanstackStart } from '@tanstack/react-start/plugin/vite'
import viteReact from '@vitejs/plugin-react'
import { defineConfig } from 'vite'

const BROWSER_ONLY_MODE = 'browser-only'

export default defineConfig(({ mode }) => ({
  define: {
    __SIRANNON_BROWSER_ONLY__: JSON.stringify(mode === BROWSER_ONLY_MODE),
  },
  optimizeDeps: {
    exclude: ['wa-sqlite'],
  },
  plugins: [tailwindcss(), tanstackStart({ spa: { enabled: true } }), viteReact()],
}))
