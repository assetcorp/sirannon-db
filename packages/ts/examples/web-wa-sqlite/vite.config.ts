import tailwindcss from '@tailwindcss/vite'
import { tanstackStart } from '@tanstack/react-start/plugin/vite'
import viteReact from '@vitejs/plugin-react'
import { defineConfig } from 'vite'

export default defineConfig({
  optimizeDeps: {
    exclude: ['wa-sqlite'],
  },
  plugins: [tailwindcss(), tanstackStart({ spa: { enabled: true } }), viteReact()],
})
