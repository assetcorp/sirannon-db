import { tanstackStart } from '@tanstack/react-start/plugin/vite'
import viteReact from '@vitejs/plugin-react'
import { defineConfig } from 'vite'

const DEFAULT_APP_PORT = 3000

export default defineConfig({
  server: {
    port: Number(process.env.PORT ?? DEFAULT_APP_PORT),
  },
  plugins: [tanstackStart(), viteReact()],
})
