import { THEME_BOOT_SCRIPT } from '@delali/sirannon-example-shared/theme'
import { createRootRoute, HeadContent, Outlet, Scripts } from '@tanstack/react-router'
import type { ReactNode } from 'react'
import { readThemeChoice } from '../lib/theme.functions'
import '../styles.css'

const asset = (name: string) => `${import.meta.env.BASE_URL}${name}`

function head() {
  return {
    meta: [
      {
        charSet: 'utf-8',
      },
      {
        name: 'viewport',
        content: 'width=device-width, initial-scale=1',
      },
      {
        title: 'Sirannon Inventory Demo',
      },
      {
        name: 'description',
        content:
          'A fulfillment operations console where every list on the page is a live query served over one Sirannon WebSocket.',
      },
      {
        name: 'theme-color',
        content: '#0d9488',
      },
    ],
    links: [
      { rel: 'icon', type: 'image/svg+xml', href: asset('sirannon.svg') },
      { rel: 'icon', href: asset('sirannon.ico'), sizes: 'any' },
      { rel: 'apple-touch-icon', href: asset('sirannon-apple.png') },
      { rel: 'manifest', href: asset('manifest.json') },
    ],
    scripts: [{ children: THEME_BOOT_SCRIPT }],
  }
}

export const Route = createRootRoute({
  head,
  loader: () => readThemeChoice(),
  component: RootComponent,
  notFoundComponent: NotFoundComponent,
})

function RootComponent() {
  return (
    <RootDocument>
      <Outlet />
    </RootDocument>
  )
}

function RootDocument({ children }: Readonly<{ children: ReactNode }>) {
  return (
    <html lang="en" suppressHydrationWarning>
      <head>
        <HeadContent />
      </head>
      <body>
        {children}
        <Scripts />
      </body>
    </html>
  )
}

function NotFoundComponent() {
  return (
    <main className="not-found">
      <h1>Page not found</h1>
      <p>The requested route is not available in this example.</p>
    </main>
  )
}
