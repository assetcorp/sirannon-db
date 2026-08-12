import { THEME_BOOT_SCRIPT } from '@delali/sirannon-example-shared/theme'
import { TooltipProvider } from '@delali/sirannon-example-shared/ui/tooltip'
import { createRootRoute, HeadContent, Outlet, Scripts } from '@tanstack/react-router'
import { readThemeChoice } from '../lib/theme.functions'
import '../styles.css'

const asset = (name: string) => `${import.meta.env.BASE_URL}${name}`

export const Route = createRootRoute({
  head: () => ({
    meta: [
      { charSet: 'utf-8' },
      { name: 'viewport', content: 'width=device-width, initial-scale=1' },
      { title: 'Sirannon · Entitlement Control Plane' },
      {
        name: 'description',
        content:
          'A three-node Sirannon cluster running a SaaS entitlement control plane, with etcd authority, gRPC replication, and automatic failover.',
      },
      { name: 'theme-color', content: '#0d9488' },
    ],
    links: [
      { rel: 'icon', type: 'image/svg+xml', href: asset('sirannon.svg') },
      { rel: 'icon', href: asset('sirannon.ico'), sizes: 'any' },
      { rel: 'apple-touch-icon', href: asset('sirannon-apple.png') },
      { rel: 'manifest', href: asset('manifest.json') },
    ],
    scripts: [{ children: THEME_BOOT_SCRIPT }],
  }),
  loader: () => readThemeChoice(),
  component: RootLayout,
  notFoundComponent: NotFoundComponent,
})

function RootLayout() {
  return (
    <html lang="en" suppressHydrationWarning>
      <head>
        <HeadContent />
      </head>
      <body>
        <TooltipProvider>
          <Outlet />
        </TooltipProvider>
        <Scripts />
      </body>
    </html>
  )
}

function NotFoundComponent() {
  return (
    <main className="flex min-h-dvh flex-col items-center justify-center gap-2 px-6 text-center">
      <h1 className="text-2xl font-semibold tracking-tight">Page not found</h1>
      <p className="text-muted-foreground text-sm">The requested route is not available in this example.</p>
    </main>
  )
}
