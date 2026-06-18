import { createRootRoute, HeadContent, Outlet, Scripts } from '@tanstack/react-router'
import { TooltipProvider } from '@/components/ui/tooltip'
import '../styles.css'

export const Route = createRootRoute({
  head: () => ({
    meta: [
      { charSet: 'utf-8' },
      { name: 'viewport', content: 'width=device-width, initial-scale=1' },
      { title: 'Sirannon · Entitlement Control Plane' },
    ],
  }),
  component: RootLayout,
  notFoundComponent: NotFoundComponent,
})

function RootLayout() {
  return (
    <html lang="en" className="dark">
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
