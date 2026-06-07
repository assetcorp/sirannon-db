import { createRootRoute, HeadContent, Outlet, Scripts } from '@tanstack/react-router'
import '../styles.css'

export const Route = createRootRoute({
  component: RootLayout,
  notFoundComponent: NotFoundComponent,
})

function RootLayout() {
  return (
    <html lang="en">
      <head>
        <HeadContent />
      </head>
      <body>
        <Outlet />
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
