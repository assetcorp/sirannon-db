import { ThemeToggle as ThemeChoiceToggle } from '@delali/sirannon-example-shared/components/theme-toggle'
import { getRouteApi } from '@tanstack/react-router'

const rootRoute = getRouteApi('__root__')

export function ThemeToggle() {
  const initialChoice = rootRoute.useLoaderData()
  return <ThemeChoiceToggle initialChoice={initialChoice} />
}
