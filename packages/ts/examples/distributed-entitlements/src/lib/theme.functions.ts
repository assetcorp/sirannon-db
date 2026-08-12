import { parseThemeChoice, THEME_COOKIE_NAME, type ThemeChoice } from '@delali/sirannon-example-shared/theme'
import { createServerFn } from '@tanstack/react-start'
import { getCookie } from '@tanstack/react-start/server'

export const readThemeChoice = createServerFn().handler(
  (): ThemeChoice => parseThemeChoice(getCookie(THEME_COOKIE_NAME)),
)
