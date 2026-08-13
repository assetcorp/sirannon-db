export type ThemeChoice = 'system' | 'light' | 'dark'

export const THEME_COOKIE_NAME = 'sirannon-theme'

export const THEME_CHOICES: ThemeChoice[] = ['system', 'light', 'dark']

const THEME_COOKIE_MAX_AGE_SECONDS = 31_536_000

const THEME_COOKIE_PATTERN = new RegExp(`(?:^|; )${THEME_COOKIE_NAME}=([^;]*)`)

export function parseThemeChoice(value: string | null | undefined): ThemeChoice {
  return value === 'light' || value === 'dark' ? value : 'system'
}

export function readStoredChoice(): ThemeChoice {
  try {
    return parseThemeChoice(document.cookie.match(THEME_COOKIE_PATTERN)?.[1])
  } catch {
    return 'system'
  }
}

export function storeChoice(choice: ThemeChoice): void {
  try {
    if (choice === 'system') {
      document.cookie = `${THEME_COOKIE_NAME}=; path=/; max-age=0; samesite=lax`
      return
    }
    document.cookie = `${THEME_COOKIE_NAME}=${choice}; path=/; max-age=${THEME_COOKIE_MAX_AGE_SECONDS}; samesite=lax`
  } catch {}
}

export function prefersDark(): boolean {
  return window.matchMedia('(prefers-color-scheme: dark)').matches
}

export function applyTheme(choice: ThemeChoice): void {
  const dark = choice === 'dark' || (choice === 'system' && prefersDark())
  document.documentElement.classList.toggle('dark', dark)
}

export function watchSystemTheme(onChange: () => void): () => void {
  const query = window.matchMedia('(prefers-color-scheme: dark)')
  query.addEventListener('change', onChange)
  return () => {
    query.removeEventListener('change', onChange)
  }
}

export const THEME_BOOT_SCRIPT = `(function(){try{var m=document.cookie.match(/(?:^|; )${THEME_COOKIE_NAME}=([^;]*)/);var c=m?m[1]:null;var d=c==='dark'||(c!=='light'&&window.matchMedia('(prefers-color-scheme: dark)').matches);document.documentElement.classList.toggle('dark',d)}catch(e){}})()`
