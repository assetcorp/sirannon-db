import { createIcons, Monitor, Moon, Sun } from 'lucide'

export type ThemeChoice = 'system' | 'light' | 'dark'

const STORAGE_KEY = 'sirannon-theme'

const OPTIONS: Array<{ choice: ThemeChoice; icon: string; label: string }> = [
  { choice: 'system', icon: 'monitor', label: 'Match the system setting' },
  { choice: 'light', icon: 'sun', label: 'Light theme' },
  { choice: 'dark', icon: 'moon', label: 'Dark theme' },
]

function readStoredChoice(): ThemeChoice {
  try {
    const stored = localStorage.getItem(STORAGE_KEY)
    return stored === 'light' || stored === 'dark' ? stored : 'system'
  } catch {
    return 'system'
  }
}

function storeChoice(choice: ThemeChoice): void {
  try {
    if (choice === 'system') {
      localStorage.removeItem(STORAGE_KEY)
      return
    }
    localStorage.setItem(STORAGE_KEY, choice)
  } catch {}
}

function applyTheme(choice: ThemeChoice): void {
  const dark = choice === 'dark' || (choice === 'system' && window.matchMedia('(prefers-color-scheme: dark)').matches)
  document.documentElement.classList.toggle('dark', dark)
}

/**
 * Renders the theme control into the given container and applies the stored choice.
 */
export function mountThemeToggle(container: HTMLElement): void {
  let choice = readStoredChoice()

  container.className = 'theme-toggle'
  container.innerHTML = `<legend class="sr-only">Theme</legend>${OPTIONS.map(
    option =>
      `<label class="theme-option" title="${option.label}"><input type="radio" name="theme" value="${option.choice}" /><i data-lucide="${option.icon}"></i><span class="sr-only">${option.label}</span></label>`,
  ).join('')}`

  const inputs = Array.from(container.querySelectorAll<HTMLInputElement>('input[name="theme"]'))

  function render(): void {
    for (const input of inputs) {
      input.checked = input.value === choice
    }
    applyTheme(choice)
  }

  for (const input of inputs) {
    input.addEventListener('change', () => {
      const next = input.value
      if (next !== 'system' && next !== 'light' && next !== 'dark') return
      choice = next
      storeChoice(choice)
      render()
    })
  }

  window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', () => {
    if (choice === 'system') {
      applyTheme('system')
    }
  })

  render()
  createIcons({ icons: { Monitor, Moon, Sun } })
}
