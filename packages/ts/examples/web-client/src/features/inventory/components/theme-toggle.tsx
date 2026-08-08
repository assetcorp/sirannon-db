import { Monitor, Moon, Sun } from 'lucide-react'
import { type ChangeEvent, useCallback, useEffect, useState } from 'react'
import {
  applyTheme,
  readStoredChoice,
  storeChoice,
  THEME_CHOICES,
  type ThemeChoice,
  watchSystemTheme,
} from '../../../lib/theme'

const OPTION_ICON = {
  system: Monitor,
  light: Sun,
  dark: Moon,
} as const

const OPTION_LABEL = {
  system: 'Match the system setting',
  light: 'Light theme',
  dark: 'Dark theme',
} as const

function ThemeOption({
  choice,
  selected,
  onSelect,
}: {
  choice: ThemeChoice
  selected: boolean
  onSelect: (choice: ThemeChoice) => void
}) {
  const Icon = OPTION_ICON[choice]
  const handleChange = useCallback(
    (event: ChangeEvent<HTMLInputElement>) => {
      if (event.target.checked) {
        onSelect(choice)
      }
    },
    [choice, onSelect],
  )

  return (
    <label className="theme-option" title={OPTION_LABEL[choice]}>
      <input type="radio" name="theme" value={choice} checked={selected} onChange={handleChange} />
      <Icon size={14} aria-hidden="true" />
      <span className="sr-only">{OPTION_LABEL[choice]}</span>
    </label>
  )
}

export function ThemeToggle() {
  const [choice, setChoice] = useState<ThemeChoice | null>(null)

  useEffect(() => {
    setChoice(readStoredChoice())
  }, [])

  useEffect(() => {
    if (choice === null) return
    applyTheme(choice)
    if (choice !== 'system') return
    return watchSystemTheme(() => {
      applyTheme('system')
    })
  }, [choice])

  const handleSelect = useCallback((next: ThemeChoice) => {
    storeChoice(next)
    setChoice(next)
  }, [])

  return (
    <fieldset className="theme-toggle">
      <legend className="sr-only">Theme</legend>
      {THEME_CHOICES.map(option => (
        <ThemeOption key={option} choice={option} selected={option === choice} onSelect={handleSelect} />
      ))}
    </fieldset>
  )
}
