import { Monitor, Moon, Sun } from 'lucide-react'
import { type ChangeEvent, useCallback, useEffect, useState } from 'react'
import { applyTheme, storeChoice, THEME_CHOICES, type ThemeChoice, watchSystemTheme } from '../theme'
import { Tooltip, TooltipContent, TooltipTrigger } from './ui/tooltip'

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
    <Tooltip>
      <TooltipTrigger asChild>
        <label className="text-muted-foreground hover:text-foreground has-checked:bg-background has-checked:text-foreground has-checked:shadow-xs has-focus-visible:ring-ring/50 flex size-6 cursor-pointer items-center justify-center rounded-sm transition-colors has-focus-visible:ring-[3px]">
          <input
            type="radio"
            name="theme"
            value={choice}
            checked={selected}
            onChange={handleChange}
            className="sr-only"
          />
          <Icon className="size-3.5" aria-hidden="true" />
          <span className="sr-only">{OPTION_LABEL[choice]}</span>
        </label>
      </TooltipTrigger>
      <TooltipContent>{OPTION_LABEL[choice]}</TooltipContent>
    </Tooltip>
  )
}

export function ThemeToggle({ initialChoice }: { initialChoice: ThemeChoice }) {
  const [choice, setChoice] = useState<ThemeChoice>(initialChoice)

  useEffect(() => {
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
    <fieldset className="border-border bg-muted/60 flex items-center gap-0.5 rounded-md border p-0.5">
      <legend className="sr-only">Theme</legend>
      {THEME_CHOICES.map(option => (
        <ThemeOption key={option} choice={option} selected={option === choice} onSelect={handleSelect} />
      ))}
    </fieldset>
  )
}
