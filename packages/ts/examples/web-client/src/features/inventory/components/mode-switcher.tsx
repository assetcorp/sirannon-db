import { Database, ServerCog } from 'lucide-react'
import { useCallback } from 'react'
import type { DemoMode, ModeOptionData } from '../types'
import { MODE_OPTIONS } from '../types'

export function ModeSwitcher({ activeMode, onChange }: { activeMode: DemoMode; onChange: (mode: DemoMode) => void }) {
  const optionNodes = MODE_OPTIONS.map(option => (
    <ModeOption key={option.mode} option={option} active={option.mode === activeMode} onChange={onChange} />
  ))

  return <div className="mode-switcher">{optionNodes}</div>
}

function ModeOption({
  option,
  active,
  onChange,
}: {
  option: ModeOptionData
  active: boolean
  onChange: (mode: DemoMode) => void
}) {
  const handleClick = useCallback(() => {
    onChange(option.mode)
  }, [onChange, option.mode])

  const icon = option.mode === 'app-actions' ? <ServerCog size={18} /> : <Database size={18} />

  return (
    <button
      type="button"
      className={active ? 'mode-option active' : 'mode-option'}
      onClick={handleClick}
      aria-pressed={active}
    >
      <span className="mode-icon">{icon}</span>
      <span className="mode-copy">
        <strong>{option.title}</strong>
        <span>{option.route}</span>
        <small>{option.summary}</small>
      </span>
    </button>
  )
}
