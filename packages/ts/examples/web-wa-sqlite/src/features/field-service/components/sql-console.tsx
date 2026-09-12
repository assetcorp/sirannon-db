import { Button } from '@delali/sirannon-example-shared/ui/button'
import { Play, Terminal, X } from 'lucide-react'
import { type ChangeEvent, type KeyboardEvent, useCallback, useEffect, useRef, useState } from 'react'
import type { FieldDevice } from '../../../lib/field-device'
import { runStatement, type StatementResult } from '../../../lib/sql-console'
import type { ConsoleHeight } from '../use-console-height'
import { ConsoleResizer } from './console-resizer'
import { SqlConsoleResults } from './sql-console-results'

const STARTER_STATEMENTS: readonly { label: string; sql: string }[] = [
  { label: 'Count by status', sql: 'SELECT status, count(*) AS orders FROM work_orders GROUP BY status' },
  { label: 'Every row', sql: 'SELECT * FROM work_orders ORDER BY site' },
  {
    label: 'Add one',
    sql: `INSERT INTO work_orders (id, site, task, updated_at)
VALUES (hex(randomblob(8)), 'Console yard', 'Written from the SQL console', datetime('now'))`,
  },
]

function StarterButton({ label, sql, onPick }: { label: string; sql: string; onPick: (sql: string) => void }) {
  const handleClick = useCallback(() => {
    onPick(sql)
  }, [onPick, sql])

  return (
    <Button variant="ghost" size="xs" className="text-muted-foreground font-mono" onClick={handleClick}>
      {label}
    </Button>
  )
}

function summarise(result: StatementResult): string {
  const elapsed = `${result.elapsedMs.toFixed(1)} ms`
  if (result.error !== null) return `failed in ${elapsed}`
  if (result.changes !== null) return `${result.changes} changed in ${elapsed}`
  return `${result.rowCount} ${result.rowCount === 1 ? 'row' : 'rows'} in ${elapsed}`
}

export function SqlConsole({
  device,
  open,
  onClose,
  consoleHeight,
}: {
  device: FieldDevice
  open: boolean
  onClose: () => void
  consoleHeight: ConsoleHeight
}) {
  const editorRef = useRef<HTMLTextAreaElement>(null)
  const [sql, setSql] = useState('SELECT * FROM work_orders ORDER BY site')
  const [result, setResult] = useState<StatementResult | null>(null)
  const [running, setRunning] = useState(false)
  const [history, setHistory] = useState<readonly string[]>([])
  const [historyIndex, setHistoryIndex] = useState<number | null>(null)
  const draftRef = useRef('')

  useEffect(() => {
    if (open) editorRef.current?.focus()
  }, [open])

  const handleRun = useCallback(() => {
    if (running) return
    setRunning(true)
    setHistoryIndex(null)
    void runStatement(device.db, sql).then(outcome => {
      setResult(outcome)
      setRunning(false)
      if (outcome.sql !== '') {
        setHistory(entries => (entries[entries.length - 1] === outcome.sql ? entries : [...entries, outcome.sql]))
      }
    })
  }, [device.db, running, sql])

  const recall = useCallback(
    (step: number) => {
      if (history.length === 0) return
      const current = historyIndex ?? history.length
      const next = Math.min(Math.max(current + step, 0), history.length)
      if (next === current) return
      if (historyIndex === null) draftRef.current = sql
      setHistoryIndex(next === history.length ? null : next)
      setSql(next === history.length ? draftRef.current : (history[next] ?? ''))
    },
    [history, historyIndex, sql],
  )

  const handleKeyDown = useCallback(
    (event: KeyboardEvent<HTMLTextAreaElement>) => {
      if (event.key === 'Enter' && (event.metaKey || event.ctrlKey)) {
        event.preventDefault()
        handleRun()
        return
      }
      if (event.key === 'Escape') {
        event.preventDefault()
        onClose()
        return
      }
      const caret = event.currentTarget.selectionStart
      if (event.key === 'ArrowUp' && caret === 0) {
        event.preventDefault()
        recall(-1)
        return
      }
      if (event.key === 'ArrowDown' && caret === event.currentTarget.value.length) {
        event.preventDefault()
        recall(1)
      }
    },
    [handleRun, onClose, recall],
  )

  const handleChange = useCallback((event: ChangeEvent<HTMLTextAreaElement>) => {
    setSql(event.target.value)
  }, [])

  if (!open) return null

  return (
    <section
      aria-label="SQL console"
      style={{ height: consoleHeight.height }}
      className="bg-background/95 border-border fixed inset-x-0 bottom-0 z-30 flex flex-col border-t shadow-2xl backdrop-blur"
    >
      <ConsoleResizer {...consoleHeight} />
      <header className="border-border/60 flex h-9 shrink-0 items-center gap-2 border-b px-3">
        <Terminal className="text-muted-foreground size-3.5" aria-hidden="true" />
        <span className="text-xs font-semibold">SQL console</span>
        <span className="text-muted-foreground font-mono text-xs">{`${device.name} · local database`}</span>
        <Button variant="ghost" size="icon-xs" className="ml-auto" aria-label="Close the console" onClick={onClose}>
          <X aria-hidden="true" />
        </Button>
      </header>

      <div className="border-border/60 shrink-0 border-b">
        <textarea
          ref={editorRef}
          value={sql}
          spellCheck={false}
          onChange={handleChange}
          onKeyDown={handleKeyDown}
          aria-label="SQL statement"
          className="h-24 w-full resize-none bg-transparent px-3 py-2 font-mono text-xs leading-relaxed outline-none"
        />
        <div className="flex flex-wrap items-center gap-1 px-3 pb-2">
          <Button size="xs" onClick={handleRun} disabled={running}>
            <Play aria-hidden="true" />
            Run
          </Button>
          {STARTER_STATEMENTS.map(starter => (
            <StarterButton key={starter.label} label={starter.label} sql={starter.sql} onPick={setSql} />
          ))}
          <span className="text-muted-foreground ml-auto font-mono text-xs tabular-nums">
            {result === null ? '' : summarise(result)}
          </span>
        </div>
      </div>

      <div className="min-h-0 flex-1 overflow-auto">
        <SqlConsoleResults result={result} />
      </div>
    </section>
  )
}
