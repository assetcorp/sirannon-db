import { cn } from '@delali/sirannon-example-shared/lib/utils'
import { CircleCheck, TriangleAlert } from 'lucide-react'
import { MAX_DISPLAYED_ROWS, type StatementResult } from '../../../lib/sql-console'

function Notice({ tone, children }: { tone: 'error' | 'ok'; children: string }) {
  const Icon = tone === 'error' ? TriangleAlert : CircleCheck
  return (
    <p
      className={cn(
        'flex items-start gap-2 px-4 py-3 font-mono text-xs',
        tone === 'error' ? 'text-destructive' : 'text-success',
      )}
    >
      <Icon className="mt-px size-3.5 shrink-0" aria-hidden="true" />
      <span className="whitespace-pre-wrap">{children}</span>
    </p>
  )
}

export function SqlConsoleResults({ result }: { result: StatementResult | null }) {
  if (result === null) {
    return (
      <div className="text-muted-foreground px-4 py-3 font-mono text-xs">
        <p>work_orders(id, site, task, status, technician, note, updated_at)</p>
        <p className="mt-2">Press Ctrl+Enter or Cmd+Enter to run one statement.</p>
      </div>
    )
  }

  if (result.error !== null) {
    return <Notice tone="error">{result.error}</Notice>
  }

  if (result.changes !== null) {
    return <Notice tone="ok">{`${result.changes} ${result.changes === 1 ? 'row' : 'rows'} changed`}</Notice>
  }

  if (result.rowCount === 0) {
    return <Notice tone="ok">0 rows</Notice>
  }

  return (
    <div className="overflow-auto">
      <table className="w-full border-collapse text-left font-mono text-xs">
        <thead className="bg-muted/70 text-muted-foreground sticky top-0 backdrop-blur">
          <tr>
            {result.columns.map(column => (
              <th key={column} className="border-border/60 border-b px-3 py-1.5 font-medium whitespace-nowrap">
                {column}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {result.rows.map((row, rowIndex) => (
            <tr key={`${rowIndex}:${row.join('')}`} className="hover:bg-muted/40">
              {row.map((cell, cellIndex) => (
                <td
                  key={result.columns[cellIndex] ?? String(cellIndex)}
                  className={cn(
                    'border-border/40 max-w-80 truncate border-b px-3 py-1.5',
                    cell === 'NULL' && 'text-muted-foreground italic',
                  )}
                  title={cell}
                >
                  {cell}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
      {result.rowCount > MAX_DISPLAYED_ROWS ? (
        <p className="text-muted-foreground px-3 py-2 font-mono text-xs">
          {`Showing the first ${MAX_DISPLAYED_ROWS} of ${result.rowCount} rows.`}
        </p>
      ) : null}
    </div>
  )
}
