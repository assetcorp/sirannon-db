import { cn } from '@delali/sirannon-example-shared/lib/utils'
import { type KeyboardEvent, type PointerEvent, useCallback, useState } from 'react'
import type { ConsoleHeight } from '../use-console-height'

const KEYBOARD_STEP_PX = 32

export function ConsoleResizer({ height, minHeight, maxHeight, setHeight }: ConsoleHeight) {
  const [dragging, setDragging] = useState(false)

  const handlePointerDown = useCallback((event: PointerEvent<HTMLHRElement>) => {
    if (event.button !== 0) return
    event.preventDefault()
    event.currentTarget.setPointerCapture(event.pointerId)
    setDragging(true)
  }, [])

  const handlePointerMove = useCallback(
    (event: PointerEvent<HTMLHRElement>) => {
      if (!event.currentTarget.hasPointerCapture(event.pointerId)) return
      setHeight(window.innerHeight - event.clientY)
    },
    [setHeight],
  )

  const handlePointerUp = useCallback((event: PointerEvent<HTMLHRElement>) => {
    if (event.currentTarget.hasPointerCapture(event.pointerId)) {
      event.currentTarget.releasePointerCapture(event.pointerId)
    }
    setDragging(false)
  }, [])

  const handleKeyDown = useCallback(
    (event: KeyboardEvent<HTMLHRElement>) => {
      if (event.key === 'ArrowUp') {
        event.preventDefault()
        setHeight(height + KEYBOARD_STEP_PX)
        return
      }
      if (event.key === 'ArrowDown') {
        event.preventDefault()
        setHeight(height - KEYBOARD_STEP_PX)
        return
      }
      if (event.key === 'Home') {
        event.preventDefault()
        setHeight(maxHeight)
        return
      }
      if (event.key === 'End') {
        event.preventDefault()
        setHeight(minHeight)
      }
    },
    [height, maxHeight, minHeight, setHeight],
  )

  return (
    <div className="absolute inset-x-0 -top-2 z-10 h-4">
      <hr
        aria-label="Resize the SQL console"
        aria-valuenow={height}
        aria-valuemin={minHeight}
        aria-valuemax={maxHeight}
        tabIndex={0}
        onPointerDown={handlePointerDown}
        onPointerMove={handlePointerMove}
        onPointerUp={handlePointerUp}
        onPointerCancel={handlePointerUp}
        onKeyDown={handleKeyDown}
        className="peer focus-visible:outline-ring absolute inset-0 m-0 h-full cursor-ns-resize touch-none border-0 bg-transparent outline-none focus-visible:outline-2"
      />
      <span
        aria-hidden="true"
        className={cn(
          'bg-border peer-hover:bg-primary peer-focus-visible:bg-primary pointer-events-none absolute top-1.5 left-1/2 h-1 w-14 -translate-x-1/2 rounded-full transition-colors',
          dragging && 'bg-primary',
        )}
      />
    </div>
  )
}
