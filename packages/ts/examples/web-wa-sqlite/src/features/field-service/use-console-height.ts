import { useCallback, useEffect, useState } from 'react'

const MIN_CONSOLE_HEIGHT_PX = 224
const MAX_CONSOLE_HEIGHT_RATIO = 0.9
const DEFAULT_CONSOLE_HEIGHT_RATIO = 0.45
const PRERENDER_VIEWPORT_HEIGHT_PX = 800

export type ConsoleHeight = {
  height: number
  minHeight: number
  maxHeight: number
  setHeight: (next: number) => void
}

function viewportHeight(): number {
  return typeof window === 'undefined' ? PRERENDER_VIEWPORT_HEIGHT_PX : window.innerHeight
}

function largestAllowedHeight(): number {
  return Math.max(MIN_CONSOLE_HEIGHT_PX, Math.round(viewportHeight() * MAX_CONSOLE_HEIGHT_RATIO))
}

function startingHeight(): number {
  return Math.max(MIN_CONSOLE_HEIGHT_PX, Math.round(viewportHeight() * DEFAULT_CONSOLE_HEIGHT_RATIO))
}

export function useConsoleHeight(): ConsoleHeight {
  const [maxHeight, setMaxHeight] = useState(largestAllowedHeight)
  const [height, setStoredHeight] = useState(startingHeight)

  useEffect(() => {
    const handleViewportResize = () => {
      const allowed = largestAllowedHeight()
      setMaxHeight(allowed)
      setStoredHeight(current => Math.min(current, allowed))
    }
    handleViewportResize()
    window.addEventListener('resize', handleViewportResize)
    return () => {
      window.removeEventListener('resize', handleViewportResize)
    }
  }, [])

  const setHeight = useCallback(
    (next: number) => {
      setStoredHeight(Math.min(Math.max(Math.round(next), MIN_CONSOLE_HEIGHT_PX), maxHeight))
    },
    [maxHeight],
  )

  return { height, minHeight: MIN_CONSOLE_HEIGHT_PX, maxHeight, setHeight }
}
