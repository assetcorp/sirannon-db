export function writeWithBackpressure<T>(
  stream: {
    write(msg: T): boolean
    once(event: string, listener: (...args: unknown[]) => void): unknown
    removeListener(event: string, listener: (...args: unknown[]) => void): unknown
  },
  message: T,
): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    const ok = stream.write(message)
    if (ok) {
      resolve()
      return
    }
    const onDrain = () => {
      stream.removeListener('error', onError)
      resolve()
    }
    const onError = (...args: unknown[]) => {
      stream.removeListener('drain', onDrain)
      const err = args[0]
      reject(err instanceof Error ? err : new Error(String(err)))
    }
    stream.once('drain', onDrain)
    stream.once('error', onError)
  })
}
