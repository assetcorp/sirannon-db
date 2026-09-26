export function unrefTimer(timer: ReturnType<typeof setInterval>): void {
  const unref = (timer as { unref?: () => void }).unref
  unref?.call(timer)
}
