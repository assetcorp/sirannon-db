type RunExclusive = (op: () => Promise<void>) => Promise<void>

/**
 * Starts a copy while no other operation holds the writer, and releases the
 * writer as soon as the first step of the copy completes. SQLite copies no
 * pages while a transaction is open on the source connection, although the
 * copy can continue through a transaction that opens after it has begun, so
 * the copy needs the writer only for its first step.
 *
 * @param runExclusive - Runs an operation while no other operation holds the writer.
 * @param start - Starts the copy, and calls the callback that it receives once the first step completes.
 * @returns The result of the copy.
 */
export async function startCopyWithoutHoldingWriter<T>(
  runExclusive: RunExclusive,
  start: (onFirstStep: () => void) => Promise<T>,
): Promise<T> {
  let begin!: () => void
  let release!: () => void
  const writerHeld = new Promise<void>(resolve => {
    begin = resolve
  })
  const firstStepDone = new Promise<void>(resolve => {
    release = resolve
  })

  const run = (async () => {
    await writerHeld
    return start(release)
  })()
  run.then(release, release)

  await runExclusive(async () => {
    begin()
    await firstStepDone
  })
  return run
}
