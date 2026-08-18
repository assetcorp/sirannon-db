type RunExclusive = (op: () => Promise<void>) => Promise<void>

/**
 * Starts a copy while nothing else holds the writer, and hands the writer back
 * as soon as the copy's first step is done. SQLite copies no pages at all when
 * a transaction is already open on the source connection, and it handles a
 * transaction that opens once the copy is under way, so the copy needs the
 * writer only to start.
 *
 * @param runExclusive - Runs an operation with nothing else holding the writer.
 * @param start - Starts the copy, and calls the callback it receives once the first step is done.
 * @returns What the copy produced.
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
