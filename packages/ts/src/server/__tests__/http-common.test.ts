import { describe, expect, it } from 'vitest'
import { initAbortHandler, readBody } from '../http-handler.js'
import { createMockResponse } from './helpers.js'

describe('request abort handling', () => {
  it('notifies registered abort listeners and immediately invokes late listeners', () => {
    const mock = createMockResponse()
    const abort = initAbortHandler(mock.res)

    let firstCount = 0
    abort.onAbort(() => {
      firstCount++
    })

    mock.abort()
    expect(firstCount).toBe(1)
    expect(abort.aborted).toBe(true)

    let lateCount = 0
    abort.onAbort(() => {
      lateCount++
    })
    expect(lateCount).toBe(1)
  })
})

describe('readBody', () => {
  it('rejects when already aborted before reading starts', async () => {
    const mock = createMockResponse()
    const abort = {
      aborted: true,
      onAbort: () => {},
      claim: () => false,
    }

    await expect(readBody(mock.res, 1024, abort)).rejects.toThrow('Request aborted')
  })

  it('rejects when aborted after listener registration', async () => {
    const mock = createMockResponse()
    const abort = initAbortHandler(mock.res)
    const pending = readBody(mock.res, 1024, abort)

    mock.abort()

    await expect(pending).rejects.toThrow('Request aborted')
  })

  it('returns 413 and rejects when payload exceeds limit', async () => {
    const mock = createMockResponse()
    const abort = initAbortHandler(mock.res)
    const pending = readBody(mock.res, 3, abort)

    mock.data('abcd', true)

    await expect(pending).rejects.toThrow('Payload too large')
    expect(mock.state.status).toBe('413')
    const body = JSON.parse(mock.state.body ?? '{}') as { error?: { code?: string } }
    expect(body.error?.code).toBe('PAYLOAD_TOO_LARGE')
  })

  it('resolves from multiple chunks', async () => {
    const mock = createMockResponse()
    const abort = initAbortHandler(mock.res)
    const pending = readBody(mock.res, 1024, abort)

    mock.data('ab', false)
    mock.data('cd', true)

    await expect(pending).resolves.toEqual(Buffer.from('abcd'))
  })

  it('ignores abort notifications after completion', async () => {
    const mock = createMockResponse()
    const abort = initAbortHandler(mock.res)
    const pending = readBody(mock.res, 1024, abort)

    mock.data('done', true)
    await expect(pending).resolves.toEqual(Buffer.from('done'))

    expect(() => mock.abort()).not.toThrow()
  })

  it('ignores additional data events after completion', async () => {
    const mock = createMockResponse()
    const abort = initAbortHandler(mock.res)
    const pending = readBody(mock.res, 1024, abort)

    mock.data('done', true)
    mock.data('ignored', true)

    await expect(pending).resolves.toEqual(Buffer.from('done'))
  })

  it('skips sending 413 when the response is already spoken for', async () => {
    const mock = createMockResponse()
    const abort = {
      aborted: false,
      onAbort: () => {},
      claim: () => false,
    }

    const pending = readBody(mock.res, 3, abort)
    mock.data('abcd', true)

    await expect(pending).rejects.toThrow('Payload too large')
    expect(mock.state.status).toBeUndefined()
  })

  it('claims the response exactly once for the 413', async () => {
    const mock = createMockResponse()
    const abort = initAbortHandler(mock.res)
    const pending = readBody(mock.res, 3, abort)

    mock.data('abcd', true)

    await expect(pending).rejects.toThrow('Payload too large')
    expect(mock.state.status).toBe('413')
    expect(abort.claim()).toBe(false)
  })
})
