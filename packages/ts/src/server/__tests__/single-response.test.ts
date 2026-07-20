import { describe, expect, it } from 'vitest'
import type { OnRequestHook, RequestContext } from '../../core/types.js'
import { initAbortHandler, readBody } from '../http-common.js'
import { runOnRequest } from '../request-hook.js'
import { createMockResponse, wait } from './helpers.js'

const ctx: RequestContext = {
  headers: {},
  method: 'post',
  path: '/db/main/execute',
  databaseId: 'main',
  remoteAddress: '127.0.0.1',
}

function denyAfterTick(): OnRequestHook {
  return async () => {
    await wait(1)
    return { status: 403, code: 'DENIED', message: 'Denied by policy' }
  }
}

function throwAfterTick(): OnRequestHook {
  return async () => {
    await wait(1)
    throw new Error('hook exploded')
  }
}

describe('one request answers exactly once', () => {
  it('keeps a slow denying hook quiet once the body reader has sent a 413', async () => {
    const mock = createMockResponse()
    const guard = initAbortHandler(mock.res)

    const body = readBody(mock.res, 3, guard)
    const hook = runOnRequest(mock.res, guard, ctx, denyAfterTick())
    mock.data('too long', true)

    await expect(body).rejects.toThrow('Payload too large')
    await expect(hook).resolves.toBe(false)

    expect(mock.state.ends).toBe(1)
    expect(mock.state.status).toBe('413')
  })

  it('keeps a slow throwing hook quiet once the body reader has sent a 413', async () => {
    const mock = createMockResponse()
    const guard = initAbortHandler(mock.res)

    const body = readBody(mock.res, 3, guard)
    const hook = runOnRequest(mock.res, guard, ctx, throwAfterTick())
    mock.data('too long', true)

    await expect(body).rejects.toThrow('Payload too large')
    await expect(hook).resolves.toBe(false)

    expect(mock.state.ends).toBe(1)
    expect(mock.state.status).toBe('413')
  })

  it('keeps the body reader quiet once a hook has already denied the request', async () => {
    const mock = createMockResponse()
    const guard = initAbortHandler(mock.res)

    const body = readBody(mock.res, 3, guard)
    await expect(runOnRequest(mock.res, guard, ctx, denyAfterTick())).resolves.toBe(false)
    mock.data('too long', true)

    await expect(body).rejects.toThrow('Payload too large')

    expect(mock.state.ends).toBe(1)
    expect(mock.state.status).toBe('403')
  })

  it('keeps the body reader quiet once a hook has already thrown', async () => {
    const mock = createMockResponse()
    const guard = initAbortHandler(mock.res)

    const body = readBody(mock.res, 3, guard)
    await expect(runOnRequest(mock.res, guard, ctx, throwAfterTick())).resolves.toBe(false)
    mock.data('too long', true)

    await expect(body).rejects.toThrow('Payload too large')

    expect(mock.state.ends).toBe(1)
    expect(mock.state.status).toBe('500')
  })

  it('says nothing at all when the client disconnects before either writer runs', async () => {
    const mock = createMockResponse()
    const guard = initAbortHandler(mock.res)

    const body = readBody(mock.res, 3, guard)
    const hook = runOnRequest(mock.res, guard, ctx, denyAfterTick())
    mock.abort()
    mock.data('too long', true)

    await expect(body).rejects.toThrow('Request aborted')
    await expect(hook).resolves.toBe(false)

    expect(mock.state.ends).toBe(0)
  })

  it('leaves the response unclaimed for a hook that allows the request through', async () => {
    const mock = createMockResponse()
    const guard = initAbortHandler(mock.res)

    const body = readBody(mock.res, 1024, guard)
    await expect(runOnRequest(mock.res, guard, ctx, () => undefined)).resolves.toBe(true)
    mock.data('{}', true)

    await expect(body).resolves.toEqual(Buffer.from('{}'))

    expect(mock.state.ends).toBe(0)
    expect(guard.claim()).toBe(true)
  })
})
