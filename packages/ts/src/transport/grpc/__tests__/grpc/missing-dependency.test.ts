import { afterEach, describe, expect, it, vi } from 'vitest'

const GRPC_PACKAGE_SPECIFIERS = ['@grpc/grpc-js', '@bufbuild/protobuf/wire', 'grpc-health-check']

afterEach(() => {
  for (const specifier of GRPC_PACKAGE_SPECIFIERS) {
    vi.doUnmock(specifier)
  }
  vi.resetModules()
})

describe('GrpcReplicationTransport without its gRPC packages', () => {
  it.each([
    ['@grpc/grpc-js', '@grpc/grpc-js'],
    ['@bufbuild/protobuf/wire', '@bufbuild/protobuf'],
    ['grpc-health-check', 'grpc-health-check'],
  ])('imports the transport and fails connect with TRANSPORT_DEPENDENCY_MISSING when %s is absent', async (specifier, packageName) => {
    vi.doMock(specifier, () => {
      throw new Error(`Cannot find package '${packageName}'`)
    })
    vi.resetModules()

    const { GrpcReplicationTransport } = await import('../../index.js')
    const transport = new GrpcReplicationTransport({ insecure: true })

    await expect(transport.connect('replica-eu-west-1', { localRole: 'replica' })).rejects.toMatchObject({
      code: 'TRANSPORT_DEPENDENCY_MISSING',
      message: expect.stringContaining(`needs the '${packageName}' package`),
    })
    expect(transport.connected).toBe(false)
  })
})
