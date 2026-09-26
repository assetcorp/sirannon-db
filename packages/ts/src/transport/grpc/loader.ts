import { type OptionalDependency, optionalDependencyError } from '../../core/optional-dependency.js'
import type { connectToEndpoint } from './client-streams.js'
import type { forwardOverRpc } from './forward-rpc.js'
import type { startServer } from './server-streams.js'

export interface GrpcRuntime {
  startServer: typeof startServer
  connectToEndpoint: typeof connectToEndpoint
  forwardOverRpc: typeof forwardOverRpc
}

const GRPC_INSTALL_COMMAND = 'pnpm add -E @grpc/grpc-js @bufbuild/protobuf grpc-health-check'

function grpcDependency(packageName: string): OptionalDependency {
  return {
    packageName,
    neededBy: 'The gRPC replication transport',
    installCommand: GRPC_INSTALL_COMMAND,
    code: 'TRANSPORT_DEPENDENCY_MISSING',
  }
}

async function requirePackage(packageName: string, load: () => Promise<unknown>): Promise<void> {
  try {
    await load()
  } catch (err) {
    throw optionalDependencyError(grpcDependency(packageName), err)
  }
}

export async function loadGrpcRuntime(): Promise<GrpcRuntime> {
  await requirePackage('@grpc/grpc-js', () => import('@grpc/grpc-js'))
  await requirePackage('@bufbuild/protobuf', () => import('@bufbuild/protobuf/wire'))
  await requirePackage('grpc-health-check', () => import('grpc-health-check'))

  const [serverStreams, clientStreams, forwardRpc] = await Promise.all([
    import('./server-streams.js'),
    import('./client-streams.js'),
    import('./forward-rpc.js'),
  ])
  return {
    startServer: serverStreams.startServer,
    connectToEndpoint: clientStreams.connectToEndpoint,
    forwardOverRpc: forwardRpc.forwardOverRpc,
  }
}
