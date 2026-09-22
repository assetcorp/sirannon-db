import type { Server } from '@grpc/grpc-js'

export const GRACEFUL_SHUTDOWN_DEADLINE_MS = 2_000

export function shutdownGrpcServer(server: Server, deadlineMs: number = GRACEFUL_SHUTDOWN_DEADLINE_MS): Promise<void> {
  return new Promise<void>(resolve => {
    let settled = false
    const finish = () => {
      if (settled) return
      settled = true
      clearTimeout(deadline)
      resolve()
    }

    const deadline = setTimeout(() => {
      server.forceShutdown()
      finish()
    }, deadlineMs)

    server.tryShutdown(err => {
      if (err) {
        server.forceShutdown()
      }
      finish()
    })
  })
}
