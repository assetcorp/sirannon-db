import { execFile } from 'node:child_process'
import { createServer } from 'node:net'
import { promisify } from 'node:util'

const execFileAsync = promisify(execFile)

export interface DockerRunResult {
  stdout: string
  stderr: string
}

export interface EtcdClusterHandle {
  networkName: string
  containerNames: string[]
  clientPorts: number[]
  endpoints: string[]
}

export interface ToxiproxyContainerHandle {
  containerName: string
  apiPort: number
}

const ETCD_IMAGE = 'gcr.io/etcd-development/etcd:v3.6.0'
const TOXIPROXY_IMAGE = 'ghcr.io/shopify/toxiproxy:2.12.0'

export async function docker(args: string[], timeoutMs = 60_000): Promise<DockerRunResult> {
  try {
    const { stdout, stderr } = await execFileAsync('docker', args, {
      timeout: timeoutMs,
      maxBuffer: 1024 * 1024 * 8,
    })
    return { stdout, stderr }
  } catch (err: unknown) {
    const details = commandErrorDetails(err)
    throw new Error(`docker ${args.join(' ')} failed${details}`)
  }
}

export async function ensureFailoverImages(): Promise<void> {
  await ensureImage(ETCD_IMAGE)
  await ensureImage(TOXIPROXY_IMAGE)
}

export async function startEtcdCluster(namePrefix: string): Promise<EtcdClusterHandle> {
  const networkName = `${namePrefix}-etcd-net`
  const clientPorts = await allocatePorts(3)
  const containerNames = ['etcd1', 'etcd2', 'etcd3'].map(name => `${namePrefix}-${name}`)
  const initialCluster = containerNames
    .map((containerName, index) => `etcd${index + 1}=http://${containerName}:2380`)
    .join(',')

  await docker(['network', 'create', networkName])

  for (let index = 0; index < containerNames.length; index++) {
    const containerName = containerNames[index]
    const clientPort = clientPorts[index]
    if (!containerName || clientPort === undefined) {
      throw new Error('Invalid etcd cluster allocation')
    }

    await docker(
      [
        'run',
        '--detach',
        '--rm',
        '--name',
        containerName,
        '--network',
        networkName,
        '-p',
        `127.0.0.1:${clientPort}:2379`,
        ETCD_IMAGE,
        '/usr/local/bin/etcd',
        '--name',
        `etcd${index + 1}`,
        '--data-dir',
        '/etcd-data',
        '--listen-client-urls',
        'http://0.0.0.0:2379',
        '--advertise-client-urls',
        `http://${containerName}:2379`,
        '--listen-peer-urls',
        'http://0.0.0.0:2380',
        '--initial-advertise-peer-urls',
        `http://${containerName}:2380`,
        '--initial-cluster',
        initialCluster,
        '--initial-cluster-state',
        'new',
        '--initial-cluster-token',
        namePrefix,
      ],
      30_000,
    )
  }

  const endpoints = clientPorts.map(port => `http://127.0.0.1:${port}`)
  await Promise.all(endpoints.map(endpoint => waitForHttpOk(`${endpoint}/health`, 30_000)))
  return { networkName, containerNames, clientPorts, endpoints }
}

export async function startToxiproxyContainer(
  namePrefix: string,
  proxyPorts: number[],
): Promise<ToxiproxyContainerHandle> {
  const apiPort = await allocatePort()
  const containerName = `${namePrefix}-toxiproxy`
  const portArgs = ['-p', `127.0.0.1:${apiPort}:8474`]
  for (const port of proxyPorts) {
    portArgs.push('-p', `127.0.0.1:${port}:${port}`)
  }

  await docker(['run', '--detach', '--rm', '--name', containerName, ...portArgs, TOXIPROXY_IMAGE], 30_000)
  await waitForHttpOk(`http://127.0.0.1:${apiPort}/version`, 30_000)
  return { containerName, apiPort }
}

export async function cleanupDockerResources(handles: { containers: string[]; networks: string[] }): Promise<void> {
  if (handles.containers.length > 0) {
    await docker(['rm', '-f', ...handles.containers], 30_000).catch(() => undefined)
  }
  for (const network of handles.networks) {
    await docker(['network', 'rm', network], 30_000).catch(() => undefined)
  }
}

export async function allocatePorts(count: number): Promise<number[]> {
  const ports: number[] = []
  for (let index = 0; index < count; index++) {
    ports.push(await allocatePort())
  }
  return ports
}

export async function allocatePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const server = createServer()
    server.on('error', reject)
    server.listen(0, '127.0.0.1', () => {
      const address = server.address()
      server.close(closeErr => {
        if (closeErr) {
          reject(closeErr)
          return
        }
        if (typeof address === 'object' && address !== null) {
          resolve(address.port)
          return
        }
        reject(new Error('Could not allocate a loopback port'))
      })
    })
  })
}

export function failoverRunPrefix(): string {
  const suffix = `${process.pid}-${Date.now().toString(36)}`
  return `sirannon-failover-${suffix}`
}

async function ensureImage(image: string): Promise<void> {
  const images = await docker(['images', '--format', '{{.Repository}}:{{.Tag}}'], 15_000)
  const localImages = new Set(images.stdout.split(/\r?\n/).filter(line => line.trim() !== ''))
  if (!localImages.has(image)) {
    await docker(['pull', image], 180_000)
  }
}

async function waitForHttpOk(url: string, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs
  let lastError: Error | null = null

  while (Date.now() < deadline) {
    try {
      const response = await fetch(url)
      if (response.ok) return
      lastError = new Error(`${url} returned ${response.status}`)
    } catch (err: unknown) {
      lastError = err instanceof Error ? err : new Error(String(err))
    }
    await sleep(100)
  }

  throw lastError ?? new Error(`Timed out waiting for ${url}`)
}

function commandErrorDetails(err: unknown): string {
  if (typeof err !== 'object' || err === null) {
    return `: ${String(err)}`
  }
  const error = err as {
    message?: string
    stdout?: string | Buffer
    stderr?: string | Buffer
  }
  const parts = [error.message, toText(error.stdout), toText(error.stderr)].filter(part => part && part.trim() !== '')
  return parts.length > 0 ? `: ${parts.join('\n')}` : ''
}

function toText(value: string | Buffer | undefined): string {
  if (value === undefined) return ''
  return Buffer.isBuffer(value) ? value.toString('utf8') : value
}

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => {
    const timer = setTimeout(resolve, ms) as ReturnType<typeof setTimeout> & {
      unref?: () => void
    }
    timer.unref?.()
  })
}
