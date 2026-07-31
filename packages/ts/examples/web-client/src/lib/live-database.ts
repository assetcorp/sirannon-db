import { SirannonClient } from '@delali/sirannon-db/client'
import { DATABASE_ID, DEFAULT_DATA_ENDPOINT, toWebSocketAuthProtocol, WAREHOUSE_DEMO_TOKEN } from './demo-config'

const DATA_ENDPOINT = import.meta.env.VITE_SIRANNON_ENDPOINT ?? DEFAULT_DATA_ENDPOINT
const DEMO_TOKEN = import.meta.env.VITE_SIRANNON_DEMO_TOKEN ?? WAREHOUSE_DEMO_TOKEN

const client = new SirannonClient(DATA_ENDPOINT, {
  transport: 'websocket',
  webSocketProtocols: [toWebSocketAuthProtocol(DEMO_TOKEN)],
})

export const liveDatabase = client.database(DATABASE_ID)
