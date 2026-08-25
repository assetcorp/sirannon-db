import { toSubprotocolCredential } from '@delali/sirannon-db'
import { SirannonClient } from '@delali/sirannon-db/client'
import { DATABASE_ID, DEFAULT_DATA_ENDPOINT, WAREHOUSE_DEMO_TOKEN, WEBSOCKET_AUTH_PROTOCOL_PREFIX } from './demo-config'

const DATA_ENDPOINT = import.meta.env.VITE_SIRANNON_ENDPOINT ?? DEFAULT_DATA_ENDPOINT
const DEMO_TOKEN = import.meta.env.VITE_SIRANNON_DEMO_TOKEN ?? WAREHOUSE_DEMO_TOKEN

const client = new SirannonClient(DATA_ENDPOINT, {
  transport: 'websocket',
  webSocketProtocols: [toSubprotocolCredential(WEBSOCKET_AUTH_PROTOCOL_PREFIX, DEMO_TOKEN)],
})

export const liveDatabase = client.database(DATABASE_ID)
