import type { InventorySnapshot } from '../../lib/schemas'

export type DemoMode = 'app-actions' | 'driver-access'
export type ConnectionState = 'connecting' | 'live' | 'offline'

export interface LoaderData extends InventorySnapshot {
  initialError: string | null
}

export interface ModeOptionData {
  mode: DemoMode
  title: string
  route: string
  summary: string
}

export interface ProductStats {
  totalProducts: number
  totalStock: number
  lowStock: number
}

export interface ProductFormState {
  name: string
  price: string
  stock: string
}

export const EMPTY_FORM: ProductFormState = {
  name: '',
  price: '',
  stock: '',
}

export const EMPTY_SNAPSHOT: InventorySnapshot = {
  products: [],
  activity: [],
}

export const RECEIVE_QUANTITY = 10

export const MODE_OPTIONS: ModeOptionData[] = [
  {
    mode: 'app-actions',
    title: 'Application API',
    route: 'Browser -> App server -> Sirannon',
    summary: 'Validated domain actions on the server with live subscriptions in the browser.',
  },
  {
    mode: 'driver-access',
    title: 'Direct Data API',
    route: 'Browser -> Sirannon HTTP',
    summary: 'The browser uses the client driver directly against the demo allowlist.',
  },
]
