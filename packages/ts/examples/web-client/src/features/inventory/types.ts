export type WriteMode = 'app-server' | 'browser-direct'
export type ConnectionState = 'connecting' | 'live' | 'offline'

export interface ModeOptionData {
  mode: WriteMode
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

export const RECEIVE_QUANTITY = 10

export const MODE_OPTIONS: ModeOptionData[] = [
  {
    mode: 'app-server',
    title: 'Write through the app server',
    route: 'Browser -> App server -> Sirannon HTTP',
    summary: 'A server function validates the input, then calls the registered write over HTTP.',
  },
  {
    mode: 'browser-direct',
    title: 'Write from the browser',
    route: 'Browser -> Sirannon WebSocket',
    summary: 'The browser calls the same registered write over the socket its live queries run on.',
  },
]
