export const existsSync = () => false
export const mkdirSync = () => {}
export const rmSync = () => {}
export const readdirSync = () => []
export const readFileSync = () => ''
export const writeFileSync = () => {}
export const lstatSync = () => ({})
export const statSync = () => ({})
export const resolve = (...args: string[]) => args.join('/')
export const dirname = (p: string) => p
export const join = (...args: string[]) => args.join('/')
export const basename = (p: string) => p
export const type = () => 'browser'
export const release = () => '0.0.0'
export const tmpdir = () => '/tmp'
export const cpus = () => []
export const totalmem = () => 0
export default {}
