export function progress(message: string): void {
  console.log(`[${new Date().toISOString().slice(11, 19)}] ${message}`)
}
