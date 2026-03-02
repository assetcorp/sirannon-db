export interface Engine {
  name: 'sirannon' | 'postgres'
  setup(schemaSql: string): Promise<void>
  seed(insertSql: string, rows: unknown[][]): Promise<void>
  cleanup(): Promise<void>
  getInfo(): Promise<Record<string, string>>
}
