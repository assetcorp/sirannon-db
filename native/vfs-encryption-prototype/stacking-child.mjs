import { readFileSync } from 'node:fs'
import { backup, DatabaseSync } from 'node:sqlite'

const [order, encryptionExtension, streamExtension, databasePath] = process.argv.slice(2)
const MARKER = 'sirannon-plaintext-marker'
const masterKey = Buffer.alloc(32, 9)

const scratch = new DatabaseSync(':memory:', { allowExtension: true })
scratch.enableLoadExtension(true)
const extensions = order === 'encryption-first' ? [encryptionExtension, streamExtension] : [streamExtension, encryptionExtension]
for (const extension of extensions) scratch.loadExtension(extension)
scratch.prepare('select sirannon_encryption_prototype_key(?, ?, ?)').get(databasePath, masterKey, 'stack')

const db = new DatabaseSync(databasePath)
db.exec(`pragma temp_store=memory; pragma journal_mode=wal; create table t(id integer primary key, body text)`)
const insert = db.prepare('insert into t(body) values (?)')
for (let i = 0; i < 200; i++) insert.run(`${MARKER}-${i}`)
db.exec('pragma wal_checkpoint(truncate)')


const streamId = scratch.prepare('select sirannon_stream_open(4096, 0, 0, 0) id').get().id
const pages = await backup(db, `file:sirannon-stream-${streamId}?vfs=sirannon`, { rate: 16 })
scratch.prepare('select sirannon_stream_finish(?)').get(streamId)
const pieces = []
for (;;) {
  const row = scratch.prepare('select sirannon_stream_take(?) piece').get(streamId)
  if (!row.piece) break
  const framed = Buffer.from(row.piece)
  pieces.push({ index: framed.readUInt32LE(0), bytes: framed.subarray(8) })
}
const streamed = Buffer.concat(pieces.sort((a, b) => a.index - b.index).map(piece => piece.bytes))
const file = readFileSync(databasePath)
db.close()
console.log(
  JSON.stringify({
    order,
    vfsBelowEncryptionShim: scratch.prepare('select sirannon_encryption_prototype_vfs_below() below').get().below,

    pagesCopied: pages,
    streamedBytes: streamed.length,
    streamedHoldsPlaintext: streamed.includes(MARKER),
    streamedReservedByte: streamed[20],
    fileHoldsPlaintext: file.includes(MARKER),
  }),
)
