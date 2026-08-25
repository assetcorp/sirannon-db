import { spawnSync } from 'node:child_process'
import { copyFileSync, existsSync, mkdtempSync, readFileSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'
import { backup, DatabaseSync } from 'node:sqlite'
import { fileURLToPath } from 'node:url'
import { build } from './build.mjs'
import {
  decryptPage,
  KEY_RECORD_BYTES,
  NONCE_BYTES,
  PAGE_RESERVED_BYTES,
  PLAINTEXT_HEADER_BYTES,
  readKeyRecord,
  TAG_BYTES,
  unwrapDataKey,
  walkLog,
} from './inspect.mjs'

const here = dirname(fileURLToPath(import.meta.url))
const repo = join(here, '..', '..')
const streamExtension = join(repo, 'native', 'npm', 'darwin-arm64', 'sirannonvfs.dylib')
const walFormat = await import(join(repo, 'packages', 'ts', 'src', 'core', 'backup', 'wal-format.ts'))
const MARKER = 'sirannon-plaintext-marker'
const ROWS = 300
const masterKey = Buffer.alloc(32, 5)

const extension = build()
if (!existsSync(streamExtension)) {
  spawnSync('pnpm', ['build:vfs'], { cwd: join(repo, 'packages', 'ts'), stdio: 'inherit' })
}
const dir = mkdtempSync(join(tmpdir(), 'sirannon-encryption-prototype-'))
const mainPath = join(dir, 'main.db')
const copyPath = join(dir, 'copy.db')
const crashPath = join(dir, 'crash.db')
const plainCopyPath = join(dir, 'plain-copy.db')

const scratch = new DatabaseSync(':memory:', { allowExtension: true })
scratch.enableLoadExtension(true)
scratch.loadExtension(extension)
const registerKey = scratch.prepare('select sirannon_encryption_prototype_key(?, ?, ?) path')
for (const path of [mainPath, copyPath, crashPath]) registerKey.get(path, masterKey, 'prototype-master')

function report(title, facts) {
  console.log(`\n${title}`)
  for (const [name, value] of Object.entries(facts)) console.log(`  ${name}: ${JSON.stringify(value)}`)
}

function openWithoutShim(path) {
  const child = spawnSync(
    process.execPath,
    ['-e', `const {DatabaseSync}=require('node:sqlite'); try { const d=new DatabaseSync(${JSON.stringify(path)}); console.log(JSON.stringify(d.prepare('select count(*) c from t').get())) } catch (e) { console.log('ERROR ' + e.message) }`],
    { encoding: 'utf8' },
  )
  return child.stdout.trim()
}

const db = new DatabaseSync(mainPath)
db.exec('pragma temp_store=memory; pragma wal_autocheckpoint=0')
const journalMode = db.prepare('pragma journal_mode=wal').get().journal_mode
db.exec('create table t(id integer primary key, body text)')
const insert = db.prepare('insert into t(body) values (?)')
db.exec('begin')
for (let i = 0; i < ROWS; i++) insert.run(`${MARKER}-${i}`)
db.exec('commit')
for (let i = 0; i < 5; i++) db.prepare('update t set body = body || ? where id = ?').run('-again', i + 1)

const mainBytesWhileOpen = readFileSync(mainPath)
const walBytes = readFileSync(`${mainPath}-wal`)
copyFileSync(mainPath, crashPath)
copyFileSync(`${mainPath}-wal`, `${crashPath}-wal`)
const crash = new DatabaseSync(crashPath)
const crashRows = crash.prepare('select count(*) c, max(body) last from t').get()
const crashIntegrity = crash.prepare('pragma integrity_check').get().integrity_check
crash.close()

report('1. Encrypted database in WAL mode', {
  journalMode,
  rowsThroughShim: db.prepare('select count(*) c from t').get().c,
  integrityCheckThroughShim: db.prepare('pragma integrity_check').get().integrity_check,
  vfsBelowEncryptionShim: scratch.prepare('select sirannon_encryption_prototype_vfs_below() below').get().below,
  openWithoutShim: openWithoutShim(mainPath),
  mainFileHoldsPlaintext: mainBytesWhileOpen.includes(MARKER),
  walFileHoldsPlaintext: walBytes.includes(MARKER),
  reservedBytesInHeader: mainBytesWhileOpen[20],
  headerPlaintext: mainBytesWhileOpen.subarray(0, 16).toString(),
  recoveredFromCopiedWalThroughShim: { ...crashRows, integrity: crashIntegrity },
})

const walNoKey = walkLog(walBytes, walFormat)
const record = readKeyRecord(mainBytesWhileOpen)
const dataKey = unwrapDataKey(masterKey, record)
const walWithKey = walkLog(walBytes, walFormat, (page, frame) => decryptPage(dataKey, page, frame, 0))
const walWithWrongKey = (() => {
  try {
    return walkLog(walBytes, walFormat, (page, frame) => decryptPage(Buffer.alloc(32, 1), page, frame, 0))
  } catch (err) {
    return `threw ${err.message}`
  }
})()

report('3. Write-ahead log frames against core/backup/wal-format.ts', {
  walBytes: walBytes.length,
  frames: walNoKey.frames,
  pageSizeInLogHeader: walNoKey.header?.pageSize,
  acceptedAsWritten: walNoKey.accepted,
  firstRejectedAsWritten: walNoKey.rejectedAt,
  acceptedAfterInMemoryDecrypt: walWithKey.accepted,
  firstRejectedAfterInMemoryDecrypt: walWithKey.rejectedAt,
  withWrongKey: walWithWrongKey,
})

const pageOne = decryptPage(dataKey, mainBytesWhileOpen.subarray(0, record.pageSize), 1, PLAINTEXT_HEADER_BYTES)
report('5. Reserved-byte arithmetic in page 1', {
  pageSize: record.pageSize,
  reservedBytesPerPage: PAGE_RESERVED_BYTES,
  tailStartsAt: record.pageSize - PAGE_RESERVED_BYTES,
  nonceBytes: NONCE_BYTES,
  tagBytes: TAG_BYTES,
  keyRecordBytes: KEY_RECORD_BYTES,
  keyRecordVersion: record.version,
  masterKeyName: record.masterName,
  usedOfReserved: NONCE_BYTES + TAG_BYTES + KEY_RECORD_BYTES,
  spareBytes: record.spare.length,
  spareAllZero: record.spare.every(byte => byte === 0),
  dataKeyUnwrapped: dataKey.length,
  pageOneBodyDecryptsToSqliteBtreeHeader: pageOne[PLAINTEXT_HEADER_BYTES],
  reservedShareOf4096: `${((PAGE_RESERVED_BYTES / 4096) * 100).toFixed(1)}%`,
  reservedShareOf65536: `${((PAGE_RESERVED_BYTES / 65536) * 100).toFixed(1)}%`,
})

db.exec('pragma wal_checkpoint(truncate)')
const pagesCopied = await backup(db, copyPath, { rate: 8 })
const copy = new DatabaseSync(copyPath)
const copyBytes = readFileSync(copyPath)
const plainPages = await backup(db, plainCopyPath, { rate: 8 })
const plainBytes = readFileSync(plainCopyPath)
report('2. Stepped backup through node:sqlite backup()', {
  pagesCopied,
  copyOpensThroughShim: copy.prepare('select count(*) c from t').get().c,
  copyIntegrity: copy.prepare('pragma integrity_check').get().integrity_check,
  copyReservedBytes: copyBytes[20],
  copyHoldsPlaintext: copyBytes.includes(MARKER),
  copyKeyRecordMasterName: readKeyRecord(copyBytes).masterName,
  copyDataKeyMatchesSource: unwrapDataKey(masterKey, readKeyRecord(copyBytes)).equals(dataKey),
  copyOpensWithoutShim: openWithoutShim(copyPath),
  destinationWithNoKeyPagesCopied: plainPages,
  destinationWithNoKeyHoldsPlaintext: plainBytes.includes(MARKER),
  destinationWithNoKeyReservedBytes: plainBytes[20],
})
copy.close()
db.close()

for (const order of ['encryption-first', 'stream-first']) {
  const child = spawnSync(
    process.execPath,
    [join(here, 'stacking-child.mjs'), order, extension, streamExtension, join(dir, `${order}.db`)],
    { encoding: 'utf8' },
  )
  report(`4. Stacking with the streaming VFS, ${order}`, child.status === 0 ? JSON.parse(child.stdout.trim()) : { failed: child.stderr })
}

rmSync(dir, { recursive: true, force: true })
