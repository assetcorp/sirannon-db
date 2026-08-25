#include "prototype-vfs.h"
SQLITE_EXTENSION_INIT1

#include <string.h>

#define WAL_HEADER_BYTES 32
#define WAL_FRAME_HEADER_BYTES 24
#define WAL_SUFFIX "-wal"

static sqlite3_vfs prototypeVfs;

static void pageAad(unsigned char *aad, int value) {
  aad[0] = (unsigned char)(value >> 24);
  aad[1] = (unsigned char)(value >> 16);
  aad[2] = (unsigned char)(value >> 8);
  aad[3] = (unsigned char)value;
}

static int walFrameShape(PrototypeFile *p, int amount, sqlite3_int64 offset, int *pageAt, int *frameNumber) {
  sqlite3_int64 relative = offset - WAL_HEADER_BYTES;
  int frameBytes = WAL_FRAME_HEADER_BYTES + p->walPageSize;
  if (p->walPageSize == 0 || offset < WAL_HEADER_BYTES) return 0;
  if (relative % frameBytes == 0 && amount == frameBytes) {
    *pageAt = WAL_FRAME_HEADER_BYTES;
    *frameNumber = (int)(relative / frameBytes) + 1;
    return 1;
  }
  if (relative % frameBytes == WAL_FRAME_HEADER_BYTES && amount == p->walPageSize) {
    *pageAt = 0;
    *frameNumber = (int)(relative / frameBytes) + 1;
    return 1;
  }
  return 0;
}

static int fileRead(sqlite3_file *file, void *buffer, int amount, sqlite3_int64 offset) {
  PrototypeFile *p = (PrototypeFile *)file;
  sqlite3_file *sub = ORIGFILE(file);
  unsigned char *bytes = (unsigned char *)buffer;
  unsigned char aad[4];
  int rc = sub->pMethods->xRead(sub, buffer, amount, offset);
  if (rc != SQLITE_OK || !p->keyed) return rc;
  if (p->isWal) {
    int pageAt = 0;
    int frameNumber = 0;
    if (offset == 0 && amount >= WAL_HEADER_BYTES) {
      p->walPageSize = (bytes[8] << 24) | (bytes[9] << 16) | (bytes[10] << 8) | bytes[11];
    }
    if (walFrameShape(p, amount, offset, &pageAt, &frameNumber)) {
      pageAad(aad, frameNumber);
      return prototypeDecryptPage(p->keyed, 0, aad, 4, bytes + pageAt, p->walPageSize);
    }
    return SQLITE_OK;
  }
  if (offset == 0 && amount >= PROTOTYPE_PLAINTEXT_HEADER_BYTES && memcmp(bytes, "SQLite format 3", 16) == 0) {
    p->keyed->pageSize = prototypeParsePageSize(bytes);
  }
  if (p->keyed->pageSize && amount == p->keyed->pageSize && offset % p->keyed->pageSize == 0) {
    int pageNumber = (int)(offset / p->keyed->pageSize) + 1;
    pageAad(aad, pageNumber);
    return prototypeDecryptPage(p->keyed, pageNumber, aad, 4, bytes, p->keyed->pageSize);
  }
  return SQLITE_OK;
}

static int writeEncrypted(PrototypeFile *p, sqlite3_file *sub, const unsigned char *bytes, int amount,
                          sqlite3_int64 offset, int pageNumber, int pageAt, int pageSize, int aadValue) {
  unsigned char aad[4];
  unsigned char *out = (unsigned char *)sqlite3_malloc(amount);
  int rc;
  if (!out) return SQLITE_NOMEM;
  memcpy(out, bytes, (size_t)amount);
  pageAad(aad, aadValue);
  rc = prototypeEncryptPage(p->keyed, pageNumber, aad, 4, bytes + pageAt, out + pageAt, pageSize);
  if (rc == SQLITE_OK) rc = sub->pMethods->xWrite(sub, out, amount, offset);
  sqlite3_free(out);
  return rc;
}

static int fileWrite(sqlite3_file *file, const void *buffer, int amount, sqlite3_int64 offset) {
  PrototypeFile *p = (PrototypeFile *)file;
  sqlite3_file *sub = ORIGFILE(file);
  const unsigned char *bytes = (const unsigned char *)buffer;
  if (!p->keyed) return sub->pMethods->xWrite(sub, buffer, amount, offset);
  if (p->isWal) {
    int pageAt = 0;
    int frameNumber = 0;
    if (offset == 0 && amount >= WAL_HEADER_BYTES) {
      p->walPageSize = (bytes[8] << 24) | (bytes[9] << 16) | (bytes[10] << 8) | bytes[11];
    }
    if (walFrameShape(p, amount, offset, &pageAt, &frameNumber)) {
      return writeEncrypted(p, sub, bytes, amount, offset, 0, pageAt, p->walPageSize, frameNumber);
    }
    return sub->pMethods->xWrite(sub, buffer, amount, offset);
  }
  if (offset == 0 && amount >= PROTOTYPE_PLAINTEXT_HEADER_BYTES && memcmp(bytes, "SQLite format 3", 16) == 0) {
    if (bytes[20] != PROTOTYPE_PAGE_RESERVED_BYTES) return SQLITE_IOERR_WRITE;
    p->keyed->pageSize = prototypeParsePageSize(bytes);
  }
  if (p->keyed->pageSize && amount == p->keyed->pageSize && offset % p->keyed->pageSize == 0) {
    int pageNumber = (int)(offset / p->keyed->pageSize) + 1;
    return writeEncrypted(p, sub, bytes, amount, offset, pageNumber, 0, p->keyed->pageSize, pageNumber);
  }
  return SQLITE_IOERR_WRITE;
}

static const sqlite3_io_methods prototypeIoMethods = {
  3,
  prototypeFileClose,
  fileRead,
  fileWrite,
  prototypeFileTruncate,
  prototypeFileSync,
  prototypeFileSize,
  prototypeFileLock,
  prototypeFileUnlock,
  prototypeFileCheckReservedLock,
  prototypeFileControl,
  prototypeFileSectorSize,
  prototypeFileDeviceCharacteristics,
  prototypeFileShmMap,
  prototypeFileShmLock,
  prototypeFileShmBarrier,
  prototypeFileShmUnmap,
  prototypeFileFetch,
  prototypeFileUnfetch,
};

static KeyedDatabase *keyedForWal(const char *name) {
  size_t length = strlen(name);
  size_t suffix = strlen(WAL_SUFFIX);
  char *databasePath;
  KeyedDatabase *keyed;
  if (length <= suffix || strcmp(name + length - suffix, WAL_SUFFIX) != 0) return 0;
  databasePath = sqlite3_mprintf("%.*s", (int)(length - suffix), name);
  keyed = databasePath ? prototypeKeyedByPath(databasePath) : 0;
  sqlite3_free(databasePath);
  return keyed;
}

static int vfsOpen(sqlite3_vfs *vfs, const char *name, sqlite3_file *file, int flags, int *outFlags) {
  PrototypeFile *p = (PrototypeFile *)file;
  sqlite3_file *sub = ORIGFILE(file);
  sqlite3_vfs *base = ORIGVFS(vfs);
  int rc;
  memset(p, 0, sizeof(PrototypeFile));
  p->name = name;
  if (name && (flags & SQLITE_OPEN_MAIN_DB)) p->keyed = prototypeKeyedByPath(name);
  if (name && (flags & SQLITE_OPEN_WAL)) {
    p->keyed = keyedForWal(name);
    p->isWal = 1;
  }
  rc = base->xOpen(base, name, sub, flags, outFlags);
  if (rc != SQLITE_OK) return rc;
  file->pMethods = &prototypeIoMethods;
  if (p->keyed && !p->isWal) {
    rc = prototypeReadKeyRecord(p->keyed, sub);
    if (rc == SQLITE_OK && !p->keyed->hasDataKey) rc = prototypeStartNewDataKey(p->keyed);
    if (rc != SQLITE_OK) {
      sub->pMethods->xClose(sub);
      file->pMethods = 0;
      return rc;
    }
  }
  if (p->keyed && p->isWal) {
    unsigned char header[WAL_HEADER_BYTES];
    sqlite3_int64 size = 0;
    if (sub->pMethods->xFileSize(sub, &size) == SQLITE_OK && size >= WAL_HEADER_BYTES &&
        sub->pMethods->xRead(sub, header, WAL_HEADER_BYTES, 0) == SQLITE_OK) {
      p->walPageSize = (header[8] << 24) | (header[9] << 16) | (header[10] << 8) | header[11];
    }
  }
  return SQLITE_OK;
}

static sqlite3_vfs prototypeVfs = {
  2,
  0,
  1024,
  0,
  PROTOTYPE_VFS_NAME,
  0,
  vfsOpen,
  prototypeVfsDelete,
  prototypeVfsAccess,
  prototypeVfsFullPathname,
  prototypeVfsDlOpen,
  prototypeVfsDlError,
  prototypeVfsDlSym,
  prototypeVfsDlClose,
  prototypeVfsRandomness,
  prototypeVfsSleep,
  prototypeVfsCurrentTime,
  prototypeVfsGetLastError,
  prototypeVfsCurrentTimeInt64,
  0,
  0,
  0,
};

static int reserveBytesOnKeyedConnections(sqlite3 *db, char **error, const sqlite3_api_routines *api) {
  const char *path = sqlite3_db_filename(db, "main");
  int reserved = PROTOTYPE_PAGE_RESERVED_BYTES;
  (void)error;
  (void)api;
  if (!path || !path[0] || !prototypeKeyedByPath(path)) return SQLITE_OK;
  return sqlite3_file_control(db, "main", SQLITE_FCNTL_RESERVE_BYTES, &reserved);
}

static void registerKeyFunction(sqlite3_context *context, int argc, sqlite3_value **argv) {
  const char *path = (const char *)sqlite3_value_text(argv[0]);
  const unsigned char *key = (const unsigned char *)sqlite3_value_blob(argv[1]);
  const char *masterName = (const char *)sqlite3_value_text(argv[2]);
  char full[1024];
  (void)argc;
  if (!path || !key || sqlite3_value_bytes(argv[1]) != PROTOTYPE_KEY_BYTES) {
    sqlite3_result_error(context, "the key must be exactly 32 bytes and the path a text value", -1);
    return;
  }
  if (ORIGVFS(&prototypeVfs)->xFullPathname(ORIGVFS(&prototypeVfs), path, sizeof(full), full) & 0xff) {
    sqlite3_result_error(context, "the path could not be resolved", -1);
    return;
  }
  if (!prototypeRegisterKey(full, key, masterName ? masterName : "")) {
    sqlite3_result_error_nomem(context);
    return;
  }
  sqlite3_result_text(context, full, -1, SQLITE_TRANSIENT);
}

static void vfsBelowFunction(sqlite3_context *context, int argc, sqlite3_value **argv) {
  (void)argc;
  (void)argv;
  sqlite3_result_text(context, ORIGVFS(&prototypeVfs)->zName, -1, SQLITE_TRANSIENT);
}

#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_encryptionprototype_init(sqlite3 *db, char **error, const sqlite3_api_routines *api) {
  int rc;
  sqlite3_vfs *base;
  SQLITE_EXTENSION_INIT2(api);
  (void)error;
  if (!sqlite3_vfs_find(PROTOTYPE_VFS_NAME)) {
    base = sqlite3_vfs_find(0);
    if (!base) return SQLITE_ERROR;
    prototypeVfs.iVersion = base->iVersion;
    prototypeVfs.pAppData = base;
    prototypeVfs.szOsFile = base->szOsFile + (int)sizeof(PrototypeFile);
    prototypeVfs.mxPathname = base->mxPathname;
    rc = sqlite3_vfs_register(&prototypeVfs, 1);
    if (rc != SQLITE_OK) return rc;
    rc = sqlite3_auto_extension((void (*)(void))reserveBytesOnKeyedConnections);
    if (rc != SQLITE_OK) return rc;
  }
  rc = sqlite3_create_function(db, "sirannon_encryption_prototype_key", 3, SQLITE_UTF8 | SQLITE_DIRECTONLY, 0,
                               registerKeyFunction, 0, 0);
  if (rc == SQLITE_OK) {
    rc = sqlite3_create_function(db, "sirannon_encryption_prototype_vfs_below", 0, SQLITE_UTF8 | SQLITE_DIRECTONLY, 0,
                                 vfsBelowFunction, 0, 0);
  }
  return rc == SQLITE_OK ? SQLITE_OK_LOAD_PERMANENTLY : rc;
}
