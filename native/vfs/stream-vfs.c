#include "stream-vfs.h"

#include <string.h>

#include "piece-stream.h"
#include "runtime.h"
#include "scratch-file.h"

SQLITE_EXTENSION_INIT3

#define SIRANNON_SECTOR_BYTES 4096

typedef struct SirannonFile {
  sqlite3_file base;
  SirannonStream *stream;
  SirannonScratch *scratch;
} SirannonFile;

static int sirannonVfsRegistered = 0;

static int fileClose(sqlite3_file *file) {
  SirannonFile *handle = (SirannonFile *)file;
  sirannonEnter();
  if (handle->stream) {
    handle->stream->openFiles--;
  } else if (handle->scratch) {
    handle->scratch->openFiles--;
    if (handle->scratch->openFiles <= 0 && (handle->scratch->deleteOnClose || handle->scratch->unlinked)) {
      sirannonScratchRelease(handle->scratch);
    }
  }
  handle->stream = 0;
  handle->scratch = 0;
  sirannonLeave();
  return SQLITE_OK;
}

static int fileRead(sqlite3_file *file, void *bytes, int amount, sqlite3_int64 offset) {
  SirannonFile *handle = (SirannonFile *)file;
  int rc;
  sirannonEnter();
  rc = handle->stream ? sirannonStreamRead(handle->stream, bytes, amount, offset)
                      : sirannonScratchRead(handle->scratch, bytes, amount, offset);
  sirannonLeave();
  return rc;
}

static int fileWrite(sqlite3_file *file, const void *bytes, int amount, sqlite3_int64 offset) {
  SirannonFile *handle = (SirannonFile *)file;
  int rc;
  sirannonEnter();
  rc = handle->stream ? sirannonStreamWrite(handle->stream, bytes, amount, offset)
                      : sirannonScratchWrite(handle->scratch, bytes, amount, offset);
  sirannonLeave();
  return rc;
}

static int fileTruncate(sqlite3_file *file, sqlite3_int64 size) {
  SirannonFile *handle = (SirannonFile *)file;
  int rc = SQLITE_OK;
  sirannonEnter();
  if (handle->stream) rc = sirannonStreamTruncate(handle->stream, size);
  else if (size < handle->scratch->size) handle->scratch->size = size;
  sirannonLeave();
  return rc;
}

static int fileSync(sqlite3_file *file, int flags) {
  (void)file;
  (void)flags;
  return SQLITE_OK;
}

static int fileSize(sqlite3_file *file, sqlite3_int64 *size) {
  SirannonFile *handle = (SirannonFile *)file;
  sirannonEnter();
  *size = handle->stream ? handle->stream->size : handle->scratch->size;
  sirannonLeave();
  return SQLITE_OK;
}

static int fileLock(sqlite3_file *file, int level) {
  (void)file;
  (void)level;
  return SQLITE_OK;
}

static int fileUnlock(sqlite3_file *file, int level) {
  (void)file;
  (void)level;
  return SQLITE_OK;
}

static int fileCheckReservedLock(sqlite3_file *file, int *result) {
  (void)file;
  *result = 0;
  return SQLITE_OK;
}

static int fileControl(sqlite3_file *file, int op, void *arg) {
  (void)file;
  if (op == SQLITE_FCNTL_VFSNAME) {
    *(char **)arg = sqlite3_mprintf("%s", SIRANNON_VFS_NAME);
    return SQLITE_OK;
  }
  return SQLITE_NOTFOUND;
}

static int fileSectorSize(sqlite3_file *file) {
  (void)file;
  return SIRANNON_SECTOR_BYTES;
}

static int fileDeviceCharacteristics(sqlite3_file *file) {
  (void)file;
  return SQLITE_IOCAP_ATOMIC | SQLITE_IOCAP_SAFE_APPEND | SQLITE_IOCAP_SEQUENTIAL | SQLITE_IOCAP_POWERSAFE_OVERWRITE;
}

static const sqlite3_io_methods sirannonIoMethods = {
  1,
  fileClose,
  fileRead,
  fileWrite,
  fileTruncate,
  fileSync,
  fileSize,
  fileLock,
  fileUnlock,
  fileCheckReservedLock,
  fileControl,
  fileSectorSize,
  fileDeviceCharacteristics,
  0,
  0,
  0,
  0,
  0,
  0,
};

static int vfsOpen(sqlite3_vfs *vfs, const char *name, sqlite3_file *file, int flags, int *outFlags) {
  SirannonFile *handle = (SirannonFile *)file;
  sqlite3_int64 id = 0;
  (void)vfs;
  memset(handle, 0, sizeof(SirannonFile));
  if (outFlags) *outFlags = flags;
  sirannonEnter();
  if (sirannonStreamIdFromName(name, &id)) {
    SirannonStream *stream = sirannonStreamById(id);
    if (!stream) {
      sirannonLeave();
      return SQLITE_CANTOPEN;
    }
    stream->openFiles++;
    handle->stream = stream;
  } else {
    SirannonScratch *scratch = sirannonScratchByName(name);
    if (!scratch) scratch = sirannonScratchOpen(name);
    if (!scratch) {
      sirannonLeave();
      return SQLITE_NOMEM;
    }
    scratch->openFiles++;
    if (flags & SQLITE_OPEN_DELETEONCLOSE) scratch->deleteOnClose = 1;
    handle->scratch = scratch;
  }
  handle->base.pMethods = &sirannonIoMethods;
  sirannonLeave();
  return SQLITE_OK;
}

static int vfsDelete(sqlite3_vfs *vfs, const char *name, int syncDir) {
  SirannonScratch *scratch;
  (void)vfs;
  (void)syncDir;
  sirannonEnter();
  scratch = sirannonScratchByName(name);
  if (scratch) {
    if (scratch->openFiles > 0) scratch->unlinked = 1;
    else sirannonScratchRelease(scratch);
  }
  sirannonLeave();
  return SQLITE_OK;
}

static int vfsAccess(sqlite3_vfs *vfs, const char *name, int flags, int *result) {
  sqlite3_int64 id = 0;
  (void)vfs;
  (void)flags;
  sirannonEnter();
  if (sirannonStreamIdFromName(name, &id)) *result = sirannonStreamById(id) ? 1 : 0;
  else *result = sirannonScratchByName(name) ? 1 : 0;
  sirannonLeave();
  return SQLITE_OK;
}

static int vfsFullPathname(sqlite3_vfs *vfs, const char *name, int size, char *out) {
  (void)vfs;
  sqlite3_snprintf(size, out, "%s", name);
  return SQLITE_OK;
}

static sqlite3_vfs *host(sqlite3_vfs *vfs) {
  return (sqlite3_vfs *)vfs->pAppData;
}

static int vfsRandomness(sqlite3_vfs *vfs, int size, char *out) {
  return host(vfs)->xRandomness(host(vfs), size, out);
}

static int vfsSleep(sqlite3_vfs *vfs, int microseconds) {
  return host(vfs)->xSleep(host(vfs), microseconds);
}

static int vfsCurrentTime(sqlite3_vfs *vfs, double *out) {
  return host(vfs)->xCurrentTime(host(vfs), out);
}

static int vfsGetLastError(sqlite3_vfs *vfs, int size, char *out) {
  (void)vfs;
  if (size > 0) out[0] = '\0';
  return SQLITE_OK;
}

static int vfsCurrentTimeInt64(sqlite3_vfs *vfs, sqlite3_int64 *out) {
  return host(vfs)->xCurrentTimeInt64(host(vfs), out);
}

static sqlite3_vfs sirannonVfs = {
  2,
  sizeof(SirannonFile),
  1024,
  0,
  SIRANNON_VFS_NAME,
  0,
  vfsOpen,
  vfsDelete,
  vfsAccess,
  vfsFullPathname,
  0,
  0,
  0,
  0,
  vfsRandomness,
  vfsSleep,
  vfsCurrentTime,
  vfsGetLastError,
  vfsCurrentTimeInt64,
  0,
  0,
  0,
};

int sirannonVfsRegister(void) {
  int rc;
  if (sirannonVfsRegistered) return SQLITE_OK;
  rc = sirannonRuntimeStart();
  if (rc != SQLITE_OK) return rc;
  sirannonVfs.pAppData = sqlite3_vfs_find(0);
  if (!sirannonVfs.pAppData) return SQLITE_ERROR;
  sirannonVfs.mxPathname = ((sqlite3_vfs *)sirannonVfs.pAppData)->mxPathname;
  rc = sqlite3_vfs_register(&sirannonVfs, 0);
  if (rc == SQLITE_OK) sirannonVfsRegistered = 1;
  return rc;
}
