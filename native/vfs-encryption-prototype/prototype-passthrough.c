#include "prototype-vfs.h"

SQLITE_EXTENSION_INIT3

int prototypeFileClose(sqlite3_file *file) {
  sqlite3_file *sub = ORIGFILE(file);
  return sub->pMethods->xClose(sub);
}

int prototypeFileTruncate(sqlite3_file *file, sqlite3_int64 size) {
  sqlite3_file *sub = ORIGFILE(file);
  return sub->pMethods->xTruncate(sub, size);
}

int prototypeFileSync(sqlite3_file *file, int flags) {
  sqlite3_file *sub = ORIGFILE(file);
  return sub->pMethods->xSync(sub, flags);
}

int prototypeFileSize(sqlite3_file *file, sqlite3_int64 *size) {
  sqlite3_file *sub = ORIGFILE(file);
  return sub->pMethods->xFileSize(sub, size);
}

int prototypeFileLock(sqlite3_file *file, int level) {
  sqlite3_file *sub = ORIGFILE(file);
  return sub->pMethods->xLock(sub, level);
}

int prototypeFileUnlock(sqlite3_file *file, int level) {
  sqlite3_file *sub = ORIGFILE(file);
  return sub->pMethods->xUnlock(sub, level);
}

int prototypeFileCheckReservedLock(sqlite3_file *file, int *result) {
  sqlite3_file *sub = ORIGFILE(file);
  return sub->pMethods->xCheckReservedLock(sub, result);
}

int prototypeFileControl(sqlite3_file *file, int op, void *arg) {
  sqlite3_file *sub = ORIGFILE(file);
  int rc = sub->pMethods->xFileControl(sub, op, arg);
  if (rc == SQLITE_OK && op == SQLITE_FCNTL_VFSNAME) {
    *(char **)arg = sqlite3_mprintf("%s/%z", PROTOTYPE_VFS_NAME, *(char **)arg);
  }
  return rc;
}

int prototypeFileSectorSize(sqlite3_file *file) {
  sqlite3_file *sub = ORIGFILE(file);
  return sub->pMethods->xSectorSize(sub);
}

int prototypeFileDeviceCharacteristics(sqlite3_file *file) {
  sqlite3_file *sub = ORIGFILE(file);
  return sub->pMethods->xDeviceCharacteristics(sub) & ~SQLITE_IOCAP_SUBPAGE_READ;
}

int prototypeFileShmMap(sqlite3_file *file, int page, int pageSize, int extend, void volatile **out) {
  sqlite3_file *sub = ORIGFILE(file);
  return sub->pMethods->xShmMap(sub, page, pageSize, extend, out);
}

int prototypeFileShmLock(sqlite3_file *file, int offset, int count, int flags) {
  sqlite3_file *sub = ORIGFILE(file);
  return sub->pMethods->xShmLock(sub, offset, count, flags);
}

void prototypeFileShmBarrier(sqlite3_file *file) {
  sqlite3_file *sub = ORIGFILE(file);
  sub->pMethods->xShmBarrier(sub);
}

int prototypeFileShmUnmap(sqlite3_file *file, int deleteFlag) {
  sqlite3_file *sub = ORIGFILE(file);
  return sub->pMethods->xShmUnmap(sub, deleteFlag);
}

int prototypeFileFetch(sqlite3_file *file, sqlite3_int64 offset, int amount, void **out) {
  (void)file;
  (void)offset;
  (void)amount;
  *out = 0;
  return SQLITE_OK;
}

int prototypeFileUnfetch(sqlite3_file *file, sqlite3_int64 offset, void *page) {
  (void)file;
  (void)offset;
  (void)page;
  return SQLITE_OK;
}

int prototypeVfsDelete(sqlite3_vfs *vfs, const char *name, int syncDir) {
  return ORIGVFS(vfs)->xDelete(ORIGVFS(vfs), name, syncDir);
}

int prototypeVfsAccess(sqlite3_vfs *vfs, const char *name, int flags, int *result) {
  return ORIGVFS(vfs)->xAccess(ORIGVFS(vfs), name, flags, result);
}

int prototypeVfsFullPathname(sqlite3_vfs *vfs, const char *name, int size, char *out) {
  return ORIGVFS(vfs)->xFullPathname(ORIGVFS(vfs), name, size, out);
}

void *prototypeVfsDlOpen(sqlite3_vfs *vfs, const char *name) {
  return ORIGVFS(vfs)->xDlOpen(ORIGVFS(vfs), name);
}

void prototypeVfsDlError(sqlite3_vfs *vfs, int size, char *out) {
  ORIGVFS(vfs)->xDlError(ORIGVFS(vfs), size, out);
}

void (*prototypeVfsDlSym(sqlite3_vfs *vfs, void *handle, const char *symbol))(void) {
  return ORIGVFS(vfs)->xDlSym(ORIGVFS(vfs), handle, symbol);
}

void prototypeVfsDlClose(sqlite3_vfs *vfs, void *handle) {
  ORIGVFS(vfs)->xDlClose(ORIGVFS(vfs), handle);
}

int prototypeVfsRandomness(sqlite3_vfs *vfs, int size, char *out) {
  return ORIGVFS(vfs)->xRandomness(ORIGVFS(vfs), size, out);
}

int prototypeVfsSleep(sqlite3_vfs *vfs, int microseconds) {
  return ORIGVFS(vfs)->xSleep(ORIGVFS(vfs), microseconds);
}

int prototypeVfsCurrentTime(sqlite3_vfs *vfs, double *out) {
  return ORIGVFS(vfs)->xCurrentTime(ORIGVFS(vfs), out);
}

int prototypeVfsGetLastError(sqlite3_vfs *vfs, int size, char *out) {
  return ORIGVFS(vfs)->xGetLastError(ORIGVFS(vfs), size, out);
}

int prototypeVfsCurrentTimeInt64(sqlite3_vfs *vfs, sqlite3_int64 *out) {
  return ORIGVFS(vfs)->xCurrentTimeInt64(ORIGVFS(vfs), out);
}
