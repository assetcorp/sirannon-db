#ifndef SIRANNON_ENCRYPTION_PROTOTYPE_VFS_H
#define SIRANNON_ENCRYPTION_PROTOTYPE_VFS_H

#include "prototype-keys.h"
#include "sqlite3ext.h"

#define PROTOTYPE_VFS_NAME "sirannon-encryption-prototype"

typedef struct PrototypeFile {
  sqlite3_file base;
  KeyedDatabase *keyed;
  int isWal;
  int walPageSize;
  const char *name;
} PrototypeFile;

#define ORIGVFS(p) ((sqlite3_vfs *)((p)->pAppData))
#define ORIGFILE(p) ((sqlite3_file *)(((PrototypeFile *)(p)) + 1))

int prototypeFileClose(sqlite3_file *file);
int prototypeFileTruncate(sqlite3_file *file, sqlite3_int64 size);
int prototypeFileSync(sqlite3_file *file, int flags);
int prototypeFileSize(sqlite3_file *file, sqlite3_int64 *size);
int prototypeFileLock(sqlite3_file *file, int level);
int prototypeFileUnlock(sqlite3_file *file, int level);
int prototypeFileCheckReservedLock(sqlite3_file *file, int *result);
int prototypeFileControl(sqlite3_file *file, int op, void *arg);
int prototypeFileSectorSize(sqlite3_file *file);
int prototypeFileDeviceCharacteristics(sqlite3_file *file);
int prototypeFileShmMap(sqlite3_file *file, int page, int pageSize, int extend, void volatile **out);
int prototypeFileShmLock(sqlite3_file *file, int offset, int count, int flags);
void prototypeFileShmBarrier(sqlite3_file *file);
int prototypeFileShmUnmap(sqlite3_file *file, int deleteFlag);
int prototypeFileFetch(sqlite3_file *file, sqlite3_int64 offset, int amount, void **out);
int prototypeFileUnfetch(sqlite3_file *file, sqlite3_int64 offset, void *page);
int prototypeVfsDelete(sqlite3_vfs *vfs, const char *name, int syncDir);
int prototypeVfsAccess(sqlite3_vfs *vfs, const char *name, int flags, int *result);
int prototypeVfsFullPathname(sqlite3_vfs *vfs, const char *name, int size, char *out);
void *prototypeVfsDlOpen(sqlite3_vfs *vfs, const char *name);
void prototypeVfsDlError(sqlite3_vfs *vfs, int size, char *out);
void (*prototypeVfsDlSym(sqlite3_vfs *vfs, void *handle, const char *symbol))(void);
void prototypeVfsDlClose(sqlite3_vfs *vfs, void *handle);
int prototypeVfsRandomness(sqlite3_vfs *vfs, int size, char *out);
int prototypeVfsSleep(sqlite3_vfs *vfs, int microseconds);
int prototypeVfsCurrentTime(sqlite3_vfs *vfs, double *out);
int prototypeVfsGetLastError(sqlite3_vfs *vfs, int size, char *out);
int prototypeVfsCurrentTimeInt64(sqlite3_vfs *vfs, sqlite3_int64 *out);

#endif
