#include "prototype-keys.h"

#include <string.h>

SQLITE_EXTENSION_INIT3

static KeyedDatabase *registry = 0;

KeyedDatabase *prototypeKeyedByPath(const char *path) {
  KeyedDatabase *entry;
  for (entry = registry; entry; entry = entry->next) {
    if (strcmp(entry->path, path) == 0) return entry;
  }
  return 0;
}

KeyedDatabase *prototypeRegisterKey(const char *path, const unsigned char *masterKey, const char *masterName) {
  KeyedDatabase *entry = prototypeKeyedByPath(path);
  if (!entry) {
    entry = (KeyedDatabase *)sqlite3_malloc(sizeof(KeyedDatabase));
    if (!entry) return 0;
    memset(entry, 0, sizeof(KeyedDatabase));
    entry->path = sqlite3_mprintf("%s", path);
    entry->next = registry;
    registry = entry;
  }
  memcpy(entry->masterKey, masterKey, PROTOTYPE_KEY_BYTES);
  memset(entry->masterName, 0, PROTOTYPE_MASTER_NAME_BYTES);
  strncpy((char *)entry->masterName, masterName, PROTOTYPE_MASTER_NAME_BYTES);
  entry->hasDataKey = 0;
  entry->pageSize = 0;
  return entry;
}

int prototypeParsePageSize(const unsigned char *header) {
  int pageSize = (header[16] << 8) | header[17];
  if (pageSize == 1) return 65536;
  return pageSize;
}

int prototypeReadKeyRecord(KeyedDatabase *keyed, sqlite3_file *file) {
  unsigned char header[PROTOTYPE_PLAINTEXT_HEADER_BYTES];
  unsigned char tail[PROTOTYPE_PAGE_RESERVED_BYTES];
  sqlite3_int64 size = 0;
  int rc = file->pMethods->xFileSize(file, &size);
  if (rc != SQLITE_OK) return rc;
  if (size < PROTOTYPE_PLAINTEXT_HEADER_BYTES) return SQLITE_OK;
  rc = file->pMethods->xRead(file, header, PROTOTYPE_PLAINTEXT_HEADER_BYTES, 0);
  if (rc != SQLITE_OK) return rc;
  if (memcmp(header, "SQLite format 3", 16) != 0) return SQLITE_NOTADB;
  if (header[20] != PROTOTYPE_PAGE_RESERVED_BYTES) return SQLITE_NOTADB;
  keyed->pageSize = prototypeParsePageSize(header);
  rc = file->pMethods->xRead(file, tail, PROTOTYPE_PAGE_RESERVED_BYTES, keyed->pageSize - PROTOTYPE_PAGE_RESERVED_BYTES);
  if (rc != SQLITE_OK) return rc;
  memcpy(keyed->record, tail + PROTOTYPE_NONCE_BYTES + PROTOTYPE_TAG_BYTES, PROTOTYPE_KEY_RECORD_BYTES);
  if (prototypeUnwrapDataKey(keyed->masterKey, keyed->record, keyed->dataKey)) return SQLITE_NOTADB;
  keyed->hasDataKey = 1;
  return SQLITE_OK;
}

int prototypeStartNewDataKey(KeyedDatabase *keyed) {
  if (prototypeRandomBytes(keyed->dataKey, PROTOTYPE_KEY_BYTES)) return SQLITE_IOERR;
  if (prototypeWrapDataKey(keyed->masterKey, keyed->masterName, keyed->dataKey, keyed->record)) return SQLITE_IOERR;
  keyed->hasDataKey = 1;
  return SQLITE_OK;
}

static int bodyOffset(int pageNumber) {
  return pageNumber == 1 ? PROTOTYPE_PLAINTEXT_HEADER_BYTES : 0;
}

int prototypeEncryptPage(KeyedDatabase *keyed, int pageNumber, unsigned char *aad, int aadLength,
                         const unsigned char *page, unsigned char *out, int pageSize) {
  int start = bodyOffset(pageNumber);
  int bodyLength = pageSize - PROTOTYPE_PAGE_RESERVED_BYTES - start;
  unsigned char *tail = out + pageSize - PROTOTYPE_PAGE_RESERVED_BYTES;
  unsigned char *nonce = tail;
  unsigned char *tag = tail + PROTOTYPE_NONCE_BYTES;
  if (!keyed->hasDataKey) return SQLITE_IOERR;
  memcpy(out, page, (size_t)start);
  memset(tail, 0, PROTOTYPE_PAGE_RESERVED_BYTES);
  if (prototypeRandomBytes(nonce, PROTOTYPE_NONCE_BYTES)) return SQLITE_IOERR;
  if (prototypeSeal(keyed->dataKey, nonce, aad, aadLength, page + start, bodyLength, out + start, tag)) {
    return SQLITE_IOERR;
  }
  if (pageNumber == 1) memcpy(tag + PROTOTYPE_TAG_BYTES, keyed->record, PROTOTYPE_KEY_RECORD_BYTES);
  return SQLITE_OK;
}

static int wholePageIsZero(const unsigned char *page, int pageSize) {
  int i;
  for (i = 0; i < pageSize; i++) {
    if (page[i]) return 0;
  }
  return 1;
}

int prototypeDecryptPage(KeyedDatabase *keyed, int pageNumber, unsigned char *aad, int aadLength,
                         unsigned char *page, int pageSize) {
  int start = bodyOffset(pageNumber);
  int bodyLength = pageSize - PROTOTYPE_PAGE_RESERVED_BYTES - start;
  unsigned char *tail = page + pageSize - PROTOTYPE_PAGE_RESERVED_BYTES;
  unsigned char *nonce = tail;
  unsigned char *tag = tail + PROTOTYPE_NONCE_BYTES;
  if (wholePageIsZero(page, pageSize)) return SQLITE_OK;
  if (!keyed->hasDataKey) return SQLITE_IOERR_DATA;
  if (prototypeOpen(keyed->dataKey, nonce, aad, aadLength, page + start, bodyLength, tag, page + start)) {
    return SQLITE_IOERR_DATA;
  }
  memset(tail, 0, PROTOTYPE_PAGE_RESERVED_BYTES);
  return SQLITE_OK;
}
