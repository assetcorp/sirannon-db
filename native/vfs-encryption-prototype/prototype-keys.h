#ifndef SIRANNON_ENCRYPTION_PROTOTYPE_KEYS_H
#define SIRANNON_ENCRYPTION_PROTOTYPE_KEYS_H

#include "prototype-crypto.h"
#include "sqlite3ext.h"

typedef struct KeyedDatabase {
  struct KeyedDatabase *next;
  char *path;
  unsigned char masterKey[PROTOTYPE_KEY_BYTES];
  unsigned char masterName[PROTOTYPE_MASTER_NAME_BYTES];
  unsigned char dataKey[PROTOTYPE_KEY_BYTES];
  unsigned char record[PROTOTYPE_KEY_RECORD_BYTES];
  int hasDataKey;
  int pageSize;
} KeyedDatabase;

KeyedDatabase *prototypeKeyedByPath(const char *path);
KeyedDatabase *prototypeRegisterKey(const char *path, const unsigned char *masterKey, const char *masterName);
int prototypeParsePageSize(const unsigned char *header);
int prototypeReadKeyRecord(KeyedDatabase *keyed, sqlite3_file *file);
int prototypeStartNewDataKey(KeyedDatabase *keyed);
int prototypeEncryptPage(KeyedDatabase *keyed, int pageNumber, unsigned char *aad, int aadLength,
                         const unsigned char *page, unsigned char *out, int pageSize);
int prototypeDecryptPage(KeyedDatabase *keyed, int pageNumber, unsigned char *aad, int aadLength,
                         unsigned char *page, int pageSize);

#endif
