#ifndef SIRANNON_SCRATCH_FILE_H
#define SIRANNON_SCRATCH_FILE_H

#include "sqlite3ext.h"

typedef struct SirannonScratch SirannonScratch;

struct SirannonScratch {
  SirannonScratch *next;
  char *name;
  unsigned char *data;
  sqlite3_int64 size;
  sqlite3_int64 allocated;
  int openFiles;
  int deleteOnClose;
  int unlinked;
};

SirannonScratch *sirannonScratchByName(const char *name);
SirannonScratch *sirannonScratchOpen(const char *name);
int sirannonScratchWrite(SirannonScratch *scratch, const void *bytes, int amount, sqlite3_int64 offset);
int sirannonScratchRead(SirannonScratch *scratch, void *bytes, int amount, sqlite3_int64 offset);
void sirannonScratchRelease(SirannonScratch *scratch);

#endif
