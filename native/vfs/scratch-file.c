#include "scratch-file.h"

#include <string.h>

SQLITE_EXTENSION_INIT3

static SirannonScratch *sirannonScratches = 0;

SirannonScratch *sirannonScratchByName(const char *name) {
  SirannonScratch *scratch;
  if (!name) return 0;
  for (scratch = sirannonScratches; scratch; scratch = scratch->next) {
    if (scratch->name && strcmp(scratch->name, name) == 0 && !scratch->unlinked) return scratch;
  }
  return 0;
}

SirannonScratch *sirannonScratchOpen(const char *name) {
  SirannonScratch *scratch = (SirannonScratch *)sqlite3_malloc(sizeof(SirannonScratch));
  if (!scratch) return 0;
  memset(scratch, 0, sizeof(SirannonScratch));
  if (name) {
    scratch->name = sqlite3_mprintf("%s", name);
    if (!scratch->name) {
      sqlite3_free(scratch);
      return 0;
    }
  }
  scratch->next = sirannonScratches;
  sirannonScratches = scratch;
  return scratch;
}

static int grow(SirannonScratch *scratch, sqlite3_int64 needed) {
  sqlite3_int64 allocated = scratch->allocated ? scratch->allocated : 8192;
  unsigned char *grown;
  if (needed <= scratch->allocated) return SQLITE_OK;
  while (allocated < needed) allocated *= 2;
  grown = (unsigned char *)sqlite3_realloc64(scratch->data, (sqlite3_uint64)allocated);
  if (!grown) return SQLITE_NOMEM;
  memset(grown + scratch->allocated, 0, (size_t)(allocated - scratch->allocated));
  scratch->data = grown;
  scratch->allocated = allocated;
  return SQLITE_OK;
}

int sirannonScratchWrite(SirannonScratch *scratch, const void *bytes, int amount, sqlite3_int64 offset) {
  int rc = grow(scratch, offset + amount);
  if (rc != SQLITE_OK) return rc;
  memcpy(scratch->data + offset, bytes, (size_t)amount);
  if (offset + amount > scratch->size) scratch->size = offset + amount;
  return SQLITE_OK;
}

int sirannonScratchRead(SirannonScratch *scratch, void *bytes, int amount, sqlite3_int64 offset) {
  sqlite3_int64 available = scratch->size - offset;
  if (available >= amount) {
    memcpy(bytes, scratch->data + offset, (size_t)amount);
    return SQLITE_OK;
  }
  if (available < 0) available = 0;
  if (available > 0) memcpy(bytes, scratch->data + offset, (size_t)available);
  memset((unsigned char *)bytes + available, 0, (size_t)(amount - available));
  return SQLITE_IOERR_SHORT_READ;
}

void sirannonScratchRelease(SirannonScratch *scratch) {
  SirannonScratch **link = &sirannonScratches;
  while (*link) {
    if (*link == scratch) {
      *link = scratch->next;
      break;
    }
    link = &(*link)->next;
  }
  sqlite3_free(scratch->name);
  sqlite3_free(scratch->data);
  sqlite3_free(scratch);
}
