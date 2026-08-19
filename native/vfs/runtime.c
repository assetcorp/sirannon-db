#include "runtime.h"

SQLITE_EXTENSION_INIT3

static sqlite3_mutex *sirannonMutex = 0;

int sirannonRuntimeStart(void) {
  if (sirannonMutex) return SQLITE_OK;
  sirannonMutex = sqlite3_mutex_alloc(SQLITE_MUTEX_FAST);
  return sirannonMutex ? SQLITE_OK : SQLITE_NOMEM;
}

void sirannonEnter(void) {
  sqlite3_mutex_enter(sirannonMutex);
}

void sirannonLeave(void) {
  sqlite3_mutex_leave(sirannonMutex);
}
