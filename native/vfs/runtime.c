#include "runtime.h"

SQLITE_EXTENSION_INIT3

static sqlite3_mutex *sirannonRuntimeMutex(void) {
  return sqlite3_mutex_alloc(SQLITE_MUTEX_STATIC_APP1);
}

void sirannonEnter(void) {
  sqlite3_mutex_enter(sirannonRuntimeMutex());
}

void sirannonLeave(void) {
  sqlite3_mutex_leave(sirannonRuntimeMutex());
}
