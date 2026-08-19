#ifndef SIRANNON_RUNTIME_H
#define SIRANNON_RUNTIME_H

#include "sqlite3ext.h"

int sirannonRuntimeStart(void);
void sirannonEnter(void);
void sirannonLeave(void);

#endif
