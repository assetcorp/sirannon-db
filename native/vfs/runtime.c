#include "runtime.h"

#ifdef _WIN32
#include <windows.h>

static SRWLOCK sirannonLock = SRWLOCK_INIT;
static CONDITION_VARIABLE sirannonTurn = CONDITION_VARIABLE_INIT;

void sirannonEnter(void) {
  AcquireSRWLockExclusive(&sirannonLock);
}

void sirannonLeave(void) {
  ReleaseSRWLockExclusive(&sirannonLock);
}

void sirannonAwaitTurn(int microseconds) {
  DWORD milliseconds = (DWORD)((microseconds + 999) / 1000);
  SleepConditionVariableSRW(&sirannonTurn, &sirannonLock, milliseconds < 1 ? 1 : milliseconds, 0);
}

void sirannonWakeWaiters(void) {
  WakeAllConditionVariable(&sirannonTurn);
}
#else
#include <pthread.h>
#include <time.h>

#define SIRANNON_NANOSECONDS_PER_SECOND 1000000000L

static pthread_mutex_t sirannonLock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t sirannonTurn = PTHREAD_COND_INITIALIZER;

void sirannonEnter(void) {
  pthread_mutex_lock(&sirannonLock);
}

void sirannonLeave(void) {
  pthread_mutex_unlock(&sirannonLock);
}

void sirannonAwaitTurn(int microseconds) {
  struct timespec deadline;
  if (clock_gettime(CLOCK_REALTIME, &deadline) != 0) return;
  deadline.tv_nsec += (long)microseconds * 1000;
  deadline.tv_sec += deadline.tv_nsec / SIRANNON_NANOSECONDS_PER_SECOND;
  deadline.tv_nsec %= SIRANNON_NANOSECONDS_PER_SECOND;
  pthread_cond_timedwait(&sirannonTurn, &sirannonLock, &deadline);
}

void sirannonWakeWaiters(void) {
  pthread_cond_broadcast(&sirannonTurn);
}
#endif
