#include "runtime.h"

#ifdef _WIN32
#ifndef _WIN32_WINNT
#define _WIN32_WINNT 0x0600
#endif
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

#define SIRANNON_MICROSECONDS_PER_SECOND 1000000
#define SIRANNON_NANOSECONDS_PER_SECOND 1000000000L

static pthread_mutex_t sirannonLock = PTHREAD_MUTEX_INITIALIZER;

void sirannonEnter(void) {
  pthread_mutex_lock(&sirannonLock);
}

void sirannonLeave(void) {
  pthread_mutex_unlock(&sirannonLock);
}

#ifdef __APPLE__
static pthread_cond_t sirannonTurn = PTHREAD_COND_INITIALIZER;

static pthread_cond_t *preparedTurn(void) {
  return &sirannonTurn;
}

void sirannonAwaitTurn(int microseconds) {
  struct timespec span;
  span.tv_sec = microseconds / SIRANNON_MICROSECONDS_PER_SECOND;
  span.tv_nsec = (long)(microseconds % SIRANNON_MICROSECONDS_PER_SECOND) * 1000;
  pthread_cond_timedwait_relative_np(preparedTurn(), &sirannonLock, &span);
}
#else
static pthread_cond_t sirannonTurn;
static pthread_once_t sirannonTurnPrepared = PTHREAD_ONCE_INIT;

static void prepareTurn(void) {
  pthread_condattr_t attributes;
  pthread_condattr_init(&attributes);
  pthread_condattr_setclock(&attributes, CLOCK_MONOTONIC);
  pthread_cond_init(&sirannonTurn, &attributes);
  pthread_condattr_destroy(&attributes);
}

static pthread_cond_t *preparedTurn(void) {
  pthread_once(&sirannonTurnPrepared, prepareTurn);
  return &sirannonTurn;
}

void sirannonAwaitTurn(int microseconds) {
  pthread_cond_t *turn = preparedTurn();
  struct timespec deadline;
  if (clock_gettime(CLOCK_MONOTONIC, &deadline) != 0) return;
  deadline.tv_sec += microseconds / SIRANNON_MICROSECONDS_PER_SECOND;
  deadline.tv_nsec += (long)(microseconds % SIRANNON_MICROSECONDS_PER_SECOND) * 1000;
  deadline.tv_sec += deadline.tv_nsec / SIRANNON_NANOSECONDS_PER_SECOND;
  deadline.tv_nsec %= SIRANNON_NANOSECONDS_PER_SECOND;
  pthread_cond_timedwait(turn, &sirannonLock, &deadline);
}
#endif

void sirannonWakeWaiters(void) {
  pthread_cond_broadcast(preparedTurn());
}
#endif
