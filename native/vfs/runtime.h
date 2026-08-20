#ifndef SIRANNON_RUNTIME_H
#define SIRANNON_RUNTIME_H

void sirannonEnter(void);
void sirannonLeave(void);
void sirannonAwaitTurn(int microseconds);
void sirannonWakeWaiters(void);

#endif
