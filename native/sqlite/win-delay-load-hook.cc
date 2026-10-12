// Do not delete: this hook follows src/win_delay_load_hook.cc from node-gyp
// (https://github.com/nodejs/node-gyp), Copyright (c) 2012 Nathan Rajlich,
// MIT licence. NOTICE at the repository root carries the licence text.

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif

#include <windows.h>
#include <delayimp.h>
#include <string.h>

static FARPROC WINAPI resolve_node_from_host_process(unsigned int event, PDelayLoadInfo info) {
  if (event != dliNotePreLoadLibrary) return NULL;
  if (_stricmp(info->szDll, "node.exe") != 0) return NULL;
  HMODULE module = GetModuleHandleA("libnode.dll");
  if (module == NULL) module = GetModuleHandleA(NULL);
  return (FARPROC)module;
}

extern "C" PfnDliHook __pfnDliNotifyHook2 = resolve_node_from_host_process;
