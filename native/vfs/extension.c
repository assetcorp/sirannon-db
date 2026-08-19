#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT1

#include <string.h>

#include "piece-stream.h"
#include "runtime.h"
#include "stream-vfs.h"

#define SIRANNON_VFS_VERSION "1"

static SirannonStream *argumentStream(sqlite3_context *context, sqlite3_value *value) {
  SirannonStream *stream = sirannonStreamById(sqlite3_value_int64(value));
  if (!stream) sqlite3_result_error(context, "no open backup stream carries that identifier", -1);
  return stream;
}

static void versionFunction(sqlite3_context *context, int argc, sqlite3_value **argv) {
  (void)argc;
  (void)argv;
  sqlite3_result_text(context, SIRANNON_VFS_VERSION, -1, SQLITE_STATIC);
}

static void openFunction(sqlite3_context *context, int argc, sqlite3_value **argv) {
  int pieceBytes = sqlite3_value_int(argv[0]);
  int maxQueued = sqlite3_value_int(argv[1]);
  int waitWhenFull = sqlite3_value_int(argv[2]);
  SirannonStream *stream;
  (void)argc;
  if (pieceBytes < SIRANNON_MIN_PIECE_BYTES || pieceBytes > SIRANNON_MAX_PIECE_BYTES || (pieceBytes % 512) != 0) {
    sqlite3_result_error(context, "a piece must be a whole number of 512-byte blocks between 512 bytes and 256 MiB", -1);
    return;
  }
  sirannonEnter();
  stream = sirannonStreamOpen(pieceBytes, maxQueued, waitWhenFull);
  if (stream) sqlite3_result_int64(context, stream->id);
  else sqlite3_result_error_nomem(context);
  sirannonLeave();
}

static void takeFunction(sqlite3_context *context, int argc, sqlite3_value **argv) {
  SirannonStream *stream;
  SirannonPiece *piece;
  unsigned char *framed;
  int length;
  (void)argc;
  sirannonEnter();
  stream = argumentStream(context, argv[0]);
  piece = stream ? sirannonStreamTakePiece(stream) : 0;
  sirannonLeave();
  if (!stream) return;
  if (!piece) {
    sqlite3_result_null(context);
    return;
  }
  length = piece->length;
  framed = (unsigned char *)sqlite3_malloc(SIRANNON_PIECE_HEADER_BYTES + length);
  if (!framed) {
    sirannonPieceFree(piece);
    sqlite3_result_error_nomem(context);
    return;
  }
  framed[0] = (unsigned char)(piece->index & 0xff);
  framed[1] = (unsigned char)((piece->index >> 8) & 0xff);
  framed[2] = (unsigned char)((piece->index >> 16) & 0xff);
  framed[3] = (unsigned char)((piece->index >> 24) & 0xff);
  framed[4] = (unsigned char)(length & 0xff);
  framed[5] = (unsigned char)((length >> 8) & 0xff);
  framed[6] = (unsigned char)((length >> 16) & 0xff);
  framed[7] = (unsigned char)((length >> 24) & 0xff);
  memcpy(framed + SIRANNON_PIECE_HEADER_BYTES, piece->data, (size_t)length);
  sirannonPieceFree(piece);
  sqlite3_result_blob64(context, framed, (sqlite3_uint64)(SIRANNON_PIECE_HEADER_BYTES + length), sqlite3_free);
}

static void writtenFunction(sqlite3_context *context, int argc, sqlite3_value **argv) {
  SirannonStream *stream;
  (void)argc;
  sirannonEnter();
  stream = argumentStream(context, argv[0]);
  if (stream) sqlite3_result_int64(context, stream->size);
  sirannonLeave();
}

static void errorFunction(sqlite3_context *context, int argc, sqlite3_value **argv) {
  SirannonStream *stream;
  (void)argc;
  sirannonEnter();
  stream = argumentStream(context, argv[0]);
  if (stream && stream->failure) sqlite3_result_text(context, stream->failure, -1, SQLITE_TRANSIENT);
  else if (stream) sqlite3_result_null(context);
  sirannonLeave();
}

static void finishFunction(sqlite3_context *context, int argc, sqlite3_value **argv) {
  SirannonStream *stream;
  (void)argc;
  sirannonEnter();
  stream = argumentStream(context, argv[0]);
  if (stream && stream->failure) sqlite3_result_error(context, stream->failure, -1);
  else if (stream && sirannonStreamFinish(stream) != SQLITE_OK) sqlite3_result_error_nomem(context);
  else if (stream) sqlite3_result_int64(context, stream->size);
  sirannonLeave();
}

static void closeFunction(sqlite3_context *context, int argc, sqlite3_value **argv) {
  SirannonStream *stream;
  (void)argc;
  sirannonEnter();
  stream = argumentStream(context, argv[0]);
  if (stream && stream->openFiles > 0) {
    sqlite3_result_error(context, "the copy still holds the destination open, so its stream cannot be released yet", -1);
  } else if (stream) {
    sqlite3_result_int64(context, stream->size);
    sirannonStreamRelease(stream);
  }
  sirannonLeave();
}

static const sqlite3_api_routines *boundApi = 0;

typedef struct SirannonFunction {
  const char *name;
  int arguments;
  void (*implementation)(sqlite3_context *, int, sqlite3_value **);
} SirannonFunction;

static const SirannonFunction sirannonFunctions[] = {
  {"sirannon_vfs_version", 0, versionFunction},
  {"sirannon_stream_open", 3, openFunction},
  {"sirannon_stream_take", 1, takeFunction},
  {"sirannon_stream_written", 1, writtenFunction},
  {"sirannon_stream_error", 1, errorFunction},
  {"sirannon_stream_finish", 1, finishFunction},
  {"sirannon_stream_close", 1, closeFunction},
};

#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_sirannonvfs_init(sqlite3 *db, char **errorMessage, const sqlite3_api_routines *api) {
  size_t i;
  int rc;
  SQLITE_EXTENSION_INIT2(api);
  if (boundApi && boundApi != api) {
    *errorMessage = sqlite3_mprintf(
      "this process already loaded the Sirannon streaming extension into another SQLite build, and one process loads "
      "it into one build");
    sqlite3_api = boundApi;
    return SQLITE_ERROR;
  }
  boundApi = api;
  rc = sirannonVfsRegister();
  if (rc != SQLITE_OK) return rc;
  for (i = 0; i < sizeof(sirannonFunctions) / sizeof(sirannonFunctions[0]); i++) {
    rc = sqlite3_create_function(
      db,
      sirannonFunctions[i].name,
      sirannonFunctions[i].arguments,
      SQLITE_UTF8 | SQLITE_DIRECTONLY,
      0,
      sirannonFunctions[i].implementation,
      0,
      0);
    if (rc != SQLITE_OK) return rc;
  }
  return SQLITE_OK_LOAD_PERMANENTLY;
}
