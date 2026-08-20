#ifndef SIRANNON_PIECE_STREAM_H
#define SIRANNON_PIECE_STREAM_H

#include "sqlite3ext.h"

#define SIRANNON_STREAM_PREFIX "sirannon-stream-"
#define SIRANNON_PIECE_HEADER_BYTES 8
#define SIRANNON_MIN_PIECE_BYTES 512
#define SIRANNON_MAX_PIECE_BYTES (256 * 1024 * 1024)

typedef struct SirannonPiece SirannonPiece;
typedef struct SirannonStream SirannonStream;

struct SirannonPiece {
  SirannonPiece *next;
  int index;
  int length;
  unsigned char *data;
};

struct SirannonStream {
  SirannonStream *next;
  sqlite3_int64 id;
  int pieceBytes;
  int maxQueued;
  int waitWhenFull;
  int stoppedTakerMicroseconds;
  unsigned char *first;
  unsigned char *current;
  int currentIndex;
  sqlite3_int64 size;
  sqlite3_int64 piecesEmitted;
  SirannonPiece *queueHead;
  SirannonPiece *queueTail;
  int queued;
  int finished;
  int openFiles;
  sqlite3_int64 takerSeenAt;
  sqlite3_int64 pieceLetPastAt;
  char *failure;
};

SirannonStream *sirannonStreamOpen(int pieceBytes, int maxQueued, int waitWhenFull,
                                   sqlite3_int64 stoppedTakerMicroseconds);
SirannonStream *sirannonStreamById(sqlite3_int64 id);
int sirannonStreamIdFromName(const char *name, sqlite3_int64 *id);
int sirannonStreamWrite(SirannonStream *stream, const void *bytes, int amount, sqlite3_int64 offset);
int sirannonStreamRead(SirannonStream *stream, void *bytes, int amount, sqlite3_int64 offset);
int sirannonStreamTruncate(SirannonStream *stream, sqlite3_int64 size);
int sirannonStreamFinish(SirannonStream *stream);
SirannonPiece *sirannonStreamTakePiece(SirannonStream *stream);
void sirannonStreamTakerSeen(SirannonStream *stream);
void sirannonPieceFree(SirannonPiece *piece);
void sirannonStreamRelease(SirannonStream *stream);

#endif
