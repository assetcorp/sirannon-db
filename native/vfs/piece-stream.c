#include "piece-stream.h"

#include <string.h>

#ifdef _WIN32
#include <windows.h>
#else
#include <time.h>
#endif

#include "runtime.h"

SQLITE_EXTENSION_INIT3

#define SIRANNON_FULL_QUEUE_WAIT_US 5000
#define SIRANNON_FULL_QUEUE_LIMIT_US 60000000
#define SIRANNON_STOPPED_CONSUMER_US 30000
#define SIRANNON_MAX_PIECE_INDEX 2147483647

static sqlite3_int64 monotonicMicroseconds(void) {
#ifdef _WIN32
  LARGE_INTEGER frequency;
  LARGE_INTEGER ticks;
  if (!QueryPerformanceFrequency(&frequency) || frequency.QuadPart <= 0) return 0;
  if (!QueryPerformanceCounter(&ticks)) return 0;
  return (sqlite3_int64)((ticks.QuadPart / frequency.QuadPart) * 1000000 +
                         ((ticks.QuadPart % frequency.QuadPart) * 1000000) / frequency.QuadPart);
#else
  struct timespec now;
  if (clock_gettime(CLOCK_MONOTONIC, &now) != 0) return 0;
  return (sqlite3_int64)now.tv_sec * 1000000 + now.tv_nsec / 1000;
#endif
}

static SirannonStream *sirannonStreams = 0;
static sqlite3_int64 sirannonNextStreamId = 1;

static void recordFailure(SirannonStream *stream, const char *message) {
  if (stream->failure) return;
  stream->failure = sqlite3_mprintf("%s", message);
  sirannonWakeWaiters();
}

SirannonStream *sirannonStreamById(sqlite3_int64 id) {
  SirannonStream *stream;
  for (stream = sirannonStreams; stream; stream = stream->next) {
    if (stream->id == id) return stream;
  }
  return 0;
}

int sirannonStreamIdFromName(const char *name, sqlite3_int64 *id) {
  size_t prefix = strlen(SIRANNON_STREAM_PREFIX);
  sqlite3_int64 parsed = 0;
  size_t digits = 0;
  size_t i;
  const char *cursor;
  if (!name) return 0;
  for (cursor = name; *cursor; cursor++) {
    if (*cursor == '/' || *cursor == '\\') name = cursor + 1;
  }
  if (strncmp(name, SIRANNON_STREAM_PREFIX, prefix) != 0) return 0;
  for (i = prefix; name[i] >= '0' && name[i] <= '9'; i++) {
    parsed = parsed * 10 + (name[i] - '0');
    digits++;
    if (digits > 18) return 0;
  }
  if (digits == 0 || name[i] != '\0') return 0;
  *id = parsed;
  return 1;
}

SirannonStream *sirannonStreamOpen(int pieceBytes, int maxQueued, int waitWhenFull) {
  SirannonStream *stream = (SirannonStream *)sqlite3_malloc(sizeof(SirannonStream));
  if (!stream) return 0;
  memset(stream, 0, sizeof(SirannonStream));
  stream->pieceBytes = pieceBytes;
  stream->maxQueued = maxQueued;
  stream->waitWhenFull = waitWhenFull;
  stream->currentIndex = -1;
  stream->consumerSeenAt = monotonicMicroseconds();
  stream->id = sirannonNextStreamId++;
  stream->next = sirannonStreams;
  sirannonStreams = stream;
  return stream;
}

void sirannonStreamConsumerSeen(SirannonStream *stream) {
  stream->consumerSeenAt = monotonicMicroseconds();
}

void sirannonPieceFree(SirannonPiece *piece) {
  sqlite3_free(piece->data);
  sqlite3_free(piece);
}

SirannonPiece *sirannonStreamTakePiece(SirannonStream *stream) {
  SirannonPiece *piece = stream->queueHead;
  stream->consumerSeenAt = monotonicMicroseconds();
  if (!piece) return 0;
  stream->queueHead = piece->next;
  if (!stream->queueHead) stream->queueTail = 0;
  stream->queued--;
  sirannonWakeWaiters();
  return piece;
}

static int enqueue(SirannonStream *stream, int index, unsigned char *data, int length) {
  SirannonPiece *piece = (SirannonPiece *)sqlite3_malloc(sizeof(SirannonPiece));
  if (!piece) return SQLITE_NOMEM;
  piece->next = 0;
  piece->index = index;
  piece->length = length;
  piece->data = data;
  if (stream->queueTail) stream->queueTail->next = piece;
  else stream->queueHead = piece;
  stream->queueTail = piece;
  stream->queued++;
  stream->piecesEmitted++;
  return SQLITE_OK;
}

static int pieceLength(SirannonStream *stream, int index) {
  sqlite3_int64 start = (sqlite3_int64)index * stream->pieceBytes;
  sqlite3_int64 remaining = stream->size - start;
  if (remaining <= 0) return 0;
  if (remaining > stream->pieceBytes) return stream->pieceBytes;
  return (int)remaining;
}

static int waitForQueue(SirannonStream *stream) {
  sqlite3_int64 idleFor;
  if (stream->maxQueued <= 0 || !stream->waitWhenFull) return SQLITE_OK;
  while (stream->queued >= stream->maxQueued) {
    if (stream->failure) return SQLITE_IOERR_WRITE;
    idleFor = monotonicMicroseconds() - stream->consumerSeenAt;
    if (idleFor >= SIRANNON_FULL_QUEUE_LIMIT_US) {
      recordFailure(
        stream,
        "the run took no piece from it for a whole minute, so the copy stopped rather than hold more of the database "
        "in memory");
      return SQLITE_IOERR_WRITE;
    }
    if (idleFor >= SIRANNON_STOPPED_CONSUMER_US) return SQLITE_OK;
    sirannonAwaitTurn(SIRANNON_FULL_QUEUE_WAIT_US);
  }
  return SQLITE_OK;
}

static int flushCurrent(SirannonStream *stream) {
  int rc;
  if (stream->currentIndex < 0) return SQLITE_OK;
  rc = enqueue(stream, stream->currentIndex, stream->current, pieceLength(stream, stream->currentIndex));
  if (rc != SQLITE_OK) return rc;
  stream->current = 0;
  stream->currentIndex = -1;
  return SQLITE_OK;
}

static int emitEmptyPiece(SirannonStream *stream, int index) {
  int length = pieceLength(stream, index);
  unsigned char *data;
  if (length <= 0) return SQLITE_OK;
  data = (unsigned char *)sqlite3_malloc(stream->pieceBytes);
  if (!data) return SQLITE_NOMEM;
  memset(data, 0, (size_t)stream->pieceBytes);
  return enqueue(stream, index, data, length);
}

static int pieceBuffer(SirannonStream *stream, int index, unsigned char **buffer) {
  int rc;
  int missing;
  if (index == 0) {
    if (!stream->first) {
      stream->first = (unsigned char *)sqlite3_malloc(stream->pieceBytes);
      if (!stream->first) return SQLITE_NOMEM;
      memset(stream->first, 0, (size_t)stream->pieceBytes);
    }
    *buffer = stream->first;
    return SQLITE_OK;
  }
  if (index == stream->currentIndex) {
    *buffer = stream->current;
    return SQLITE_OK;
  }
  if (index < stream->currentIndex) {
    recordFailure(
      stream,
      "SQLite wrote back into a part of the copy that had already gone to the destination, which this route cannot "
      "fetch back");
    return SQLITE_IOERR_WRITE;
  }
  rc = waitForQueue(stream);
  if (rc != SQLITE_OK) return rc;
  missing = stream->currentIndex < 0 ? 1 : stream->currentIndex + 1;
  rc = flushCurrent(stream);
  if (rc != SQLITE_OK) return rc;
  for (; missing < index; missing++) {
    rc = emitEmptyPiece(stream, missing);
    if (rc != SQLITE_OK) return rc;
  }
  stream->current = (unsigned char *)sqlite3_malloc(stream->pieceBytes);
  if (!stream->current) return SQLITE_NOMEM;
  memset(stream->current, 0, (size_t)stream->pieceBytes);
  stream->currentIndex = index;
  *buffer = stream->current;
  return SQLITE_OK;
}

int sirannonStreamWrite(SirannonStream *stream, const void *bytes, int amount, sqlite3_int64 offset) {
  const unsigned char *source = (const unsigned char *)bytes;
  if (stream->failure) return SQLITE_IOERR_WRITE;
  while (amount > 0) {
    sqlite3_int64 position = offset / stream->pieceBytes;
    int index;
    int within = (int)(offset % stream->pieceBytes);
    int room = stream->pieceBytes - within;
    int chunk = amount < room ? amount : room;
    unsigned char *buffer;
    int rc;
    if (position > SIRANNON_MAX_PIECE_INDEX) {
      recordFailure(stream, "the copy reached further than a piece index can count, so the piece size is too small for it");
      return SQLITE_IOERR_WRITE;
    }
    index = (int)position;
    if (offset + chunk > stream->size) stream->size = offset + chunk;
    rc = pieceBuffer(stream, index, &buffer);
    if (rc != SQLITE_OK) return rc;
    memcpy(buffer + within, source, (size_t)chunk);
    source += chunk;
    offset += chunk;
    amount -= chunk;
  }
  return SQLITE_OK;
}

int sirannonStreamRead(SirannonStream *stream, void *bytes, int amount, sqlite3_int64 offset) {
  unsigned char *target = (unsigned char *)bytes;
  int served = 0;
  if (stream->failure) return SQLITE_IOERR_READ;
  while (served < amount) {
    sqlite3_int64 at = offset + served;
    sqlite3_int64 position = at / stream->pieceBytes;
    int index = position > SIRANNON_MAX_PIECE_INDEX ? -1 : (int)position;
    int within = (int)(at % stream->pieceBytes);
    int room = stream->pieceBytes - within;
    int chunk = (amount - served) < room ? (amount - served) : room;
    if (at >= stream->size) break;
    if (index == 0) {
      if (stream->first) memcpy(target + served, stream->first + within, (size_t)chunk);
      else memset(target + served, 0, (size_t)chunk);
    }
    else if (index == stream->currentIndex) memcpy(target + served, stream->current + within, (size_t)chunk);
    else if (index > stream->currentIndex) memset(target + served, 0, (size_t)chunk);
    else {
      recordFailure(
        stream,
        "SQLite read back a part of the copy that had already gone to the destination, which this route cannot fetch "
        "back");
      return SQLITE_IOERR_READ;
    }
    served += chunk;
  }
  if (served < amount) {
    memset(target + served, 0, (size_t)(amount - served));
    return SQLITE_IOERR_SHORT_READ;
  }
  return SQLITE_OK;
}

int sirannonStreamTruncate(SirannonStream *stream, sqlite3_int64 size) {
  if (stream->failure) return SQLITE_IOERR_TRUNCATE;
  if (size < stream->size && stream->piecesEmitted > 0 &&
      size < (sqlite3_int64)stream->currentIndex * stream->pieceBytes) {
    recordFailure(
      stream,
      "SQLite shortened the copy past a part that had already gone to the destination, which this route cannot take "
      "back");
    return SQLITE_IOERR_TRUNCATE;
  }
  stream->size = size;
  return SQLITE_OK;
}

int sirannonStreamFinish(SirannonStream *stream) {
  int rc;
  if (stream->finished) return SQLITE_OK;
  rc = flushCurrent(stream);
  if (rc != SQLITE_OK) return rc;
  if (stream->first) {
    int length = pieceLength(stream, 0);
    if (length > 0) {
      rc = enqueue(stream, 0, stream->first, length);
      if (rc != SQLITE_OK) return rc;
    } else {
      sqlite3_free(stream->first);
    }
    stream->first = 0;
  }
  stream->finished = 1;
  return SQLITE_OK;
}

void sirannonStreamRelease(SirannonStream *stream) {
  SirannonStream **link = &sirannonStreams;
  SirannonPiece *piece = stream->queueHead;
  while (*link) {
    if (*link == stream) {
      *link = stream->next;
      break;
    }
    link = &(*link)->next;
  }
  while (piece) {
    SirannonPiece *next = piece->next;
    sirannonPieceFree(piece);
    piece = next;
  }
  sqlite3_free(stream->first);
  sqlite3_free(stream->current);
  sqlite3_free(stream->failure);
  sqlite3_free(stream);
}
