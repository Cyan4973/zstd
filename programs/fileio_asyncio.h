/*
 * Synchronous IO adaptor preserving the historical AIO_* API.
 */
#ifndef ZSTD_FILEIO_ASYNCIO_H
#define ZSTD_FILEIO_ASYNCIO_H

#include <stdio.h>
#include "../lib/common/mem.h"
#include "fileio_types.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct WritePoolCtx_s {
    const FIO_prefs_t* prefs;
    size_t bufferSize;
    FILE* file;
    unsigned storedSkips;
    int asyncEnabled;
} WritePoolCtx_t;

typedef struct ReadPoolCtx_s {
    const FIO_prefs_t* prefs;
    FILE* file;
    size_t bufferSize;
    U8* primary;
    U8* coalesce;
    U8* current;
    size_t currentCapacity;
    size_t loaded;
    int reachedEof;
    int asyncEnabled;
} ReadPoolCtx_t;

typedef struct IOJob_s {
    WritePoolCtx_t* ctx;
    FILE* file;
    void* buffer;
    size_t bufferSize;
    size_t usedBufferSize;
    U64 offset;
} IOJob_t;

int AIO_supported(void);

WritePoolCtx_t* AIO_WritePool_create(const FIO_prefs_t* prefs, size_t bufferSize);
void AIO_WritePool_free(WritePoolCtx_t* ctx);
IOJob_t* AIO_WritePool_acquireJob(WritePoolCtx_t *ctx);
void AIO_WritePool_releaseIoJob(IOJob_t *job);
void AIO_WritePool_enqueueAndReacquireWriteJob(IOJob_t **job);
void AIO_WritePool_sparseWriteEnd(WritePoolCtx_t *ctx);
void AIO_WritePool_setFile(WritePoolCtx_t *ctx, FILE* file);
FILE* AIO_WritePool_getFile(const WritePoolCtx_t* ctx);
int  AIO_WritePool_closeFile(WritePoolCtx_t *ctx);
void AIO_WritePool_setAsync(WritePoolCtx_t* ctx, int async);

ReadPoolCtx_t* AIO_ReadPool_create(const FIO_prefs_t* prefs, size_t bufferSize);
void AIO_ReadPool_free(ReadPoolCtx_t* ctx);
void AIO_ReadPool_setAsync(ReadPoolCtx_t* ctx, int async);
void AIO_ReadPool_consumeBytes(ReadPoolCtx_t *ctx, size_t n);
size_t AIO_ReadPool_fillBuffer(ReadPoolCtx_t *ctx, size_t n);
size_t AIO_ReadPool_consumeAndRefill(ReadPoolCtx_t *ctx);
void AIO_ReadPool_setFile(ReadPoolCtx_t *ctx, FILE* file);
FILE* AIO_ReadPool_getFile(const ReadPoolCtx_t *ctx);
int  AIO_ReadPool_closeFile(ReadPoolCtx_t *ctx);

#ifdef __cplusplus
}
#endif

#endif /* ZSTD_FILEIO_ASYNCIO_H */
