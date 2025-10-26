/*
 * Synchronous implementation of the historical AIO_* interface.
 * This keeps existing call sites untouched while removing the
 * heavy thread-pool orchestration used previously.
 */
#include <assert.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>

#include "fileio_asyncio.h"
#include "fileio_common.h"
#include "util.h"
#include "platform.h"

/* Sparse write helpers copied from original implementation */
static unsigned AIO_sparseWrite(FILE* file,
                                const void* buffer, size_t bufferSize,
                                const FIO_prefs_t* const prefs,
                                unsigned storedSkips)
{
    const size_t* const bufferT = (const size_t*)buffer;
    size_t bufferSizeT = bufferSize / sizeof(size_t);
    const size_t* const bufferTEnd = bufferT + bufferSizeT;
    const size_t* ptrT = bufferT;
    static const size_t segmentSizeT = (32 KB) / sizeof(size_t);

    if (prefs->testMode) return 0;

    if (!prefs->sparseFileSupport) {
        size_t const sizeCheck = fwrite(buffer, 1, bufferSize, file);
        if (sizeCheck != bufferSize)
            EXM_THROW(70, "Write error : cannot write block : %s",
                      strerror(errno));
        return 0;
    }

    if (storedSkips > 1 GB) {
        if (LONG_SEEK(file, 1 GB, SEEK_CUR) != 0)
            EXM_THROW(91, "1 GB skip error (sparse file support)");
        storedSkips -= 1 GB;
    }

    while (ptrT < bufferTEnd) {
        size_t nb0T;
        size_t seg0SizeT = segmentSizeT;
        if (seg0SizeT > bufferSizeT) seg0SizeT = bufferSizeT;
        bufferSizeT -= seg0SizeT;
        for (nb0T=0; (nb0T < seg0SizeT) && (ptrT[nb0T] == 0); nb0T++) ;
        storedSkips += (unsigned)(nb0T * sizeof(size_t));
        if (nb0T != seg0SizeT) {
            size_t const nbNon0ST = seg0SizeT - nb0T;
            if (LONG_SEEK(file, storedSkips, SEEK_CUR) != 0)
                EXM_THROW(92, "Sparse skip error ; try --no-sparse");
            storedSkips = 0;
            if (fwrite(ptrT + nb0T, sizeof(size_t), nbNon0ST, file) != nbNon0ST)
                EXM_THROW(93, "Write error : cannot write block : %s",
                          strerror(errno));
        }
        ptrT += seg0SizeT;
    }

    {   static size_t const maskT = sizeof(size_t)-1;
        if (bufferSize & maskT) {
            const char* const restStart = (const char*)bufferTEnd;
            const char* restPtr = restStart;
            const char* const restEnd = (const char*)buffer + bufferSize;
            assert(restEnd > restStart && restEnd < restStart + sizeof(size_t));
            for ( ; (restPtr < restEnd) && (*restPtr == 0); restPtr++) ;
            storedSkips += (unsigned) (restPtr - restStart);
            if (restPtr != restEnd) {
                size_t const restSize = (size_t)(restEnd - restPtr);
                if (LONG_SEEK(file, storedSkips, SEEK_CUR) != 0)
                    EXM_THROW(92, "Sparse skip error ; try --no-sparse");
                if (fwrite(restPtr, 1, restSize, file) != restSize)
                    EXM_THROW(95, "Write error : cannot write end of decoded block : %s",
                              strerror(errno));
                storedSkips = 0;
            }   }   }
    return storedSkips;
}

static void AIO_sparseWriteEnd(const FIO_prefs_t* const prefs, FILE* file, unsigned storedSkips)
{
    if (!file) return;
    if (prefs->testMode) {
        assert(storedSkips == 0);
        return;
    }
    if (storedSkips>0) {
        if (LONG_SEEK(file, storedSkips-1, SEEK_CUR) != 0)
            EXM_THROW(69, "Final skip error (sparse file support)");
        {   const char lastZeroByte[1] = { 0 };
            if (fwrite(lastZeroByte, 1, 1, file) != 1)
                EXM_THROW(69, "Write error : cannot write last zero : %s", strerror(errno));
        }
    }
}

int AIO_supported(void)
{
#ifdef ZSTD_MULTITHREAD
    return 1;
#else
    return 0;
#endif
}

/* ========================= Write pool ======================= */

WritePoolCtx_t* AIO_WritePool_create(const FIO_prefs_t* prefs, size_t bufferSize)
{
    WritePoolCtx_t* ctx = (WritePoolCtx_t*)calloc(1, sizeof(*ctx));
    if (!ctx) EXM_THROW(21, "Not enough memory");
    ctx->prefs = prefs;
    ctx->bufferSize = bufferSize ? bufferSize : ZSTD_DStreamOutSize();
    ctx->file = NULL;
    ctx->storedSkips = 0;
    ctx->asyncEnabled = prefs->asyncIO && AIO_supported();
    return ctx;
}

void AIO_WritePool_free(WritePoolCtx_t* ctx)
{
    if (!ctx) return;
    if (ctx->file)
        AIO_WritePool_closeFile(ctx);
    free(ctx);
}

IOJob_t* AIO_WritePool_acquireJob(WritePoolCtx_t *ctx)
{
    IOJob_t* job = (IOJob_t*)malloc(sizeof(*job));
    if (!job) EXM_THROW(21, "Not enough memory");
    job->ctx = ctx;
    job->file = ctx->file;
    job->bufferSize = ctx->bufferSize;
    job->buffer = malloc(ctx->bufferSize);
    if (!job->buffer)
        EXM_THROW(21, "Not enough memory");
    job->usedBufferSize = 0;
    job->offset = 0;
    return job;
}

void AIO_WritePool_releaseIoJob(IOJob_t *job)
{
    if (!job) return;
    free(job->buffer);
    free(job);
}

static void AIO_WritePool_run(IOJob_t* job)
{
    WritePoolCtx_t* const ctx = job->ctx;
    if (ctx->file && !ctx->prefs->testMode && job->usedBufferSize) {
        ctx->storedSkips = AIO_sparseWrite(ctx->file,
                                           job->buffer,
                                           job->usedBufferSize,
                                           ctx->prefs,
                                           ctx->storedSkips);
    }
    job->usedBufferSize = 0;
    job->file = ctx->file;
}

void AIO_WritePool_enqueueAndReacquireWriteJob(IOJob_t **jobPtr)
{
    IOJob_t* const job = *jobPtr;
    AIO_WritePool_run(job);
    *jobPtr = job;
}

void AIO_WritePool_sparseWriteEnd(WritePoolCtx_t *ctx)
{
    if (!ctx || !ctx->file) return;
    AIO_sparseWriteEnd(ctx->prefs, ctx->file, ctx->storedSkips);
    ctx->storedSkips = 0;
}

void AIO_WritePool_setFile(WritePoolCtx_t *ctx, FILE* file)
{
    if (!ctx) return;
    if (ctx->file == file) return;
    if (ctx->file)
        AIO_WritePool_sparseWriteEnd(ctx);
    ctx->file = file;
    ctx->storedSkips = 0;
}

FILE* AIO_WritePool_getFile(const WritePoolCtx_t* ctx)
{
    return ctx ? ctx->file : NULL;
}

int AIO_WritePool_closeFile(WritePoolCtx_t *ctx)
{
    int ret = 0;
    if (!ctx || ctx->file == NULL)
        return 0;
    AIO_WritePool_sparseWriteEnd(ctx);
    ret = fclose(ctx->file);
    ctx->file = NULL;
    return ret;
}

void AIO_WritePool_setAsync(WritePoolCtx_t* ctx, int async)
{
    if (!ctx) return;
    ctx->asyncEnabled = async && AIO_supported();
}

/* ========================= Read pool ======================== */

ReadPoolCtx_t* AIO_ReadPool_create(const FIO_prefs_t* prefs, size_t bufferSize)
{
    ReadPoolCtx_t* ctx = (ReadPoolCtx_t*)calloc(1, sizeof(*ctx));
    if (!ctx) EXM_THROW(21, "Not enough memory");
    ctx->prefs = prefs;
    ctx->bufferSize = bufferSize ? bufferSize : ZSTD_DStreamInSize();
    ctx->primary = (U8*)malloc(ctx->bufferSize);
    ctx->coalesce = (U8*)malloc(ctx->bufferSize * 2);
    if (!ctx->primary || !ctx->coalesce)
        EXM_THROW(21, "Not enough memory");
    ctx->current = ctx->primary;
    ctx->currentCapacity = ctx->bufferSize;
    ctx->loaded = 0;
    ctx->file = NULL;
    ctx->reachedEof = 0;
    ctx->asyncEnabled = prefs->asyncIO;
    return ctx;
}

void AIO_ReadPool_free(ReadPoolCtx_t* ctx)
{
    if (!ctx) return;
    if (ctx->file)
        fclose(ctx->file);
    free(ctx->primary);
    free(ctx->coalesce);
    free(ctx);
}

void AIO_ReadPool_setAsync(ReadPoolCtx_t* ctx, int async)
{
    if (!ctx) return;
    ctx->asyncEnabled = async;
}

static void AIO_ReadPool_reset(ReadPoolCtx_t* ctx)
{
    ctx->current = ctx->primary;
    ctx->currentCapacity = ctx->bufferSize;
    ctx->loaded = 0;
    ctx->reachedEof = 0;
}

static void AIO_ReadPool_normalize(ReadPoolCtx_t* ctx)
{
    if (ctx->current != ctx->primary && ctx->loaded <= ctx->bufferSize) {
        if (ctx->loaded)
            memmove(ctx->primary, ctx->current, ctx->loaded);
        ctx->current = ctx->primary;
        ctx->currentCapacity = ctx->bufferSize;
    } else if (ctx->current != ctx->primary) {
        if (ctx->loaded)
            memmove(ctx->coalesce, ctx->current, ctx->loaded);
        ctx->current = ctx->coalesce;
        ctx->currentCapacity = ctx->bufferSize * 2;
    }
}

size_t AIO_ReadPool_fillBuffer(ReadPoolCtx_t *ctx, size_t n)
{
    size_t added = 0;
    if (!ctx || ctx->file == NULL)
        return 0;

    if (n > ctx->bufferSize * 2)
        n = ctx->bufferSize * 2;

    if (ctx->loaded >= n)
        return 0;

    if (n > ctx->currentCapacity) {
        ctx->current = ctx->coalesce;
        ctx->currentCapacity = ctx->bufferSize * 2;
    }

    AIO_ReadPool_normalize(ctx);

    while (ctx->loaded < n && !ctx->reachedEof) {
        size_t const room = ctx->currentCapacity - ctx->loaded;
        size_t const readBytes = fread(ctx->current + ctx->loaded, 1, room, ctx->file);
        if (readBytes == 0) {
            if (ferror(ctx->file))
                EXM_THROW(37, "Read error");
            ctx->reachedEof = 1;
            break;
        }
        ctx->loaded += readBytes;
        added += readBytes;
        if (readBytes < room)
            ctx->reachedEof = 1;
    }
    return added;
}

void AIO_ReadPool_consumeBytes(ReadPoolCtx_t *ctx, size_t n)
{
    assert(ctx);
    assert(n <= ctx->loaded);
    ctx->current += n;
    ctx->loaded -= n;
    if (ctx->loaded == 0)
        AIO_ReadPool_reset(ctx);
}

size_t AIO_ReadPool_consumeAndRefill(ReadPoolCtx_t *ctx)
{
    if (!ctx) return 0;
    AIO_ReadPool_reset(ctx);
    return AIO_ReadPool_fillBuffer(ctx, ctx->bufferSize);
}

void AIO_ReadPool_setFile(ReadPoolCtx_t *ctx, FILE* file)
{
    if (!ctx) return;
    ctx->file = file;
    AIO_ReadPool_reset(ctx);
}

FILE* AIO_ReadPool_getFile(const ReadPoolCtx_t *ctx)
{
    return ctx ? ctx->file : NULL;
}

int AIO_ReadPool_closeFile(ReadPoolCtx_t *ctx)
{
    int ret = 0;
    if (!ctx || ctx->file == NULL)
        return 0;
    ret = fclose(ctx->file);
    ctx->file = NULL;
    AIO_ReadPool_reset(ctx);
    return ret;
}
