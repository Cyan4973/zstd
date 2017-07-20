/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include <stdio.h>      /* fprintf */
#include <stdlib.h>     /* malloc, free */
#include <pthread.h>    /* pthread functions */
#include <string.h>     /* memset */
#include "zstd_internal.h"
#include "util.h"

#define DISPLAY(...) fprintf(stderr, __VA_ARGS__)
#define PRINT(...) fprintf(stdout, __VA_ARGS__)
#define DEBUG(l, ...) { if (g_displayLevel>=l) { DISPLAY(__VA_ARGS__); } }
#define FILE_CHUNK_SIZE 4 << 20
#define MAX_NUM_JOBS 2
#define stdinmark  "/*stdin*\\"
#define stdoutmark "/*stdout*\\"
#define MAX_PATH 256
#define DEFAULT_DISPLAY_LEVEL 1
#define DEFAULT_COMPRESSION_LEVEL 6
#define DEFAULT_ADAPT_PARAM 0
#define MAX_COMPRESSION_LEVEL_CHANGE 4

static int g_displayLevel = DEFAULT_DISPLAY_LEVEL;
static unsigned g_compressionLevel = DEFAULT_COMPRESSION_LEVEL;
static UTIL_time_t g_startTime;
static size_t g_streamedSize = 0;
static unsigned g_useProgressBar = 0;
static UTIL_freq_t g_ticksPerSecond;
static unsigned g_forceCompressionLevel = 0;

typedef struct {
    void* start;
    size_t size;
    size_t capacity;
} buffer_t;

typedef struct {
    size_t filled;
    buffer_t buffer;
} inBuff_t;

typedef struct {
    buffer_t src;
    buffer_t dst;
    unsigned compressionLevel;
    unsigned jobID;
    unsigned lastJob;
    size_t compressedSize;
    size_t dictSize;
} jobDescription;

typedef struct {
    pthread_mutex_t pMutex;
    int noError;
} mutex_t;

typedef struct {
    pthread_cond_t pCond;
    int noError;
} cond_t;

typedef struct {
    unsigned compressionLevel;
    unsigned numActiveThreads;
    unsigned numJobs;
    unsigned nextJobID;
    unsigned threadError;
    unsigned jobReadyID;
    unsigned jobCompressedID;
    unsigned jobWriteID;
    unsigned allJobsCompleted;
    unsigned adaptParam;
    double compressionCompletionMeasured;
    double writeCompletionMeasured;
    double createCompletionMeasured;
    double compressionCompletion;
    double writeCompletion;
    double createCompletion;
    mutex_t jobCompressed_mutex;
    cond_t jobCompressed_cond;
    mutex_t jobReady_mutex;
    cond_t jobReady_cond;
    mutex_t allJobsCompleted_mutex;
    cond_t allJobsCompleted_cond;
    mutex_t jobWrite_mutex;
    cond_t jobWrite_cond;
    mutex_t completion_mutex;
    mutex_t wait_mutex;
    size_t lastDictSize;
    inBuff_t input;
    jobDescription* jobs;
    ZSTD_CCtx* cctx;
} adaptCCtx;

typedef struct {
    adaptCCtx* ctx;
    FILE* dstFile;
} outputThreadArg;

typedef struct {
    FILE* srcFile;
    adaptCCtx* ctx;
    outputThreadArg* otArg;
} fcResources;

static void freeCompressionJobs(adaptCCtx* ctx)
{
    unsigned u;
    for (u=0; u<ctx->numJobs; u++) {
        jobDescription job = ctx->jobs[u];
        free(job.dst.start);
        free(job.src.start);
    }
}

static int destroyMutex(mutex_t* mutex)
{
    if (mutex->noError) {
        int const ret = pthread_mutex_destroy(&mutex->pMutex);
        return ret;
    }
    return 0;
}

static int destroyCond(cond_t* cond)
{
    if (cond->noError) {
        int const ret = pthread_cond_destroy(&cond->pCond);
        return ret;
    }
    return 0;
}

static int freeCCtx(adaptCCtx* ctx)
{
    if (!ctx) return 0;
    {
        int error = 0;
        error |= destroyMutex(&ctx->jobCompressed_mutex);
        error |= destroyCond(&ctx->jobCompressed_cond);
        error |= destroyMutex(&ctx->jobReady_mutex);
        error |= destroyCond(&ctx->jobReady_cond);
        error |= destroyMutex(&ctx->allJobsCompleted_mutex);
        error |= destroyCond(&ctx->allJobsCompleted_cond);
        error |= destroyMutex(&ctx->jobWrite_mutex);
        error |= destroyCond(&ctx->jobWrite_cond);
        error |= destroyMutex(&ctx->completion_mutex);
        error |= destroyMutex(&ctx->wait_mutex);
        error |= ZSTD_isError(ZSTD_freeCCtx(ctx->cctx));
        free(ctx->input.buffer.start);
        if (ctx->jobs){
            freeCompressionJobs(ctx);
            free(ctx->jobs);
        }
        free(ctx);
        return error;
    }
}

static int initMutex(mutex_t* mutex)
{
    int const ret = pthread_mutex_init(&mutex->pMutex, NULL);
    mutex->noError = !ret;
    return ret;
}

static int initCond(cond_t* cond)
{
    int const ret = pthread_cond_init(&cond->pCond, NULL);
    cond->noError = !ret;
    return ret;
}

static int initCCtx(adaptCCtx* ctx, unsigned numJobs)
{
    ctx->compressionLevel = g_compressionLevel;
    {
        int pthreadError = 0;
        pthreadError |= initMutex(&ctx->jobCompressed_mutex);
        pthreadError |= initCond(&ctx->jobCompressed_cond);
        pthreadError |= initMutex(&ctx->jobReady_mutex);
        pthreadError |= initCond(&ctx->jobReady_cond);
        pthreadError |= initMutex(&ctx->allJobsCompleted_mutex);
        pthreadError |= initCond(&ctx->allJobsCompleted_cond);
        pthreadError |= initMutex(&ctx->jobWrite_mutex);
        pthreadError |= initCond(&ctx->jobWrite_cond);
        pthreadError |= initMutex(&ctx->completion_mutex);
        pthreadError |= initMutex(&ctx->wait_mutex);
        if (pthreadError) return pthreadError;
    }
    ctx->numJobs = numJobs;
    ctx->jobReadyID = 0;
    ctx->jobCompressedID = 0;
    ctx->jobWriteID = 0;
    ctx->lastDictSize = 0;
    ctx->createCompletionMeasured = 1;
    ctx->compressionCompletionMeasured = 1;
    ctx->writeCompletionMeasured = 1;

    ctx->jobs = calloc(1, numJobs*sizeof(jobDescription));

    if (!ctx->jobs) {
        DISPLAY("Error: could not allocate space for jobs during context creation\n");
        return 1;
    }

    /* initializing jobs */
    {
        unsigned jobNum;
        for (jobNum=0; jobNum<numJobs; jobNum++) {
            jobDescription* job = &ctx->jobs[jobNum];
            job->src.start = malloc(2 * FILE_CHUNK_SIZE);
            job->dst.start = malloc(ZSTD_compressBound(FILE_CHUNK_SIZE));
            job->lastJob = 0;
            if (!job->src.start || !job->dst.start) {
                DISPLAY("Could not allocate buffers for jobs\n");
                return 1;
            }
            job->src.capacity = FILE_CHUNK_SIZE;
            job->dst.capacity = ZSTD_compressBound(FILE_CHUNK_SIZE);
        }
    }

    ctx->nextJobID = 0;
    ctx->threadError = 0;
    ctx->allJobsCompleted = 0;
    ctx->adaptParam = DEFAULT_ADAPT_PARAM;

    ctx->cctx = ZSTD_createCCtx();
    if (!ctx->cctx) {
        DISPLAY("Error: could not allocate ZSTD_CCtx\n");
        return 1;
    }

    ctx->input.filled = 0;
    ctx->input.buffer.capacity = 2 * FILE_CHUNK_SIZE;

    ctx->input.buffer.start = malloc(ctx->input.buffer.capacity);
    if (!ctx->input.buffer.start) {
        DISPLAY("Error: could not allocate input buffer\n");
        return 1;
    }
    return 0;
}

static adaptCCtx* createCCtx(unsigned numJobs)
{

    adaptCCtx* const ctx = calloc(1, sizeof(adaptCCtx));
    if (ctx == NULL) {
        DISPLAY("Error: could not allocate space for context\n");
        return NULL;
    }
    {
        int const error = initCCtx(ctx, numJobs);
        if (error) {
            freeCCtx(ctx);
            return NULL;
        }
        return ctx;
    }
}

static void signalErrorToThreads(adaptCCtx* ctx)
{
    ctx->threadError = 1;
    pthread_mutex_lock(&ctx->jobReady_mutex.pMutex);
    pthread_cond_signal(&ctx->jobReady_cond.pCond);
    pthread_mutex_unlock(&ctx->jobReady_mutex.pMutex);

    pthread_mutex_lock(&ctx->jobCompressed_mutex.pMutex);
    pthread_cond_signal(&ctx->jobCompressed_cond.pCond);
    pthread_mutex_unlock(&ctx->jobReady_mutex.pMutex);

    pthread_mutex_lock(&ctx->jobWrite_mutex.pMutex);
    pthread_cond_signal(&ctx->jobWrite_cond.pCond);
    pthread_mutex_unlock(&ctx->jobWrite_mutex.pMutex);

    pthread_mutex_lock(&ctx->allJobsCompleted_mutex.pMutex);
    pthread_cond_signal(&ctx->allJobsCompleted_cond.pCond);
    pthread_mutex_unlock(&ctx->allJobsCompleted_mutex.pMutex);
}

static void waitUntilAllJobsCompleted(adaptCCtx* ctx)
{
    if (!ctx) return;
    pthread_mutex_lock(&ctx->allJobsCompleted_mutex.pMutex);
    while (ctx->allJobsCompleted == 0 && !ctx->threadError) {
        pthread_cond_wait(&ctx->allJobsCompleted_cond.pCond, &ctx->allJobsCompleted_mutex.pMutex);
    }
    pthread_mutex_unlock(&ctx->allJobsCompleted_mutex.pMutex);
}

/*
 * Compression level is changed depending on which part of the compression process is lagging
 * Currently, three theads exist for job creation, compression, and file writing respectively.
 * adaptCompressionLevel() increments or decrements compression level based on which of the threads is lagging
 * job creation or file writing lag => increased compression level
 * compression thread lag           => decreased compression level
 * detecting which thread is lagging is done by keeping track of how many calls each thread makes to pthread_cond_wait
 */
static void adaptCompressionLevel(adaptCCtx* ctx)
{
    if (g_forceCompressionLevel) {
        ctx->compressionLevel = g_compressionLevel;
    }
    else {
        DEBUG(2, "compression level %u\n", ctx->compressionLevel);
        /* check if compression is too slow */
        unsigned createChange;
        unsigned writeChange;
        unsigned compressionChange;
        pthread_mutex_lock(&ctx->completion_mutex.pMutex);
        createChange = MAX_COMPRESSION_LEVEL_CHANGE - ctx->createCompletionMeasured * MAX_COMPRESSION_LEVEL_CHANGE;
        writeChange = MAX_COMPRESSION_LEVEL_CHANGE - ctx->writeCompletionMeasured * MAX_COMPRESSION_LEVEL_CHANGE;
        compressionChange = MAX_COMPRESSION_LEVEL_CHANGE - ctx->compressionCompletionMeasured * MAX_COMPRESSION_LEVEL_CHANGE;
        DEBUG(2, "createCompletionMeasured %f\n", ctx->createCompletionMeasured);
        DEBUG(2, "compressionCompletionMeasured %f\n", ctx->compressionCompletionMeasured);
        DEBUG(2, "writeCompletionMeasured %f\n", ctx->writeCompletionMeasured);
        pthread_mutex_unlock(&ctx->completion_mutex.pMutex);

        {
            unsigned const compressionFastChange = MIN(MIN(createChange, writeChange), ZSTD_maxCLevel() - ctx->compressionLevel);
            DEBUG(2, "compressionFastChange %u\n", compressionFastChange);

            if (compressionFastChange) {
                DEBUG(2, "compression level too low\n");
                ctx->compressionLevel += compressionFastChange;
            }
            else {
                unsigned const compressionSlowChange = MIN(compressionChange, ctx->compressionLevel-1);
                DEBUG(2, "compression level too high\n");
                ctx->compressionLevel -= compressionSlowChange;
            }
        }

        /* reset */
        pthread_mutex_lock(&ctx->completion_mutex.pMutex);
        ctx->createCompletionMeasured = 1;
        ctx->compressionCompletionMeasured = 1;
        ctx->writeCompletionMeasured = 1;
        pthread_mutex_unlock(&ctx->completion_mutex.pMutex);
        DEBUG(2, "\n");
    }
}

static size_t getUseableDictSize(unsigned compressionLevel)
{
    ZSTD_parameters params = ZSTD_getParams(compressionLevel, 0, 0);
    unsigned overlapLog = compressionLevel >= (unsigned)ZSTD_maxCLevel() ? 0 : 3;
    size_t overlapSize = 1 << (params.cParams.windowLog - overlapLog);
    return overlapSize;
}

static void* compressionThread(void* arg)
{
    adaptCCtx* ctx = (adaptCCtx*)arg;
    unsigned currJob = 0;
    for ( ; ; ) {
        unsigned const currJobIndex = currJob % ctx->numJobs;
        jobDescription* job = &ctx->jobs[currJobIndex];
        DEBUG(3, "compressionThread(): waiting on job ready\n");

        /* new job, reset completion */
        pthread_mutex_lock(&ctx->completion_mutex.pMutex);
        ctx->compressionCompletion = 0;
        pthread_mutex_unlock(&ctx->completion_mutex.pMutex);

        pthread_mutex_lock(&ctx->jobReady_mutex.pMutex);
        while(currJob + 1 > ctx->jobReadyID && !ctx->threadError) {
            pthread_mutex_lock(&ctx->completion_mutex.pMutex);
            /* compression thread is waiting, take measurements of write completion and read completion */
            ctx->createCompletionMeasured = ctx->createCompletion;
            ctx->writeCompletionMeasured = ctx->writeCompletion;
            DEBUG(2, "compression thread waiting : createCompletionMeasured %f : writeCompletionMeasured %f\n", ctx->createCompletionMeasured, ctx->writeCompletionMeasured);
            DEBUG(3, "create completion: %f\n", ctx->createCompletion);
            pthread_mutex_unlock(&ctx->completion_mutex.pMutex);
            DEBUG(3, "waiting on job ready, nextJob: %u\n", currJob);
            pthread_cond_wait(&ctx->jobReady_cond.pCond, &ctx->jobReady_mutex.pMutex);
        }
        pthread_mutex_unlock(&ctx->jobReady_mutex.pMutex);

        /* reset create completion */
        pthread_mutex_lock(&ctx->completion_mutex.pMutex);
        ctx->createCompletion = 0;
        pthread_mutex_unlock(&ctx->completion_mutex.pMutex);

        DEBUG(3, "compressionThread(): continuing after job ready\n");
        DEBUG(3, "DICTIONARY ENDED\n");
        DEBUG(3, "%.*s", (int)job->src.size, (char*)job->src.start);

        /* adapt compression level */
        adaptCompressionLevel(ctx);

        /* compress the data */
        {
            size_t const compressionBlockSize = 1 << 17; /* 128 KB */
            unsigned const cLevel = ctx->compressionLevel;
            unsigned blockNum = 0;
            size_t remaining = job->src.size;
            size_t srcPos = 0;
            size_t dstPos = 0;
            DEBUG(3, "cLevel used: %u\n", cLevel);
            DEBUG(3, "compression level used: %u\n", cLevel);

            /* reset compressed size */
            job->compressedSize = 0;

            /* begin compression */
            {
                size_t const useDictSize = MIN(getUseableDictSize(cLevel), job->dictSize);
                DEBUG(3, "useDictSize: %zu, job->dictSize: %zu\n", useDictSize, job->dictSize);
                size_t const dictModeError = ZSTD_setCCtxParameter(ctx->cctx, ZSTD_p_forceRawDict, 1);
                size_t const initError = ZSTD_compressBegin_usingDict(ctx->cctx, job->src.start + job->dictSize - useDictSize, useDictSize, cLevel);
                size_t const windowSizeError = ZSTD_setCCtxParameter(ctx->cctx, ZSTD_p_forceWindow, 1);
                if (ZSTD_isError(dictModeError) || ZSTD_isError(initError) || ZSTD_isError(windowSizeError)) {
                    DISPLAY("Error: something went wrong while starting compression\n");
                    signalErrorToThreads(ctx);
                    return arg;
                }
            }

            do {
                size_t const actualBlockSize = MIN(remaining, compressionBlockSize);
                DEBUG(3, "remaining: %zu\n", remaining);
                DEBUG(3, "actualBlockSize: %zu\n", actualBlockSize);

                /* continue compression */
                if (currJob != 0 || blockNum != 0) { /* not first block of first job flush/overwrite the frame header */
                    size_t const hSize = ZSTD_compressContinue(ctx->cctx, job->dst.start + dstPos, job->dst.capacity - dstPos, job->src.start + job->dictSize + srcPos, 0);
                    if (ZSTD_isError(hSize)) {
                        DISPLAY("Error: something went wrong while continuing compression\n");
                        job->compressedSize = hSize;
                        signalErrorToThreads(ctx);
                        return arg;
                    }
                    ZSTD_invalidateRepCodes(ctx->cctx);
                }
                {
                    DEBUG(3, "write out ending: %d\n", job->lastJob && (remaining == actualBlockSize));
                    DEBUG(3, "lastJob %u\n", job->lastJob);
                    DEBUG(3, "compressionBlockSize %zu\n", compressionBlockSize);
                    size_t const ret = (job->lastJob && remaining == actualBlockSize) ?
                                            ZSTD_compressEnd     (ctx->cctx, job->dst.start + dstPos, job->dst.capacity - dstPos, job->src.start + job->dictSize + srcPos, actualBlockSize) :
                                            ZSTD_compressContinue(ctx->cctx, job->dst.start + dstPos, job->dst.capacity - dstPos, job->src.start + job->dictSize + srcPos, actualBlockSize);
                    if (ZSTD_isError(ret)) {
                        DISPLAY("Error: something went wrong during compression: %s\n", ZSTD_getErrorName(ret));
                        signalErrorToThreads(ctx);
                        return arg;
                    }
                    job->compressedSize += ret;
                    remaining -= actualBlockSize;
                    srcPos += actualBlockSize;
                    dstPos += ret;
                    blockNum++;

                    /* update completion */
                    pthread_mutex_lock(&ctx->completion_mutex.pMutex);
                    ctx->compressionCompletion = 1 - (double)remaining/job->src.size;
                    DEBUG(3, "update on job %u: compression completion %f\n", currJob, ctx->compressionCompletion);
                    pthread_mutex_unlock(&ctx->completion_mutex.pMutex);
                }
            } while (remaining != 0);
            job->dst.size = job->compressedSize;
        }
        pthread_mutex_lock(&ctx->jobCompressed_mutex.pMutex);
        ctx->jobCompressedID++;
        DEBUG(3, "signaling for job %u\n", currJob);
        pthread_cond_signal(&ctx->jobCompressed_cond.pCond);
        pthread_mutex_unlock(&ctx->jobCompressed_mutex.pMutex);
        DEBUG(3, "finished job compression %u\n", currJob);
        currJob++;
        if (job->lastJob || ctx->threadError) {
            /* finished compressing all jobs */
            DEBUG(3, "all jobs finished compressing\n");
            break;
        }
    }
    return arg;
}

static void displayProgress(unsigned jobDoneID, unsigned cLevel, unsigned last)
{
    if (!g_useProgressBar) return;
    UTIL_time_t currTime;
    UTIL_getTime(&currTime);
    double const timeElapsed = (double)(UTIL_getSpanTimeMicro(g_ticksPerSecond, g_startTime, currTime) / 1000.0);
    double const sizeMB = (double)g_streamedSize / (1 << 20);
    double const avgCompRate = sizeMB * 1000 / timeElapsed;
    fprintf(stderr, "\r| %4u jobs completed | Current Compresion Level: %2u | Time Elapsed: %5.0f ms | Data Size: %7.1f MB | Avg Compression Rate: %6.2f MB/s |", jobDoneID, cLevel, timeElapsed, sizeMB, avgCompRate);
    if (last) {
        fprintf(stderr, "\n");
    }
    else {
        fflush(stderr);
    }
}

static void* outputThread(void* arg)
{
    outputThreadArg* const otArg = (outputThreadArg*)arg;
    adaptCCtx* const ctx = otArg->ctx;
    FILE* const dstFile = otArg->dstFile;

    unsigned currJob = 0;
    for ( ; ; ) {
        unsigned const currJobIndex = currJob % ctx->numJobs;
        jobDescription* job = &ctx->jobs[currJobIndex];
        DEBUG(3, "outputThread(): waiting on job compressed\n");

        /* new job, reset completion */
        pthread_mutex_lock(&ctx->completion_mutex.pMutex);
        ctx->writeCompletion = 0;
        pthread_mutex_unlock(&ctx->completion_mutex.pMutex);

        pthread_mutex_lock(&ctx->jobCompressed_mutex.pMutex);
        while (currJob + 1 > ctx->jobCompressedID && !ctx->threadError) {
            pthread_mutex_lock(&ctx->completion_mutex.pMutex);
            /* write thread is waiting, take measurement of compression completion */
            ctx->compressionCompletionMeasured = ctx->compressionCompletion;
            DEBUG(2, "write thread waiting : compressionCompletionMeasured %f\n", ctx->compressionCompletionMeasured);
            pthread_mutex_unlock(&ctx->completion_mutex.pMutex);
            DEBUG(3, "waiting on job compressed, nextJob: %u\n", currJob);
            pthread_cond_wait(&ctx->jobCompressed_cond.pCond, &ctx->jobCompressed_mutex.pMutex);
        }
        pthread_mutex_unlock(&ctx->jobCompressed_mutex.pMutex);

        /* reset compression completion */
        pthread_mutex_lock(&ctx->completion_mutex.pMutex);
        ctx->compressionCompletion = 0;
        pthread_mutex_unlock(&ctx->completion_mutex.pMutex);

        DEBUG(3, "outputThread(): continuing after job compressed\n");
        {
            size_t const compressedSize = job->compressedSize;
            size_t remaining = compressedSize;
            if (ZSTD_isError(compressedSize)) {
                DISPLAY("Error: an error occurred during compression\n");
                signalErrorToThreads(ctx);
                return arg;
            }
            {
                // size_t const writeSize = fwrite(job->dst.start, 1, compressedSize, dstFile);
                size_t const blockSize = compressedSize >> 7;
                size_t pos = 0;
                for ( ; ; ) {
                    size_t const writeSize = MIN(remaining, blockSize);
                    size_t const ret = fwrite(job->dst.start + pos, 1, writeSize, dstFile);
                    if (ret != writeSize) break;
                    pos += ret;
                    remaining -= ret;

                    /* update completion variable for writing */
                    pthread_mutex_lock(&ctx->completion_mutex.pMutex);
                    ctx->writeCompletion = 1 - (double)remaining/compressedSize;
                    pthread_mutex_unlock(&ctx->completion_mutex.pMutex);

                    if (remaining == 0) break;
                }
                if (pos != compressedSize) {
                    DISPLAY("Error: an error occurred during file write operation\n");
                    signalErrorToThreads(ctx);
                    return arg;
                }
            }
        }
        DEBUG(3, "finished job write %u\n", currJob);
        currJob++;
        displayProgress(currJob, ctx->compressionLevel, job->lastJob);
        DEBUG(3, "locking job write mutex\n");
        pthread_mutex_lock(&ctx->jobWrite_mutex.pMutex);
        ctx->jobWriteID++;
        pthread_cond_signal(&ctx->jobWrite_cond.pCond);
        pthread_mutex_unlock(&ctx->jobWrite_mutex.pMutex);
        DEBUG(3, "unlocking job write mutex\n");

        if (job->lastJob || ctx->threadError) {
            /* finished with all jobs */
            DEBUG(3, "all jobs finished writing\n");
            pthread_mutex_lock(&ctx->allJobsCompleted_mutex.pMutex);
            ctx->allJobsCompleted = 1;
            pthread_cond_signal(&ctx->allJobsCompleted_cond.pCond);
            pthread_mutex_unlock(&ctx->allJobsCompleted_mutex.pMutex);
            break;
        }

    }
    return arg;
}

static int createCompressionJob(adaptCCtx* ctx, size_t srcSize, int last)
{
    unsigned const nextJob = ctx->nextJobID;
    unsigned const nextJobIndex = nextJob % ctx->numJobs;
    jobDescription* job = &ctx->jobs[nextJobIndex];
    DEBUG(3, "createCompressionJob(): wait for job write\n");
    pthread_mutex_lock(&ctx->jobWrite_mutex.pMutex);
    DEBUG(3, "Creating new compression job -- nextJob: %u, jobCompressedID: %u, jobWriteID: %u, numJObs: %u\n", nextJob,ctx->jobCompressedID, ctx->jobWriteID, ctx->numJobs);
    while (nextJob - ctx->jobWriteID >= ctx->numJobs && !ctx->threadError) {
        pthread_mutex_lock(&ctx->completion_mutex.pMutex);
        /* creation thread is waiting, take measurement of compression completion */
        ctx->compressionCompletionMeasured = ctx->compressionCompletion;
        DEBUG(2, "creation thread waiting : compression completion measured : %f\n", ctx->compressionCompletionMeasured);
        DEBUG(3, "writeCompletion: %f\n", ctx->writeCompletion);
        pthread_mutex_unlock(&ctx->completion_mutex.pMutex);
        DEBUG(3, "waiting on job Write, nextJob: %u\n", nextJob);
        pthread_cond_wait(&ctx->jobWrite_cond.pCond, &ctx->jobWrite_mutex.pMutex);
    }
    pthread_mutex_unlock(&ctx->jobWrite_mutex.pMutex);

    /* reset write completion */
    pthread_mutex_lock(&ctx->completion_mutex.pMutex);
    ctx->writeCompletion = 0;
    pthread_mutex_unlock(&ctx->completion_mutex.pMutex);
    DEBUG(3, "createCompressionJob(): continuing after job write\n");

    DEBUG(3, "filled: %zu, srcSize: %zu\n", ctx->input.filled, srcSize);
    job->compressionLevel = ctx->compressionLevel;
    job->src.size = srcSize;
    job->jobID = nextJob;
    job->lastJob = last;
    {
        /* swap buffer */
        void* const copy = job->src.start;
        job->src.start = ctx->input.buffer.start;
        ctx->input.buffer.start = copy;
    }
    job->dictSize = ctx->lastDictSize;

    DEBUG(3, "finished job creation %u\n", nextJob);
    ctx->nextJobID++;
    DEBUG(3, "filled: %zu, srcSize: %zu\n", ctx->input.filled, srcSize);
    /* if not on the last job, reuse data as dictionary in next job */
    if (!last) {
        size_t const oldDictSize = ctx->lastDictSize;
        DEBUG(3, "oldDictSize %zu\n", oldDictSize);
        memcpy(ctx->input.buffer.start, job->src.start + oldDictSize, srcSize);
        ctx->lastDictSize = srcSize;
        ctx->input.filled = srcSize;
    }

    /* signal job ready */
    pthread_mutex_lock(&ctx->jobReady_mutex.pMutex);
    ctx->jobReadyID++;
    pthread_cond_signal(&ctx->jobReady_cond.pCond);
    pthread_mutex_unlock(&ctx->jobReady_mutex.pMutex);

    return 0;
}

static int performCompression(adaptCCtx* ctx, FILE* const srcFile, outputThreadArg* otArg)
{
    if (!ctx || !srcFile || !otArg) {
        return 1;
    }

    /* create output thread */
    {
        pthread_t out;
        if (pthread_create(&out, NULL, &outputThread, otArg)) {
            DISPLAY("Error: could not create output thread\n");
            signalErrorToThreads(ctx);
            return 1;
        }
    }

    /* create compression thread */
    {
        pthread_t compression;
        if (pthread_create(&compression, NULL, &compressionThread, ctx)) {
            DISPLAY("Error: could not create compression thread\n");
            signalErrorToThreads(ctx);
            return 1;
        }
    }
    {
        unsigned currJob = 0;
        /* creating jobs */
        for ( ; ; ) {
            size_t pos = 0;
            size_t const readBlockSize = 1 << 15;
            size_t remaining = FILE_CHUNK_SIZE;

            /* new job reset completion */
            pthread_mutex_lock(&ctx->completion_mutex.pMutex);
            ctx->createCompletion = 0;
            pthread_mutex_unlock(&ctx->completion_mutex.pMutex);

            while (remaining != 0 && !feof(srcFile)) {
                size_t const ret = fread(ctx->input.buffer.start + ctx->input.filled + pos, 1, readBlockSize, srcFile);
                if (ret != readBlockSize && !feof(srcFile)) {
                    /* error could not read correct number of bytes */
                    DISPLAY("Error: problem occurred during read from src file\n");
                    signalErrorToThreads(ctx);
                    return 1;
                }
                pos += ret;
                remaining -= ret;
                pthread_mutex_lock(&ctx->completion_mutex.pMutex);
                ctx->createCompletion = 1 - (double)remaining/((size_t)FILE_CHUNK_SIZE);
                DEBUG(3, "create completion: %f\n", ctx->createCompletion);
                pthread_mutex_unlock(&ctx->completion_mutex.pMutex);
            }
            if (remaining != 0 && !feof(srcFile)) {
                DISPLAY("Error: problem occurred during read from src file\n");
                signalErrorToThreads(ctx);
                return 1;
            }
            g_streamedSize += pos;
            /* reading was fine, now create the compression job */
            {
                int const last = feof(srcFile);
                int const error = createCompressionJob(ctx, pos, last);
                if (error != 0) {
                    signalErrorToThreads(ctx);
                    return error;
                }
            }
            currJob++;
            if (feof(srcFile)) {
                DEBUG(3, "THE STREAM OF DATA ENDED %u\n", ctx->nextJobID);
                break;
            }
        }
    }
    /* success -- created all jobs */
    return 0;
}

static fcResources createFileCompressionResources(const char* const srcFilename, const char* const dstFilenameOrNull)
{
    fcResources fcr;
    unsigned const stdinUsed = !strcmp(srcFilename, stdinmark);
    FILE* const srcFile = stdinUsed ? stdin : fopen(srcFilename, "rb");
    const char* const outFilenameIntermediate = (stdinUsed && !dstFilenameOrNull) ? stdoutmark : dstFilenameOrNull;
    const char* outFilename = outFilenameIntermediate;
    char fileAndSuffix[MAX_PATH];
    size_t const numJobs = MAX_NUM_JOBS;

    memset(&fcr, 0, sizeof(fcr));

    if (!outFilenameIntermediate) {
        if (snprintf(fileAndSuffix, MAX_PATH, "%s.zst", srcFilename) + 1 > MAX_PATH) {
            DISPLAY("Error: output filename is too long\n");
            return fcr;
        }
        outFilename = fileAndSuffix;
    }

    {
        unsigned const stdoutUsed = !strcmp(outFilename, stdoutmark);
        FILE* const dstFile = stdoutUsed ? stdout : fopen(outFilename, "wb");
        fcr.otArg = malloc(sizeof(outputThreadArg));
        if (!fcr.otArg) {
            DISPLAY("Error: could not allocate space for output thread argument\n");
            return fcr;
        }
        fcr.otArg->dstFile = dstFile;
    }
    /* checking for errors */
    if (!fcr.otArg->dstFile || !srcFile) {
        DISPLAY("Error: some file(s) could not be opened\n");
        return fcr;
    }

    /* creating context */
    fcr.ctx = createCCtx(numJobs);
    fcr.otArg->ctx = fcr.ctx;
    fcr.srcFile = srcFile;
    return fcr;
}

static int freeFileCompressionResources(fcResources* fcr)
{
    int ret = 0;
    waitUntilAllJobsCompleted(fcr->ctx);
    ret |= (fcr->srcFile != NULL) ? fclose(fcr->srcFile) : 0;
    ret |= (fcr->ctx != NULL) ? freeCCtx(fcr->ctx) : 0;
    if (fcr->otArg) {
        ret |= (fcr->otArg->dstFile != stdout) ? fclose(fcr->otArg->dstFile) : 0;
        free(fcr->otArg);
        /* no need to freeCCtx() on otArg->ctx because it should be the same context */
    }
    return ret;
}

static int compressFilename(const char* const srcFilename, const char* const dstFilenameOrNull)
{
    int ret = 0;
    UTIL_getTime(&g_startTime);
    g_streamedSize = 0;
    fcResources fcr = createFileCompressionResources(srcFilename, dstFilenameOrNull);
    ret |= performCompression(fcr.ctx, fcr.srcFile, fcr.otArg);
    ret |= freeFileCompressionResources(&fcr);
    return ret;
}

static int compressFilenames(const char** filenameTable, unsigned numFiles, unsigned forceStdout)
{
    int ret = 0;
    unsigned fileNum;
    for (fileNum=0; fileNum<numFiles; fileNum++) {
        const char* filename = filenameTable[fileNum];
        if (!forceStdout) {
            ret |= compressFilename(filename, NULL);
        }
        else {
            ret |= compressFilename(filename, stdoutmark);
        }

    }
    return ret;
}

/*! readU32FromChar() :
    @return : unsigned integer value read from input in `char` format
    allows and interprets K, KB, KiB, M, MB and MiB suffix.
    Will also modify `*stringPtr`, advancing it to position where it stopped reading.
    Note : function result can overflow if digit string > MAX_UINT */
static unsigned readU32FromChar(const char** stringPtr)
{
    unsigned result = 0;
    while ((**stringPtr >='0') && (**stringPtr <='9'))
        result *= 10, result += **stringPtr - '0', (*stringPtr)++ ;
    if ((**stringPtr=='K') || (**stringPtr=='M')) {
        result <<= 10;
        if (**stringPtr=='M') result <<= 10;
        (*stringPtr)++ ;
        if (**stringPtr=='i') (*stringPtr)++;
        if (**stringPtr=='B') (*stringPtr)++;
    }
    return result;
}

static void help()
{
    PRINT("Usage:\n");
    PRINT("  ./multi [options] [file(s)]\n");
    PRINT("\n");
    PRINT("Options:\n");
    PRINT("  -oFILE : specify the output file name\n");
    PRINT("  -v     : display debug information\n");
    PRINT("  -i#    : provide initial compression level\n");
    PRINT("  -h     : display help/information\n");
    PRINT("  -f     : force the compression level to stay constant\n");
}
/* return 0 if successful, else return error */
int main(int argCount, const char* argv[])
{
    const char* outFilename = NULL;
    const char** filenameTable = (const char**)malloc(argCount*sizeof(const char*));
    unsigned filenameIdx = 0;
    filenameTable[0] = stdinmark;
    unsigned forceStdout = 0;
    int ret = 0;
    int argNum;

    UTIL_initTimer(&g_ticksPerSecond);

    if (filenameTable == NULL) {
        DISPLAY("Error: could not allocate sapce for filename table.\n");
        return 1;
    }

    for (argNum=1; argNum<argCount; argNum++) {
        const char* argument = argv[argNum];

        /* output filename designated with "-o" */
        if (argument[0]=='-' && strlen(argument) > 1) {
            switch (argument[1]) {
                case 'o':
                    argument += 2;
                    outFilename = argument;
                    break;
                case 'v':
                    g_displayLevel++;
                    break;
                case 'i':
                    argument += 2;
                    g_compressionLevel = readU32FromChar(&argument);
                    DEBUG(3, "g_compressionLevel: %u\n", g_compressionLevel);
                    break;
                case 'h':
                    help();
                    goto _main_exit;
                case 'p':
                    g_useProgressBar = 1;
                    break;
                case 'c':
                    forceStdout = 1;
                    outFilename = stdoutmark;
                    break;
                case 'f':
                    g_forceCompressionLevel = 1;
                    break;
                default:
                    DISPLAY("Error: invalid argument provided\n");
                    ret = 1;
                    goto _main_exit;
            }
            continue;
        }

        /* regular files to be compressed */
        filenameTable[filenameIdx++] = argument;
    }

    /* error checking with number of files */
    if (filenameIdx > 1 && (outFilename != NULL && strcmp(outFilename, stdoutmark))) {
        DISPLAY("Error: multiple input files provided, cannot use specified output file\n");
        ret = 1;
        goto _main_exit;
    }

    /* compress files */
    if (filenameIdx <= 1) {
        ret |= compressFilename(filenameTable[0], outFilename);
    }
    else {
        ret |= compressFilenames(filenameTable, filenameIdx, forceStdout);
    }
_main_exit:
    free(filenameTable);
    return ret;
}
