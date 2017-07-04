#define DISPLAY(...) fprintf(stderr, __VA_ARGS__)
#define FILE_CHUNK_SIZE 4 << 20
typedef unsigned char BYTE;

#include <stdio.h>      /* fprintf */
#include <stdlib.h>     /* malloc, free */
#include <pthread.h>    /* pthread functions */
#include <string.h>     /* memset */
#include "zstd.h"



typedef struct {
    void* start;
    size_t size;
} buffer_t;

typedef struct {
    buffer_t src;
    buffer_t dst;
    unsigned compressionLevel;
    unsigned jobID;
    unsigned jobCompleted;
    unsigned jobReady;
    pthread_mutex_t* jobCompleted_mutex;
    pthread_cond_t* jobCompleted_cond;
    pthread_mutex_t* jobReady_mutex;
    pthread_cond_t* jobReady_cond;
    size_t compressedSize;
} jobDescription;

typedef struct {
    unsigned compressionLevel;
    unsigned numActiveThreads;
    unsigned numJobs;
    unsigned lastJobID;
    unsigned nextJobID;
    unsigned threadError;
    unsigned allJobsCompleted;
    pthread_mutex_t jobCompleted_mutex;
    pthread_cond_t jobCompleted_cond;
    pthread_mutex_t jobReady_mutex;
    pthread_cond_t jobReady_cond;
    pthread_mutex_t allJobsCompleted_mutex;
    pthread_cond_t allJobsCompleted_cond;
    jobDescription* jobs;
    FILE* dstFile;
} adaptCCtx;

static adaptCCtx* createCCtx(unsigned numJobs, const char* const outFilename)
{

    adaptCCtx* ctx = malloc(sizeof(adaptCCtx));
    if (ctx == NULL) {
        DISPLAY("Error: could not allocate space for context\n");
        return NULL;
    }
    memset(ctx, 0, sizeof(adaptCCtx));
    ctx->compressionLevel = 6; /* default */
    pthread_mutex_init(&ctx->jobCompleted_mutex, NULL);
    pthread_cond_init(&ctx->jobCompleted_cond, NULL);
    pthread_mutex_init(&ctx->jobReady_mutex, NULL);
    pthread_cond_init(&ctx->jobReady_cond, NULL);
    pthread_mutex_init(&ctx->allJobsCompleted_mutex, NULL);
    pthread_cond_init(&ctx->allJobsCompleted_cond, NULL);
    ctx->numJobs = numJobs;
    ctx->lastJobID = -1; /* intentional underflow */
    ctx->jobs = calloc(1, numJobs*sizeof(jobDescription));
    DISPLAY("jobs %p\n", ctx->jobs);
    {
        unsigned u;
        for (u=0; u<numJobs; u++) {
            ctx->jobs[u].jobCompleted_mutex = &ctx->jobCompleted_mutex;
            ctx->jobs[u].jobCompleted_cond = &ctx->jobCompleted_cond;
            ctx->jobs[u].jobReady_mutex = &ctx->jobReady_mutex;
            ctx->jobs[u].jobReady_cond = &ctx->jobReady_cond;
        }
    }
    ctx->nextJobID = 0;
    ctx->threadError = 0;
    ctx->allJobsCompleted = 0;
    if (!ctx->jobs) {
        DISPLAY("Error: could not allocate space for jobs during context creation\n");
        return NULL;
    }
    {
        FILE* dstFile = fopen(outFilename, "wb");
        if (dstFile == NULL) {
            DISPLAY("Error: could not open output file\n");
            return NULL;
        }
        ctx->dstFile = dstFile;
    }
    return ctx;
}

static void freeCompressionJobs(adaptCCtx* ctx)
{
    unsigned u;
    for (u=0; u<ctx->numJobs; u++) {
        DISPLAY("freeing compression job %u\n", u);
        DISPLAY("%u\n", ctx->numJobs);
        jobDescription job = ctx->jobs[u];
        if (job.dst.start) free(job.dst.start);
        if (job.src.start) free(job.src.start);
    }
}

static int freeCCtx(adaptCCtx* ctx)
{
    pthread_mutex_lock(&ctx->allJobsCompleted_mutex);
    while (ctx->allJobsCompleted == 0) {
        pthread_cond_wait(&ctx->allJobsCompleted_cond, &ctx->allJobsCompleted_mutex);
    }
    pthread_mutex_unlock(&ctx->allJobsCompleted_mutex);
    {
        int const completedMutexError = pthread_mutex_destroy(&ctx->jobCompleted_mutex);
        int const completedCondError = pthread_cond_destroy(&ctx->jobCompleted_cond);
        int const readyMutexError = pthread_mutex_destroy(&ctx->jobReady_mutex);
        int const readyCondError = pthread_cond_destroy(&ctx->jobReady_cond);
        int const allJobsMutexError = pthread_mutex_destroy(&ctx->allJobsCompleted_mutex);
        int const allJobsCondError = pthread_cond_destroy(&ctx->allJobsCompleted_cond);
        int const fileError =  fclose(ctx->dstFile);
        freeCompressionJobs(ctx);
        free(ctx->jobs);
        return completedMutexError | completedCondError | readyMutexError | readyCondError | fileError | allJobsMutexError | allJobsCondError;
    }
}

static void* compressionThread(void* arg)
{
    DISPLAY("started compression thread\n");
    adaptCCtx* ctx = (adaptCCtx*)arg;
    unsigned currJob = 0;
    for ( ; ; ) {
        jobDescription* job = &ctx->jobs[currJob];
        pthread_mutex_lock(job->jobReady_mutex);
        while(job->jobReady == 0) {
            DISPLAY("waiting\n");
            pthread_cond_wait(job->jobReady_cond, job->jobReady_mutex);
        }
        pthread_mutex_unlock(job->jobReady_mutex);
        /* compress the data */
        {
            size_t const compressedSize = ZSTD_compress(job->dst.start, job->dst.size, job->src.start, job->src.size, job->compressionLevel);
            if (ZSTD_isError(compressedSize)) {
                ctx->threadError = 1;
                DISPLAY("Error: somethign went wrong during compression\n");
                return arg;
            }
            job->compressedSize = compressedSize;
        }
        pthread_mutex_lock(job->jobCompleted_mutex);
        job->jobCompleted = 1;
        pthread_cond_signal(job->jobCompleted_cond);
        pthread_mutex_unlock(job->jobCompleted_mutex);
        currJob++;
        if (currJob >= ctx->lastJobID || ctx->threadError) {
            /* finished compressing all jobs */
            break;
        }
    }
    return arg;
}

static void* outputThread(void* arg)
{
    DISPLAY("started output thread\n");
    adaptCCtx* ctx = (adaptCCtx*)arg;

    unsigned currJob = 0;
    for ( ; ; ) {
        jobDescription* job = &ctx->jobs[currJob];
        pthread_mutex_lock(job->jobCompleted_mutex);
        while (job->jobCompleted == 0) {
            pthread_cond_wait(job->jobCompleted_cond, job->jobCompleted_mutex);
        }
        pthread_mutex_unlock(job->jobCompleted_mutex);
        {
            size_t const compressedSize = job->compressedSize;
            if (ZSTD_isError(compressedSize)) {
                DISPLAY("Error: an error occurred during compression\n");
                return arg; /* TODO: return something else if error */
            }
            {
                size_t const writeSize = fwrite(ctx->jobs[currJob].dst.start, 1, compressedSize, ctx->dstFile);
                if (writeSize != compressedSize) {
                    DISPLAY("Error: an error occurred during file write operation\n");
                    return arg; /* TODO: return something else if error */
                }
            }
        }
        currJob++;
        if (currJob >= ctx->lastJobID || ctx->threadError) {
            /* finished with all jobs */
            pthread_mutex_lock(&ctx->allJobsCompleted_mutex);
            ctx->allJobsCompleted = 1;
            pthread_cond_signal(&ctx->allJobsCompleted_cond);
            pthread_mutex_unlock(&ctx->allJobsCompleted_mutex);
            break;
        }
    }
    return arg;
}


static size_t getFileSize(const char* const filename)
{
    FILE* fd = fopen(filename, "rb");
    if (fd == NULL) {
        DISPLAY("Error: could not open file in order to get file size\n");
        return -1; /* intentional underflow */
    }
    if (fseek(fd, 0, SEEK_END) != 0) {
        DISPLAY("Error: fseek failed during file size computation\n");
        return -1;
    }
    {
        size_t const fileSize = ftell(fd);
        if (fclose(fd) != 0) {
            DISPLAY("Error: could not close file during file size computation\n");
            return -1;
        }
        return fileSize;
    }
}

static int createCompressionJob(adaptCCtx* ctx, BYTE* data, size_t srcSize)
{
    unsigned const nextJob = ctx->nextJobID;
    jobDescription* job = &ctx->jobs[nextJob];
    job->compressionLevel = ctx->compressionLevel;
    job->src.start = malloc(srcSize);
    job->src.size = srcSize;
    job->dst.size = ZSTD_compressBound(srcSize);
    job->dst.start = malloc(job->dst.size);
    job->jobCompleted = 0;
    job->jobCompleted_cond = &ctx->jobCompleted_cond;
    job->jobCompleted_mutex = &ctx->jobCompleted_mutex;
    job->jobReady_cond = &ctx->jobReady_cond;
    job->jobReady_mutex = &ctx->jobReady_mutex;
    job->jobID = nextJob;
    if (!job->src.start || !job->dst.start) {
        /* problem occurred, free things then return */
        DISPLAY("Error: problem occurred during job creation\n");
        if (job->src.start) free(job->src.start);
        if (job->dst.start) free(job->dst.start);
        return 1;
    }
    memcpy(job->src.start, data, srcSize);
    pthread_mutex_lock(job->jobReady_mutex);
    job->jobReady = 1;
    pthread_cond_signal(job->jobReady_cond);
    pthread_mutex_unlock(job->jobReady_mutex);
    ctx->nextJobID++;
    return 0;
}

/* return 0 if successful, else return error */
int main(int argCount, const char* argv[])
{
    if (argCount < 2) {
        DISPLAY("Error: not enough arguments\n");
        return 1;
    }
    const char* const srcFilename = argv[1];
    const char* const dstFilename = argv[2];
    BYTE* const src = malloc(FILE_CHUNK_SIZE);
    FILE* const srcFile = fopen(srcFilename, "rb");
    size_t fileSize = getFileSize(srcFilename);
    size_t const numJobsPrelim = (fileSize >> 22) + 1; /* TODO: figure out why can't divide here */
    size_t const numJobs = (numJobsPrelim * FILE_CHUNK_SIZE) == fileSize ? numJobsPrelim : numJobsPrelim + 1;
    int ret = 0;
    adaptCCtx* ctx = NULL;


    /* checking for errors */
    if (fileSize == (size_t)(-1)) {
        ret = 1;
        goto cleanup;
    }
    if (!srcFilename || !dstFilename || !src || !srcFile) {
        DISPLAY("Error: initial variables could not be allocated\n");
        ret = 1;
        goto cleanup;
    }

    /* creating context */
    ctx = createCCtx(numJobs, dstFilename);
    if (ctx == NULL) {
        ret = 1;
        goto cleanup;
    }

    /* create output thread */
    {
        pthread_t out;
        if (pthread_create(&out, NULL, &outputThread, ctx)) {
            DISPLAY("Error: could not create output thread\n");
            ret = 1;
            goto cleanup;
        }
    }

    /* create compression thread */
    {
        pthread_t compression;
        if (pthread_create(&compression, NULL, &compressionThread, ctx)) {
            DISPLAY("Error: could not create compression thread\n");
            ret = 1;
            goto cleanup;
        }
    }

    /* creating jobs */
    for ( ; ; ) {
        size_t const readSize = fread(src, 1, FILE_CHUNK_SIZE, srcFile);
        if (readSize != FILE_CHUNK_SIZE && !feof(srcFile)) {
            DISPLAY("Error: problem occurred during read from src file\n");
            ret = 1;
            goto cleanup;
        }
        /* reading was fine, now create the compression job */
        {
            int const error = createCompressionJob(ctx, src, readSize);
            if (error != 0) {
                ret = error;
                goto cleanup;
            }
        }
        if (feof(srcFile)) {
            ctx->lastJobID = ctx->nextJobID;
            break;
        }
    }

cleanup:
    /* file compression completed */
    ret  |= (srcFile != NULL) ? fclose(srcFile) : 0;
    ret |= (ctx != NULL) ? freeCCtx(ctx) : 0;
    if (src != NULL) free(src);
    return ret;
}
