/*
 * Minimal thread pool for the zstd CLI.
 * Derived from LZ4's threadpool implementation (GPLv2).
 */

#include <assert.h>
#include <stdlib.h>
#include "fio_threadpool.h"

#if !FIO_THREADPOOL_MULTITHREAD

struct FIO_TPool_s { int dummy; };
static struct FIO_TPool_s g_pool;

FIO_TPool* FIO_TPool_create(int nbThreads, int queueSize)
{
    (void)nbThreads; (void)queueSize;
    return &g_pool;
}

void FIO_TPool_free(FIO_TPool* ctx)
{
    (void)ctx;
}

void FIO_TPool_submit(FIO_TPool* ctx, void (*job_function)(void*), void* arg)
{
    (void)ctx;
    job_function(arg);
}

void FIO_TPool_wait(FIO_TPool* ctx)
{
    (void)ctx;
}

#elif defined(_WIN32)

#include <windows.h>

typedef struct FIO_TPool_s {
    HANDLE completionPort;
    HANDLE* workerThreads;
    int nbWorkers;
    LONG nbPendingJobs;
    HANDLE jobSlots;
    HANDLE allJobsCompleted;
} FIO_TPool;

static DWORD WINAPI FIO_workerThread(LPVOID lpParameter)
{
    FIO_TPool* const pool = (FIO_TPool*)lpParameter;
    DWORD bytesTransferred;
    ULONG_PTR completionKey;
    LPOVERLAPPED overlapped;

    while (GetQueuedCompletionStatus(pool->completionPort,
                                    &bytesTransferred, &completionKey,
                                    &overlapped, INFINITE)) {
        (void)bytesTransferred;
        if (overlapped == NULL) break;
        ((void (*)(void*))completionKey)(overlapped);
        if (InterlockedDecrement(&pool->nbPendingJobs) == 0)
            SetEvent(pool->allJobsCompleted);
        ReleaseSemaphore(pool->jobSlots, 1, NULL);
    }
    return 0;
}

FIO_TPool* FIO_TPool_create(int nbThreads, int queueSize)
{
    int i;
    FIO_TPool* pool;
    if (nbThreads <= 0 || queueSize <= 0) return NULL;
    if (nbThreads > FIO_THREADPOOL_MAX_WORKERS) nbThreads = FIO_THREADPOOL_MAX_WORKERS;

    pool = (FIO_TPool*)calloc(1, sizeof(*pool));
    if (!pool) return NULL;

    pool->completionPort = CreateIoCompletionPort(INVALID_HANDLE_VALUE, NULL, 0, nbThreads);
    if (!pool->completionPort) goto _cleanup;

    pool->workerThreads = (HANDLE*)malloc(sizeof(HANDLE) * nbThreads);
    if (!pool->workerThreads) goto _cleanup;
    pool->nbWorkers = nbThreads;

    for (i = 0; i < nbThreads; ++i) {
        pool->workerThreads[i] = CreateThread(NULL, 0, FIO_workerThread, pool, 0, NULL);
        if (!pool->workerThreads[i]) goto _cleanup;
    }

    pool->jobSlots = CreateSemaphore(NULL, queueSize + nbThreads, queueSize + nbThreads, NULL);
    if (!pool->jobSlots) goto _cleanup;

    pool->allJobsCompleted = CreateEvent(NULL, FALSE, FALSE, NULL);
    if (!pool->allJobsCompleted) goto _cleanup;

    pool->nbPendingJobs = 0;
    return pool;

_cleanup:
    FIO_TPool_free(pool);
    return NULL;
}

void FIO_TPool_free(FIO_TPool* pool)
{
    if (!pool) return;
    if (pool->completionPort) {
        int i;
        for (i = 0; i < pool->nbWorkers; ++i)
            PostQueuedCompletionStatus(pool->completionPort, 0, 0, NULL);
        WaitForMultipleObjects(pool->nbWorkers, pool->workerThreads, TRUE, INFINITE);
        for (i = 0; i < pool->nbWorkers; ++i)
            CloseHandle(pool->workerThreads[i]);
        CloseHandle(pool->completionPort);
    }
    CloseHandle(pool->jobSlots);
    CloseHandle(pool->allJobsCompleted);
    free(pool->workerThreads);
    free(pool);
}

void FIO_TPool_submit(FIO_TPool* pool, void (*job_function)(void*), void* arg)
{
    assert(pool);
    WaitForSingleObject(pool->jobSlots, INFINITE);
    ResetEvent(pool->allJobsCompleted);
    InterlockedIncrement(&pool->nbPendingJobs);
    PostQueuedCompletionStatus(pool->completionPort, 0, (ULONG_PTR)job_function, (LPOVERLAPPED)arg);
}

void FIO_TPool_wait(FIO_TPool* pool)
{
    WaitForSingleObject(pool->allJobsCompleted, INFINITE);
}

#else

#include <pthread.h>

typedef struct {
    void (*function)(void*);
    void* arg;
} FIO_TPoolJob;

struct FIO_TPool_s {
    pthread_t* threads;
    size_t threadCount;
    size_t queueSize;
    FIO_TPoolJob* queue;
    size_t queueHead;
    size_t queueTail;
    size_t jobsPending;
    int shutdown;

    pthread_mutex_t mutex;
    pthread_cond_t condNotEmpty;
    pthread_cond_t condNotFull;
};

static void* FIO_TPool_worker(void* opaque)
{
    FIO_TPool* pool = (FIO_TPool*)opaque;
    for (;;) {
        FIO_TPoolJob job;
        pthread_mutex_lock(&pool->mutex);
        while (!pool->shutdown && pool->queueHead == pool->queueTail)
            pthread_cond_wait(&pool->condNotEmpty, &pool->mutex);
        if (pool->shutdown) {
            pthread_mutex_unlock(&pool->mutex);
            return NULL;
        }
        job = pool->queue[pool->queueTail];
        pool->queueTail = (pool->queueTail + 1) % pool->queueSize;
        pool->jobsPending++;
        pthread_cond_signal(&pool->condNotFull);
        pthread_mutex_unlock(&pool->mutex);

        job.function(job.arg);

        pthread_mutex_lock(&pool->mutex);
        pool->jobsPending--;
        if (pool->jobsPending == 0 && pool->queueHead == pool->queueTail)
            pthread_cond_signal(&pool->condNotFull);
        pthread_mutex_unlock(&pool->mutex);
    }
}

FIO_TPool* FIO_TPool_create(int nbThreads, int queueCapacity)
{
    int i;
    FIO_TPool* pool;
    if (nbThreads <= 0 || queueCapacity <= 0) return NULL;
    if (nbThreads > FIO_THREADPOOL_MAX_WORKERS) nbThreads = FIO_THREADPOOL_MAX_WORKERS;

    pool = (FIO_TPool*)calloc(1, sizeof(*pool));
    if (!pool) return NULL;

    pool->threads = (pthread_t*)malloc(sizeof(pthread_t) * (size_t)nbThreads);
    pool->queueSize = (size_t)queueCapacity + 1;
    pool->queue = (FIO_TPoolJob*)calloc(pool->queueSize, sizeof(FIO_TPoolJob));
    if (!pool->threads || !pool->queue) {
        FIO_TPool_free(pool);
        return NULL;
    }

    pthread_mutex_init(&pool->mutex, NULL);
    pthread_cond_init(&pool->condNotEmpty, NULL);
    pthread_cond_init(&pool->condNotFull, NULL);

    pool->queueHead = 0;
    pool->queueTail = 0;
    pool->jobsPending = 0;
    pool->shutdown = 0;

    for (i = 0; i < nbThreads; ++i) {
        if (pthread_create(&pool->threads[i], NULL, FIO_TPool_worker, pool)) {
            pool->threadCount = (size_t)i;
            FIO_TPool_free(pool);
            return NULL;
        }
    }
    pool->threadCount = (size_t)nbThreads;
    return pool;
}

void FIO_TPool_free(FIO_TPool* pool)
{
    size_t i;
    if (!pool) return;
    if (pool->threads) {
        pthread_mutex_lock(&pool->mutex);
        pool->shutdown = 1;
        pthread_cond_broadcast(&pool->condNotEmpty);
        pthread_mutex_unlock(&pool->mutex);
        for (i = 0; i < pool->threadCount; ++i)
            pthread_join(pool->threads[i], NULL);
    }
    pthread_mutex_destroy(&pool->mutex);
    pthread_cond_destroy(&pool->condNotEmpty);
    pthread_cond_destroy(&pool->condNotFull);
    free(pool->threads);
    free(pool->queue);
    free(pool);
}

void FIO_TPool_submit(FIO_TPool* pool, void (*job_function)(void*), void* arg)
{
    pthread_mutex_lock(&pool->mutex);
    while ((pool->queueHead + 1) % pool->queueSize == pool->queueTail)
        pthread_cond_wait(&pool->condNotFull, &pool->mutex);
    pool->queue[pool->queueHead].function = job_function;
    pool->queue[pool->queueHead].arg = arg;
    pool->queueHead = (pool->queueHead + 1) % pool->queueSize;
    pthread_cond_signal(&pool->condNotEmpty);
    pthread_mutex_unlock(&pool->mutex);
}

void FIO_TPool_wait(FIO_TPool* pool)
{
    pthread_mutex_lock(&pool->mutex);
    while (pool->jobsPending > 0 || pool->queueHead != pool->queueTail)
        pthread_cond_wait(&pool->condNotFull, &pool->mutex);
    pthread_mutex_unlock(&pool->mutex);
}

#endif
