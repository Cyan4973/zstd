/*
 * Minimal thread pool for the zstd CLI.
 * Adapted from the LZ4 project (GPLv2).
 */

#ifndef FIO_THREADPOOL_H
#define FIO_THREADPOOL_H

#include "platform.h"

#ifdef __cplusplus
extern "C" {
#endif

#ifndef FIO_THREADPOOL_MULTITHREAD
#  if defined(ZSTD_MULTITHREAD)
#    define FIO_THREADPOOL_MULTITHREAD 1
#  else
#    define FIO_THREADPOOL_MULTITHREAD 0
#  endif
#endif

#ifndef FIO_THREADPOOL_MAX_WORKERS
#  define FIO_THREADPOOL_MAX_WORKERS 256
#endif

typedef struct FIO_TPool_s FIO_TPool;

FIO_TPool* FIO_TPool_create(int nbThreads, int queueSize);
void FIO_TPool_free(FIO_TPool* ctx);
void FIO_TPool_submit(FIO_TPool* ctx, void (*job_function)(void*), void* arg);
void FIO_TPool_wait(FIO_TPool* ctx);

#ifdef __cplusplus
}
#endif

#endif /* FIO_THREADPOOL_H */
