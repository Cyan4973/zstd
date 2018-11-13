/*
 * Copyright (c) 2016-present, Yann Collet, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under both the BSD-style license (found in the
 * LICENSE file in the root directory of this source tree) and the GPLv2 (found
 * in the COPYING file in the root directory of this source tree).
 * You may select, at your option, one of the above-listed licenses.
 */


/* benchfn :
 * benchmark any function on a set of input
 * providing result in nanoSecPerRun
 * or detecting and returning an error
 */

#if defined (__cplusplus)
extern "C" {
#endif

#ifndef BENCH_FN_H_23876
#define BENCH_FN_H_23876

/* ===  Dependencies  === */
#include <stddef.h>   /* size_t */


/* ====  Benchmark any function, iterated on a set of blocks  ==== */

/* BMK_runTime_t: valid result type */

typedef struct {
    unsigned long long nanoSecPerRun;  /* time per iteration (over all blocks) */
    size_t sumOfReturn;         /* sum of return values */
} BMK_runTime_t;


/* type expressing the outcome of a benchmark run by BMK_benchFunction().
 * benchmark outcome might be valid or invalid.
 * benchmark outcome can be invalid if and errorFn was provided.
 * BMK_runOutcome_t must be considered "opaque" : never access its members directly.
 * Instead, use its assigned methods :
 * BMK_isSuccessful_runOutcome, BMK_extract_runTime, BMK_extract_errorResult.
 * The structure is only described here to allow its allocation on stack. */

typedef struct {
    BMK_runTime_t internal_never_ever_use_directly;
    size_t error_result_never_ever_use_directly;
    int error_tag_never_ever_use_directly;
} BMK_runOutcome_t;


typedef size_t (*BMK_benchFn_t)(const void* src, size_t srcSize, void* dst, size_t dstCapacity, void* customPayload);
typedef size_t (*BMK_initFn_t)(void* initPayload);
typedef unsigned (*BMK_errorFn_t)(size_t);


/* BMK_benchFunction() :
 * This function times the execution of 2 argument functions, benchFn and initFn  */

/* benchFn - (*benchFn)(srcBuffers[i], srcSizes[i], dstBuffers[i], dstCapacities[i], benchPayload)
 *      is run nbLoops times
 * initFn - (*initFn)(initPayload) is run once per run, at the beginning.
 *      This argument can be NULL, in which case nothing is run.
 * errorFn - is a function run on each return value of benchFn.
 *      Argument errorFn can be NULL, in which case nothing is run.
 *      Otherwise, it must return 0 when benchFn was successful, and >= 1 if it detects an error.
 *      Execution is stopped as soon as an error is detected,
 *      the triggering return value can then be retrieved with BMK_extract_errorResult().
 * blockCount - number of blocks. Size of all array parameters : srcBuffers, srcSizes, dstBuffers, dstCapacities, blockResults
 * srcBuffers - an array of buffers to be operated on by benchFn
 * srcSizes - an array of the sizes of above buffers
 * dstBuffers - an array of buffers to be written into by benchFn
 * dstCapacities - an array of the capacities of above buffers
 * blockResults - Optional: store the return value of benchFn for each block. Use NULL if this result is not requested.
 * nbLoops - defines number of times benchFn is run. Minimum value is 1. A 0 is interpreted as a 1.
 * @return: a variant, which express either an error, or can generate a valid BMK_runTime_t result.
 *          Use BMK_isSuccessful_runOutcome() to check if function was successful.
 *          If yes, extract the result with BMK_extract_runTime(),
 *          it will contain :
 *              .sumOfReturn : the sum of all return values of benchFn through all of blocks
 *              .nanoSecPerRun : time per run of benchFn + (time for initFn / nbLoops)
 *          .sumOfReturn is generally intended for functions which return a # of bytes written into dstBuffer,
 *              in which case, this value will be the total amount of bytes written into dstBuffer.
 */
BMK_runOutcome_t BMK_benchFunction(
                        BMK_benchFn_t benchFn, void* benchPayload,
                        BMK_initFn_t initFn, void* initPayload,
                        BMK_errorFn_t errorFn,
                        size_t blockCount,
                        const void *const * srcBuffers, const size_t* srcSizes,
                        void *const * dstBuffers, const size_t* dstCapacities,
                        size_t* blockResults,
                        unsigned nbLoops);



/* check first if the benchmark was successful or not */
int BMK_isSuccessful_runOutcome(BMK_runOutcome_t outcome);

/* If the benchmark was successful, extract the result.
 * note : this function will abort() program execution if benchmark failed !
 *        always check if benchmark was successful first !
 */
BMK_runTime_t BMK_extract_runTime(BMK_runOutcome_t outcome);

/* when benchmark failed, it means one invocation of `benchFn` failed.
 * The failure was detected by `errorFn`, operating on return value of `benchFn`.
 * Returns the faulty return value.
 * note : this function will abort() program execution if benchmark did not failed.
 *        always check if benchmark failed first !
 */
size_t BMK_extract_errorResult(BMK_runOutcome_t outcome);



/* ====  Benchmark any function, returning intermediate results  ==== */

/* state information tracking benchmark session */
typedef struct BMK_timedFnState_s BMK_timedFnState_t;

/* BMK_benchTimedFn() :
 * Similar to BMK_benchFunction(), most arguments being identical.
 * Automatically determines `nbLoops` so that each result is regularly produced at interval of about run_ms.
 * Note : minimum `nbLoops` is 1, therefore a run may last more than run_ms, and possibly even more than total_ms.
 * Usage - initialize timedFnState, select benchmark duration (total_ms) and each measurement duration (run_ms)
 *         call BMK_benchTimedFn() repetitively, each measurement is supposed to last about run_ms
 *         Check if total time budget is spent or exceeded, using BMK_isCompleted_TimedFn()
 */
BMK_runOutcome_t BMK_benchTimedFn(
                    BMK_timedFnState_t* timedFnState,
                    BMK_benchFn_t benchFn, void* benchPayload,
                    BMK_initFn_t initFn, void* initPayload,
                    BMK_errorFn_t errorFn,
                    size_t blockCount,
                    const void *const * srcBlockBuffers, const size_t* srcBlockSizes,
                    void *const * dstBlockBuffers, const size_t* dstBlockCapacities,
                    size_t* blockResults);

/* Tells if duration of all benchmark runs has exceeded total_ms
 */
int BMK_isCompleted_TimedFn(const BMK_timedFnState_t* timedFnState);

/* BMK_createTimedFnState() and BMK_resetTimedFnState() :
 * Create/Set BMK_timedFnState_t for next benchmark session,
 * which shall last a minimum of total_ms milliseconds,
 * producing intermediate results, paced at interval of (approximately) run_ms.
 */
BMK_timedFnState_t* BMK_createTimedFnState(unsigned total_ms, unsigned run_ms);
void BMK_resetTimedFnState(BMK_timedFnState_t* timedFnState, unsigned total_ms, unsigned run_ms);
void BMK_freeTimedFnState(BMK_timedFnState_t* state);



#endif   /* BENCH_FN_H_23876 */

#if defined (__cplusplus)
}
#endif
