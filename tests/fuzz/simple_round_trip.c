/*
 * Copyright (c) 2016-2020, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under both the BSD-style license (found in the
 * LICENSE file in the root directory of this source tree) and the GPLv2 (found
 * in the COPYING file in the root directory of this source tree).
 * You may select, at your option, one of the above-listed licenses.
 */

/**
 * This fuzz target performs a zstd round-trip test (compress & decompress),
 * compares the result with the original, and calls abort() on corruption.
 */

#define ZSTD_STATIC_LINKING_ONLY

#include <stddef.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "fuzz_helpers.h"
#include "zstd_helpers.h"
#include "fuzz_data_producer.h"

static ZSTD_CCtx *cctx = NULL;
static ZSTD_DCtx *dctx = NULL;

static size_t roundTripTest(void *result, size_t resultCapacity,
                            void *compressed, size_t compressedCapacity,
                            const void *src, size_t srcSize,
                            FUZZ_dataProducer_t *producer)
{
    size_t cSize;
    if (FUZZ_dataProducer_uint32Range(producer, 0, 1)) {
        FUZZ_setRandomParameters(cctx, srcSize, producer);
        cSize = ZSTD_compress2(cctx, compressed, compressedCapacity, src, srcSize);
    } else {
      int const cLevel = FUZZ_dataProducer_int32Range(producer, kMinClevel, kMaxClevel);

        cSize = ZSTD_compressCCtx(
            cctx, compressed, compressedCapacity, src, srcSize, cLevel);
    }
    FUZZ_ZASSERT(cSize);
    return ZSTD_decompressDCtx(dctx, result, resultCapacity, compressed, cSize);
}

int LLVMFuzzerTestOneInput(const uint8_t *src, size_t size)
{
    size_t const rBufSize = size;
    void* rBuf = malloc(rBufSize);
    size_t cBufSize = ZSTD_compressBound(size) * 2;
    void* cBuf;

    /* Give a random portion of src data to the producer, to use for
    parameter generation. The rest will be used for (de)compression */
    FUZZ_dataProducer_t *producer = FUZZ_dataProducer_create(src, size);
    size = FUZZ_dataProducer_reserveDataPrefix(producer);

    /* Half of the time fuzz with a 1 byte smaller output size.
     * This will still succeed because we don't use a dictionary, so the dictID
     * field is empty, giving us 4 bytes of overhead.
     */
    cBufSize -= FUZZ_dataProducer_uint32Range(producer, 0, 1);

    cBuf = malloc(cBufSize);

    FUZZ_ASSERT(cBuf && rBuf);

    if (!cctx) {
        cctx = ZSTD_createCCtx();
        FUZZ_ASSERT(cctx);
    }
    if (!dctx) {
        dctx = ZSTD_createDCtx();
        FUZZ_ASSERT(dctx);
    }

    {
        size_t const result =
            roundTripTest(rBuf, rBufSize, cBuf, cBufSize, src, size, producer);
        FUZZ_ZASSERT(result);
        FUZZ_ASSERT_MSG(result == size, "Incorrect regenerated size");
        FUZZ_ASSERT_MSG(!memcmp(src, rBuf, size), "Corruption!");
    }
    free(rBuf);
    free(cBuf);
    FUZZ_dataProducer_free(producer);
#ifndef STATEFUL_FUZZING
    ZSTD_freeCCtx(cctx); cctx = NULL;
    ZSTD_freeDCtx(dctx); dctx = NULL;
#endif
    return 0;
}
