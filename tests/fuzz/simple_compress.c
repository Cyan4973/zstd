/*
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under both the BSD-style license (found in the
 * LICENSE file in the root directory of this source tree) and the GPLv2 (found
 * in the COPYING file in the root directory of this source tree).
 */

/**
 * This fuzz target attempts to comprss the fuzzed data with the simple
 * compression function with an output buffer that may be too small to
 * ensure that the compressor never crashes.
 */

#include <stddef.h>
#include <stdlib.h>
#include <stdio.h>
#include "fuzz_helpers.h"
#include "zstd.h"
#include "fuzz_data_producer.h"

static ZSTD_CCtx *cctx = NULL;

int LLVMFuzzerTestOneInput(const uint8_t *src, size_t size)
{
    FUZZ_dataProducer_t *producer = FUZZ_dataProducer_create(src, size);

    int const level = (int)FUZZ_dataProducer_uint32Range(
                                producer, 0, 19 + 3) - 3; /* [-3, 19] */
    size_t const maxSize = ZSTD_compressBound(size);
    size_t const bufSize = FUZZ_dataProducer_uint32Range(producer, 0, maxSize);

    size = FUZZ_dataProducer_remainingBytes(producer);

    if (!cctx) {
        cctx = ZSTD_createCCtx();
        FUZZ_ASSERT(cctx);
    }

    void *rBuf = malloc(bufSize);
    FUZZ_ASSERT(rBuf);
    ZSTD_compressCCtx(cctx, rBuf, bufSize, src, size, level);
    free(rBuf);
    FUZZ_dataProducer_free(producer);
#ifndef STATEFUL_FUZZING
    ZSTD_freeCCtx(cctx); cctx = NULL;
#endif
    return 0;
}
