#include <stdlib.h>
#include <stdio.h>

#include "ldm.h"
#include "ldm_hashtable.h"
#include "mem.h"

//TODO: move def somewhere else.

// Number of elements per hash bucket.
// HASH_BUCKET_SIZE_LOG defined in ldm.h
#define HASH_BUCKET_SIZE_LOG 2 // MAX is 4 for now
#define HASH_BUCKET_SIZE (1 << (HASH_BUCKET_SIZE_LOG))

// TODO: rename. Number of hash buckets.
#define LDM_HASHLOG ((LDM_MEMORY_USAGE)-4-HASH_BUCKET_SIZE_LOG)
#define ZSTD_SKIP
//#define TMP_TST

struct LDM_hashTable {
  U32 size;  // Number of buckets
  U32 maxEntries;  // Rename...
  LDM_hashEntry *entries;  // 1-D array for now.
  BYTE *bucketOffsets;     // Pointer to current insert position.

  // Position corresponding to offset=0 in LDM_hashEntry.
  const BYTE *offsetBase;
  U32 minMatchLength;
  U32 maxWindowSize;
};

LDM_hashTable *HASH_createTable(U32 size, const BYTE *offsetBase,
                                U32 minMatchLength, U32 maxWindowSize) {
  LDM_hashTable *table = malloc(sizeof(LDM_hashTable));
  table->size = size >> HASH_BUCKET_SIZE_LOG;
  table->maxEntries = size;
  table->entries = calloc(size, sizeof(LDM_hashEntry));
  table->bucketOffsets = calloc(size >> HASH_BUCKET_SIZE_LOG, sizeof(BYTE));
  table->offsetBase = offsetBase;
  table->minMatchLength = minMatchLength;
  table->maxWindowSize = maxWindowSize;
  return table;
}

static LDM_hashEntry *getBucket(const LDM_hashTable *table, const hash_t hash) {
  return table->entries + (hash << HASH_BUCKET_SIZE_LOG);
}

#if TMP_ZSTDTOGGLE
static unsigned ZSTD_NbCommonBytes (register size_t val)
{
    if (MEM_isLittleEndian()) {
        if (MEM_64bits()) {
#       if defined(_MSC_VER) && defined(_WIN64)
            unsigned long r = 0;
            _BitScanForward64( &r, (U64)val );
            return (unsigned)(r>>3);
#       elif defined(__GNUC__) && (__GNUC__ >= 3)
            return (__builtin_ctzll((U64)val) >> 3);
#       else
            static const int DeBruijnBytePos[64] = { 0, 0, 0, 0, 0, 1, 1, 2,
                                                     0, 3, 1, 3, 1, 4, 2, 7,
                                                     0, 2, 3, 6, 1, 5, 3, 5,
                                                     1, 3, 4, 4, 2, 5, 6, 7,
                                                     7, 0, 1, 2, 3, 3, 4, 6,
                                                     2, 6, 5, 5, 3, 4, 5, 6,
                                                     7, 1, 2, 4, 6, 4, 4, 5,
                                                     7, 2, 6, 5, 7, 6, 7, 7 };
            return DeBruijnBytePos[((U64)((val & -(long long)val) * 0x0218A392CDABBD3FULL)) >> 58];
#       endif
        } else { /* 32 bits */
#       if defined(_MSC_VER)
            unsigned long r=0;
            _BitScanForward( &r, (U32)val );
            return (unsigned)(r>>3);
#       elif defined(__GNUC__) && (__GNUC__ >= 3)
            return (__builtin_ctz((U32)val) >> 3);
#       else
            static const int DeBruijnBytePos[32] = { 0, 0, 3, 0, 3, 1, 3, 0,
                                                     3, 2, 2, 1, 3, 2, 0, 1,
                                                     3, 3, 1, 2, 2, 2, 2, 0,
                                                     3, 1, 2, 0, 1, 0, 1, 1 };
            return DeBruijnBytePos[((U32)((val & -(S32)val) * 0x077CB531U)) >> 27];
#       endif
        }
    } else {  /* Big Endian CPU */
        if (MEM_64bits()) {
#       if defined(_MSC_VER) && defined(_WIN64)
            unsigned long r = 0;
            _BitScanReverse64( &r, val );
            return (unsigned)(r>>3);
#       elif defined(__GNUC__) && (__GNUC__ >= 3)
            return (__builtin_clzll(val) >> 3);
#       else
            unsigned r;
            const unsigned n32 = sizeof(size_t)*4;   /* calculate this way due to compiler complaining in 32-bits mode */
            if (!(val>>n32)) { r=4; } else { r=0; val>>=n32; }
            if (!(val>>16)) { r+=2; val>>=8; } else { val>>=24; }
            r += (!val);
            return r;
#       endif
        } else { /* 32 bits */
#       if defined(_MSC_VER)
            unsigned long r = 0;
            _BitScanReverse( &r, (unsigned long)val );
            return (unsigned)(r>>3);
#       elif defined(__GNUC__) && (__GNUC__ >= 3)
            return (__builtin_clz((U32)val) >> 3);
#       else
            unsigned r;
            if (!(val>>16)) { r=2; val>>=8; } else { r=0; val>>=24; }
            r += (!val);
            return r;
#       endif
    }   }
}

// From lib/compress/zstd_compress.c
static size_t ZSTD_count(const BYTE *pIn, const BYTE *pMatch,
                         const BYTE *const pInLimit) {
    const BYTE * const pStart = pIn;
    const BYTE * const pInLoopLimit = pInLimit - (sizeof(size_t)-1);

    while (pIn < pInLoopLimit) {
        size_t const diff = MEM_readST(pMatch) ^ MEM_readST(pIn);
        if (!diff) {
          pIn += sizeof(size_t);
          pMatch += sizeof(size_t);
          continue;
        }
        pIn += ZSTD_NbCommonBytes(diff);
        return (size_t)(pIn - pStart);
    }

    if (MEM_64bits()) {
      if ((pIn < (pInLimit - 3)) && (MEM_read32(pMatch) == MEM_read32(pIn))) {
        pIn += 4;
        pMatch += 4;
      }
    }
    if ((pIn < (pInLimit - 1)) && (MEM_read16(pMatch) == MEM_read16(pIn))) {
      pIn += 2;
      pMatch += 2;
    }
    if ((pIn < pInLimit) && (*pMatch == *pIn)) {
      pIn++;
    }
    return (size_t)(pIn - pStart);
}

U32 countBackwardsMatch(const BYTE *pIn, const BYTE *pAnchor,
                        const BYTE *pMatch, const BYTE *pBase) {
  U32 matchLength = 0;
  while (pIn > pAnchor && pMatch > pBase && pIn[-1] == pMatch[-1]) {
    pIn--;
    pMatch--;
    matchLength++;
  }
  return matchLength;
}

LDM_hashEntry *HASH_getValidEntry(const LDM_hashTable *table,
                                  const hash_t hash,
                                  const U32 checksum,
                                  const BYTE *pIn,
                                  const BYTE *pEnd,
                                  U32 *matchLength,
                                  U32 *backwardsMatchLength,
                                  const BYTE *pAnchor) {
  LDM_hashEntry *bucket = getBucket(table, hash);
  LDM_hashEntry *cur = bucket;
  LDM_hashEntry *bestEntry = NULL;
  U32 bestMatchLength = 0;
  U32 forwardMatch = 0;
  U32 backwardMatch = 0;
#ifdef TMP_TST
  U32 numBetter = 0;
#endif
  for (; cur < bucket + HASH_BUCKET_SIZE; ++cur) {
    // Check checksum for faster check.
    const BYTE *pMatch = cur->offset + table->offsetBase;
    if (cur->checksum == checksum && pIn - pMatch <= table->maxWindowSize) {
      U32 forwardMatchLength = ZSTD_count(pIn, pMatch, pEnd);
      U32 backwardMatchLength, totalMatchLength;
      if (forwardMatchLength < table->minMatchLength) {
        continue;
      }
      backwardMatchLength =
          countBackwardsMatch(pIn, pAnchor, cur->offset + table->offsetBase,
                              table->offsetBase);

      totalMatchLength = forwardMatchLength + backwardMatchLength;

      if (totalMatchLength >= bestMatchLength) {
        bestMatchLength = totalMatchLength;
        forwardMatch = forwardMatchLength;
        backwardMatch = backwardMatchLength;
        bestEntry = cur;
#ifdef TMP_TST
        numBetter++;
#endif

#ifdef ZSTD_SKIP
        *matchLength = forwardMatchLength;
        *backwardsMatchLength = backwardMatchLength;

        return cur;
#endif
//        *matchLength = forwardMatchLength;
//        return cur;
      }
    }
  }
  if (bestEntry != NULL && bestMatchLength > table->minMatchLength) {
#ifdef TMP_TST
    printf("Num better %u\n", numBetter - 1);
#endif
    *matchLength = forwardMatch;
    *backwardsMatchLength = backwardMatch;
    return bestEntry;
  }
  return NULL;
}

#else

static int isValidMatch(const BYTE *pIn, const BYTE *pMatch,
                        U32 minMatchLength, U32 maxWindowSize) {
  printf("HERE\n");
  U32 lengthLeft = minMatchLength;
  const BYTE *curIn = pIn;
  const BYTE *curMatch = pMatch;

  if (pIn - pMatch > maxWindowSize) {
    return 0;
  }

  for (; lengthLeft >= 4; lengthLeft -= 4) {
    if (MEM_read32(curIn) != MEM_read32(curMatch)) {
      return 0;
    }
    curIn += 4;
    curMatch += 4;
  }
  return 1;
}

//TODO: clean up function call. This is not at all decoupled from LDM.
LDM_hashEntry *HASH_getValidEntry(const LDM_hashTable *table,
                                  const hash_t hash,
                                  const U32 checksum,
                                  const BYTE *pIn,
                                  const BYTE *pEnd,
                                  U32 *matchLength,
                                  U32 *backwardsMatchLength,
                                  const BYTE *pAnchor) {
  LDM_hashEntry *bucket = getBucket(table, hash);
  LDM_hashEntry *cur = bucket;
  (void)matchLength;
  (void)backwardsMatchLength;
  (void)pAnchor; for (; cur < bucket + HASH_BUCKET_SIZE; ++cur) {
    // Check checksum for faster check.
    const BYTE *pMatch = cur->offset + table->offsetBase;
    (void)pEnd;

    if (cur->checksum == checksum &&
        isValidMatch(pIn, pMatch, table->minMatchLength, table->maxWindowSize)) {
      return cur;
    }
  }
  return NULL;
}

#endif
hash_t HASH_hashU32(U32 value) {
  return ((value * 2654435761U) >> (32 - LDM_HASHLOG));
}


LDM_hashEntry *HASH_getEntryFromHash(const LDM_hashTable *table,
                                     const hash_t hash,
                                     const U32 checksum) {
  // Loop through bucket.
  // TODO: in order of recency???
  LDM_hashEntry *bucket = getBucket(table, hash);
  LDM_hashEntry *cur = bucket;
  for(; cur < bucket + HASH_BUCKET_SIZE; ++cur) {
    if (cur->checksum == checksum) {
      return cur;
    }
  }
  return NULL;
}

void HASH_insert(LDM_hashTable *table,
                 const hash_t hash, const LDM_hashEntry entry) {
  *(getBucket(table, hash) + table->bucketOffsets[hash]) = entry;
  table->bucketOffsets[hash]++;
  table->bucketOffsets[hash] &= HASH_BUCKET_SIZE - 1;
}

U32 HASH_getSize(const LDM_hashTable *table) {
  return table->size;
}

void HASH_destroyTable(LDM_hashTable *table) {
  free(table->entries);
  free(table->bucketOffsets);
  free(table);
}

void HASH_outputTableOccupancy(const LDM_hashTable *table) {
  U32 ctr = 0;
  LDM_hashEntry *cur = table->entries;
  LDM_hashEntry *end = table->entries + (table->size * HASH_BUCKET_SIZE);
  for (; cur < end; ++cur) {
    if (cur->offset == 0) {
      ctr++;
    }
  }

  printf("Num buckets, bucket size: %d, %d\n", table->size, HASH_BUCKET_SIZE);
  printf("Hash table size, empty slots, %% empty: %u, %u, %.3f\n",
         table->maxEntries, ctr,
         100.0 * (double)(ctr) / table->maxEntries);
}
