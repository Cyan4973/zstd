#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdio.h>

#include "ldm.h"
#include "util.h"

#define HASH_EVERY 7

#define LDM_MEMORY_USAGE 14
#define LDM_HASHLOG (LDM_MEMORY_USAGE-2)
#define LDM_HASHTABLESIZE (1 << (LDM_MEMORY_USAGE))
#define LDM_HASHTABLESIZE_U32 ((LDM_HASHTABLESIZE) >> 2)
#define LDM_HASH_SIZE_U32 (1 << (LDM_HASHLOG))

#define LDM_OFFSET_SIZE 4

#define WINDOW_SIZE (1 << 20)
#define MAX_WINDOW_SIZE 31
#define HASH_SIZE 8
#define MINMATCH 8

#define ML_BITS 4
#define ML_MASK ((1U<<ML_BITS)-1)
#define RUN_BITS (8-ML_BITS)
#define RUN_MASK ((1U<<RUN_BITS)-1)

//#define LDM_DEBUG

typedef  uint8_t BYTE;
typedef uint16_t U16;
typedef uint32_t U32;
typedef  int32_t S32;
typedef uint64_t U64;

typedef uint32_t offset_t;
typedef uint32_t hash_t;

typedef struct LDM_hashEntry {
  offset_t offset;
} LDM_hashEntry;

typedef struct LDM_compressStats {
  U32 numMatches;
  U32 totalMatchLength;
  U32 totalLiteralLength;
  U64 totalOffset;
} LDM_compressStats;

static void LDM_printCompressStats(const LDM_compressStats *stats) {
  printf("=====================\n");
  printf("Compression statistics\n");
  printf("Total number of matches: %u\n", stats->numMatches);
  printf("Average match length: %.1f\n", ((double)stats->totalMatchLength) /
                                         (double)stats->numMatches);
  printf("Average literal length: %.1f\n",
         ((double)stats->totalLiteralLength) / (double)stats->numMatches);
  printf("Average offset length: %.1f\n",
         ((double)stats->totalOffset) / (double)stats->numMatches);
  printf("=====================\n");
}

typedef struct LDM_CCtx {
  size_t isize;             /* Input size */
  size_t maxOSize;          /* Maximum output size */

  const BYTE *ibase;        /* Base of input */
  const BYTE *ip;           /* Current input position */
  const BYTE *iend;         /* End of input */

  // Maximum input position such that hashing at the position does not exceed
  // end of input.
  const BYTE *ihashLimit;

  // Maximum input position such that finding a match of at least the minimum
  // match length does not exceed end of input.
  const BYTE *imatchLimit;

  const BYTE *obase;        /* Base of output */
  BYTE *op;                 /* Output */

  const BYTE *anchor;       /* Anchor to start of current (match) block */

  LDM_compressStats stats;            /* Compression statistics */

  LDM_hashEntry hashTable[LDM_HASHTABLESIZE_U32];

  const BYTE *lastPosHashed;          /* Last position hashed */
  hash_t lastHash;                    /* Hash corresponding to lastPosHashed */
  const BYTE *forwardIp;
  hash_t forwardHash;

  unsigned step;

} LDM_CCtx;


static hash_t LDM_hash(U32 sequence) {
  return ((sequence * 2654435761U) >> ((32)-LDM_HASHLOG));
}

/*
static hash_t LDM_hash5(U64 sequence) {
  static const U64 prime5bytes = 889523592379ULL;
  static const U64 prime8bytes = 11400714785074694791ULL;
  const U32 hashLog = LDM_HASHLOG;
  if (LDM_isLittleEndian())
    return (((sequence << 24) * prime5bytes) >> (64 - hashLog));
  else
    return (((sequence >> 24) * prime8bytes) >> (64 - hashLog));
}
*/

static hash_t LDM_hashPosition(const void * const p) {
  return LDM_hash(LDM_read32(p));
}

static void LDM_putHashOfCurrentPositionFromHash(
    LDM_CCtx *cctx, hash_t hash) {
  if (((cctx->ip - cctx->ibase) & HASH_EVERY) != HASH_EVERY) {
    return;
  }
  (cctx->hashTable)[hash] = (LDM_hashEntry){ (hash_t)(cctx->ip - cctx->ibase) };
  cctx->lastPosHashed = cctx->ip;
  cctx->lastHash = hash;
}

static void LDM_putHashOfCurrentPosition(LDM_CCtx *cctx) {
  hash_t hash = LDM_hashPosition(cctx->ip);
  LDM_putHashOfCurrentPositionFromHash(cctx, hash);
}

static const BYTE *LDM_get_position_on_hash(
    hash_t h, void *tableBase, const BYTE *srcBase) {
  const LDM_hashEntry * const hashTable = (LDM_hashEntry *)tableBase;
  return hashTable[h].offset + srcBase;
}

static BYTE LDM_read_byte(const void *memPtr) {
  BYTE val;
  memcpy(&val, memPtr, 1);
  return val;
}

static unsigned LDM_count(const BYTE *pIn, const BYTE *pMatch,
                          const BYTE *pInLimit) {
  const BYTE * const pStart = pIn;
  while (pIn < pInLimit - 1) {
    BYTE const diff = LDM_read_byte(pMatch) ^ LDM_read_byte(pIn);
    if (!diff) {
      pIn++;
      pMatch++;
      continue;
    }
    return (unsigned)(pIn - pStart);
  }
  return (unsigned)(pIn - pStart);
}

void LDM_readHeader(const void *src, size_t *compressSize,
                    size_t *decompressSize) {
  const U32 *ip = (const U32 *)src;
  *compressSize = *ip++;
  *decompressSize = *ip;
}

static void LDM_initializeCCtx(LDM_CCtx *cctx,
                               const void *src, size_t srcSize,
                               void *dst, size_t maxDstSize) {
  cctx->isize = srcSize;
  cctx->maxOSize = maxDstSize;

  cctx->ibase = (const BYTE *)src;
  cctx->ip = cctx->ibase;
  cctx->iend = cctx->ibase + srcSize;

  cctx->ihashLimit = cctx->iend - HASH_SIZE;
  cctx->imatchLimit = cctx->iend - MINMATCH;

  cctx->obase = (BYTE *)dst;
  cctx->op = (BYTE *)dst;

  cctx->anchor = cctx->ibase;

  memset(&(cctx->stats), 0, sizeof(cctx->stats));
  memset(cctx->hashTable, 0, sizeof(cctx->hashTable));

  cctx->lastPosHashed = NULL;
  cctx->forwardIp = NULL;

  cctx->step = 1;
}

static int LDM_findBestMatch(LDM_CCtx *cctx, const BYTE **match) {
  cctx->forwardIp = cctx->ip;

  do {
    hash_t const h = cctx->forwardHash;
    cctx->ip = cctx->forwardIp;
    cctx->forwardIp += cctx->step;

    if (cctx->forwardIp > cctx->imatchLimit) {
      return 1;
    }

    *match = LDM_get_position_on_hash(h, cctx->hashTable, cctx->ibase);

    cctx->forwardHash = LDM_hashPosition(cctx->forwardIp);
    LDM_putHashOfCurrentPositionFromHash(cctx, h);
  } while (cctx->ip - *match > WINDOW_SIZE ||
           LDM_read64(*match) != LDM_read64(cctx->ip));
  return 0;
}

// TODO: srcSize and maxDstSize is unused
size_t LDM_compress(const void *src, size_t srcSize,
                    void *dst, size_t maxDstSize) {
  LDM_CCtx cctx;
  LDM_initializeCCtx(&cctx, src, srcSize, dst, maxDstSize);

  /* Hash the first position and put it into the hash table. */
  LDM_putHashOfCurrentPosition(&cctx);
  cctx.ip++;
  cctx.forwardHash = LDM_hashPosition(cctx.ip);

  // TODO: loop condition is not accurate.
  while (1) {
    const BYTE *match;

    /**
     * Find a match.
     * If no more matches can be found (i.e. the length of the remaining input
     * is less than the minimum match length), then stop searching for matches
     * and encode the final literals.
     */
    if (LDM_findBestMatch(&cctx, &match) != 0) {
      goto _last_literals;
    }

    cctx.stats.numMatches++;

    /**
     * Catchup: look back to extend the match backwards from the found match.
     */
    while (cctx.ip > cctx.anchor && match > cctx.ibase &&
           cctx.ip[-1] == match[-1]) {
      cctx.ip--;
      match--;
    }

    /**
     * Write current block (literals, literal length, match offset, match
     * length) and update pointers and hashes.
     */
    {
      unsigned const literalLength = (unsigned)(cctx.ip - cctx.anchor);
      unsigned const offset = cctx.ip - match;
      unsigned const matchLength = LDM_count(
          cctx.ip + MINMATCH, match + MINMATCH, cctx.ihashLimit);
      BYTE *token = cctx.op++;

      cctx.stats.totalLiteralLength += literalLength;
      cctx.stats.totalOffset += offset;
      cctx.stats.totalMatchLength += matchLength + MINMATCH;

      /* Encode the literal length. */
      if (literalLength >= RUN_MASK) {
        int len = (int)literalLength - RUN_MASK;
        *token = (RUN_MASK << ML_BITS);
        for (; len >= 255; len -= 255) {
          *(cctx.op)++ = 255;
        }
        *(cctx.op)++ = (BYTE)len;
      } else {
        *token = (BYTE)(literalLength << ML_BITS);
      }

      /* Encode the literals. */
      memcpy(cctx.op, cctx.anchor, literalLength);
      cctx.op += literalLength;

      /* Encode the offset. */
      LDM_write32(cctx.op, offset);
      cctx.op += LDM_OFFSET_SIZE;

      /* Encode match length */
      if (matchLength >= ML_MASK) {
        unsigned matchLengthRemaining = matchLength;
        *token += ML_MASK;
        matchLengthRemaining -= ML_MASK;
        LDM_write32(cctx.op, 0xFFFFFFFF);
        while (matchLengthRemaining >= 4*0xFF) {
          cctx.op += 4;
          LDM_write32(cctx.op, 0xffffffff);
          matchLengthRemaining -= 4*0xFF;
        }
        cctx.op += matchLengthRemaining / 255;
        *(cctx.op)++ = (BYTE)(matchLengthRemaining % 255);
      } else {
        *token += (BYTE)(matchLength);
      }

      /* Update input pointer, inserting hashes into hash table along the
       * way.
       */
      while (cctx.ip < cctx.anchor + MINMATCH + matchLength + literalLength) {
        LDM_putHashOfCurrentPosition(&cctx);
        cctx.ip++;
      }
    }

    // Set start of next block to current input pointer.
    cctx.anchor = cctx.ip;
    LDM_putHashOfCurrentPosition(&cctx);
    cctx.forwardHash = LDM_hashPosition(++cctx.ip);
  }
_last_literals:
  /* Encode the last literals (no more matches). */
  {
    size_t const lastRun = (size_t)(cctx.iend - cctx.anchor);
    if (lastRun >= RUN_MASK) {
      size_t accumulator = lastRun - RUN_MASK;
      *(cctx.op)++ = RUN_MASK << ML_BITS;
      for(; accumulator >= 255; accumulator -= 255) {
        *(cctx.op)++ = 255;
      }
      *(cctx.op)++ = (BYTE)accumulator;
    } else {
      *(cctx.op)++ = (BYTE)(lastRun << ML_BITS);
    }
    memcpy(cctx.op, cctx.anchor, lastRun);
    cctx.op += lastRun;
  }
  LDM_printCompressStats(&cctx.stats);
  return (cctx.op - (const BYTE *)cctx.obase);
}

typedef struct LDM_DCtx {
  size_t compressSize;
  size_t maxDecompressSize;

  const BYTE *ibase;   /* Base of input */
  const BYTE *ip;      /* Current input position */
  const BYTE *iend;    /* End of source */

  const BYTE *obase;   /* Base of output */
  BYTE *op;            /* Current output position */
  const BYTE *oend;    /* End of output */
} LDM_DCtx;

static void LDM_initializeDCtx(LDM_DCtx *dctx,
                               const void *src, size_t compressSize,
                               void *dst, size_t maxDecompressSize) {
  dctx->compressSize = compressSize;
  dctx->maxDecompressSize = maxDecompressSize;

  dctx->ibase = src;
  dctx->ip = (const BYTE *)src;
  dctx->iend = dctx->ip + dctx->compressSize;
  dctx->op = dst;
  dctx->oend = dctx->op + dctx->maxDecompressSize;

}

size_t LDM_decompress(const void *src, size_t compressSize,
                      void *dst, size_t maxDecompressSize) {
  LDM_DCtx dctx;
  LDM_initializeDCtx(&dctx, src, compressSize, dst, maxDecompressSize);

  while (dctx.ip < dctx.iend) {
    BYTE *cpy;
    const BYTE *match;
    size_t length, offset;

    /* Get the literal length. */
    unsigned const token = *(dctx.ip)++;
    if ((length = (token >> ML_BITS)) == RUN_MASK) {
      unsigned s;
      do {
        s = *(dctx.ip)++;
        length += s;
      } while (s == 255);
    }

    /* Copy literals. */
    cpy = dctx.op + length;
    memcpy(dctx.op, dctx.ip, length);
    dctx.ip += length;
    dctx.op = cpy;

    //TODO : dynamic offset size
    offset = LDM_read32(dctx.ip);
    dctx.ip += LDM_OFFSET_SIZE;
    match = dctx.op - offset;

    /* Get the match length. */
    length = token & ML_MASK;
    if (length == ML_MASK) {
      unsigned s;
      do {
        s = *(dctx.ip)++;
        length += s;
      } while (s == 255);
    }
    length += MINMATCH;

    /* Copy match. */
    cpy = dctx.op + length;

    // Inefficient for now
    while (match < cpy - offset && dctx.op < dctx.oend) {
      *(dctx.op)++ = *match++;
    }
  }
  return dctx.op - (BYTE *)dst;
}


