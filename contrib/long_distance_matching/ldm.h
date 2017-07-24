#ifndef LDM_H
#define LDM_H

#include <stddef.h>   /* size_t */

#include "mem.h"    // from /lib/common/mem.h

#define LDM_COMPRESS_SIZE 8
#define LDM_DECOMPRESS_SIZE 8
#define LDM_HEADER_SIZE ((LDM_COMPRESS_SIZE)+(LDM_DECOMPRESS_SIZE))
#define LDM_OFFSET_SIZE 4

// Defines the size of the hash table.
// Note that this is not the number of buckets.
// Currently this should be less than WINDOW_SIZE_LOG + 4?
#define LDM_MEMORY_USAGE 22
#define HASH_BUCKET_SIZE_LOG 3 // MAX is 4 for now

// Defines the lag in inserting elements into the hash table.
#define LDM_LAG 0

#define LDM_WINDOW_SIZE_LOG 28
#define LDM_WINDOW_SIZE (1 << (LDM_WINDOW_SIZE_LOG))

//These should be multiples of four (and perhaps set to the same value?).
#define LDM_MIN_MATCH_LENGTH 64
#define LDM_HASH_LENGTH 64


typedef struct LDM_compressStats LDM_compressStats;
typedef struct LDM_CCtx LDM_CCtx;
typedef struct LDM_DCtx LDM_DCtx;


/**
 *  Compresses src into dst.
 *
 *  NB: This currently ignores maxDstSize and assumes enough space is available.
 *
 *  Block format (see lz4 documentation for more information):
 *  github.com/lz4/lz4/blob/dev/doc/lz4_Block_format.md
 *
 *  A block is composed of sequences. Each sequence begins with a token, which
 *  is a one-byte value separated into two 4-bit fields.
 *
 *  The first field uses the four high bits of the token and encodes the literal
 *  length. If the field value is 0, there is no literal. If it is 15,
 *  additional bytes are added (each ranging from 0 to 255) to the previous
 *  value to produce a total length.
 *
 *  Following the token and optional length bytes are the literals.
 *
 *  Next are the 4 bytes representing the offset of the match (2 in lz4),
 *  representing the position to copy the literals.
 *
 *  The lower four bits of the token encode the match length. With additional
 *  bytes added similarly to the additional literal length bytes after the offset.
 *
 *  The last sequence is incomplete and stops right after the literals.
 *
 */
size_t LDM_compress(const void *src, size_t srcSize,
                    void *dst, size_t maxDstSize);

/**
 * Initialize the compression context.
 *
 * Allocates memory for the hash table.
 */
void LDM_initializeCCtx(LDM_CCtx *cctx,
                        const void *src, size_t srcSize,
                        void *dst, size_t maxDstSize);

/**
 * Frees up memory allocating in initializeCCtx
 */
void LDM_destroyCCtx(LDM_CCtx *cctx);

/**
 * Prints the distribution of offsets in the hash table.
 *
 * The offsets are defined as the distance of the hash table entry from the
 * current input position of the cctx.
 */
void LDM_outputHashTableOffsetHistogram(const LDM_CCtx *cctx);

/**
 * Outputs compression statistics to stdout.
 */
void LDM_printCompressStats(const LDM_compressStats *stats);
/**
 * Checks whether the LDM_MIN_MATCH_LENGTH bytes from p are the same as the
 * LDM_MIN_MATCH_LENGTH bytes from match and also if
 * pIn - pMatch <= LDM_WINDOW_SIZE.
 *
 * This assumes LDM_MIN_MATCH_LENGTH is a multiple of four.
 *
 * Return 1 if valid, 0 otherwise.
 */
int LDM_isValidMatch(const BYTE *pIn, const BYTE *pMatch);

/**
 *  Counts the number of bytes that match from pIn and pMatch,
 *  up to pInLimit.
 */
U32 LDM_countMatchLength(const BYTE *pIn, const BYTE *pMatch,
                         const BYTE *pInLimit);

/**
 * Encode the literal length followed by the literals.
 *
 * The literal length is written to the upper four bits of pToken, with
 * additional bytes written to the output as needed (see lz4).
 *
 * This is followed by literalLength bytes corresponding to the literals.
 */
void LDM_encodeLiteralLengthAndLiterals(
    LDM_CCtx *cctx, BYTE *pToken, const U32 literalLength);

/**
 * Write current block (literals, literal length, match offset,
 * match length).
 */
void LDM_outputBlock(LDM_CCtx *cctx,
                     const U32 literalLength,
                     const U32 offset,
                     const U32 matchLength);

/**
 * Decompresses src into dst.
 *
 * Note: assumes src does not have a header.
 */
size_t LDM_decompress(const void *src, size_t srcSize,
                      void *dst, size_t maxDstSize);

/**
 * Initialize the decompression context.
 */
void LDM_initializeDCtx(LDM_DCtx *dctx,
                        const void *src, size_t compressedSize,
                        void *dst, size_t maxDecompressedSize);

/**
 * Reads the header from src and writes the compressed size and
 * decompressed size into compressedSize and decompressedSize respectively.
 *
 * NB: LDM_compress and LDM_decompress currently do not add/read headers.
 */
void LDM_readHeader(const void *src, U64 *compressedSize,
                    U64 *decompressedSize);

void LDM_outputConfiguration(void);

void LDM_test(const BYTE *src);

#endif /* LDM_H */
