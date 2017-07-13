// TODO: file size must fit into a U32

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <zstd.h>

#include <fcntl.h>
#include "ldm.h"

#define DEBUG
//#define TEST

/* Compress file given by fname and output to oname.
 * Returns 0 if successful, error code otherwise.
 */
static int compress(const char *fname, const char *oname) {
  int fdin, fdout;
  struct stat statbuf;
  char *src, *dst;
  size_t maxCompressSize, compressSize;

  /* Open the input file. */
  if ((fdin = open(fname, O_RDONLY)) < 0) {
    perror("Error in file opening");
    return 1;
  }

  /* Open the output file. */
  if ((fdout = open(oname, O_RDWR | O_CREAT | O_TRUNC, (mode_t)0600)) < 0) {
    perror("Can't create output file");
    return 1;
  }

  /* Find the size of the input file. */
  if (fstat (fdin, &statbuf) < 0) {
    perror("Fstat error");
    return 1;
  }

  maxCompressSize = statbuf.st_size + LDM_HEADER_SIZE;

 /* Go to the location corresponding to the last byte. */
 /* TODO: fallocate? */
  if (lseek(fdout, maxCompressSize - 1, SEEK_SET) == -1) {
    perror("lseek error");
    return 1;
  }

 /* Write a dummy byte at the last location. */
  if (write(fdout, "", 1) != 1) {
    perror("write error");
    return 1;
  }

  /* mmap the input file. */
  if ((src = mmap(0, statbuf.st_size, PROT_READ,  MAP_SHARED, fdin, 0))
          == (caddr_t) - 1) {
      perror("mmap error for input");
      return 1;
  }

  /* mmap the output file */
  if ((dst = mmap(0, maxCompressSize, PROT_READ | PROT_WRITE,
                  MAP_SHARED, fdout, 0)) == (caddr_t) - 1) {
      perror("mmap error for output");
      return 1;
  }

/*
#ifdef TEST
  LDM_test(src, statbuf.st_size,
           dst + LDM_HEADER_SIZE, statbuf.st_size);
#endif
*/

  compressSize = LDM_HEADER_SIZE +
      LDM_compress(src, statbuf.st_size,
                   dst + LDM_HEADER_SIZE, statbuf.st_size);

  // Write compress and decompress size to header
  // TODO: should depend on LDM_DECOMPRESS_SIZE write32
  memcpy(dst, &compressSize, 8);
  memcpy(dst + 8, &(statbuf.st_size), 8);

#ifdef DEBUG
  printf("Compressed size: %zu\n", compressSize);
  printf("Decompressed size: %zu\n", (size_t)statbuf.st_size);
#endif

  // Truncate file to compressSize.
  ftruncate(fdout, compressSize);

  printf("%25s : %6u -> %7u - %s (%.1f%%)\n", fname,
         (unsigned)statbuf.st_size, (unsigned)compressSize, oname,
         (double)compressSize / (statbuf.st_size) * 100);

  // Close files.
  close(fdin);
  close(fdout);
  return 0;
}

/* Decompress file compressed using LDM_compress.
 * The input file should have the LDM_HEADER followed by payload.
 * Returns 0 if succesful, and an error code otherwise.
 */
static int decompress(const char *fname, const char *oname) {
  int fdin, fdout;
  struct stat statbuf;
  char *src, *dst;
  size_t compressSize, decompressSize, outSize;

  /* Open the input file. */
  if ((fdin = open(fname, O_RDONLY)) < 0) {
    perror("Error in file opening");
    return 1;
  }

  /* Open the output file. */
  if ((fdout = open(oname, O_RDWR | O_CREAT | O_TRUNC, (mode_t)0600)) < 0) {
    perror("Can't create output file");
    return 1;
  }

  /* Find the size of the input file. */
  if (fstat (fdin, &statbuf) < 0) {
    perror("Fstat error");
    return 1;
  }

  /* mmap the input file. */
  if ((src = mmap(0, statbuf.st_size, PROT_READ,  MAP_SHARED, fdin, 0))
          == (caddr_t) - 1) {
      perror("mmap error for input");
      return 1;
  }

  /* Read the header. */
  LDM_readHeader(src, &compressSize, &decompressSize);

  /* Go to the location corresponding to the last byte. */
  if (lseek(fdout, decompressSize - 1, SEEK_SET) == -1) {
    perror("lseek error");
    return 1;
  }

  /* write a dummy byte at the last location */
  if (write(fdout, "", 1) != 1) {
    perror("write error");
    return 1;
  }

  /* mmap the output file */
  if ((dst = mmap(0, decompressSize, PROT_READ | PROT_WRITE,
                  MAP_SHARED, fdout, 0)) == (caddr_t) - 1) {
      perror("mmap error for output");
      return 1;
  }

  outSize = LDM_decompress(
      src + LDM_HEADER_SIZE, statbuf.st_size - LDM_HEADER_SIZE,
      dst, decompressSize);

  printf("Ret size out: %zu\n", outSize);
  ftruncate(fdout, outSize);

  close(fdin);
  close(fdout);
  return 0;
}

/* Compare two files.
 * Returns 0 iff they are the same.
 */
static int compare(FILE *fp0, FILE *fp1) {
  int result = 0;
  while (result == 0) {
    char b0[1024];
    char b1[1024];
    const size_t r0 = fread(b0, 1, sizeof(b0), fp0);
    const size_t r1 = fread(b1, 1, sizeof(b1), fp1);

    result = (int)r0 - (int)r1;

    if (0 == r0 || 0 == r1) break;

    if (0 == result) result = memcmp(b0, b1, r0);
  }
  return result;
}

/* Verify the input file is the same as the decompressed file. */
static void verify(const char *inpFilename, const char *decFilename) {
  FILE *inpFp = fopen(inpFilename, "rb");
  FILE *decFp = fopen(decFilename, "rb");

  printf("verify : %s <-> %s\n", inpFilename, decFilename);
  {
    const int cmp = compare(inpFp, decFp);
    if(0 == cmp) {
      printf("verify : OK\n");
    } else {
      printf("verify : NG\n");
    }
  }

	fclose(decFp);
	fclose(inpFp);
}

int main(int argc, const char *argv[]) {
  const char * const exeName = argv[0];
  char inpFilename[256] = { 0 };
  char ldmFilename[256] = { 0 };
  char decFilename[256] = { 0 };

  if (argc < 2) {
    printf("Wrong arguments\n");
    printf("Usage:\n");
    printf("%s FILE\n", exeName);
    return 1;
  }

  snprintf(inpFilename, 256, "%s", argv[1]);
  snprintf(ldmFilename, 256, "%s.ldm", argv[1]);
  snprintf(decFilename, 256, "%s.ldm.dec", argv[1]);

 	printf("inp = [%s]\n", inpFilename);
	printf("ldm = [%s]\n", ldmFilename);
	printf("dec = [%s]\n", decFilename);


  /* Compress */
  {
    struct timeval tv1, tv2;
    gettimeofday(&tv1, NULL);
    if (compress(inpFilename, ldmFilename)) {
        printf("Compress error");
        return 1;
    }
    gettimeofday(&tv2, NULL);
    printf("Total compress time = %f seconds\n",
           (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 +
           (double) (tv2.tv_sec - tv1.tv_sec));
  }

  /* Decompress */
  {
    struct timeval tv1, tv2;
    gettimeofday(&tv1, NULL);
    if (decompress(ldmFilename, decFilename)) {
        printf("Decompress error");
        return 1;
    }
    gettimeofday(&tv2, NULL);
    printf("Total decompress time = %f seconds\n",
          (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 +
          (double) (tv2.tv_sec - tv1.tv_sec));
  }
  /* verify */
  verify(inpFilename, decFilename);
  return 0;
}
