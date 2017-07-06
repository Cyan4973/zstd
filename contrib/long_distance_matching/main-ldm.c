#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <zstd.h>

#include <fcntl.h>
#include "ldm.h"

#define BUF_SIZE 16*1024  // Block size
#define LDM_HEADER_SIZE 8
#define DEBUG

#if 0
static size_t compress_file(FILE *in, FILE *out, size_t *size_in,
                            size_t *size_out) {
  char *src, *buf = NULL;
  size_t r = 1;
  size_t size, n, k, count_in = 0, count_out = 0, offset, frame_size = 0;

  src = malloc(BUF_SIZE);
  if (!src) {
    printf("Not enough memory\n");
    goto cleanup;
  }

  size = BUF_SIZE + LDM_HEADER_SIZE;
  buf = malloc(size);
  if (!buf) {
    printf("Not enough memory\n");
    goto cleanup;
  }


  for (;;) {
    k = fread(src, 1, BUF_SIZE, in);
    if (k == 0)
      break;
    count_in += k;

    n = LDM_compress(src, buf, k, BUF_SIZE);

    // n = k;
    // offset += n;
    offset = k;
    count_out += k;

//    k = fwrite(src, 1, offset, out);

    k = fwrite(buf, 1, offset, out);
    if (k < offset) {
      if (ferror(out))
        printf("Write failed\n");
      else
        printf("Short write\n");
      goto cleanup;
    }

  }
  *size_in = count_in;
  *size_out = count_out;
  r = 0;
 cleanup:
  free(src);
  free(buf);
  return r;
}

static size_t decompress_file(FILE *in, FILE *out) {
  void *src = malloc(BUF_SIZE);
  void *dst = NULL;
  size_t dst_capacity = BUF_SIZE;
  size_t ret = 1;
  size_t bytes_written = 0;

  if (!src) {
    perror("decompress_file(src)");
    goto cleanup;
  }

  while (ret != 0) {
    /* Load more input */
    size_t src_size = fread(src, 1, BUF_SIZE, in);
    void *src_ptr = src;
    void *src_end = src_ptr + src_size;
    if (src_size == 0 || ferror(in)) {
      printf("(TODO): Decompress: not enough input or error reading file\n");
      //TODO
      ret = 0;
      goto cleanup;
    }

    /* Allocate destination buffer if it hasn't been allocated already */
    if (!dst) {
      dst = malloc(dst_capacity);
      if (!dst) {
        perror("decompress_file(dst)");
        goto cleanup;
      }
    }

    // TODO

    /* Decompress:
     * Continue while there is more input to read.
     */
    while (src_ptr != src_end && ret != 0) {
      // size_t dst_size = src_size;
      size_t dst_size = LDM_decompress(src, dst, src_size, dst_capacity);
      size_t written = fwrite(dst, 1, dst_size, out);
//      printf("Writing %zu bytes\n", dst_size);
      bytes_written += dst_size;
      if (written != dst_size) {
        printf("Decompress: Failed to write to file\n");
        goto cleanup;
      }
      src_ptr += src_size;
      src_size = src_end - src_ptr;
    }

    /* Update input */

  }

  printf("Wrote %zu bytes\n", bytes_written);

 cleanup:
  free(src);
  free(dst);

  return ret;
}
#endif

static size_t compress(const char *fname, const char *oname) {
  int fdin, fdout;
  struct stat statbuf;
  char *src, *dst;

  /* open the input file */
  if ((fdin = open(fname, O_RDONLY)) < 0) {
    perror("Error in file opening");
    return 1;
  }

  /* open the output file */
  if ((fdout = open(oname, O_RDWR | O_CREAT | O_TRUNC, (mode_t)0600)) < 0) {
    perror("Can't create output file");
    return 1;
  }

  /* find size of input file */
  if (fstat (fdin, &statbuf) < 0) {
    perror("Fstat error");
    return 1;
  }

 /* go to the location corresponding to the last byte */
 if (lseek(fdout, statbuf.st_size - 1, SEEK_SET) == -1) {
    perror("lseek error");
    return 1;
 }
 
 /* write a dummy byte at the last location */
 if (write(fdout, "", 1) != 1) {
     perror("write error");
     return 1;
 }

  /* mmap the input file */
  if ((src = mmap(0, statbuf.st_size, PROT_READ,  MAP_SHARED, fdin, 0))
          == (caddr_t) - 1) {
      perror("mmap error for input");
      return 1;
  }

  /* mmap the output file */
  if ((dst = mmap(0, statbuf.st_size, PROT_READ | PROT_WRITE,
                  MAP_SHARED, fdout, 0)) == (caddr_t) - 1) {
      perror("mmap error for output");
      return 1;
  }

  /* Copy input file to output file */
//  memcpy(dst, src, statbuf.st_size);
  size_t size_out = ZSTD_compress(dst, statbuf.st_size, 
                                  src, statbuf.st_size, 1);
  printf("%25s : %6u -> %7u - %s (%.1f%%)\n", fname, 
         (unsigned)statbuf.st_size, (unsigned)size_out, oname,
         (double)size_out / (statbuf.st_size) * 100);

  close(fdin);
  close(fdout);
  return 0;
}

static size_t decompress(const char *fname, const char *oname) {
  int fdin, fdout;
  struct stat statbuf;
  char *src, *dst;

  /* open the input file */
  if ((fdin = open(fname, O_RDONLY)) < 0) {
    perror("Error in file opening");
    return 1;
  }

  /* open the output file */
  if ((fdout = open(oname, O_RDWR | O_CREAT | O_TRUNC, (mode_t)0600)) < 0) {
    perror("Can't create output file");
    return 1;
  }

  /* find size of input file */
  if (fstat (fdin, &statbuf) < 0) {
    perror("Fstat error");
    return 1;
  }

  /* go to the location corresponding to the last byte */
  if (lseek(fdout, statbuf.st_size - 1, SEEK_SET) == -1) {
    perror("lseek error");
    return 1;
  }
 
  /* write a dummy byte at the last location */
  if (write(fdout, "", 1) != 1) {
    perror("write error");
    return 1;
  }

  /* mmap the input file */
  if ((src = mmap(0, statbuf.st_size, PROT_READ,  MAP_SHARED, fdin, 0))
          == (caddr_t) - 1) {
      perror("mmap error for input");
      return 1;
  }

  /* mmap the output file */
  if ((dst = mmap(0, statbuf.st_size, PROT_READ | PROT_WRITE,
                  MAP_SHARED, fdout, 0)) == (caddr_t) - 1) {
      perror("mmap error for output");
      return 1;
  }

  /* Copy input file to output file */
//  memcpy(dst, src, statbuf.st_size);

  size_t size_out = ZSTD_decompress(dst, statbuf.st_size, 
                                    src, statbuf.st_size);


  close(fdin);
  close(fdout);
  return 0;
}

static int compare(FILE *fp0, FILE *fp1) {
  int result = 0;
  while (result == 0) {
    char b0[1024];
    char b1[1024];
    const size_t r0 = fread(b0, 1, sizeof(b0), fp0);
    const size_t r1 = fread(b1, 1, sizeof(b1), fp1);

    result = (int)r0 - (int)r1;

    if (0 == r0 || 0 == r1) {
      break;
    }
    if (0 == result) {
      result = memcmp(b0, b1, r0);
    }
  }
  return result;
}

static void verify(const char *inpFilename, const char *decFilename) {
  FILE *inpFp = fopen(inpFilename, "rb");
  FILE *decFp = fopen(decFilename, "rb");

  printf("verify : %s <-> %s\n", inpFilename, decFilename);
	const int cmp = compare(inpFp, decFp);
	if(0 == cmp) {
		printf("verify : OK\n");
	} else {
		printf("verify : NG\n");
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

  /* compress */
  if (compress(inpFilename, ldmFilename)) {
      printf("Compress error");
      return 1;
  }

  /* decompress */
  if (decompress(ldmFilename, decFilename)) {
      printf("Decompress error");
      return 1;
  }

  /* verify */
  verify(inpFilename, decFilename);
}

#if 0
int main2(int argc, char *argv[]) {
  char inpFilename[256] = { 0 };
  char ldmFilename[256] = { 0 };
  char decFilename[256] = { 0 };

  if (argc < 2) {
    printf("Please specify input filename\n");
    return 0;
  }
  snprintf(inpFilename, 256, "%s", argv[1]);
  snprintf(ldmFilename, 256, "%s.ldm", argv[1]);
  snprintf(decFilename, 256, "%s.ldm.dec", argv[1]);

    printf("inp = [%s]\n", inpFilename);
	printf("ldm = [%s]\n", ldmFilename);
	printf("dec = [%s]\n", decFilename);

  /* compress */
  {
    FILE *inpFp = fopen(inpFilename, "rb");
    FILE *outFp = fopen(ldmFilename, "wb");
    size_t sizeIn = 0;
    size_t sizeOut = 0;
    size_t ret;
		printf("compress : %s -> %s\n", inpFilename, ldmFilename);
		ret = compress_file(inpFp, outFp, &sizeIn, &sizeOut);
		if (ret) {
			printf("compress : failed with code %zu\n", ret);
			return ret;
		}
		printf("%s: %zu → %zu bytes, %.1f%%\n",
			inpFilename, sizeIn, sizeOut,
			(double)sizeOut / sizeIn * 100);
		printf("compress : done\n");

		fclose(outFp);
		fclose(inpFp);
  }

	/* decompress */
	{
    FILE *inpFp = fopen(ldmFilename, "rb");
		FILE *outFp = fopen(decFilename, "wb");
		size_t ret;

		printf("decompress : %s -> %s\n", ldmFilename, decFilename);
		ret = decompress_file(inpFp, outFp);
		if (ret) {
			printf("decompress : failed with code %zu\n", ret);
			return ret;
		}
		printf("decompress : done\n");

		fclose(outFp);
		fclose(inpFp);
	}

	/* verify */
	{
    FILE *inpFp = fopen(inpFilename, "rb");
		FILE *decFp = fopen(decFilename, "rb");

		printf("verify : %s <-> %s\n", inpFilename, decFilename);
		const int cmp = compare(inpFp, decFp);
		if(0 == cmp) {
			printf("verify : OK\n");
		} else {
			printf("verify : NG\n");
		}

		fclose(decFp);
		fclose(inpFp);
	}
  return 0;
}
#endif

