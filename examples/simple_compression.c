/*
  Simple compression
  Educational program using zstd library
  Copyright (C) Yann Collet 2016

  GPL v2 License

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation; either version 2 of the License, or
  (at your option) any later version.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License along
  with this program; if not, write to the Free Software Foundation, Inc.,
  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

  You can contact the author at :
  - zstd homepage : http://www.zstd.net/
*/

#include <stdlib.h>    // malloc, exit
#include <stdio.h>     // printf
#include <string.h>    // strerror
#include <errno.h>     // errno
#include <sys/stat.h>  // stat
#include <zstd.h>      // presumes zstd library is installed


static off_t fsize_X(const char *filename)
{
    struct stat st;
    if (stat(filename, &st) == 0) return st.st_size;
    /* error */
    printf("stat: %s : %s \n", filename, strerror(errno));
    exit(1);
}

static FILE* fopen_X(const char *filename, const char *instruction)
{
    FILE* const inFile = fopen(filename, instruction);
    if (inFile) return inFile;
    /* error */
    printf("fopen: %s : %s \n", filename, strerror(errno));
    exit(2);
}

static void* malloc_X(size_t size)
{
    void* const buff = malloc(size);
    if (buff) return buff;
    /* error */
    printf("malloc: %s \n", strerror(errno));
    exit(3);
}

static void* loadFile_X(const char* fileName, size_t* size)
{
    off_t const buffSize = fsize_X(fileName);
    FILE* const inFile = fopen_X(fileName, "rb");
    void* const buffer = malloc_X(buffSize);
    size_t const readSize = fread(buffer, 1, buffSize, inFile);
    if (readSize != (size_t)buffSize) {
        printf("fread: %s : %s \n", fileName, strerror(errno));
        exit(4);
    }
    fclose(inFile);
    *size = buffSize;
    return buffer;
}


static void compress(const char* fname)
{
    size_t fSize;
    void* const fBuff = loadFile_X(fname, &fSize);
    size_t const cBuffSize = ZSTD_compressBound(fSize);
    void* const cBuff = malloc_X(cBuffSize);

    size_t const cSize = ZSTD_compress(cBuff, cBuffSize, fBuff, fSize, 1);
    if (ZSTD_isError(cSize)) {
        printf("error compressing %s : %s \n", fname, ZSTD_getErrorName(cSize));
        exit(7);
    }

    /* success */
    printf("%25s : %6u -> %7u \n", fname, (unsigned)fSize, (unsigned)cSize);

    free(fBuff);
    free(cBuff);
}


int main(int argc, const char** argv)
{
    const char* const exeName = argv[0];

    if (argc!=2) {
        printf("wrong arguments\n");
        printf("usage:\n");
        printf("%s FILE\n", exeName);
        return 1;
    }

    compress(argv[1]);

    printf("%s compressed. \n", argv[1]);
}
