# Linux Kernel Patch

There are two pieces, the `zstd_compress` and `zstd_decompress` kernel modules, and the BtrFS patch.
The patches are based off of the linux kernel master branch (version 4.10).

## Zstd Kernel modules

* The header is in `include/linux/zstd.h`.
* It is split up into `zstd_compress` and `zstd_decompress`, which can be loaded independently.
* Source files are in `lib/zstd/`.
* `lib/Kconfig` and `lib/Makefile` need to be modified by applying `lib/Kconfig.diff` and `lib/Makefile.diff` respectively.
* `test/UserlandTest.cpp` contains tests for the patch in userland by mocking the kernel headers.
  It can be run with the following commands:
  ```
  cd test
  make googletest
  make UserlandTest
  ./UserlandTest
  ```

## BtrFS

* The patch is located in `btrfs.diff`.
* Additionally `fs/btrfs/zstd.c` is provided as a source for convenience.
* The patch seems to be working, it doesn't crash the kernel, and compresses at speeds and ratios athat are expected.
  It can still use some more testing for fringe features, like printing options.

### Benchmarks

Benchmarks run on a Ubuntu 14.04 with 2 cores and 4 GiB of RAM.
The VM is running on a Macbook Pro with a 3.1 GHz Intel Core i7 processor,
16 GB of ram, and a SSD.

The compression benchmark is copying 10 copies of the
unzipped [silesia corpus](http://mattmahoney.net/dc/silesia.html) into a BtrFS
filesystem mounted with `-o compress-force={none, lzo, zlib, zstd}`.
The decompression benchmark is timing how long it takes to `tar` all 10 copies
into `/dev/null`.
The compression ratio is measured by comparing the output of `df` and `du`.
See `btrfs-benchmark.sh` for details.

| Algorithm | Compression ratio | Compression speed | Decompression speed |
|-----------|-------------------|-------------------|---------------------|
| None      | 0.99              | 504 MB/s          | 686 MB/s            |
| lzo       | 1.66              | 398 MB/s          | 442 MB/s            |
| zlib      | 2.58              | 65 MB/s           | 241 MB/s            |
| zstd 1    | 2.57              | 260 MB/s          | 383 MB/s            |
| zstd 3    | 2.71              | 174 MB/s          | 408 MB/s            |
| zstd 6    | 2.87              | 70 MB/s           | 398 MB/s            |
| zstd 9    | 2.92              | 43 MB/s           | 406 MB/s            |
| zstd 12   | 2.93              | 21 MB/s           | 408 MB/s            |
| zstd 15   | 3.01              | 11 MB/s           | 354 MB/s            |
