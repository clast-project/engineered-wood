# Iceberg Java's DV and Puffin test vectors

Vendored unchanged from [apache/iceberg](https://github.com/apache/iceberg) at tag `apache-iceberg-1.12.0`,
the version the Spark corpus was written with. Apache License 2.0, as is this repository.

| Here | Upstream |
|---|---|
| `deletes/*.bin` | `core/src/test/resources/org/apache/iceberg/deletes/` |
| `puffin/v1/*.bin` | `core/src/test/resources/org/apache/iceberg/puffin/v1/` |

`vectors.json` records each file's SHA-256 and what it holds. The contents come from the Java tests that use
the files: `TestBitmapPositionDeleteIndex`, `TestRoaringPositionBitmap` and `TestPuffinReader`.

- `*-position-index.bin` are whole `deletion-vector-v1` blobs: a 4-byte big-endian length, the magic
  `D1 D3 39 64`, a portable 64-bit roaring bitmap, and a big-endian CRC-32 of the magic and bitmap. Java
  checks byte equality against what it serializes.
- `64map*.bin` are bare portable 64-bit roaring bitmaps, CRoaring's official format examples. Java reads
  three of them and refuses `64maphighvals.bin`, which is **mangled upstream**: every byte from 0x80 up was
  replaced by U+FFFD (`EF BF BD`), as a text conversion does, so it is not a valid bitmap. Java's refusal
  holds only because the first key it reads is that replacement text. Don't use it as a positive case.
- `puffin/v1/*.bin` are Puffin files: one empty, and one with two blobs both uncompressed and zstd-compressed.

`JavaVectorTests` checks the hashes and decodes every file independently of Java.
