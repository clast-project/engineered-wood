# EngineeredWood.Parquet.Tests

## Known pre-existing failures

Running the full suite on `master` produces 12 failing tests (per target
framework) that are not caused by recent changes. They fall into two
clusters:

### ALP decoder fixture-missing failures (7 tests)

```
AlpDecoderTests.ReadParquetTesting_AlpArade_BitExact
AlpDecoderTests.ReadParquetTesting_AlpSpotify1_BitExact
AlpDecoderTests.ReadParquetTesting_AlpFloatArade_BitExact
AlpDecoderTests.ReadParquetTesting_AlpFloatSpotify1_BitExact
AlpDecoderTests.ReadParquetTesting_AlpJavaArade_BitExact
AlpDecoderTests.ReadParquetTesting_AlpJavaSpotify1_BitExact
AlpDecoderTests.ReadParquetTesting_AlpJavaFloatArade_BitExact
AlpDecoderTests.ReadParquetTesting_AlpJavaFloatSpotify1_BitExact
```

Failure message: `Missing test data file: alp_arade.parquet` (etc.).

These tests expect `alp_*.parquet` / `alp_*_expect.csv` files from
[apache/parquet-testing PR #100](https://github.com/apache/parquet-testing/pull/100).
The `parquet-testing` submodule is pinned to a commit that pre-dates
that PR, so the fixture files simply aren't on disk and the decoder
code path is never exercised. To clear these, bump the submodule to a
commit that includes PR #100.

### `dict-page-offset-zero.parquet` reader gap (4 tests)

```
ReadRowGroupTests.DictPageOffsetZero_EdgeCase
ReadRowGroupTests.SweepTest_AllFlatPlainDictSnappyUncompressedFiles
MetadataDecoderTests.AllTestFiles_ParseWithoutError
ParquetFileReaderTests.AllTestFiles_ParseThroughFullPipeline
```

Failure message: `Cannot skip unknown Thrift type 14.`

All four hit the same root cause when reading
`parquet-testing/data/dict-page-offset-zero.parquet`. The proximate
error is from `ThriftCompactReader.Skip`, but Thrift compact's type
field is the wire type — parquet-format never emits types above 12, so
seeing `14` means the metadata reader desynced earlier (mis-decoded a
varint, missed a pending-bool, or consumed the wrong element count for
a list/map) and is now interpreting a value byte as a field header.

This is a real `MetadataDecoder` bug, not a malformed fixture: the file
isn't in any "*malformed*" skip list and there's a dedicated
`DictPageOffsetZero_EdgeCase` test asserting it should round-trip
(`batch.Length > 0`). The fix is to trace the reader against the
parquet-format thrift schema for this file and locate the misalignment.

## Skipping the known failures locally

To run only the tests that should pass:

```
dotnet test test/EngineeredWood.Parquet.Tests/EngineeredWood.Parquet.Tests.csproj `
  --filter "FullyQualifiedName!~AlpDecoderTests&FullyQualifiedName!~DictPageOffsetZero_EdgeCase&FullyQualifiedName!=EngineeredWood.Tests.Parquet.Metadata.MetadataDecoderTests.AllTestFiles_ParseWithoutError&FullyQualifiedName!~SweepTest_AllFlatPlainDictSnappyUncompressedFiles&FullyQualifiedName!=EngineeredWood.Tests.Parquet.ParquetFileReaderTests.AllTestFiles_ParseThroughFullPipeline"
```
