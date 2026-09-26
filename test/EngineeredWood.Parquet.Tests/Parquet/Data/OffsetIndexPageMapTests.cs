// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

#pragma warning disable EWPARQUET0003 // one layout writes FSST, whose symbol table precedes the data pages

using System.Buffers;
using System.Buffers.Binary;
using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using EngineeredWood.Compression;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Data;
using EngineeredWood.Parquet.Metadata;
using EngineeredWood.Parquet.Schema;
using EngineeredWood.Tests.Parquet.Metadata;

namespace EngineeredWood.Tests.Parquet.Data;

/// <summary>
/// The batched read's page map built from a chunk's OffsetIndex
/// (<see cref="PageMapBuilder.BuildFromOffsetIndex"/>) instead of by reading the chunk to scan its
/// page headers. The oracle is the header scan: both maps must describe the same pages, and a
/// batched read must return what a whole read does.
/// </summary>
public class OffsetIndexPageMapTests : IDisposable
{
    private readonly string _tempDir;

    public OffsetIndexPageMapTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-offsetindex-map-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    public static TheoryData<string, DataPageVersion, CompressionCodec, bool, ByteArrayEncoding> Layouts() => new()
    {
        { "v2-snappy-dict", DataPageVersion.V2, CompressionCodec.Snappy, true, ByteArrayEncoding.DeltaLengthByteArray },
        { "v1-snappy-dict", DataPageVersion.V1, CompressionCodec.Snappy, true, ByteArrayEncoding.DeltaLengthByteArray },
        { "v2-plain", DataPageVersion.V2, CompressionCodec.Uncompressed, false, ByteArrayEncoding.Plain },
        { "v1-zstd", DataPageVersion.V1, CompressionCodec.Zstd, false, ByteArrayEncoding.DeltaByteArray },
        { "v2-fsst", DataPageVersion.V2, CompressionCodec.Uncompressed, false, ByteArrayEncoding.Fsst },
    };

    // ───── EW-written files ─────

    [Theory]
    [MemberData(nameof(Layouts))]
    public async Task BatchedRead_MatchesAWholeRead(
        string label, DataPageVersion pageVersion, CompressionCodec codec, bool dictionary, ByteArrayEncoding strings)
    {
        _ = label;
        string path = await WriteAsync(pageVersion, codec, dictionary, strings, writePageIndex: true);
        var whole = await ReadAsync(path, new ParquetReadOptions());

        foreach (var options in BatchOptions())
            AssertConcatenationEquals(whole, await ReadAsync(path, options));
    }

    [Theory]
    [MemberData(nameof(Layouts))]
    public async Task IndexMap_DescribesThePagesTheHeadersDo(
        string label, DataPageVersion pageVersion, CompressionCodec codec, bool dictionary, ByteArrayEncoding strings)
    {
        _ = label;
        string path = await WriteAsync(pageVersion, codec, dictionary, strings, writePageIndex: true);
        Assert.Equal(4, AssertIndexMapsAgree(path));
    }

    /// <summary>
    /// The point of the change: with an OffsetIndex, no read fetches a whole chunk's data pages to
    /// find them. Without one, the header scan does exactly that.
    /// </summary>
    [Fact]
    public async Task WithAnIndex_TheDataPagesAreNotReadWhole()
    {
        string indexed = await WriteAsync(DataPageVersion.V2, CompressionCodec.Snappy, true, ByteArrayEncoding.DeltaLengthByteArray, writePageIndex: true);
        string plain = await WriteAsync(DataPageVersion.V2, CompressionCodec.Snappy, true, ByteArrayEncoding.DeltaLengthByteArray, writePageIndex: false);

        var (indexedWhole, indexedBytes) = await CountWholeChunkReadsAsync(indexed);
        var (plainWhole, plainBytes) = await CountWholeChunkReadsAsync(plain);

        Assert.Equal(0, indexedWhole);
        Assert.Equal(4, plainWhole);
        Assert.True(indexedBytes < plainBytes, $"indexed read {indexedBytes} bytes, header scan {plainBytes}");
    }

    /// <summary>
    /// An index that cannot describe the chunk is ignored: the read falls back to the header scan
    /// and returns the right rows.
    /// </summary>
    [Fact]
    public async Task AnImpossibleIndex_FallsBackToTheHeaders()
    {
        string path = await WriteAsync(DataPageVersion.V2, CompressionCodec.Snappy, true, ByteArrayEncoding.DeltaLengthByteArray, writePageIndex: true);
        var whole = await ReadAsync(path, new ParquetReadOptions());

        // First rows that do not rise: pages 1 and 2 swapped. (A swap keeps the encoding's length.)
        TamperOffsetIndex(path, "id", locations =>
        {
            long first = locations[1].FirstRowIndex;
            locations[1] = locations[1] with { FirstRowIndex = locations[2].FirstRowIndex };
            locations[2] = locations[2] with { FirstRowIndex = first };
        });

        foreach (var options in BatchOptions())
            AssertConcatenationEquals(whole, await ReadAsync(path, options));
    }

    /// <summary>
    /// An index whose first page starts a few bytes into the real first data page's header makes
    /// the prefix before it end inside that header. The prefix cannot be scanned, and that is the
    /// index's fault, not the file's, so the read falls back to the header scan instead of failing.
    /// </summary>
    [Fact]
    public async Task AnIndexThatCutsTheDictionaryPrefixMidHeader_FallsBackToTheHeaders()
    {
        string path = await WriteAsync(DataPageVersion.V2, CompressionCodec.Snappy, true, ByteArrayEncoding.DeltaLengthByteArray, writePageIndex: true);
        var whole = await ReadAsync(path, new ParquetReadOptions());
        var (_, metadata) = ReadFooter(path);
        string dictionaryColumn = metadata.RowGroups[0].Columns
            .First(c => c.MetaData!.DictionaryPageOffset is > 0).MetaData!.PathInSchema[^1];

        TamperOffsetIndex(path, dictionaryColumn, locations => locations[0] = locations[0] with
        {
            Offset = locations[0].Offset + 3,
            CompressedPageSize = locations[0].CompressedPageSize - 3,
        });

        foreach (var options in BatchOptions())
            AssertConcatenationEquals(whole, await ReadAsync(path, options));
    }

    /// <summary>
    /// An index that is consistent on its own but moves a page boundary disagrees with the page's
    /// header, and that is refused rather than decoded into misaligned rows.
    /// </summary>
    [Fact]
    public async Task AnIndexThatDisagreesWithAHeader_IsRefused()
    {
        string path = await WriteAsync(DataPageVersion.V2, CompressionCodec.Snappy, true, ByteArrayEncoding.DeltaLengthByteArray, writePageIndex: true);
        TamperOffsetIndex(path, "id", locations => locations[1] = locations[1] with { FirstRowIndex = locations[1].FirstRowIndex + 1 });

        var ex = await Assert.ThrowsAsync<ParquetFormatException>(
            () => ReadAsync(path, new ParquetReadOptions { BatchSize = 1000 }));
        Assert.Contains("Column 'id': OffsetIndex page 0 spans", ex.Message);
    }

    [Fact]
    public void BuildFromOffsetIndex_IgnoresAnIndexThatCannotTileTheChunk()
    {
        string path = WriteAsync(DataPageVersion.V2, CompressionCodec.Uncompressed, false, ByteArrayEncoding.Plain, writePageIndex: true)
            .GetAwaiter().GetResult();
        var (file, metadata) = ReadFooter(path);
        var schema = SchemaDescriptorFor(metadata);
        var rowGroup = metadata.RowGroups[0];
        var chunk = rowGroup.Columns[0];
        var meta = chunk.MetaData!;
        var column = schema.Columns[0];
        int rows = (int)rowGroup.NumRows;
        long start = meta.DataPageOffset;
        long end = start + meta.TotalCompressedSize;
        var good = MetadataDecoder.DecodeOffsetIndex(Slice(file, chunk.OffsetIndexOffset!.Value, chunk.OffsetIndexLength!.Value)).PageLocations.ToArray();
        Assert.True(good.Length >= 3);

        ColumnPageMap? Build(Action<PageLocation[]> tamper)
        {
            var locations = (PageLocation[])good.Clone();
            tamper(locations);
            return PageMapBuilder.BuildFromOffsetIndex(
                ReadOnlySpan<byte>.Empty, start, end, new OffsetIndex { PageLocations = locations }, rows, column, meta);
        }

        Assert.NotNull(Build(_ => { }));
        Assert.Null(PageMapBuilder.BuildFromOffsetIndex(
            ReadOnlySpan<byte>.Empty, start, end, new OffsetIndex { PageLocations = [] }, rows, column, meta));
        Assert.Null(Build(l => l[0] = l[0] with { Offset = l[0].Offset + 1 }));
        Assert.Null(Build(l => l[0] = l[0] with { FirstRowIndex = 1 }));
        Assert.Null(Build(l => l[1] = l[1] with { Offset = l[0].Offset + l[0].CompressedPageSize - 1 }));
        Assert.Null(Build(l => l[^1] = l[^1] with { CompressedPageSize = (int)(end - l[^1].Offset) + 1 }));
        Assert.Null(Build(l => l[^1] = l[^1] with { FirstRowIndex = rows }));
        Assert.Null(Build(l => l[1] = l[1] with { CompressedPageSize = 0 }));
    }

    // ───── Fixtures from other writers ─────

    /// <summary>
    /// For every flat column chunk of every fixture that has an OffsetIndex (parquet-mr, parquet-cpp,
    /// parquet-rs), the index-built map locates the same pages, with the same rows, as the header
    /// scan, and each page's resolved header is the one the scan read.
    /// </summary>
    [Theory]
    [MemberData(nameof(PageIndexCodecTests.FixturesWithPageIndexes), MemberType = typeof(PageIndexCodecTests))]
    public void Fixture_IndexMapDescribesThePagesTheHeadersDo(string fileName)
    {
        AssertIndexMapsAgree(TestData.GetPath(fileName));
    }

    /// <summary>
    /// The theory above skips chunks without an OffsetIndex, repeated columns and chunks the scan
    /// cannot read, so on its own it would pass having compared nothing. When this was written, 22
    /// of the page-index fixtures had a flat chunk with an OffsetIndex; the other 4 have only
    /// repeated columns.
    /// </summary>
    [Fact]
    public void Fixtures_IndexMapsAreActuallyCompared()
    {
        var compared = PageIndexCodecTests.FixturesWithPageIndexes().Cast<object[]>()
            .Select(row => (string)row[0])
            .Where(name => AssertIndexMapsAgree(TestData.GetPath(name)) > 0)
            .ToList();

        Assert.True(compared.Count >= 22, $"only {compared.Count} fixtures compared an index map: {string.Join(", ", compared)}");
    }

    [Theory]
    [MemberData(nameof(PageIndexCodecTests.FixturesWithPageIndexes), MemberType = typeof(PageIndexCodecTests))]
    public async Task Fixture_BatchedReadMatchesAWholeRead(string fileName)
    {
        string path = TestData.GetPath(fileName);
        var (_, metadata) = ReadFooter(path);
        var flat = FlatTopLevelLeaves(metadata);
        if (flat.Count == 0)
            return;

        IReadOnlyList<RecordBatch> whole;
        try
        {
            whole = await ReadAsync(path, new ParquetReadOptions(), flat);
        }
        catch (Exception ex) when (ex is NotSupportedException or ParquetFormatException or AggregateException)
        {
            // Not this test's subject: an unsupported codec or type (net472 has no halffloat, and
            // two such columns fail together as an AggregateException), or a deliberately broken file.
            return;
        }

        foreach (var options in BatchOptions())
            AssertConcatenationEquals(whole, await ReadAsync(path, options, flat));
    }

    // ───── Helpers ─────

    private static IEnumerable<ParquetReadOptions> BatchOptions()
    {
        yield return new ParquetReadOptions { BatchSize = 1 };
        yield return new ParquetReadOptions { BatchSize = 7 };
        yield return new ParquetReadOptions { BatchSize = 1000 };
        yield return new ParquetReadOptions { MaxBatchByteSize = 4096 };
        yield return new ParquetReadOptions { BatchSize = 333, MaxBatchByteSize = 20_000 };
    }

    private async Task<string> WriteAsync(
        DataPageVersion pageVersion, CompressionCodec codec, bool dictionary, ByteArrayEncoding strings, bool writePageIndex)
    {
        const int rows = 20_000;
        var id = new Int64Array.Builder();
        var n = new Int32Array.Builder();
        var s = new StringArray.Builder();
        var d = new DoubleArray.Builder();
        for (int r = 0; r < rows; r++)
        {
            id.Append(r);
            if (r % 11 == 0) n.AppendNull(); else n.Append(r % 97);
            if (r % 13 == 0) s.AppendNull(); else s.Append("value-" + (r % 500).ToString("D5") + new string('x', r % 17));
            d.Append(r * 0.25);
        }

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("n", Int32Type.Default, true))
            .Field(new Field("s", StringType.Default, true))
            .Field(new Field("d", DoubleType.Default, false))
            .Build();
        var batch = new RecordBatch(schema, [id.Build(), n.Build(), s.Build(), d.Build()], rows);

        string path = Path.Combine(_tempDir, Guid.NewGuid().ToString("N")[..8] + ".parquet");
        await using var file = new LocalSequentialFile(path);
        await using var writer = new ParquetFileWriter(file, ownsFile: false, ParquetWriteOptions.Default with
        {
            WritePageIndex = writePageIndex,
            DataPageVersion = pageVersion,
            Compression = codec,
            DictionaryEnabled = dictionary,
            ByteArrayEncoding = strings,
            DataPageSize = 4096,
        });
        await writer.WriteRowGroupAsync(batch);
        await writer.CloseAsync();
        return path;
    }

    private static async Task<IReadOnlyList<RecordBatch>> ReadAsync(
        string path, ParquetReadOptions options, IReadOnlyList<string>? columns = null)
    {
        await using var input = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(input, ownsFile: false, options);
        var metadata = await reader.ReadMetadataAsync();
        var batches = new List<RecordBatch>();
        for (int g = 0; g < metadata.RowGroups.Count; g++)
        {
            await foreach (var batch in reader.ReadRowGroupBatchesAsync(g, columns))
                batches.Add(batch);
        }

        return batches;
    }

    /// <summary>
    /// Reads <paramref name="path"/> in batches of 1000 rows and returns how many reads fetched some
    /// chunk's data pages whole, and how many bytes were read in all.
    /// </summary>
    private static async Task<(int WholeChunkReads, long Bytes)> CountWholeChunkReadsAsync(string path)
    {
        var (_, metadata) = ReadFooter(path);
        var dataRegions = metadata.RowGroups.SelectMany(rg => rg.Columns)
            .Select(c => (Start: c.MetaData!.DataPageOffset, End: ChunkStart(c.MetaData) + c.MetaData.TotalCompressedSize))
            .ToList();

        await using var input = new CountingFile(new LocalRandomAccessFile(path));
        using var reader = new ParquetFileReader(input, ownsFile: false, new ParquetReadOptions { BatchSize = 1000 });
        await foreach (var _ in reader.ReadAllAsync()) { }

        int whole = input.Read.Count(r => dataRegions.Any(d => r.Offset <= d.Start && r.Offset + r.Length >= d.End));
        return (whole, input.Read.Sum(r => r.Length));
    }

    /// <summary>
    /// Builds, for every flat chunk of <paramref name="path"/> with an OffsetIndex, the index map
    /// and the header-scan map, and asserts they agree page for page. Returns how many were compared.
    /// </summary>
    private static int AssertIndexMapsAgree(string path)
    {
        var (file, metadata) = ReadFooter(path);
        var schema = SchemaDescriptorFor(metadata);
        int compared = 0;

        foreach (var rowGroup in metadata.RowGroups)
        {
            for (int c = 0; c < rowGroup.Columns.Count; c++)
            {
                var chunk = rowGroup.Columns[c];
                var column = schema.Columns[c];
                var meta = chunk.MetaData!;
                if (chunk.OffsetIndexOffset is not { } offset || column.MaxRepetitionLevel > 0 || rowGroup.NumRows == 0)
                    continue;

                long start = ChunkStart(meta);
                long end = start + meta.TotalCompressedSize;
                byte[] chunkBytes = Slice(file, start, (int)meta.TotalCompressedSize).ToArray();

                ColumnPageMap scanned;
                try
                {
                    scanned = PageMapBuilder.Build(chunkBytes, column, meta);
                }
                catch (Exception ex) when (ex is NotSupportedException or ParquetFormatException)
                {
                    continue; // e.g. a codec EW does not read; the header scan is the oracle, so skip
                }

                var index = MetadataDecoder.DecodeOffsetIndex(Slice(file, offset, chunk.OffsetIndexLength!.Value));
                var fromIndex = PageMapBuilder.BuildFromOffsetIndex(
                    chunkBytes.AsSpan(0, (int)(index.PageLocations[0].Offset - start)), start, end, index,
                    (int)rowGroup.NumRows, column, meta);

                string where = $"{Path.GetFileName(path)} column {column.DottedPath}";
                Assert.True(fromIndex is not null, $"{where}: index map refused");
                Assert.False(fromIndex!.HeadersResolved);
                Assert.Equal(scanned.CumulativeRows, fromIndex.CumulativeRows);
                Assert.Equal(scanned.Dictionary is null, fromIndex.Dictionary is null);
                Assert.Equal(scanned.SymbolTable is null, fromIndex.SymbolTable is null);

                for (int p = 0; p < fromIndex.Pages.Length; p++)
                {
                    var located = fromIndex.Pages[p];
                    var resolved = PageMapBuilder.ResolveEntry(
                        fromIndex, p, chunkBytes.AsSpan(located.Offset, located.CompressedSize), column, meta, out int headerSize);
                    Assert.Equal(scanned.Pages[p], resolved);
                    Assert.Equal(located.Offset + headerSize, resolved.Offset);
                }

                compared++;
            }
        }

        return compared;
    }

    /// <summary>The top-level columns that are non-repeated leaves: a read of only these takes the flat batched path.</summary>
    private static List<string> FlatTopLevelLeaves(FileMetaData metadata)
    {
        var leaves = new List<string>();
        int i = 1;
        for (int child = 0; child < metadata.Schema[0].NumChildren; child++)
        {
            var element = metadata.Schema[i];
            if (element.NumChildren is null or 0 && element.RepetitionType != FieldRepetitionType.Repeated)
                leaves.Add(element.Name);
            i = SkipSubtree(metadata.Schema, i);
        }

        return leaves;
    }

    private static int SkipSubtree(IReadOnlyList<SchemaElement> schema, int i)
    {
        int children = schema[i].NumChildren ?? 0;
        i++;
        for (int c = 0; c < children; c++)
            i = SkipSubtree(schema, i);
        return i;
    }

    /// <summary>Where the reader starts a chunk: its dictionary or symbol-table page, else its first data page.</summary>
    private static long ChunkStart(ColumnMetaData meta) =>
        meta.DictionaryPageOffset is > 0 and long dpo ? dpo
        : meta.SymbolTablePageOffset is > 0 and long stpo ? stpo
        : meta.DataPageOffset;

    private static SchemaDescriptor SchemaDescriptorFor(FileMetaData metadata) => new(metadata.Schema);

    /// <summary>Rewrites one column's OffsetIndexes in <paramref name="path"/> in place; the encoding must keep its length.</summary>
    private static void TamperOffsetIndex(string path, string column, Action<PageLocation[]> tamper)
    {
        var (file, metadata) = ReadFooter(path);
        foreach (var chunk in metadata.RowGroups.SelectMany(rg => rg.Columns))
        {
            if (chunk.MetaData!.PathInSchema[^1] != column)
                continue;

            int offset = checked((int)chunk.OffsetIndexOffset!.Value);
            int length = chunk.OffsetIndexLength!.Value;
            var locations = MetadataDecoder.DecodeOffsetIndex(file.AsSpan(offset, length)).PageLocations.ToArray();
            tamper(locations);
            byte[] encoded = MetadataEncoder.EncodeOffsetIndex(new OffsetIndex { PageLocations = locations });
            Assert.Equal(length, encoded.Length);
            encoded.CopyTo(file, offset);
        }

        File.WriteAllBytes(path, file);
    }

    private static ReadOnlySpan<byte> Slice(byte[] file, long offset, int length) =>
        file.AsSpan(checked((int)offset), length);

    private static (byte[] Bytes, FileMetaData Metadata) ReadFooter(string path)
    {
        byte[] bytes = File.ReadAllBytes(path);
        int length = BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(bytes.Length - 8, 4));
        return (bytes, MetadataDecoder.DecodeFileMetaData(bytes.AsSpan(bytes.Length - 8 - length, length)));
    }

    /// <summary>
    /// Asserts that <paramref name="actual"/>, concatenated, holds the same flat columns, cell for
    /// cell, as <paramref name="expected"/>.
    /// </summary>
    private static void AssertConcatenationEquals(IReadOnlyList<RecordBatch> expected, IReadOnlyList<RecordBatch> actual)
    {
        Assert.Equal(expected.Sum(b => b.Length), actual.Sum(b => b.Length));
        if (expected.Count == 0)
            return;

        int columns = expected[0].ColumnCount;
        for (int c = 0; c < columns; c++)
        {
            var expectedCells = expected.SelectMany(b => Cells(b.Column(c))).ToList();
            var actualCells = actual.SelectMany(b => Cells(b.Column(c))).ToList();
            for (int r = 0; r < expectedCells.Count; r++)
            {
                if (expectedCells[r] != actualCells[r])
                    Assert.Fail($"column {expected[0].Schema.FieldsList[c].Name}, row {r}: expected {expectedCells[r]}, got {actualCells[r]}");
            }
        }
    }

    /// <summary>Each cell of a flat array, as a string that is equal exactly when the values are.</summary>
    private static IEnumerable<string> Cells(IArrowArray array)
    {
        for (int i = 0; i < array.Length; i++)
            yield return Cell(array, i);
    }

    private static string Cell(IArrowArray array, int i)
    {
        if (array.IsNull(i))
            return "<null>";

        switch (array)
        {
            case BooleanArray b:
                return b.GetValue(i)!.Value ? "true" : "false";
            case BinaryArray b:
                return Convert.ToBase64String(b.GetBytes(i).ToArray());
            case FixedSizeBinaryArray f:
                return Convert.ToBase64String(f.GetBytes(i).ToArray());
            case NullArray:
                return "<null>";
        }

        if (array.Data.DataType is FixedWidthType fixedWidth && fixedWidth.BitWidth % 8 == 0)
        {
            int width = fixedWidth.BitWidth / 8;
            return Convert.ToBase64String(
                array.Data.Buffers[1].Span.Slice((array.Data.Offset + i) * width, width).ToArray());
        }

        throw new NotSupportedException($"No cell comparison for {array.Data.DataType.Name}.");
    }

    /// <summary>Records every range read.</summary>
    private sealed class CountingFile(IRandomAccessFile inner) : IRandomAccessFile
    {
        private readonly List<FileRange> _read = new();

        public IReadOnlyList<FileRange> Read => _read;

        public ValueTask<long> GetLengthAsync(CancellationToken cancellationToken = default) =>
            inner.GetLengthAsync(cancellationToken);

        public ValueTask<IMemoryOwner<byte>> ReadAsync(FileRange range, CancellationToken cancellationToken = default)
        {
            lock (_read) _read.Add(range);
            return inner.ReadAsync(range, cancellationToken);
        }

        public ValueTask<IReadOnlyList<IMemoryOwner<byte>>> ReadRangesAsync(
            IReadOnlyList<FileRange> ranges, CancellationToken cancellationToken = default)
        {
            lock (_read) _read.AddRange(ranges);
            return inner.ReadRangesAsync(ranges, cancellationToken);
        }

        public ValueTask DisposeAsync() => inner.DisposeAsync();

        public void Dispose() => inner.Dispose();
    }
}
