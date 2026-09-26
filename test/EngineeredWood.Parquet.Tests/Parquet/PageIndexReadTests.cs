// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers;
using System.Buffers.Binary;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Metadata;
using EngineeredWood.Tests.Parquet.Metadata;

namespace EngineeredWood.Tests.Parquet;

/// <summary>
/// <see cref="ParquetFileReader.ReadPageIndexAsync"/>:
/// reading a row group's page index, from files written by parquet-mr, parquet-cpp, parquet-rs and
/// EngineeredWood.
/// </summary>
public class PageIndexReadTests : IDisposable
{
    private readonly string _tempDir;

    public PageIndexReadTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-pageindex-read-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    // ───── Fixtures ─────

    /// <summary>
    /// For every row group and column of every fixture that carries a page index, the reader returns
    /// exactly what decoding the footer's byte ranges directly gives, and reports absence where the
    /// footer records none.
    /// </summary>
    [Theory]
    [MemberData(nameof(PageIndexCodecTests.FixturesWithPageIndexes), MemberType = typeof(PageIndexCodecTests))]
    public async Task Fixture_ReadsWhatTheFooterLocates(string fileName)
    {
        string path = TestData.GetPath(fileName);
        byte[] file = File.ReadAllBytes(path);
        await using var input = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(input, ownsFile: false);
        var metadata = await reader.ReadMetadataAsync();

        for (int g = 0; g < metadata.RowGroups.Count; g++)
        {
            var indexes = await reader.ReadPageIndexAsync(g);
            var chunks = metadata.RowGroups[g].Columns;
            Assert.Equal(chunks.Count, indexes.Count);

            for (int c = 0; c < chunks.Count; c++)
            {
                var chunk = chunks[c];
                var index = indexes[c];
                Assert.Equal(c, index.Column);

                bool hasColumnIndex = chunk.ColumnIndexOffset is not null && chunk.ColumnIndexLength is not null;
                bool hasOffsetIndex = chunk.OffsetIndexOffset is not null && chunk.OffsetIndexLength is not null;
                Assert.Equal(hasColumnIndex, index.HasColumnIndex);
                Assert.Equal(hasOffsetIndex, index.HasOffsetIndex);

                if (hasColumnIndex)
                {
                    var expected = file.AsSpan(checked((int)chunk.ColumnIndexOffset!.Value), chunk.ColumnIndexLength!.Value);
                    Assert.Equal(expected.ToArray(), MetadataEncoder.EncodeColumnIndex(index.ColumnIndex!));
                }
                else
                {
                    Assert.Null(index.ColumnIndex);
                }

                if (hasOffsetIndex)
                {
                    var expected = file.AsSpan(checked((int)chunk.OffsetIndexOffset!.Value), chunk.OffsetIndexLength!.Value);
                    Assert.Equal(expected.ToArray(), MetadataEncoder.EncodeOffsetIndex(index.OffsetIndex!));
                }
                else
                {
                    Assert.Null(index.OffsetIndex);
                }
            }
        }
    }

    [Fact]
    public async Task Fixture_OffsetIndexWithoutColumnIndex()
    {
        // parquet-mr writes no ColumnIndex for INT96 (its order is undefined).
        var index = (await ReadAsync(TestData.GetPath("int96_from_spark.parquet"), 0)).Single();

        Assert.True(index.HasOffsetIndex);
        Assert.False(index.HasColumnIndex);
        Assert.NotEmpty(index.OffsetIndex!.PageLocations);
        Assert.Null(index.ColumnIndex);
    }

    [Fact]
    public async Task Fixture_NullPages()
    {
        var index = (await ReadAsync(TestData.GetPath("int32_with_null_pages.parquet"), 0)).Single();
        Assert.Contains(true, index.ColumnIndex!.NullPages);
    }

    /// <summary>
    /// floating_orders_nan_count (parquet-mr) pairs each float type under both orders; row group 1
    /// holds NaNs. Under IEEE 754 total order the ColumnIndex is kept, with NaN counts. Under
    /// TYPE_ORDER a page of only NaNs has no valid bound, and the writer omits the ColumnIndex while
    /// keeping the OffsetIndex, which is the rule EngineeredWood's writer follows too.
    /// </summary>
    [Fact]
    public async Task Fixture_NanCounts_AndTheTypeOrderOmission()
    {
        var indexes = await ReadAsync(TestData.GetPath("floating_orders_nan_count.parquet"), 1);

        foreach (int totalOrder in new[] { 0, 2, 4 }) // float_ieee754, double_ieee754, float16_ieee754
        {
            Assert.True(indexes[totalOrder].HasColumnIndex);
            Assert.True(indexes[totalOrder].ColumnIndex!.NanCounts!.Sum() > 0);
        }

        foreach (int typeOrder in new[] { 1, 3, 5 }) // float_typedef, double_typedef, float16_typedef
        {
            Assert.False(indexes[typeOrder].HasColumnIndex);
            Assert.True(indexes[typeOrder].HasOffsetIndex);
        }
    }

    [Fact]
    public async Task FileWithoutIndexes_ReportsAbsenceAndReadsNothing()
    {
        await using var input = new CountingFile(new LocalRandomAccessFile(TestData.GetPath("alltypes_plain.parquet")));
        using var reader = new ParquetFileReader(input, ownsFile: false);
        await reader.ReadMetadataAsync();
        input.Reset();

        var indexes = await reader.ReadPageIndexAsync(0);

        Assert.All(indexes, i =>
        {
            Assert.False(i.HasColumnIndex);
            Assert.False(i.HasOffsetIndex);
            Assert.Null(i.ColumnIndex);
            Assert.Null(i.OffsetIndex);
        });
        Assert.Equal(0, input.Calls);
    }

    // ───── EW-written files ─────

    [Fact]
    public async Task SelectedColumns_ComeBackInTheOrderAsked()
    {
        string path = await WriteSortedAsync(rowGroups: 3);
        await using var input = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(input, ownsFile: false);

        var all = await reader.ReadPageIndexAsync(1);
        var some = await reader.ReadPageIndexAsync(1, ["s", "x"]);

        Assert.Equal([0, 1, 2], all.Select(i => i.Column));
        Assert.Equal([2, 0], some.Select(i => i.Column));
        Assert.Equal(["s", "x"], some.Select(i => i.Path.Single()));
        Assert.Equal(MetadataEncoder.EncodeColumnIndex(all[2].ColumnIndex!), MetadataEncoder.EncodeColumnIndex(some[0].ColumnIndex!));
        Assert.Equal(MetadataEncoder.EncodeOffsetIndex(all[0].OffsetIndex!), MetadataEncoder.EncodeOffsetIndex(some[1].OffsetIndex!));
    }

    /// <summary>
    /// Columns are named as for ReadRowGroupAsync: a top-level group selects all its leaves, and a
    /// leaf can also be named by its dotted path.
    /// </summary>
    [Fact]
    public async Task GroupName_SelectsItsLeaves()
    {
        var a = new Int32Array.Builder();
        var b = new StringArray.Builder();
        var id = new Int64Array.Builder();
        for (int i = 0; i < 100; i++)
        {
            a.Append(i);
            b.Append("b" + i);
            id.Append(i);
        }

        var structType = new StructType([new Field("a", Int32Type.Default, false), new Field("b", StringType.Default, false)]);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("st", structType, false))
            .Build();
        var batch = new RecordBatch(schema,
            [id.Build(), new StructArray(structType, 100, [a.Build(), b.Build()], ArrowBuffer.Empty, 0)], 100);
        string path = Path.Combine(_tempDir, "struct.parquet");
        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false))
        {
            await writer.WriteRowGroupAsync(batch);
            await writer.CloseAsync();
        }

        await using var input = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(input, ownsFile: false);

        var group = await reader.ReadPageIndexAsync(0, ["st"]);
        var leaf = await reader.ReadPageIndexAsync(0, ["st.b"]);

        Assert.Equal(["st.a", "st.b"], group.Select(i => string.Join(".", i.Path)));
        Assert.Equal([1, 2], group.Select(i => i.Column));
        Assert.Equal(2, leaf.Single().Column);
        Assert.All(group, i => Assert.True(i.HasColumnIndex && i.HasOffsetIndex));
    }

    /// <summary>
    /// Selecting nothing must be callable without a cast. With a second overload on
    /// IReadOnlyList&lt;int&gt;, <c>null</c>, <c>default</c> and <c>[]</c> were ambiguous.
    /// </summary>
    [Fact]
    public async Task NullDefaultAndEmpty_AreUnambiguous()
    {
        string path = await WriteSortedAsync(rowGroups: 1);
        await using var input = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(input, ownsFile: false);

        Assert.Equal(3, (await reader.ReadPageIndexAsync(0, null)).Count);
        Assert.Equal(3, (await reader.ReadPageIndexAsync(0, default)).Count);
        Assert.Empty(await reader.ReadPageIndexAsync(0, []));
    }

    /// <summary>
    /// On a sorted column the index can be checked against the data: in row group g, x = row, so a
    /// page's minimum is its first row and its maximum the row before the next page's first.
    /// </summary>
    [Fact]
    public async Task SortedColumn_BoundsMatchTheRows()
    {
        const int rowsPerGroup = 10_000;
        string path = await WriteSortedAsync(rowGroups: 3, rowsPerGroup);
        await using var input = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(input, ownsFile: false);

        for (int g = 0; g < 3; g++)
        {
            var x = (await reader.ReadPageIndexAsync(g, ["x"])).Single();
            var locations = x.OffsetIndex!.PageLocations;
            Assert.True(locations.Count > 5);
            for (int p = 0; p < locations.Count; p++)
            {
                long first = g * (long)rowsPerGroup + locations[p].FirstRowIndex;
                long last = g * (long)rowsPerGroup + (p + 1 < locations.Count ? locations[p + 1].FirstRowIndex : rowsPerGroup) - 1;
                Assert.Equal(first, BinaryPrimitives.ReadInt64LittleEndian(x.ColumnIndex!.MinValues[p]));
                Assert.Equal(last, BinaryPrimitives.ReadInt64LittleEndian(x.ColumnIndex.MaxValues[p]));
            }
        }
    }

    // ───── I/O ─────

    [Fact]
    public async Task OneRequest_FetchesEveryRequestedIndex()
    {
        string path = await WriteSortedAsync(rowGroups: 3);
        await using var input = new CountingFile(new LocalRandomAccessFile(path));
        using var reader = new ParquetFileReader(input, ownsFile: false);
        await reader.ReadMetadataAsync();
        input.Reset();

        var indexes = await reader.ReadPageIndexAsync(1);

        Assert.Equal(1, input.Calls);
        Assert.InRange(input.Ranges, 1, 2);
        Assert.All(indexes, i => Assert.True(i.HasColumnIndex && i.HasOffsetIndex));
    }

    /// <summary>The page index is read only when asked for: reading every row group never touches it.</summary>
    [Fact]
    public async Task ReadingRowGroups_NeverTouchesTheIndexRegion()
    {
        string path = await WriteSortedAsync(rowGroups: 3);
        var (bytes, metadata) = ReadFooter(path);
        long indexStart = metadata.RowGroups.SelectMany(rg => rg.Columns).Min(c => Math.Min(c.ColumnIndexOffset!.Value, c.OffsetIndexOffset!.Value));
        long footerStart = bytes.Length - 8 - BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(bytes.Length - 8));

        await using var input = new CountingFile(new LocalRandomAccessFile(path));
        using var reader = new ParquetFileReader(input, ownsFile: false);
        await reader.ReadMetadataAsync();
        input.Reset();
        for (int g = 0; g < metadata.RowGroups.Count; g++)
            await reader.ReadRowGroupAsync(g);

        Assert.All(input.Read, range => Assert.True(
            range.Offset + range.Length <= indexStart || range.Offset >= footerStart,
            $"read [{range.Offset}, {range.Offset + range.Length}) overlaps the index region [{indexStart}, {footerStart})"));
    }

    // ───── Malformed input and arguments ─────

    [Fact]
    public async Task IndexOutsideTheFile_IsRejected()
    {
        string path = await RewriteFooterAsync(c => new ColumnChunk
        {
            FileOffset = c.FileOffset,
            MetaData = c.MetaData,
            ColumnIndexOffset = 1L << 40,
            ColumnIndexLength = 100,
            OffsetIndexOffset = c.OffsetIndexOffset,
            OffsetIndexLength = c.OffsetIndexLength,
        });

        var e = await Assert.ThrowsAsync<ParquetFormatException>(() => ReadAsync(path, 0).AsTask());
        Assert.Contains("outside the file", e.Message);
    }

    /// <summary>
    /// A chunk stored in another file has offsets into that file. Reading them from this one would
    /// return unrelated bytes, so the chunk is refused instead.
    /// </summary>
    [Fact]
    public async Task ExternalColumnChunk_IsRefused()
    {
        string path = await RewriteFooterAsync(c => new ColumnChunk
        {
            FilePath = "part-00001.parquet",
            FileOffset = c.FileOffset,
            MetaData = c.MetaData,
            ColumnIndexOffset = c.ColumnIndexOffset,
            ColumnIndexLength = c.ColumnIndexLength,
            OffsetIndexOffset = c.OffsetIndexOffset,
            OffsetIndexLength = c.OffsetIndexLength,
        });

        var e = await Assert.ThrowsAsync<NotSupportedException>(() => ReadAsync(path, 0).AsTask());
        Assert.Contains("part-00001.parquet", e.Message);
    }

    [Fact]
    public async Task HalfSpecifiedRange_IsAbsent()
    {
        string path = await RewriteFooterAsync(c => new ColumnChunk
        {
            FileOffset = c.FileOffset,
            MetaData = c.MetaData,
            ColumnIndexOffset = c.ColumnIndexOffset,
            ColumnIndexLength = null,
            OffsetIndexOffset = c.OffsetIndexOffset,
            OffsetIndexLength = c.OffsetIndexLength,
        });

        Assert.All(await ReadAsync(path, 0), i =>
        {
            Assert.False(i.HasColumnIndex);
            Assert.True(i.HasOffsetIndex);
        });
    }

    /// <summary>A corrupt index is found when it is decoded, and does not stop the others being read.</summary>
    [Fact]
    public async Task CorruptIndex_FailsOnAccess()
    {
        string path = await WriteSortedAsync(rowGroups: 1);
        var (bytes, metadata) = ReadFooter(path);
        var chunk = metadata.RowGroups[0].Columns[0];
        bytes.AsSpan(checked((int)chunk.ColumnIndexOffset!.Value), chunk.ColumnIndexLength!.Value).Clear();
        File.WriteAllBytes(path, bytes);

        var indexes = await ReadAsync(path, 0);

        Assert.True(indexes[0].HasColumnIndex);
        Assert.Throws<ParquetFormatException>(() => indexes[0].ColumnIndex);
        Assert.NotNull(indexes[0].OffsetIndex);
        Assert.NotNull(indexes[1].ColumnIndex);
    }

    [Fact]
    public async Task Arguments_AreChecked()
    {
        string path = await WriteSortedAsync(rowGroups: 1);
        await using var input = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(input, ownsFile: false);

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => reader.ReadPageIndexAsync(1).AsTask());
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => reader.ReadPageIndexAsync(-1).AsTask());
        await Assert.ThrowsAsync<ArgumentException>(() => reader.ReadPageIndexAsync(0, ["nope"]).AsTask());
    }

    // ───── Helpers ─────

    private static async ValueTask<IReadOnlyList<ColumnChunkPageIndex>> ReadAsync(string path, int rowGroup)
    {
        await using var input = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(input, ownsFile: false);
        return await reader.ReadPageIndexAsync(rowGroup);
    }

    /// <summary>Three columns — x = row number, k = row / 100, s a string — in row groups of <paramref name="rowsPerGroup"/>.</summary>
    private async Task<string> WriteSortedAsync(int rowGroups, int rowsPerGroup = 10_000)
    {
        int rows = rowGroups * rowsPerGroup;
        var x = new Int64Array.Builder();
        var k = new Int32Array.Builder();
        var s = new StringArray.Builder();
        for (int r = 0; r < rows; r++)
        {
            x.Append(r);
            k.Append(r / 100);
            s.Append("value-" + r.ToString("D7"));
        }

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", Int64Type.Default, false))
            .Field(new Field("k", Int32Type.Default, false))
            .Field(new Field("s", StringType.Default, false))
            .Build();
        var batch = new RecordBatch(schema, [x.Build(), k.Build(), s.Build()], rows);

        string path = Path.Combine(_tempDir, Guid.NewGuid().ToString("N")[..8] + ".parquet");
        await using var file = new LocalSequentialFile(path);
        await using var writer = new ParquetFileWriter(file, ownsFile: false, ParquetWriteOptions.Default with
        {
            WritePageIndex = true,
            DataPageSize = 4096,
            RowGroupMaxRows = rowsPerGroup,
        });
        await writer.WriteRowGroupAsync(batch);
        await writer.CloseAsync();
        return path;
    }

    private static (byte[] Bytes, FileMetaData Metadata) ReadFooter(string path)
    {
        byte[] bytes = File.ReadAllBytes(path);
        int length = BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(bytes.Length - 8, 4));
        return (bytes, MetadataDecoder.DecodeFileMetaData(bytes.AsSpan(bytes.Length - 8 - length, length)));
    }

    /// <summary>Rewrites every column chunk of an EW-written file's footer, leaving the body as it is.</summary>
    private async Task<string> RewriteFooterAsync(Func<ColumnChunk, ColumnChunk> rewrite)
    {
        string path = await WriteSortedAsync(rowGroups: 1);
        var (bytes, metadata) = ReadFooter(path);
        int footerLength = BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(bytes.Length - 8, 4));
        int bodyLength = bytes.Length - 8 - footerLength;

        var rewritten = new FileMetaData
        {
            Version = metadata.Version,
            Schema = metadata.Schema,
            NumRows = metadata.NumRows,
            RowGroups = metadata.RowGroups.Select(rg => new RowGroup
            {
                Columns = rg.Columns.Select(rewrite).ToArray(),
                TotalByteSize = rg.TotalByteSize,
                NumRows = rg.NumRows,
                TotalCompressedSize = rg.TotalCompressedSize,
                Ordinal = rg.Ordinal,
            }).ToArray(),
            CreatedBy = metadata.CreatedBy,
            KeyValueMetadata = metadata.KeyValueMetadata,
            ColumnOrders = metadata.ColumnOrders,
        };
        byte[] footer = MetadataEncoder.EncodeFileMetaData(rewritten);

        using var output = new MemoryStream();
        output.Write(bytes, 0, bodyLength);
        output.Write(footer, 0, footer.Length);
        var suffix = new byte[8];
        BinaryPrimitives.WriteInt32LittleEndian(suffix, footer.Length);
        "PAR1"u8.CopyTo(suffix.AsSpan(4));
        output.Write(suffix, 0, suffix.Length);

        string rewrittenPath = Path.Combine(_tempDir, Guid.NewGuid().ToString("N")[..8] + ".parquet");
        File.WriteAllBytes(rewrittenPath, output.ToArray());
        return rewrittenPath;
    }

    /// <summary>Records every range read, and how many calls read them.</summary>
    private sealed class CountingFile(IRandomAccessFile inner) : IRandomAccessFile
    {
        private readonly List<FileRange> _read = new();

        public int Calls { get; private set; }

        public int Ranges => _read.Count;

        public IReadOnlyList<FileRange> Read => _read;

        public void Reset()
        {
            Calls = 0;
            _read.Clear();
        }

        public ValueTask<long> GetLengthAsync(CancellationToken cancellationToken = default) =>
            inner.GetLengthAsync(cancellationToken);

        public ValueTask<IMemoryOwner<byte>> ReadAsync(FileRange range, CancellationToken cancellationToken = default)
        {
            Calls++;
            _read.Add(range);
            return inner.ReadAsync(range, cancellationToken);
        }

        public ValueTask<IReadOnlyList<IMemoryOwner<byte>>> ReadRangesAsync(
            IReadOnlyList<FileRange> ranges, CancellationToken cancellationToken = default)
        {
            Calls++;
            _read.AddRange(ranges);
            return inner.ReadRangesAsync(ranges, cancellationToken);
        }

        public ValueTask DisposeAsync() => inner.DisposeAsync();

        public void Dispose() => inner.Dispose();
    }
}
