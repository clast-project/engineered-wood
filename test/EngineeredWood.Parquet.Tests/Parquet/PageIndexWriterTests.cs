// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Compression;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Data;
using EngineeredWood.Parquet.Metadata;

namespace EngineeredWood.Tests.Parquet;

/// <summary>
/// Page-index writing (<see cref="ParquetWriteOptions.WritePageIndex"/>): the ColumnIndex and
/// OffsetIndex of every column chunk, checked against the file they describe.
/// </summary>
/// <remarks>
/// The checks do not trust the encoder. Every <see cref="PageLocation"/> must name a real data-page
/// header, with its size and its first row; every page's bounds must be consistent with the chunk
/// statistics (the minimum of the page minimums is the chunk minimum, and so on); and on a sorted
/// column each page's bounds must be exactly its first and last values. The external oracles —
/// DataFusion, which prunes with the index, and pyarrow — are in <see cref="PageIndexInteropTests"/>.
/// </remarks>
public class PageIndexWriterTests : IDisposable
{
    private readonly string _tempDir;

    public PageIndexWriterTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-pageindex-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private static readonly ParquetWriteOptions Indexed = ParquetWriteOptions.Default with
    {
        WritePageIndex = true,
        DataPageSize = 1024,
    };

    // ───── Default ─────

    [Fact]
    public async Task Default_WritesNoPageIndex()
    {
        Assert.False(ParquetWriteOptions.Default.WritePageIndex);

        string path = await WriteAsync(MixedBatch(1000, nullable: true), ParquetWriteOptions.Default);

        Assert.All(ReadFile(path).Metadata.RowGroups.SelectMany(rg => rg.Columns), c =>
        {
            Assert.Null(c.OffsetIndexOffset);
            Assert.Null(c.ColumnIndexOffset);
        });
    }

    // ───── Self-consistency across the matrix ─────

    public static TheoryData<DataPageVersion, bool, bool> Matrix()
    {
        var data = new TheoryData<DataPageVersion, bool, bool>();
        foreach (var version in new[] { DataPageVersion.V1, DataPageVersion.V2 })
        foreach (bool dictionary in new[] { true, false })
        foreach (bool nullable in new[] { true, false })
            data.Add(version, dictionary, nullable);
        return data;
    }

    [Theory]
    [MemberData(nameof(Matrix))]
    public async Task FileWriter_IndexesAgreeWithTheFile(DataPageVersion version, bool dictionary, bool nullable)
    {
        var options = Indexed with
        {
            DataPageVersion = version,
            DictionaryEnabled = dictionary,
            PageIndexTruncateLength = null,
            RowGroupMaxRows = 3000,
        };
        string path = await WriteAsync(MixedBatch(7000, nullable, nested: true), options);

        // 3 row groups of 12 leaves: the 10 flat columns, the list's element and the struct's field.
        Assert.Equal(3 * 12, AssertIndexesAgreeWithFile(path, expectColumnIndexOnEveryChunk: true));
    }

    [Theory]
    [MemberData(nameof(Matrix))]
    public async Task BufferedWriter_IndexesAgreeWithTheFile(DataPageVersion version, bool dictionary, bool nullable)
    {
        var options = Indexed with
        {
            DataPageVersion = version,
            DictionaryEnabled = dictionary,
            PageIndexTruncateLength = null,
            RowGroupMaxRows = 3000,
        };
        var batch = MixedBatch(7000, nullable, nested: false);
        string path = Path.Combine(_tempDir, Guid.NewGuid().ToString("N")[..8] + ".parquet");
        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new BufferedParquetWriter(file, ownsFile: false, options))
        {
            int offset = 0;
            foreach (int length in new[] { 1000, 2500, 3500 })
            {
                await writer.AppendAsync(Slice(batch, offset, length));
                offset += length;
            }

            await writer.CloseAsync();
        }

        // The buffered writer flushes everything it holds once it passes the limit: 3500 rows, then 3500.
        Assert.Equal(2 * 10, AssertIndexesAgreeWithFile(path, expectColumnIndexOnEveryChunk: true));
    }

    // ───── Exact bounds ─────

    [Theory]
    [InlineData(DataPageVersion.V1, false)]
    [InlineData(DataPageVersion.V1, true)]
    [InlineData(DataPageVersion.V2, false)]
    [InlineData(DataPageVersion.V2, true)]
    public async Task SortedColumn_PageBoundsAreItsFirstAndLastValues(DataPageVersion version, bool descending)
    {
        const int rows = 20_000;
        var builder = new Int64Array.Builder();
        for (int i = 0; i < rows; i++)
            builder.Append(descending ? rows - i : i * 3L);
        string path = await WriteAsync(Batch("x", builder.Build(), nullable: false),
            Indexed with { DataPageVersion = version, DictionaryEnabled = false });

        var chunk = Chunks(path).Single();
        var locations = chunk.OffsetIndex!.PageLocations;
        var index = chunk.ColumnIndex!;
        Assert.True(locations.Count > 10);

        for (int p = 0; p < locations.Count; p++)
        {
            long first = locations[p].FirstRowIndex;
            long last = (p + 1 < locations.Count ? locations[p + 1].FirstRowIndex : rows) - 1;
            long firstValue = descending ? rows - first : first * 3;
            long lastValue = descending ? rows - last : last * 3;

            Assert.Equal(Math.Min(firstValue, lastValue), BinaryPrimitives.ReadInt64LittleEndian(index.MinValues[p]));
            Assert.Equal(Math.Max(firstValue, lastValue), BinaryPrimitives.ReadInt64LittleEndian(index.MaxValues[p]));
        }

        Assert.Equal(descending ? BoundaryOrder.Descending : BoundaryOrder.Ascending, index.BoundaryOrder);
    }

    [Fact]
    public async Task UnorderedColumn_IsUnordered()
    {
        var rng = new Random(5);
        var builder = new Int32Array.Builder();
        for (int i = 0; i < 10_000; i++)
            builder.Append(rng.Next());
        string path = await WriteAsync(Batch("x", builder.Build(), nullable: false), Indexed);

        Assert.Equal(BoundaryOrder.Unordered, Chunks(path).Single().ColumnIndex!.BoundaryOrder);
    }

    [Fact]
    public async Task SinglePageChunk_IsAscending()
    {
        var column = new Int32Array.Builder().Append(9).Append(-4).Append(7).Build();
        string path = await WriteAsync(Batch("x", column, nullable: false), Indexed);

        var chunk = Chunks(path).Single();
        Assert.Single(chunk.OffsetIndex!.PageLocations);
        Assert.Equal(BoundaryOrder.Ascending, chunk.ColumnIndex!.BoundaryOrder);
        Assert.Equal(-4, BinaryPrimitives.ReadInt32LittleEndian(chunk.ColumnIndex.MinValues[0]));
        Assert.Equal(9, BinaryPrimitives.ReadInt32LittleEndian(chunk.ColumnIndex.MaxValues[0]));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task NullPages_HaveEmptyBoundsAndTheirNullCounts(bool dictionary)
    {
        // The first 3000 rows are null, then values; small pages put several pages wholly in the nulls.
        var builder = new Int32Array.Builder();
        for (int i = 0; i < 6000; i++)
        {
            if (i < 3000) builder.AppendNull();
            else builder.Append(i % 50);
        }

        string path = await WriteAsync(Batch("x", builder.Build(), nullable: true),
            Indexed with { DictionaryEnabled = dictionary, DataPageSize = 256 });

        var chunk = Chunks(path).Single();
        var index = chunk.ColumnIndex!;
        var locations = chunk.OffsetIndex!.PageLocations;
        Assert.Contains(true, index.NullPages);
        Assert.Contains(false, index.NullPages);

        for (int p = 0; p < index.NullPages.Count; p++)
        {
            long rows = (p + 1 < locations.Count ? locations[p + 1].FirstRowIndex : 6000) - locations[p].FirstRowIndex;
            if (index.NullPages[p])
            {
                Assert.Empty(index.MinValues[p]);
                Assert.Empty(index.MaxValues[p]);
                Assert.Equal(rows, index.NullCounts![p]);
            }
        }

        Assert.Equal(3000, index.NullCounts!.Sum());
    }

    [Theory]
    [InlineData(DataPageVersion.V1, true)]
    [InlineData(DataPageVersion.V2, false)]
    public async Task Unsigned_And_Decimal_BoundsFollowTheirLogicalOrder(DataPageVersion version, bool dictionary)
    {
        var u32 = new UInt32Array.Builder();
        var dec = new Decimal128Array.Builder(new Decimal128Type(20, 2));
        for (int i = 0; i < 4000; i++)
        {
            u32.Append(i % 2 == 0 ? (uint)(i % 7) : 3_000_000_000u + (uint)(i % 5));
            dec.Append(i % 2 == 0 ? -(i % 11) : i % 13);
        }

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("u32", u32.Build().Data.DataType, false))
            .Field(new Field("dec", new Decimal128Type(20, 2), false))
            .Build();
        string path = await WriteAsync(new RecordBatch(schema, [u32.Build(), dec.Build()], 4000),
            Indexed with { DataPageVersion = version, DictionaryEnabled = dictionary });

        AssertIndexesAgreeWithFile(path, expectColumnIndexOnEveryChunk: true);
        var chunks = Chunks(path);

        // Every page holds both small and huge values, and both negative and positive decimals.
        var u32Index = chunks[0].ColumnIndex!;
        Assert.All(u32Index.MinValues, v => Assert.True(BitConverter.ToUInt32(v, 0) < 7));
        Assert.All(u32Index.MaxValues, v => Assert.True(BitConverter.ToUInt32(v, 0) >= 3_000_000_000u));

        var decIndex = chunks[1].ColumnIndex!;
        Assert.All(decIndex.MinValues, v => Assert.True((sbyte)v[0] < 0 || v.All(b => b == 0)));
    }

    // ───── Floating point ─────

    private static RecordBatch DoublesWithAnAllNaNPage(out int nanRows)
    {
        var builder = new DoubleArray.Builder();
        nanRows = 1000;
        for (int i = 0; i < 4000; i++)
            builder.Append(i is >= 1000 and < 2000 ? double.NaN : i * 0.5);
        return Batch("x", builder.Build(), nullable: false);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task AllNaNPage_UnderTypeDefinedOrder_OmitsTheColumnIndex(bool dictionary)
    {
        string path = await WriteAsync(DoublesWithAnAllNaNPage(out _),
            Indexed with { DictionaryEnabled = dictionary, DataPageSize = 512 });

        var chunk = Chunks(path).Single();
        Assert.Null(chunk.Chunk.ColumnIndexOffset);
        Assert.NotNull(chunk.OffsetIndex);
        Assert.True(chunk.OffsetIndex!.PageLocations.Count > 4);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task AllNaNPage_UnderTotalOrder_IsBoundedByNaN(bool dictionary)
    {
        string path = await WriteAsync(DoublesWithAnAllNaNPage(out int nanRows),
            Indexed with
            {
                DictionaryEnabled = dictionary,
                DataPageSize = 512,
                FloatingPointOrder = FloatingPointColumnOrder.Ieee754TotalOrder,
            });

        var index = Chunks(path).Single().ColumnIndex!;
        Assert.Contains(index.MinValues, v => double.IsNaN(BitConverter.ToDouble(v, 0)));
        Assert.Equal(nanRows, index.NanCounts!.Sum());
        Assert.Equal(BoundaryOrder.Unordered, index.BoundaryOrder);
        AssertIndexesAgreeWithFile(path, expectColumnIndexOnEveryChunk: true);
    }

    [Fact]
    public async Task SomeNaNs_AreCountedAndExcludedFromBounds()
    {
        var builder = new FloatArray.Builder();
        for (int i = 0; i < 3000; i++)
            builder.Append(i % 10 == 0 ? float.NaN : i);
        string path = await WriteAsync(Batch("x", builder.Build(), nullable: false),
            Indexed with { DictionaryEnabled = false });

        var index = Chunks(path).Single().ColumnIndex!;
        Assert.Equal(300, index.NanCounts!.Sum());
        Assert.All(index.MinValues.Concat(index.MaxValues), v => Assert.False(float.IsNaN(BitConverter.ToSingle(v, 0))));
        Assert.Equal(BoundaryOrder.Ascending, index.BoundaryOrder);
    }

    // ───── Scope ─────

    [Fact]
    public async Task ColumnWithoutStatistics_GetsAnOffsetIndexOnly()
    {
        string path = await WriteAsync(MixedBatch(3000, nullable: true),
            Indexed with { ColumnWriteStatistics = new Dictionary<string, bool> { ["i64"] = false } });

        foreach (var chunk in Chunks(path))
        {
            Assert.NotNull(chunk.OffsetIndex);
            bool i64 = chunk.Chunk.MetaData!.PathInSchema![0] == "i64";
            Assert.Equal(!i64, chunk.ColumnIndex is not null);
        }
    }

#if NET8_0_OR_GREATER // Half and HalfFloatArray do not exist on .NET Framework.
    [Fact]
    public async Task Float16_GetsAnOffsetIndexOnly()
    {
        var builder = new HalfFloatArray.Builder();
        for (int i = 0; i < 100; i++)
            builder.Append((Half)(i - 50));
        string path = await WriteAsync(Batch("h", builder.Build(), nullable: false), Indexed);

        var chunk = Chunks(path).Single();
        Assert.NotNull(chunk.OffsetIndex);
        Assert.Null(chunk.ColumnIndex);
    }
#endif

    [Fact]
    public async Task RepeatedLeaves_RowsCountRecordsNotValues()
    {
        var list = new ListArray.Builder(Int64Type.Default);
        var values = (Int64Array.Builder)list.ValueBuilder;
        for (int r = 0; r < 500; r++)
        {
            list.Append();
            for (int j = 0; j < 1 + r % 40; j++)
                values.Append(r);
        }

        var array = list.Build();
        string path = await WriteAsync(Batch("l", array, nullable: true), Indexed with { DictionaryEnabled = false });

        var chunk = Chunks(path).Single();
        var locations = chunk.OffsetIndex!.PageLocations;
        Assert.True(locations.Count > 5);

        // Every value in record r is r, so a page's minimum is the record it starts with.
        for (int p = 0; p < locations.Count; p++)
            Assert.Equal(locations[p].FirstRowIndex, BinaryPrimitives.ReadInt64LittleEndian(chunk.ColumnIndex!.MinValues[p]));
        AssertIndexesAgreeWithFile(path, expectColumnIndexOnEveryChunk: true);
    }

    [Fact]
    public async Task Fsst_SymbolTablePageIsNotInTheOffsetIndex()
    {
#pragma warning disable EWPARQUET0003
        var options = Indexed with { ByteArrayEncoding = ByteArrayEncoding.Fsst, DictionaryEnabled = false };
#pragma warning restore EWPARQUET0003
        string path = await WriteAsync(MixedBatch(3000, nullable: false), options);

        var chunk = Chunks(path).Single(c => c.Chunk.MetaData!.PathInSchema![0] == "str_hi");
        Assert.NotNull(chunk.Chunk.MetaData!.SymbolTablePageOffset);
        Assert.Equal(chunk.Chunk.MetaData.DataPageOffset, chunk.OffsetIndex!.PageLocations[0].Offset);
        AssertIndexesAgreeWithFile(path, expectColumnIndexOnEveryChunk: true);
    }

    [Fact]
    public async Task RunEndEncodedColumn_BoundsComeFromTheRuns()
    {
        var runEnds = new Int32Array.Builder();
        var runValues = new Int64Array.Builder();
        for (int run = 0; run < 400; run++)
        {
            runEnds.Append((run + 1) * 25);
            runValues.Append(run % 2 == 0 ? run : -run);
        }

        var ree = new RunEndEncodedArray(runEnds.Build(), runValues.Build());
        string path = await WriteAsync(Batch("r", ree, nullable: false), Indexed with { DataPageSize = 64 });

        Assert.NotNull(Chunks(path).Single().Chunk.MetaData!.DictionaryPageOffset);
        AssertIndexesAgreeWithFile(path, expectColumnIndexOnEveryChunk: true);
    }

    // ───── Truncation ─────

    [Fact]
    public async Task LongStrings_AreTruncatedToTheLimit()
    {
        var builder = new StringArray.Builder();
        for (int i = 0; i < 2000; i++)
            builder.Append(new string((char)('a' + i % 26), 100) + i);
        string path = await WriteAsync(Batch("s", builder.Build(), nullable: false),
            Indexed with { DictionaryEnabled = false, PageIndexTruncateLength = 10 });

        var index = Chunks(path).Single().ColumnIndex!;
        Assert.All(index.MinValues.Concat(index.MaxValues), v => Assert.True(v.Length <= 10, $"{v.Length} bytes"));
        AssertIndexesAgreeWithFile(path, expectColumnIndexOnEveryChunk: true, truncated: true);
    }

    [Fact]
    public async Task TruncationOff_KeepsFullBounds()
    {
        var builder = new StringArray.Builder();
        for (int i = 0; i < 500; i++)
            builder.Append(new string('q', 300) + i.ToString("D4"));
        string path = await WriteAsync(Batch("s", builder.Build(), nullable: false),
            Indexed with { DictionaryEnabled = false, PageIndexTruncateLength = null });

        var index = Chunks(path).Single().ColumnIndex!;
        Assert.All(index.MinValues.Concat(index.MaxValues), v => Assert.Equal(304, v.Length));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void TruncateLength_MustBePositive(int length)
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => ParquetWriteOptions.Default with { PageIndexTruncateLength = length });
    }

    [Fact]
    public async Task DecimalsAreNeverTruncated()
    {
        var builder = new Decimal256Array.Builder(new Decimal256Type(70, 0));
        for (int i = 0; i < 300; i++)
            builder.Append(i - 150);
        string path = await WriteAsync(Batch("d", builder.Build(), nullable: false),
            Indexed with { PageIndexTruncateLength = 4 });

        Assert.All(Chunks(path).Single().ColumnIndex!.MinValues, v => Assert.Equal(32, v.Length));
    }

    // ───── Layout ─────

    [Fact]
    public async Task Indexes_FollowTheLastRowGroup_ColumnIndexesFirst()
    {
        string path = await WriteAsync(MixedBatch(6000, nullable: true), Indexed with { RowGroupMaxRows = 2000 });
        var (bytes, metadata) = ReadFile(path);
        var chunks = metadata.RowGroups.SelectMany(rg => rg.Columns).ToList();

        long lastChunkEnd = chunks.Max(c => c.MetaData!.DataPageOffset + c.MetaData.TotalCompressedSize);
        long footerStart = bytes.Length - 8 - BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(bytes.Length - 8));

        var columnIndexes = chunks.Where(c => c.ColumnIndexOffset is not null).ToList();
        var offsetIndexes = chunks.Select(c => (c.OffsetIndexOffset!.Value, c.OffsetIndexLength!.Value)).ToList();

        // Contiguous, in row-group/column order: every ColumnIndex, then every OffsetIndex, then the footer.
        long position = lastChunkEnd;
        foreach (var c in columnIndexes)
        {
            Assert.Equal(position, c.ColumnIndexOffset);
            position += c.ColumnIndexLength!.Value;
        }

        foreach (var (offset, length) in offsetIndexes)
        {
            Assert.Equal(position, offset);
            position += length;
        }

        Assert.Equal(footerStart, position);
    }

    // ───── The check ─────

    private sealed record ChunkIndex(
        int RowGroup, long RowGroupRows, ColumnChunk Chunk, OffsetIndex? OffsetIndex, ColumnIndex? ColumnIndex)
    {
        public void Deconstruct(out int rowGroup, out long rows, out ColumnChunk chunk, out OffsetIndex? oi, out ColumnIndex? ci)
            => (rowGroup, rows, chunk, oi, ci) = (RowGroup, RowGroupRows, Chunk, OffsetIndex, ColumnIndex);
    }

    private static List<ChunkIndex> Chunks(string path)
    {
        var (bytes, metadata) = ReadFile(path);
        var result = new List<ChunkIndex>();
        for (int g = 0; g < metadata.RowGroups.Count; g++)
        {
            foreach (var chunk in metadata.RowGroups[g].Columns)
            {
                var oi = chunk.OffsetIndexOffset is { } o
                    ? MetadataDecoder.DecodeOffsetIndex(bytes.AsSpan(checked((int)o), chunk.OffsetIndexLength!.Value))
                    : null;
                var ci = chunk.ColumnIndexOffset is { } c
                    ? MetadataDecoder.DecodeColumnIndex(bytes.AsSpan(checked((int)c), chunk.ColumnIndexLength!.Value))
                    : null;
                result.Add(new ChunkIndex(g, metadata.RowGroups[g].NumRows, chunk, oi, ci));
            }
        }

        return result;
    }

    /// <summary>
    /// Checks every chunk's indexes against the pages and the chunk statistics, and returns how many
    /// chunks it checked.
    /// </summary>
    private static int AssertIndexesAgreeWithFile(string path, bool expectColumnIndexOnEveryChunk, bool truncated = false)
    {
        var (bytes, metadata) = ReadFile(path);
        var schema = new EngineeredWood.Parquet.Schema.SchemaDescriptor(metadata.Schema);
        int count = 0;

        foreach (var (_, rows, chunk, offsetIndex, columnIndex) in Chunks(path))
        {
            var meta = chunk.MetaData!;
            string name = string.Join(".", meta.PathInSchema!);
            Assert.True(offsetIndex is not null, $"{name}: no OffsetIndex");
            if (expectColumnIndexOnEveryChunk)
                Assert.True(columnIndex is not null, $"{name}: no ColumnIndex");

            // OffsetIndex: every data page, in order, at its header, with its full size and first row.
            var pages = WalkDataPages(bytes, meta);
            var locations = offsetIndex!.PageLocations;
            Assert.Equal(pages.Count, locations.Count);
            Assert.Equal(0, locations[0].FirstRowIndex);
            for (int p = 0; p < pages.Count; p++)
            {
                Assert.Equal(pages[p].Offset, locations[p].Offset);
                Assert.Equal(pages[p].Size, locations[p].CompressedPageSize);
                long next = p + 1 < pages.Count ? locations[p + 1].FirstRowIndex : rows;
                Assert.True(next > locations[p].FirstRowIndex, $"{name}: page {p} starts no row");
                if (pages[p].NumRows is { } pageRows)
                    Assert.Equal(pageRows, next - locations[p].FirstRowIndex);
            }

            count++;
            if (columnIndex is null)
                continue;

            // ColumnIndex: one entry per page, and consistent with the chunk statistics.
            Assert.Equal(locations.Count, columnIndex.NullPages.Count);
            Assert.Equal(locations.Count, columnIndex.MinValues.Count);
            Assert.Equal(locations.Count, columnIndex.MaxValues.Count);

            var stats = meta.Statistics!;
            Assert.Equal(stats.NullCount, columnIndex.NullCounts!.Sum());
            if (meta.Type is PhysicalType.Float or PhysicalType.Double)
                Assert.Equal(stats.NanCount, columnIndex.NanCounts!.Sum());
            else
                Assert.Null(columnIndex.NanCounts);

            for (int p = 0; p < locations.Count; p++)
            {
                if (columnIndex.NullPages[p])
                {
                    Assert.Empty(columnIndex.MinValues[p]);
                    Assert.Empty(columnIndex.MaxValues[p]);
                }
            }

            // A page bounded by NaN (IEEE 754 total order, all-NaN page) has no part in the chunk bounds,
            // which exclude NaN.
            var nonNull = Enumerable.Range(0, locations.Count)
                .Where(p => !columnIndex.NullPages[p] && !IsNaN(columnIndex.MinValues[p], meta.Type))
                .ToList();
            if (nonNull.Count == 0 || truncated)
                continue;

            var order = OrderOf(schema, meta);
            var min = nonNull.Select(p => columnIndex.MinValues[p]).Aggregate((a, b) => StatisticsCollector.CompareValues(a, b, meta.Type, order) <= 0 ? a : b);
            var max = nonNull.Select(p => columnIndex.MaxValues[p]).Aggregate((a, b) => StatisticsCollector.CompareValues(a, b, meta.Type, order) >= 0 ? a : b);

            // Chunk bounds are shortened to 64 bytes; everything here is shorter, so they compare exactly.
            Assert.Equal(stats.MinValue, min);
            Assert.Equal(stats.MaxValue, max);
            foreach (int p in nonNull)
                Assert.True(StatisticsCollector.CompareValues(columnIndex.MinValues[p], columnIndex.MaxValues[p], meta.Type, order) <= 0,
                    $"{name}: page {p} min above max");
        }

        return count;
    }

    private static bool IsNaN(byte[] bound, PhysicalType type) => type switch
    {
        PhysicalType.Float => float.IsNaN(BitConverter.ToSingle(bound, 0)),
        PhysicalType.Double => double.IsNaN(BitConverter.ToDouble(bound, 0)),
        _ => false,
    };

    private static StatisticsOrder OrderOf(EngineeredWood.Parquet.Schema.SchemaDescriptor schema, ColumnMetaData meta)
    {
        var leaf = schema.Columns.Single(c => c.Path.SequenceEqual(meta.PathInSchema!)).SchemaElement;
        return leaf.LogicalType switch
        {
            LogicalType.IntType { IsSigned: false } => StatisticsOrder.Unsigned,
            LogicalType.DecimalType when meta.Type == PhysicalType.FixedLenByteArray => StatisticsOrder.SignedBigEndian,
            _ => StatisticsOrder.Default,
        };
    }

    private sealed record PageSpan(long Offset, int Size, int? NumRows);

    private static List<PageSpan> WalkDataPages(byte[] file, ColumnMetaData meta)
    {
        long start = new[] { meta.DictionaryPageOffset, meta.SymbolTablePageOffset, meta.DataPageOffset }
            .Where(o => o is not null).Min()!.Value;
        long end = start + meta.TotalCompressedSize;
        var pages = new List<PageSpan>();
        for (long position = start; position < end;)
        {
            var header = PageHeaderDecoder.Decode(file.AsSpan(checked((int)position)), out int headerLength);
            int size = headerLength + header.CompressedPageSize;
            if (header.Type is PageType.DataPage or PageType.DataPageV2)
                pages.Add(new PageSpan(position, size, header.DataPageHeaderV2?.NumRows));
            position += size;
        }

        return pages;
    }

    // ───── Data ─────

    private static (byte[] Bytes, FileMetaData Metadata) ReadFile(string path)
    {
        byte[] bytes = File.ReadAllBytes(path);
        int footerLength = BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(bytes.Length - 8, 4));
        return (bytes, MetadataDecoder.DecodeFileMetaData(bytes.AsSpan(bytes.Length - 8 - footerLength, footerLength)));
    }

    private async Task<string> WriteAsync(RecordBatch batch, ParquetWriteOptions options)
    {
        string path = Path.Combine(_tempDir, Guid.NewGuid().ToString("N")[..8] + ".parquet");
        await using var file = new LocalSequentialFile(path);
        await using var writer = new ParquetFileWriter(file, ownsFile: false, options);
        await writer.WriteRowGroupAsync(batch);
        await writer.CloseAsync();
        return path;
    }

    private static RecordBatch Batch(string name, IArrowArray column, bool nullable) =>
        new(new Apache.Arrow.Schema.Builder().Field(new Field(name, column.Data.DataType, nullable)).Build(),
            [column], column.Length);

    private static RecordBatch Slice(RecordBatch batch, int offset, int length) =>
        new(batch.Schema,
            batch.Arrays.Select(a => EngineeredWood.Arrow.ArrowCompute.Take(a, Enumerable.Range(offset, length).ToArray())).ToArray(),
            length);

    /// <summary>
    /// Every column type whose bounds follow a different rule, and — with <paramref name="nested"/> —
    /// a list and a struct. Values are ordered in stretches so that some pages are ordered and some
    /// are not, and nulls fall in runs so that some pages are entirely null.
    /// </summary>
    private static RecordBatch MixedBatch(int rows, bool nullable, bool nested = false)
    {
        var rng = new Random(390);
        var i32 = new Int32Array.Builder();
        var i64 = new Int64Array.Builder();
        var u32 = new UInt32Array.Builder();
        var dbl = new DoubleArray.Builder();
        var flt = new FloatArray.Builder();
        var strLo = new StringArray.Builder();
        var strHi = new StringArray.Builder();
        var flag = new BooleanArray.Builder();
        var date = new Date32Array.Builder();
        var ts = new TimestampArray.Builder(new TimestampType(Apache.Arrow.Types.TimeUnit.Microsecond, "UTC"));
        var list = new ListArray.Builder(Int32Type.Default);
        var listValues = (Int32Array.Builder)list.ValueBuilder;
        var structA = new Int64Array.Builder();

        var epoch = new DateTimeOffset(2021, 6, 1, 0, 0, 0, TimeSpan.Zero);
        for (int i = 0; i < rows; i++)
        {
            bool isNull = nullable && (i / 300) % 5 == 1;
            if (isNull)
            {
                i32.AppendNull(); i64.AppendNull(); u32.AppendNull(); dbl.AppendNull(); flt.AppendNull();
                strLo.AppendNull(); strHi.AppendNull(); flag.AppendNull(); date.AppendNull(); ts.AppendNull();
                list.AppendNull();
            }
            else
            {
                i32.Append(rng.Next(-100, 100));
                i64.Append(i < rows / 2 ? i : -i);
                u32.Append(i % 3 == 0 ? 4_000_000_000u - (uint)i : (uint)(i % 17));
                dbl.Append(i % 4 == 0 ? -0.0 : Math.Round(rng.NextDouble() * 100 - 50, 3));
                flt.Append(i % 9 == 0 ? 0.0f : (float)(i % 29) - 14f);
                strLo.Append("k" + (i % 13));
                strHi.Append($"value-{rng.Next():x8}-{i:D6}");
                flag.Append(i % 7 < 3);
                date.Append(epoch.AddDays(i % 900).Date);
                ts.Append(epoch.AddSeconds(i * 61L));
                list.Append();
                for (int j = 0; j < i % 5; j++)
                    listValues.Append(i * 10 + j);
            }

            structA.Append(rows - i);
        }

        var fields = new List<Field>
        {
            new("i32", Int32Type.Default, nullable),
            new("i64", Int64Type.Default, nullable),
            new("u32", UInt32Type.Default, nullable),
            new("dbl", DoubleType.Default, nullable),
            new("flt", FloatType.Default, nullable),
            new("str_lo", StringType.Default, nullable),
            new("str_hi", StringType.Default, nullable),
            new("flag", BooleanType.Default, nullable),
            new("date", Date32Type.Default, nullable),
            new("ts", new TimestampType(Apache.Arrow.Types.TimeUnit.Microsecond, "UTC"), nullable),
        };
        var arrays = new List<IArrowArray>
        {
            i32.Build(), i64.Build(), u32.Build(), dbl.Build(), flt.Build(),
            strLo.Build(), strHi.Build(), flag.Build(), date.Build(), ts.Build(),
        };

        if (nested)
        {
            var listArray = list.Build();
            var structType = new StructType([new Field("a", Int64Type.Default, false)]);
            fields.Add(new("list", listArray.Data.DataType, true));
            fields.Add(new("struct", structType, true));
            arrays.Add(listArray);
            arrays.Add(new StructArray(structType, rows, [structA.Build()], ArrowBuffer.Empty, 0));
        }

        return new RecordBatch(new Apache.Arrow.Schema(fields, null), arrays, rows);
    }
}
