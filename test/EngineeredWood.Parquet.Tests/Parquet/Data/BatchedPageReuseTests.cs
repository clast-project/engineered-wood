// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers;
using System.Buffers.Binary;
using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using EngineeredWood.Compression;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Metadata;
using TimeUnit = Apache.Arrow.Types.TimeUnit;

namespace EngineeredWood.Tests.Parquet.Data;

/// <summary>
/// The flat batched read decodes each page once, and keeps the rows a batch did not use for the
/// next (#408). Its batches must still be what a whole read returns, and each must own its arrays,
/// so that a caller can dispose one and go on reading.
/// </summary>
public class BatchedPageReuseTests : IDisposable
{
    private const int Rows = 20_000;

    private readonly string _tempDir;

    public BatchedPageReuseTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-page-reuse-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    public static TheoryData<DataPageVersion, bool, ByteArrayOutputKind, DecimalOutputKind, bool> Layouts()
    {
        var data = new TheoryData<DataPageVersion, bool, ByteArrayOutputKind, DecimalOutputKind, bool>();
        foreach (var version in new[] { DataPageVersion.V1, DataPageVersion.V2 })
        foreach (bool dictionary in new[] { false, true })
        {
            data.Add(version, dictionary, ByteArrayOutputKind.Default, DecimalOutputKind.Default, false);
            data.Add(version, dictionary, ByteArrayOutputKind.ViewType, DecimalOutputKind.Decimal128, true);
            data.Add(version, dictionary, ByteArrayOutputKind.LargeOffsets, DecimalOutputKind.Default, true);
        }

        return data;
    }

    /// <summary>
    /// Every leaf type EW reads, batched at sizes that cut pages, equals a whole read. Each batch is
    /// disposed as soon as it is read, before the next is asked for, so a batch that shared a buffer
    /// with another would fail here.
    /// </summary>
    [Theory]
    [MemberData(nameof(Layouts))]
    public async Task BatchedRead_OfEveryLeafType_MatchesAWholeRead(
        DataPageVersion version, bool dictionary, ByteArrayOutputKind byteArrays, DecimalOutputKind decimals, bool guids)
    {
        string path = await WriteAllTypesAsync(version, dictionary);
        var registry = guids ? new ExtensionTypeRegistry() : null;
        registry?.Register(GuidExtensionDefinition.Instance);
        ParquetReadOptions Options(int? batchSize, long? maxBytes) => new()
        {
            ByteArrayOutput = byteArrays,
            DecimalOutput = decimals,
            ExtensionRegistry = registry,
            BatchSize = batchSize,
            MaxBatchByteSize = maxBytes,
        };

        var (wholeTypes, whole) = await ReadCellsAsync(path, Options(null, null));
        Assert.Equal(1, whole.Batches);

        foreach (var (batchSize, maxBytes) in new (int?, long?)[] { (7, null), (333, null), (4096, null), (null, 16_384) })
        {
            var (types, batched) = await ReadCellsAsync(path, Options(batchSize, maxBytes));
            Assert.True(batched.Batches > 1, $"BatchSize {batchSize}, MaxBatchByteSize {maxBytes}: one batch");
            Assert.Equal(wholeTypes, types);
            for (int c = 0; c < whole.Columns.Length; c++)
            {
                var expected = whole.Columns[c];
                var actual = batched.Columns[c];
                Assert.Equal(expected.Count, actual.Count);
                for (int r = 0; r < expected.Count; r++)
                {
                    if (expected[r] != actual[r])
                        Assert.Fail($"BatchSize {batchSize}, MaxBatchByteSize {maxBytes}: column {types[c]} #{c}, row {r}: expected {expected[r]}, got {actual[r]}");
                }
            }
        }
    }

    /// <summary>
    /// No data page is fetched more than twice: once when a batch first reaches it, and at most
    /// once more when a batch that starts in it holds more rows than decoding it again repeats, so
    /// is decoded afresh from it rather than copied. Before #408 a page was fetched again for
    /// every batch it overlapped, which read the data pages 3-8 times over at 64Ki-row batches.
    /// </summary>
    [Theory]
    [InlineData(false, 333)]
    [InlineData(true, 333)]
    [InlineData(false, 7)]
    [InlineData(true, 7)]
    public async Task BatchedRead_FetchesNoDataPageMoreThanTwice(bool dictionary, int batchSize)
    {
        string path = await WriteAllTypesAsync(DataPageVersion.V2, dictionary);
        var metadata = ReadFooter(path);
        var dataPages = metadata.RowGroups[0].Columns
            .Select(c => (Start: c.MetaData!.DataPageOffset, End: ChunkStart(c.MetaData) + c.MetaData.TotalCompressedSize))
            .ToList();

        await using var input = new CountingFile(new LocalRandomAccessFile(path));
        using (var reader = new ParquetFileReader(input, ownsFile: false, new ParquetReadOptions { BatchSize = batchSize }))
        {
            int batches = 0;
            await foreach (var batch in reader.ReadRowGroupBatchesAsync(0))
            {
                batches++;
                batch.Dispose();
            }

            Assert.True(batches > 1);
        }

        // How many times each byte of the file was fetched.
        var fetches = new int[new FileInfo(path).Length];
        foreach (var range in input.Read)
        {
            for (long b = range.Offset; b < range.Offset + range.Length; b++)
                fetches[b]++;
        }

        foreach (var (start, end) in dataPages)
        {
            for (long b = start; b < end; b++)
            {
                if (fetches[b] is < 1 or > 2)
                    Assert.Fail($"data byte {b} fetched {fetches[b]} times");
            }
        }
    }

    private async Task<string> WriteAllTypesAsync(DataPageVersion version, bool dictionary)
    {
        var fields = new List<Field>();
        var arrays = new List<IArrowArray>();
        void Add(string name, IArrowArray array)
        {
            fields.Add(new Field(name, array.Data.DataType, nullable: true));
            arrays.Add(array);
        }

        // A null every 7 rows, and values that repeat every 300 so a dictionary is worth it.
        bool IsNull(int r) => r % 7 == 3;
        int V(int r) => r % 300;

        Add("bool", Build(new BooleanArray.Builder(), (b, r) => b.Append(V(r) % 3 == 0), (b, _) => b.AppendNull()));
        Add("i8", Build(new Int8Array.Builder(), (b, r) => b.Append((sbyte)(V(r) - 150)), (b, _) => b.AppendNull()));
        Add("u8", Build(new UInt8Array.Builder(), (b, r) => b.Append((byte)V(r)), (b, _) => b.AppendNull()));
        Add("i16", Build(new Int16Array.Builder(), (b, r) => b.Append((short)(V(r) * 97 - 9000)), (b, _) => b.AppendNull()));
        Add("u16", Build(new UInt16Array.Builder(), (b, r) => b.Append((ushort)(V(r) * 211)), (b, _) => b.AppendNull()));
        Add("i32", Build(new Int32Array.Builder(), (b, r) => b.Append(V(r) * 7919 - 1_000_000), (b, _) => b.AppendNull()));
        Add("u32", Build(new UInt32Array.Builder(), (b, r) => b.Append((uint)V(r) * 14_000_000u), (b, _) => b.AppendNull()));
        Add("i64", Build(new Int64Array.Builder(), (b, r) => b.Append(V(r) * 1_000_000_007L - 5), (b, _) => b.AppendNull()));
        Add("u64", Build(new UInt64Array.Builder(), (b, r) => b.Append((ulong)V(r) * 61_000_000_000_000_000UL), (b, _) => b.AppendNull()));
#if NET5_0_OR_GREATER
        Add("f16", Build(new HalfFloatArray.Builder(), (b, r) => b.Append((Half)(V(r) * 0.5f)), (b, _) => b.AppendNull()));
#endif
        Add("f32", Build(new FloatArray.Builder(), (b, r) => b.Append(V(r) * 0.25f), (b, _) => b.AppendNull()));
        Add("f64", Build(new DoubleArray.Builder(), (b, r) => b.Append(V(r) * 1.0 / 3), (b, _) => b.AppendNull()));
        Add("str", Build(new StringArray.Builder(), (b, r) => b.Append("value-" + V(r) + new string('x', V(r) % 23)), (b, _) => b.AppendNull()));
        Add("bin", Build(new BinaryArray.Builder(), (b, r) => b.Append(BitConverter.GetBytes(V(r) * 31L).AsSpan()), (b, _) => b.AppendNull()));
        Add("date", Build(new Date32Array.Builder(), (b, r) => b.Append(new DateTime(2000, 1, 1).AddDays(V(r))), (b, _) => b.AppendNull()));
        Add("t32", Build(new Time32Array.Builder(TimeUnit.Millisecond), (b, r) => b.Append(V(r) * 1000), (b, _) => b.AppendNull()));
        Add("t64", Build(new Time64Array.Builder(TimeUnit.Microsecond), (b, r) => b.Append(V(r) * 1_000_000L), (b, _) => b.AppendNull()));
        Add("ts", Build(new TimestampArray.Builder(new TimestampType(TimeUnit.Microsecond, "UTC")), (b, r) => b.Append(new DateTimeOffset(2020, 1, 1, 0, 0, 0, TimeSpan.Zero).AddSeconds(V(r))), (b, _) => b.AppendNull()));
        Add("d9", Build(new Decimal128Array.Builder(new Decimal128Type(9, 2)), (b, r) => b.Append(V(r) * 1.25m - 100m), (b, _) => b.AppendNull()));
        Add("d18", Build(new Decimal128Array.Builder(new Decimal128Type(18, 4)), (b, r) => b.Append(V(r) * 12345.6789m), (b, _) => b.AppendNull()));
        Add("d30", Build(new Decimal128Array.Builder(new Decimal128Type(30, 5)), (b, r) => b.Append(V(r) * 98765432.10987m), (b, _) => b.AppendNull()));
        Add("d50", Build(new Decimal256Array.Builder(new Decimal256Type(50, 5)), (b, r) => b.Append(V(r) * 98765432.10987m), (b, _) => b.AppendNull()));

        var guid = new GuidArray.Builder();
        for (int r = 0; r < Rows; r++)
        {
            if (IsNull(r))
                guid.AppendNull();
            else
                guid.Append(new Guid(V(r), 1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        }

        Add("guid", guid.Build(allocator: null));

        TArray Build<TBuilder, TArray>(IArrowArrayBuilder<TArray, TBuilder> builder, Action<TBuilder, int> append, Action<TBuilder, int> appendNull)
            where TBuilder : IArrowArrayBuilder<TArray>
            where TArray : IArrowArray
        {
            for (int r = 0; r < Rows; r++)
            {
                if (IsNull(r))
                    appendNull((TBuilder)builder, r);
                else
                    append((TBuilder)builder, r);
            }

            return builder.Build(allocator: null);
        }

        var batch = new RecordBatch(new Apache.Arrow.Schema(fields, null), arrays, Rows);
        string path = Path.Combine(_tempDir, Guid.NewGuid().ToString("N")[..8] + ".parquet");
        await using var file = new LocalSequentialFile(path);
        await using var writer = new ParquetFileWriter(file, ownsFile: false, ParquetWriteOptions.Default with
        {
            DataPageVersion = version,
            DictionaryEnabled = dictionary,
            Compression = CompressionCodec.Snappy,
            DataPageSize = 2048,
        });
        await writer.WriteRowGroupAsync(batch);
        await writer.CloseAsync();
        return path;
    }

    /// <summary>
    /// Reads every batch, turning each cell into a string, and disposes each batch before asking
    /// for the next.
    /// </summary>
    private static async Task<(List<string> Types, (int Batches, List<string>[] Columns) Cells)> ReadCellsAsync(
        string path, ParquetReadOptions options)
    {
        await using var input = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(input, ownsFile: false, options);
        List<string>? types = null;
        List<string>[]? columns = null;
        int batches = 0;
        await foreach (var batch in reader.ReadRowGroupBatchesAsync(0))
        {
            using (batch)
            {
                batches++;
                types ??= Enumerable.Range(0, batch.ColumnCount).Select(c => batch.Column(c).Data.DataType.Name).ToList();
                columns ??= Enumerable.Range(0, batch.ColumnCount).Select(_ => new List<string>(Rows)).ToArray();
                for (int c = 0; c < batch.ColumnCount; c++)
                {
                    var array = batch.Column(c);
                    for (int r = 0; r < array.Length; r++)
                        columns[c].Add(Cell(array, r));
                }
            }
        }

        return (types!, (batches, columns!));
    }

    private static string Cell(IArrowArray array, int i)
    {
        if (array.IsNull(i))
            return "<null>";

        switch (array)
        {
            case ExtensionArray extension:
                return Cell(extension.Storage, i);
            case BooleanArray b:
                return b.GetValue(i)!.Value ? "true" : "false";
            case BinaryArray b:
                return Convert.ToBase64String(b.GetBytes(i).ToArray());
            case LargeBinaryArray b:
                return Convert.ToBase64String(b.GetBytes(i).ToArray());
            case BinaryViewArray b:
                return Convert.ToBase64String(b.GetBytes(i).ToArray());
        }

        if (array.Data.DataType is FixedWidthType fixedWidth && fixedWidth.BitWidth % 8 == 0)
        {
            int width = fixedWidth.BitWidth / 8;
            return Convert.ToBase64String(
                array.Data.Buffers[1].Span.Slice((array.Data.Offset + i) * width, width).ToArray());
        }

        throw new NotSupportedException($"No cell comparison for {array.Data.DataType.Name}.");
    }

    private static long ChunkStart(ColumnMetaData meta) =>
        meta.DictionaryPageOffset is > 0 and long dpo ? dpo
        : meta.SymbolTablePageOffset is > 0 and long stpo ? stpo
        : meta.DataPageOffset;

    private static FileMetaData ReadFooter(string path)
    {
        byte[] bytes = File.ReadAllBytes(path);
        int length = BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(bytes.Length - 8, 4));
        return MetadataDecoder.DecodeFileMetaData(bytes.AsSpan(bytes.Length - 8 - length, length));
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
