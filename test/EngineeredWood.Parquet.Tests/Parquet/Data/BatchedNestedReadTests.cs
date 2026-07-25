// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.Tests.Parquet.Data;

/// <summary>
/// Covers reading nested (list / struct / map) columns under <see cref="ParquetReadOptions.BatchSize"/>
/// and <see cref="ParquetReadOptions.MaxBatchByteSize"/>. The contract: streaming a row group in
/// bounded batches must reconstruct exactly what a single unbounded read produces — row for row,
/// element for element — regardless of where the batch boundaries fall relative to page boundaries.
/// </summary>
public class BatchedNestedReadTests : IDisposable
{
    private readonly string _tempDir;

    public BatchedNestedReadTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-batched-nested-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private string TempPath(string name) => Path.Combine(_tempDir, name);

    private async Task<string> WriteAsync(string name, RecordBatch batch, ParquetWriteOptions? options = null)
    {
        string path = TempPath(name);
        await using var file = new LocalSequentialFile(path);
        await using var writer = new ParquetFileWriter(file, ownsFile: false, options ?? ParquetWriteOptions.Default);
        await writer.WriteRowGroupAsync(batch);
        await writer.CloseAsync();
        return path;
    }

    private static async Task<List<RecordBatch>> ReadBatchesAsync(string path, ParquetReadOptions options)
    {
        await using var file = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(file, ownsFile: false, options);
        var batches = new List<RecordBatch>();
        await foreach (var b in reader.ReadAllAsync())
            batches.Add(b);
        return batches;
    }

    // ---- Column builders ----

    /// <summary>Variable-length int lists, including nulls and empty lists.</summary>
    private static IArrowArray BuildRaggedListColumn(int rows, out ListType type)
    {
        type = new ListType(new Field("element", Int32Type.Default, nullable: true));
        var values = new Int32Array.Builder();
        var offsets = new int[rows + 1];
        var validity = new byte[(rows + 7) / 8];
        int nullCount = 0;
        int offset = 0;

        for (int i = 0; i < rows; i++)
        {
            offsets[i] = offset;
            if (i % 13 == 4) { nullCount++; continue; } // null list

            validity[i >> 3] |= (byte)(1 << (i & 7));
            int len = i % 5; // 0..4, includes empty lists
            for (int j = 0; j < len; j++, offset++)
            {
                if (j == 2) values.AppendNull();
                else values.Append(i * 100 + j);
            }
        }
        offsets[rows] = offset;

        var data = new ArrayData(type, rows, nullCount, 0,
            [new ArrowBuffer(validity), new ArrowBuffer(MemoryMarshal.AsBytes(offsets.AsSpan()).ToArray())],
            [values.Build().Data]);
        return new ListArray(data);
    }

    /// <summary>Fixed-length float lists (the fast-path shape).</summary>
    private static IArrowArray BuildFixedListColumn(int rows, int length, out ListType type)
    {
        type = new ListType(new Field("element", FloatType.Default, nullable: false));
        var values = new FloatArray.Builder();
        var offsets = new int[rows + 1];
        for (int i = 0; i < rows; i++)
        {
            offsets[i] = i * length;
            for (int j = 0; j < length; j++) values.Append(i * length + j + 0.5f);
        }
        offsets[rows] = rows * length;

        var data = new ArrayData(type, rows, 0, 0,
            [ArrowBuffer.Empty, new ArrowBuffer(MemoryMarshal.AsBytes(offsets.AsSpan()).ToArray())],
            [values.Build().Data]);
        return new ListArray(data);
    }

    private static StructArray BuildStructColumn(int rows, out StructType type)
    {
        var idField = new Field("id", Int32Type.Default, nullable: false);
        var labelField = new Field("label", StringType.Default, nullable: true);
        type = new StructType([idField, labelField]);

        var ids = new Int32Array.Builder();
        var labels = new StringArray.Builder();
        for (int i = 0; i < rows; i++)
        {
            ids.Append(i);
            if (i % 7 == 0) labels.AppendNull();
            else labels.Append($"row-{i}");
        }

        return new StructArray(type, rows, [ids.Build(), labels.Build()], ArrowBuffer.Empty, nullCount: 0);
    }

    // ---- Equivalence check ----

    /// <summary>
    /// Asserts that the concatenation of <paramref name="batches"/> reproduces <paramref name="expected"/>
    /// exactly, comparing each cell via the string form of <c>GetValue</c>/nested accessors.
    /// </summary>
    private static void AssertConcatenationEquals(RecordBatch expected, List<RecordBatch> batches, int? batchSize = null)
    {
        int total = batches.Sum(b => b.Length);
        Assert.Equal(expected.Length, total);

        if (batchSize is int bs)
        {
            // Every batch but the last must be exactly batchSize rows.
            for (int i = 0; i < batches.Count - 1; i++)
                Assert.Equal(bs, batches[i].Length);
            Assert.True(batches[^1].Length <= bs && batches[^1].Length > 0);
        }

        for (int c = 0; c < expected.ColumnCount; c++)
        {
            var exp = expected.Column(c);
            int row = 0;
            foreach (var batch in batches)
            {
                var act = batch.Column(c);
                Assert.Equal(exp.GetType(), act.GetType());
                for (int i = 0; i < act.Length; i++, row++)
                    Assert.Equal(CellToString(exp, row), CellToString(act, i));
            }
            Assert.Equal(expected.Length, row);
        }
    }

    private static string CellToString(IArrowArray array, int index)
    {
        if (array.IsNull(index)) return "<null>";
        switch (array)
        {
            case Int32Array a: return a.GetValue(index)!.Value.ToString();
            case FloatArray a: return a.GetValue(index)!.Value.ToString("R");
            case StringArray a: return "\"" + a.GetString(index) + "\"";
            case ListArray l:
            {
                var vals = l.GetSlicedValues(index);
                var parts = new List<string>();
                for (int j = 0; j < vals.Length; j++) parts.Add(CellToString(vals, j));
                return "[" + string.Join(",", parts) + "]";
            }
            case StructArray s:
            {
                var parts = new List<string>();
                foreach (var f in s.Fields) parts.Add(CellToString(f, index));
                return "{" + string.Join(",", parts) + "}";
            }
            default:
                throw new NotSupportedException(array.GetType().Name);
        }
    }

    // ---- Tests ----

    public static TheoryData<int> BatchSizes() => new() { 1, 7, 64, 333, 1000 };

    [Theory]
    [MemberData(nameof(BatchSizes))]
    public async Task RaggedListColumn_BatchedEqualsSingle(int batchSize)
    {
        const int rows = 2000;
        var col = BuildRaggedListColumn(rows, out var listType);
        var schema = new Apache.Arrow.Schema.Builder().Field(new Field("vec", listType, nullable: true)).Build();
        var batch = new RecordBatch(schema, [col], rows);

        // Small pages so batch boundaries fall mid-page and mid-record.
        string path = await WriteAsync("ragged.parquet", batch,
            ParquetWriteOptions.Default with { DataPageSize = 4 * 1024 });

        var expected = await ReadSingleAsync(path);
        var batched = await ReadBatchesAsync(path, new ParquetReadOptions { BatchSize = batchSize });
        AssertConcatenationEquals(expected, batched, batchSize);
    }

    [Theory]
    [MemberData(nameof(BatchSizes))]
    public async Task FixedListColumn_BatchedEqualsSingle_WithFastPath(int batchSize)
    {
        const int rows = 3000, length = 4;
        var col = BuildFixedListColumn(rows, length, out var listType);
        var schema = new Apache.Arrow.Schema.Builder().Field(new Field("vec", listType, nullable: false)).Build();
        var batch = new RecordBatch(schema, [col], rows);

        string path = await WriteAsync("fixed.parquet", batch,
            ParquetWriteOptions.Default with { DataPageSize = 8 * 1024 });

        var expected = await ReadSingleAsync(path);
        var batched = await ReadBatchesAsync(path,
            new ParquetReadOptions { BatchSize = batchSize, FixedListFastPath = true });
        AssertConcatenationEquals(expected, batched, batchSize);
    }

    [Theory]
    [MemberData(nameof(BatchSizes))]
    public async Task StructColumn_BatchedEqualsSingle(int batchSize)
    {
        const int rows = 2500;
        var col = BuildStructColumn(rows, out var structType);
        var schema = new Apache.Arrow.Schema.Builder().Field(new Field("s", structType, nullable: false)).Build();
        var batch = new RecordBatch(schema, [col], rows);

        string path = await WriteAsync("struct.parquet", batch,
            ParquetWriteOptions.Default with { DataPageSize = 4 * 1024 });

        var expected = await ReadSingleAsync(path);
        var batched = await ReadBatchesAsync(path, new ParquetReadOptions { BatchSize = batchSize });
        AssertConcatenationEquals(expected, batched, batchSize);
    }

    [Theory]
    [MemberData(nameof(BatchSizes))]
    public async Task MixedFlatAndNestedColumns_BatchedEqualsSingle(int batchSize)
    {
        const int rows = 2200;
        var ids = new Int32Array.Builder();
        for (int i = 0; i < rows; i++) ids.Append(i * 3);
        var listCol = BuildRaggedListColumn(rows, out var listType);
        var structCol = BuildStructColumn(rows, out var structType);

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int32Type.Default, nullable: false))
            .Field(new Field("vec", listType, nullable: true))
            .Field(new Field("s", structType, nullable: false))
            .Build();
        var batch = new RecordBatch(schema, [ids.Build(), listCol, structCol], rows);

        string path = await WriteAsync("mixed.parquet", batch,
            ParquetWriteOptions.Default with { DataPageSize = 4 * 1024 });

        var expected = await ReadSingleAsync(path);
        var batched = await ReadBatchesAsync(path, new ParquetReadOptions { BatchSize = batchSize });
        AssertConcatenationEquals(expected, batched, batchSize);
    }

    [Fact]
    public async Task NestedColumn_RespectsMaxBatchByteSize()
    {
        const int rows = 4000;
        var col = BuildRaggedListColumn(rows, out var listType);
        var schema = new Apache.Arrow.Schema.Builder().Field(new Field("vec", listType, nullable: true)).Build();
        var batch = new RecordBatch(schema, [col], rows);

        string path = await WriteAsync("bytes.parquet", batch,
            ParquetWriteOptions.Default with { DataPageSize = 4 * 1024 });

        var expected = await ReadSingleAsync(path);
        // Budget well below the column's total uncompressed size so the row group cannot fit in one
        // batch and the byte-budget sizing is actually exercised on the nested path.
        var batched = await ReadBatchesAsync(path, new ParquetReadOptions { MaxBatchByteSize = 2 * 1024 });

        Assert.True(batched.Count > 1, "byte budget should force multiple batches");
        AssertConcatenationEquals(expected, batched);
    }

    private async Task<RecordBatch> ReadSingleAsync(string path)
    {
        await using var file = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(file, ownsFile: false);
        return await reader.ReadRowGroupAsync(0);
    }
}
