// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// #469: a Delta table has one decimal type, <c>Decimal128</c> (<see cref="DeltaTable.ArrowSchema"/>), but reads
/// used the Parquet reader's default <see cref="DecimalOutputKind"/>, the narrowest type that fits. So a column of
/// precision 18 or less came back as <c>Decimal32</c>/<c>Decimal64</c>, whoever wrote the file: EW's own
/// FIXED_LEN_BYTE_ARRAY(16) files as well as the INT32/INT64-backed ones Spark writes. Every path that reads a data
/// file must hand back the declared type.
/// </summary>
public class DecimalReadTypeTests : IDisposable
{
    private readonly string _tempDir;

    public DecimalReadTypeTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_decread_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            Directory.Delete(_tempDir, recursive: true);
        }
    }

    private LocalTableFileSystem Fs => new(_tempDir);

    private static readonly Decimal128Type D9 = new(9, 2);
    private static readonly Decimal128Type D18 = new(18, 4);

    private static Apache.Arrow.Schema TableSchema { get; } = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("d9", D9, true))
        .Field(new Field("d18", D18, true))
        .Build();

    private static RecordBatch Rows(params (long Id, decimal? D9, decimal? D18)[] rows)
    {
        var ids = new Int64Array.Builder();
        var d9 = new Decimal128Array.Builder(D9);
        var d18 = new Decimal128Array.Builder(D18);
        foreach (var (id, a, b) in rows)
        {
            ids.Append(id);
            if (a is null)
            {
                d9.AppendNull();
            }
            else
            {
                d9.Append(a.Value);
            }
            if (b is null)
            {
                d18.AppendNull();
            }
            else
            {
                d18.Append(b.Value);
            }
        }
        return new RecordBatch(TableSchema, [ids.Build(), d9.Build(), d18.Build()], rows.Length);
    }

    // The INT32/INT64-backed form Spark writes by default for precision <= 18.
    private static RecordBatch NarrowRows(params (long Id, int D9Unscaled, long D18Unscaled)[] rows)
    {
        var d32 = new Decimal32Type(9, 2);
        var d64 = new Decimal64Type(18, 4);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("d9", d32, true))
            .Field(new Field("d18", d64, true))
            .Build();
        var ids = new Int64Array.Builder();
        foreach (var r in rows)
        {
            ids.Append(r.Id);
        }
        var d9 = new Decimal32Array(new ArrayData(d32, rows.Length, 0, 0,
            [ArrowBuffer.Empty, new ArrowBuffer(rows.SelectMany(r => BitConverter.GetBytes(r.D9Unscaled)).ToArray())]));
        var d18 = new Decimal64Array(new ArrayData(d64, rows.Length, 0, 0,
            [ArrowBuffer.Empty, new ArrowBuffer(rows.SelectMany(r => BitConverter.GetBytes(r.D18Unscaled)).ToArray())]));
        return new RecordBatch(schema, [ids.Build(), d9, d18], rows.Length);
    }

    /// <summary>Writes the batches it is handed as they are, as a foreign writer would.</summary>
#pragma warning disable EWDELTA0001 // codec seam is experimental
    private sealed class VerbatimWriter(string root) : IDataFileWriter
#pragma warning restore EWDELTA0001
    {
        public async ValueTask<long> WriteAsync(
            IAsyncEnumerable<RecordBatch> batches, string relativePath, CancellationToken cancellationToken)
        {
            string path = Path.Combine(root, relativePath);
            Directory.CreateDirectory(Path.GetDirectoryName(path)!);
            await using (var file = new LocalSequentialFile(path))
            {
                await using var writer = new ParquetFileWriter(file, ownsFile: false);
                await foreach (var b in batches.WithCancellation(cancellationToken))
                {
                    await writer.WriteRowGroupAsync(b, cancellationToken);
                }
                await writer.CloseAsync(cancellationToken);
            }
            return new FileInfo(path).Length;
        }
    }

    /// <summary>A table whose data files hold INT32/INT64-backed decimals, then reopened with no host writer.</summary>
    private async Task<DeltaTable> CreateWithNarrowFilesAsync(
        IReadOnlyDictionary<string, string>? configuration, params RecordBatch[] batches)
    {
#pragma warning disable EWDELTA0001
        var options = DeltaTableOptions.Default with { DataFileWriter = new VerbatimWriter(_tempDir) };
#pragma warning restore EWDELTA0001
        await using (var host = await DeltaTable.CreateAsync(Fs, TableSchema, options, configuration: configuration))
        {
            foreach (var b in batches)
            {
                await host.WriteAsync([b]);
            }
        }

        // The files really are narrow: what the reader hands back unasked.
        string first = Directory.EnumerateFiles(_tempDir, "*.parquet").First();
        await using (var file = new LocalRandomAccessFile(first))
        await using (var reader = new ParquetFileReader(file, ownsFile: false))
        {
            var raw = await reader.ReadRowGroupAsync(0);
            Assert.IsType<Decimal32Type>(raw.Schema.FieldsList[1].DataType);
            Assert.IsType<Decimal64Type>(raw.Schema.FieldsList[2].DataType);
        }

        return await DeltaTable.OpenAsync(Fs);
    }

    private static void AssertDeclaredTypes(RecordBatch batch)
    {
        foreach (var (name, declared) in new[] { ("d9", D9), ("d18", D18) })
        {
            int index = batch.Schema.GetFieldIndex(name);
            var type = Assert.IsType<Decimal128Type>(batch.Schema.FieldsList[index].DataType);
            Assert.Equal((declared.Precision, declared.Scale), (type.Precision, type.Scale));
            Assert.IsType<Decimal128Array>(batch.Column(index));
        }
    }

    private static List<(long Id, decimal? D9, decimal? D18)> Values(IEnumerable<RecordBatch> batches)
    {
        var result = new List<(long, decimal?, decimal?)>();
        foreach (var b in batches)
        {
            var ids = (Int64Array)b.Column(b.Schema.GetFieldIndex("id"));
            var d9 = (Decimal128Array)b.Column(b.Schema.GetFieldIndex("d9"));
            var d18 = (Decimal128Array)b.Column(b.Schema.GetFieldIndex("d18"));
            for (int i = 0; i < b.Length; i++)
            {
                result.Add((ids.GetValue(i)!.Value, d9.GetValue(i), d18.GetValue(i)));
            }
        }
        result.Sort((x, y) => x.Item1.CompareTo(y.Item1));
        return result;
    }

    private static async Task<List<RecordBatch>> ReadAllAsync(DeltaTable table)
    {
        var batches = new List<RecordBatch>();
        await foreach (var b in table.ReadAllAsync())
        {
            batches.Add(b);
        }
        return batches;
    }

    private static Func<RecordBatch, BooleanArray> Where(Func<long, bool> predicate) => batch =>
    {
        var ids = (Int64Array)batch.Column(batch.Schema.GetFieldIndex("id"));
        var b = new BooleanArray.Builder();
        for (int i = 0; i < batch.Length; i++)
        {
            b.Append(predicate(ids.GetValue(i)!.Value));
        }
        return b.Build();
    };

    // The issue's first probe: EW's own FIXED_LEN_BYTE_ARRAY(16) files.
    [Fact]
    public async Task Read_EwWrittenFile_ReturnsTheDeclaredDecimal128()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, TableSchema);
        await table.WriteAsync([Rows((1, 1.25m, -3.5m), (2, null, 99_999_999_999_999.9999m))]);

        var batches = await ReadAllAsync(table);

        Assert.All(batches, AssertDeclaredTypes);
        Assert.Equal([(1, 1.25m, -3.5m), (2, null, 99_999_999_999_999.9999m)], Values(batches));
    }

    // The issue's second probe: INT32/INT64-backed files, as Spark writes them.
    [Fact]
    public async Task Read_NarrowBackedFile_ReturnsTheDeclaredDecimal128()
    {
        await using var table = await CreateWithNarrowFilesAsync(null, NarrowRows((1, -125, 50_000), (2, 99_999_999, -1)));

        var batches = await ReadAllAsync(table);

        Assert.All(batches, AssertDeclaredTypes);
        Assert.Equal([(1, -1.25m, 5m), (2, 999_999.99m, -0.0001m)], Values(batches));
        Assert.All(batches, b => Assert.Equal(
            table.ArrowSchema.FieldsList.Select(f => f.DataType.TypeId),
            b.Schema.FieldsList.Select(f => f.DataType.TypeId)));
    }

    // The table's schema is what it is: a caller's read options cannot make a read contradict it.
    [Fact]
    public async Task Read_WithCallerReadOptions_StillReturnsDecimal128()
    {
        await using (var host = await CreateWithNarrowFilesAsync(null, NarrowRows((1, 7, 8)))) { }
        var options = DeltaTableOptions.Default with
        {
            ParquetReadOptions = ParquetReadOptions.Default with { DecimalOutput = DecimalOutputKind.Default },
        };
        await using var table = await DeltaTable.OpenAsync(Fs, options);

        var batches = await ReadAllAsync(table);

        Assert.All(batches, AssertDeclaredTypes);
    }

    [Fact]
    public async Task ReadChanges_NarrowBackedFile_ReturnsDecimal128()
    {
        await using var table = await CreateWithNarrowFilesAsync(
            new Dictionary<string, string> { ["delta.enableChangeDataFeed"] = "true" },
            NarrowRows((1, 100, 200)));

        var batches = new List<RecordBatch>();
        await foreach (var b in table.ReadChangesAsync(new DeltaChangeReadOptions { StartVersion = 1, EndVersion = 1 }))
        {
            batches.Add(b);
        }

        Assert.NotEmpty(batches);
        Assert.All(batches, AssertDeclaredTypes);
        Assert.Equal([(1, 1m, 0.02m)], Values(batches));
    }

    // Rewrites read and re-write: they must keep every value, and leave the table readable as declared.
    [Fact]
    public async Task Compact_NarrowBackedFiles_KeepsValues()
    {
        await using var table = await CreateWithNarrowFilesAsync(null,
            NarrowRows((1, -125, 1)), NarrowRows((2, 99_999_999, long.MaxValue / 10)));

        Assert.NotNull(await table.CompactAsync());
        var batches = await ReadAllAsync(table);

        Assert.All(batches, AssertDeclaredTypes);
        Assert.Equal([(1, -1.25m, 0.0001m), (2, 999_999.99m, (long.MaxValue / 10) / 10_000m)], Values(batches));
    }

    [Fact]
    public async Task Update_NarrowBackedFile_KeepsTheUntouchedValues()
    {
        await using var table = await CreateWithNarrowFilesAsync(null, NarrowRows((1, 100, 1), (2, 200, 2)));

        await table.UpdateAsync(Where(id => id == 2), batch =>
        {
            int index = batch.Schema.GetFieldIndex("d9");
            var d9 = new Decimal128Array.Builder(D9);
            for (int i = 0; i < batch.Length; i++)
            {
                d9.Append(42m);
            }
            var columns = Enumerable.Range(0, batch.ColumnCount)
                .Select(c => c == index ? d9.Build() : batch.Column(c)).ToList();
            return new RecordBatch(batch.Schema, columns, batch.Length);
        });
        var batches = await ReadAllAsync(table);

        Assert.All(batches, AssertDeclaredTypes);
        Assert.Equal([(1, 1m, 0.0001m), (2, 42m, 0.0002m)], Values(batches));
    }
}
