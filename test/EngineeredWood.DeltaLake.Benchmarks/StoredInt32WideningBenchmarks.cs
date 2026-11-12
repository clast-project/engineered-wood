// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using BenchmarkDotNet.Attributes;
using EngineeredWood.DeltaLake.Table.TypeWidening;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Metadata;

namespace EngineeredWood.DeltaLake.Benchmarks;

/// <summary>
/// Reading a <c>byte</c>/<c>short</c> column that a Delta table has widened (#839), the two ways the read can
/// go: <b>narrow</b>, the reader's INT(8)/INT(16) arrays widened afterwards by <see cref="ValueWidener"/> (what
/// every read did before); <b>stored</b>, the column read as the INT32 it stores
/// (<see cref="ParquetFileReader.ReadStoredInt32"/>), widened once more only for a <c>long</c> target.
///
/// <para>One column, one row group, Snappy (the writer default), a local file the OS has cached. One row in ten
/// is null. Each invocation opens the file, so the footer is in both legs.</para>
/// </summary>
[MemoryDiagnoser]
public class StoredInt32WideningBenchmarks
{
    [Params(1_048_576)]
    public int Rows { get; set; }

    private string _dir = null!;
    private string _bytes = null!;
    private string _shorts = null!;

    [GlobalSetup]
    public async Task Setup()
    {
        _dir = Path.Combine(Path.GetTempPath(), "ew-839-bench-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dir);
        var rng = new Random(839);
        var b = new Int8Array.Builder();
        var s = new Int16Array.Builder();
        for (int i = 0; i < Rows; i++)
        {
            if (i % 10 == 3)
            {
                b.AppendNull();
                s.AppendNull();
                continue;
            }
            b.Append((sbyte)rng.Next(sbyte.MinValue, sbyte.MaxValue + 1));
            s.Append((short)rng.Next(short.MinValue, short.MaxValue + 1));
        }
        _bytes = await WriteAsync("b", b.Build());
        _shorts = await WriteAsync("s", s.Build());
    }

    [GlobalCleanup]
    public void Cleanup() => Directory.Delete(_dir, recursive: true);

    private async Task<string> WriteAsync(string name, IArrowArray column)
    {
        string path = Path.Combine(_dir, name + ".parquet");
        var schema = new Apache.Arrow.Schema([new Field(name, column.Data.DataType, true)], null);
        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false))
        {
            await writer.WriteRowGroupAsync(new RecordBatch(schema, [column], column.Length));
            await writer.CloseAsync();
        }
        return path;
    }

    private static async Task<long> ReadAsync(string path, IArrowType target, bool stored)
    {
        using var reader = new ParquetFileReader(new LocalRandomAccessFile(path));
        if (stored)
        {
            var schema = await reader.GetSchemaAsync().ConfigureAwait(false);
            reader.ReadStoredInt32(new HashSet<SchemaElement>(schema.Columns.Select(c => c.SchemaElement)));
        }
        long rows = 0;
        await foreach (var batch in reader.ReadAllAsync().ConfigureAwait(false))
        {
            var widened = ValueWidener.WidenArray(batch.Column(0), target);
            if (widened.Data.DataType.TypeId != target.TypeId)
                throw new InvalidOperationException($"read {widened.Data.DataType}, expected {target}");
            rows += widened.Length;
            if (!ReferenceEquals(widened, batch.Column(0)))
                widened.Dispose();
            batch.Dispose();
        }
        return rows;
    }

    [Benchmark(Baseline = true, Description = "byte->int, narrow then widen")]
    public Task<long> ByteToIntNarrow() => ReadAsync(_bytes, Int32Type.Default, stored: false);

    [Benchmark(Description = "byte->int, stored INT32")]
    public Task<long> ByteToIntStored() => ReadAsync(_bytes, Int32Type.Default, stored: true);

    [Benchmark(Description = "short->int, narrow then widen")]
    public Task<long> ShortToIntNarrow() => ReadAsync(_shorts, Int32Type.Default, stored: false);

    [Benchmark(Description = "short->int, stored INT32")]
    public Task<long> ShortToIntStored() => ReadAsync(_shorts, Int32Type.Default, stored: true);

    [Benchmark(Description = "short->long, narrow then widen")]
    public Task<long> ShortToLongNarrow() => ReadAsync(_shorts, Int64Type.Default, stored: false);

    [Benchmark(Description = "short->long, stored INT32 then widen")]
    public Task<long> ShortToLongStored() => ReadAsync(_shorts, Int64Type.Default, stored: true);
}
