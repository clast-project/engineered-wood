// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.ChangeDataFeed;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.DeltaLake.Table.Compaction;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// #851: a parallel read reserves before each decode, and the scan settles the batch it hands on. Bringing a data
/// file's batch to the table's shape adds columns the Parquet read never decodes: partition values tiled per row,
/// the NULLs of a column the file predates, widened types. Reserving the raw batch alone left every unit's first
/// item short by them, and a scan's unit is a row group, so unbatched that was every item.
/// </summary>
public class HostAddedBytesTests : IDisposable
{
    private const int Files = 8;
    private const int Rows = 40_000;

    /// <summary>Longer than a view's 12 inlined bytes, so a view column keeps it in a data buffer.</summary>
    private const string Region = "region-of-the-world-number-07-of-the-many-regions-there-are";

    private readonly string _tempDir;
    private readonly LocalTableFileSystem _fs;

    public HostAddedBytesTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_host_bytes_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
        _fs = new LocalTableFileSystem(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private static readonly Apache.Arrow.Schema PartitionedSchema = new Apache.Arrow.Schema.Builder()
        .Field(new Field("v", Int64Type.Default, false))
        .Field(new Field("region", StringType.Default, true))
        .Field(new Field("bucket", Int32Type.Default, true))
        .Field(new Field("tag", StringType.Default, true))
        .Build();

    /// <summary>Data files of one row group whose only data column is a run-length int64: what they decode to is a
    /// small part of what a scan hands on. Partitioned by a long string, an int and an always-NULL string, and with
    /// four columns added after the data, each kind of what reshaping adds is there.</summary>
    private async Task CreateTableAsync(int files, ColumnMappingMode mapping, bool changeFeed = false, bool evolve = true)
    {
        var configuration = new Dictionary<string, string>();
        if (changeFeed)
            configuration["delta.enableChangeDataFeed"] = "true";
        await using var table = await DeltaTable.CreateAsync(
            _fs, PartitionedSchema, new DeltaTableOptions { ParquetWriteOptions = new ParquetWriteOptions { RowGroupMaxRows = Rows } },
            partitionColumns: ["region", "bucket", "tag"], columnMappingMode: mapping, configuration: configuration);
        for (int f = 0; f < files; f++)
        {
            var v = new Int64Array.Builder();
            var region = new StringArray.Builder();
            var bucket = new Int32Array.Builder();
            var tag = new StringArray.Builder();
            for (int r = 0; r < Rows; r++)
            {
                v.Append(r % 3);
                region.Append(Region);
                bucket.Append(f);
                tag.AppendNull();
            }
            await table.WriteAsync([new RecordBatch(PartitionedSchema, [v.Build(), region.Build(), bucket.Build(), tag.Build()], Rows)]);
        }
        if (!evolve)
            return;
        await table.AddColumnAsync(new Field("x_id", Int64Type.Default, true));
        await table.AddColumnAsync(new Field("x_note", StringType.Default, true));
        await table.AddColumnAsync(new Field("x_flag", BooleanType.Default, true));
        await table.AddColumnAsync(new Field("x_point", new Apache.Arrow.Types.StructType(
        [
            new Field("a", Int32Type.Default, true),
            new Field("b", StringType.Default, true),
        ]), true));
    }

    /// <summary>
    /// The estimate is what reshaping adds: the bytes a scan's batch of a file carries over the raw batch the Parquet
    /// read decodes from it, in each representation of byte arrays and under each column mapping mode.
    /// </summary>
    [Theory]
    [InlineData(ByteArrayOutputKind.Default, ColumnMappingMode.None)]
    [InlineData(ByteArrayOutputKind.ViewType, ColumnMappingMode.None)]
    [InlineData(ByteArrayOutputKind.LargeOffsets, ColumnMappingMode.None)]
    [InlineData(ByteArrayOutputKind.Default, ColumnMappingMode.Name)]
    [InlineData(ByteArrayOutputKind.ViewType, ColumnMappingMode.Id)]
    public async Task TheEstimate_IsWhatReshapingAdds(ByteArrayOutputKind kind, ColumnMappingMode mapping)
    {
        await CreateTableAsync(files: 1, mapping);
        var readOptions = new ParquetReadOptions { ByteArrayOutput = kind };
        await using var table = await DeltaTable.OpenAsync(_fs, new DeltaTableOptions { ParquetReadOptions = readOptions });
        var snapshot = table.CurrentSnapshot;
        var add = Assert.Single(snapshot.ActiveFiles.Values);

        await using var file = new LocalRandomAccessFile(Path.Combine(_tempDir, DeltaPath.Decode(add.Path)));
        using var reader = new ParquetFileReader(file, ownsFile: false, readOptions);
        long raw = 0;
        await foreach (var batch in reader.ReadAllAsync())
        {
            using (batch)
                raw += RollingDataFileWriter.EstimatedBytes(batch);
        }

        long shaped = 0;
        int rows = 0;
        await foreach (var batch in table.ReadAsync())
        {
            using (batch)
            {
                shaped += RollingDataFileWriter.EstimatedBytes(batch);
                rows += batch.Length;
            }
        }
        Assert.Equal(Rows, rows);

        var partitionColumns = snapshot.Metadata.PartitionColumns;
        long perRow = DeltaTable.ReconcileAddedBytesPerRow(
            snapshot, null, add.PartitionValues, partitionColumns, partitionColumns.Count > 0, mapping,
            ColumnMapping.BuildLogicalToPhysicalMap(snapshot.Schema, mapping), await reader.GetArrowSchemaAsync(), kind);

        long added = shaped - raw;
        Assert.True(added > 50L * Rows, $"reshaping added {added} bytes: nothing to estimate");
        // Never short, beyond each buffer's padding; over by no more than the rounding of a row's bits to bytes.
        Assert.True(perRow * Rows >= added - 4096 && perRow * Rows <= added + Rows, $"estimated {perRow} B/row = {perRow * Rows}, reshaping added {added} ({added / (double)Rows:F3} B/row)");
    }

    /// <summary>
    /// #852 review: a partition column is sized from the value the reshaping tiles down it, resolved as it resolves
    /// it (<see cref="Partitioning.PartitionUtils.AddPartitionColumns"/>, the oracle here).
    /// <list type="bullet">
    /// <item>Hive's sentinel is NULL for an int column (validity and values), but text in a string column.</item>
    /// <item>A logical-keyed map after chained renames (logical <c>x</c> physical <c>p1</c>, logical <c>p1</c>
    /// physical <c>p2</c>): a physical-first lookup sized <c>x</c> from <c>p1</c>'s one-byte value.</item>
    /// </list>
    /// </summary>
    [Theory]
    [InlineData("sentinel")]
    [InlineData("chained renames")]
    [InlineData("empty and missing")]
    public async Task TheEstimate_SizesAPartitionValueAsReshapingResolvesIt(string scenario)
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("v", Int64Type.Default, false))
            .Field(new Field("x", StringType.Default, true))
            .Field(new Field("p1", StringType.Default, true))
            .Field(new Field("n", Int32Type.Default, true))
            .Build();
        await using var table = await DeltaTable.CreateAsync(_fs, schema, partitionColumns: ["x", "p1", "n"]);
        var snapshot = table.CurrentSnapshot;
        var partitionColumns = snapshot.Metadata.PartitionColumns;
        string longValue = new('z', 100);
        var (values, logicalToPhysical) = scenario switch
        {
            "sentinel" => (
                new Dictionary<string, string> { ["x"] = "__HIVE_DEFAULT_PARTITION__", ["p1"] = "B", ["n"] = "__HIVE_DEFAULT_PARTITION__" },
                new Dictionary<string, string>()),
            "chained renames" => (
                new Dictionary<string, string> { ["x"] = longValue, ["p1"] = "B", ["n"] = "7" },
                new Dictionary<string, string> { ["x"] = "p1", ["p1"] = "p2", ["n"] = "n" }),
            _ => (
                new Dictionary<string, string> { ["x"] = "", ["n"] = "" },
                new Dictionary<string, string>()),
        };

        var data = new Apache.Arrow.Schema.Builder().Field(new Field("v", Int64Type.Default, false)).Build();
        var v = new Int64Array.Builder();
        for (int r = 0; r < Rows; r++)
            v.Append(r);
        using var raw = new RecordBatch(data, [v.Build()], Rows);
        using var shaped = Partitioning.PartitionUtils.AddPartitionColumns(
            raw, snapshot.ArrowSchema, values, partitionColumns, logicalToPhysical);
        long added = RollingDataFileWriter.EstimatedBytes(shaped) - RollingDataFileWriter.EstimatedBytes(raw);

        long perRow = DeltaTable.ReconcileAddedBytesPerRow(
            snapshot, null, values, partitionColumns, true, ColumnMappingMode.Name, logicalToPhysical, data,
            byteArrays: null);
        Assert.True(perRow * Rows >= added - 4096 && perRow * Rows <= added + Rows,
            $"estimated {perRow} B/row = {perRow * Rows}, the partition columns hold {added} ({added / (double)Rows:F3} B/row)");
    }

    /// <summary>
    /// A widened column adds the wider type's width over the one the file holds: an int32 read as int64 four bytes a
    /// row. A short widened to int adds nothing, because the read already decodes it as the INT32 it stores (#839).
    /// </summary>
    [Fact]
    public async Task TheEstimate_CountsWidening()
    {
        var narrow = new Apache.Arrow.Schema.Builder()
            .Field(new Field("s", Int16Type.Default, true))
            .Field(new Field("i", Int32Type.Default, true))
            .Build();
        await using (var create = await DeltaTable.CreateAsync(
                         _fs, narrow, new DeltaTableOptions { ParquetWriteOptions = new ParquetWriteOptions { RowGroupMaxRows = Rows } },
                         configuration: new Dictionary<string, string> { [EngineeredWood.DeltaLake.Schema.TypeWidening.EnableKey] = "true" }))
        {
            var sb = new Int16Array.Builder();
            var ib = new Int32Array.Builder();
            for (int r = 0; r < Rows; r++)
            {
                sb.Append((short)(r % 7));
                ib.Append(r);
            }
            await create.WriteAsync([new RecordBatch(narrow, [sb.Build(), ib.Build()], Rows)]);
            var wide = new Apache.Arrow.Schema.Builder()
                .Field(new Field("s", Int32Type.Default, true))
                .Field(new Field("i", Int64Type.Default, true))
                .Build();
            await create.WriteAsync(
                [new RecordBatch(wide, [new Int32Array.Builder().Append(1).Build(), new Int64Array.Builder().Append(1).Build()], 1)],
                mergeSchema: true);
        }

        await using var table = await DeltaTable.OpenAsync(_fs);
        var snapshot = table.CurrentSnapshot;
        var add = snapshot.ActiveFiles.Values.Single(a => a.Size == snapshot.ActiveFiles.Values.Max(f => f.Size));

        // The reader as the scan opens it: widened shorts read as their stored INT32.
        await using var file = new LocalRandomAccessFile(Path.Combine(_tempDir, DeltaPath.Decode(add.Path)));
        using var reader = new ParquetFileReader(file, ownsFile: false);
        await EngineeredWood.DeltaLake.Table.TypeWidening.StoredInt32Columns.ApplyAsync(reader, snapshot, ColumnMappingMode.None, default);
        long raw = 0;
        await foreach (var batch in reader.ReadAllAsync())
        {
            using (batch)
                raw += RollingDataFileWriter.EstimatedBytes(batch);
        }
        long shaped = 0;
        await foreach (var batch in table.ReadAsync())
        {
            using (batch)
            {
                if (batch.Length == Rows)
                    shaped += RollingDataFileWriter.EstimatedBytes(batch);
            }
        }

        long perRow = DeltaTable.ReconcileAddedBytesPerRow(
            snapshot, null, add.PartitionValues, [], false, ColumnMappingMode.None,
            new Dictionary<string, string>(), await reader.GetArrowSchemaAsync(), byteArrays: null);
        long added = shaped - raw;
        Assert.True(added >= 3L * Rows, $"reshaping added {added} bytes: the int is not widened");
        Assert.True(perRow * Rows >= added - 4096 && perRow * Rows <= added + Rows,
            $"estimated {perRow} B/row = {perRow * Rows}, reshaping added {added} ({added / (double)Rows:F3} B/row)");
    }

    /// <summary>
    /// A scan reads the eight files' row groups at once under a budget of two of the batches it hands on. Reserving
    /// what each decodes to, a ninth of that, let them all in at once (#851).
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AScan_ReservesWhatReshapingAdds(bool batched)
    {
        await CreateTableAsync(Files, ColumnMappingMode.None);
        var readOptions = new ParquetReadOptions { BatchSize = batched ? Rows / 4 : null };
        long largest = 0;
        await using (var serial = await DeltaTable.OpenAsync(_fs, new DeltaTableOptions { ParquetReadOptions = readOptions, MaxConcurrentReads = 1 }))
        {
            await foreach (var b in serial.ReadAsync())
            {
                largest = Math.Max(largest, RollingDataFileWriter.EstimatedBytes(b));
                b.Dispose();
            }
        }

        var shared = new EngineeredWood.Memory.MemoryBudget(long.MaxValue);
        await using var table = await DeltaTable.OpenAsync(_fs, new DeltaTableOptions
        {
            ParquetReadOptions = readOptions,
            MaxConcurrentReads = Files,
            MaxBufferedReadBytes = 2 * largest,
            MemoryBudget = shared,
        });
        int rows = 0;
        await foreach (var b in table.ReadAsync())
        {
            rows += b.Length;
            b.Dispose();
            await Task.Delay(20);
        }

        Assert.Equal(Files * Rows, rows);
        // The budget, plus the one batch the front may run over by.
        Assert.InRange(shared.PeakBytes, 1, 3 * largest);
        Assert.Equal(0, shared.UsedBytes);
    }

    /// <summary>The change feed brings each file to the table's shape too (<see cref="AScan_ReservesWhatReshapingAdds"/>).</summary>
    [Fact]
    public async Task TheChangeFeed_ReservesWhatReshapingAdds()
    {
        await CreateTableAsync(Files, ColumnMappingMode.None, changeFeed: true);
        var changes = new DeltaChangeReadOptions { StartVersion = 1, EndVersion = Files };
        long largest = 0;
        await using (var serial = await DeltaTable.OpenAsync(_fs, new DeltaTableOptions { MaxConcurrentReads = 1 }))
        {
            await foreach (var b in serial.ReadChangesAsync(changes))
            {
                largest = Math.Max(largest, RollingDataFileWriter.EstimatedBytes(b));
                b.Dispose();
            }
        }

        var shared = new EngineeredWood.Memory.MemoryBudget(long.MaxValue);
        await using var table = await DeltaTable.OpenAsync(_fs, new DeltaTableOptions
        {
            MaxConcurrentReads = Files,
            MaxBufferedReadBytes = 2 * largest,
            MemoryBudget = shared,
        });
        int rows = 0;
        await foreach (var b in table.ReadChangesAsync(changes))
        {
            rows += b.Length;
            b.Dispose();
            await Task.Delay(20);
        }

        Assert.Equal(Files * Rows, rows);
        Assert.InRange(shared.PeakBytes, 1, 3 * largest);
        Assert.Equal(0, shared.UsedBytes);
    }
}
