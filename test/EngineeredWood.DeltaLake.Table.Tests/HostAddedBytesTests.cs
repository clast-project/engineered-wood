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

    private static readonly Apache.Arrow.Types.StructType Point = new([new Field("a", Int32Type.Default, true)]);

    private static readonly Apache.Arrow.Schema PartitionedSchema = new Apache.Arrow.Schema.Builder()
        .Field(new Field("v", Int64Type.Default, false))
        .Field(new Field("s", Point, true))
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
            var a = new Int32Array.Builder();
            var region = new StringArray.Builder();
            var bucket = new Int32Array.Builder();
            var tag = new StringArray.Builder();
            for (int r = 0; r < Rows; r++)
            {
                v.Append(r % 3);
                a.Append(r);
                region.Append(Region);
                bucket.Append(f);
                tag.AppendNull();
            }
            var s = new StructArray(Point, Rows, [a.Build()], ArrowBuffer.Empty, 0);
            await table.WriteAsync([new RecordBatch(PartitionedSchema, [v.Build(), s, region.Build(), bucket.Build(), tag.Build()], Rows)]);
        }
        if (!evolve)
            return;
        // Inside a struct the files hold, as well as beside it (#852 review round 2).
        await table.AddFieldAsync(["s"], new Field("b", StringType.Default, true));
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
        var reshape = DeltaTable.ReconcileAddedBytesPerRow(
            snapshot, null, add.PartitionValues, partitionColumns, partitionColumns.Count > 0, mapping,
            ColumnMapping.BuildLogicalToPhysicalMap(snapshot.Schema, mapping), await reader.GetArrowSchemaAsync(), kind);

        // One batch of the whole decode: both parts are per row of it.
        long perRow = reshape.PerRow + reshape.PerDecodedRow;
        long added = shaped - raw;
        Assert.True(added > 50L * Rows, $"reshaping added {added} bytes: nothing to estimate");
        // Never short, beyond each buffer's padding; over by no more than the rounding of each part's bits to bytes.
        Assert.True(perRow * Rows >= added - 4096 && perRow * Rows <= added + 2 * Rows, $"estimated {perRow} B/row = {perRow * Rows}, reshaping added {added} ({added / (double)Rows:F3} B/row)");
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

        var reshape = DeltaTable.ReconcileAddedBytesPerRow(
            snapshot, null, values, partitionColumns, true, ColumnMappingMode.Name, logicalToPhysical, data,
            byteArrays: null);
        long perRow = reshape.PerRow;
        Assert.Equal(0, reshape.PerDecodedRow);
        Assert.True(perRow * Rows >= added - 4096 && perRow * Rows <= added + 2 * Rows,
            $"estimated {perRow} B/row = {perRow * Rows}, the partition columns hold {added} ({added / (double)Rows:F3} B/row)");
    }

    /// <summary>
    /// #852 review round 2: what reshaping adds INSIDE a column the file holds (<see cref="SchemaEvolution"/> recurses
    /// through structs, lists and maps), sized against the reshaping itself on a batch of the file's older shape. A
    /// field added inside a list or map element costs it once per element: here three a row, as the file's leaf
    /// counts say. What it builds there is sized by the column's children, which a batch sliced from a larger decode
    /// shares unsliced: a quarter of a decode costs what the whole decode does, so it is counted per row decoded.
    /// </summary>
    [Theory]
    [InlineData("struct, two levels", false)]
    [InlineData("list element", false)]
    [InlineData("map value", false)]
    [InlineData("widened inside a struct", false)]
    [InlineData("struct, two levels", true)]
    [InlineData("list element", true)]
    [InlineData("map value", true)]
    [InlineData("widened inside a struct", true)]
    public async Task TheEstimate_CountsWhatReshapingAddsInsideAColumn(string scenario, bool sliced)
    {
        const int elements = 3;
        IArrowType Element(bool evolved) => new Apache.Arrow.Types.StructType(evolved
            ? [new Field("a", Int32Type.Default, true), new Field("b", Int64Type.Default, true), new Field("c", StringType.Default, true)]
            : [new Field("a", Int32Type.Default, true)]);
        IArrowType Column(bool evolved) => scenario switch
        {
            "struct, two levels" => new Apache.Arrow.Types.StructType(
            [
                new Field("x", Int32Type.Default, true),
                new Field("inner", Element(evolved), true),
            ]),
            "list element" => new ListType(new Field("element", Element(evolved), true)),
            "map value" => new Apache.Arrow.Types.MapType(
                new Field("key", StringType.Default, false), new Field("value", Element(evolved), true)),
            _ => new Apache.Arrow.Types.StructType(
                [new Field("a", evolved ? Int64Type.Default : Int32Type.Default, true)]),
        };

        var tableSchema = new Apache.Arrow.Schema.Builder().Field(new Field("c", Column(true), true)).Build();
        var configuration = scenario.StartsWith("widened", StringComparison.Ordinal)
            ? new Dictionary<string, string> { [EngineeredWood.DeltaLake.Schema.TypeWidening.EnableKey] = "true" }
            : null;
        await using var table = await DeltaTable.CreateAsync(_fs, tableSchema, configuration: configuration);
        var snapshot = table.CurrentSnapshot;
        if (configuration is not null)
            Assert.True(DeltaTable.ReadsWiden(snapshot), "the table should read widened types");

        var fileSchema = new Apache.Arrow.Schema.Builder().Field(new Field("c", Column(false), true)).Build();
        // Sliced: the second quarter of a decode of four times the rows.
        int decoded = sliced ? 4 * Rows : Rows;
        using var whole = new RecordBatch(fileSchema, [OldShape(Column(false), decoded, elements)], decoded);
        using var raw = sliced ? whole.Slice(Rows, Rows) : whole;
        using var shaped = DeltaTable.ReconcileToTableShape(
            raw, snapshot, null, null, [], false, new Dictionary<string, string>());
        long added = RollingDataFileWriter.EstimatedBytes(shaped) - RollingDataFileWriter.EstimatedBytes(raw);

        var reshape = DeltaTable.ReconcileAddedBytesPerRow(
            snapshot, null, null, [], false, ColumnMappingMode.None, new Dictionary<string, string>(), fileSchema,
            byteArrays: null, new Dictionary<string, double> { ["c"] = scenario is "list element" or "map value" ? elements : 1 });
        long estimate = reshape.PerRow * Rows + reshape.PerDecodedRow * decoded;
        Assert.True(added > Rows, $"reshaping added {added} bytes: nothing to estimate");
        Assert.True(estimate >= added - 4096 && estimate <= added + 2 * decoded,
            $"estimated {reshape} = {estimate} for {Rows} of {decoded} rows decoded, reshaping added {added}");
    }

    /// <summary>A column of <paramref name="type"/> (the file's older shape), <paramref name="elements"/> entries to a
    /// list or map, every leaf set.</summary>
    private static IArrowArray OldShape(IArrowType type, int length, int elements)
    {
        switch (type)
        {
            case Apache.Arrow.Types.MapType map:
            {
                var keys = new StringArray.Builder();
                for (int i = 0; i < length * elements; i++)
                    keys.Append($"k{i % elements}");
                var entries = new StructArray(
                    new Apache.Arrow.Types.StructType([map.KeyField, map.ValueField]), length * elements,
                    [keys.Build(), OldShape(map.ValueField.DataType, length * elements, elements)], ArrowBuffer.Empty, 0);
                return new MapArray(map, length, Offsets(length, elements), entries, ArrowBuffer.Empty, 0);
            }
            case ListType list:
                return new ListArray(list, length, Offsets(length, elements),
                    OldShape(list.ValueDataType, length * elements, elements), ArrowBuffer.Empty, 0);
            case Apache.Arrow.Types.StructType structType:
                return new StructArray(structType, length,
                    structType.Fields.Select(f => OldShape(f.DataType, length, elements)).ToList(), ArrowBuffer.Empty, 0);
            default:
            {
                var values = new Int32Array.Builder();
                for (int i = 0; i < length; i++)
                    values.Append(i);
                return values.Build();
            }
        }
    }

    private static ArrowBuffer Offsets(int length, int elements)
    {
        var offsets = new ArrowBuffer.Builder<int>(length + 1);
        for (int i = 0; i <= length; i++)
            offsets.Append(i * elements);
        return offsets.Build();
    }

    /// <summary>
    /// #852 review round 3: the feed's own columns, as its builders build them (the oracle here), validity included.
    /// A commit with no timestamp gets an all-NULL <c>_commit_timestamp</c>, which carries a validity bitmap; the
    /// emitted row-tracking pair is built by builders whose validity, where a value is NULL, has room to grow. Exact
    /// within rounding where those are NULL, and never short where they are not.
    /// </summary>
    [Theory]
    [InlineData("insert", false, true)]
    [InlineData("insert", true, true)]
    [InlineData(null, true, true)]
    [InlineData("update_preimage", true, false)]
    public void TheFeedsOwnColumns_AreSizedWithTheirValidity(string? changeType, bool emitRowTracking, bool nulls)
    {
        var schema = new Apache.Arrow.Schema.Builder().Field(new Field("v", Int64Type.Default, false)).Build();
        var v = new Int64Array.Builder();
        for (int r = 0; r < Rows; r++)
            v.Append(r);
        using var raw = new RecordBatch(schema, [v.Build()], Rows);

        var batch = changeType is null ? raw : ChangeDataFeed.CdfWriter.AddChangeTypeColumn(raw, changeType);
        batch = ChangeDataFeed.CdfReader.AddMetadataColumns(batch, 7, nulls ? null : 1_794_486_519_997L);
        if (emitRowTracking)
        {
            batch = ChangeDataFeed.CdfReader.ResolveRowTracking(
                batch, null, null, baseRowId: nulls ? null : 100, defaultRowCommitVersion: nulls ? null : 3,
                nulls ? null : Enumerable.Range(0, Rows).Select(i => (long)i).ToList(), "_row_id", "_row_commit_version");
        }
        long added = RollingDataFileWriter.EstimatedBytes(batch) - RollingDataFileWriter.EstimatedBytes(raw);

        long estimate = ChangeDataFeed.CdfReader.FeedColumnsBytesPerRow(changeType, emitRowTracking) * Rows;
        Assert.True(estimate >= added - 4096, $"estimated {estimate}, the feed added {added}");
        if (nulls)
            Assert.True(estimate <= added + Rows, $"estimated {estimate}, the feed added {added}");
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

        var reshape = DeltaTable.ReconcileAddedBytesPerRow(
            snapshot, null, add.PartitionValues, [], false, ColumnMappingMode.None,
            new Dictionary<string, string>(), await reader.GetArrowSchemaAsync(), byteArrays: null);
        long perRow = reshape.PerRow + reshape.PerDecodedRow;
        long added = shaped - raw;
        Assert.True(added >= 3L * Rows, $"reshaping added {added} bytes: the int is not widened");
        Assert.True(perRow * Rows >= added - 4096 && perRow * Rows <= added + 2 * Rows,
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
        // The budget, plus the one batch the front may run over by, within 2% of a batch: what the estimate leaves out
        // is each buffer's padding and the raw decode's own small misses, beside the ninth of a batch it once reserved.
        Assert.InRange(shared.PeakBytes, 1, 3 * largest + largest / 50);
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
        Assert.InRange(shared.PeakBytes, 1, 3 * largest + largest / 50);
        Assert.Equal(0, shared.UsedBytes);
    }
}
