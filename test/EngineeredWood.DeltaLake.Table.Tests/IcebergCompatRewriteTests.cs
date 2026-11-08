// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.RowTracking;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.IO.Local;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// The row-rewriting DML paths on IcebergCompat tables (#756). Each rewritten file is written as an append writes one:
/// it holds the partition columns (IcebergCompat V1/V2/V3 store the partition values in the data file too), and on V3
/// its materialized row-tracking columns carry Iceberg's row-lineage field ids, as delta-spark 4.4 writes UPDATE output.
/// Spark checks neither (none of its IcebergCompat checks reads a data file), so these tests read the files.
/// </summary>
public class IcebergCompatRewriteTests : IDisposable
{
    private readonly string _tempDir;

    public IcebergCompatRewriteTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_iceberg_rewrite_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private LocalTableFileSystem Fs => new(_tempDir);

    private static readonly Apache.Arrow.Schema IdValueKey = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("v", Int64Type.Default, true))
        .Field(new Field("k", Int32Type.Default, true))
        .Build();

    // Rows (id, id * 10, k).
    private static RecordBatch Rows(int? k, params long[] ids)
    {
        var idBuilder = new Int64Array.Builder();
        var vBuilder = new Int64Array.Builder();
        var kBuilder = new Int32Array.Builder();
        foreach (long id in ids)
        {
            idBuilder.Append(id);
            vBuilder.Append(id * 10);
            if (k is int value)
                kBuilder.Append(value);
            else
                kBuilder.AppendNull();
        }
        return new RecordBatch(IdValueKey, [idBuilder.Build(), vBuilder.Build(), kBuilder.Build()], ids.Length);
    }

    private static RecordBatch WithColumn(RecordBatch batch, string name, IArrowArray column)
    {
        var columns = Enumerable.Range(0, batch.ColumnCount).Select(batch.Column).ToArray();
        columns[batch.Schema.GetFieldIndex(name)] = column;
        return new RecordBatch(batch.Schema, columns, batch.Length);
    }

    private static RecordBatch SetV(RecordBatch batch, long value, Func<int, bool>? which = null)
    {
        var old = (Int64Array)batch.Column(batch.Schema.GetFieldIndex("v"));
        var builder = new Int64Array.Builder();
        for (int i = 0; i < batch.Length; i++)
        {
            if (which is null || which(i))
                builder.Append(value);
            else if (old.IsNull(i))
                builder.AppendNull();
            else
                builder.Append(old.GetValue(i)!.Value);
        }
        return WithColumn(batch, "v", builder.Build());
    }

    private static RecordBatch SetK(RecordBatch batch, int value)
    {
        var builder = new Int32Array.Builder();
        for (int i = 0; i < batch.Length; i++)
            builder.Append(value);
        return WithColumn(batch, "k", builder.Build());
    }

    private Task<DeltaTable> CreateAsync(string enableKey, bool enableDeletionVectors = false) =>
        DeltaTable.CreateAsync(Fs, IdValueKey, partitionColumns: ["k"], columnMappingMode: ColumnMappingMode.Name,
            enableDeletionVectors: enableDeletionVectors,
            configuration: new Dictionary<string, string> { [enableKey] = "true" }).AsTask();

    private static string PhysicalK(DeltaTable table) => ColumnMapping.GetPhysicalName(
        table.CurrentSnapshot.Schema.Fields.Single(f => f.Name == "k"), ColumnMappingMode.Name);

    private static readonly (long Id, long V, int? K)[] Initial =
        [(1, 10, 7), (2, 20, 7), (3, 30, 7), (4, 40, 9), (5, 50, 9)];

    public static TheoryData<string, string, bool> Cases()
    {
        var data = new TheoryData<string, string, bool>();
        foreach (string key in new[] { IcebergCompat.EnableV2Key, IcebergCompat.EnableV3Key })
        {
            foreach (string op in new[] { "delete", "delete-rows", "update", "update-move", "update-rows", "merge" })
                data.Add(key, op, false);
        }
        // V3 allows deletion vectors, so UPDATE and MERGE write their post-images to new files beside a vector.
        foreach (string op in new[] { "update", "update-move", "merge" })
            data.Add(IcebergCompat.EnableV3Key, op, true);
        return data;
    }

    /// <summary>
    /// Every file the operation writes holds the partition column, physical-named, with the add's partition value in
    /// every row (an UPDATE that moves a row writes the new value), and the column stays out of the stats. On V3 the
    /// row-tracking columns carry the row-lineage field ids, and every surviving row keeps its stable row id.
    /// </summary>
    [Theory]
    [MemberData(nameof(Cases))]
    public async Task ARewrite_WritesItsFilesAsAnAppendDoes(string enableKey, string op, bool deletionVectors)
    {
        await using var table = await CreateAsync(enableKey, deletionVectors);
        await table.WriteAsync([Rows(7, 1, 2, 3)]);
        await table.WriteAsync([Rows(9, 4, 5)]);
        bool v3 = enableKey == IcebergCompat.EnableV3Key;
        var rowIdsBefore = await RowIdsByIdAsync(table);
        var before = table.CurrentSnapshot.ActiveFiles.Keys.ToHashSet();
        var mode = deletionVectors ? RowDeleteMode.DeletionVector : RowDeleteMode.CopyOnWrite;
        string physicalK = PhysicalK(table);
        // The file of partition k=7, where id 2 is the row at position 1.
        string sevens = table.CurrentSnapshot.ActiveFiles.Values.Single(a => a.PartitionValues[physicalK] == "7").Path;
        var idTwo = RowSelection.ByPath(
            new Dictionary<string, IReadOnlyCollection<long>> { [sevens] = [1L] });

        var expected = Initial.ToList();
        switch (op)
        {
            case "delete":
                Assert.Equal(1, (await table.DeleteAsync(Ex.Equal("id", 2L))).RowsDeleted);
                expected.RemoveAll(r => r.Id == 2);
                break;
            case "delete-rows":
                Assert.Equal(1, (await table.DeleteRowsAsync(idTwo, mode)).RowsDeleted);
                expected.RemoveAll(r => r.Id == 2);
                break;
            case "update":
                await table.UpdateAsync(Ex.Equal("id", 2L), b => SetV(b, 99), mode: mode);
                expected[1] = (2, 99, 7);
                break;
            case "update-move":
                await table.UpdateAsync(Ex.Equal("id", 2L), b => SetK(b, 8), mode: mode);
                expected[1] = (2, 20, 8);
                break;
            case "update-rows":
                await table.UpdateRowsAsync(idTwo, (_, batches, positions) =>
                    batches.Select((b, bi) => SetV(b, 99, i => positions[bi].GetValue(i) == 1)).ToList());
                expected[1] = (2, 99, 7);
                break;
            case "merge":
                await table.MergeAsync([Rows(7, 2, 6)], new MergeOptions
                {
                    Keys = ["id"],
                    DeleteMode = mode,
                    WhenMatched = [MergeAction.UpdateSet(new MergeAssignment("v", Ex.Lit(99L)))],
                    WhenNotMatched = [MergeAction.Insert],
                });
                expected[1] = (2, 99, 7);
                expected.Add((6, 60, 7));
                break;
        }

        var written = table.CurrentSnapshot.ActiveFiles.Values.Where(a => !before.Contains(a.Path)).ToList();
        Assert.NotEmpty(written);
        var (rowIdName, rowVersionName) = RowTrackingConfig.TryGetMaterializedColumnNames(
            table.CurrentSnapshot.Metadata.Configuration);
        int withRowTracking = 0;
        foreach (var add in written)
        {
            // A rewritten file carries its rows' materialized ids, with the row-lineage field ids on V3. (A file of
            // MERGE's inserted rows alone is an append and carries none.)
            var columns = await FileColumnIdsAsync(add.Path);
            if (columns.TryGetValue(rowIdName ?? "", out int? rowIdFieldId))
            {
                withRowTracking++;
                Assert.Equal(v3 ? IcebergCompat.RowIdFieldId : null, rowIdFieldId);
                Assert.Equal(v3 ? IcebergCompat.RowCommitVersionFieldId : null, columns[rowVersionName!]);
            }
            int? partition = add.PartitionValues[physicalK] is { } text
                ? int.Parse(text, System.Globalization.CultureInfo.InvariantCulture)
                : null;
            Assert.Equal([partition], await DistinctFileValuesAsync(add.Path, physicalK));
            Assert.DoesNotContain(physicalK, add.Stats!);
        }
        if (v3)
            Assert.NotEqual(0, withRowTracking);

        Assert.Equal(expected.OrderBy(r => r.Id), await ReadAsync(table));
        if (v3)
        {
            foreach (var (id, rowId) in await RowIdsByIdAsync(table))
            {
                if (rowIdsBefore.TryGetValue(id, out long prior))
                    Assert.Equal(prior, rowId);
            }
        }
        await using var reopened = await DeltaTable.OpenAsync(Fs);
        Assert.Equal(expected.OrderBy(r => r.Id), await ReadAsync(reopened));
    }

    /// <summary>
    /// A file of the NULL partition is rewritten with a null partition column, as the append path writes it.
    /// </summary>
    [Theory]
    [InlineData(IcebergCompat.EnableV2Key)]
    [InlineData(IcebergCompat.EnableV3Key)]
    public async Task ARewriteOfTheNullPartition_WritesANullPartitionColumn(string enableKey)
    {
        await using var table = await CreateAsync(enableKey);
        await table.WriteAsync([Rows(null, 1, 2, 3)]);
        var before = table.CurrentSnapshot.ActiveFiles.Keys.ToHashSet();
        string physicalK = PhysicalK(table);

        await table.UpdateAsync(Ex.Equal("id", 2L), b => SetV(b, 99));

        var add = Assert.Single(table.CurrentSnapshot.ActiveFiles.Values, a => !before.Contains(a.Path));
        Assert.Null(add.PartitionValues[physicalK]);
        Assert.Equal([(int?)null], await DistinctFileValuesAsync(add.Path, physicalK));
        Assert.Equal([(1L, 10L, (int?)null), (2, 99, null), (3, 30, null)], await ReadAsync(table));
    }

    // ---- Logical and physical names that overlap (#774) ---------------------------------------------------------

    // x is stored under the physical name p1 and p1 under p2, as on a table that had column mapping turned on and then
    // renamed p1 to x and p2 to p1 (see PhysicalNameResolutionTests).
    private static StructField Mapped(string name, string type, int id, string physical) => new()
    {
        Name = name,
        Type = new PrimitiveType { TypeName = type },
        Nullable = true,
        Metadata = new Dictionary<string, string>
        {
            [ColumnMapping.FieldIdKey] = id.ToString(System.Globalization.CultureInfo.InvariantCulture),
            [ColumnMapping.PhysicalNameKey] = physical,
        },
    };

    private static readonly Apache.Arrow.Schema XP1V = new Apache.Arrow.Schema.Builder()
        .Field(new Field("x", StringType.Default, true))
        .Field(new Field("p1", StringType.Default, true))
        .Field(new Field("v", Int64Type.Default, true))
        .Build();

    private static RecordBatch XP1VRow(long v) => new(XP1V,
        [
            new StringArray.Builder().Append("A").Build(),
            new StringArray.Builder().Append("B").Build(),
            new Int64Array.Builder().Append(v).Build(),
        ], 1);

    /// <summary>
    /// The add.partitionValues a rewrite starts from is read as the read path reads it. Its spelling is decided once,
    /// from every partition column, not key by key: a logical-keyed map <c>{x: A, p1: B}</c> (an older writer's) holds
    /// x = A, p1 = B, and a physical-keyed map lacking p2 (<c>{p1: A}</c>) holds x = A and a null p1. Looked up key by
    /// key, x took the logical p1's value from the first, and p1 took x's from the second. Copilot on #774 found this
    /// in the rewrite path; compaction had the same lookup.
    /// </summary>
    [Theory]
    [InlineData("logical", "update")]
    [InlineData("logical", "compact")]
    [InlineData("physical-sparse", "update")]
    [InlineData("physical-sparse", "compact")]
    public async Task OverlappingNames_TheRewrittenFileHoldsWhatAReadReturns(string spelling, string op)
    {
        var schema = new Schema.StructType
        {
            Fields = [Mapped("x", "string", 1, "p1"), Mapped("p1", "string", 2, "p2"), Mapped("v", "long", 3, "v")],
        };
        await using (var created = await DeltaTable.CreateAsync(Fs, XP1V, partitionColumns: ["x", "p1"],
            columnMappingMode: ColumnMappingMode.Name, preAssignedSchema: schema,
            configuration: new Dictionary<string, string> { [IcebergCompat.EnableV2Key] = "true" }))
        {
            await created.WriteAsync([XP1VRow(1)]);
            var original = Assert.Single(created.CurrentSnapshot.ActiveFiles.Values);
            Assert.Equal("A", original.PartitionValues["p1"]);
            Assert.Equal("B", original.PartitionValues["p2"]);

            // Re-add the file (twice, so compaction has two to join) under the other spelling.
            var values = spelling == "logical"
                ? new Dictionary<string, string> { ["x"] = "A", ["p1"] = "B" }
                : new Dictionary<string, string> { ["p1"] = "A" };
            var actions = new List<DeltaAction>
            {
                new RemoveFile { Path = original.Path, DeletionTimestamp = 0, DataChange = true },
            };
            string dir = Path.GetDirectoryName(Path.Combine(_tempDir, DeltaPath.Decode(original.Path)))!;
            foreach (string name in new[] { "copy-1.parquet", "copy-2.parquet" })
            {
                File.Copy(Path.Combine(_tempDir, DeltaPath.Decode(original.Path)), Path.Combine(dir, name));
                string path = original.Path.Substring(0, original.Path.LastIndexOf('/') + 1) + name;
                actions.Add(original with { Path = path, PartitionValues = values });
            }
            File.WriteAllBytes(
                Path.Combine(_tempDir, "_delta_log", $"{created.CurrentSnapshot.Version + 1:D20}.json"),
                ActionSerializer.Serialize(actions));
        }

        await using var table = await DeltaTable.OpenAsync(Fs);
        string? expectedP1 = spelling == "logical" ? "B" : null;
        Assert.All(await ReadXP1Async(table), row => Assert.Equal(("A", expectedP1), row));
        var before = table.CurrentSnapshot.ActiveFiles.Keys.ToHashSet();

        if (op == "update")
            await table.UpdateAsync(Ex.Equal("v", 1L), b => SetV(b, 99), mode: RowDeleteMode.CopyOnWrite);
        else
            Assert.NotNull(await table.CompactAsync(new CompactionOptions { MinFileSize = long.MaxValue }));

        var written = table.CurrentSnapshot.ActiveFiles.Values.Where(a => !before.Contains(a.Path)).ToList();
        Assert.NotEmpty(written);
        foreach (var add in written)
        {
            Assert.Equal(["A"], await DistinctFileStringsAsync(add.Path, "p1")); // x
            Assert.Equal([expectedP1], await DistinctFileStringsAsync(add.Path, "p2")); // p1
        }
        Assert.All(await ReadXP1Async(table), row => Assert.Equal(("A", expectedP1), row));
    }

    private static async Task<List<(string? X, string? P1)>> ReadXP1Async(DeltaTable table)
    {
        var rows = new List<(string?, string?)>();
        await foreach (var batch in table.ReadAllAsync())
        {
            var xs = (StringArray)batch.Column(batch.Schema.GetFieldIndex("x"));
            var p1s = (StringArray)batch.Column(batch.Schema.GetFieldIndex("p1"));
            for (int i = 0; i < batch.Length; i++)
                rows.Add((xs.GetString(i), p1s.GetString(i)));
        }
        Assert.NotEmpty(rows);
        return rows;
    }

    private async Task<List<string?>> DistinctFileStringsAsync(string path, string column)
    {
        await using var file = await Fs.OpenReadAsync(DeltaPath.Decode(path));
        using var reader = new Parquet.ParquetFileReader(file, ownsFile: false);
        var values = new HashSet<string?>();
        await foreach (var batch in reader.ReadAllAsync())
        {
            int index = batch.Schema.GetFieldIndex(column);
            Assert.True(index >= 0, $"'{path}' has no column '{column}'.");
            var array = (StringArray)batch.Column(index);
            for (int i = 0; i < batch.Length; i++)
                values.Add(array.GetString(i));
        }
        return values.ToList();
    }

    // The top-level Parquet columns of a data file, with the field id each carries (null for none).
    private async Task<Dictionary<string, int?>> FileColumnIdsAsync(string path)
    {
        await using var file = await Fs.OpenReadAsync(DeltaPath.Decode(path));
        using var reader = new Parquet.ParquetFileReader(file, ownsFile: false);
        var root = (await reader.GetSchemaAsync()).Root;
        return root.Children.ToDictionary(c => c.Name, c => c.Element.FieldId);
    }

    private async Task<List<int?>> DistinctFileValuesAsync(string path, string column)
    {
        await using var file = await Fs.OpenReadAsync(DeltaPath.Decode(path));
        using var reader = new Parquet.ParquetFileReader(file, ownsFile: false);
        var values = new HashSet<int?>();
        await foreach (var batch in reader.ReadAllAsync())
        {
            int index = batch.Schema.GetFieldIndex(column);
            Assert.True(index >= 0, $"'{path}' has no column '{column}'.");
            var array = (Int32Array)batch.Column(index);
            for (int i = 0; i < batch.Length; i++)
                values.Add(array.GetValue(i));
        }
        return values.ToList();
    }

    private static async Task<List<(long Id, long V, int? K)>> ReadAsync(DeltaTable table)
    {
        var rows = new List<(long, long, int?)>();
        await foreach (var batch in table.ReadAllAsync())
        {
            var ids = (Int64Array)batch.Column(batch.Schema.GetFieldIndex("id"));
            var vs = (Int64Array)batch.Column(batch.Schema.GetFieldIndex("v"));
            var ks = (Int32Array)batch.Column(batch.Schema.GetFieldIndex("k"));
            for (int i = 0; i < batch.Length; i++)
                rows.Add((ids.GetValue(i)!.Value, vs.GetValue(i)!.Value, ks.GetValue(i)));
        }
        return rows.OrderBy(r => r.Item1).ToList();
    }

    private static async Task<Dictionary<long, long>> RowIdsByIdAsync(DeltaTable table)
    {
        var map = new Dictionary<long, long>();
        if (!RowTrackingConfig.IsEnabled(table.CurrentSnapshot.Metadata.Configuration))
            return map;
        await foreach (var batch in table.ReadAsync(new DeltaReadOptions { Metadata = DeltaRowMetadata.RowTracking }))
        {
            var ids = (Int64Array)batch.Column(batch.Schema.GetFieldIndex("id"));
            var rowIds = (Int64Array)batch.Column(batch.Schema.GetFieldIndex(RowTrackingConfig.RowIdColumnName));
            for (int i = 0; i < batch.Length; i++)
                map[ids.GetValue(i)!.Value] = rowIds.GetValue(i)!.Value;
        }
        return map;
    }
}
