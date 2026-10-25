// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.Expressions;
using EngineeredWood.IO.Local;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// Which <c>partitionValues</c> spellings mean NULL, and that the scan, the pruner and the writer agree on them
/// (#511). NULL is JSON null, an empty string for any type (the protocol's rule), and Hive's
/// <c>__HIVE_DEFAULT_PARTITION__</c> sentinel for a column that is not a string (Spark's read). For a string
/// column the sentinel is its text, as Spark and Delta Kernel read it. The pruner took the NULL spellings for
/// values and skipped those files for <c>IS NULL</c>. The table is partitioned by <c>part</c>: ten rows in one
/// file (ids 0..9) and ten in another (ids 10..19), the second re-added with the spelling under test.
/// </summary>
public class PartitionNullSentinelTests : IDisposable
{
    private const string Sentinel = "__HIVE_DEFAULT_PARTITION__";

    private readonly string _tempDir;

    public PartitionNullSentinelTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_partnull_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    /// <summary>The spellings that mean NULL.</summary>
    public static TheoryData<string, string> NullSpellings => new()
    {
        { "string", "" },
        { "int", Sentinel },
        { "int", "" },
    };

    private static Apache.Arrow.Schema SchemaOf(string type, bool nullable) => new Apache.Arrow.Schema.Builder()
        .Field(new Field("part", type == "string" ? StringType.Default : Int32Type.Default, nullable))
        .Field(new Field("id", Int64Type.Default, false))
        .Build();

    private ValueTask<DeltaTable> NewTableAsync(Apache.Arrow.Schema schema) => DeltaTable.CreateAsync(
        new LocalTableFileSystem(Path.Combine(_tempDir, Guid.NewGuid().ToString("N"))), schema,
        partitionColumns: ["part"], enableDeletionVectors: true);

    private static RecordBatch Rows(Apache.Arrow.Schema schema, params string?[] parts)
    {
        IArrowArray partArray;
        if (schema.GetFieldByName("part").DataType is StringType)
        {
            var builder = new StringArray.Builder();
            foreach (var p in parts) { if (p is null) builder.AppendNull(); else builder.Append(p); }
            partArray = builder.Build();
        }
        else
        {
            var builder = new Int32Array.Builder();
            foreach (var p in parts) { if (p is null) builder.AppendNull(); else builder.Append(int.Parse(p)); }
            partArray = builder.Build();
        }
        var ids = new Int64Array.Builder();
        for (int i = 0; i < parts.Length; i++) ids.Append(i);
        return new RecordBatch(schema, [partArray, ids.Build()], parts.Length);
    }

    /// <summary>Ids 0..9 in partition <c>1</c> and 10..19 in <c>2</c>, whose file is then re-added with
    /// <paramref name="spelling"/> as its partition value.</summary>
    private async Task<DeltaTable> CreateAsync(string type, string? spelling, bool nullable = true)
    {
        var schema = SchemaOf(type, nullable);
        var table = await NewTableAsync(schema);
        await table.WriteAsync([Rows(schema, [.. Enumerable.Repeat("1", 10), .. Enumerable.Repeat("2", 10)])]);

        var file = table.CurrentSnapshot.ActiveFiles.Values.Single(f => f.PartitionValues["part"] == "2");
        await using var transaction = table.StartTransaction();
        transaction.StageActions(
        [
            new RemoveFile
            {
                Path = file.Path,
                DeletionTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                DataChange = false,
            },
            file with
            {
                PartitionValues = new Dictionary<string, string> { ["part"] = spelling! },
                DataChange = false,
            },
        ]);
        await transaction.CommitAsync();
        // A JSON-null value may come back as a missing key, which means the same.
        Assert.Equal(spelling, table.CurrentSnapshot.ActiveFiles[file.Path].PartitionValues
            .TryGetValue("part", out var stored) ? stored : null);
        return table;
    }

    private static async Task<List<long>> IdsAsync(DeltaTable table, Predicate? filter = null)
    {
        var ids = new List<long>();
        await foreach (var batch in table.ReadAsync(new DeltaReadOptions { Filter = filter }))
        {
            var column = (Int64Array)batch.Column("id");
            for (int i = 0; i < batch.Length; i++)
                ids.Add(column.GetValue(i)!.Value);
        }
        ids.Sort();
        return ids;
    }

    private static List<long> Range(int start, int count) => Enumerable.Range(start, count).Select(i => (long)i).ToList();

    private static readonly List<long> NullIds = Range(10, 10);
    private static readonly List<long> ValueIds = Range(0, 10);

    // ── The NULL spellings ──

    [Theory]
    [MemberData(nameof(NullSpellings))]
    public async Task Scan_IsNull_FindsTheFile(string type, string spelling)
    {
        await using var table = await CreateAsync(type, spelling);

        Assert.Equal(NullIds, await IdsAsync(table, Ex.IsNull("part")));
        Assert.Equal(ValueIds, await IdsAsync(table, Ex.IsNotNull("part")));
    }

    [Theory]
    [MemberData(nameof(NullSpellings))]
    public async Task Delete_IsNull_DeletesTheFile(string type, string spelling)
    {
        await using var table = await CreateAsync(type, spelling);

        var (deleted, _) = await table.DeleteAsync(Ex.IsNull("part"));

        Assert.Equal(10, deleted);
        Assert.Equal(ValueIds, await IdsAsync(table));
    }

    /// <summary>The partition arm beside a data arm: the file is read, not removed whole, and still found.</summary>
    [Theory]
    [MemberData(nameof(NullSpellings))]
    public async Task Delete_IsNullAndData_DeletesFromTheFile(string type, string spelling)
    {
        await using var table = await CreateAsync(type, spelling);

        var (deleted, _) = await table.DeleteAsync(Ex.And(Ex.IsNull("part"), Ex.GreaterThanOrEqual("id", 15L)));

        Assert.Equal(5, deleted);
        Assert.Equal(Range(0, 15), await IdsAsync(table));
    }

    [Theory]
    [MemberData(nameof(NullSpellings))]
    public async Task Delete_IsNotNull_LeavesTheFile(string type, string spelling)
    {
        await using var table = await CreateAsync(type, spelling);

        var (deleted, _) = await table.DeleteAsync(Ex.IsNotNull("part"));

        Assert.Equal(10, deleted);
        Assert.Equal(NullIds, await IdsAsync(table));
    }

    // ── The sentinel in a string column is its text ──

    [Fact]
    public async Task StringSentinel_IsTheText()
    {
        await using var table = await CreateAsync("string", Sentinel);

        Assert.Empty(await IdsAsync(table, Ex.IsNull("part")));
        Assert.Equal(NullIds, await IdsAsync(table, Ex.Equal("part", Sentinel)));
        var (deleted, _) = await table.DeleteAsync(Ex.IsNull("part"));
        Assert.Equal(0, deleted);
    }

    /// <summary>The pruner reads a partition value as the scan does.</summary>
    [Theory]
    [InlineData("string", "", true)]
    [InlineData("integer", "", true)]
    [InlineData("date", "", true)]
    [InlineData("integer", Sentinel, true)]
    [InlineData("date", Sentinel, true)]
    [InlineData("string", Sentinel, false)]
    [InlineData("integer", "5", false)]
    public void Pruner_ReadsThePartitionValueAsTheScanDoes(string type, string spelling, bool isNull)
    {
        var pruner = new DeltaFilePruner(PrunerSchema(type, nullable: true), partitionColumns: ["part"]);
        var addFile = FileWith(spelling);

        Assert.Equal(isNull, pruner.ShouldInclude(addFile, Ex.IsNull("part")));
        Assert.Equal(!isNull, pruner.ShouldInclude(addFile, Ex.IsNotNull("part")));
    }

    // ── A NULL in a non-nullable column is refused, not read ──

    public static TheoryData<string, string?> NotNullViolations => new()
    {
        { "string", null },
        { "string", "" },
        { "int", Sentinel },
        { "int", "" },
    };

    [Theory]
    [MemberData(nameof(NotNullViolations))]
    public async Task NonNullableColumn_ScanRefusesTheFile(string type, string? spelling)
    {
        await using var table = await CreateAsync(type, spelling, nullable: false);

        var error = await Assert.ThrowsAsync<DeltaFormatException>(() => IdsAsync(table));
        Assert.Equal(DeltaTableErrorCodes.NotNullConstraintViolated, error.ErrorCode);
        Assert.Contains("'part'", error.Message);
        // A filter that a skip could answer reads the file, and so refuses it too.
        await Assert.ThrowsAsync<DeltaFormatException>(() => IdsAsync(table, Ex.IsNotNull("part")));
        await Assert.ThrowsAsync<DeltaFormatException>(() => IdsAsync(table, Ex.IsNull("part")));
    }

    [Theory]
    [MemberData(nameof(NotNullViolations))]
    public async Task NonNullableColumn_DeleteRefusesTheFile(string type, string? spelling)
    {
        await using var table = await CreateAsync(type, spelling, nullable: false);
        long version = table.CurrentSnapshot.Version;

        await Assert.ThrowsAsync<DeltaFormatException>(() => table.DeleteAsync(Ex.IsNotNull("part")).AsTask());
        await Assert.ThrowsAsync<DeltaFormatException>(() => table.DeleteAsync(Ex.IsNull("part")).AsTask());
        Assert.Equal(version, table.CurrentSnapshot.Version);
    }

    [Theory]
    [InlineData("string", "")]
    [InlineData("integer", Sentinel)]
    public void Pruner_NonNullableColumn_KeepsTheFile(string type, string spelling)
    {
        var pruner = new DeltaFilePruner(PrunerSchema(type, nullable: false), partitionColumns: ["part"]);
        var addFile = FileWith(spelling);

        Assert.True(pruner.ShouldInclude(addFile, Ex.IsNull("part")));
        Assert.True(pruner.ShouldInclude(addFile, Ex.IsNotNull("part")));
    }

    // ── The writer ──

    /// <summary>An empty string is written as null, as Spark writes it, since it reads back as NULL.</summary>
    [Fact]
    public async Task Write_EmptyString_IsStoredAsNull()
    {
        var schema = SchemaOf("string", nullable: true);
        await using var table = await NewTableAsync(schema);

        await table.WriteAsync([Rows(schema, "", "a")]);

        var values = table.CurrentSnapshot.ActiveFiles.Values
            .Select(f => f.PartitionValues.TryGetValue("part", out var v) ? v : null)
            .OrderBy(v => v, StringComparer.Ordinal).ToList();
        Assert.Equal([null, "a"], values);
        Assert.Equal([0L], await IdsAsync(table, Ex.IsNull("part")));
    }

    /// <summary>A NULL row and a row holding the sentinel's text are different partitions.</summary>
    [Fact]
    public async Task Write_NullAndSentinelText_StayApart()
    {
        var schema = SchemaOf("string", nullable: true);
        await using var table = await NewTableAsync(schema);

        await table.WriteAsync([Rows(schema, null, Sentinel)]);

        Assert.Equal(2, table.CurrentSnapshot.ActiveFiles.Count);
        var parts = new SortedDictionary<long, string?>();
        await foreach (var batch in table.ReadAsync(new DeltaReadOptions()))
        {
            var ids = (Int64Array)batch.Column("id");
            var values = (StringArray)batch.Column("part");
            for (int i = 0; i < batch.Length; i++)
                parts[ids.GetValue(i)!.Value] = values.IsNull(i) ? null : values.GetString(i);
        }
        Assert.Equal([null, Sentinel], parts.Values);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public async Task Write_NonNullableColumn_RefusesNullOrEmpty(string? value)
    {
        var schema = SchemaOf("string", nullable: false);
        await using var table = await NewTableAsync(schema);
        long version = table.CurrentSnapshot.Version;

        var error = await Assert.ThrowsAsync<ArgumentException>(
            () => table.WriteAsync([Rows(schema, "a", value)]).AsTask());
        Assert.Contains("'part'", error.Message);
        Assert.Equal(version, table.CurrentSnapshot.Version);
    }

    /// <summary>The batch's own field may be nullable where the table's is not; the table's is the one that
    /// counts, on the committing path and the staged one.</summary>
    [Theory]
    [InlineData(null, false)]
    [InlineData("", false)]
    [InlineData(null, true)]
    [InlineData("", true)]
    public async Task Write_NullableBatchField_NonNullableTable_IsRefused(string? value, bool staged)
    {
        await using var table = await NewTableAsync(SchemaOf("string", nullable: false));
        long version = table.CurrentSnapshot.Version;
        var batch = Rows(SchemaOf("string", nullable: true), "a", value);

        var error = await Assert.ThrowsAsync<ArgumentException>(() => staged
            ? table.WriteDataFilesAsync([batch]).AsTask()
            : table.WriteAsync([batch]).AsTask());
        Assert.Contains("'part'", error.Message);
        Assert.Equal(version, table.CurrentSnapshot.Version);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public async Task Update_NullOrEmptyInNonNullableColumn_IsRefused(string? value)
    {
        var schema = SchemaOf("string", nullable: false);
        await using var table = await NewTableAsync(schema);
        await table.WriteAsync([Rows(schema, "a", "b")]);
        long version = table.CurrentSnapshot.Version;

        await Assert.ThrowsAsync<ArgumentException>(() => table.UpdateAsync(
            Ex.Equal("id", 0L),
            batch =>
            {
                var parts = new StringArray.Builder();
                for (int i = 0; i < batch.Length; i++) { if (value is null) parts.AppendNull(); else parts.Append(value); }
                int index = batch.Schema.GetFieldIndex("part");
                var columns = batch.Arrays.ToArray();
                columns[index] = parts.Build();
                return new RecordBatch(batch.Schema, columns, batch.Length);
            }).AsTask());
        Assert.Equal(version, table.CurrentSnapshot.Version);
    }

    /// <summary>A CHECK constraint sees the value the table stores: an empty string is stored as NULL.</summary>
    [Fact]
    public async Task Write_CheckConstraint_SeesTheStoredNull()
    {
        var schema = SchemaOf("string", nullable: true);
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(Path.Combine(_tempDir, Guid.NewGuid().ToString("N"))), schema,
            partitionColumns: ["part"],
            configuration: new Dictionary<string, string> { ["delta.constraints.c"] = "part IS NOT NULL" });
        long version = table.CurrentSnapshot.Version;

        await Assert.ThrowsAnyAsync<Exception>(() => table.WriteAsync([Rows(schema, "a", "")]).AsTask());
        Assert.Equal(version, table.CurrentSnapshot.Version);
    }

    /// <summary>A generated column is computed from the value the table stores, so it agrees with a read.</summary>
    [Fact]
    public async Task Write_GeneratedColumn_SeesTheStoredNull()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("part", StringType.Default, true))
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("g", BooleanType.Default, true,
                new Dictionary<string, string> { ["delta.generationExpression"] = "part IS NULL" }))
            .Build();
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(Path.Combine(_tempDir, Guid.NewGuid().ToString("N"))), schema,
            partitionColumns: ["part"]);

        await table.WriteAsync([Rows(SchemaOf("string", nullable: true), "", "a")]);

        var rows = new SortedDictionary<long, bool?>();
        await foreach (var batch in table.ReadAsync(new DeltaReadOptions()))
        {
            var ids = (Int64Array)batch.Column("id");
            var g = (BooleanArray)batch.Column("g");
            for (int i = 0; i < batch.Length; i++)
                rows[ids.GetValue(i)!.Value] = g.GetValue(i);
        }
        Assert.Equal([true, false], rows.Values);
    }

    /// <summary>Two partition columns whose values hold the old group key's separator stay two partitions.</summary>
    [Fact]
    public async Task Write_ValuesHoldingTheKeySeparator_StayApart()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("p1", StringType.Default, true))
            .Field(new Field("p2", StringType.Default, true))
            .Field(new Field("id", Int64Type.Default, false))
            .Build();
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(Path.Combine(_tempDir, Guid.NewGuid().ToString("N"))), schema,
            partitionColumns: ["p1", "p2"]);
        string[] p1 = ["a\u0000=b", "a"];
        string[] p2 = ["c", "b\u0000=c"];
        await table.WriteAsync([new RecordBatch(schema,
            [new StringArray.Builder().AppendRange(p1).Build(), new StringArray.Builder().AppendRange(p2).Build(),
             new Int64Array.Builder().Append(0).Append(1).Build()], 2)]);

        Assert.Equal(2, table.CurrentSnapshot.ActiveFiles.Count);
        await foreach (var batch in table.ReadAsync(new DeltaReadOptions()))
        {
            for (int i = 0; i < batch.Length; i++)
            {
                int id = (int)((Int64Array)batch.Column("id")).GetValue(i)!.Value;
                Assert.Equal(p1[id], ((StringArray)batch.Column("p1")).GetString(i));
                Assert.Equal(p2[id], ((StringArray)batch.Column("p2")).GetString(i));
            }
        }
    }

    // ── Overwrites match the NULL partition in every spelling ──

    /// <summary>A file an older writer left with <c>""</c> is the NULL partition, which a dynamic overwrite of
    /// NULL rows replaces.</summary>
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public async Task DynamicOverwrite_ReplacesALegacyEmptyStringFile(string? incoming)
    {
        await using var table = await CreateAsync("string", "");
        var schema = SchemaOf("string", nullable: true);

        await table.DynamicOverwriteAsync([Rows(schema, incoming)]);

        // The new row is id 0 in the NULL partition; ids 10..19 (the legacy file) are gone, partition 1 stays.
        Assert.Equal([0L, .. ValueIds], await IdsAsync(table));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public async Task OverwritePartitions_NullTarget_MatchesEveryNullSpelling(string? target)
    {
        await using var table = await CreateAsync("string", "");
        var schema = SchemaOf("string", nullable: true);

        await table.OverwritePartitionsAsync([Rows(schema, "")], new Dictionary<string, string> { ["part"] = target! });

        Assert.Equal([0L, .. ValueIds], await IdsAsync(table));
    }

    [Fact]
    public void CanonicalPartitionKey_EveryNullSpellingIsOnePartition()
    {
        var schema = new DeltaLake.Schema.StructType
        {
            Fields =
            [
                new DeltaLake.Schema.StructField
                {
                    Name = "s", Type = new DeltaLake.Schema.PrimitiveType { TypeName = "string" }, Nullable = true,
                },
                new DeltaLake.Schema.StructField
                {
                    Name = "n", Type = new DeltaLake.Schema.PrimitiveType { TypeName = "integer" }, Nullable = true,
                },
            ],
        };
        string Key(Dictionary<string, string> values) =>
            DeltaTable.CanonicalPartitionKey(values, ["s", "n"], null, schema);
        var allNull = Key([]);

        Assert.Equal(allNull, Key(new() { ["s"] = null!, ["n"] = null! }));
        Assert.Equal(allNull, Key(new() { ["s"] = "", ["n"] = "" }));
        Assert.Equal(allNull, Key(new() { ["n"] = Sentinel }));
        Assert.NotEqual(allNull, Key(new() { ["s"] = Sentinel }));
        Assert.NotEqual(allNull, Key(new() { ["n"] = "1" }));
    }

    // ── IcebergCompat files store their partition values; the log's are the ones a read returns ──

    /// <summary>The file holds <c>part = 'a'</c>, materialized as IcebergCompat requires, and the log says
    /// <c>""</c>, which reads as NULL. Row-group pruning on the file's own copy skipped every row.</summary>
    [Fact]
    public async Task IcebergCompat_MaterializedValue_DoesNotDecideAPartitionFilter()
    {
        var schema = SchemaOf("string", nullable: true);
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(Path.Combine(_tempDir, Guid.NewGuid().ToString("N"))), schema,
            partitionColumns: ["part"], columnMappingMode: DeltaLake.Schema.ColumnMappingMode.Name,
            configuration: new Dictionary<string, string> { ["delta.enableIcebergCompatV2"] = "true" });
        await table.WriteAsync([Rows(schema, [.. Enumerable.Repeat("a", 10)])]);
        var file = table.CurrentSnapshot.ActiveFiles.Values.Single();
        await using (var transaction = table.StartTransaction())
        {
            transaction.StageActions(
            [
                new RemoveFile
                {
                    Path = file.Path,
                    DeletionTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                    DataChange = false,
                },
                file with
                {
                    PartitionValues = file.PartitionValues.ToDictionary(kv => kv.Key, _ => ""),
                    DataChange = false,
                },
            ]);
            await transaction.CommitAsync();
        }

        Assert.Equal(Range(0, 10), await IdsAsync(table, Ex.IsNull("part")));
        var (deleted, _) = await table.DeleteAsync(Ex.IsNull("part"));
        Assert.Equal(10, deleted);
    }

    private static DeltaLake.Schema.StructType PrunerSchema(string type, bool nullable) => new()
    {
        Fields =
        [
            new DeltaLake.Schema.StructField
            {
                Name = "part", Type = new DeltaLake.Schema.PrimitiveType { TypeName = type }, Nullable = nullable,
            },
            new DeltaLake.Schema.StructField
            {
                Name = "id", Type = new DeltaLake.Schema.PrimitiveType { TypeName = "long" }, Nullable = false,
            },
        ],
    };

    private static AddFile FileWith(string spelling) => new()
    {
        Path = "f.parquet",
        PartitionValues = new Dictionary<string, string> { ["part"] = spelling },
        Size = 1,
        ModificationTime = 0,
        DataChange = true,
        Stats = """{"numRecords":10}""",
    };
}
