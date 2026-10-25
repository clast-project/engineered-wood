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
/// A file whose <c>partitionValues</c> spell NULL as Hive's <c>__HIVE_DEFAULT_PARTITION__</c> sentinel, or as
/// <c>""</c> for a column that is not a string, reads as all NULL, so the pruner must see it that way too: it
/// took the spelling for a value and skipped the file for <c>IS NULL</c> (#511). The table is partitioned by
/// <c>part</c>: ten rows of a value and ten of NULL, the NULL file re-added with the spelling under test.
/// </summary>
public class PartitionNullSentinelTests : IDisposable
{
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

    public static TheoryData<string, string> Spellings => new()
    {
        { "string", "__HIVE_DEFAULT_PARTITION__" },
        { "int", "__HIVE_DEFAULT_PARTITION__" },
        { "int", "" },
    };

    private async Task<DeltaTable> CreateAsync(string type, string spelling)
    {
        IArrowType partType = type == "string" ? StringType.Default : Int32Type.Default;
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("part", partType, true))
            .Field(new Field("id", Int64Type.Default, false))
            .Build();
        var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(Path.Combine(_tempDir, Guid.NewGuid().ToString("N"))), schema,
            partitionColumns: ["part"], enableDeletionVectors: true);

        IArrowArray parts;
        if (type == "string")
        {
            var builder = new StringArray.Builder();
            for (int i = 0; i < 20; i++) { if (i < 10) builder.Append("a"); else builder.AppendNull(); }
            parts = builder.Build();
        }
        else
        {
            var builder = new Int32Array.Builder();
            for (int i = 0; i < 20; i++) { if (i < 10) builder.Append(1); else builder.AppendNull(); }
            parts = builder.Build();
        }
        var ids = new Int64Array.Builder();
        for (int i = 0; i < 20; i++) ids.Append(i);
        await table.WriteAsync([new RecordBatch(schema, [parts, ids.Build()], 20)]);

        // The NULL partition's file, which this engine writes without the key, re-added with the spelling.
        var file = table.CurrentSnapshot.ActiveFiles.Values.Single(f => !f.PartitionValues.TryGetValue("part", out var v) || v is null);
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
                PartitionValues = new Dictionary<string, string> { ["part"] = spelling },
                DataChange = false,
            },
        ]);
        await transaction.CommitAsync();
        Assert.Equal(spelling, table.CurrentSnapshot.ActiveFiles[file.Path].PartitionValues["part"]);
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

    private static readonly List<long> NullIds = Enumerable.Range(10, 10).Select(i => (long)i).ToList();
    private static readonly List<long> ValueIds = Enumerable.Range(0, 10).Select(i => (long)i).ToList();

    /// <summary>The pruner reads a partition value as the scan does: NULL for the sentinel, and for <c>""</c>
    /// only when the column is not a string, since an empty string is a string's value.</summary>
    [Theory]
    [InlineData("string", "__HIVE_DEFAULT_PARTITION__", true)]
    [InlineData("integer", "__HIVE_DEFAULT_PARTITION__", true)]
    [InlineData("integer", "", true)]
    [InlineData("date", "", true)]
    [InlineData("string", "", false)]
    [InlineData("integer", "5", false)]
    public void Pruner_ReadsThePartitionValueAsTheScanDoes(string type, string spelling, bool isNull)
    {
        var schema = new DeltaLake.Schema.StructType
        {
            Fields =
            [
                new DeltaLake.Schema.StructField
                {
                    Name = "part", Type = new DeltaLake.Schema.PrimitiveType { TypeName = type }, Nullable = true,
                },
                new DeltaLake.Schema.StructField
                {
                    Name = "id", Type = new DeltaLake.Schema.PrimitiveType { TypeName = "long" }, Nullable = false,
                },
            ],
        };
        var pruner = new DeltaFilePruner(schema, partitionColumns: ["part"]);
        var addFile = new AddFile
        {
            Path = "f.parquet",
            PartitionValues = new Dictionary<string, string> { ["part"] = spelling },
            Size = 1,
            ModificationTime = 0,
            DataChange = true,
            Stats = """{"numRecords":10}""",
        };

        Assert.Equal(isNull, pruner.ShouldInclude(addFile, Ex.IsNull("part")));
        Assert.Equal(!isNull, pruner.ShouldInclude(addFile, Ex.IsNotNull("part")));
    }

    [Theory]
    [MemberData(nameof(Spellings))]
    public async Task Scan_IsNull_FindsTheFile(string type, string spelling)
    {
        await using var table = await CreateAsync(type, spelling);

        Assert.Equal(NullIds, await IdsAsync(table, Ex.IsNull("part")));
        Assert.Equal(ValueIds, await IdsAsync(table, Ex.IsNotNull("part")));
    }

    [Theory]
    [MemberData(nameof(Spellings))]
    public async Task Delete_IsNull_DeletesTheFile(string type, string spelling)
    {
        await using var table = await CreateAsync(type, spelling);

        var (deleted, _) = await table.DeleteAsync(Ex.IsNull("part"));

        Assert.Equal(10, deleted);
        Assert.Equal(ValueIds, await IdsAsync(table));
    }

    /// <summary>The partition arm beside a data arm: the file is read, not removed whole, and still found.</summary>
    [Theory]
    [MemberData(nameof(Spellings))]
    public async Task Delete_IsNullAndData_DeletesFromTheFile(string type, string spelling)
    {
        await using var table = await CreateAsync(type, spelling);

        var (deleted, _) = await table.DeleteAsync(Ex.And(Ex.IsNull("part"), Ex.GreaterThanOrEqual("id", 15L)));

        Assert.Equal(5, deleted);
        Assert.Equal(Enumerable.Range(0, 15).Select(i => (long)i).ToList(), await IdsAsync(table));
    }

    [Theory]
    [MemberData(nameof(Spellings))]
    public async Task Delete_IsNotNull_LeavesTheFile(string type, string spelling)
    {
        await using var table = await CreateAsync(type, spelling);

        var (deleted, _) = await table.DeleteAsync(Ex.IsNotNull("part"));

        Assert.Equal(10, deleted);
        Assert.Equal(NullIds, await IdsAsync(table));
    }
}
