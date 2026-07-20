// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.Expressions;
using EngineeredWood.IO.Local;
using ArrowStructType = Apache.Arrow.Types.StructType;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// Stats for STRUCT columns: collected per leaf as nested JSON objects mirroring the schema, parsed back into
/// dotted keys ("s.a"), and consumed by the pruner so a predicate on a nested field prunes files like a
/// top-level column would.
/// </summary>
public class NestedStatsPruningTests : IDisposable
{
    private readonly string _tempDir;

    public NestedStatsPruningTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_nstats_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private static Apache.Arrow.Schema NestedSchema() =>
        new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("s", new ArrowStructType(
            [
                new Field("a", Int64Type.Default, true),
                new Field("b", StringType.Default, true),
            ]), true))
            .Build();

    private static RecordBatch Batch(Apache.Arrow.Schema schema, long id, long a, string b)
    {
        var structType = (ArrowStructType)schema.FieldsList[1].DataType;
        var nested = new StructArray(structType, 1,
        [
            new Int64Array.Builder().Append(a).Build(),
            new StringArray.Builder().Append(b).Build(),
        ], ArrowBuffer.Empty);
        return new RecordBatch(schema,
            [new Int64Array.Builder().Append(id).Build(), nested], 1);
    }

    [Fact]
    public async Task StatsCollector_EmitsNestedMinMaxAndNullCount()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = NestedSchema();

        await using var table = await DeltaTable.CreateAsync(fs, schema);
        await table.WriteAsync([Batch(schema, 1, 42, "hello")]);

        string stats = table.CurrentSnapshot.ActiveFiles.Values.Single().Stats!;
        using var doc = JsonDocument.Parse(stats);
        var root = doc.RootElement;

        // Nested stats mirror the schema: minValues.s.a, not minValues["s.a"].
        var minS = root.GetProperty("minValues").GetProperty("s");
        Assert.Equal(42L, minS.GetProperty("a").GetInt64());
        Assert.Equal("hello", minS.GetProperty("b").GetString());

        var maxS = root.GetProperty("maxValues").GetProperty("s");
        Assert.Equal(42L, maxS.GetProperty("a").GetInt64());

        var nullS = root.GetProperty("nullCount").GetProperty("s");
        Assert.Equal(0L, nullS.GetProperty("a").GetInt64());
    }

    [Fact]
    public void ColumnStats_FlattensNestedKeysIncludingNullCount()
    {
        string json = """
        {
          "numRecords": 2,
          "minValues": {"id": 1, "s": {"a": 10, "b": "x"}},
          "maxValues": {"id": 2, "s": {"a": 20, "b": "y"}},
          "nullCount": {"id": 0, "s": {"a": 1, "b": 0}}
        }
        """;

        var stats = ColumnStats.Parse(json)!;

        Assert.Equal(2L, stats.NumRecords);
        Assert.Equal(10L, stats.MinValues!["s.a"].GetInt64());
        Assert.Equal(20L, stats.MaxValues!["s.a"].GetInt64());
        Assert.Equal("y", stats.MaxValues!["s.b"].GetString());
        // Nested nullCount objects used to be dropped entirely at parse.
        Assert.Equal(1L, stats.NullCount!["s.a"]);
        Assert.Equal(0L, stats.NullCount!["id"]);
    }

    // A literal dotted column name colliding with a struct leaf path is ambiguous — pruning must never guess,
    // so the key is dropped and the file is simply kept.
    [Fact]
    public void ColumnStats_CollidingDottedKeyIsPoisoned()
    {
        string json = """
        {
          "numRecords": 1,
          "minValues": {"s.a": 99, "s": {"a": 10}},
          "nullCount": {"s.a": 5, "s": {"a": 1}}
        }
        """;

        var stats = ColumnStats.Parse(json)!;

        Assert.False(stats.MinValues!.ContainsKey("s.a"));
        Assert.False(stats.NullCount!.ContainsKey("s.a"));
    }

    // ── Pruner-level unit tests (exercise the decision directly, not just end-to-end) ──

    private static AddFile MakeAdd(string stats) => new()
    {
        Path = "part-0.parquet",
        PartitionValues = new Dictionary<string, string>(),
        Size = 1,
        ModificationTime = 0,
        DataChange = true,
        Stats = stats,
    };

    private static EngineeredWood.DeltaLake.Schema.StructType PrunerSchema(
        IReadOnlyDictionary<string, string>? sMeta = null,
        IReadOnlyDictionary<string, string>? aMeta = null) => new()
    {
        Fields =
        [
            new StructField
            {
                Name = "id", Type = new PrimitiveType { TypeName = "long" }, Nullable = false,
            },
            new StructField
            {
                Name = "s",
                Type = new EngineeredWood.DeltaLake.Schema.StructType
                {
                    Fields =
                    [
                        new StructField
                        {
                            Name = "a", Type = new PrimitiveType { TypeName = "integer" },
                            Nullable = true, Metadata = aMeta?.ToDictionary(k => k.Key, v => v.Value),
                        },
                        new StructField
                        {
                            Name = "b", Type = new PrimitiveType { TypeName = "string" }, Nullable = true,
                        },
                    ],
                },
                Nullable = true,
                Metadata = sMeta?.ToDictionary(k => k.Key, v => v.Value),
            },
        ],
    };

    private const string NestedStats =
        """{"numRecords":10,"minValues":{"id":1,"s":{"a":5,"b":"aaa"}},"maxValues":{"id":10,"s":{"a":9,"b":"zzz"}},"nullCount":{"id":0,"s":{"a":0,"b":10}}}""";

    [Fact]
    public void NestedComparison_PrunesAndKeeps()
    {
        var pruner = new DeltaFilePruner(PrunerSchema(), []);
        var add = MakeAdd(NestedStats);

        // s.a is bounded [5, 9] — outside prunes, inside keeps.
        Assert.False(pruner.ShouldInclude(add, Ex.Equal("s.a", LiteralValue.Of(4))));
        Assert.True(pruner.ShouldInclude(add, Ex.Equal("s.a", LiteralValue.Of(7))));
        Assert.False(pruner.ShouldInclude(add, Ex.GreaterThan("s.a", LiteralValue.Of(9))));
        Assert.True(pruner.ShouldInclude(add, Ex.LessThan("s.a", LiteralValue.Of(6))));
        // String leaf, byte-order bounds ["aaa", "zzz"].
        Assert.False(pruner.ShouldInclude(add, Ex.Equal("s.b", LiteralValue.Of("zzzz"))));
    }

    [Fact]
    public void NestedNullCount_Prunes()
    {
        var pruner = new DeltaFilePruner(PrunerSchema(), []);
        var add = MakeAdd(NestedStats);

        // s.a has no nulls; s.b is all-null (10 of 10). This is the arm that silently did nothing
        // while nested nullCount objects were dropped at parse.
        Assert.False(pruner.ShouldInclude(add, Ex.IsNull("s.a")));
        Assert.True(pruner.ShouldInclude(add, Ex.IsNull("s.b")));
        Assert.False(pruner.ShouldInclude(add, Ex.IsNotNull("s.b")));
    }

    [Fact]
    public void ColumnMapping_ResolvesDottedPhysicalKeys()
    {
        // Stats keyed by PHYSICAL names at every level; the predicate references logical "s.a".
        var pruner = new DeltaFilePruner(
            PrunerSchema(
                sMeta: new Dictionary<string, string> { [ColumnMapping.PhysicalNameKey] = "col-s" },
                aMeta: new Dictionary<string, string> { [ColumnMapping.PhysicalNameKey] = "col-a" }),
            []);
        var add = MakeAdd(
            """{"numRecords":10,"minValues":{"col-s":{"col-a":5}},"maxValues":{"col-s":{"col-a":9}},"nullCount":{"col-s":{"col-a":0}}}""");

        Assert.False(pruner.ShouldInclude(add, Ex.Equal("s.a", LiteralValue.Of(4))));
        Assert.True(pruner.ShouldInclude(add, Ex.Equal("s.a", LiteralValue.Of(7))));
    }

    [Fact]
    public void DottedNameCollision_IsPoisonedNotGuessed()
    {
        // A literal top-level column named "s.a" alongside struct leaf s.a is ambiguous — never prune.
        var schema = new EngineeredWood.DeltaLake.Schema.StructType
        {
            Fields =
            [
                new StructField
                {
                    Name = "s.a", Type = new PrimitiveType { TypeName = "integer" }, Nullable = true,
                },
                new StructField
                {
                    Name = "s",
                    Type = new EngineeredWood.DeltaLake.Schema.StructType
                    {
                        Fields =
                        [
                            new StructField
                            {
                                Name = "a", Type = new PrimitiveType { TypeName = "integer" },
                                Nullable = true,
                            },
                        ],
                    },
                    Nullable = true,
                },
            ],
        };
        var pruner = new DeltaFilePruner(schema, []);
        // Stats that would prune under EITHER interpretation — the file must still be kept.
        var add = MakeAdd(
            """{"numRecords":10,"minValues":{"s.a":100,"s":{"a":100}},"maxValues":{"s.a":200,"s":{"a":200}},"nullCount":{"s.a":0,"s":{"a":0}}}""");

        Assert.True(pruner.ShouldInclude(add, Ex.Equal("s.a", LiteralValue.Of(4))));
    }

    [Fact]
    public void MissingNestedStats_KeepsFile()
    {
        // Pruning is superset-safe: an unresolvable reference evaluates Unknown, so the file is kept.
        var pruner = new DeltaFilePruner(PrunerSchema(), []);
        var add = MakeAdd(
            """{"numRecords":10,"minValues":{"id":1},"maxValues":{"id":10},"nullCount":{"id":0}}""");

        Assert.True(pruner.ShouldInclude(add, Ex.Equal("s.a", LiteralValue.Of(4))));
    }

    // The point of all of it: a predicate on a nested leaf prunes files.
    [Fact]
    public async Task Read_NestedPredicate_PrunesNonMatchingFiles()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = NestedSchema();
        var options = new DeltaTableOptions { CheckpointInterval = 0 };

        await using var table = await DeltaTable.CreateAsync(fs, schema, options);
        await table.WriteAsync([Batch(schema, 1, 10, "low")]);
        await table.WriteAsync([Batch(schema, 2, 500, "high")]);
        Assert.Equal(2, table.CurrentSnapshot.FileCount);

        // s.a = 500 lies outside file 1's [10,10] bounds, so that file is pruned on stats alone.
        var rows = new List<long>();
        await foreach (var b in table.ReadAllAsync(columns: null, Ex.Equal("s.a", 500L)))
        {
            var ids = (Int64Array)b.Column(0);
            for (int i = 0; i < b.Length; i++)
                rows.Add(ids.GetValue(i)!.Value);
        }

        Assert.Equal([2L], rows);
    }
}
