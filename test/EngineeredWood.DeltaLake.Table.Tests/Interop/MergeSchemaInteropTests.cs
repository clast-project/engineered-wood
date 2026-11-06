// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests.Interop;

/// <summary>
/// <para><b>Tier 3.</b> engineered-wood's <c>mergeSchema</c> append (#568) against Spark's own schema evolution:
/// each case has Spark create and seed two identical tables, then appends the same wider row to one with
/// <see cref="DeltaTable.WriteAsync(IReadOnlyList{RecordBatch}, DeltaWriteMode, CancellationToken, IReadOnlyList{string}, bool?, CommitProperties, bool)"/>
/// and <c>mergeSchema: true</c>, and to the other with <c>spark.databricks.delta.schema.autoMerge.enabled</c> and
/// <c>INSERT … BY NAME</c>. It requires the same logical schema (names, types, nullability, recorded type
/// changes), the same table features and the same rows, and that Spark can append through the table EW merged.
/// See <see cref="Spark"/> for setup and cost.</para>
/// </summary>
[Collection("Interop")]
public class MergeSchemaInteropTests : IDisposable
{
    private readonly string _ours;
    private readonly string _theirs;

    public MergeSchemaInteropTests()
    {
        string root = Path.Combine(Path.GetTempPath(), $"delta_mergeschema_interop_{Guid.NewGuid():N}");
        _ours = Path.Combine(root, "ours");
        _theirs = Path.Combine(root, "theirs");
        Directory.CreateDirectory(_ours);
        Directory.CreateDirectory(_theirs);
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        string root = Path.GetDirectoryName(_ours)!;
        if (Directory.Exists(root))
        {
            try { Directory.Delete(root, recursive: true); } catch { }
        }
    }

    internal static JsonElement Sql(string dir, params string[] statements)
    {
        var result = Spark.InvokeRaw("sql", new { path = dir, sql = statements });
        Assert.True(result.GetProperty("ok").GetBoolean(), result.ToString());
        return result;
    }

    private static void CreateAndSeed(string dir, string idType, bool columnMapping)
    {
        string properties = columnMapping
            ? "'delta.enableTypeWidening'='true', 'delta.columnMapping.mode'='name'"
            : "'delta.enableTypeWidening'='true'";
        Sql(dir,
            $"CREATE TABLE delta.`{{path}}` (id {idType}, p STRING, s STRUCT<a: INT>) USING delta "
            + $"TBLPROPERTIES ({properties})",
            "INSERT INTO delta.`{path}` VALUES (1, 'a', named_struct('a', 10))");
    }

    /// <summary>The newest metaData's schema, logically: names, types, nullability and recorded type changes,
    /// without column-mapping ids and physical names.</summary>
    internal static string LogicalSchema(string dir)
    {
        string? schemaString = null;
        foreach (var file in Directory.GetFiles(Path.Combine(dir, "_delta_log"), "*.json").OrderBy(f => f, StringComparer.Ordinal))
        {
            foreach (var line in File.ReadAllLines(file))
            {
                if (string.IsNullOrWhiteSpace(line))
                    continue;
                using var doc = JsonDocument.Parse(line);
                if (doc.RootElement.TryGetProperty("metaData", out var metaData))
                    schemaString = metaData.GetProperty("schemaString").GetString();
            }
        }
        using var schema = JsonDocument.Parse(schemaString!);
        var sb = new StringBuilder();
        Describe(schema.RootElement, sb);
        return sb.ToString();
    }

    private static void Describe(JsonElement type, StringBuilder sb)
    {
        if (type.ValueKind == JsonValueKind.String)
        {
            sb.Append(type.GetString());
            return;
        }
        switch (type.GetProperty("type").GetString())
        {
            case "struct":
                sb.Append("struct<");
                foreach (var field in type.GetProperty("fields").EnumerateArray())
                {
                    sb.Append(field.GetProperty("name").GetString()).Append(':');
                    Describe(field.GetProperty("type"), sb);
                    sb.Append(field.GetProperty("nullable").GetBoolean() ? "" : " not null");
                    if (field.TryGetProperty("metadata", out var md) && md.TryGetProperty("delta.typeChanges", out var changes))
                    {
                        sb.Append(" changed[");
                        foreach (var c in changes.EnumerateArray())
                        {
                            sb.Append(c.GetProperty("fromType").GetString()).Append("->").Append(c.GetProperty("toType").GetString());
                            if (c.TryGetProperty("fieldPath", out var fieldPath))
                                sb.Append(" at ").Append(fieldPath.GetString());
                            sb.Append(';');
                        }
                        sb.Append(']');
                    }
                    sb.Append(", ");
                }
                sb.Append('>');
                break;
            case "array":
                sb.Append("array<");
                Describe(type.GetProperty("elementType"), sb);
                sb.Append(type.GetProperty("containsNull").GetBoolean() ? "" : " not null");
                sb.Append('>');
                break;
            case "map":
                sb.Append("map<");
                Describe(type.GetProperty("keyType"), sb);
                sb.Append(',');
                Describe(type.GetProperty("valueType"), sb);
                sb.Append(type.GetProperty("valueContainsNull").GetBoolean() ? "" : " not null");
                sb.Append('>');
                break;
        }
    }

    internal static List<string> SparkRows(string dir) =>
        [.. Spark.Invoke("read", new { path = dir }).GetProperty("rows").EnumerateArray()
            .Select(r => r.GetRawText())
            .OrderBy(x => x, StringComparer.Ordinal)];

    internal static List<string> Features(string dir) =>
        [.. Spark.Invoke("read", new { path = dir }).GetProperty("detail").GetProperty("table_features")
            .EnumerateArray().Select(f => f.GetString()!).OrderBy(f => f, StringComparer.Ordinal)];

    public static TheoryData<bool, bool> Cases => new()
    {
        // widen id int -> long, column mapping
        { false, false },
        { true, false },
        { false, true },
        { true, true },
    };

    [SkippableTheory]
    [MemberData(nameof(Cases))]
    public async Task MergeSchema_LeavesWhatSparksSchemaEvolutionLeaves(bool widen, bool columnMapping)
    {
        Spark.Require();
        string idType = widen ? "INT" : "BIGINT";
        CreateAndSeed(_ours, idType, columnMapping);
        CreateAndSeed(_theirs, idType, columnMapping);

        Sql(_theirs,
            "SET spark.databricks.delta.schema.autoMerge.enabled = true",
            "INSERT INTO delta.`{path}` BY NAME SELECT 5000000000L AS id, 'c' AS p, "
            + "named_struct('a', 20, 'b', 'bee') AS s, 'x' AS extra",
            "SET spark.databricks.delta.schema.autoMerge.enabled = false");

        var inner = new StructType([new Field("a", Int32Type.Default, true), new Field("b", StringType.Default, true)]);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, true))
            .Field(new Field("p", StringType.Default, true))
            .Field(new Field("s", inner, true))
            .Field(new Field("extra", StringType.Default, true))
            .Build();
        var s = new StructArray(inner, 1,
            [new Int32Array.Builder().Append(20).Build(), new StringArray.Builder().Append("bee").Build()],
            ArrowBuffer.Empty);
        var batch = new RecordBatch(schema,
            [new Int64Array.Builder().Append(5_000_000_000L).Build(), new StringArray.Builder().Append("c").Build(), s,
             new StringArray.Builder().Append("x").Build()], 1);
        await using (var table = await DeltaTable.OpenAsync(new LocalTableFileSystem(_ours)))
            await table.WriteAsync([batch], mergeSchema: true);

        Assert.Equal(LogicalSchema(_theirs), LogicalSchema(_ours));
        Assert.Equal(Features(_theirs), Features(_ours));
        Assert.Equal(SparkRows(_theirs), SparkRows(_ours));

        // Spark writes through the table EW merged, filling every column.
        Sql(_ours, "INSERT INTO delta.`{path}` VALUES (6, 'd', named_struct('a', 30, 'b', 'sea'), 'y')");
    }

    // ── Inside arrays and maps (#603) ──

    private static void CreateAndSeedCollections(string dir, bool columnMapping, string? constraint = null)
    {
        string properties = columnMapping
            ? "'delta.enableTypeWidening'='true', 'delta.columnMapping.mode'='name'"
            : "'delta.enableTypeWidening'='true'";
        var statements = new List<string>
        {
            "CREATE TABLE delta.`{path}` (id INT, items ARRAY<STRUCT<a: INT>>, xs ARRAY<INT>, m MAP<STRING, INT>) "
            + $"USING delta TBLPROPERTIES ({properties})",
            "INSERT INTO delta.`{path}` VALUES (1, array(named_struct('a', 10)), array(1), map('k', 1))",
        };
        if (constraint is not null)
            statements.Add($"ALTER TABLE delta.`{{path}}` ADD CONSTRAINT c CHECK ({constraint})");
        Sql(dir, [.. statements]);
    }

    /// <summary>The EW batch matching Spark's <c>SELECT 2 AS id, array(named_struct('a', 20, 'b', 'bee')) AS
    /// items, array(5000000000L) AS xs, map('k', 5000000000L) AS m</c>.</summary>
    private static RecordBatch WiderCollections()
    {
        var element = new StructType([new Field("a", Int32Type.Default, true), new Field("b", StringType.Default, true)]);
        var itemsType = new ListType(new Field("element", element, true));
        var xsType = new ListType(new Field("element", Int64Type.Default, true));
        var mType = new MapType(new Field("key", StringType.Default, false), new Field("value", Int64Type.Default, true));
        var oneOffsets = new ArrowBuffer.Builder<int>().Append(0).Append(1).Build();

        var items = new ListArray(itemsType, 1, oneOffsets,
            new StructArray(element, 1,
                [new Int32Array.Builder().Append(20).Build(), new StringArray.Builder().Append("bee").Build()],
                ArrowBuffer.Empty),
            ArrowBuffer.Empty);
        var xs = new ListArray(xsType, 1, oneOffsets, new Int64Array.Builder().Append(5_000_000_000L).Build(),
            ArrowBuffer.Empty);
        var entries = new StructArray(mType.KeyValueType, 1,
            [new StringArray.Builder().Append("k").Build(), new Int64Array.Builder().Append(5_000_000_000L).Build()],
            ArrowBuffer.Empty);
        var m = new MapArray(mType, 1, oneOffsets, entries, ArrowBuffer.Empty);

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int32Type.Default, true))
            .Field(new Field("items", itemsType, true))
            .Field(new Field("xs", xsType, true))
            .Field(new Field("m", mType, true))
            .Build();
        return new RecordBatch(schema, [new Int32Array.Builder().Append(2).Build(), items, xs, m], 1);
    }

    private const string SparkWiderCollections =
        "INSERT INTO delta.`{path}` BY NAME SELECT 2 AS id, array(named_struct('a', 20, 'b', 'bee')) AS items, "
        + "array(5000000000L) AS xs, map('k', 5000000000L) AS m";

    public static TheoryData<bool> ColumnMappingCases => new() { false, true };

    [SkippableTheory]
    [MemberData(nameof(ColumnMappingCases))]
    public async Task MergeSchema_InsideArraysAndMaps_LeavesWhatSparksSchemaEvolutionLeaves(bool columnMapping)
    {
        Spark.Require();
        CreateAndSeedCollections(_ours, columnMapping);
        CreateAndSeedCollections(_theirs, columnMapping);

        Sql(_theirs,
            "SET spark.databricks.delta.schema.autoMerge.enabled = true",
            SparkWiderCollections,
            "SET spark.databricks.delta.schema.autoMerge.enabled = false");

        await using (var table = await DeltaTable.OpenAsync(new LocalTableFileSystem(_ours)))
            await table.WriteAsync([WiderCollections()], mergeSchema: true);

        Assert.Equal(LogicalSchema(_theirs), LogicalSchema(_ours));
        Assert.Equal(Features(_theirs), Features(_ours));
        Assert.Equal(SparkRows(_theirs), SparkRows(_ours));

        // Spark writes through the table EW merged, filling the new field and the wider values.
        Sql(_ours,
            "INSERT INTO delta.`{path}` VALUES (3, array(named_struct('a', 30, 'b', 'sea')), array(6000000000L), "
            + "map('j', 6000000000L))");
    }

    /// <summary>A CHECK constraint that reads the array through its column (<c>xs[0]</c>) keeps the element from
    /// being widened, in Spark (measured: <c>DELTA_CONSTRAINT_DATA_TYPE_MISMATCH</c>, "Column xs.element has data
    /// type INT and cannot be altered to data type BIGINT…") and in EW, with the same error code.</summary>
    [SkippableFact]
    public async Task MergeSchema_WideningAnArrayElementAConstraintReads_IsRefusedAsSparkRefusesIt()
    {
        Spark.Require();
        CreateAndSeedCollections(_ours, columnMapping: false, constraint: "xs[0] > 0");
        CreateAndSeedCollections(_theirs, columnMapping: false, constraint: "xs[0] > 0");

        var spark = Spark.InvokeRaw("sql", new
        {
            path = _theirs,
            sql = new[]
            {
                "SET spark.databricks.delta.schema.autoMerge.enabled = true",
                SparkWiderCollections,
            },
        });
        // The failed INSERT left the shared session's setting on.
        Sql(_theirs, "SET spark.databricks.delta.schema.autoMerge.enabled = false");
        Assert.False(spark.GetProperty("ok").GetBoolean(), spark.ToString());
        string sparkError = spark.GetProperty("error").GetString()!;

        await using var table = await DeltaTable.OpenAsync(new LocalTableFileSystem(_ours));
        long before = table.CurrentSnapshot.Version;
        var ex = await Assert.ThrowsAsync<DeltaFormatException>(async () =>
            await table.WriteAsync([WiderCollections()], mergeSchema: true));

        Assert.Equal(DeltaTableErrorCodes.ConstraintDataTypeMismatch, ex.ErrorCode);
        Assert.Contains($"[{ex.ErrorCode}]", sparkError);
        Assert.Equal(before, table.CurrentSnapshot.Version);
    }
}
