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

    private static JsonElement Sql(string dir, params string[] statements)
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
    private static string LogicalSchema(string dir)
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
                            sb.Append(c.GetProperty("fromType").GetString()).Append("->").Append(c.GetProperty("toType").GetString()).Append(';');
                        sb.Append(']');
                    }
                    sb.Append(", ");
                }
                sb.Append('>');
                break;
            case "array":
                sb.Append("array<");
                Describe(type.GetProperty("elementType"), sb);
                sb.Append('>');
                break;
            case "map":
                sb.Append("map<");
                Describe(type.GetProperty("keyType"), sb);
                sb.Append(',');
                Describe(type.GetProperty("valueType"), sb);
                sb.Append('>');
                break;
        }
    }

    private static List<string> SparkRows(string dir) =>
        [.. Spark.Invoke("read", new { path = dir }).GetProperty("rows").EnumerateArray()
            .Select(r => r.GetRawText())
            .OrderBy(x => x, StringComparer.Ordinal)];

    private static List<string> Features(string dir) =>
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
}
