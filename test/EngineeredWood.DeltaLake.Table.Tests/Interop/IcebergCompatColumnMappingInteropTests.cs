// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests.Interop;

/// <summary>
/// <b>Tier 3.</b> #757: Spark and EW each create an IcebergCompat table with <c>delta.columnMapping.mode</c> left
/// out, and both turn on <c>name</c> mode. The two tables must agree on protocol, table features and configuration
/// keys, both must carry physical names, and Spark must write through EW's. V1/V2 run on every lane; V3 needs
/// delta-spark 4.4 (4.3.1 declares V3 but auto-sets nothing), and on 4.4 Spark's own V3 checks run on the insert.
/// </summary>
[Collection("Interop")]
public class IcebergCompatColumnMappingInteropTests : IDisposable
{
    private const string V3Conf = "spark.databricks.delta.uniform.iceberg.v3.enabled";

    private readonly string _sparkDir;
    private readonly string _ewDir;

    public IcebergCompatColumnMappingInteropTests()
    {
        string root = Path.Combine(Path.GetTempPath(), $"delta_iceberg_cm_interop_{Guid.NewGuid():N}");
        _sparkDir = Path.Combine(root, "spark");
        _ewDir = Path.Combine(root, "ew");
        Directory.CreateDirectory(_sparkDir);
        Directory.CreateDirectory(_ewDir);
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        string root = Path.GetDirectoryName(_sparkDir)!;
        if (Directory.Exists(root))
        {
            try { Directory.Delete(root, recursive: true); } catch { }
        }
    }

    private static readonly Apache.Arrow.Schema IdS = new Apache.Arrow.Schema.Builder()
        .Field(new Apache.Arrow.Field("id", Apache.Arrow.Types.Int64Type.Default, true))
        .Field(new Apache.Arrow.Field("s", Apache.Arrow.Types.StringType.Default, true))
        .Build();

    private static void RequireDeltaSpark(int minMinor)
    {
        Spark.Require();
        // Spark.Version is "<pyspark>/<delta-spark>", e.g. "4.2.0/4.4.1".
        string version = Spark.Version ?? "";
        int slash = version.IndexOf('/');
        var parts = slash < 0 ? [] : version.Substring(slash + 1).Trim().Split('.');
        bool atLeast = parts.Length >= 2 && int.TryParse(parts[0], out int major)
            && int.TryParse(parts[1], out int minor) && (major > 4 || (major == 4 && minor >= minMinor));
        Skip.IfNot(atLeast, $"needs delta-spark 4.{minMinor}+; resolved {version}");
    }

    private static JsonElement Sql(string path, params string[] statements) =>
        Spark.Invoke("sql", new
        {
            path,
            sql = statements,
            confs = new Dictionary<string, string> { [V3Conf] = "true" },
        });

    private static List<string> Features(JsonElement result) =>
        [.. result.GetProperty("detail").GetProperty("table_features").EnumerateArray().Select(f => f.GetString()!)];

    private static Dictionary<string, string> Properties(JsonElement result) =>
        result.GetProperty("detail").GetProperty("properties").EnumerateObject()
            .ToDictionary(p => p.Name, p => p.Value.GetString()!);

    private static void AssertPhysicalNames(DeltaTable table)
    {
        foreach (var field in table.CurrentSnapshot.Schema.Fields)
        {
            Assert.True(field.Metadata!.ContainsKey(ColumnMapping.FieldIdKey));
            Assert.NotEqual(field.Name, ColumnMapping.GetPhysicalName(field, ColumnMappingMode.Name));
        }
    }

    [SkippableTheory]
    [InlineData("delta.enableIcebergCompatV1", 0)]
    [InlineData("delta.enableIcebergCompatV2", 0)]
    [InlineData("delta.enableIcebergCompatV3", 4)]
    public async Task CreateWithTheModeLeftOut_SparkAndEwBothTurnOnNameMode(string enableKey, int minMinor)
    {
        RequireDeltaSpark(minMinor);

        var spark = Sql(_sparkDir,
            $"CREATE TABLE delta.`{{path}}` (id BIGINT, s STRING) USING delta TBLPROPERTIES ('{enableKey}' = 'true')");
        var sparkProperties = Properties(spark);
        Assert.Equal("name", sparkProperties[ColumnMapping.ModeKey]);

        await using (var sparkTable = await DeltaTable.OpenAsync(new LocalTableFileSystem(_sparkDir)))
            AssertPhysicalNames(sparkTable);

        await using (var ewTable = await DeltaTable.CreateAsync(new LocalTableFileSystem(_ewDir), IdS,
            configuration: new Dictionary<string, string> { [enableKey] = "true" }))
        {
            AssertPhysicalNames(ewTable);
        }

        // Spark writes through EW's table (on 4.4 its IcebergCompat checks run on this commit) and reads it back.
        var ew = Sql(_ewDir, "INSERT INTO delta.`{path}` VALUES (1, 'a'), (2, 'b')");
        Assert.Equal(2, ew.GetProperty("row_count").GetInt32());

        // EW declares every feature Spark does. Beyond them it names exactly the ones writer v5 implies, which is the
        // known, documented difference in column mapping's protocol shape (doc/known-issues.md, "Column-mapping
        // protocol shape differs from Spark's"), not something #757 changes: it is the same with an explicit mode.
        var sparkFeatures = Features(spark);
        var ewFeatures = Features(ew);
        Assert.Empty(sparkFeatures.Except(ewFeatures));
        Assert.Empty(ewFeatures.Except(sparkFeatures)
            .Except(["changeDataFeed", "checkConstraints", "generatedColumns"]));
        Assert.Equal(7, ew.GetProperty("detail").GetProperty("min_writer_version").GetInt32());

        // delta-spark 4.3+ also writes delta.universalFormat.iceberg.atomicConversion.supported, which matters only to
        // a UniForm converter; EW does not write it (#745, doc/known-issues.md).
        var ewProperties = Properties(ew);
        Assert.Equal(sparkProperties.Keys
                .Where(k => k != "delta.universalFormat.iceberg.atomicConversion.supported")
                .OrderBy(k => k, StringComparer.Ordinal),
            ewProperties.Keys.OrderBy(k => k, StringComparer.Ordinal));
        Assert.Equal("name", ewProperties[ColumnMapping.ModeKey]);
        Assert.Equal(sparkProperties[ColumnMapping.MaxColumnIdKey], ewProperties[ColumnMapping.MaxColumnIdKey]);
    }
}
