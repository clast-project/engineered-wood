// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Reflection;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// <see cref="DeltaTableErrorCodes"/> — that the table layer's failures are distinguishable without
/// reading the message, and that its codes live in the same flat namespace as the log layer's.
/// </summary>
public class DeltaTableErrorCodeTests : IDisposable
{
    private readonly string _tempDir;

    public DeltaTableErrorCodeTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_tblerr_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private static Apache.Arrow.Schema RegionIdSchema { get; } = new Apache.Arrow.Schema.Builder()
        .Field(new Field("region", StringType.Default, false))
        .Field(new Field("id", Int64Type.Default, false))
        .Build();

    private static RecordBatch Rows(params (string Region, long Id)[] rows)
    {
        var region = new StringArray.Builder();
        var id = new Int64Array.Builder();
        foreach (var (r, i) in rows) { region.Append(r); id.Append(i); }
        return new RecordBatch(RegionIdSchema, [region.Build(), id.Build()], rows.Length);
    }

    private LocalTableFileSystem Fs() => new(_tempDir);

    // Checkpointing off: these tests care about the rejection, not the log layout.
    private static DeltaTableOptions Options => new() { CheckpointInterval = 0 };

    // ── Write-mode misuse: four sites, one code ──

    [Fact]
    public async Task DynamicPartitionOverwrite_OnUnpartitionedTable_IsInvalidWriteMode()
    {
        await using var table = await DeltaTable.CreateAsync(Fs(), RegionIdSchema, Options);
        await table.WriteAsync([Rows(("us", 1))]);

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(
            async () => await table.DynamicOverwriteAsync([Rows(("us", 2))]));

        Assert.Equal(DeltaTableErrorCodes.InvalidWriteMode, ex.ErrorCode);
    }

    [Fact]
    public async Task OverwritePartitions_WithANonPartitionKey_IsInvalidPartitionColumn()
    {
        await using var table = await DeltaTable.CreateAsync(
            Fs(), RegionIdSchema, Options, partitionColumns: ["region"]);
        await table.WriteAsync([Rows(("us", 1))]);

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(
            async () => await table.OverwritePartitionsAsync(
                [Rows(("us", 2))],
                new Dictionary<string, string> { ["id"] = "1" }));

        // Not ColumnNotFound: `id` IS a column, it is just not a PARTITION column, and the fix differs.
        Assert.Equal(DeltaTableErrorCodes.InvalidPartitionColumn, ex.ErrorCode);
        Assert.NotEqual(DeltaTableErrorCodes.ColumnNotFound, ex.ErrorCode);
    }

    [Fact]
    public async Task OverwritePartitions_WithDataOutsideTheTarget_IsItsOwnCode()
    {
        await using var table = await DeltaTable.CreateAsync(
            Fs(), RegionIdSchema, Options, partitionColumns: ["region"]);
        await table.WriteAsync([Rows(("us", 1))]);

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(
            async () => await table.OverwritePartitionsAsync(
                [Rows(("eu", 9))],   // eu is not the partition being replaced
                new Dictionary<string, string> { ["region"] = "us" }));

        // A DATA problem, not a configuration one — the request was well-formed.
        Assert.Equal(DeltaTableErrorCodes.DataOutsideTargetPartitions, ex.ErrorCode);
    }

    // ── Table configuration ──

    [Fact]
    public async Task ClusteringPlusPartitioning_IsMutuallyExclusive()
    {
        var ex = await Assert.ThrowsAsync<DeltaFormatException>(
            async () => await DeltaTable.CreateAsync(
                Fs(), RegionIdSchema, Options,
                partitionColumns: ["region"],
                clusteringColumns: ["id"]));

        Assert.Equal(DeltaTableErrorCodes.ClusteringWithPartitioning, ex.ErrorCode);
    }

    [Fact]
    public async Task ClusteringOnAnUnknownColumn_IsColumnNotFound()
    {
        var ex = await Assert.ThrowsAsync<DeltaFormatException>(
            async () => await DeltaTable.CreateAsync(
                Fs(), RegionIdSchema, Options, clusteringColumns: ["nosuchcolumn"]));

        Assert.Equal(DeltaTableErrorCodes.ColumnNotFound, ex.ErrorCode);
    }

    // ── Guards on the code set ──

    private static List<(string Name, string Value)> DeclaredCodes() =>
        typeof(DeltaTableErrorCodes)
            .GetFields(BindingFlags.Public | BindingFlags.Static)
            .Where(f => f.IsLiteral && f.FieldType == typeof(string))
            .Select(f => (f.Name, (string)f.GetRawConstantValue()!))
            .ToList();

    [Fact]
    public void EveryCodeIsUniqueAndWellFormed()
    {
        var codes = DeclaredCodes();
        Assert.NotEmpty(codes);
        Assert.Empty(codes.GroupBy(c => c.Value).Where(g => g.Count() > 1).Select(g => g.Key));

        foreach (var (name, value) in codes)
        {
            Assert.True(
                System.Text.RegularExpressions.Regex.IsMatch(value, "^DELTA_[A-Z0-9_]+$"),
                $"{name} = \"{value}\" is not in DELTA_SCREAMING_SNAKE form.");
        }
    }

    /// <summary>
    /// The two layers share ONE flat namespace of values — that is the whole reason these are strings
    /// rather than an enum. A value defined in both would make a consumer's single switch ambiguous,
    /// and nothing else would catch it because the two classes are compiled separately.
    /// </summary>
    [Fact]
    public void TableCodesDoNotCollideWithLogLayerCodes()
    {
        var logCodes = typeof(DeltaErrorCodes)
            .GetFields(BindingFlags.Public | BindingFlags.Static)
            .Where(f => f.IsLiteral && f.FieldType == typeof(string))
            .Select(f => (string)f.GetRawConstantValue()!)
            .ToHashSet(StringComparer.Ordinal);

        var collisions = DeclaredCodes()
            .Where(c => logCodes.Contains(c.Value))
            .Select(c => c.Value)
            .ToList();

        Assert.True(collisions.Count == 0,
            "these values are defined in both layers: " + string.Join(", ", collisions));
    }

    /// <summary>
    /// The table layer's half of the same guard the log layer carries: a new
    /// <c>throw new DeltaFormatException(message)</c> must not compile, ship, and quietly return a
    /// consumer to matching on prose.
    /// </summary>
    [Fact]
    public void NoTableLayerThrowSiteOmitsItsCode()
    {
        string? root = FindRepoRoot();
        if (root is null)
            return; // sources are not laid out beside the binaries in this run

        string sourceDir = Path.Combine(root, "src", "EngineeredWood.DeltaLake.Table");
        Assert.True(Directory.Exists(sourceDir), $"expected sources at {sourceDir}");

        var offenders = new List<string>();
        foreach (string file in Directory.EnumerateFiles(sourceDir, "*.cs", SearchOption.AllDirectories))
        {
            if (file.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}")) continue;

            string text = File.ReadAllText(file);
            const string marker = "new DeltaFormatException(";
            for (int i = text.IndexOf(marker, StringComparison.Ordinal); i >= 0;
                 i = text.IndexOf(marker, i + 1, StringComparison.Ordinal))
            {
                string window = text.Substring(i, Math.Min(160, text.Length - i));
                // Either layer's codes are acceptable; the table layer may raise a log-layer condition.
                if (window.IndexOf("DeltaTableErrorCodes.", StringComparison.Ordinal) < 0 &&
                    window.IndexOf("DeltaErrorCodes.", StringComparison.Ordinal) < 0)
                {
                    int line = text.Take(i).Count(c => c == '\n') + 1;
                    offenders.Add($"{Path.GetFileName(file)}:{line}");
                }
            }
        }

        Assert.True(offenders.Count == 0,
            "these throw sites carry no error code: " + string.Join(", ", offenders));
    }

    private static string? FindRepoRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null)
        {
            if (File.Exists(Path.Combine(dir.FullName, "engineered-wood.slnx")))
                return dir.FullName;
            dir = dir.Parent;
        }
        return null;
    }
}
