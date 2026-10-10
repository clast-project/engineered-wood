// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.ChangeDataFeed;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// Issue #222: <see cref="DeltaTableOptions.ChangeFeedSchemaFromVersionChecksum"/> lets a change feed under
/// column mapping take its end version's schema from that version's checksum instead of a replay. The checksum
/// is TAMPERED in most of these tests — a column renamed in its recorded schema only — because that is the one
/// way to tell from the outside which source the feed's schema came from. It is also the hazard the option is
/// off by default for.
/// </summary>
public class CdfSchemaFromChecksumTests : IDisposable
{
    private readonly string _tempDir;

    public CdfSchemaFromChecksumTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_cdfcrc_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    private static readonly string[] FeedColumns =
        [CdfConfig.ChangeTypeColumn, CdfConfig.CommitVersionColumn, CdfConfig.CommitTimestampColumn];

    private static readonly Apache.Arrow.Schema IdValue = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("value", StringType.Default, true))
        .Build();

    private string ChecksumFile(long version) =>
        Path.Combine(_tempDir, DeltaVersion.ChecksumPath(version).Replace('/', Path.DirectorySeparatorChar));

    /// <summary>v1 writes (1, "a"), v2 renames <c>value</c> to <c>label</c>, so v1 is a past end version.</summary>
    private async Task<long> BuildHistoryAsync(ColumnMappingMode mode)
    {
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), IdValue, columnMappingMode: mode,
            configuration: new Dictionary<string, string> { [CdfConfig.EnableKey] = "true" });
        long v1 = await table.WriteAsync([new RecordBatch(IdValue,
            [new Int64Array.Builder().Append(1).Build(), new StringArray.Builder().Append("a").Build()], 1)]);
        if (mode != ColumnMappingMode.None)
            await table.RenameColumnAsync("value", "label");
        else
            await table.WriteAsync([new RecordBatch(IdValue,
                [new Int64Array.Builder().Append(2).Build(), new StringArray.Builder().Append("b").Build()], 1)]);
        Assert.True(File.Exists(ChecksumFile(v1)), "the writer should have left a checksum at v1");
        return v1;
    }

    /// <summary>Rewrites the checksum at <paramref name="version"/> with <c>value</c> renamed in its schema.</summary>
    private void TamperChecksumSchema(long version)
    {
        string path = ChecksumFile(version);
        var checksum = VersionChecksumSerializer.Deserialize(File.ReadAllBytes(path), version);
        string schema = checksum.Metadata.SchemaString;
        string tampered = schema.Replace("\"name\":\"value\"", "\"name\":\"tampered\"");
        Assert.NotEqual(schema, tampered);
        File.WriteAllBytes(path, VersionChecksumSerializer.Serialize(
            checksum with { Metadata = checksum.Metadata with { SchemaString = tampered } }));
    }

    private async Task<RecordBatch> ReadOneVersionAsync(long version, bool fromChecksum)
    {
        await using var table = await DeltaTable.OpenAsync(
            new LocalTableFileSystem(_tempDir),
            new DeltaTableOptions { ChangeFeedSchemaFromVersionChecksum = fromChecksum });
        var list = new List<RecordBatch>();
        await foreach (var b in table.ReadChangesAsync(
            new DeltaChangeReadOptions { StartVersion = version, EndVersion = version }))
        {
            list.Add(b);
        }
        return Assert.Single(list);
    }

    private static string[] Names(RecordBatch b) => b.Schema.FieldsList.Select(f => f.Name).ToArray();

    [Fact]
    public async Task OptedIn_SchemaComesFromTheChecksum()
    {
        long v1 = await BuildHistoryAsync(ColumnMappingMode.Name);
        TamperChecksumSchema(v1);

        var batch = await ReadOneVersionAsync(v1, fromChecksum: true);

        // The physical column is the same one — only its logical name differs — so the data still lands.
        Assert.Equal(["id", "tampered", .. FeedColumns], Names(batch));
        Assert.Equal("a", ((StringArray)batch.Column(1)).GetString(0));
    }

    [Fact]
    public async Task ByDefault_TheChecksumIsNotConsulted()
    {
        long v1 = await BuildHistoryAsync(ColumnMappingMode.Name);
        TamperChecksumSchema(v1);

        var batch = await ReadOneVersionAsync(v1, fromChecksum: false);

        Assert.Equal(["id", "value", .. FeedColumns], Names(batch));
        Assert.Equal("a", ((StringArray)batch.Column(1)).GetString(0));
    }

    [Fact]
    public async Task OptedIn_AnHonestChecksumGivesTheReplaysAnswer()
    {
        long v1 = await BuildHistoryAsync(ColumnMappingMode.Id);

        var fromChecksum = await ReadOneVersionAsync(v1, fromChecksum: true);
        var fromReplay = await ReadOneVersionAsync(v1, fromChecksum: false);

        Assert.Equal(["id", "value", .. FeedColumns], Names(fromChecksum));
        Assert.Equal(Names(fromReplay), Names(fromChecksum));
        Assert.Equal("a", ((StringArray)fromChecksum.Column(1)).GetString(0));
    }

    [Theory]
    [InlineData("missing")]
    [InlineData("garbage")]
    [InlineData("unparseable-schema")]
    [InlineData("unknown-mapping-mode")]
    public async Task OptedIn_AnUnusableChecksumFallsBackToTheReplay(string damage)
    {
        long v1 = await BuildHistoryAsync(ColumnMappingMode.Name);
        string path = ChecksumFile(v1);
        VersionChecksum checksum;
        switch (damage)
        {
            case "missing":
                File.Delete(path);
                break;
            case "garbage":
                File.WriteAllText(path, "{ not json");
                break;
            case "unparseable-schema":
                checksum = VersionChecksumSerializer.Deserialize(File.ReadAllBytes(path), v1);
                File.WriteAllBytes(path, VersionChecksumSerializer.Serialize(
                    checksum with { Metadata = checksum.Metadata with { SchemaString = "{\"type\":" } }));
                break;
            case "unknown-mapping-mode":
                // Schema and protocol intact, so the snapshot builds; only the reader would trip on this.
                checksum = VersionChecksumSerializer.Deserialize(File.ReadAllBytes(path), v1);
                var configuration = checksum.Metadata.Configuration!.ToDictionary(kv => kv.Key, kv => kv.Value);
                configuration[ColumnMapping.ModeKey] = "bogus";
                File.WriteAllBytes(path, VersionChecksumSerializer.Serialize(
                    checksum with { Metadata = checksum.Metadata with { Configuration = configuration } }));
                break;
        }

        var batch = await ReadOneVersionAsync(v1, fromChecksum: true);

        Assert.Equal(["id", "value", .. FeedColumns], Names(batch));
        Assert.Equal("a", ((StringArray)batch.Column(1)).GetString(0));
    }

    [Fact]
    public async Task OptedIn_WithoutColumnMapping_TheLatestSchemaIsUsedAndTheChecksumIsNot()
    {
        long v1 = await BuildHistoryAsync(ColumnMappingMode.None);
        TamperChecksumSchema(v1);

        var batch = await ReadOneVersionAsync(v1, fromChecksum: true);

        Assert.Equal(["id", "value", .. FeedColumns], Names(batch));
    }
}
