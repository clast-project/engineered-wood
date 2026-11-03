// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// <c>delta.minReaderVersion</c> / <c>delta.minWriterVersion</c> table properties are a protocol floor and are not
/// stored (#639). The expected protocols are the ones delta-spark 4.0.0 writes for the same
/// <c>CREATE TABLE ... TBLPROPERTIES</c>, measured.
/// </summary>
public class ProtocolFloorPropertyTests : IDisposable
{
    private readonly string _tempDir;

    public ProtocolFloorPropertyTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_floor_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private LocalTableFileSystem Fs() => new(_tempDir);

    // Nullable, as in the Spark measurements: a NOT NULL column is an invariant, which an explicit floor must declare
    // (see Create_NotNullColumn_DeclaresInvariants).
    private static readonly Apache.Arrow.Schema IdSchema = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, true))
        .Build();

    private static RecordBatch Ids(params long[] ids)
    {
        var builder = new Int64Array.Builder();
        foreach (long id in ids)
            builder.Append(id);
        return new RecordBatch(IdSchema, [builder.Build()], ids.Length);
    }

    /// <summary><c>key=value;key=value</c>, as compact as a theory row allows.</summary>
    internal static Dictionary<string, string> Parse(string properties) =>
        properties.Split(new[] { ';' }, StringSplitOptions.RemoveEmptyEntries)
            .Select(pair => pair.Split(new[] { '=' }, 2))
            .ToDictionary(pair => pair[0], pair => pair[1]);

    private static string[]? Split(string? list) =>
        list is null ? null : list.Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries);

    private static void AssertFeatures(string[]? expected, IReadOnlyCollection<string>? actual)
    {
        if (expected is null)
        {
            Assert.Null(actual);
            return;
        }
        Assert.NotNull(actual);
        Assert.Equal(expected.OrderBy(f => f, StringComparer.Ordinal), actual!.OrderBy(f => f, StringComparer.Ordinal));
    }

    private static void AssertNoVersionKeys(MetadataAction metadata)
    {
        foreach (string key in metadata.Configuration?.Keys ?? [])
        {
            Assert.False(string.Equals(key, "delta.minReaderVersion", StringComparison.OrdinalIgnoreCase), key);
            Assert.False(string.Equals(key, "delta.minWriterVersion", StringComparison.OrdinalIgnoreCase), key);
        }
    }

    // Each row is what Spark 4.0.0 / Delta 4.0.0 wrote for CREATE TABLE ... TBLPROPERTIES with these properties.
    [Theory]
    [InlineData("delta.minWriterVersion=1", 1, 1, null, null)]
    [InlineData("delta.minWriterVersion=2", 1, 2, null, null)]
    [InlineData("delta.minWriterVersion=3", 1, 3, null, null)]
    [InlineData("delta.minWriterVersion=4", 1, 4, null, null)]
    [InlineData("delta.minWriterVersion=5", 1, 4, null, null)]
    [InlineData("delta.minWriterVersion=6", 1, 6, null, null)]
    [InlineData("delta.minWriterVersion=7", 1, 1, null, null)]
    [InlineData("delta.minReaderVersion=1", 1, 2, null, null)]
    [InlineData("delta.minReaderVersion=2", 1, 2, null, null)]
    [InlineData("delta.minReaderVersion=3", 1, 2, null, null)]
    [InlineData("delta.minReaderVersion=2;delta.minWriterVersion=4", 1, 4, null, null)]
    [InlineData("delta.minReaderVersion=3;delta.minWriterVersion=4", 1, 4, null, null)]
    [InlineData("delta.minReaderVersion=2;delta.minWriterVersion=5", 2, 5, null, null)]
    [InlineData("delta.minReaderVersion=2;delta.minWriterVersion=7", 1, 1, null, null)]
    [InlineData("delta.minReaderVersion=3;delta.minWriterVersion=7", 1, 1, null, null)]
    [InlineData("DELTA.MINWRITERVERSION=4", 1, 4, null, null)]
    [InlineData("delta.MinWriterVersion=4", 1, 4, null, null)]
    [InlineData("delta.minWriterVersion=4;delta.feature.deletionVectors=supported", 3, 7, "deletionVectors",
        "deletionVectors,checkConstraints,generatedColumns,invariants,changeDataFeed,appendOnly")]
    [InlineData("delta.minWriterVersion=3;delta.feature.changeDataFeed=supported", 1, 7, null,
        "changeDataFeed,checkConstraints,invariants,appendOnly")]
    [InlineData("delta.minWriterVersion=7;delta.feature.checkConstraints=supported", 1, 7, null, "checkConstraints")]
    [InlineData("delta.minWriterVersion=7;delta.enableDeletionVectors=true", 3, 7, "deletionVectors", "deletionVectors")]
    [InlineData("delta.minWriterVersion=4;delta.enableDeletionVectors=true", 3, 7, "deletionVectors",
        "deletionVectors,checkConstraints,generatedColumns,invariants,changeDataFeed,appendOnly")]
    [InlineData("delta.minWriterVersion=2;delta.enableChangeDataFeed=true", 1, 7, null,
        "changeDataFeed,invariants,appendOnly")]
    [InlineData("delta.minWriterVersion=7;delta.columnMapping.mode=name", 2, 7, null, "columnMapping")]
    [InlineData("delta.minReaderVersion=3;delta.columnMapping.mode=name", 2, 7, null,
        "columnMapping,invariants,appendOnly")]
    [InlineData("delta.minWriterVersion=1;delta.appendOnly=true", 1, 7, null, "appendOnly")]
    [InlineData("delta.minWriterVersion=7;delta.appendOnly=true", 1, 7, null, "appendOnly")]
    [InlineData("delta.minWriterVersion=7;delta.appendOnly=false", 1, 1, null, null)]
    [InlineData("delta.minWriterVersion=1;delta.constraints.c=id > 0", 1, 7, null, "checkConstraints")]
    [InlineData("delta.minWriterVersion=7;delta.constraints.c=id > 0", 1, 7, null, "checkConstraints")]
    public async Task Create_RaisesTheProtocolAsSparkDoes(
        string properties, int reader, int writer, string? readerFeatures, string? writerFeatures)
    {
        await using var table = await DeltaTable.CreateAsync(Fs(), IdSchema, configuration: Parse(properties));

        var protocol = table.CurrentSnapshot.Protocol;
        Assert.Equal((reader, writer), (protocol.MinReaderVersion, protocol.MinWriterVersion));
        AssertFeatures(Split(readerFeatures), protocol.ReaderFeatures);
        AssertFeatures(Split(writerFeatures), protocol.WriterFeatures);
        AssertNoVersionKeys(table.CurrentSnapshot.Metadata);

        // And the table is one this library goes on writing.
        await table.WriteAsync([Ids(1)]);
    }

    // Measured on Spark 4.0.0: a NOT NULL column, top-level or inside a struct, is an invariant (Invariants.getFromSchema
    // counts it), so a floor that does not imply invariants gets the feature named. The default floor implies it.
    [Theory]
    [InlineData("7", false, 1, 7, "invariants")]
    [InlineData("1", false, 1, 7, "invariants")]
    [InlineData("7", true, 1, 7, "invariants")]
    [InlineData(null, false, 1, 2, null)]
    public async Task Create_NotNullColumn_DeclaresInvariants(
        string? floor, bool nested, int reader, int writer, string? writerFeatures)
    {
        var field = nested
            ? new Field("s", new Apache.Arrow.Types.StructType([new Field("a", Int64Type.Default, false)]), true)
            : new Field("id", Int64Type.Default, false);
        var schema = new Apache.Arrow.Schema.Builder().Field(field).Build();
        var configuration = floor is null ? null : new Dictionary<string, string> { ["delta.minWriterVersion"] = floor };

        await using var table = await DeltaTable.CreateAsync(Fs(), schema, configuration: configuration);

        var protocol = table.CurrentSnapshot.Protocol;
        Assert.Equal((reader, writer), (protocol.MinReaderVersion, protocol.MinWriterVersion));
        AssertFeatures(Split(writerFeatures), protocol.WriterFeatures);
    }

    /// <summary>
    /// A <c>(1, 6)</c> table does not support <c>columnMapping</c> (it needs reader 2), so moving it to writer 7, by an
    /// ALTER or a REPLACE, lists writer 6's other features and not that one. Measured on Spark 4.0.0 for both.
    /// </summary>
    [Fact]
    public async Task WriterSixFloor_ThroughAlterAndReplace_NeverListsColumnMapping()
    {
        string[] writerSix =
            ["appendOnly", "invariants", "checkConstraints", "changeDataFeed", "generatedColumns", "identityColumns"];
        await using (var created = await DeltaTable.CreateAsync(Fs(), IdSchema, configuration: Parse("delta.minWriterVersion=6")))
        {
            Assert.Equal((1, 6), (created.CurrentSnapshot.Protocol.MinReaderVersion, created.CurrentSnapshot.Protocol.MinWriterVersion));

            // ALTER: a schema change that needs a reader-writer feature.
            await created.AddColumnAsync(new Field("ts", new TimestampType(TimeUnit.Microsecond, (string?)null), true));
            var altered = created.CurrentSnapshot.Protocol;
            AssertFeatures(["timestampNtz"], altered.ReaderFeatures);
            AssertFeatures([.. writerSix, "timestampNtz"], altered.WriterFeatures);
        }

        // REPLACE of a (1, 6) table with one that needs table features.
        var other = Path.Combine(_tempDir, "other");
        Directory.CreateDirectory(other);
        var otherFs = new LocalTableFileSystem(other);
        await using (await DeltaTable.CreateAsync(otherFs, IdSchema, configuration: Parse("delta.minWriterVersion=6")))
        {
        }
        await using var replaced = await DeltaTable.CreateOrReplaceAsync(
            otherFs, IdSchema, [Ids(1)], enableDeletionVectors: true);
        AssertFeatures(["deletionVectors"], replaced.CurrentSnapshot.Protocol.ReaderFeatures);
        AssertFeatures([.. writerSix, "deletionVectors"], replaced.CurrentSnapshot.Protocol.WriterFeatures);
    }

    /// <summary>The other direction, which Spark refuses (see the class remarks on REPLACE): a deletion-vector table
    /// replaced with a writer-6 floor gets writer 6's features, without <c>columnMapping</c>.</summary>
    [Fact]
    public async Task Replace_OfATableFeaturesTable_WithAWriterSixFloor_LeavesOutColumnMapping()
    {
        await using (await DeltaTable.CreateAsync(Fs(), IdSchema, enableDeletionVectors: true))
        {
        }

        await using var table = await DeltaTable.CreateOrReplaceAsync(
            Fs(), IdSchema, [Ids(1)], configuration: Parse("delta.minWriterVersion=6"));

        AssertFeatures(["deletionVectors"], table.CurrentSnapshot.Protocol.ReaderFeatures);
        AssertFeatures(
            ["deletionVectors", "appendOnly", "invariants", "checkConstraints", "changeDataFeed", "generatedColumns",
                "identityColumns"],
            table.CurrentSnapshot.Protocol.WriterFeatures);
    }

    [Theory]
    [InlineData("delta.minWriterVersion", "8")]
    [InlineData("delta.minWriterVersion", "0")]
    [InlineData("delta.minWriterVersion", "-1")]
    [InlineData("delta.minWriterVersion", "abc")]
    [InlineData("delta.minWriterVersion", "4.0")]
    [InlineData("delta.minWriterVersion", " 4")]
    [InlineData("delta.minReaderVersion", "4")]
    [InlineData("delta.minReaderVersion", "x")]
    public async Task Create_RefusesAVersionSparkRefuses(string key, string value)
    {
        var ex = await Assert.ThrowsAsync<DeltaFormatException>(async () =>
            await DeltaTable.CreateAsync(
                Fs(), IdSchema, configuration: new Dictionary<string, string> { [key] = value }));

        Assert.Equal(DeltaTableErrorCodes.InvalidProtocolVersionProperty, ex.ErrorCode);
        Assert.Equal(-1, await new TransactionLog(Fs()).GetLatestVersionAsync());
    }

    [Fact]
    public async Task Create_WithoutAFloor_IsUnchanged()
    {
        await using var table = await DeltaTable.CreateAsync(
            Fs(), IdSchema, configuration: new Dictionary<string, string> { ["delta.constraints.c"] = "id > 0" });

        var protocol = table.CurrentSnapshot.Protocol;
        Assert.Equal((1, 3), (protocol.MinReaderVersion, protocol.MinWriterVersion));
        Assert.Null(protocol.WriterFeatures);
    }

    [Fact]
    public async Task Create_ColumnMappingArgumentWithAFloor_NamesTheFeature()
    {
        await using var table = await DeltaTable.CreateAsync(
            Fs(), IdSchema, columnMappingMode: ColumnMappingMode.Name,
            configuration: new Dictionary<string, string> { ["delta.minWriterVersion"] = "7" });

        var protocol = table.CurrentSnapshot.Protocol;
        Assert.Equal((2, 7), (protocol.MinReaderVersion, protocol.MinWriterVersion));
        AssertFeatures(["columnMapping"], protocol.WriterFeatures);
        await table.WriteAsync([Ids(1)]);
    }

    [Fact]
    public async Task Replace_RaisesALegacyTable()
    {
        await using (await DeltaTable.CreateAsync(Fs(), IdSchema))
        {
        }

        await using var table = await DeltaTable.CreateOrReplaceAsync(
            Fs(), IdSchema, [Ids(1)], configuration: Parse("delta.minWriterVersion=4"));

        var protocol = table.CurrentSnapshot.Protocol;
        Assert.Equal((1, 4), (protocol.MinReaderVersion, protocol.MinWriterVersion));
        AssertNoVersionKeys(table.CurrentSnapshot.Metadata);
    }

    [Fact]
    public async Task Replace_WithALowerFloor_KeepsTheProtocol()
    {
        await using (await DeltaTable.CreateAsync(Fs(), IdSchema, configuration: Parse("delta.minWriterVersion=4")))
        {
        }

        await using var table = await DeltaTable.CreateOrReplaceAsync(
            Fs(), IdSchema, [Ids(1)], configuration: Parse("delta.minWriterVersion=3"));

        var protocol = table.CurrentSnapshot.Protocol;
        Assert.Equal((1, 4), (protocol.MinReaderVersion, protocol.MinWriterVersion));
    }

    /// <summary>
    /// Spark 4.0.0 refuses this one (<c>DELTA_READ_FEATURE_PROTOCOL_REQUIRES_WRITE</c>) when it merges a legacy writer
    /// floor into a reader-3 table. This library accepts it: the merged protocol never downgrades and lists what the
    /// floor implies, which is a valid protocol for the request.
    /// </summary>
    [Fact]
    public async Task Replace_OfATableFeaturesTable_WithALegacyFloor_AddsWhatTheFloorImplies()
    {
        await using (await DeltaTable.CreateAsync(Fs(), IdSchema, enableDeletionVectors: true))
        {
        }

        await using var table = await DeltaTable.CreateOrReplaceAsync(
            Fs(), IdSchema, [Ids(1)], configuration: Parse("delta.minWriterVersion=4"));

        var protocol = table.CurrentSnapshot.Protocol;
        Assert.Equal((3, 7), (protocol.MinReaderVersion, protocol.MinWriterVersion));
        AssertFeatures(["deletionVectors"], protocol.ReaderFeatures);
        AssertFeatures(
            ["deletionVectors", "checkConstraints", "generatedColumns", "invariants", "changeDataFeed", "appendOnly"],
            protocol.WriterFeatures);
    }

    [Fact]
    public async Task CallerMetadata_IntroducingAVersionProperty_IsRefused()
    {
        await using var table = await DeltaTable.CreateAsync(Fs(), IdSchema);
        var withKey = table.CurrentSnapshot.Metadata with
        {
            Configuration = new Dictionary<string, string> { ["delta.minWriterVersion"] = "4" },
        };

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(async () =>
            await table.CommitDataFilesAsync(System.Array.Empty<WrittenDataFile>(), extraActions: [withKey]));
        Assert.Equal(DeltaTableErrorCodes.UnsupportedFeaturesInConfig, ex.ErrorCode);
        Assert.Equal(0, table.CurrentSnapshot.Version);
    }
}
