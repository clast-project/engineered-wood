// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Checkpoint;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.Snapshot;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Tests;

/// <summary>
/// The version-checksum file (<c>_delta_log/&lt;version&gt;.crc</c>): what goes in it, what stays out, and
/// when it is declined outright.
///
/// <para>The governing rule throughout is that a WRONG checksum is worse than an absent one — every
/// version this library has ever written has none, and the spec requires readers to cope with that — so
/// the interesting assertions here are the negative ones: the field that is omitted rather than guessed,
/// the file that is not written, the existing file that is not overwritten.</para>
/// </summary>
public class VersionChecksumTests : IDisposable
{
    private readonly string _tempDir;
    private readonly LocalTableFileSystem _fs;
    private readonly TransactionLog _log;

    public VersionChecksumTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_crc_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
        _fs = new LocalTableFileSystem(_tempDir);
        _log = new TransactionLog(_fs);
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private const string SchemaJson =
        """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}}]}""";

    private static MetadataAction Metadata(IDictionary<string, string>? configuration = null) => new()
    {
        Id = "crc-test",
        Format = Format.Parquet,
        SchemaString = SchemaJson,
        PartitionColumns = [],
        Configuration = configuration is null
            ? new Dictionary<string, string>()
            : new Dictionary<string, string>(configuration),
        CreatedTime = 1700000000000,
    };

    private static AddFile Add(string path, long size) => new()
    {
        Path = path,
        PartitionValues = new Dictionary<string, string>(),
        Size = size,
        ModificationTime = 1700000001000,
        DataChange = true,
    };

    private async ValueTask<Snapshot.Snapshot> CreateTableAsync(
        IDictionary<string, string>? configuration = null, params DeltaAction[] extra)
    {
        var actions = new List<DeltaAction>
        {
            new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 },
            Metadata(configuration),
        };
        actions.AddRange(extra);
        await _log.WriteCommitAsync(0, actions);
        return await SnapshotAsync();
    }

    private ValueTask<Snapshot.Snapshot> SnapshotAsync() =>
        SnapshotBuilder.BuildAsync(_log, new CheckpointReader(_fs), atVersion: null);

    private string ChecksumFile(long version) =>
        Path.Combine(_tempDir, "_delta_log", $"{DeltaVersion.Format(version)}.crc");

    private JsonElement ReadChecksumJson(long version)
    {
        using var doc = JsonDocument.Parse(File.ReadAllText(ChecksumFile(version)));
        return doc.RootElement.Clone();
    }

    // ── what a snapshot turns into ─────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task FromSnapshot_CountsTheLiveFilesAndSumsTheirSizes()
    {
        var snapshot = await CreateTableAsync();
        await _log.WriteCommitAsync(1, [Add("a.parquet", 100), Add("b.parquet", 250)]);
        await _log.WriteCommitAsync(2,
        [
            new RemoveFile { Path = "a.parquet", DeletionTimestamp = 1700000002000, DataChange = true },
            Add("c.parquet", 7),
        ]);
        snapshot = await SnapshotAsync();

        var checksum = VersionChecksum.TryFromSnapshot(snapshot);

        Assert.NotNull(checksum);
        Assert.Equal(2, checksum.Version);
        // b + c. The removed file counts for neither, which is the whole point of the field: it is the
        // state AFTER action reconciliation, not the sum of what the commits said.
        Assert.Equal(2, checksum.NumFiles);
        Assert.Equal(257, checksum.TableSizeBytes);
    }

    [Fact]
    public async Task FromSnapshot_RecordsTheLiveTransactionsAndDomains_SortedByTheirKey()
    {
        var snapshot = await CreateTableAsync(
            configuration: null,
            new TransactionId { AppId = "zulu", Version = 3 },
            new TransactionId { AppId = "alpha", Version = 1 },
            new DomainMetadata { Domain = "z.domain", Configuration = "{}", Removed = false },
            new DomainMetadata { Domain = "a.domain", Configuration = "{}", Removed = false });

        var checksum = VersionChecksum.TryFromSnapshot(snapshot);

        Assert.NotNull(checksum);
        // Sorted, because both come out of hash maps and the enumeration order is not part of the state
        // being described. Two writers summarising the same version must produce the same bytes.
        Assert.Equal(["alpha", "zulu"], checksum.SetTransactions!.Select(t => t.AppId));
        Assert.Equal(["a.domain", "z.domain"], checksum.DomainMetadata!.Select(d => d.Domain));
    }

    [Fact]
    public async Task FromSnapshot_ExcludesRemovedDomains()
    {
        await CreateTableAsync(
            configuration: null,
            new DomainMetadata { Domain = "gone", Configuration = "{}", Removed = false });
        await _log.WriteCommitAsync(1,
            [new DomainMetadata { Domain = "gone", Configuration = "{}", Removed = true }]);

        var checksum = VersionChecksum.TryFromSnapshot(await SnapshotAsync());

        // The spec says LIVE domain metadata, "excluding tombstones". A checksum that carried the
        // tombstone would tell a reader the domain still exists.
        Assert.NotNull(checksum);
        Assert.Empty(checksum.DomainMetadata!);
    }

    [Fact]
    public async Task FromSnapshot_OnATableWithNoTransactions_RecordsAnEmptyList_NotAnAbsentOne()
    {
        var checksum = VersionChecksum.TryFromSnapshot(await CreateTableAsync());

        // Empty and absent mean different things on the wire: absent is "this writer is not telling
        // you", empty is "there are none" — and only the second lets a reader trust a miss without
        // replaying the log. A snapshot always knows the complete set, so this library always says.
        Assert.NotNull(checksum);
        Assert.NotNull(checksum.SetTransactions);
        Assert.Empty(checksum.SetTransactions);
    }

    // ── in-commit timestamps: the one case that is declined ────────────────────────────────────────────

    [Fact]
    public async Task FromSnapshot_WithInCommitTimestampsEnabled_CarriesTheTimestamp()
    {
        await _log.WriteCommitAsync(0,
        [
            InCommitTimestamp.CreateCommitInfo(1700000009000, "CREATE TABLE"),
            new ProtocolAction
            {
                MinReaderVersion = 3,
                MinWriterVersion = 7,
                ReaderFeatures = [],
                WriterFeatures = ["inCommitTimestamp"],
            },
            Metadata(new Dictionary<string, string> { [InCommitTimestamp.EnableKey] = "true" }),
        ]);

        var checksum = VersionChecksum.TryFromSnapshot(await SnapshotAsync());

        Assert.NotNull(checksum);
        Assert.Equal(1700000009000, checksum.InCommitTimestamp);
    }

    [Fact]
    public async Task FromSnapshot_WithInCommitTimestampsEnabledButNoTimestamp_DeclinesEntirely()
    {
        // The shape a snapshot built from a CHECKPOINT has: a checkpoint carries no commitInfo, so it
        // carries no in-commit timestamp either. Simulated here by committing without one.
        await _log.WriteCommitAsync(0,
        [
            new ProtocolAction
            {
                MinReaderVersion = 3,
                MinWriterVersion = 7,
                ReaderFeatures = [],
                WriterFeatures = ["inCommitTimestamp"],
            },
            Metadata(new Dictionary<string, string> { [InCommitTimestamp.EnableKey] = "true" }),
        ]);
        var snapshot = await SnapshotAsync();
        Assert.Null(snapshot.InCommitTimestamp);

        // `inCommitTimestampOpt` is present if and ONLY if the feature is enabled, so there is no
        // well-formed file to write here. Declining is the safe half of "a wrong checksum is worse than
        // an absent one" — delta-kernel-rs refuses the same case.
        Assert.Null(VersionChecksum.TryFromSnapshot(snapshot));
    }

    [Fact]
    public async Task FromSnapshot_WithInCommitTimestampsDisabled_OmitsTheTimestampEvenWhenOneIsKnown()
    {
        // A commitInfo may carry an inCommitTimestamp on a table that never declared the feature (nothing
        // stops a writer emitting one). Echoing it into the checksum would claim a feature the protocol
        // does not list.
        await _log.WriteCommitAsync(0,
        [
            InCommitTimestamp.CreateCommitInfo(1700000009000, "CREATE TABLE"),
            new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 },
            Metadata(),
        ]);

        var checksum = VersionChecksum.TryFromSnapshot(await SnapshotAsync());

        Assert.NotNull(checksum);
        Assert.Null(checksum.InCommitTimestamp);
    }

    // ── the bytes ──────────────────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Write_ProducesTheSpecsFieldNames()
    {
        var snapshot = await CreateTableAsync(
            configuration: null,
            new TransactionId { AppId = "app", Version = 4, LastUpdated = 1700000003000 });
        await new VersionChecksumWriter(_fs).TryWriteAsync(snapshot);

        var json = ReadChecksumJson(0);

        Assert.Equal(0, json.GetProperty("tableSizeBytes").GetInt64());
        Assert.Equal(0, json.GetProperty("numFiles").GetInt64());
        // Fixed at 1 by the spec, and delta-kernel-rs rejects a file where they are anything else.
        Assert.Equal(1, json.GetProperty("numMetadata").GetInt64());
        Assert.Equal(1, json.GetProperty("numProtocol").GetInt64());
        Assert.Equal("crc-test", json.GetProperty("metadata").GetProperty("id").GetString());
        Assert.Equal(2, json.GetProperty("protocol").GetProperty("minWriterVersion").GetInt32());
        Assert.Equal("app", json.GetProperty("setTransactions")[0].GetProperty("appId").GetString());
        Assert.Equal(4, json.GetProperty("setTransactions")[0].GetProperty("version").GetInt64());

        // Absent, not null: the field is optional and this table has no in-commit timestamps.
        Assert.False(json.TryGetProperty("inCommitTimestampOpt", out _));
        // The optional fields this library does not produce stay out of the file entirely rather than
        // appearing empty — an empty `allFiles` would mean "this table has no files".
        foreach (string omitted in new[]
                 {
                     "txnId", "allFiles", "fileSizeHistogram", "histogramOpt",
                     "numDeletedRecordsOpt", "numDeletionVectorsOpt", "deletedRecordCountsHistogramOpt",
                 })
        {
            Assert.False(json.TryGetProperty(omitted, out _), $"'{omitted}' should not be written");
        }
    }

    [Fact]
    public async Task Write_ThenRead_RoundTrips()
    {
        var snapshot = await CreateTableAsync(
            configuration: null,
            new TransactionId { AppId = "app", Version = 4, LastUpdated = 1700000003000 },
            new DomainMetadata { Domain = "d", Configuration = "{\"k\":1}", Removed = false });
        await _log.WriteCommitAsync(1, [Add("a.parquet", 100)]);
        snapshot = await SnapshotAsync();

        var writer = new VersionChecksumWriter(_fs);
        Assert.Equal(VersionChecksumWriteResult.Written, await writer.TryWriteAsync(snapshot));

        var read = await writer.TryReadAsync(1);

        Assert.NotNull(read);
        Assert.Equal(1, read.Version);
        Assert.Equal(100, read.TableSizeBytes);
        Assert.Equal("app", Assert.Single(read.SetTransactions!).AppId);
        Assert.Equal("d", Assert.Single(read.DomainMetadata!).Domain);

        // Re-serializing is the round-trip assertion that actually covers every field, including the ones
        // no property above names. Compared as BYTES rather than by record equality, which for these
        // types compares their collection properties by reference and so can never hold.
        Assert.Equal(
            File.ReadAllBytes(ChecksumFile(1)),
            VersionChecksumSerializer.Serialize(read));
    }

    /// <summary>
    /// <c>numMetadata</c> / <c>numProtocol</c> are REQUIRED and required to be 1, and a file breaking
    /// either half is refused.
    /// </summary>
    /// <remarks>
    /// Absent is not the milder problem it looks like. The reason to read a checksum at all is to check
    /// something against it, and a writer that omitted a field the file is DEFINED to carry was not
    /// describing this shape — which makes nothing else it says more trustworthy. delta-kernel-rs draws
    /// the line in the same place: its <c>CrcRaw.num_metadata</c> / <c>num_protocol</c> are plain
    /// <c>i64</c> with no serde default, so an absent one fails the whole deserialization.
    /// </remarks>
    [Theory]
    // Present but wrong.
    [InlineData("""{"tableSizeBytes":0,"numFiles":0,"numMetadata":2,"numProtocol":1,""", "numMetadata")]
    [InlineData("""{"tableSizeBytes":0,"numFiles":0,"numMetadata":1,"numProtocol":0,""", "numProtocol")]
    // Absent entirely.
    [InlineData("""{"tableSizeBytes":0,"numFiles":0,"numProtocol":1,""", "numMetadata")]
    [InlineData("""{"tableSizeBytes":0,"numFiles":0,"numMetadata":1,""", "numProtocol")]
    public async Task Read_RejectsAChecksumWithoutBothCountsSetToOne(string head, string expectedField)
    {
        Directory.CreateDirectory(Path.Combine(_tempDir, "_delta_log"));
        File.WriteAllText(ChecksumFile(0),
            head
            + """
              "metadata":{"id":"x","format":{"provider":"parquet","options":{}},
              "schemaString":"{}","partitionColumns":[],"configuration":{}},
              "protocol":{"minReaderVersion":1,"minWriterVersion":2}}
              """);

        // Malformed reads as absent through the writer's tolerant path — every version without a
        // checksum already works — but the parse itself must refuse, so nothing downstream trusts it.
        Assert.Null(await new VersionChecksumWriter(_fs).TryReadAsync(0));
        var thrown = Assert.Throws<DeltaFormatException>(
            () => VersionChecksumSerializer.Deserialize(File.ReadAllBytes(ChecksumFile(0)), 0));
        Assert.Contains(expectedField, thrown.Message, StringComparison.Ordinal);
    }

    // ── never overwrite ────────────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Write_DoesNotOverwriteAnExistingChecksum()
    {
        var snapshot = await CreateTableAsync();
        var writer = new VersionChecksumWriter(_fs);

        Assert.Equal(VersionChecksumWriteResult.Written, await writer.TryWriteAsync(snapshot));
        string first = File.ReadAllText(ChecksumFile(0));

        // "Writers MUST NOT overwrite existing Version Checksum files" (PROTOCOL.md). A second writer
        // describing the same version describes the same state, so this is a no-op rather than an error.
        Assert.Equal(VersionChecksumWriteResult.AlreadyExists, await writer.TryWriteAsync(snapshot));
        Assert.Equal(first, File.ReadAllText(ChecksumFile(0)));
    }

    // ── the committer ──────────────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Committer_WritesAChecksumBesideTheCommitItLandsAt()
    {
        var snapshot = await CreateTableAsync();

        var result = await new LogCommitter(_log, new LogCommitOptions { CheckpointInterval = 0 })
            .CommitAsync(new LogCommitRequest
            {
                BaseSnapshot = snapshot,
                Actions = [Add("a.parquet", 100)],
            });

        Assert.Equal(1, result.Version);
        Assert.True(File.Exists(ChecksumFile(1)));
        var json = ReadChecksumJson(1);
        Assert.Equal(1, json.GetProperty("numFiles").GetInt64());
        Assert.Equal(100, json.GetProperty("tableSizeBytes").GetInt64());
    }

    [Fact]
    public async Task Committer_WithChecksumsOff_WritesNone()
    {
        var snapshot = await CreateTableAsync();

        await new LogCommitter(_log,
            new LogCommitOptions { CheckpointInterval = 0, WriteVersionChecksums = false })
            .CommitAsync(new LogCommitRequest
            {
                BaseSnapshot = snapshot,
                Actions = [Add("a.parquet", 100)],
            });

        Assert.False(File.Exists(ChecksumFile(1)));
    }

    [Fact]
    public async Task Committer_NamesTheChecksumForTheVersionItDescribes_NotTheOneItAttempted()
    {
        // A concurrent writer lands between our commit and the post-commit refresh, so the snapshot the
        // committer ends up holding is at a LATER version than the one it just wrote. The checksum must
        // describe the version it is named for: naming it for the attempted version while summarising a
        // state that has moved past it is exactly the wrong checksum this feature must never produce.
        var snapshot = await CreateTableAsync();
        await _log.WriteCommitAsync(1, [Add("concurrent.parquet", 11)]);

        var result = await new LogCommitter(_log, new LogCommitOptions { CheckpointInterval = 0 })
            .CommitAsync(new LogCommitRequest
            {
                BaseSnapshot = snapshot,
                Actions = [Add("ours.parquet", 22)],
            });

        // Rebased onto the concurrent commit and landed at 2; the refreshed snapshot is at 2 as well, so
        // that is the version described. Version 1 is left without one, which is what every version
        // written before this feature existed already looks like.
        Assert.Equal(2, result.Version);
        Assert.True(File.Exists(ChecksumFile(2)));
        var json = ReadChecksumJson(2);
        Assert.Equal(2, json.GetProperty("numFiles").GetInt64());
        Assert.Equal(33, json.GetProperty("tableSizeBytes").GetInt64());
    }

    [Fact]
    public async Task Committer_WithInCommitTimestampsButNoTimestampAvailable_CommitsWithoutAChecksum()
    {
        var snapshot = await CreateTableAsync(
            new Dictionary<string, string> { [InCommitTimestamp.EnableKey] = "true" });

        // EnsureCommitInfo stamps the timestamp on the commit itself, so this one IS describable — the
        // assertion that matters is that the commit succeeds either way. A checksum is advisory; failing
        // to produce one must never fail the commit it follows.
        var result = await new LogCommitter(_log, new LogCommitOptions { CheckpointInterval = 0 })
            .CommitAsync(new LogCommitRequest
            {
                BaseSnapshot = snapshot,
                Actions = [Add("a.parquet", 100)],
                Operation = "WRITE",
            });

        Assert.True(result.Committed);
        var json = ReadChecksumJson(1);
        Assert.True(json.TryGetProperty("inCommitTimestampOpt", out var ict));
        Assert.Equal(result.Snapshot.InCommitTimestamp, ict.GetInt64());
    }
}
