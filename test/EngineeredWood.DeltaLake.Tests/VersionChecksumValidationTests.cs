// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Checkpoint;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.Snapshot;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Tests;

/// <summary>
/// Validating a reconstructed snapshot against the <c>.crc</c> written beside its version.
///
/// <para>The comparison is worth having because it is an INDEPENDENT second copy of the state — the one
/// thing a log replay cannot check itself against. Most of what follows is therefore about the two ways
/// that value gets thrown away: reporting a difference where the two sources are saying the same thing in
/// different words (which trains a caller to ignore the report), and reporting agreement where one side
/// never spoke (which is worse, because a caller then acts on a confirmation nobody gave).</para>
///
/// <para>Every case is built by describing a real snapshot and then editing the RECORDED side, so the
/// snapshot under comparison is always one the log actually produced.</para>
/// </summary>
public class VersionChecksumValidationTests : IDisposable
{
    private readonly string _tempDir;
    private readonly LocalTableFileSystem _fs;
    private readonly TransactionLog _log;

    public VersionChecksumValidationTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_crcval_{Guid.NewGuid():N}");
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
        Id = "crc-validation",
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

    /// <summary>The checksum this library would write for <paramref name="snapshot"/>: the agreeing case.</summary>
    private static VersionChecksum Recorded(Snapshot.Snapshot snapshot) =>
        VersionChecksum.TryFromSnapshot(snapshot)
            ?? throw new InvalidOperationException("the snapshot should be describable");

    private static VersionChecksumFieldComparison Field(
        VersionChecksumValidation validation, string name) =>
        Assert.Single(validation.Fields, f => f.Field == name);

    // ── the agreeing case, and what it covers ──────────────────────────────────────────────────────────

    /// <summary>
    /// The checksum this library writes for a snapshot validates against that same snapshot, with nothing
    /// left uncompared. The second half is the assertion that matters: a validation that quietly compared
    /// nothing would also report no disagreements.
    /// </summary>
    [Fact]
    public async Task Validate_AgreesWithTheChecksumWrittenForTheSameSnapshot()
    {
        var snapshot = await CreateTableAsync();
        await _log.WriteCommitAsync(1, [Add("a.parquet", 100), Add("b.parquet", 250)]);
        snapshot = await SnapshotAsync();

        await new VersionChecksumWriter(_fs).TryWriteAsync(snapshot);
        var validation = await new VersionChecksumValidator(_fs).ValidateAsync(snapshot);

        Assert.Equal(VersionChecksumValidationOutcome.Agrees, validation.Outcome);
        Assert.Empty(validation.Disagreements);
        Assert.Empty(validation.Unchecked);
        Assert.Equal("350", Field(validation, "tableSizeBytes").Recorded);
        Assert.Equal("2", Field(validation, "numFiles").Recorded);
    }

    // ── a disagreement names both values and where each came from ──────────────────────────────────────

    /// <summary>
    /// The report has to survive being read by someone who was not there. delta-spark fails this exact
    /// comparison with DELTA_STATE_RECOVER_ERROR — "the metadata of your Delta table could not be
    /// recovered ... did you manually delete files in the _delta_log directory?" — which names neither
    /// the field, nor either value, nor the fact that nothing was deleted. This asserts the opposite.
    /// </summary>
    [Fact]
    public async Task Validate_NamesTheField_BothValues_AndWhichSideEachCameFrom()
    {
        var snapshot = await CreateTableAsync();
        await _log.WriteCommitAsync(1, [Add("a.parquet", 100)]);
        snapshot = await SnapshotAsync();

        var drifted = Recorded(snapshot) with { NumFiles = 7, TableSizeBytes = 999 };
        var validation = VersionChecksumValidator.Compare(drifted, snapshot);

        Assert.Equal(VersionChecksumValidationOutcome.Disagrees, validation.Outcome);
        Assert.False(validation.IsConsistent);
        Assert.Equal(
            ["numFiles", "tableSizeBytes"],
            validation.Disagreements.Select(d => d.Field).OrderBy(f => f, StringComparer.Ordinal));

        var numFiles = Field(validation, "numFiles");
        Assert.Equal("7", numFiles.Recorded);
        Assert.Equal("1", numFiles.Reconstructed);

        string description = validation.Describe();
        Assert.Contains("version 1", description, StringComparison.Ordinal);
        Assert.Contains("numFiles: the checksum says 7, the snapshot says 1", description,
            StringComparison.Ordinal);
    }

    /// <summary>
    /// An empty string is not an absent value, and this is the case that made the feature worth having:
    /// <c>CheckpointWriter</c> wrote absent optional fields as <c>""</c> and <c>0</c>, so the same table
    /// read back differently depending on whether the reader came through the log or the checkpoint. It
    /// took a Spark round trip and a 600-second hang to find. Here it is one comparison and one line.
    /// </summary>
    [Fact]
    public async Task Validate_AnEmptyStringIsNotAnAbsentValue()
    {
        var snapshot = await CreateTableAsync();

        var coerced = Recorded(snapshot) with
        {
            Metadata = snapshot.Metadata with { Name = "", Description = "" },
        };
        var validation = VersionChecksumValidator.Compare(coerced, snapshot);

        Assert.Equal(VersionChecksumValidationOutcome.Disagrees, validation.Outcome);
        var name = Field(validation, "metadata.name");
        Assert.Equal("\"\"", name.Recorded);
        Assert.Null(name.Reconstructed);
        Assert.Contains("the snapshot says (absent)", name.ToString(), StringComparison.Ordinal);
    }

    /// <summary>The same coercion one level down, on the field it actually bit: <c>txn.lastUpdated</c>.</summary>
    [Fact]
    public async Task Validate_AZeroTimestampIsNotAnAbsentOne()
    {
        var snapshot = await CreateTableAsync(
            configuration: null,
            new TransactionId { AppId = "streamer", Version = 4, LastUpdated = null });

        var coerced = Recorded(snapshot) with
        {
            SetTransactions = [new TransactionId { AppId = "streamer", Version = 4, LastUpdated = 0 }],
        };
        var validation = VersionChecksumValidator.Compare(coerced, snapshot);

        Assert.Equal(VersionChecksumValidationOutcome.Disagrees, validation.Outcome);
        var txn = Field(validation, "setTransactions[streamer]");
        Assert.Equal("version=4, lastUpdated=0", txn.Recorded);
        Assert.Equal("version=4, lastUpdated=(absent)", txn.Reconstructed);
    }

    // ── absent is not empty, which is the easiest thing here to get wrong ───────────────────────────────

    /// <summary>
    /// A checksum that does not record <c>setTransactions</c> has said NOTHING about the app
    /// transactions. Reporting that as agreement is how a reader concludes a committed idempotent write
    /// never happened and runs it a second time — so it is reported as not-recorded, and it shows up in
    /// <see cref="VersionChecksumValidation.Unchecked"/> where a caller counting coverage will see it.
    /// </summary>
    [Fact]
    public async Task Validate_AnUnrecordedTransactionListIsNotCoverage()
    {
        var snapshot = await CreateTableAsync(
            configuration: null,
            new TransactionId { AppId = "streamer", Version = 4, LastUpdated = 1700000000000 });

        var silent = Recorded(snapshot) with { SetTransactions = null, DomainMetadata = null };
        var validation = VersionChecksumValidator.Compare(silent, snapshot);

        // Nothing DISAGREED — but nothing confirmed the transactions either, and the report says so.
        Assert.Equal(VersionChecksumValidationOutcome.Agrees, validation.Outcome);
        Assert.Equal(
            VersionChecksumFieldOutcome.NotRecorded, Field(validation, "setTransactions").Outcome);
        Assert.Equal(
            ["domainMetadata", "setTransactions"],
            validation.Unchecked.Select(f => f.Field).OrderBy(f => f, StringComparer.Ordinal));
        Assert.Contains("Not compared:", validation.Describe(), StringComparison.Ordinal);
    }

    /// <summary>
    /// And the other side of that: a checksum that records an EMPTY list has made a claim, so a
    /// transaction the log knows about is a real disagreement rather than a silence.
    /// </summary>
    [Fact]
    public async Task Validate_AnEmptyTransactionListIsAClaim()
    {
        var snapshot = await CreateTableAsync(
            configuration: null,
            new TransactionId { AppId = "streamer", Version = 4, LastUpdated = 1700000000000 });

        var claims = Recorded(snapshot) with { SetTransactions = [] };
        var validation = VersionChecksumValidator.Compare(claims, snapshot);

        Assert.Equal(VersionChecksumValidationOutcome.Disagrees, validation.Outcome);
        var txn = Field(validation, "setTransactions[streamer]");
        Assert.Null(txn.Recorded);
        Assert.Equal("version=4, lastUpdated=1700000000000", txn.Reconstructed);
    }

    /// <summary>
    /// A <c>removed</c> domain in the recorded list says what the snapshot's silence says: not live. The
    /// field is defined as the live set, so a tombstone in it is not a domain the snapshot is missing.
    /// </summary>
    [Fact]
    public async Task Validate_ARecordedTombstoneIsNotALiveDomain()
    {
        var snapshot = await CreateTableAsync();

        var withTombstone = Recorded(snapshot) with
        {
            DomainMetadata =
            [
                new DomainMetadata { Domain = "gone", Configuration = "{}", Removed = true },
            ],
        };

        Assert.Equal(
            VersionChecksumValidationOutcome.Agrees,
            VersionChecksumValidator.Compare(withTombstone, snapshot).Outcome);
    }

    // ── same statement, different spelling ─────────────────────────────────────────────────────────────

    /// <summary>
    /// This library models an empty table configuration as null and delta-spark models it as an empty
    /// map. If those compared as different, every Spark-written checksum of a table with no properties
    /// would be reported as drift — and a report that cries wolf on every table is one nobody reads.
    /// </summary>
    [Fact]
    public async Task Validate_AnAbsentCollectionAndAnEmptyOneAreTheSameStatement()
    {
        var snapshot = await CreateTableAsync();

        var spelledDifferently = Recorded(snapshot) with
        {
            Metadata = snapshot.Metadata with { Configuration = null },
            Protocol = snapshot.Protocol with { ReaderFeatures = [], WriterFeatures = [] },
        };

        Assert.Equal(
            VersionChecksumValidationOutcome.Agrees,
            VersionChecksumValidator.Compare(spelledDifferently, snapshot).Outcome);
    }

    /// <summary>
    /// <c>schemaString</c> is JSON inside a JSON string, so two engines can spell the same schema
    /// differently. What is compared is what it MEANS.
    /// </summary>
    [Fact]
    public async Task Validate_ComparesTheSchemaByMeaningNotBySpelling()
    {
        var snapshot = await CreateTableAsync();

        var respelled = Recorded(snapshot) with
        {
            Metadata = snapshot.Metadata with
            {
                SchemaString =
                    """
                    { "type" : "struct",
                      "fields" : [ { "name" : "id", "nullable" : false,
                                     "metadata" : {}, "type" : "long" } ] }
                    """,
            },
        };

        Assert.Equal(
            VersionChecksumValidationOutcome.Agrees,
            VersionChecksumValidator.Compare(respelled, snapshot).Outcome);
    }

    /// <summary>...but a schema that means something else is a difference, respelling or not.</summary>
    [Fact]
    public async Task Validate_ADifferentSchemaIsADifference()
    {
        var snapshot = await CreateTableAsync();

        var different = Recorded(snapshot) with
        {
            Metadata = snapshot.Metadata with
            {
                SchemaString =
                    """{"type":"struct","fields":[{"name":"id","type":"string","nullable":false,"metadata":{}}]}""",
            },
        };
        var validation = VersionChecksumValidator.Compare(different, snapshot);

        Assert.Equal(VersionChecksumValidationOutcome.Disagrees, validation.Outcome);
        Assert.Equal(
            VersionChecksumFieldOutcome.Disagrees, Field(validation, "metadata.schemaString").Outcome);
    }

    /// <summary>
    /// Partition columns are ORDERED state — they are the directory nesting order — so a reordering is a
    /// different table layout, not a different spelling of the same one.
    /// </summary>
    [Fact]
    public async Task Validate_PartitionColumnOrderIsPartOfTheState()
    {
        await _log.WriteCommitAsync(0,
        [
            new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 },
            Metadata() with { PartitionColumns = ["year", "month"] },
        ]);
        var snapshot = await SnapshotAsync();

        var reordered = Recorded(snapshot) with
        {
            Metadata = snapshot.Metadata with { PartitionColumns = ["month", "year"] },
        };
        var validation = VersionChecksumValidator.Compare(reordered, snapshot);

        Assert.Equal(VersionChecksumValidationOutcome.Disagrees, validation.Outcome);
        Assert.Equal("[month, year]", Field(validation, "metadata.partitionColumns").Recorded);
        Assert.Equal("[year, month]", Field(validation, "metadata.partitionColumns").Reconstructed);
    }

    /// <summary>Protocol features are a SET, so their order is not part of what the protocol says.</summary>
    [Fact]
    public async Task Validate_ProtocolFeatureOrderIsNot()
    {
        await _log.WriteCommitAsync(0,
        [
            new ProtocolAction
            {
                MinReaderVersion = 3,
                MinWriterVersion = 7,
                ReaderFeatures = ["deletionVectors"],
                WriterFeatures = ["deletionVectors", "appendOnly"],
            },
            Metadata(),
        ]);
        var snapshot = await SnapshotAsync();

        var reordered = Recorded(snapshot) with
        {
            Protocol = snapshot.Protocol with { WriterFeatures = ["appendOnly", "deletionVectors"] },
        };

        Assert.Equal(
            VersionChecksumValidationOutcome.Agrees,
            VersionChecksumValidator.Compare(reordered, snapshot).Outcome);
    }

    // ── in-commit timestamps: absence means something on both sides ────────────────────────────────────

    /// <summary>
    /// A snapshot bootstrapped from a CHECKPOINT has no in-commit timestamp — a checkpoint holds no
    /// <c>commitInfo</c>, which is where the timestamp lives. That is an ordinary read of an ordinary
    /// table, so the recorded value has nothing to be held against rather than something to disagree
    /// with, and it must not be reported as drift.
    /// </summary>
    [Fact]
    public async Task Validate_ASnapshotWithNoInCommitTimestampHasNothingToDisagreeWith()
    {
        var snapshot = await CreateTableAsync(
            new Dictionary<string, string> { ["delta.enableInCommitTimestamps"] = "true" });

        var recorded = Recorded(WithInCommitTimestamp(snapshot, 1700000005000));
        var validation = VersionChecksumValidator.Compare(
            recorded, WithInCommitTimestamp(snapshot, null));

        Assert.Equal(VersionChecksumValidationOutcome.Agrees, validation.Outcome);
        var ict = Field(validation, "inCommitTimestampOpt");
        Assert.Equal(VersionChecksumFieldOutcome.NotReconstructed, ict.Outcome);
        Assert.Equal("1700000005000", ict.Recorded);
    }

    /// <summary>Two timestamps that both exist and differ are a plain disagreement.</summary>
    [Fact]
    public async Task Validate_TwoInCommitTimestampsThatDifferDisagree()
    {
        var snapshot = await CreateTableAsync(
            new Dictionary<string, string> { ["delta.enableInCommitTimestamps"] = "true" });

        var recorded = Recorded(WithInCommitTimestamp(snapshot, 1));
        var validation = VersionChecksumValidator.Compare(
            recorded, WithInCommitTimestamp(snapshot, 2));

        Assert.Equal(VersionChecksumValidationOutcome.Disagrees, validation.Outcome);
    }

    /// <summary>
    /// With the feature OFF the field is not part of the state, and a snapshot may still be carrying a
    /// timestamp read off some earlier <c>commitInfo</c>. Comparing against a field the writer was right
    /// to omit would manufacture a difference out of a correctly written file, so the field is not
    /// reported at all.
    /// </summary>
    [Fact]
    public async Task Validate_IgnoresTheTimestampEntirelyWhenTheFeatureIsOff()
    {
        var snapshot = await CreateTableAsync();
        var validation = VersionChecksumValidator.Compare(
            Recorded(snapshot), WithInCommitTimestamp(snapshot, 1700000009000));

        Assert.Equal(VersionChecksumValidationOutcome.Agrees, validation.Outcome);
        Assert.DoesNotContain(validation.Fields, f => f.Field == "inCommitTimestampOpt");
    }

    // ── the report has to be legible, or the values in it are not evidence ─────────────────────────────

    /// <summary>
    /// A schema string is the one field here routinely longer than the display window, and two schemas
    /// that differ almost always agree for most of their length. Showing the first N characters of each
    /// would print two IDENTICAL values under the word "disagrees" — a report that looks like a bug in
    /// the validator and tells the reader nothing. The window follows the difference.
    /// </summary>
    [Fact]
    public async Task Validate_ShowsWhereTwoLongValuesDiffer_NotTheirCommonPrefix()
    {
        // ~40 identical fields, then one that differs — the difference is thousands of characters in.
        static string WideSchema(string lastField)
        {
            var fields = Enumerable.Range(0, 40)
                .Select(i => $$$"""{"name":"column_number_{{{i}}}","type":"long","nullable":true,"metadata":{}}""")
                .Append($$$"""{"name":"{{{lastField}}}","type":"string","nullable":true,"metadata":{}}""");
            return $$"""{"type":"struct","fields":[{{string.Join(",", fields)}}]}""";
        }

        await _log.WriteCommitAsync(0,
        [
            new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 },
            Metadata() with { SchemaString = WideSchema("reconstructed_tail") },
        ]);
        var snapshot = await SnapshotAsync();

        var different = Recorded(snapshot) with
        {
            Metadata = snapshot.Metadata with { SchemaString = WideSchema("recorded_tail") },
        };
        var validation = VersionChecksumValidator.Compare(different, snapshot);

        var schema = Field(validation, "metadata.schemaString");
        Assert.Equal(VersionChecksumFieldOutcome.Disagrees, schema.Outcome);

        // The point: two rendered values that a reader can tell apart, each showing its own side of the
        // difference, and each saying where in the value it starts.
        Assert.NotEqual(schema.Recorded, schema.Reconstructed);
        Assert.Contains("recorded_tail", schema.Recorded!, StringComparison.Ordinal);
        Assert.Contains("reconstructed_tail", schema.Reconstructed!, StringComparison.Ordinal);
        Assert.Contains("(from char ", schema.Recorded!, StringComparison.Ordinal);
    }

    /// <summary>
    /// <see cref="VersionChecksumValidation.Describe"/> is one line per field, so a value carrying a
    /// newline would break the report into pieces that no longer say which field they belong to — and a
    /// value carrying a quote would leave a reader unable to see where it ended. Free text is exactly
    /// what <c>name</c> and <c>description</c> hold.
    /// </summary>
    [Fact]
    public async Task Validate_EscapesWhatWouldBeReadAsTheReportsOwnPunctuation()
    {
        var snapshot = await CreateTableAsync();

        var awkward = Recorded(snapshot) with
        {
            Metadata = snapshot.Metadata with { Name = "two\nlines and a \" quote" },
        };
        var validation = VersionChecksumValidator.Compare(awkward, snapshot);

        var name = Field(validation, "metadata.name");
        Assert.Equal("\"two\\nlines and a \\\" quote\"", name.Recorded);
        Assert.DoesNotContain('\n', name.Recorded!);

        // One header line and one field line, still.
        Assert.Equal(2, validation.Describe().Split(Environment.NewLine).Length);
    }

    // ── no checksum, and a checksum that is not one ────────────────────────────────────────────────────

    /// <summary>
    /// Every version this library has ever written has no checksum and the spec requires readers to cope,
    /// so an absent one is an answer rather than a failure — and it is NOT agreement.
    /// </summary>
    [Fact]
    public async Task Validate_AnAbsentChecksumIsAnOutcomeNotAFailure()
    {
        var snapshot = await CreateTableAsync();

        var validation = await new VersionChecksumValidator(_fs).ValidateAsync(snapshot);

        Assert.Equal(VersionChecksumValidationOutcome.Absent, validation.Outcome);
        Assert.Empty(validation.Fields);
        Assert.True(validation.IsConsistent);
        Assert.Contains("no version checksum", validation.Describe(), StringComparison.Ordinal);
    }

    /// <summary>
    /// "Nobody wrote one" and "somebody wrote one we cannot read" are different facts about a table, and
    /// the second is the one worth chasing — a writer emitting checksums this library rejects is a
    /// compatibility problem that absence would hide forever.
    /// </summary>
    [Fact]
    public async Task Validate_AChecksumThatIsNotOneIsUnreadableNotAbsent()
    {
        var snapshot = await CreateTableAsync();
        // Structurally valid JSON, and missing numMetadata / numProtocol, which the spec pins at 1.
        await _fs.TryWriteAllBytesAsync(
            DeltaVersion.ChecksumPath(0),
            System.Text.Encoding.UTF8.GetBytes("""{"tableSizeBytes":0,"numFiles":0}"""));

        var validation = await new VersionChecksumValidator(_fs).ValidateAsync(snapshot);

        Assert.Equal(VersionChecksumValidationOutcome.Unreadable, validation.Outcome);
        Assert.Contains("numMetadata", validation.Reason!, StringComparison.Ordinal);
        Assert.Contains("could not be parsed", validation.Describe(), StringComparison.Ordinal);
    }

    /// <summary>
    /// Two versions of a table are SUPPOSED to differ, so comparing a checksum against a snapshot at
    /// another version answers nothing — and would report a table working exactly as intended as drift.
    /// </summary>
    [Fact]
    public async Task Validate_RefusesToCompareTwoDifferentVersions()
    {
        var snapshot = await CreateTableAsync();
        await _log.WriteCommitAsync(1, [Add("a.parquet", 100)]);
        var later = await SnapshotAsync();

        var error = Assert.Throws<ArgumentException>(
            () => VersionChecksumValidator.Compare(Recorded(snapshot), later));
        Assert.Contains("version 0", error.Message, StringComparison.Ordinal);
        Assert.Contains("version 1", error.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// The same snapshot carrying a different in-commit timestamp — including none, which is what a
    /// snapshot bootstrapped from a checkpoint has. <see cref="Snapshot.Snapshot"/> is a class rather
    /// than a record, so this is the copy constructor it does not have.
    /// </summary>
    private static Snapshot.Snapshot WithInCommitTimestamp(
        Snapshot.Snapshot snapshot, long? inCommitTimestamp) => new()
    {
        Version = snapshot.Version,
        Metadata = snapshot.Metadata,
        Protocol = snapshot.Protocol,
        Schema = snapshot.Schema,
        ArrowSchema = snapshot.ArrowSchema,
        ActiveFiles = snapshot.ActiveFiles,
        AppTransactions = snapshot.AppTransactions,
        DomainMetadata = snapshot.DomainMetadata,
        Tombstones = snapshot.Tombstones,
        InCommitTimestamp = inCommitTimestamp,
        RowIdHighWaterMark = snapshot.RowIdHighWaterMark,
    };
}
