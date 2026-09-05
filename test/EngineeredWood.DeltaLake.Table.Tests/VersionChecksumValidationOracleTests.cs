// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// The checksums this library writes, held against the state this library rebuilds — used as an ORACLE
/// over real tables rather than as a unit of behaviour.
///
/// <para>Why that is worth a test class of its own: a <c>.crc</c> is written once, from the snapshot the
/// committing writer had in hand, and every later read reconstructs that same version from a different
/// starting point — a full log replay, a replay from a checkpoint, a replay from a compacted range. Those
/// paths are supposed to agree and nothing else compares them. The last time they did not, the cause was
/// <c>CheckpointWriter</c> coercing absent optional fields to <c>""</c> and <c>0</c>, so a table read
/// through a checkpoint carried <c>name: ""</c> where the log said <c>name: null</c>; it took a Spark
/// round trip, an error naming the wrong cause, and a 600-second hang to find. The comparison below is
/// the one that names it directly.</para>
/// </summary>
public class VersionChecksumValidationOracleTests : IDisposable
{
    private readonly string _tempDir;

    public VersionChecksumValidationOracleTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_crc_oracle_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private static readonly Apache.Arrow.Schema TestSchema = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("region", StringType.Default, true))
        .Build();

    private static RecordBatch Batch(params long[] ids)
    {
        var idBuilder = new Int64Array.Builder();
        idBuilder.AppendRange(ids);
        var regionBuilder = new StringArray.Builder();
        foreach (long id in ids)
            regionBuilder.Append(id % 2 == 0 ? "us" : "eu");
        return new RecordBatch(TestSchema, [idBuilder.Build(), regionBuilder.Build()], ids.Length);
    }

    private string LogDir => Path.Combine(_tempDir, "_delta_log");

    private IEnumerable<long> ChecksummedVersions() =>
        Directory.GetFiles(LogDir, "*.crc")
            .Select(f => long.Parse(Path.GetFileNameWithoutExtension(f)!))
            .OrderBy(v => v);

    /// <summary>
    /// Every version of a table that has been through the commit paths validates against the checksum
    /// written beside it — and validates COMPLETELY, with nothing left uncompared. The second half is
    /// what keeps this honest: a checksum recording neither <c>setTransactions</c> nor
    /// <c>domainMetadata</c> would agree just as loudly while confirming much less.
    /// </summary>
    [Fact]
    public async Task EveryVersion_ValidatesAgainstTheChecksumWrittenBesideIt()
    {
        var fs = new LocalTableFileSystem(_tempDir);

        // Checkpointing off so every version is rebuilt by full replay; the checkpoint path is the next
        // test, and it is a different claim.
        var options = new DeltaTableOptions { CheckpointInterval = 0 };
        await using var table = await DeltaTable.CreateAsync(fs, TestSchema, options);

        await table.WriteAsync([Batch(1, 2, 3)]);
        await table.WriteAsync([Batch(4, 5)], DeltaWriteMode.Overwrite);
        await table.AddColumnAsync(new Field("extra", StringType.Default, true));
        await table.SetDomainMetadataAsync("app.domain", """{"v":1}""");

        var txn = table.StartTransaction();
        await txn.WriteAsync([Batch(6)]);
        txn.RequireAppTransaction("producer", version: 7);
        await txn.CommitAsync();

        await table.RemoveDomainMetadataAsync("app.domain");

        var validator = new VersionChecksumValidator(fs);
        var versions = ChecksummedVersions().ToList();

        foreach (long version in versions)
        {
            var snapshot = await table.GetSnapshotAtVersionAsync(version);
            var validation = await validator.ValidateAsync(snapshot);

            Assert.True(
                validation.Outcome == VersionChecksumValidationOutcome.Agrees,
                validation.Describe());
            Assert.Empty(validation.Unchecked);
        }

        // The sweep really did run: a table that committed once would satisfy the loop above trivially.
        Assert.True(versions.Count >= 6, $"expected at least 6 checksummed versions, got {versions.Count}");
    }

    /// <summary>
    /// The same comparison across the seam that has actually broken: the checksum is written from a
    /// snapshot built by REPLAYING THE LOG, and validated here against one bootstrapped from a
    /// CHECKPOINT. Every field the checkpoint round-trips is compared, which is the class of defect
    /// <c>CheckpointWriter</c>'s null coercion belonged to — <c>metadata.name</c>, <c>createdTime</c> and
    /// a <c>txn</c>'s <c>lastUpdated</c> all pass through it, and all three are optional, which is what
    /// made them easy to coerce and impossible to notice.
    /// </summary>
    [Fact]
    public async Task AVersionRebuiltFromACheckpoint_StillAgreesWithItsChecksum()
    {
        var fs = new LocalTableFileSystem(_tempDir);

        await using (var table = await DeltaTable.CreateAsync(
            fs, TestSchema, new DeltaTableOptions { CheckpointInterval = 2 }))
        {
            await table.WriteAsync([Batch(1, 2)]);
            await table.SetDomainMetadataAsync("app.domain", """{"v":1}""");

            var txn = table.StartTransaction();
            await txn.WriteAsync([Batch(3)]);
            txn.RequireAppTransaction("producer", version: 7);
            await txn.CommitAsync();

            await table.WriteAsync([Batch(4)]);
        }

        long checkpointVersion = Directory.GetFiles(LogDir, "*.checkpoint.parquet")
            .Select(f => long.Parse(Path.GetFileName(f)!.Split('.')[0]))
            .Max();

        // The snapshot below is only built from the checkpoint if _last_checkpoint names it — a stale or
        // unusable hint falls back to a full replay, which would leave this test measuring the path it
        // was written to avoid.
        var hint = await new DeltaLake.Checkpoint.CheckpointReader(fs).ReadLastCheckpointAsync();
        Assert.Equal(checkpointVersion, hint?.Version);

        // Reopened cold, so nothing is served out of the writer's own memory.
        await using var reader = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));
        var snapshot = await reader.GetSnapshotAtVersionAsync(checkpointVersion);
        var validation = await new VersionChecksumValidator(fs).ValidateAsync(snapshot);

        Assert.True(
            validation.Outcome == VersionChecksumValidationOutcome.Agrees, validation.Describe());

        // ...and it agreed about the fields that carry an absent value through the checkpoint, rather
        // than agreeing about the counts and staying silent on the rest.
        foreach (string field in new[]
        {
            "metadata.name", "metadata.description", "metadata.createdTime",
            "setTransactions[producer]", "domainMetadata[app.domain]",
        })
        {
            Assert.Equal(
                VersionChecksumFieldOutcome.Agrees,
                Assert.Single(validation.Fields, f => f.Field == field).Outcome);
        }
    }
}
