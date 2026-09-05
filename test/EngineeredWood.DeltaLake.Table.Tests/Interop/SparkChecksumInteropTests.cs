// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.DeltaLake.Log;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests.Interop;

/// <summary>
/// <para><b>Tier 3.</b> The reference implementation's own version checksums, held against the state EW
/// reconstructs from the same log. See <see cref="Spark"/> for setup and cost.</para>
///
/// <para>This is the cross-check with the best odds of finding the next bug, and it is nearly free: the
/// tier already writes Spark tables on every run, and delta-spark writes a <c>.crc</c> beside every commit
/// it makes. Every other assertion in this tier compares what the two engines can be made to SAY — rows
/// back, a DESCRIBE DETAIL field, a protocol number. A checksum is different in kind: it is Spark's own
/// summary of the state at a version, written by the engine that produced it, and comparing our
/// reconstruction against it exercises the reconciliation itself rather than a projection of it. The
/// defect class it covers is the one that has actually bitten — the reader that quietly disagrees with
/// the writer about an optional field, which no row-count assertion can see.</para>
///
/// <para>Where Spark records less than we do (its <c>setTransactions</c> and <c>domainMetadata</c> are
/// optional and often absent), those fields come back as not-recorded rather than as agreement, and this
/// test does not pretend otherwise — it asserts on the fields Spark actually spoke about.</para>
/// </summary>
[Collection("Interop")]
public class SparkChecksumInteropTests : IDisposable
{
    private readonly string _tempDir;

    public SparkChecksumInteropTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_spark_crc_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    private string LogDir => Path.Combine(_tempDir, "_delta_log");

    /// <summary>
    /// Spark writes a table and then mutates it, and EW's reconstruction of every version agrees with the
    /// checksum Spark wrote for that version.
    /// </summary>
    /// <remarks>
    /// The DELETE and the second INSERT are there so the log contains versions whose reconciliation is
    /// not trivial: a version where a file is removed is the one where a count computed from "what the
    /// commit said" diverges from one computed from "what is live", and it is the divergence a checksum
    /// is shaped to catch.
    /// </remarks>
    [SkippableFact]
    public async Task SparkWrittenChecksums_AgreeWithWhatEwReconstructs()
    {
        Spark.Require();

        Spark.Invoke("write", new
        {
            path = _tempDir,
            schema = "id long, region string",
            rows = new object[]
            {
                new object[] { 1L, "us" }, new object[] { 2L, "eu" }, new object[] { 3L, "us" },
            },
            sql = new[]
            {
                "DELETE FROM delta.`{path}` WHERE id = 2",
                "INSERT INTO delta.`{path}` VALUES (4, 'apac')",
                "ALTER TABLE delta.`{path}` SET TBLPROPERTIES ('custom.owner' = 'interop')",
            },
        });

        // Parsed through the library's own recogniser rather than by globbing: Hadoop's local
        // filesystem writes its OWN checksum sidecar beside every file it creates, named
        // `.00000000000000000000.crc`, so a Spark-written log directory contains .crc files that are
        // nothing to do with Delta. They differ only by a leading dot.
        var checksums = Directory.GetFiles(LogDir, "*.crc")
            .Select(Path.GetFileName)
            .Where(name => DeltaVersion.TryParseChecksumVersion(name!, out _))
            .Select(name =>
            {
                DeltaVersion.TryParseChecksumVersion(name!, out long version);
                return version;
            })
            .OrderBy(v => v)
            .ToList();

        // Not a skip. delta-spark writes these by default, and the day it stops is the day this tier
        // quietly loses its only foreign-checksum coverage — which is worth a failure that says so.
        Assert.True(checksums.Count > 0,
            $"delta-spark ({Spark.Version}) wrote no .crc files; this test measures nothing without them");

        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.OpenAsync(fs);
        var validator = new VersionChecksumValidator(fs);

        foreach (long version in checksums)
        {
            var snapshot = await table.GetSnapshotAtVersionAsync(version);
            var validation = await validator.ValidateAsync(snapshot);

            Assert.True(
                validation.Outcome == VersionChecksumValidationOutcome.Agrees,
                $"version {version}: {validation.Describe()}");

            // And it agreed about something. Spark leaves several fields unrecorded, so "no
            // disagreements" on its own is a claim a checksum full of silence would also satisfy —
            // these four are the ones delta-spark always writes.
            foreach (string field in new[]
            {
                "tableSizeBytes", "numFiles", "metadata.schemaString", "protocol.minWriterVersion",
            })
            {
                Assert.Equal(
                    VersionChecksumFieldOutcome.Agrees,
                    Assert.Single(validation.Fields, f => f.Field == field).Outcome);
            }
        }
    }
}
