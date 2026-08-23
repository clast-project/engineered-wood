// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;

namespace EngineeredWood.DeltaLake.Table.Tests.Interop;

/// <summary>
/// Runs the whole scenario matrix ONCE and shares it across the class. Every test below reads a
/// different row of one measurement, and each run costs a Spark session plus five table builds — so
/// re-measuring per test would multiply the tier's slowest command by six for no extra coverage.
/// </summary>
public sealed class ConflictSemanticsFixture : IDisposable
{
    private readonly string _tempDir;
    private readonly Lazy<JsonElement?> _measured;

    public ConflictSemanticsFixture()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_conflict_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
        _measured = new Lazy<JsonElement?>(() =>
            Spark.Available ? Spark.Invoke("conflict_semantics", new { path = _tempDir }) : null);
    }

    /// <summary>The measurement. The tier gate lives HERE rather than in each test: touching this
    /// either yields a real result or skips the caller — or fails it, under <c>EW_REQUIRE_*</c> —
    /// before there is anything to dereference. That is why the tests below carry no check of their
    /// own, and why the backing value may still be null while this property never returns one.</summary>
    public JsonElement Result
    {
        get
        {
            Spark.Require();
            return _measured.Value!.Value;
        }
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }
}

/// <summary>
/// <para><b>Tier 3.</b> What Delta's OWN conflict checker does — the two things EW asserts about the
/// reference implementation from having read its source rather than from having watched it run.</para>
///
/// <para>Both feed issue #15's open question 4 (whether EW's <c>WriteSerializable</c> should exempt
/// <c>concurrentDeleteRead</c> for a transaction holding row-level deletes). The decision itself is made
/// on <see cref="DeltaTransaction.DeclareWholeTableRead"/>'s own terms and does not need these; what
/// these do is stop a claim about Spark from going unchecked, which matters however that resolves.</para>
///
/// <para>Driving this needs the JVM's <c>OptimisticTransaction</c> directly, because Spark has no SQL for
/// "declare a read, then commit something unrelated" — its statements declare their reads implicitly.
/// Delta's <c>readWholeTable()</c> is the exact analogue of
/// <see cref="DeltaTransaction.DeclareWholeTableRead"/>, which is what makes this a measurement rather
/// than an analogy.</para>
/// </summary>
[Collection("Interop")]
public class ConflictSemanticsInteropTests : IClassFixture<ConflictSemanticsFixture>
{
    private readonly ConflictSemanticsFixture _fixture;

    public ConflictSemanticsInteropTests(ConflictSemanticsFixture fixture) => _fixture = fixture;

    private static string VerdictOf(JsonElement result, string scenario) =>
        result.GetProperty("scenarios").EnumerateArray()
            .Single(s => s.GetProperty("name").GetString() == scenario)
            .GetProperty("verdict").GetString()!;

    /// <summary>
    /// <b>OSS Delta has exactly ONE isolation level.</b> <c>delta.isolationLevel='WriteSerializable'</c>
    /// is REJECTED outright — "requirement failed: delta.isolationLevel must be Serializable".
    ///
    /// <para>This is the load-bearing fact for open question 4, and it is not what the question assumed.
    /// "What does Spark do at WriteSerializable?" has no answer in the reference implementation, because
    /// the level does not exist there: it is a Databricks extension. So EW's
    /// <see cref="IsolationLevel.WriteSerializable"/> has no OSS counterpart to conform to or diverge
    /// from, and any claim of the form "Delta does X at WriteSerializable" is a claim about Databricks,
    /// not about the engine this tier runs.</para>
    /// </summary>
    [SkippableFact]
    public void OssDelta_AcceptsOnlySerializable()
    {
        var result = _fixture.Result;

        var levels = result.GetProperty("isolation_levels");
        Assert.Equal("accepted", levels.GetProperty("Serializable").GetString());

        string writeSerializable = levels.GetProperty("WriteSerializable").GetString()!;
        Assert.StartsWith("rejected", writeSerializable);
        Assert.Contains("must be Serializable", writeSerializable);
    }

    /// <summary>
    /// A transaction that declared a whole-table read ABORTS against a concurrent delete — in both of
    /// Delta's delete shapes, the copy-on-write rewrite and the deletion vector. EW does the same thing,
    /// so the two agree on the case open question 4 is about.
    ///
    /// <para>Delta names it <c>ConcurrentAppend</c> rather than <c>ConcurrentDeleteRead</c>, because a
    /// delete of either shape emits an <c>add</c> alongside its <c>remove</c> (the rewritten file, or the
    /// same file carrying the new vector) and Delta tests the append rule first. EW reaches the same
    /// verdict by the other branch. The disagreement is in the label, not the outcome — which is exactly
    /// why this is worth having measured rather than reasoned about.</para>
    /// </summary>
    [SkippableTheory]
    [InlineData("whole_vs_delete_cow")]
    [InlineData("whole_vs_delete_dv")]
    public void WholeTableReader_AbortsAgainstAConcurrentDelete(string scenario)
    {
        var result = _fixture.Result;
        Assert.Equal("ConcurrentAppend", VerdictOf(result, scenario));
    }

    /// <summary>
    /// <b>A whole-table read covers concurrent ADDS in Delta too</b>, not only removes: the same
    /// declaration aborts against a plain blind append that removes nothing.
    ///
    /// <para>This is the Spark counterpart of the finding that decided open question 4 locally. EW's
    /// <c>ReadSet.WholeTable</c> feeds both the delete-read check and the append check, so the proposal to
    /// drop the flag would suppress BOTH — discarding the declaration rather than narrowing it. Delta's
    /// own whole-table read has the same two-sided reach, so that is not an artefact of EW's
    /// implementation: a host that declared a whole-table read and then had it dropped would lose
    /// coverage the reference implementation also provides.</para>
    /// </summary>
    [SkippableFact]
    public void WholeTableReader_AbortsAgainstABlindAppend_SoTheDeclarationCoversAddsToo()
    {
        var result = _fixture.Result;
        Assert.Equal("ConcurrentAppend", VerdictOf(result, "whole_vs_blind_append"));
    }

    /// <summary>The control, and the reason the three above mean anything: the SAME concurrent delete
    /// against a transaction that declared NOTHING commits cleanly. The aborts are caused by the
    /// declaration, not by something incidental to driving a transaction through py4j.</summary>
    [SkippableFact]
    public void UndeclaredTransaction_CommitsThroughTheSameConcurrentDelete()
    {
        var result = _fixture.Result;
        Assert.Equal("committed", VerdictOf(result, "undeclared_vs_delete"));
    }

    /// <summary>A file-level read (Delta's <c>filterFiles()</c>, the analogue of EW's
    /// <see cref="DeltaTransaction.DeclareFilesRead"/> over every file) aborts on the same interleaving — so
    /// the abort does not depend on the read being declared as WHOLE-table. Recorded because it bounds
    /// what "declare something narrower instead" can buy a host, which is exactly what
    /// <see cref="DeltaTransaction.DeclareFilesRead"/> lets one say: narrower helps only when it actually
    /// excludes the racer's files, and here it does not.</summary>
    [SkippableFact]
    public void FileLevelReader_AlsoAbortsAgainstAConcurrentDelete()
    {
        var result = _fixture.Result;
        Assert.Equal("ConcurrentAppend", VerdictOf(result, "filtered_vs_delete_dv"));
    }

    // ── domainMetadata (#109) ──
    //
    // EW's DELTA_DOMAIN_METADATA_CONFLICT rule was written from Delta's `checkIfDomainMetadataConflict`
    // bytecode, because no source checkout exists on the build machines. These three turn that reading
    // into a measurement — the same reason the four above exist.
    //
    // The scenario tables declare `delta.feature.domainMetadata`, and that is load-bearing rather than
    // tidy: Delta's check returns immediately when the protocol lacks the feature, so without it all
    // three would report "committed" and the measurement would mean nothing.

    private static string DomainVerdictOf(JsonElement result, string scenario) =>
        result.GetProperty("domain_scenarios").EnumerateArray()
            .Single(s => s.GetProperty("name").GetString() == scenario)
            .GetProperty("verdict").GetString()!;

    /// <summary>
    /// Two transactions writing the SAME domain: Delta refuses the second. Measured verdict is
    /// <c>ConcurrentTransactionException</c> — "A conflicting metadata domain acme.retention is added."
    ///
    /// <para>This is the rule EW mirrors as <see cref="DeltaErrorCodes.DomainMetadataConflict"/>. EW
    /// gives it a name of its own rather than Delta's <c>DELTA_CONCURRENT_TRANSACTION</c>, whose
    /// catalogued message is entirely about two streaming queries sharing a checkpoint — but the VERDICT
    /// is the one pinned here, and it is the verdict that has to match.</para>
    ///
    /// <para>It is also the rule that made #109 safe rather than merely possible: those five paths used
    /// to be protected by the version collision itself, and giving them a retry without this would have
    /// let the loser silently overwrite an edit its author never saw.</para>
    /// </summary>
    [SkippableFact]
    public void ConcurrentWriteOfTheSameDomain_IsRefusedByDelta()
    {
        var result = _fixture.Result;
        Assert.Equal("ConcurrentTransaction", DomainVerdictOf(result, "domain_same"));
    }

    /// <summary>
    /// The control: two transactions writing DIFFERENT domains both commit. Without this, the case above
    /// would be consistent with "Delta refuses any concurrent domainMetadata", which is a materially
    /// stricter rule than the one EW implemented.
    /// </summary>
    [SkippableFact]
    public void ConcurrentWriteOfADifferentDomain_CommitsCleanly()
    {
        var result = _fixture.Result;
        Assert.Equal("committed", DomainVerdictOf(result, "domain_different"));
    }

    /// <summary>
    /// <b>The row-tracking high-water mark is exempt — measured, not inferred.</b> Two transactions both
    /// writing <c>delta.rowTracking</c> commit, where two writing a user domain do not.
    ///
    /// <para>This is the one with real downside if EW had it backwards. Every commit that adds files to
    /// a row-tracking table advances this domain, so a rule without the exemption would make two ordinary
    /// concurrent appends conflict on a domain neither writer ever named — turning row tracking on would
    /// cost a table its concurrency. EW's <c>ConflictChecker.WrittenDomains</c> excludes it for exactly
    /// the reason Delta's <c>resolveConflict</c> does: the mark is reconciled by a rebase (re-derived
    /// from the version that landed), not contested.</para>
    /// </summary>
    [SkippableFact]
    public void ConcurrentAdvanceOfTheRowTrackingDomain_IsExemptAndCommits()
    {
        var result = _fixture.Result;
        Assert.Equal("committed", DomainVerdictOf(result, "domain_row_tracking"));
    }
}
