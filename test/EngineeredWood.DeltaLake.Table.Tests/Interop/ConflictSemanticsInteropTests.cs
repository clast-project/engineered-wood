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
            Spark.EnsureAvailable() ? Spark.Invoke("conflict_semantics", new { path = _tempDir }) : null);
    }

    /// <summary>The measurement, or null when the toolchain is absent (the tests then self-skip).</summary>
    public JsonElement? Result => _measured.Value;

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
    [Fact]
    public void OssDelta_AcceptsOnlySerializable()
    {
        if (_fixture.Result is not { } result) return;

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
    [Theory]
    [InlineData("whole_vs_delete_cow")]
    [InlineData("whole_vs_delete_dv")]
    public void WholeTableReader_AbortsAgainstAConcurrentDelete(string scenario)
    {
        if (_fixture.Result is not { } result) return;
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
    [Fact]
    public void WholeTableReader_AbortsAgainstABlindAppend_SoTheDeclarationCoversAddsToo()
    {
        if (_fixture.Result is not { } result) return;
        Assert.Equal("ConcurrentAppend", VerdictOf(result, "whole_vs_blind_append"));
    }

    /// <summary>The control, and the reason the three above mean anything: the SAME concurrent delete
    /// against a transaction that declared NOTHING commits cleanly. The aborts are caused by the
    /// declaration, not by something incidental to driving a transaction through py4j.</summary>
    [Fact]
    public void UndeclaredTransaction_CommitsThroughTheSameConcurrentDelete()
    {
        if (_fixture.Result is not { } result) return;
        Assert.Equal("committed", VerdictOf(result, "undeclared_vs_delete"));
    }

    /// <summary>A file-level read (Delta's <c>filterFiles()</c>, the analogue of EW's
    /// <see cref="DeltaTransaction.DeclareFilesRead"/> over every file) aborts on the same interleaving — so
    /// the abort does not depend on the read being declared as WHOLE-table. Recorded because it bounds
    /// what "declare something narrower instead" can buy a host, which is exactly what
    /// <see cref="DeltaTransaction.DeclareFilesRead"/> lets one say: narrower helps only when it actually
    /// excludes the racer's files, and here it does not.</summary>
    [Fact]
    public void FileLevelReader_AlsoAbortsAgainstAConcurrentDelete()
    {
        if (_fixture.Result is not { } result) return;
        Assert.Equal("ConcurrentAppend", VerdictOf(result, "filtered_vs_delete_dv"));
    }
}
