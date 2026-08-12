// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;

namespace EngineeredWood.DeltaLake.Table.Tests.Interop;

/// <summary>
/// Runs the five commit shapes ONCE and shares the measurement; each test reads a different row of it.
/// </summary>
public sealed class DeltaRsBlindAppendGroundTruthFixture : IDisposable
{
    private readonly string _tempDir;
    private readonly Lazy<JsonElement?> _measured;

    public DeltaRsBlindAppendGroundTruthFixture()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"deltars_blindappend_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
        _measured = new Lazy<JsonElement?>(() =>
            DeltaRs.EnsureAvailable()
                ? DeltaRs.Invoke("blind_append_ground_truth", new { path = _tempDir })
                : null);
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
/// <b>Tier 1.</b> The delta-rs half of the blind-append ground truth — the population that
/// <c>ConflictChecker.InferBlindAppend</c> answers for in full.
///
/// <para><see cref="BlindAppendGroundTruthTests"/> measures delta-spark, which declares
/// <c>commitInfo.isBlindAppend</c> on every commit; there the inference is a fallback that a declaration
/// overrides. delta-rs declares nothing, so on a table it maintains the inference IS the answer, and the
/// two suites are measuring genuinely different exposures rather than the same one twice.</para>
///
/// <para>Each case asserts the pair — what was declared, and what the commit's actions look like — because
/// it is the pair that says which way our inference goes. The insert-only MERGE is the one that motivated
/// #127: adds and a cdc file, no remove, and no declaration to correct it.</para>
/// </summary>
[Collection("Interop")]
public class DeltaRsBlindAppendGroundTruthTests : IClassFixture<DeltaRsBlindAppendGroundTruthFixture>
{
    private readonly DeltaRsBlindAppendGroundTruthFixture _fixture;

    public DeltaRsBlindAppendGroundTruthTests(DeltaRsBlindAppendGroundTruthFixture fixture) =>
        _fixture = fixture;

    private static JsonElement Scenario(JsonElement result, string name) =>
        result.GetProperty("scenarios").EnumerateArray()
            .Single(s => s.GetProperty("name").GetString() == name);

    /// <summary>
    /// The premise the whole issue rests on: delta-rs writes no <c>isBlindAppend</c> on ANY commit shape.
    /// This was read out of its source (an <c>Option&lt;bool&gt;</c> nothing computes); here it is observed.
    /// If delta-rs starts declaring the field, the inference stops being load-bearing for its tables and
    /// this test is how we find out.
    /// </summary>
    [Fact]
    public void DeltaRs_DeclaresIsBlindAppend_OnNoCommitAtAll()
    {
        if (_fixture.Result is not { } result) return;

        foreach (var scenario in result.GetProperty("scenarios").EnumerateArray())
        {
            Assert.False(
                scenario.GetProperty("field_present").GetBoolean(),
                $"delta-rs {result.GetProperty("deltalake")} declared isBlindAppend for "
                + $"'{scenario.GetProperty("name")}' — the inference is no longer its only governor, "
                + "and ConflictChecker.IsBlindAppend's remarks say otherwise");
        }
    }

    /// <summary>
    /// ⚠ THE CASE #127 FIXES. An insert-only MERGE reads the target to decide what is missing, and on a
    /// CDF table delta-rs commits adds plus a cdc file and NO remove. Without the cdc clause every other
    /// part of the inference sees adds only and calls this blind — skipping a concurrent-append check that
    /// is owed, with nothing declared to overrule it.
    /// </summary>
    [Fact]
    public void InsertOnlyMerge_EmitsCdcWithNoRemove_WhichIsTheOnlyEvidenceItRead()
    {
        if (_fixture.Result is not { } result) return;

        var merge = Scenario(result, "merge_insert_only");
        Assert.True(merge.GetProperty("has_cdc").GetBoolean());
        Assert.False(
            merge.GetProperty("has_remove").GetBoolean(),
            "if an insert-only MERGE started carrying removes, the inference would already reach the "
            + "right verdict and this would stop being the case that needed cdc");
    }

    /// <summary>
    /// The control. A plain append on the same CDF-enabled table carries no cdc file, so the new clause
    /// does not fire and an ordinary append stays blind. Without this, the cdc clause could be reading
    /// "CDF is enabled" rather than "this statement changed rows", and every append on such a table would
    /// start conflicting — a far more expensive error than the one being fixed.
    /// </summary>
    [Fact]
    public void PlainAppend_OnTheSameCdfTable_CarriesNoCdc()
    {
        if (_fixture.Result is not { } result) return;

        var append = Scenario(result, "append");
        Assert.False(append.GetProperty("has_cdc").GetBoolean());
        Assert.True(append.GetProperty("only_adds").GetBoolean());
    }

    /// <summary>
    /// UPDATE, DELETE and matched MERGE all carry removes, so the inference already refused them and the
    /// cdc clause changes no verdict here. Worth pinning: it bounds the change to exactly one commit shape,
    /// rather than leaving "adding cdc made things stricter" as an open question across all DML.
    /// </summary>
    [Theory]
    [InlineData("update")]
    [InlineData("delete")]
    [InlineData("merge_matched_update")]
    public void RowChangingDml_AlreadyCarriedRemoves_SoTheVerdictIsUnchanged(string name)
    {
        if (_fixture.Result is not { } result) return;

        var scenario = Scenario(result, name);
        Assert.True(scenario.GetProperty("has_cdc").GetBoolean());
        Assert.True(
            scenario.GetProperty("has_remove").GetBoolean(),
            $"'{name}' no longer carries a remove, so cdc is now the ONLY thing making it not blind — "
            + "which makes the clause load-bearing for a case this test assumed it was not");
    }
}
