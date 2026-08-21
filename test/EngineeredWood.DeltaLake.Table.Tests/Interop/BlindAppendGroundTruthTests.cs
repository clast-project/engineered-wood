// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;

namespace EngineeredWood.DeltaLake.Table.Tests.Interop;

/// <summary>
/// Runs the scenario set ONCE and shares it: five commits plus a Spark session, and every test below
/// reads a different row of the same measurement.
/// </summary>
public sealed class BlindAppendGroundTruthFixture : IDisposable
{
    private readonly string _tempDir;
    private readonly Lazy<JsonElement?> _measured;

    public BlindAppendGroundTruthFixture()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_blindappend_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
        _measured = new Lazy<JsonElement?>(() =>
            Spark.EnsureAvailable()
                ? Spark.Invoke("blind_append_ground_truth", new { path = _tempDir })
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
/// <b>Tier 3.</b> What delta-spark actually records in <c>commitInfo.isBlindAppend</c>, per operation
/// shape — the measurement issue #88 asks for BEFORE the behaviour changes, not after.
///
/// <para>The claim under test is that EW's reader-side inference ("only adds ⇒ blind") disagrees with
/// what Delta records, and that it disagrees in the UNSAFE direction: a statement that READ the table and
/// emitted nothing but adds looks blind to us and is not. That claim was made from reading Delta's source.
/// These tests make it an observation.</para>
///
/// <para>Each case therefore asserts BOTH halves — what Spark recorded, and what the commit's actions look
/// like — because it is the pair that demonstrates the problem. A commit whose file actions are adds only
/// and whose recorded flag is <c>false</c> is a commit EW would exempt from the concurrent-append check
/// and Spark would not.</para>
/// </summary>
[Collection("Interop")]
public class BlindAppendGroundTruthTests : IClassFixture<BlindAppendGroundTruthFixture>
{
    private readonly BlindAppendGroundTruthFixture _fixture;

    public BlindAppendGroundTruthTests(BlindAppendGroundTruthFixture fixture) => _fixture = fixture;

    private static JsonElement Scenario(JsonElement result, string name) =>
        result.GetProperty("scenarios").EnumerateArray()
            .Single(s => s.GetProperty("name").GetString() == name);

    /// <summary>
    /// Spark records the field at all — the premise everything else rests on. If delta-spark stopped
    /// writing it, EW's read side would silently fall back to inference on every Spark table and this
    /// suite would be the only thing that noticed.
    /// </summary>
    [Fact]
    public void Spark_RecordsIsBlindAppend_OnEveryCommit()
    {
        if (_fixture.Result is not { } result) return;

        foreach (var scenario in result.GetProperty("scenarios").EnumerateArray())
        {
            Assert.True(
                scenario.GetProperty("field_present").GetBoolean(),
                $"delta-spark recorded no isBlindAppend for '{scenario.GetProperty("name")}'");
        }
    }

    /// <summary>A genuine blind append — no read, adds only — is recorded <c>true</c>.</summary>
    [Fact]
    public void PlainAppend_IsRecordedBlind()
    {
        if (_fixture.Result is not { } result) return;

        var append = Scenario(result, "append");
        Assert.True(append.GetProperty("is_blind_append").GetBoolean());
        Assert.True(append.GetProperty("only_adds").GetBoolean());
    }

    /// <summary>
    /// ⚠ THE CASE THE INFERENCE GETS WRONG. An insert-only MERGE reads the target to decide what is
    /// missing, so Delta records <c>false</c> — while the commit it produces contains nothing but adds,
    /// which is exactly what EW's inference reads as blind.
    /// </summary>
    [Fact]
    public void InsertOnlyMerge_IsRecordedNotBlind_ThoughItEmitsOnlyAdds()
    {
        if (_fixture.Result is not { } result) return;

        var merge = Scenario(result, "merge_insert_only");
        Assert.False(
            merge.GetProperty("is_blind_append").GetBoolean(),
            "an insert-only MERGE reads the target; Delta should record it as not blind");
        Assert.True(
            merge.GetProperty("only_adds").GetBoolean(),
            "if the MERGE stopped emitting adds only, this stops being the case that breaks the inference "
            + "— which would be worth knowing, but means this test no longer measures what it claims");
    }

    /// <summary>
    /// The same disagreement in plain SQL: <c>INSERT INTO t SELECT … FROM t</c>, the dedupe anti-join.
    /// Two independent statement shapes reaching it is what makes this a class of commit rather than a
    /// quirk of MERGE.
    /// </summary>
    [Fact]
    public void InsertSelectFromSelf_IsRecordedNotBlind_ThoughItEmitsOnlyAdds()
    {
        if (_fixture.Result is not { } result) return;

        var insert = Scenario(result, "insert_select_self");
        Assert.False(insert.GetProperty("is_blind_append").GetBoolean());
        Assert.True(insert.GetProperty("only_adds").GetBoolean());
    }

    /// <summary>
    /// A DELETE is recorded not blind — and here the inference agrees, because the commit carries
    /// removes. The CONTROL: without it, "Spark records false" could be true of everything, and the
    /// tests above would prove nothing about which commits the inference misjudges.
    /// </summary>
    [Fact]
    public void Delete_IsRecordedNotBlind_AndTheInferenceAgrees()
    {
        if (_fixture.Result is not { } result) return;

        var delete = Scenario(result, "delete");
        Assert.False(delete.GetProperty("is_blind_append").GetBoolean());
        Assert.False(
            delete.GetProperty("only_adds").GetBoolean(),
            "a DELETE carries removes, so the shape-based inference reaches the same verdict here");
    }
}
