// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Arrow;

namespace EngineeredWood.Core.Tests.Arrow;

/// <summary>
/// Run-end encoding stores a column as (run end, value) pairs, so a constant column costs a handful of
/// bytes whatever its row count. These pin the three things a consumer has to get right: the runs are
/// clipped to the array's own window (a slice is a VIEW over children that still hold every run), nulls
/// live in the values child rather than a validity bitmap, and gathering preserves the representation.
/// </summary>
public class RunEndEncodingTests
{
    /// <summary>Builds an array from (value, row count) runs; a null value makes the whole run null.</summary>
    private static RunEndEncodedArray Ree(params (string? Value, int Length)[] runs)
    {
        var values = new StringArray.Builder();
        var ends = new Int32Array.Builder();
        int end = 0;

        foreach (var (value, length) in runs)
        {
            if (value is null) values.AppendNull();
            else values.Append(value);

            end += length;
            ends.Append(end);
        }

        return new RunEndEncodedArray(ends.Build(), values.Build());
    }

    /// <summary>The logical rows of an array, expanded, as nullable strings.</summary>
    private static string?[] Rows(RunEndEncodedArray array)
    {
        var expanded = (StringArray)RunEndEncoding.Expand(array);
        var rows = new string?[expanded.Length];
        for (int i = 0; i < expanded.Length; i++)
            rows[i] = expanded.IsNull(i) ? null : expanded.GetString(i);

        return rows;
    }

    /// <summary>Asserts the expanded rows of an array, nulls included.</summary>
    private static void AssertRows(string?[] expected, RunEndEncodedArray array)
    {
        var actual = Rows(array);
        Assert.Equal(expected.Length, actual.Length);
        for (int i = 0; i < expected.Length; i++)
            Assert.Equal(expected[i], actual[i]);
    }

    private static (int Physical, int Length)[] RunsOf(RunEndEncodedArray array)
    {
        var runs = new List<(int, int)>();
        foreach (var run in RunEndEncoding.EnumerateRuns(array))
            runs.Add((run.PhysicalIndex, run.Length));

        return runs.ToArray();
    }

    // ── Constant ──

    [Fact]
    public void Constant_IsOneRun_WhateverTheRowCount()
    {
        var array = RunEndEncoding.Constant(
            StringType.Default, System.Text.Encoding.UTF8.GetBytes("update_postimage"), 1_000_000);

        Assert.Equal(1_000_000, array.Length);
        Assert.Equal(1, array.Values.Length);
        Assert.Equal(1_000_000, RunEndEncoding.RunEndAt(array.RunEnds, 0));

        // The point of the exercise: the whole column is one value and one run end, not a million slots.
        Assert.Equal(16, array.Values.Data.Buffers[2].Length);
    }

    [Fact]
    public void Constant_ExpandsToWhatRepeatWouldHaveBuilt()
    {
        byte[] value = System.Text.Encoding.UTF8.GetBytes("delete");

        var expanded = (StringArray)RunEndEncoding.Expand(
            RunEndEncoding.Constant(StringType.Default, value, 7));
        var tiled = (StringArray)ArrowCompute.Repeat(StringType.Default, value, 7);

        Assert.Equal(tiled.Length, expanded.Length);
        for (int i = 0; i < tiled.Length; i++)
            Assert.Equal(tiled.GetString(i), expanded.GetString(i));
    }

    [Fact]
    public void Constant_ZeroRows_HasNoRunsAtAll()
    {
        // Not one run of length zero: a run end of 0 would describe a run covering no rows, which the
        // layout does not admit.
        var array = RunEndEncoding.Constant(StringType.Default, [1, 2, 3], 0);

        Assert.Equal(0, array.Length);
        Assert.Equal(0, array.Values.Length);
        Assert.Empty(RunsOf(array));
    }

    // ── Nulls ──

    /// <summary>
    /// The trap this whole layout sets. A run-end encoded array has no validity bitmap, so Arrow's own
    /// IsNull answers FALSE for every row of a column that is entirely null — the nulls are in the values
    /// child, one per run. Anything that derives definition levels, statistics or a null count from
    /// IsNull is silently wrong on such a column.
    /// </summary>
    [Fact]
    public void IsNull_AnswersFalseForANullRun_WhichIsWhyNullsAreReadFromTheValues()
    {
        var array = Ree(("a", 2), (null, 3));

        Assert.False(array.IsNull(3));
        Assert.False(array.IsNull(4));
        Assert.Equal(0, array.Data.NullCount);

        // The value the run points at is where the truth is, and Expand puts it back where Arrow expects.
        AssertRows(["a", "a", null, null, null], array);
    }

    [Fact]
    public void Expand_RestoresNullsToTheValidityBitmap()
    {
        var expanded = (StringArray)RunEndEncoding.Expand(Ree((null, 2), ("x", 1), (null, 1)));

        Assert.Equal(4, expanded.Length);
        Assert.Equal(3, expanded.NullCount);
        Assert.True(expanded.IsNull(0));
        Assert.Equal("x", expanded.GetString(2));
    }

    // ── Windows ──

    [Fact]
    public void EnumerateRuns_OverASlice_ClipsToTheWindow()
    {
        // Rows 0-2 "a", 3-6 "b", 7-8 "c"; the slice takes rows 2..6.
        var sliced = (RunEndEncodedArray)Ree(("a", 3), ("b", 4), ("c", 2)).Slice(2, 5);

        // Both ends clipped: one row of the "a" run, four of the "b" run, none of "c".
        Assert.Equal([(0, 1), (1, 4)], RunsOf(sliced));
        AssertRows(["a", "b", "b", "b", "b"], sliced);
    }

    [Fact]
    public void Compact_OfASlice_RebuildsTheRunsAndDropsTheUnreachableValues()
    {
        var sliced = (RunEndEncodedArray)Ree(("a", 3), ("b", 4), ("c", 2)).Slice(2, 5);
        var compacted = RunEndEncoding.Compact(sliced);

        Assert.Equal(0, compacted.Data.Offset);
        Assert.Equal(5, compacted.Length);

        // "c" was outside the window, so it is gone rather than carried along unreferenced.
        Assert.Equal(2, compacted.Values.Length);
        Assert.Equal(5, RunEndEncoding.RunEndAt(compacted.RunEnds, 1));
        AssertRows(["a", "b", "b", "b", "b"], compacted);
    }

    [Fact]
    public void Compact_OfAWholeArray_ReturnsItUntouched()
    {
        var array = Ree(("a", 3), ("b", 4));

        Assert.Same(array, RunEndEncoding.Compact(array));
    }

    [Fact]
    public void Compact_OfALeadingSlice_StillTrimsTheRunsPastTheEnd()
    {
        // Offset zero, but the children describe more rows than the array exposes — the case a
        // Data.Offset check alone would wave through.
        var sliced = (RunEndEncodedArray)Ree(("a", 3), ("b", 4)).Slice(0, 4);
        var compacted = RunEndEncoding.Compact(sliced);

        Assert.Equal(4, compacted.Length);
        Assert.Equal(4, RunEndEncoding.RunEndAt(compacted.RunEnds, compacted.Values.Length - 1));
        AssertRows(["a", "a", "a", "b"], compacted);
    }

    // ── Gathering ──

    [Fact]
    public void Take_CoalescesGatheredRowsBackIntoRuns()
    {
        var taken = RunEndEncoding.Take(Ree(("a", 3), ("b", 4), ("c", 2)), [0, 1, 4, 5, 6, 8]);

        Assert.Equal(6, taken.Length);
        Assert.Equal([(0, 2), (1, 3), (2, 1)], RunsOf(taken));
        AssertRows(["a", "a", "b", "b", "b", "c"], taken);
    }

    [Fact]
    public void Take_ThroughArrowCompute_KeepsTheColumnRunEndEncoded()
    {
        // Type stability is the contract: ArrowCompute.Take is handed a batch schema that declares the
        // run-end encoded type, and a column whose type disagrees with its field is a mismatch nothing
        // downstream checks.
        var taken = ArrowCompute.Take(Ree(("a", 3), ("b", 4)), new[] { 6, 5, 0 });

        var ree = Assert.IsType<RunEndEncodedArray>(taken);
        AssertRows(["b", "b", "a"], ree);
    }

    [Fact]
    public void Take_InAnOrderThatBreaksEveryRun_YieldsARunPerRow()
    {
        // The worst case, pinned deliberately: the representation is preserved rather than swapped for
        // the smaller plain one, and here that is bigger than what it replaced.
        var taken = RunEndEncoding.Take(Ree(("a", 2), ("b", 2)), [0, 2, 1, 3]);

        Assert.Equal([(0, 1), (1, 1), (2, 1), (3, 1)], RunsOf(taken));
        AssertRows(["a", "b", "a", "b"], taken);
    }

    [Fact]
    public void Take_OfNothing_IsAnEmptyArray()
    {
        var taken = RunEndEncoding.Take(Ree(("a", 3)), []);

        Assert.Equal(0, taken.Length);
        Assert.Empty(RunsOf(taken));
    }

    [Fact]
    public void Take_CarriesNullRunsThrough()
    {
        var taken = RunEndEncoding.Take(Ree(("a", 2), (null, 2)), [3, 2, 0]);

        AssertRows([null, null, "a"], taken);
    }

    // ── Run end widths ──

    [Fact]
    public void RunEndAt_ReadsEveryWidthTheSpecAllows()
    {
        var int16 = new Int16Array.Builder().Append((short)5).Build();
        var int32 = new Int32Array.Builder().Append(5).Build();
        var int64 = new Int64Array.Builder().Append(5L).Build();

        Assert.Equal(5L, RunEndEncoding.RunEndAt(int16, 0));
        Assert.Equal(5L, RunEndEncoding.RunEndAt(int32, 0));
        Assert.Equal(5L, RunEndEncoding.RunEndAt(int64, 0));
    }

    [Fact]
    public void EnumerateRuns_ReadsInt64RunEnds()
    {
        var values = new StringArray.Builder().Append("a").Append("b").Build();
        var ends = new Int64Array.Builder().Append(2L).Append(5L).Build();

        Assert.Equal([(0, 2), (1, 3)], RunsOf(new RunEndEncodedArray(ends, values)));
    }
}
