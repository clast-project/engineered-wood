// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Data;

namespace EngineeredWood.Tests.Parquet.Data;

public class DictionaryEncoderTests
{
    private static readonly ParquetWriteOptions DefaultOptions = new()
    {
        DictionaryEnabled = true,
        DictionaryPageSizeLimit = 1024 * 1024,
    };

    [Fact]
    public void TryEncode_LowCardinality_ReturnsDictionary()
    {
        // 100 values with 3 unique → 3.3% cardinality, under 20% threshold
        var builder = new Int32Array.Builder();
        for (int i = 0; i < 100; i++) builder.Append(i % 3);
        var array = builder.Build();

        var result = DictionaryEncoder.TryEncode(
            array, PhysicalType.Int32, 0, null, 100, DefaultOptions);

        Assert.NotNull(result);
        Assert.Equal(3, result.Value.DictionaryCount);
        Assert.Equal(100, Indices(result.Value).Length);
        Assert.Equal(3 * 4, result.Value.DictionaryPageData.Length); // 3 int32s
    }

    [Fact]
    public void TryEncode_HighCardinality_ReturnsNull()
    {
        // 100 unique values out of 100 → 100% cardinality
        var builder = new Int32Array.Builder();
        for (int i = 0; i < 100; i++) builder.Append(i);
        var array = builder.Build();

        var result = DictionaryEncoder.TryEncode(
            array, PhysicalType.Int32, 0, null, 100, DefaultOptions);

        Assert.Null(result);
    }

    [Fact]
    public void TryEncode_Boolean_ReturnsNull()
    {
        var builder = new BooleanArray.Builder();
        for (int i = 0; i < 100; i++) builder.Append(i % 2 == 0);
        var array = builder.Build();

        var result = DictionaryEncoder.TryEncode(
            array, PhysicalType.Boolean, 0, null, 100, DefaultOptions);

        Assert.Null(result);
    }

    [Fact]
    public void TryEncode_DictionaryDisabled_ReturnsNull()
    {
        var options = new ParquetWriteOptions { DictionaryEnabled = false };
        var builder = new Int32Array.Builder();
        for (int i = 0; i < 100; i++) builder.Append(i % 3);
        var array = builder.Build();

        var result = DictionaryEncoder.TryEncode(
            array, PhysicalType.Int32, 0, null, 100, options);

        Assert.Null(result);
    }

    [Fact]
    public void TryEncode_WithNulls_IndexesOnlyNonNull()
    {
        // 50 rows: every 5th is null, values cycle through 10 and 20
        // → 40 non-null values, 2 unique → 5% cardinality, under threshold
        var builder = new Int32Array.Builder();
        var defLevels = new int[50];
        int nonNullCount = 0;
        for (int i = 0; i < 50; i++)
        {
            if (i % 5 == 0)
            {
                builder.AppendNull();
                defLevels[i] = 0;
            }
            else
            {
                builder.Append(i % 2 == 0 ? 10 : 20);
                defLevels[i] = 1;
                nonNullCount++;
            }
        }

        var array = builder.Build();

        var result = DictionaryEncoder.TryEncode(
            array, PhysicalType.Int32, 0, defLevels, nonNullCount, DefaultOptions);

        Assert.NotNull(result);
        Assert.Equal(2, result.Value.DictionaryCount); // 10 and 20
        Assert.Equal(nonNullCount, Indices(result.Value).Length);
    }

    [Fact]
    public void TryEncode_StringColumn_LowCardinality()
    {
        var builder = new StringArray.Builder();
        for (int i = 0; i < 100; i++) builder.Append(i % 4 == 0 ? "alpha" : i % 4 == 1 ? "beta" : i % 4 == 2 ? "gamma" : "delta");
        var array = builder.Build();

        var result = DictionaryEncoder.TryEncode(
            array, PhysicalType.ByteArray, 0, null, 100, DefaultOptions);

        Assert.NotNull(result);
        Assert.Equal(4, result.Value.DictionaryCount);
        Assert.Equal(100, Indices(result.Value).Length);
    }

    [Fact]
    public void TryEncode_DictionaryPageSizeLimit_Exceeded()
    {
        // Very small page size limit that can't fit the dictionary
        var options = new ParquetWriteOptions
        {
            DictionaryEnabled = true,
            DictionaryPageSizeLimit = 4, // only room for 1 int32
        };

        var builder = new Int32Array.Builder();
        for (int i = 0; i < 100; i++) builder.Append(i % 5); // 5 unique values = 20 bytes
        var array = builder.Build();

        var result = DictionaryEncoder.TryEncode(
            array, PhysicalType.Int32, 0, null, 100, options);

        Assert.Null(result);
    }

    [Fact]
    public void TryEncode_AllSameValue_SingleEntry()
    {
        var builder = new Int64Array.Builder();
        for (int i = 0; i < 100; i++) builder.Append(42);
        var array = builder.Build();

        var result = DictionaryEncoder.TryEncode(
            array, PhysicalType.Int64, 0, null, 100, DefaultOptions);

        Assert.NotNull(result);
        Assert.Equal(1, result.Value.DictionaryCount);
        Assert.Equal(8, result.Value.DictionaryPageData.Length); // 1 int64
        Assert.All(Indices(result.Value), idx => Assert.Equal(0, idx));
    }

    // ── The constant-column fast path ──
    //
    // A column holding one value in every row skips the per-row hashing entirely: uniform value length is
    // checked in O(1), then the value buffer is compared against itself shifted by one value. The result
    // must be indistinguishable from what the hashing loop would have produced — one dictionary entry and an
    // index of 0 for every row — because the encoding is what lands in the file.

    /// <summary>
    /// The per-row indices of a result, asserting they are there. Only the run-encoded arm leaves them
    /// null, in favour of <c>IndexRuns</c>; every path these tests exercise fills them in.
    /// </summary>
    private static int[] Indices(DictionaryEncoder.DictionaryResult result) =>
        result.Indices ?? throw new Xunit.Sdk.XunitException(
            "Expected per-row dictionary indices, got the run form.");

    private static StringArray Strings(params string[] values)
    {
        var b = new StringArray.Builder();
        foreach (var v in values) b.Append(v);
        return b.Build();
    }

    [Fact]
    public void TryEncode_ConstantStringColumn_MatchesWhatHashingWouldProduce()
    {
        var array = Strings(Enumerable.Repeat("update_postimage", 64).ToArray());

        var result = DictionaryEncoder.TryEncode(
            array, PhysicalType.ByteArray, 0, null, 64, DefaultOptions);

        Assert.NotNull(result);
        Assert.Equal(1, result.Value.DictionaryCount);
        Assert.Equal(64, Indices(result.Value).Length);
        Assert.All(Indices(result.Value), idx => Assert.Equal(0, idx));
        // PLAIN dictionary page: 4-byte LE length prefix, then the value.
        Assert.Equal(4 + 16, result.Value.DictionaryPageData.Length);
        Assert.Equal("update_postimage",
            System.Text.Encoding.UTF8.GetString(result.Value.DictionaryPageData, 4, 16));
    }

    [Fact]
    public void TryEncode_VaryingLengthsOverRepeatingBytes_IsNotMistakenForConstant()
    {
        // The case the offset walk exists for. Lengths 2,1,3 over a run of 'a's hold THREE different values,
        // yet the two cheap checks both pass: the lengths average out so uniform-length is not refuted, and
        // the value buffer is trivially periodic in any period. Without the walk this encodes as one value
        // repeated — a corrupt file rather than a failure.
        //
        // Repeated to 30 rows so the 3 distinct values stay under the cardinality threshold; at 3 rows
        // TryEncode bails on cardinality before reaching any of this.
        var pattern = new[] { "aa", "a", "aaa" };
        var array = Strings(Enumerable.Range(0, 30).Select(i => pattern[i % 3]).ToArray());

        var result = DictionaryEncoder.TryEncode(
            array, PhysicalType.ByteArray, 0, null, 30, DefaultOptions);

        Assert.NotNull(result);
        Assert.Equal(3, result.Value.DictionaryCount);
        Assert.Equal([0, 1, 2, 0, 1, 2], Indices(result.Value).Take(6));
    }

    [Fact]
    public void TryEncode_SameLengthDifferentValues_IsNotMistakenForConstant()
    {
        // Uniform length, so the O(1) check cannot refute it; the periodicity compare is what does, at the
        // first differing byte. Repeated to 40 rows to clear the cardinality threshold.
        var pattern = new[] { "aaa", "bbb", "aaa", "ccc" };
        var array = Strings(Enumerable.Range(0, 40).Select(i => pattern[i % 4]).ToArray());

        var result = DictionaryEncoder.TryEncode(
            array, PhysicalType.ByteArray, 0, null, 40, DefaultOptions);

        Assert.NotNull(result);
        Assert.Equal(3, result.Value.DictionaryCount);
        Assert.Equal([0, 1, 0, 2], Indices(result.Value).Take(4));
    }

    [Fact]
    public void TryEncode_AllEmptyStrings_IsConstant()
    {
        // Value length 0 — the buffer is empty, so periodicity is vacuous and only the offset walk speaks.
        var array = Strings("", "", "");

        var result = DictionaryEncoder.TryEncode(
            array, PhysicalType.ByteArray, 0, null, 3, DefaultOptions);

        Assert.NotNull(result);
        Assert.Equal(1, result.Value.DictionaryCount);
        Assert.Equal(4, result.Value.DictionaryPageData.Length); // length prefix only
        Assert.Equal([0, 0, 0], Indices(result.Value));
    }

    [Fact]
    public void TryEncode_ConstantColumnWithNulls_FallsBackToHashing()
    {
        // The probe reads every row, so it is only valid with no nulls. With def levels present the column
        // must still encode correctly — through the hashing loop, which skips null rows.
        var array = Strings("x", "x", "x", "x");
        int[] defLevels = [1, 0, 1, 1];

        var result = DictionaryEncoder.TryEncode(
            array, PhysicalType.ByteArray, 0, defLevels, 3, DefaultOptions);

        Assert.NotNull(result);
        Assert.Equal(1, result.Value.DictionaryCount);
        Assert.Equal(3, Indices(result.Value).Length); // one per NON-NULL row
    }

    [Fact]
    public void TryEncode_ConstantFixedWidthColumn_MatchesWhatHashingWouldProduce()
    {
        var b = new Int64Array.Builder();
        for (int i = 0; i < 64; i++) b.Append(7L);

        var result = DictionaryEncoder.TryEncode(
            b.Build(), PhysicalType.Int64, 0, null, 64, DefaultOptions);

        Assert.NotNull(result);
        Assert.Equal(1, result.Value.DictionaryCount);
        Assert.Equal(8, result.Value.DictionaryPageData.Length);
        Assert.Equal(7L, BitConverter.ToInt64(result.Value.DictionaryPageData, 0));
        Assert.All(Indices(result.Value), idx => Assert.Equal(0, idx));
    }

    [Fact]
    public void TryEncode_SingleRow_IsConstant()
    {
        var result = DictionaryEncoder.TryEncode(
            Strings("only"), PhysicalType.ByteArray, 0, null, 1, DefaultOptions);

        Assert.NotNull(result);
        Assert.Equal(1, result.Value.DictionaryCount);
        Assert.Equal([0], Indices(result.Value));
    }

    // ── The run-end encoded arm ──
    //
    // A run-encoded column is hashed once per RUN and its indices come out in run form, so both the work
    // and the allocation are O(runs). What must not differ is the dictionary itself: the same entries, in
    // the same order, over the same non-null rows the per-row arms would have seen.

    /// <summary>A run-encoded string column from (value, row count) runs; a null value nulls the run.</summary>
    private static RunEndEncodedArray StringRuns(params (string? Value, int Length)[] runs)
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

    /// <summary>Asserts the run-form indices of a result, value by value.</summary>
    private static void AssertRuns(
        int[] values, int[] lengths, DictionaryEncoder.DictionaryResult result)
    {
        var runs = result.IndexRuns ?? throw new Xunit.Sdk.XunitException(
            "Expected run-form dictionary indices, got the per-row form.");

        Assert.Equal(values, runs.Values);
        Assert.Equal(lengths, runs.Lengths);
    }

    [Fact]
    public void TryEncode_ConstantRunEndEncodedColumn_IsOneEntryAndOneRun()
    {
        var result = DictionaryEncoder.TryEncode(
            StringRuns(("update_postimage", 1_000_000)),
            PhysicalType.ByteArray, 0, null, 1_000_000, DefaultOptions);

        Assert.NotNull(result);
        Assert.Equal(1, result.Value.DictionaryCount);

        // The whole point: a million rows and nothing per-row was allocated to describe them.
        Assert.Null(result.Value.Indices);
        AssertRuns([0], [1_000_000], result.Value);

        // Same PLAIN dictionary page the per-row arm produces.
        Assert.Equal(4 + 16, result.Value.DictionaryPageData.Length);
        Assert.Equal("update_postimage",
            System.Text.Encoding.UTF8.GetString(result.Value.DictionaryPageData, 4, 16));
    }

#if NET6_0_OR_GREATER
    [Fact]
    public void TryEncode_ConstantRunEndEncodedColumn_AllocatesNothingPerRow()
    {
        // The whole justification for the run arm, and the one property no other test here can see: it is
        // O(runs) in ALLOCATION as well as in time. Two regressions this catches, both of which leave
        // every other assertion in this file passing — materializing an index per row (4 MB), and sizing
        // the hash table from the cardinality cap the way the per-row arms must (8 MB).
        //
        // .NET Framework has no per-thread allocation counter, so the pin runs on the modern targets only.
        var array = StringRuns(("update_postimage", 1_000_000));

        DictionaryEncoder.TryEncode(array, PhysicalType.ByteArray, 0, null, 1_000_000, DefaultOptions);

        long before = GC.GetAllocatedBytesForCurrentThread();
        var result = DictionaryEncoder.TryEncode(
            array, PhysicalType.ByteArray, 0, null, 1_000_000, DefaultOptions);
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.NotNull(result);
        Assert.True(allocated < 64 * 1024,
            $"Encoding a 1,000,000-row constant column allocated {allocated:N0} bytes.");
    }
#endif

    [Fact]
    public void TryEncode_MultiRunColumn_BuildsTheDictionaryFromTheRunValues()
    {
        var result = DictionaryEncoder.TryEncode(
            StringRuns(("a", 100), ("b", 50), ("a", 30)),
            PhysicalType.ByteArray, 0, null, 180, DefaultOptions);

        Assert.NotNull(result);
        Assert.Equal(2, result.Value.DictionaryCount);
        AssertRuns([0, 1, 0], [100, 50, 30], result.Value);
    }

    [Fact]
    public void TryEncode_NullRuns_ContributeNoIndices()
    {
        // A null run adds nothing to the index stream, exactly as a def level of 0 does in the per-row
        // arms — and the nulls come from the run's VALUE, since the array's own IsNull answers false for
        // every row.
        var result = DictionaryEncoder.TryEncode(
            StringRuns(("a", 10), (null, 5), ("b", 10)),
            PhysicalType.ByteArray, 0, null, 20, DefaultOptions);

        Assert.NotNull(result);
        Assert.Equal(2, result.Value.DictionaryCount);
        AssertRuns([0, 1], [10, 10], result.Value);
    }

    [Fact]
    public void TryEncode_RunsRejoinedByANullRun_MergeIntoOneIndexRun()
    {
        // The two "a" runs are adjacent once the null between them contributes nothing, and the RLE
        // encoder downstream cannot merge what it is handed as two.
        var result = DictionaryEncoder.TryEncode(
            StringRuns(("a", 10), (null, 5), ("a", 10)),
            PhysicalType.ByteArray, 0, null, 20, DefaultOptions);

        Assert.NotNull(result);
        Assert.Equal(1, result.Value.DictionaryCount);
        AssertRuns([0], [20], result.Value);
    }

    [Fact]
    public void TryEncode_RunsDisagreeingWithTheCallersNonNullCount_Decline()
    {
        // The caller derives its definition levels from these same runs, so a disagreement means the two
        // views of the column have diverged. Declining sends it down the expansion path rather than
        // writing an index stream the levels cannot address.
        var result = DictionaryEncoder.TryEncode(
            StringRuns(("a", 10), (null, 5)),
            PhysicalType.ByteArray, 0, null, 15, DefaultOptions);

        Assert.Null(result);
    }

    [Fact]
    public void TryEncode_HighCardinalityRuns_Decline()
    {
        // The threshold is measured against the ROW count, not the run count — otherwise a five-run
        // column of a million rows would be rejected as 100% cardinality.
        var runs = new (string?, int)[50];
        for (int i = 0; i < runs.Length; i++)
            runs[i] = ($"value-{i}", 1);

        Assert.Null(DictionaryEncoder.TryEncode(
            StringRuns(runs), PhysicalType.ByteArray, 0, null, 50, DefaultOptions));

        // The same fifty distinct values over enough rows to clear the threshold do encode.
        for (int i = 0; i < runs.Length; i++)
            runs[i] = ($"value-{i}", 20);

        Assert.NotNull(DictionaryEncoder.TryEncode(
            StringRuns(runs), PhysicalType.ByteArray, 0, null, 1000, DefaultOptions));
    }

    [Fact]
    public void TryEncode_RunsOfALayoutTheArmCannotRead_Decline()
    {
        // LargeString maps to the BYTE_ARRAY physical type alongside String, but lays its offsets out at
        // 64 bits. Reading those as 32-bit offsets is silent corruption, so the arm declines instead and
        // the caller expands the column.
        var values = new LargeStringArray.Builder().Append("a").Build();
        var ends = new Int32Array.Builder().Append(100).Build();

        Assert.Null(DictionaryEncoder.TryEncode(
            new RunEndEncodedArray(ends, values),
            PhysicalType.ByteArray, 0, null, 100, DefaultOptions));
    }

    [Fact]
    public void TryEncode_RunsOfANarrowerTypeThanTheirPhysicalWidth_Decline()
    {
        // Int16 is written as the 4-byte INT32 physical type; its buffer is 2 bytes per value. The caller
        // widens such a column before encoding it, and this is what keeps that a requirement.
        var values = new Int16Array.Builder().Append((short)3).Build();
        var ends = new Int32Array.Builder().Append(100).Build();

        Assert.Null(DictionaryEncoder.TryEncode(
            new RunEndEncodedArray(ends, values),
            PhysicalType.Int32, 0, null, 100, DefaultOptions));
    }

    [Fact]
    public void TryEncode_Int64Runs_EncodeAgainstTheRawValueSlots()
    {
        var values = new Int64Array.Builder().Append(7L).Append(-3L).Build();
        var ends = new Int32Array.Builder().Append(40).Append(100).Build();

        var result = DictionaryEncoder.TryEncode(
            new RunEndEncodedArray(ends, values),
            PhysicalType.Int64, 0, null, 100, DefaultOptions);

        Assert.NotNull(result);
        Assert.Equal(2, result.Value.DictionaryCount);
        Assert.Equal(16, result.Value.DictionaryPageData.Length);
        Assert.Equal(7L, BitConverter.ToInt64(result.Value.DictionaryPageData, 0));
        Assert.Equal(-3L, BitConverter.ToInt64(result.Value.DictionaryPageData, 8));
        AssertRuns([0, 1], [40, 60], result.Value);
    }

    [Fact]
    public void TryEncode_ARunEncodedSlice_SeesOnlyTheRowsItExposes()
    {
        var sliced = (RunEndEncodedArray)StringRuns(("a", 30), ("b", 40)).Slice(20, 30);

        var result = DictionaryEncoder.TryEncode(
            sliced, PhysicalType.ByteArray, 0, null, 30, DefaultOptions);

        Assert.NotNull(result);
        Assert.Equal(2, result.Value.DictionaryCount);
        AssertRuns([0, 1], [10, 20], result.Value);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(8)]
    [InlineData(16)]
    [InlineData(255)]
    public void GetIndexBitWidth_CorrectForDictionarySize(int dictCount)
    {
        int bitWidth = DictionaryEncoder.GetIndexBitWidth(dictCount);
        Assert.True(bitWidth >= 1);
        // Verify all indices 0..(dictCount-1) fit in bitWidth bits
        int maxIndex = dictCount - 1;
        Assert.True(maxIndex < (1 << bitWidth),
            $"dictCount={dictCount}, bitWidth={bitWidth}, maxIndex={maxIndex}");
    }
}
