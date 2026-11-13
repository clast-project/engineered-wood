// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.InteropServices;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Expressions.Sql;

namespace EngineeredWood.Expressions.Arrow.Tests;

/// <summary>
/// A string or binary column answers the same whichever layout a reader handed it in: 32-bit offsets, 64-bit offsets
/// or views (#859). Before, a <c>utf8view</c> or <c>large_utf8</c> column raised "Cannot evaluate". Every expression
/// here runs over each layout, under each evaluator, with the direct comparison path on and off, and must give the
/// plain layout's outcome.
/// </summary>
public class ByteArrayLayoutTests
{
    public enum Layout { Offsets, LargeOffsets, Views }

    private static readonly string?[] S = ["", "ab", "abc", "é", "😀", "2026-10-01", "12", null, "a long string past the view inline limit"];
    private static readonly string?[] T = ["ab", "ab", "abd", "é", null, "x", "12", "q", "a long string past the view inline limit"];

    // One list per row; null is a null list.
    private static readonly string?[]?[] L =
        [["ab", "x"], [], null, ["abc"], ["é", null], ["12"], ["q"], ["ab"], ["a long string past the view inline limit"]];

    private static readonly bool[] StructValid = [true, true, true, false, true, true, true, true, true];

    private static readonly string[] Predicates =
    [
        "s = 'ab'", "s <> 'ab'", "s < 'abc'", "s >= 'é'", "s <=> 'ab'", "s <=> NULL",
        "s = 'a long string past the view inline limit'", "s > 'a long'",
        "s IN ('ab', 'abc', 'zz')", "s NOT IN ('ab')", "s IN ('ab', NULL)",
        "s LIKE 'ab%'", "s LIKE '%c'", "s NOT LIKE 'a%'",
        "s = t", "s < t", "s <=> t", "t = s",
        "s = 12", "s > 5", "CAST(s AS INT) = 12",
        "s IS NULL", "s IS NOT NULL",
        "upper(s) = 'AB'", "length(s) > 1", "concat(s, t) = 'abab'", "substring(s, 1, 1) = 'a'",
        "coalesce(s, t) = 'x'",
        "bin = X'6162'", "bin < X'62'", "bin = CAST(s AS BINARY)", "bin IN (X'6162', X'00')",
        "ls IS NULL", "ls IS NOT NULL",
        "st.s = 'ab'", "st.s IS NULL", "st.s LIKE 'a%'",
    ];

    public static TheoryData<Layout, string> Cases()
    {
        var data = new TheoryData<Layout, string>();
        foreach (var layout in new[] { Layout.LargeOffsets, Layout.Views })
        {
            foreach (var sql in Predicates)
                data.Add(layout, sql);
        }
        return data;
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public void APredicate_AnswersAsOverOffsets(Layout layout, string sql)
    {
        var predicate = SparkSqlParser.ParsePredicate(sql);
        foreach (var slice in new[] { false, true })
        {
            var expected = Batch(Layout.Offsets, slice);
            var actual = Batch(layout, slice);
            for (int e = 0; e < DirectColumnComparisonTests.Evaluators.Length; e++)
            {
                var want = Outcome(DirectColumnComparisonTests.Evaluators[e], false, predicate, expected);
                foreach (var direct in new[] { false, true })
                {
                    var got = Outcome(DirectColumnComparisonTests.Evaluators[e], direct, predicate, actual);
                    Assert.True(want == got,
                        $"{sql} [{DirectColumnComparisonTests.EvaluatorNames[e]}, {layout}, direct {direct}, " +
                        $"sliced {slice}]: expected {want}, got {got}");
                }
            }
        }
    }

    [Theory]
    [InlineData(Layout.LargeOffsets)]
    [InlineData(Layout.Views)]
    public void AStartsWith_AnswersAsOverOffsets(Layout layout)
    {
        foreach (var op in new[] { ComparisonOperator.StartsWith, ComparisonOperator.NotStartsWith })
        {
            var predicate = new ComparisonPredicate(Expressions.Ref("s"), op, Expressions.Lit(LiteralValue.Of("ab")));
            for (int e = 0; e < DirectColumnComparisonTests.Evaluators.Length; e++)
            {
                var want = Outcome(DirectColumnComparisonTests.Evaluators[e], false, predicate, Batch(Layout.Offsets, false));
                Assert.Equal(want, Outcome(DirectColumnComparisonTests.Evaluators[e], true, predicate, Batch(layout, false)));
            }
        }
    }

    /// <summary>A view or large string column is compared in place, by the direct kernel, not declined to the
    /// general path (which copies it).</summary>
    [Theory]
    [InlineData(Layout.LargeOffsets, "s = 'ab'")]
    [InlineData(Layout.Views, "s = 'ab'")]
    [InlineData(Layout.Views, "s > 'a long'")]
    [InlineData(Layout.Views, "s IN ('ab', 'abc')")]
    [InlineData(Layout.Views, "'ab' = s")]
    public void AStringComparison_TakesTheDirectKernel(Layout layout, string sql)
    {
        foreach (var make in DirectColumnComparisonTests.Evaluators)
        {
            var evaluator = make();
            evaluator.EvaluatePredicate(SparkSqlParser.ParsePredicate(sql), Batch(layout, false));
            Assert.Equal(1, evaluator.DirectComparisons + evaluator.DirectSets);
        }
    }

#if NET
    /// <summary>
    /// ...and copies nothing on the way, under every evaluator: the Spark registry analyses a comparison's operand
    /// types before the kernel runs, and asked for the converted column to read its type (#863 review).
    /// </summary>
    [Theory]
    [InlineData(Layout.LargeOffsets, "s = 'x'")]
    [InlineData(Layout.Views, "s = 'x'")]
    [InlineData(Layout.Views, "s IN ('x', 'y')")]
    [InlineData(Layout.Views, "st.s = 'x'")]
    public void AStringComparison_CopiesNoValues(Layout layout, string sql)
    {
        const int rows = 20_000;
        var value = new string('v', 100);   // past the view's inline limit: 2,000,000 bytes of values
        IArrowArray column = layout == Layout.Views
            ? new StringViewArray.Builder().AppendRange(Enumerable.Repeat(value, rows)).Build()
            : new LargeStringArray.Builder().AppendRange(Enumerable.Repeat(value, rows)).Build();
        var st = new StructArray(new StructType([new Field("s", column.Data.DataType, true)]), rows, [column],
            ArrowBuffer.Empty, 0);
        var batch = new RecordBatch(new Apache.Arrow.Schema(
            [new Field("s", column.Data.DataType, true), new Field("st", st.Data.DataType, true)], null), [column, st], rows);
        var predicate = SparkSqlParser.ParsePredicate(sql);

        foreach (var make in DirectColumnComparisonTests.Evaluators)
        {
            var evaluator = make();
            evaluator.EvaluatePredicate(predicate, batch);   // warm up
            long before = GC.GetAllocatedBytesForCurrentThread();
            evaluator.EvaluatePredicate(predicate, batch);
            long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
            Assert.True(allocated < 200_000, $"{sql}: {allocated} bytes allocated");
        }
    }
#endif

    /// <summary>A bare reference comes back as the batch's own array, its layout kept: nothing reads it here, so
    /// there is nothing to convert it for.</summary>
    [Theory]
    [InlineData(Layout.LargeOffsets, "s")]
    [InlineData(Layout.Views, "s")]
    [InlineData(Layout.Views, "ls")]
    public void AColumnReference_ComesBackAsTheBatchHoldsIt(Layout layout, string name)
    {
        var batch = Batch(layout, true);
        var result = new ArrowRowEvaluator().EvaluateExpression(Expressions.Ref(name), batch);
        Assert.Same(batch.Column(name), result);
    }

    /// <summary>A function's argument has its strings in the offset layout at any depth, which is the layout every
    /// function here is written against.</summary>
    [Theory]
    [InlineData(Layout.LargeOffsets)]
    [InlineData(Layout.Views)]
    public void AFunctionArgument_HasOffsetStringsAtAnyDepth(Layout layout)
    {
        var probe = new Probe();
        var result = new ArrowRowEvaluator(probe).EvaluateExpression(
            Expressions.Call("probe", Expressions.Ref("s"), Expressions.Ref("ls"), Expressions.Ref("st")),
            Batch(layout, true));
        Assert.NotNull(result);

        Assert.IsType<StringArray>(probe.Arguments![0]);
        var list = Assert.IsType<ListArray>(probe.Arguments[1]);
        Assert.IsType<StringType>(((ListType)list.Data.DataType).ValueDataType);
        Assert.IsType<StringType>(((StructType)probe.Arguments[2].Data.DataType).Fields[0].DataType);

        var plain = (ListArray)Batch(Layout.Offsets, true).Column("ls");
        Assert.Equal(plain.Length, list.Length);
        for (int i = 0; i < plain.Length; i++)
        {
            Assert.Equal(plain.IsNull(i), list.IsNull(i));
            if (plain.IsNull(i))
                continue;
            var want = (StringArray)plain.GetSlicedValues(i);
            var got = Assert.IsType<StringArray>(list.GetSlicedValues(i));
            Assert.Equal(Enumerable.Range(0, want.Length).Select(j => want.GetString(j)),
                Enumerable.Range(0, got.Length).Select(j => got.GetString(j)));
        }
    }

    private sealed class Probe : IFunctionRegistry
    {
        public IReadOnlyList<IArrowArray>? Arguments { get; private set; }

        public bool IsRegistered(string name) => name == "probe";

        public IArrowArray Invoke(string name, IReadOnlyList<IArrowArray> args, int rowCount)
        {
            Arguments = args;
            return args[0];
        }
    }

    private static string Outcome(Func<ArrowRowEvaluator> make, bool direct, Predicate predicate, RecordBatch batch)
    {
        var evaluator = make();
        evaluator.DirectColumnComparison = direct;
        try
        {
            var mask = evaluator.EvaluatePredicate(predicate, batch);
            return string.Join(",", Enumerable.Range(0, mask.Length)
                .Select(i => mask.GetValue(i) is { } v ? (v ? "T" : "F") : "n")) + $" ({mask.NullCount} null)";
        }
        catch (Exception e)
        {
            return $"{e.GetType().Name}: {e.Message}";
        }
    }

    private static RecordBatch Batch(Layout layout, bool slice)
    {
        var s = Strings(layout, S);
        var t = Strings(layout, T);
        var bin = Binaries(layout, S);
        var ls = List(layout);
        var st = Struct(layout);
        IArrowArray[] columns = [s, t, bin, ls, st];
        string[] names = ["s", "t", "bin", "ls", "st"];
        int length = S.Length;
        if (slice)
        {
            // The layouts' copies must honour an array's own offset.
            for (int i = 0; i < columns.Length; i++)
                columns[i] = ArrowArrayFactory.Slice(columns[i], 1, length - 1);
            length--;
        }

        var fields = names.Select((n, i) => new Field(n, columns[i].Data.DataType, true)).ToList();
        return new RecordBatch(new Apache.Arrow.Schema(fields, null), columns, length);
    }

    private static IArrowArray Strings(Layout layout, IReadOnlyList<string?> values)
    {
        switch (layout)
        {
            case Layout.Offsets:
            {
                var b = new StringArray.Builder();
                foreach (var v in values) { if (v is null) b.AppendNull(); else b.Append(v); }
                return b.Build();
            }
            case Layout.LargeOffsets:
            {
                var b = new LargeStringArray.Builder();
                foreach (var v in values) { if (v is null) b.AppendNull(); else b.Append(v); }
                return b.Build();
            }
            default:
            {
                var b = new StringViewArray.Builder();
                foreach (var v in values) { if (v is null) b.AppendNull(); else b.Append(v); }
                return b.Build();
            }
        }
    }

    private static IArrowArray Binaries(Layout layout, IReadOnlyList<string?> values)
    {
        switch (layout)
        {
            case Layout.Offsets:
            {
                var b = new BinaryArray.Builder();
                foreach (var v in values) { if (v is null) b.AppendNull(); else b.Append(Encoding.UTF8.GetBytes(v).AsSpan()); }
                return b.Build();
            }
            case Layout.LargeOffsets:
            {
                var b = new LargeBinaryArray.Builder();
                foreach (var v in values) { if (v is null) b.AppendNull(); else b.Append(Encoding.UTF8.GetBytes(v).AsSpan()); }
                return b.Build();
            }
            default:
            {
                var b = new BinaryViewArray.Builder();
                foreach (var v in values) { if (v is null) b.AppendNull(); else b.Append(Encoding.UTF8.GetBytes(v).AsSpan()); }
                return b.Build();
            }
        }
    }

    private static IArrowArray List(Layout layout)
    {
        var flat = new List<string?>();
        var offsets = new int[L.Length + 1];
        var validity = new byte[(L.Length + 7) / 8];
        int nulls = 0;
        for (int i = 0; i < L.Length; i++)
        {
            offsets[i] = flat.Count;
            if (L[i] is { } row)
            {
                flat.AddRange(row);
                validity[i >> 3] |= (byte)(1 << (i & 7));
            }
            else
            {
                nulls++;
            }
        }
        offsets[L.Length] = flat.Count;

        var child = Strings(layout, flat);
        var type = new ListType(new Field("element", child.Data.DataType, true));
        return new ListArray(type, L.Length, new ArrowBuffer(MemoryMarshal.AsBytes(offsets.AsSpan()).ToArray()),
            child, new ArrowBuffer(validity), nulls);
    }

    private static IArrowArray Struct(Layout layout)
    {
        var child = Strings(layout, S);
        var validity = new byte[(S.Length + 7) / 8];
        int nulls = 0;
        for (int i = 0; i < S.Length; i++)
        {
            if (StructValid[i]) validity[i >> 3] |= (byte)(1 << (i & 7));
            else nulls++;
        }
        var type = new StructType([new Field("s", child.Data.DataType, true)]);
        return new StructArray(type, S.Length, [child], new ArrowBuffer(validity), nulls);
    }
}
