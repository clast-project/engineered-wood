// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace EngineeredWood.Expressions.Arrow.Tests;

/// <summary>
/// The view kernel decides most rows from a view's length and inline prefix, and reads a data buffer only when they
/// cannot (#859). Every value here is compared under every operator against targets either side of the four-byte
/// prefix and the twelve-byte inline limit, and must answer what the general path answers over the same values as a
/// plain string column.
/// </summary>
public class StringViewKernelTests
{
    private static readonly string?[] Values =
    [
        "", "a", "abc", "abcd", "abce", "abcde", "abcdefghijkl", "abcdefghijkm", "abcdefghijklm", "abcdefghijkln",
        "abcdefghijklmnop", "abcdefghijklmnoq", "abcdx", "abd", "b", "zzzzzzzzzzzzzzzzzz", "é", "éé-long-enough-value",
        null, "abcdefghijk", "abcdefghijklmnopqrstuvwxyz",
    ];

    private static readonly string[] Targets =
    [
        "", "a", "abc", "abcd", "abcde", "abcdefghijk", "abcdefghijkl", "abcdefghijklm", "abcdefghijklmnop",
        "abcdefghijklmnopqrstuvwxyz", "abcdz", "b", "é", "éé-long-enough-value", "zzzzzzzzzzzzzzzzzzz",
    ];

    private static readonly ComparisonOperator[] Operators =
    [
        ComparisonOperator.Equal, ComparisonOperator.NotEqual, ComparisonOperator.NullSafeEqual,
        ComparisonOperator.LessThan, ComparisonOperator.LessThanOrEqual,
        ComparisonOperator.GreaterThan, ComparisonOperator.GreaterThanOrEqual,
        ComparisonOperator.StartsWith, ComparisonOperator.NotStartsWith,
    ];

    public static TheoryData<string> Shapes() => ["builder", "two buffers, padded", "sliced", "large offsets"];

    [Theory]
    [MemberData(nameof(Shapes))]
    public void EveryRow_AnswersAsTheGeneralPath(string shape)
    {
        var plain = Batch(Plain(Values));
        IArrowArray column = shape switch
        {
            "builder" => Builder(Values),
            "two buffers, padded" => HandBuilt(Values),
            "large offsets" => Large(Values),
            _ => Sliced(),
        };
        var other = Batch(column);

        foreach (var target in Targets)
        {
            foreach (var op in Operators)
            {
                var predicate = new ComparisonPredicate(Expressions.Ref("s"), op, Expressions.Lit(LiteralValue.Of(target)));
                var general = new ArrowRowEvaluator { DirectColumnComparison = false };
                var want = Mask(general.EvaluatePredicate(predicate, plain));

                var direct = new ArrowRowEvaluator();
                var got = Mask(direct.EvaluatePredicate(predicate, other));
                Assert.Equal(1, direct.DirectComparisons);
                Assert.True(want == got, $"s {op} '{target}' over {shape}: expected {want}, got {got}");
            }
        }
    }

    private static RecordBatch Batch(IArrowArray column) =>
        new(new Apache.Arrow.Schema([new Field("s", column.Data.DataType, true)], null), [column], column.Length);

    private static string Mask(BooleanArray mask) =>
        string.Join(",", Enumerable.Range(0, mask.Length).Select(i => mask.GetValue(i) is { } v ? (v ? "T" : "F") : "n"))
        + $" ({mask.NullCount} null)";

    private static StringArray Plain(IEnumerable<string?> values)
    {
        var b = new StringArray.Builder();
        foreach (var v in values) { if (v is null) b.AppendNull(); else b.Append(v); }
        return b.Build();
    }

    private static StringViewArray Builder(IEnumerable<string?> values)
    {
        var b = new StringViewArray.Builder();
        foreach (var v in values) { if (v is null) b.AppendNull(); else b.Append(v); }
        return b.Build();
    }

    private static LargeStringArray Large(IEnumerable<string?> values)
    {
        var b = new LargeStringArray.Builder();
        foreach (var v in values) { if (v is null) b.AppendNull(); else b.Append(v); }
        return b.Build();
    }

    /// <summary>The same values sliced out of a longer view array, so the views start at an offset.</summary>
    private static IArrowArray Sliced()
    {
        var padded = new List<string?> { "zz-leading-out-of-line-value", "q" };
        padded.AddRange(Values);
        padded.Add("trailing");
        return ArrowArrayFactory.Slice(Builder(padded), 2, Values.Length);
    }

    /// <summary>
    /// Views over two data buffers, alternating, with junk between values in them and junk padding after each short
    /// inline value: nothing past a value's length may decide a row.
    /// </summary>
    private static StringViewArray HandBuilt(IReadOnlyList<string?> values)
    {
        var views = new byte[values.Count * 16];
        var data = new[] { new List<byte>(), new List<byte>() };
        var validity = new byte[(values.Count + 7) / 8];
        int nulls = 0, outOfLine = 0;
        for (int i = 0; i < values.Count; i++)
        {
            var view = views.AsSpan(i * 16, 16);
            if (values[i] is not { } text)
            {
                nulls++;
                continue;
            }

            validity[i >> 3] |= (byte)(1 << (i & 7));
            var bytes = Encoding.UTF8.GetBytes(text);
            BitConverter.GetBytes(bytes.Length).CopyTo(view);
            if (bytes.Length <= 12)
            {
                view.Slice(4).Fill(0xAB);
                bytes.CopyTo(view.Slice(4));
                continue;
            }

            int buffer = outOfLine++ % 2;
            data[buffer].AddRange([0xEE, 0xEE, 0xEE]);
            int offset = data[buffer].Count;
            data[buffer].AddRange(bytes);
            bytes.AsSpan(0, 4).CopyTo(view.Slice(4));
            BitConverter.GetBytes(buffer).CopyTo(view.Slice(8));
            BitConverter.GetBytes(offset).CopyTo(view.Slice(12));
        }

        var arrayData = new ArrayData(StringViewType.Default, values.Count, nulls, 0,
        [
            new ArrowBuffer(validity), new ArrowBuffer(views),
            new ArrowBuffer(data[0].ToArray()), new ArrowBuffer(data[1].ToArray()),
        ]);
        return new StringViewArray(arrayData);
    }
}
