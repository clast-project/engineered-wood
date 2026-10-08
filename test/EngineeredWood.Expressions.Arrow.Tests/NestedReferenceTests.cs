// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Expressions.Arrow.Spark;
using EngineeredWood.Expressions.Sql;

namespace EngineeredWood.Expressions.Arrow.Tests;

/// <summary>
/// #466: a reference to a struct's field (<c>s.a</c>) used to bind only as a whole top-level column name, so it
/// could not be evaluated at all. It now walks the reference's name parts through the struct, while a quoted name
/// containing a dot (<c>`s.a`</c>) still names one top-level column.
/// </summary>
public sealed class NestedReferenceTests
{
    private static readonly SparkFunctionRegistry Ansi = new();

    private static Int64Array Longs(params long?[] values)
    {
        var builder = new Int64Array.Builder();
        foreach (var value in values)
        {
            if (value is { } present)
            {
                builder.Append(present);
            }
            else
            {
                builder.AppendNull();
            }
        }
        return builder.Build();
    }

    private static ArrowBuffer Validity(params bool[] valid)
    {
        var b = new ArrowBuffer.BitmapBuilder();
        foreach (bool v in valid)
        {
            b.Append(v);
        }
        return b.Build();
    }

    // s: struct<a: long, t: struct<u: long>>
    private static readonly StructType TType = new([new Field("u", Int64Type.Default, true)]);
    private static readonly StructType SType = new(
        [new Field("a", Int64Type.Default, true), new Field("t", TType, true)]);

    private static StructArray S(bool[] valid, long?[] a, long?[] u) =>
        new(SType, valid.Length,
            [Longs(a), new StructArray(TType, u.Length, [Longs(u)], ArrowBuffer.Empty, nullCount: 0)],
            Validity(valid), valid.Count(v => !v));

    private static RecordBatch Batch(params (string Name, IArrowArray Array)[] columns)
    {
        var schema = new Schema.Builder();
        foreach (var (name, array) in columns)
        {
            schema.Field(new Field(name, array.Data.DataType, true));
        }
        return new RecordBatch(schema.Build(), columns.Select(c => c.Array), columns[0].Array.Length);
    }

    private static bool?[] Eval(string sql, RecordBatch batch)
    {
        var result = new ArrowRowEvaluator(Ansi).EvaluatePredicate(SparkSqlParser.ParsePredicate(sql), batch);
        return Enumerable.Range(0, result.Length).Select(i => result.GetValue(i)).ToArray();
    }

    [Theory]
    [InlineData("s.a > 0")]
    [InlineData("`s`.`a` > 0")]
    [InlineData("S.A > 0")] // case-insensitive, as top-level names are
    public void AStructField_IsRead(string sql)
    {
        var batch = Batch(("s", S([true, true], [5, -1], [0, 0])));

        Assert.Equal([true, false], Eval(sql, batch));
    }

    [Fact]
    public void AFieldOfANestedStruct_IsRead()
    {
        var batch = Batch(("s", S([true, true], [0, 0], [7, 8])));

        Assert.Equal([false, true], Eval("s.t.u = 8", batch));
    }

    // Spark: a field of a null struct is null, whatever the child slot holds.
    [Fact]
    public void AFieldOfANullStructRow_IsNull()
    {
        var batch = Batch(("s", S([true, false, true], [1, 5, null], [0, 0, 0])));

        Assert.Equal([false, true, true], Eval("s.a IS NULL", batch));
        Assert.Equal([true, null, null], Eval("s.a > 0", batch));
    }

    [Fact]
    public void AFieldOfASlicedStruct_ReadsTheSlicesRows()
    {
        var batch = Batch(("s", S([true, false, true, true], [1, 2, 3, -4], [0, 0, 0, 0]))).Slice(1, 3);

        Assert.Equal([null, true, false], Eval("s.a > 0", batch));
    }

    // The case the dotted text could not tell apart: a top-level column whose name contains a dot, beside a struct
    // with the matching field.
    [Fact]
    public void AQuotedDottedName_ReadsTheTopLevelColumn_AndTheBareOne_TheField()
    {
        var batch = Batch(
            ("s", S([true], [1], [0])),
            ("s.a", Longs(100)));

        Assert.Equal([true], Eval("`s.a` = 100", batch));
        Assert.Equal([true], Eval("s.a = 1", batch));
    }

    [Theory]
    [InlineData("s.missing > 0", "has no field 'missing'")]
    [InlineData("s.a.x > 0", "'s.a' is not a struct")]
    public void AnUnresolvablePath_IsRefused_NamingWhy(string sql, string why)
    {
        var batch = Batch(("s", S([true], [1], [0])));

        var ex = Assert.Throws<ArgumentException>(() => Eval(sql, batch));

        Assert.Contains(why, ex.Message);
    }

    [Fact]
    public void AnExpressionOverAStructField_Evaluates()
    {
        var batch = Batch(("s", S([true, true], [2, 3], [0, 0])), ("b", Longs(4, 6)));

        Assert.Equal([true, true], Eval("s.a * 2 = b", batch));
    }
}
