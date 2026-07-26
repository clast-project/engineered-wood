// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Table.TypeWidening;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// <c>ValueWidener.WidenBatch</c> backfills a column the target schema declares but the batch does not carry.
/// It used to dispatch that through a handful of per-type builders behind a <c>StringArray</c> fallback, so
/// any type outside that handful — Timestamp, Decimal, Date, Binary, a nested struct — came back typed
/// <c>String</c>: an array contradicting the schema it was just placed into, with nothing raised. These pin
/// the backfilled columns to the types the target schema actually asks for.
/// </summary>
public class WidenBatchNullBackfillTests
{
    /// <summary>
    /// A batch of one Int32 column, against a target schema that widens it to Int64 and adds
    /// <paramref name="missing"/>. The widening is what makes <c>WidenBatch</c> rebuild the batch at all —
    /// with no column needing conversion it returns the batch untouched, missing columns and all (that
    /// reconcile is <c>SchemaEvolution.BackfillMissingColumns</c>'s job, not this one).
    /// </summary>
    private static RecordBatch WidenWithMissingColumn(Field missing, int rows = 3)
    {
        var sourceSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("n", Int32Type.Default, true))
            .Build();

        var n = new Int32Array.Builder();
        for (int i = 0; i < rows; i++) n.Append(i);
        var batch = new RecordBatch(sourceSchema, [n.Build()], rows);

        var targetSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("n", Int64Type.Default, true))
            .Field(missing)
            .Build();

        var widened = ValueWidener.WidenBatch(batch, targetSchema);

        Assert.Equal(2, widened.ColumnCount);
        Assert.Equal(rows, widened.Length);
        Assert.IsType<Int64Array>(widened.Column(0)); // the widening that triggered the rebuild
        return widened;
    }

    private static void AssertAllNull(IArrowArray array, int rows)
    {
        Assert.Equal(rows, array.Length);
        Assert.Equal(rows, array.Data.NullCount);

        var asArray = Assert.IsAssignableFrom<Apache.Arrow.Array>(array);
        for (int i = 0; i < rows; i++)
            Assert.True(asArray.IsNull(i), $"row {i} did not read as null");
    }

    [Fact]
    public void MissingTimestampColumn_BackfillsAsTimestampKeepingUnitAndTimezone()
    {
        // The unit and timezone are the part a builder for a different type cannot carry at all.
        var tsType = new TimestampType(TimeUnit.Millisecond, "UTC");

        var widened = WidenWithMissingColumn(new Field("ts", tsType, true));

        var ts = Assert.IsType<TimestampArray>(widened.Column(1));
        AssertAllNull(ts, 3);

        var actual = Assert.IsType<TimestampType>(ts.Data.DataType);
        Assert.Equal(TimeUnit.Millisecond, actual.Unit);
        Assert.Equal("UTC", actual.Timezone);

        // And the batch's own schema must agree with the array it holds.
        Assert.Equal(ArrowTypeId.Timestamp, widened.Schema.FieldsList[1].DataType.TypeId);
    }

    [Fact]
    public void MissingDecimalColumn_BackfillsAsDecimalKeepingPrecisionAndScale()
    {
        var decType = new Decimal128Type(precision: 38, scale: 10);

        var widened = WidenWithMissingColumn(new Field("amount", decType, true));

        var dec = Assert.IsType<Decimal128Array>(widened.Column(1));
        AssertAllNull(dec, 3);

        var actual = Assert.IsType<Decimal128Type>(dec.Data.DataType);
        Assert.Equal(38, actual.Precision);
        Assert.Equal(10, actual.Scale);
    }

    [Theory]
    [InlineData("date32")]
    [InlineData("date64")]
    [InlineData("binary")]
    [InlineData("string")]
    public void MissingColumn_BackfillsAsItsDeclaredType(string which)
    {
        var (type, expected) = which switch
        {
            "date32" => ((IArrowType)Date32Type.Default, ArrowTypeId.Date32),
            "date64" => (Date64Type.Default, ArrowTypeId.Date64),
            "binary" => (BinaryType.Default, ArrowTypeId.Binary),
            "string" => (StringType.Default, ArrowTypeId.String),
            _ => throw new ArgumentOutOfRangeException(nameof(which)),
        };

        var widened = WidenWithMissingColumn(new Field("c", type, true));

        // String is in here deliberately: it was the one type the old fallback got right by accident, so
        // its passing alongside the others is what shows the fallback was replaced rather than reordered.
        Assert.Equal(expected, widened.Column(1).Data.DataType.TypeId);
        AssertAllNull(widened.Column(1), 3);
    }

    [Fact]
    public void MissingStructColumn_BackfillsChildrenAsWell()
    {
        var structType = new Apache.Arrow.Types.StructType(
        [
            new Field("inner_ts", new TimestampType(TimeUnit.Microsecond, (string?)null), true),
            new Field("inner_n", Int64Type.Default, true),
        ]);

        var widened = WidenWithMissingColumn(new Field("nested", structType, true));

        var st = Assert.IsType<StructArray>(widened.Column(1));
        AssertAllNull(st, 3);

        // Children are all-null in their own right, at the parent's length, and keep their own types.
        Assert.Equal(2, st.Fields.Count);
        var innerTs = Assert.IsType<TimestampArray>(st.Fields[0]);
        Assert.Equal(TimeUnit.Microsecond, ((TimestampType)innerTs.Data.DataType).Unit);
        AssertAllNull(innerTs, 3);
        AssertAllNull(Assert.IsType<Int64Array>(st.Fields[1]), 3);
    }

    [Fact]
    public void MissingBooleanColumn_BackfillsAsBoolean()
    {
        var widened = WidenWithMissingColumn(new Field("flag", BooleanType.Default, true));

        var flag = Assert.IsType<BooleanArray>(widened.Column(1));
        AssertAllNull(flag, 3);
        for (int i = 0; i < 3; i++)
            Assert.Null(flag.GetValue(i));
    }
}
