// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// <c>SchemaEvolution.BackfillMissingColumns</c> materializes a column the current schema declares but a
/// data file does not carry (an ADD COLUMN after that file was written). The backfill used to be dispatched
/// through a handful of per-type builders behind a <c>StringArray</c> fallback, so any type outside that
/// handful — Timestamp, Decimal, Date, Binary, a nested struct — came back typed <c>String</c>: an array
/// contradicting the schema it was just placed into, with nothing raised. These pin the backfilled columns
/// to the types the current schema actually asks for.
/// </summary>
public class BackfillNullColumnTypeTests
{
    /// <summary>
    /// A batch of one Int32 column reconciled against expected fields that also declare
    /// <paramref name="missing"/>, which the batch does not carry.
    ///
    /// <para>Backfilling lives here rather than in <c>ValueWidener.WidenBatch</c>: the widener only converts
    /// VALUES of same-named columns and leaves the batch's own column set alone, so reconciling the column
    /// SET — backfilling an added column, dropping a removed one — is entirely this helper's job. Both the
    /// read path and compaction run it after widening.</para>
    /// </summary>
    private static RecordBatch BackfillWithMissingColumn(Field missing, int rows = 3)
    {
        var sourceSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("n", Int32Type.Default, true))
            .Build();

        var n = new Int32Array.Builder();
        for (int i = 0; i < rows; i++) n.Append(i);
        var batch = new RecordBatch(sourceSchema, [n.Build()], rows);

        var reconciled = SchemaEvolution.BackfillMissingColumns(
            batch, [new Field("n", Int32Type.Default, true), missing]);

        Assert.Equal(2, reconciled.ColumnCount);
        Assert.Equal(rows, reconciled.Length);
        Assert.IsType<Int32Array>(reconciled.Column(0)); // the present column, passed through untouched
        return reconciled;
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

        var reconciled = BackfillWithMissingColumn(new Field("ts", tsType, true));

        var ts = Assert.IsType<TimestampArray>(reconciled.Column(1));
        AssertAllNull(ts, 3);

        var actual = Assert.IsType<TimestampType>(ts.Data.DataType);
        Assert.Equal(TimeUnit.Millisecond, actual.Unit);
        Assert.Equal("UTC", actual.Timezone);

        // And the batch's own schema must agree with the array it holds.
        Assert.Equal(ArrowTypeId.Timestamp, reconciled.Schema.FieldsList[1].DataType.TypeId);
    }

    [Fact]
    public void MissingDecimalColumn_BackfillsAsDecimalKeepingPrecisionAndScale()
    {
        var decType = new Decimal128Type(precision: 38, scale: 10);

        var reconciled = BackfillWithMissingColumn(new Field("amount", decType, true));

        var dec = Assert.IsType<Decimal128Array>(reconciled.Column(1));
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

        var reconciled = BackfillWithMissingColumn(new Field("c", type, true));

        // String is in here deliberately: it was the one type the old fallback got right by accident, so
        // its passing alongside the others is what shows the fallback was replaced rather than reordered.
        Assert.Equal(expected, reconciled.Column(1).Data.DataType.TypeId);
        AssertAllNull(reconciled.Column(1), 3);
    }

    [Fact]
    public void MissingStructColumn_BackfillsChildrenAsWell()
    {
        var structType = new Apache.Arrow.Types.StructType(
        [
            new Field("inner_ts", new TimestampType(TimeUnit.Microsecond, (string?)null), true),
            new Field("inner_n", Int64Type.Default, true),
        ]);

        var reconciled = BackfillWithMissingColumn(new Field("nested", structType, true));

        var st = Assert.IsType<StructArray>(reconciled.Column(1));
        AssertAllNull(st, 3);

        // Children are all-null in their own right, at the parent's length, and keep their own types.
        Assert.Equal(2, st.Fields.Count);
        var innerTs = Assert.IsType<TimestampArray>(st.Fields[0]);
        Assert.Equal(TimeUnit.Microsecond, ((TimestampType)innerTs.Data.DataType).Unit);
        AssertAllNull(innerTs, 3);
        AssertAllNull(Assert.IsType<Int64Array>(st.Fields[1]), 3);
    }

    /// <summary>
    /// A column that passes through untouched keeps its OWN field, not the expected one. Stamping the
    /// expected field onto an array nobody converted produces a batch whose schema contradicts its arrays —
    /// the reader believes a type the buffers do not have, and the mislabel only surfaces somewhere far
    /// downstream (a schema export, an encoder walking the wrong buffer widths).
    /// </summary>
    [Fact]
    public void PassThroughColumn_KeepsItsOwnFieldNotTheExpectedOne()
    {
        var sourceSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("n", Int32Type.Default, true))
            .Build();
        var batch = new RecordBatch(
            sourceSchema, [new Int32Array.Builder().Append(1).Append(2).Build()], 2);

        // "n" is expected as Int64 — a type the reconcile does NOT convert (widening is ValueWidener's job,
        // and it runs before this) — plus a genuinely missing column to force the rebuild.
        var reconciled = SchemaEvolution.BackfillMissingColumns(
            batch,
            [
                new Field("n", Int64Type.Default, true),
                new Field("added", StringType.Default, true),
            ]);

        // The array was not converted, so it is still Int32 — and the schema must say so.
        Assert.IsType<Int32Array>(reconciled.Column(0));
        Assert.Equal(ArrowTypeId.Int32, reconciled.Schema.FieldsList[0].DataType.TypeId);

        // The backfilled column, which this helper DID build, takes the expected label.
        Assert.Equal(ArrowTypeId.String, reconciled.Schema.FieldsList[1].DataType.TypeId);
        AssertAllNull(reconciled.Column(1), 2);
    }

    [Fact]
    public void MissingBooleanColumn_BackfillsAsBoolean()
    {
        var reconciled = BackfillWithMissingColumn(new Field("flag", BooleanType.Default, true));

        var flag = Assert.IsType<BooleanArray>(reconciled.Column(1));
        AssertAllNull(flag, 3);
        for (int i = 0; i < 3; i++)
            Assert.Null(flag.GetValue(i));
    }
}
