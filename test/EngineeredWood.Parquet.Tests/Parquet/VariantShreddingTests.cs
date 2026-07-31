// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Operations.Shredding;
using Apache.Arrow.Operations.VariantJson;
using Apache.Arrow.Scalars.Variant;
using EngineeredWood.Parquet.Data;

namespace EngineeredWood.Tests.Parquet;

/// <summary>
/// Pins the properties that justify <see cref="VariantShredding"/>'s shape — both directions of the
/// VariantShredding spec over <see cref="VariantArray"/>, decided per column from its values.
/// </summary>
/// <remarks>
/// These assert BEHAVIOUR a caller depends on, not the layout of any particular shredded file: that
/// shredding is lossless (the two directions are inverses), that declining is signalled rather than
/// papered over with a re-encoded array, and that SQL null-ness stays distinct from a variant JSON
/// null. The last one is the subtle one — conflating them silently changes what <c>IS NULL</c> means
/// for every consumer of the column.
/// </remarks>
public class VariantShreddingTests
{
    private static VariantValue Obj(int a, string b) => VariantValue.FromObject(
        new Dictionary<string, VariantValue>
        {
            ["a"] = VariantValue.FromInt32(a),
            ["b"] = VariantValue.FromString(b),
        });

    private static string Json(VariantValue v) => VariantJsonWriter.ToJson(v, indented: false);

    /// <summary>Builds the canonical (unshredded) array for the same values, as the comparison baseline.</summary>
    private static VariantArray Canonical(IReadOnlyList<VariantValue> values, bool[]? isNull = null)
    {
        var builder = new VariantArray.Builder();
        for (int i = 0; i < values.Count; i++)
        {
            if (isNull is not null && isNull[i])
            {
                builder.AppendNull();
                continue;
            }
            builder.Append(values[i]);
        }
        return builder.Build(allocator: null);
    }

    [Fact]
    public void UniformObjects_Shred_AndReassembleIsTheInverse()
    {
        var values = new[] { Obj(1, "x"), Obj(2, "y"), Obj(3, "z") };

        Assert.True(VariantShredding.TryShred(values, default, out var shredded));
        Assert.NotNull(shredded);
        Assert.True(shredded!.IsShredded);
        Assert.Equal(values.Length, shredded.Length);

        // Lossless: reassembly returns the canonical form and every row's value is unchanged. This is
        // the property that lets a reader trust a shredded column at all.
        var back = VariantShredding.Reassemble(shredded);
        Assert.False(back.IsShredded);
        for (int i = 0; i < values.Length; i++)
        {
            Assert.False(back.IsNull(i));
            Assert.Equal(Json(values[i]), Json(back.GetLogicalVariantValue(i)));
        }
    }

    [Fact]
    public void MixedShapes_DeclineToShred_SoTheCallerKeepsItsOwnEncoding()
    {
        // An object, a bare int and a string in one column: no single typed_value schema covers them.
        var values = new[] { Obj(1, "x"), VariantValue.FromInt32(7), VariantValue.FromString("plain") };

        Assert.False(VariantShredding.TryShred(values, default, out var shredded));
        // Declining must NOT hand back a re-encoded unshredded array: a caller that already holds the
        // encoded bytes would pay a pointless round-trip through them.
        Assert.Null(shredded);
    }

    [Fact]
    public void SqlNullRows_RideStorageValidity_AndAreDistinctFromAVariantJsonNull()
    {
        // Row 1 is SQL NULL (masked); row 2 holds a variant JSON null as a real VALUE.
        var values = new[] { Obj(1, "x"), Obj(0, "ignored-placeholder"), VariantValue.Null, Obj(4, "w") };
        var isNull = new[] { false, true, false, false };

        Assert.True(VariantShredding.TryShred(values, isNull, out var shredded));
        Assert.NotNull(shredded);

        var back = VariantShredding.Reassemble(shredded!);
        Assert.False(back.IsNull(0));
        Assert.True(back.IsNull(1));   // SQL NULL -> storage validity
        Assert.False(back.IsNull(2));  // variant JSON null -> a present value, NOT a null row
        Assert.False(back.IsNull(3));

        Assert.Equal(Json(values[0]), Json(back.GetLogicalVariantValue(0)));
        Assert.Equal(Json(values[3]), Json(back.GetLogicalVariantValue(3)));
        Assert.True(back.GetLogicalVariantValue(2).IsNull);
    }

    [Fact]
    public void EverySqlNull_DeclinesToShred_BecauseThereIsNoShapeToInfer()
    {
        var values = new[] { VariantValue.Null, VariantValue.Null };
        Assert.False(VariantShredding.TryShred(values, new[] { true, true }, out var shredded));
        Assert.Null(shredded);
    }

    [Fact]
    public void ArrayOverload_AgreesWithTheValuesOverload()
    {
        var values = new[] { Obj(1, "x"), Obj(2, "y"), Obj(3, "z") };
        var isNull = new[] { false, true, false };

        Assert.True(VariantShredding.TryShred(values, isNull, out var fromValues));
        Assert.True(VariantShredding.TryShred(Canonical(values, isNull), out var fromArray));

        Assert.Equal(fromValues!.IsShredded, fromArray!.IsShredded);
        Assert.Equal(fromValues.Length, fromArray.Length);

        var a = VariantShredding.Reassemble(fromValues);
        var b = VariantShredding.Reassemble(fromArray);
        for (int i = 0; i < values.Length; i++)
        {
            Assert.Equal(a.IsNull(i), b.IsNull(i));
            if (!a.IsNull(i))
                Assert.Equal(Json(a.GetLogicalVariantValue(i)), Json(b.GetLogicalVariantValue(i)));
        }
    }

    [Fact]
    public void NullMaskOfTheWrongLength_Throws()
    {
        var values = new[] { Obj(1, "x"), Obj(2, "y") };
        var ex = Assert.Throws<ArgumentException>(
            () => VariantShredding.TryShred(values, new[] { false }, out _));
        Assert.Contains("null mask length", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Reassemble_OnAnUnshreddedArray_ReturnsItUnchanged()
    {
        // The read path calls this on every variant column, so an already-canonical one must be free.
        var canonical = Canonical(new[] { Obj(1, "x"), Obj(2, "y") });
        Assert.False(canonical.IsShredded);
        Assert.Same(canonical, VariantShredding.Reassemble(canonical));
    }
}
