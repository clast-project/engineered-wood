// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow.Types;
using EngineeredWood.Orc.Proto;
using OrcType = EngineeredWood.Orc.Proto.Type;

namespace EngineeredWood.Orc.Tests;

/// <summary>
/// #763: the footer's decimal precision and scale went into the Arrow type unchecked, so a footer
/// declaring decimal(10, 50000000) gave the reader a scale it sizes a power of ten by. ORC allows
/// at most 38 digits and a scale no larger than the precision.
/// </summary>
public class DecimalSchemaBoundTests
{
    private static Footer FooterWithDecimal(uint? precision, uint? scale)
    {
        var root = new OrcType { Kind = OrcType.Types.Kind.Struct };
        root.Subtypes.Add(1);
        root.FieldNames.Add("d");

        var dec = new OrcType { Kind = OrcType.Types.Kind.Decimal };
        if (precision is { } p) dec.Precision = p;
        if (scale is { } s) dec.Scale = s;

        var footer = new Footer();
        footer.Types_.Add(root);
        footer.Types_.Add(dec);
        return footer;
    }

    [Theory]
    [InlineData(38u, 38u)]
    [InlineData(10u, 0u)]
    [InlineData(1u, 1u)]
    public void AnInRangeDecimal_ReadsAsDeclared(uint precision, uint scale)
    {
        var type = Assert.IsType<Decimal128Type>(
            OrcSchema.FromFooter(FooterWithDecimal(precision, scale)).ToArrowSchema().FieldsList[0].DataType);
        Assert.Equal(((int)precision, (int)scale), (type.Precision, type.Scale));
    }

    [Theory]
    [InlineData(10u, 11u)]
    [InlineData(10u, 50_000_000u)]
    [InlineData(39u, 2u)]
    [InlineData(uint.MaxValue, uint.MaxValue)]
    public void AnOutOfRangeDecimal_IsRefused(uint precision, uint scale)
    {
        var ex = Assert.Throws<InvalidDataException>(() => OrcSchema.FromFooter(FooterWithDecimal(precision, scale)));
        Assert.Contains($"decimal({precision},{scale})", ex.Message);
    }

    [Fact]
    public void AHive11Decimal_IgnoresItsScale()
    {
        var type = Assert.IsType<Decimal128Type>(
            OrcSchema.FromFooter(FooterWithDecimal(null, 50_000_000u)).ToArrowSchema().FieldsList[0].DataType);
        Assert.Equal((38, 6), (type.Precision, type.Scale));
    }
}
