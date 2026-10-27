// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Metadata;
using EngineeredWood.Parquet.Schema;

namespace EngineeredWood.Tests.Parquet;

/// <summary>
/// INT96 bounds under INT96_TIMESTAMP_ORDER (#545): decoded as timestamps that hold for the values as
/// the reader returns them, and only when the order is declared and both ends are usable.
/// </summary>
public class Int96BoundTests
{
    private const int JulianDayOfUnixEpoch = 2_440_588;
    private const long NanosPerDay = 86_400_000_000_000;

    private static byte[] Int96(int julianDay, long nanosOfDay)
    {
        var bytes = new byte[12];
        BinaryPrimitives.WriteInt64LittleEndian(bytes, nanosOfDay);
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(8), julianDay);
        return bytes;
    }

    private static ColumnDescriptor Column(ColumnOrder order)
    {
        var element = new SchemaElement { Name = "ts", Type = PhysicalType.Int96, RepetitionType = FieldRepetitionType.Required };
        return new ColumnDescriptor
        {
            Path = ["ts"],
            PhysicalType = PhysicalType.Int96,
            MaxDefinitionLevel = 0,
            MaxRepetitionLevel = 0,
            SchemaElement = element,
            SchemaNode = new SchemaNode { Element = element, Children = [] },
            ColumnOrder = order,
        };
    }

    private static DateTimeOffset Unix(long ticks) => new(621_355_968_000_000_000L + ticks, TimeSpan.Zero);

    private static (DateTimeOffset? Min, DateTimeOffset? Max) Bounds(byte[] min, byte[] max,
        ColumnOrder order = ColumnOrder.Int96TimestampOrder)
    {
        var column = Column(order);
        return (ParquetStatisticsAccessor.DecodeLowerBound(column, min, max)?.AsDateTimeOffset,
            ParquetStatisticsAccessor.DecodeUpperBound(column, max, min)?.AsDateTimeOffset);
    }

    /// <summary>
    /// The minimum floors to the microsecond (the reader's default unit, floored), the maximum rounds
    /// up to 100 ns; both on either side of 1970.
    /// </summary>
    [Fact]
    public void Bounds_RoundOutward_ForTheValuesAsRead()
    {
        var (min, max) = Bounds(Int96(JulianDayOfUnixEpoch - 1, 1), Int96(JulianDayOfUnixEpoch, 1_001));

        Assert.Equal(Unix(-NanosPerDay / 100), min);   // 1969-12-31 00:00:00.000000001 → its microsecond
        Assert.Equal(Unix(11), max);                    // 1970-01-01 00:00:00.000001001 → up to 1.1 µs
    }

    /// <summary>Under TYPE_ORDER, an INT96 column's statistics have no defined order.</summary>
    [Fact]
    public void TypeDefinedOrder_GivesNoBounds()
    {
        var (min, max) = Bounds(Int96(JulianDayOfUnixEpoch, 0), Int96(JulianDayOfUnixEpoch, 1_000),
            ColumnOrder.TypeDefined);

        Assert.Null(min);
        Assert.Null(max);
    }

    /// <summary>
    /// Spark stores a far-future value with an overflowed, negative day, which the reader wraps back to
    /// the year-290000 date it is. In stored order it is the minimum; in time it is beyond the maximum.
    /// So if either end is outside DateTimeOffset's range, neither end is used.
    /// </summary>
    [Fact]
    public void AnEndOutsideTheRepresentableRange_GivesNoBounds()
    {
        var (min, max) = Bounds(Int96(int.MinValue, 0), Int96(JulianDayOfUnixEpoch, 0));

        Assert.Null(min);
        Assert.Null(max);
    }

    [Theory]
    [InlineData(-1L)]
    [InlineData(NanosPerDay)]
    public void NanosecondsOutsideOneDay_GiveNoBounds(long nanosOfDay)
    {
        var (min, max) = Bounds(Int96(JulianDayOfUnixEpoch, 0), Int96(JulianDayOfUnixEpoch, nanosOfDay));

        Assert.Null(min);
        Assert.Null(max);
    }

    [Fact]
    public void WrongLength_GivesNoBounds()
    {
        var (min, max) = Bounds(new byte[8], Int96(JulianDayOfUnixEpoch, 0));

        Assert.Null(min);
        Assert.Null(max);
    }
}
