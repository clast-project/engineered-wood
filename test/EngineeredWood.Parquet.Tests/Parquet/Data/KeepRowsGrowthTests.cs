// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Data;

namespace EngineeredWood.Tests.Parquet.Data;

/// <summary>
/// <see cref="ColumnBuildState.EnsurePageRoom"/>'s growth is bounded by the chunk's rows, not by the length of the
/// (pooled, so longer) keep mask, and is not doubled again by the buffer's own growth (#886 review).
/// </summary>
public class KeepRowsGrowthTests
{
    [Theory]
    [InlineData(PhysicalType.Int32)]
    [InlineData(PhysicalType.Int64)]
    [InlineData(PhysicalType.Double)]
    public void GrowthStopsAtTheChunksRows_NotAtThePooledMasksLength(PhysicalType type)
    {
        const int rows = 30;
        using var state = new ColumnBuildState(type, maxDefLevel: 0, maxRepLevel: 0, capacity: 10);
        state.SetRowSelection(new byte[64], rows); // rented masks run longer than the chunk

        // 25 needed, doubling would give 20: exactly 25, where the buffer's own doubling gave 20 x 2 = 40.
        state.EnsurePageRoom(25);
        Assert.Equal(25, ValueSlots(state, type));

        // 20 written, a 10-row page: 30 needed and doubling would give 50, past the chunk's 30 rows.
        Reserve(state, type, 20);
        state.EnsurePageRoom(10);
        Assert.Equal(rows, ValueSlots(state, type));
    }

    [Fact]
    public void ARowCountPastTheMaskIsRefused()
    {
        using var state = new ColumnBuildState(PhysicalType.Int32, maxDefLevel: 0, maxRepLevel: 0, capacity: 4);
        Assert.Throws<ArgumentOutOfRangeException>(() => state.SetRowSelection(new byte[4], 5));
    }

    private static int ValueSlots(ColumnBuildState state, PhysicalType type) => type switch
    {
        PhysicalType.Int32 => state.GetWritableValueSpan<int>().Length,
        PhysicalType.Int64 => state.GetWritableValueSpan<long>().Length,
        _ => state.GetWritableValueSpan<double>().Length,
    };

    private static void Reserve(ColumnBuildState state, PhysicalType type, int count)
    {
        switch (type)
        {
            case PhysicalType.Int32: state.ReserveValues<int>(count); break;
            case PhysicalType.Int64: state.ReserveValues<long>(count); break;
            default: state.ReserveValues<double>(count); break;
        }
    }
}
