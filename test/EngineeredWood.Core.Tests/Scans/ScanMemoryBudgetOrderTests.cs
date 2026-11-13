// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Scans;

namespace EngineeredWood.Core.Tests.Scans;

/// <summary>
/// #829: a scan's budget hands out room in unit order. A unit is not admitted while one nearer the front waits,
/// even when its own reservation fits; otherwise a wide window's units furthest back take the room as it frees,
/// and the units just behind the front cannot read until each becomes the front.
/// </summary>
public class ScanMemoryBudgetOrderTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);

    /// <summary>Long enough for a wrongly admitted reservation to complete.</summary>
    private static Task Settle() => Task.Delay(200);

    [Fact]
    public async Task AUnitThatFits_WaitsBehindAnEarlierUnitThatDoesNot()
    {
        var budget = new ScanMemoryBudget(100);
        budget.SetHead(0);
        await budget.ReserveAsync(0, 60, CancellationToken.None); // the front: admitted

        var second = budget.ReserveAsync(1, 60, CancellationToken.None).AsTask(); // 120 > 100: waits
        await Settle();
        Assert.False(second.IsCompleted);

        // 90 <= 100 would fit, but unit 1 is nearer the front and waiting.
        var third = budget.ReserveAsync(2, 30, CancellationToken.None).AsTask();
        await Settle();
        Assert.False(third.IsCompleted);
        Assert.Equal(60, budget.Used);

        // The front's item is taken: unit 1 gets the room first, then unit 2 fits beside it.
        budget.Adjust(-60);
        await second.WaitAsync(Timeout);
        await third.WaitAsync(Timeout);
        Assert.Equal(90, budget.Used);
    }

    [Fact]
    public async Task AUnitWithNoEarlierWaiter_IsAdmittedWhenItFits()
    {
        var budget = new ScanMemoryBudget(100);
        budget.SetHead(0);
        await budget.ReserveAsync(0, 60, CancellationToken.None);

        // Nothing nearer the front waits, so a later unit that fits goes straight in, as before.
        await budget.ReserveAsync(3, 30, CancellationToken.None).AsTask().WaitAsync(Timeout);
        Assert.Equal(90, budget.Used);
    }

    [Fact]
    public async Task AnEarlierWaiterThatIsCancelled_StopsHoldingBackTheUnitsBehindIt()
    {
        var budget = new ScanMemoryBudget(100);
        budget.SetHead(0);
        await budget.ReserveAsync(0, 60, CancellationToken.None);

        using var cancel = new CancellationTokenSource();
        var second = budget.ReserveAsync(1, 60, cancel.Token).AsTask();
        var third = budget.ReserveAsync(2, 30, CancellationToken.None).AsTask();
        await Settle();
        Assert.False(third.IsCompleted);

        cancel.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => second.WaitAsync(Timeout));
        await third.WaitAsync(Timeout);
        Assert.Equal(90, budget.Used);
    }

    [Fact]
    public async Task TheFront_IsAdmittedEvenBehindNoOne_WhenTheBudgetIsFull()
    {
        var budget = new ScanMemoryBudget(100);
        budget.SetHead(0);
        await budget.ReserveAsync(0, 60, CancellationToken.None);
        var second = budget.ReserveAsync(1, 60, CancellationToken.None).AsTask();
        await Settle();
        Assert.False(second.IsCompleted);

        // Unit 0 is drained without releasing (its charge is still counted), and unit 1 becomes the front with
        // nothing queued: it must go through over the budget, or the scan would stall.
        budget.SetHead(1);
        await second.WaitAsync(Timeout);
        Assert.Equal(120, budget.Used);
    }

    /// <summary>#846: a read-ahead never waits. The front takes it over the budget, as it does its decode; a unit
    /// behind it only when it fits and no unit nearer the front waits for room.</summary>
    [Fact]
    public async Task AReadAhead_IsForcedForTheFront_AndOtherwiseTakenOnlyFromRoomNobodyEarlierWaitsFor()
    {
        var budget = new ScanMemoryBudget(100);
        budget.SetHead(0);
        var front = new ScanReservation(budget, 0);
        var second = new ScanReservation(budget, 1);
        var third = new ScanReservation(budget, 2);
        await budget.ReserveAsync(0, 60, CancellationToken.None);

        Assert.True(second.TryHoldAhead(30)); // 90 <= 100
        Assert.False(third.TryHoldAhead(20)); // 110 > 100
        Assert.True(front.TryHoldAhead(50)); // the front: over the budget
        Assert.Equal(140, budget.Used);

        // Unit 1 waits for room (90 + 30 > 100); unit 2's read-ahead fits beside it (95), but is refused: the room
        // is unit 1's when it frees.
        front.ReleaseHeld(50);
        var waiting = budget.ReserveAsync(1, 30, CancellationToken.None).AsTask();
        await Settle();
        Assert.False(waiting.IsCompleted);
        Assert.False(third.TryHoldAhead(5));
        budget.Adjust(-60);
        await waiting.WaitAsync(Timeout);
        Assert.Equal(60, budget.Used);
        Assert.True(third.TryHoldAhead(20));

        // A unit releases its read-ahead one hold at a time, or all at once.
        second.ReleaseHeld(30);
        Assert.Equal(50, budget.Used);
        third.ReleaseHeld();
        second.ReleaseHeld();
        Assert.Equal(30, budget.Used);
    }

    /// <summary>#851: a first reservation covers what the host adds to each row its first batch can hold, and stays
    /// the largest estimate seen.</summary>
    [Fact]
    public void AFirstReservation_AddsTheHostsBytesForItsRows()
    {
        var budget = new ScanMemoryBudget(1L << 40);
        var reservation = new ScanReservation(budget, 0);
        reservation.SetInitialEstimate(1_000, 50);
        Assert.Equal(1_000, reservation.Estimate);

        reservation.AddedBytesPerRow = 27;
        reservation.SetInitialEstimate(1_000, 50);
        Assert.Equal(1_000 + 27 * 50, reservation.Estimate);
        reservation.SetInitialEstimate(500, 10);
        Assert.Equal(1_000 + 27 * 50, reservation.Estimate);

        // Capped, as any estimate is, rather than wrapping.
        reservation.AddedBytesPerRow = long.MaxValue / 2;
        reservation.SetInitialEstimate(long.MaxValue / 2, long.MaxValue / 2);
        Assert.Equal(EngineeredWood.Memory.MemoryBudget.MaxCharge, reservation.Estimate);
    }

    /// <summary>#852 review round 2: the bytes a host adds saturate at the most any estimate is, as a product and as a
    /// sum, rather than wrap below the raw estimate.</summary>
    [Theory]
    [InlineData(1_000L, 27L, 50L, 1_000L + 27 * 50)]
    [InlineData(1_000L, 0L, 50L, 1_000L)]
    [InlineData(1_000L, 1L << 40, 1L << 40, EngineeredWood.Memory.MemoryBudget.MaxCharge)]
    [InlineData(EngineeredWood.Memory.MemoryBudget.MaxCharge - 10, 1L, 100L, EngineeredWood.Memory.MemoryBudget.MaxCharge)]
    [InlineData(1_000L, long.MaxValue, 2L, EngineeredWood.Memory.MemoryBudget.MaxCharge)]
    public void AddedBytes_Saturate(long bytes, long perRow, long rows, long expected) =>
        Assert.Equal(expected, ScanReservation.WithAddedBytes(bytes, perRow, rows));
}
