// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Memory;
#if NETFRAMEWORK
using EngineeredWood.Core.Tests.Scans; // TaskTimeoutPolyfill
#endif

namespace EngineeredWood.Core.Tests.Memory;

/// <summary>The budget tree on its own (#719): a child's charges count against every level above it.</summary>
public class MemoryBudgetTests
{
    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void ACapBelowOne_IsRefused(long max)
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new MemoryBudget(max));
        Assert.Throws<ArgumentOutOfRangeException>(() => new MemoryBudget(10).CreateChild(max));
    }

    [Fact]
    public void AChildsCharges_CountAgainstEveryLevelAboveIt()
    {
        var root = new MemoryBudget(1000);
        var mid = root.CreateChild(500);
        var leaf = mid.CreateChild(100);
        var sibling = root.CreateChild(300);
        Assert.Same(mid, leaf.Parent);
        Assert.Null(root.Parent);

        lock (root.Gate)
        {
            Assert.True(leaf.TryReserveLocked(80, force: false, out _));
            Assert.True(sibling.TryReserveLocked(200, force: false, out _));
        }

        Assert.Equal(80, leaf.UsedBytes);
        Assert.Equal(80, mid.UsedBytes);
        Assert.Equal(200, sibling.UsedBytes);
        Assert.Equal(280, root.UsedBytes);

        lock (root.Gate)
        {
            leaf.AdjustLocked(-80);
            sibling.AdjustLocked(-200);
        }
        Assert.Equal(0, root.UsedBytes);
        Assert.Equal(280, root.PeakBytes);
        Assert.Equal(80, mid.PeakBytes);
    }

    [Fact]
    public void AReservation_IsRefusedByTheNearestFullLevel_AndChargesNothing()
    {
        var root = new MemoryBudget(100);
        var a = root.CreateChild(1000);
        var b = root.CreateChild(1000);
        lock (root.Gate)
        {
            Assert.True(a.TryReserveLocked(90, force: false, out _));
            // b has room of its own; the root does not.
            Assert.False(b.TryReserveLocked(20, force: false, out var blocking));
            Assert.Same(root, blocking);
            Assert.Equal(0, b.UsedLocked);
            Assert.Equal(90, root.UsedLocked);

            // a is not empty, and is over its parent's cap: blocked by the root too.
            Assert.False(a.TryReserveLocked(20, force: false, out blocking));
            Assert.Same(root, blocking);
        }
    }

    [Fact]
    public void AnEmptyLevel_AdmitsOneReservationLargerThanItsCap()
    {
        var root = new MemoryBudget(1000);
        var child = root.CreateChild(10);
        lock (root.Gate)
        {
            Assert.True(child.TryReserveLocked(500, force: false, out _));
            Assert.False(child.TryReserveLocked(1, force: false, out var blocking));
            Assert.Same(child, blocking);
        }
    }

    [Fact]
    public void AForcedReservation_IgnoresEveryCap()
    {
        var root = new MemoryBudget(100);
        var child = root.CreateChild(100);
        lock (root.Gate)
        {
            Assert.True(child.TryReserveLocked(100, force: false, out _));
            Assert.True(child.TryReserveLocked(1000, force: true, out var blocking));
            Assert.Null(blocking);
        }
        Assert.Equal(1100, root.UsedBytes);
    }

    [Fact]
    public void ABudgetOfLongMaxValue_RefusesWhatDoesNotFit_RatherThanWrapping()
    {
        // _used + bytes would wrap negative here, and admit the reservation.
        var root = new MemoryBudget(long.MaxValue);
        lock (root.Gate)
        {
            Assert.True(root.TryReserveLocked(long.MaxValue - 10, force: false, out _));
            Assert.False(root.TryReserveLocked(100, force: false, out var blocking));
            Assert.Same(root, blocking);
            Assert.True(root.TryReserveLocked(10, force: false, out _));
        }
        Assert.Equal(long.MaxValue, root.UsedBytes);
    }

    [Fact]
    public async Task AnEstimateFromACorruptFooter_IsCappedAtMaxCharge()
    {
        // A footer can claim any size. Capping each charge keeps a shared budget's totals far from wrapping, and
        // what is released is what was charged.
        var scan = new EngineeredWood.Scans.ScanMemoryBudget(1 << 20);
        var reservation = new EngineeredWood.Scans.ScanReservation(scan, 0);
        reservation.SetInitialEstimate(long.MaxValue);
        Assert.Equal(MemoryBudget.MaxCharge, reservation.Estimate);

        await reservation.HoldAsync(long.MaxValue, default);
        await reservation.ReserveNextAsync(default);
        Assert.Equal(2 * MemoryBudget.MaxCharge, scan.Used);

        reservation.ReleasePending();
        reservation.ReleaseHeld();
        Assert.Equal(0, scan.Used);
    }

    [Fact]
    public async Task AReleaseAnywhereBelow_WakesAWaiterOnTheLevelItFrees()
    {
        var root = new MemoryBudget(100);
        var a = root.CreateChild(100);
        Task changed;
        lock (root.Gate)
        {
            Assert.True(a.TryReserveLocked(100, force: false, out _));
            changed = root.ChangedLocked;
        }
        Assert.False(changed.IsCompleted);
        lock (root.Gate)
            a.AdjustLocked(-100);
        await changed.WaitAsync(TimeSpan.FromSeconds(10));
    }
}
