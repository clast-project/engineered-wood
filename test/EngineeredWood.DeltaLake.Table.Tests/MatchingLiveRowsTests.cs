// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using EngineeredWood.DeltaLake.DeletionVectors;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// The predicate DELETE's per-batch merge: the rows a predicate holds TRUE for that the existing vector does
/// not already delete, against the per-row check it replaced.
/// </summary>
public class MatchingLiveRowsTests
{
    [Fact]
    public void MatchesThePerRowCheck()
    {
        var rng = new Random(439);
        for (int trial = 0; trial < 300; trial++)
        {
            var deleted = new HashSet<long>();
            for (int i = 0; i < rng.Next(0, 3000); i++) deleted.Add(rng.Next(0, 200000));
            long s = rng.Next(0, 200000);
            for (long v = s; v < s + rng.Next(0, 5000); v++) deleted.Add(v); // and a run
            var bitmap = DeletionVectorBitmap.FromPositions(deleted);

            long firstRow = rng.Next(0, 200000);
            int rows = rng.Next(0, 3000);
            var builder = new BooleanArray.Builder();
            var values = new bool?[rows];
            for (int i = 0; i < rows; i++)
            {
                values[i] = rng.Next(4) switch { 0 => null, 1 => false, _ => true };
                if (values[i] is bool v) builder.Append(v); else builder.AppendNull();
            }

            var expected = Enumerable.Range(0, rows)
                .Where(i => !deleted.Contains(firstRow + i) && values[i] == true)
                .ToList();
            Assert.Equal(expected, DeltaTable.MatchingLiveRows(bitmap, firstRow, rows, builder.Build()));
        }
    }

    [Fact]
    public void RefusesAMaskOfAnotherLength()
    {
        var mask = new BooleanArray.Builder().Append(true).Build();
        Assert.Throws<ArgumentException>(() => DeltaTable.MatchingLiveRows(DeletionVectorBitmap.Empty, 0, 2, mask));
    }
}
