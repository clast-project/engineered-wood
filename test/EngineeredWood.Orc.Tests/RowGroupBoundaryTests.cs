// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Orc.Proto;

namespace EngineeredWood.Orc.Tests;

/// <summary>
/// Bit-packed streams (PRESENT, and a boolean column's DATA) across row-group and batch boundaries: every row
/// keeps its bit, a row group may start mid-byte, and the row index says where.
/// </summary>
public class RowGroupBoundaryTests
{
    private static string TempPath() => Path.Combine(Path.GetTempPath(), $"ew-rowgroups-{Guid.NewGuid():N}.orc");

    private static readonly Schema IntAndBool = new(
        [new Field("i", Int32Type.Default, true), new Field("b", BooleanType.Default, false)], null);

    /// <summary>Row <paramref name="v"/>: <c>i</c> null at odd rows, <c>b</c> true at multiples of 3.</summary>
    private static string Expected(int v) => $"{(v % 2 == 1 ? "null" : v.ToString())}/{v % 3 == 0}";

    private static async Task WriteRows(string path, OrcWriterOptions options, params int[] batchSizes)
    {
        await using var writer = OrcWriter.Create(path, IntAndBool, options);
        int v = 0;
        foreach (int size in batchSizes)
        {
            var ints = new Int32Array.Builder();
            var bools = new BooleanArray.Builder();
            for (int j = 0; j < size; j++, v++)
            {
                if (v % 2 == 1) ints.AppendNull(); else ints.Append(v);
                bools.Append(v % 3 == 0);
            }
            await writer.WriteBatchAsync(new RecordBatch(IntAndBool, [ints.Build(), bools.Build()], size));
        }
        await writer.CloseAsync();
    }

    private static async Task<List<string>> ReadRows(string path)
    {
        await using var reader = await OrcReader.OpenAsync(path);
        var rows = new List<string>();
        await foreach (var batch in reader.CreateRowReader())
        {
            var i = (Int32Array)batch.Column(0);
            var b = (BooleanArray)batch.Column(1);
            for (int r = 0; r < batch.Length; r++)
                rows.Add($"{(i.IsValid(r) ? i.GetValue(r)!.Value.ToString() : "null")}/{b.GetValue(r)}");
        }
        return rows;
    }

    [Theory]
    [InlineData(CompressionKind.None)]
    [InlineData(CompressionKind.Zstd)]
    public async Task RowGroupsEndingMidByte_KeepEveryBit(CompressionKind compression)
    {
        // Row groups of 3 rows end mid-byte. Padding the byte out there inserted bits a reader read as rows.
        var path = TempPath();
        try
        {
            await WriteRows(path, new OrcWriterOptions { RowIndexStride = 3, Compression = compression }, 3, 3, 3, 3, 5);
            var expected = Enumerable.Range(0, 17).Select(Expected).ToList();
            Assert.Equal(expected, await ReadRows(path));

            if (CrossValidationTests.IsPyArrowAvailable())
            {
                var read = CrossValidationTests.RunPython(
                    "import sys, json, pyarrow.orc as orc\n" +
                    "t = orc.read_table(sys.argv[1])\n" +
                    "print(json.dumps([('null' if i is None else str(i)) + '/' + str(b) for i, b in zip(t.column('i').to_pylist(), t.column('b').to_pylist())]))\n",
                    path);
                Assert.Equal(System.Text.Json.JsonSerializer.Serialize(expected), read.Replace(", ", ","));
            }
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task RowGroupsStartingMidByte_GiveTheirBitOffset()
    {
        var path = TempPath();
        try
        {
            await WriteRows(path, new OrcWriterOptions { RowIndexStride = 3, Compression = CompressionKind.None }, 3, 3, 3, 3);
            await using var reader = await OrcReader.OpenAsync(path);
            var index = await reader.ReadRowIndexAsync(0);

            // (byte offset, byte RLE remaining, bit offset) for the row groups starting at rows 0, 3, 6 and 9. By
            // row 9 one byte is complete and written as a 2-byte literal run (header and byte).
            ulong[][] expected = [[0, 0, 0], [0, 0, 3], [0, 0, 6], [2, 0, 1]];
            foreach (int column in new[] { 1, 2 })
            {
                var entries = index[column].Entry;
                Assert.Equal(4, entries.Count);
                for (int g = 0; g < 4; g++)
                    Assert.Equal(expected[g], entries[g].Positions.Take(3));
            }
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Theory]
    [InlineData(CompressionKind.None)]
    [InlineData(CompressionKind.Zstd)]
    public async Task NullsOnlyInALaterBatch_KeepTheEarlierRowsPresentBits(CompressionKind compression)
    {
        // The first null is in the third batch, after two row groups; the earlier rows need their bits, and
        // those row groups' index entries the PRESENT positions.
        var schema = new Schema([new Field("c", Int32Type.Default, true)], null);
        var path = TempPath();
        try
        {
            await using (var writer = OrcWriter.Create(path, schema,
                new OrcWriterOptions { RowIndexStride = 2, Compression = compression }))
            {
                foreach (var values in new[] { new int?[] { 1, 2 }, [3, 4], [5, null] })
                {
                    var b = new Int32Array.Builder();
                    foreach (var v in values)
                        if (v is null) b.AppendNull(); else b.Append(v.Value);
                    await writer.WriteBatchAsync(new RecordBatch(schema, [b.Build()], values.Length));
                }
                await writer.CloseAsync();
            }

            await using var reader = await OrcReader.OpenAsync(path);
            Assert.Equal(1, reader.NumberOfStripes);
            var entries = (await reader.ReadRowIndexAsync(0))[1].Entry;
            Assert.Equal(3, entries.Count);
            Assert.All(entries, e => Assert.Equal(entries[0].Positions.Count, e.Positions.Count));
            if (compression == CompressionKind.None)
                Assert.Equal(5, entries[0].Positions.Count); // PRESENT (3) and DATA (2)

            var read = new List<string>();
            await foreach (var batch in reader.CreateRowReader())
            {
                var c = (Int32Array)batch.Column(0);
                for (int i = 0; i < c.Length; i++)
                    read.Add(c.IsValid(i) ? c.GetValue(i)!.Value.ToString() : "null");
            }
            Assert.Equal(["1", "2", "3", "4", "5", "null"], read);

            if (CrossValidationTests.IsPyArrowAvailable())
            {
                Assert.Equal("[1, 2, 3, 4, 5, null]", CrossValidationTests.RunPython(
                    "import sys, json, pyarrow.orc as orc\n" +
                    "print(json.dumps(orc.read_table(sys.argv[1]).column('c').to_pylist()))\n", path));
            }
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task AStripeWithoutNulls_HasNoPresentStreamOrPositions()
    {
        var schema = new Schema([new Field("c", Int32Type.Default, true)], null);
        var path = TempPath();
        try
        {
            await using (var writer = OrcWriter.Create(path, schema,
                new OrcWriterOptions { RowIndexStride = 2, Compression = CompressionKind.None }))
            {
                for (int k = 0; k < 3; k++)
                {
                    var values = new Int32Array.Builder().Append(2 * k).Append(2 * k + 1).Build();
                    await writer.WriteBatchAsync(new RecordBatch(schema, [values], 2));
                }
                await writer.CloseAsync();
            }

            await using var reader = await OrcReader.OpenAsync(path);
            Assert.All((await reader.ReadRowIndexAsync(0))[1].Entry, e => Assert.Equal(2, e.Positions.Count)); // DATA only
            var read = new List<int>();
            await foreach (var batch in reader.CreateRowReader())
            {
                read.AddRange(((Int32Array)batch.Column(0)).Values.ToArray());
                GC.KeepAlive(batch);
            }

            Assert.Equal([0, 1, 2, 3, 4, 5], read);
        }
        finally
        {
            File.Delete(path);
        }
    }
}
