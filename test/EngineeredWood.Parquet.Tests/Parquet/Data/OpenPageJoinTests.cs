// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Data;

namespace EngineeredWood.Tests.Parquet.Data;

/// <summary>
/// The open page a chunk carries between appends is completed by joining it with the levels of the next append that
/// finish it. A page cut by <see cref="ParquetWriteOptions.DataPageRowCountLimit"/> ends before its values fill
/// <see cref="ParquetWriteOptions.DataPageSize"/>, so the join takes only the rows that complete it: it once took
/// the rest of the append, copied it, and wrote one page of it.
/// </summary>
public class OpenPageJoinTests
{
    private static readonly string[] Path = ["c"];

#if NET6_0_OR_GREATER
    /// <summary>
    /// Appends of 100,700 rows against 1,000-row pages leave part of a page open nearly every time, and the next append
    /// completes it. Joining the rest of each append copied about 100,000 values into a fresh array, 400 KB a join;
    /// joining the rows that complete the page copies at most 1,000. The values climb by one, so their pages are small
    /// and the copy is most of what the appends allocate.
    /// </summary>
    /// <remarks>.NET Framework has no per-thread allocation counter, so the pin runs on the modern targets only.</remarks>
    [Fact]
    public void RowLimitedJoin_CopiesThePage_NotTheRestOfTheAppend()
    {
        const int appendRows = 100_700;
        const int appends = 20;
        var options = ParquetWriteOptions.Default with { DictionaryEnabled = false, DataPageRowCountLimit = 1_000 };
        var builder = new Int32Array.Builder();
        for (int i = 0; i < appendRows; i++)
            builder.Append(i);
        var values = builder.Build();

        long Allocated()
        {
            var writer = new ColumnChunkWriter(Path, PhysicalType.Int32, 0, maxDefLevel: 0, maxRepLevel: 0, options);
            writer.AppendFlat(values);
            writer.Detach();
            long before = GC.GetAllocatedBytesForCurrentThread();
            for (int i = 0; i < appends; i++)
            {
                writer.AppendFlat(values);
                writer.Detach();
            }
            long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
            writer.Finish();
            return allocated;
        }

        Allocated();
        long allocated = Allocated();

        // Measured: 1.67 MB, against 8.88 MB when each join took the rest of its append (about 400 KB a join).
        Assert.True(allocated < 4 * 1024 * 1024,
            $"{appends} appends of {appendRows:N0} rows allocated {allocated:N0} bytes.");
        GC.KeepAlive(values);
    }
#endif

    /// <summary>
    /// A repeated column's page cut by rows counts records, not level entries: the join takes the levels of the
    /// records that complete the open page. A page cut by size inside a record backs up to the record's start, which
    /// needs one entry past the cut. Appends of a few records each write what one append writes.
    /// </summary>
    [Theory]
    [InlineData(1, 0)]
    [InlineData(7, 0)]
    [InlineData(100, 0)]
    [InlineData(0, 64)]
    [InlineData(0, 200)]
    public void RepeatedColumn_JoinsAcrossAppends_WriteWhatOneAppendWrites(int rowLimit, int pageSize)
    {
        const int records = 20_000;
        var random = new Random(rowLimit + pageSize);
        var lengths = new int[records];
        var rep = new List<int>();
        for (int r = 0; r < records; r++)
        {
            lengths[r] = 1 + random.Next(9);
            for (int i = 0; i < lengths[r]; i++)
                rep.Add(i == 0 ? 0 : 1);
        }
        int levels = rep.Count;
        var builder = new Int32Array.Builder();
        for (int i = 0; i < levels; i++)
            builder.Append(random.Next());
        var values = builder.Build();
        var repLevels = rep.ToArray();
        var defLevels = Enumerable.Repeat(1, levels).ToArray();

        var options = ParquetWriteOptions.Default with { DictionaryEnabled = false };
        options = rowLimit > 0
            ? options with { DataPageRowCountLimit = rowLimit }
            : options with { DataPageRowCountLimit = null, DataPageSize = pageSize };
        ColumnChunkWriter NewWriter() =>
            new(Path, PhysicalType.Int32, 0, maxDefLevel: 1, maxRepLevel: 1, options);

        var whole = NewWriter();
        whole.Append(values, defLevels, repLevels, levels, levels);
        var oneAppend = whole.Finish();

        var writer = NewWriter();
        int record = 0, level = 0;
        while (record < records)
        {
            int take = Math.Min(records - record, 1 + random.Next(3 * Math.Max(rowLimit, pageSize / 4) + 5));
            int count = 0;
            for (int r = record; r < record + take; r++)
                count += lengths[r];
            var slice = EngineeredWood.Arrow.ArrowCompute.Take(values, Enumerable.Range(level, count).ToArray());
            writer.Append(
                slice, defLevels.AsSpan(level, count).ToArray(), repLevels.AsSpan(level, count).ToArray(), count, count);
            writer.Detach();
            record += take;
            level += count;
        }
        var appended = writer.Finish();

        Assert.True(oneAppend.PageIndex!.PageLocations.Count > 10);
        Assert.Equal(oneAppend.Data.ToArray(), appended.Data.ToArray());
        Assert.Equal(
            oneAppend.PageIndex.PageLocations.Select(p => p.FirstRowIndex),
            appended.PageIndex!.PageLocations.Select(p => p.FirstRowIndex));
        GC.KeepAlive(values);
    }
}
