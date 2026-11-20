// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Data;

namespace EngineeredWood.Tests.Parquet.Data;

/// <summary>
/// A column chunk writer reuses an append's own arrays (definition levels, dictionary indices, narrowed decimals) for
/// the next append once <see cref="ColumnChunkWriter.Detach"/> has copied what the chunk keeps of them, and reuses the
/// arrays of the open page it joins from page to page (#940). Reuse must change nothing: the same appends written with
/// a Detach after each (reusing) and without (allocating) give the same bytes. The appends are small against a page,
/// so the open page and an undecided dictionary's held values span many of them, and any array written over too early
/// would show in the pages.
/// </summary>
public class AppendScratchReuseTests
{
    private static readonly string[] Path = ["c"];

    public static TheoryData<string, bool> Cases()
    {
        var data = new TheoryData<string, bool>();
        foreach (var kind in new[] { "dict", "dict-nullable", "plain", "plain-nullable", "declined", "decimal", "decimal-plain", "string", "string-plain", "constant" })
        {
            data.Add(kind, false);
            data.Add(kind, true);
        }
        return data;
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public void Detached_Appends_ReuseScratch_AndWriteWhatAllocatingAppendsWrite(string kind, bool varyingSizes)
    {
        var (values, physicalType, nullable, options) = Column(kind);
        ColumnChunkWriter NewWriter() =>
            new(Path, physicalType, 0, maxDefLevel: nullable ? 1 : 0, maxRepLevel: 0, options);

        var allocating = InAppends(NewWriter, values, varyingSizes, detach: false);
        var reusing = InAppends(NewWriter, values, varyingSizes, detach: true);

        Assert.True(allocating.PageIndex!.PageLocations.Count > 3, $"{allocating.PageIndex.PageLocations.Count} pages");
        Assert.Equal(allocating.Prefix?.ToArray() ?? [], reusing.Prefix?.ToArray() ?? []);
        Assert.Equal(allocating.Data.ToArray(), reusing.Data.ToArray());
        Assert.Equal(allocating.MetaData.Statistics!.MinValue, reusing.MetaData.Statistics!.MinValue);
        Assert.Equal(allocating.MetaData.Statistics.MaxValue, reusing.MetaData.Statistics.MaxValue);
        Assert.Equal(allocating.MetaData.Statistics.NullCount, reusing.MetaData.Statistics.NullCount);
        GC.KeepAlive(values);
    }

    /// <summary>
    /// A constant append is fed as one dictionary lookup and its indices filled in; a lent array is not zeroed, so
    /// entry 0 must be filled too. Runs of one value alternate with runs of another, each a whole append, so a reused
    /// array holds the other entry when entry 0 comes round again.
    /// </summary>
    [Fact]
    public void ConstantAppends_OfEntryZero_AfterAnotherEntry_ReadBack()
    {
        const int piece = 4096;
        var builder = new Int32Array.Builder();
        for (int i = 0; i < 64 * piece; i++)
            builder.Append((i / piece) % 2 == 0 ? 5 : 7);
        var values = builder.Build();
        var options = ParquetWriteOptions.Default with { DataPageSize = 1024 };
        ColumnChunkWriter NewWriter() => new(Path, PhysicalType.Int32, 0, maxDefLevel: 0, maxRepLevel: 0, options);

        var allocating = InAppends(NewWriter, values, varyingSizes: false, detach: false, piece);
        var reusing = InAppends(NewWriter, values, varyingSizes: false, detach: true, piece);

        Assert.Contains(Encoding.RleDictionary, reusing.MetaData.Encodings);
        Assert.Equal(allocating.Data.ToArray(), reusing.Data.ToArray());
    }

    /// <summary>
    /// A run-end encoded append's definition levels are written only where a run holds a value, so a reused levels
    /// array must start zeroed: left with the previous append's levels, a null run read as values. The appends are of
    /// one length, so each reuses the last one's array, and their null runs fall in different places.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void RunEndEncodedNullableAppends_ReuseZeroedLevels(bool dictionary)
    {
        const int appendLength = 1000;
        var options = ParquetWriteOptions.Default with { DataPageSize = 4096, DictionaryEnabled = dictionary };
        var random = new Random(940);
        var appends = new List<RunEndEncodedArray>();
        for (int a = 0; a < 100; a++)
        {
            var ends = new Int32Array.Builder();
            var values = new StringArray.Builder();
            for (int end = 0; end < appendLength;)
            {
                end = Math.Min(appendLength, end + 1 + random.Next(60));
                ends.Append(end);
                if (random.Next(3) == 0)
                    values.AppendNull();
                else
                    values.Append("r" + random.Next(dictionary ? 50 : int.MaxValue));
            }
            appends.Add(new RunEndEncodedArray(ends.Build(), values.Build()));
        }

        ColumnChunkWriter.ColumnChunkResult Write(bool detach)
        {
            var writer = new ColumnChunkWriter(Path, PhysicalType.ByteArray, 0, maxDefLevel: 1, maxRepLevel: 0, options);
            foreach (var append in appends)
            {
                writer.AppendFlat(append);
                if (detach)
                    writer.Detach();
            }
            return writer.Finish();
        }

        var allocating = Write(detach: false);
        var reusing = Write(detach: true);

        Assert.Equal(allocating.MetaData.Statistics!.NullCount, reusing.MetaData.Statistics!.NullCount);
        Assert.Equal(allocating.Prefix?.ToArray() ?? [], reusing.Prefix?.ToArray() ?? []);
        Assert.Equal(allocating.Data.ToArray(), reusing.Data.ToArray());
    }

    /// <summary>
    /// The bytes under a null decimal are arbitrary, and need not fit the narrower layout; they are written as zero,
    /// including into scratch that held other values.
    /// </summary>
    [Fact]
    public void DecimalNarrowing_IntoDirtyScratch_ZeroesTheBytesUnderNulls()
    {
        // Row 1 is null over a value too wide for INT64; rows 0 and 2 hold 1 and -1.
        var type = new Decimal128Type(18, 0);
        var slots = new byte[3 * 16];
        slots[0] = 1;
        slots.AsSpan(16, 16).Fill(0x7F);
        slots.AsSpan(32, 16).Fill(0xFF);
        var validity = new byte[] { 0b101 };
        var array = new Decimal128Array(new ArrayData(
            type, 3, nullCount: 1, offset: 0, [new ArrowBuffer(validity), new ArrowBuffer(slots)]));

        var scratch = Enumerable.Repeat((byte)0xAB, 64).ToArray();
        var narrowed = DecimalNarrowing.ToInteger(array, new Decimal64Type(18, 0), scratch);

        var values = narrowed.Data.Buffers[1].Span;
        Assert.Equal(1L, BitConverter.ToInt64(values.Slice(0, 8).ToArray(), 0));
        Assert.Equal(0L, BitConverter.ToInt64(values.Slice(8, 8).ToArray(), 0));
        Assert.Equal(-1L, BitConverter.ToInt64(values.Slice(16, 8).ToArray(), 0));
        Assert.True(narrowed.IsNull(1));
        GC.KeepAlive(array);
    }

#if NET6_0_OR_GREATER
    /// <summary>
    /// A run-end encoded append is fed in run form, O(runs) in allocation: it must not take a per-row index scratch it
    /// never uses (#943 review). A 1M-row single-run column would grow a 4 MB one.
    /// </summary>
    /// <remarks>.NET Framework has no per-thread allocation counter, so the pin runs on the modern targets only.</remarks>
    [Fact]
    public void RunEndEncodedAppend_TakesNoPerRowIndexScratch()
    {
        const int rows = 1_000_000;
        var ends = new Int32Array.Builder().Append(rows).Build();
        var values = new StringArray.Builder().Append("update_postimage").Build();
        var array = new RunEndEncodedArray(ends, values);
        ColumnChunkWriter NewWriter() =>
            new(Path, PhysicalType.ByteArray, 0, maxDefLevel: 0, maxRepLevel: 0, ParquetWriteOptions.Default);

        NewWriter().AppendFlat(array);

        var writer = NewWriter();
        long before = GC.GetAllocatedBytesForCurrentThread();
        writer.AppendFlat(array);
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.True(allocated < 1024 * 1024,
            $"Appending a 1,000,000-row single-run column allocated {allocated:N0} bytes.");
        GC.KeepAlive(array);
    }
#endif

    private static ColumnChunkWriter.ColumnChunkResult InAppends(
        Func<ColumnChunkWriter> newWriter, IArrowArray values, bool varyingSizes, bool detach, int piece = 777)
    {
        var writer = newWriter();
        var random = new Random(940);
        for (int offset = 0; offset < values.Length;)
        {
            int left = values.Length - offset;
            int length = !varyingSizes ? Math.Min(piece, left) : random.Next(4) switch
            {
                0 => Math.Min(left, 1 + random.Next(3)),
                1 => Math.Min(left, 1 + random.Next(100)),
                2 => Math.Min(left, 1 + random.Next(5000)),
                _ => Math.Min(left, 1 + random.Next(Math.Max(1, left))),
            };
            // A copy at offset 0, as a caller's batch would be; the copy also makes any array the writer kept from an
            // earlier append the writer's own business.
            writer.AppendFlat(EngineeredWood.Arrow.ArrowCompute.Take(values, Enumerable.Range(offset, length).ToArray()));
            if (detach)
                writer.Detach();
            offset += length;
        }
        return writer.Finish();
    }

    private static (IArrowArray Values, PhysicalType PhysicalType, bool Nullable, ParquetWriteOptions Options) Column(string kind)
    {
        const int rows = 200_000;
        var random = new Random(940);
        var small = ParquetWriteOptions.Default with { DataPageSize = 16 << 10 };
        var noDictionary = small with { DictionaryEnabled = false };

        switch (kind)
        {
            case "dict":
            case "dict-nullable":
            case "plain":
            case "plain-nullable":
            {
                bool nullable = kind.EndsWith("-nullable", StringComparison.Ordinal);
                var builder = new Int32Array.Builder();
                for (int i = 0; i < rows; i++)
                {
                    if (nullable && random.Next(5) == 0)
                        builder.AppendNull();
                    else
                        builder.Append(random.Next(kind.StartsWith("dict", StringComparison.Ordinal) ? 300 : int.MaxValue));
                }
                return (builder.Build(), PhysicalType.Int32, nullable, kind.StartsWith("dict", StringComparison.Ordinal) ? small : noDictionary);
            }

            case "declined":
            {
                // High-cardinality appends are held until the first-page test declines the dictionary.
                var builder = new Int64Array.Builder();
                var draw = new byte[8];
                for (int i = 0; i < rows; i++)
                {
                    random.NextBytes(draw);
                    builder.Append(BitConverter.ToInt64(draw, 0));
                }
                return (builder.Build(), PhysicalType.Int64, false, small);
            }

            case "decimal":
            case "decimal-plain":
            {
                // Decimal128 of precision 18 is narrowed to INT64 per append.
                var builder = new Decimal128Array.Builder(new Decimal128Type(18, 2));
                for (int i = 0; i < rows; i++)
                {
                    if (random.Next(10) == 0)
                        builder.AppendNull();
                    else
                        builder.Append((decimal)random.Next(kind == "decimal" ? 500 : int.MaxValue) / 100m);
                }
                return (builder.Build(), PhysicalType.Int64, true, kind == "decimal" ? small : noDictionary);
            }

            case "string":
            case "string-plain":
            {
                var builder = new StringArray.Builder();
                for (int i = 0; i < rows; i++)
                {
                    if (random.Next(7) == 0)
                        builder.AppendNull();
                    else
                        builder.Append("v" + random.Next(kind == "string" ? 400 : int.MaxValue));
                }
                return (builder.Build(), PhysicalType.ByteArray, true, kind == "string" ? small : noDictionary);
            }

            default: // constant: every append one value, alternating between appends of different sizes
            {
                var builder = new Int32Array.Builder();
                for (int i = 0; i < rows; i++)
                    builder.Append((i / 777) % 3);
                return (builder.Build(), PhysicalType.Int32, false, small);
            }
        }
    }
}
