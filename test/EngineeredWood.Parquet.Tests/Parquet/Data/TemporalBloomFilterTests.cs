// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

#pragma warning disable EWPARQUET0004 // One case covers the experimental carrier.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Expressions;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using Ex = EngineeredWood.Expressions.Expressions;
using TimeUnit = Apache.Arrow.Types.TimeUnit;

namespace EngineeredWood.Tests.Parquet.Data;

/// <summary>
/// A predicate on a DATE, TIME or TIMESTAMP column could never probe a bloom filter. The statistics
/// layer hands those literals over as <see cref="DateOnly"/> / <see cref="TimeOnly"/> /
/// <see cref="DateTimeOffset"/>, and the bloom coercion dispatched on the PHYSICAL type only — where
/// every one of them fell through to null and the filter went unconsulted. Enabling a bloom filter on a
/// timestamp column bought nothing.
///
/// Not a correctness bug — declining to probe only costs a pruning opportunity — but the whole point of
/// writing the filter was to get that opportunity.
///
/// The rule these pin is exactness: a literal is worth probing with only if it converts to the column's
/// unit without a remainder. A 1.5 ms literal against a MILLIS column does not, and rounding it would
/// probe for a value the caller never asked about.
/// </summary>
public sealed class TemporalBloomFilterTests : IDisposable
{
    private readonly string _tempDir;

    public TemporalBloomFilterTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-temporal-bloom-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            Directory.Delete(_tempDir, recursive: true);
        }

        GC.SuppressFinalize(this);
    }

    private static readonly DateTimeOffset Epoch = DateTimeOffset.FromUnixTimeMilliseconds(0);

    private async Task<string> WriteTimestampsAsync(
        TimeUnit unit, long[] values, IReadOnlyCollection<string>? promoted = null)
    {
        var path = Path.Combine(_tempDir, Guid.NewGuid().ToString("N")[..8] + ".parquet");
        var type = new TimestampType(unit, "UTC");

        var buffer = new ArrowBuffer.Builder<long>();
        foreach (long v in values)
        {
            buffer.Append(v);
        }

        var validity = new byte[(values.Length + 7) / 8];
        for (int i = 0; i < values.Length; i++)
        {
            validity[i / 8] |= (byte)(1 << (i % 8));
        }

        var array = new TimestampArray(
            new ArrayData(type, values.Length, 0, 0, [new ArrowBuffer(validity), buffer.Build()]));
        var schema = new Apache.Arrow.Schema([new Field("t", type, nullable: false)], null);

        await using var file = new LocalSequentialFile(path);
        await using var writer = new ParquetFileWriter(file, options: new ParquetWriteOptions
        {
            BloomFilterColumns = ["t"],
            ExtendedTimestampColumns = promoted,
        });
        await writer.WriteRowGroupAsync(new RecordBatch(schema, [array], values.Length));
        return path;
    }

    private static async Task<bool> MightContainAsync(
        string path, LiteralValue literal, bool useBloom = true)
    {
        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(
            file,
            ownsFile: false,
            new ParquetReadOptions
            {
                Filter = Ex.Equal("t", literal),
                FilterUseBloomFilters = useBloom,
            });

        // Pruning happens while enumerating, not when a row group is asked for by index -- naming one
        // explicitly is a request to read it, filter or no filter.
        await foreach (var batch in reader.ReadAllAsync())
        {
            if (batch.Length > 0)
                return true;
        }

        return false;
    }

    // 2000 ms is a GAP: inside the column's min/max, absent from it. Every probe below uses a gap value,
    // because a literal outside min/max is pruned by statistics and would pass whether or not the bloom
    // filter was ever consulted. Only a gap isolates what the filter contributes.
    private static readonly long[] WithGap = [1_000L, 3_000L];

    [Fact]
    public async Task AGapValueIsPrunedOnlyBecauseOfTheFilter()
    {
        var withFilter = await WriteTimestampsAsync(TimeUnit.Millisecond, WithGap);
        var gap = LiteralValue.Of(Epoch.AddMilliseconds(2_000));

        Assert.False(await MightContainAsync(withFilter, gap));

        // Same file, same predicate, filter not consulted: statistics alone cannot rule the gap out, so
        // the row group is read. This is the control that makes the assertion above mean something.
        Assert.True(await MightContainAsync(withFilter, gap, useBloom: false));
    }

    [Fact]
    public async Task APresentTimestampIsNotPruned()
    {
        // The direction that must never be wrong: a value that IS there has to survive.
        var path = await WriteTimestampsAsync(TimeUnit.Millisecond, WithGap);

        Assert.True(await MightContainAsync(path, LiteralValue.Of(Epoch.AddMilliseconds(3_000))));
    }

    [Fact]
    public async Task EveryStoredValueSurvivesItsOwnProbe()
    {
        long[] values = [-5_000L, -1L, 0L, 1L, 999_999L];
        var path = await WriteTimestampsAsync(TimeUnit.Microsecond, values);


        foreach (long v in values)
        {
            var literal = LiteralValue.Of(Epoch.AddTicks(v * 10));
            Assert.True(await MightContainAsync(path, literal), $"{v} µs was pruned but is present");
        }
    }

    [Fact]
    public async Task AnInexactLiteralDeclinesToProbeRatherThanRounding()
    {
        // 1500.5 ms cannot be a MILLIS value at all. Rounding it to 1500 or 1501 would probe for something
        // the caller never asked about; declining means the row group is read, which is always safe.
        //
        // The literal sits INSIDE the column's min/max on purpose: outside it, statistics would prune the
        // row group first and the test would pass without the bloom path being exercised at all.
        var path = await WriteTimestampsAsync(TimeUnit.Millisecond, [1_000L, 2_000L]);
        var inexact = Epoch.AddMilliseconds(1_500).AddTicks(5_000);

        Assert.True(await MightContainAsync(path, LiteralValue.Of(inexact)));
    }

    [Fact]
    public async Task NanosecondColumnsProbeExactly()
    {
        // A tick is 100 ns, so every DateTimeOffset lands exactly on a nanosecond count — this unit never
        // has to decline.
        var path = await WriteTimestampsAsync(TimeUnit.Nanosecond, [100L, 300L]);

        Assert.True(await MightContainAsync(path, LiteralValue.Of(Epoch.AddTicks(3))));   // 300 ns, present
        Assert.False(await MightContainAsync(path, LiteralValue.Of(Epoch.AddTicks(2))));  // 200 ns, the gap
    }

    [Fact]
    public async Task TheExtendedCarrierProbesOnItsOwnBytes()
    {
        // The filter holds the hash of the twelve bytes as they sit in the file, so the literal has to
        // become those same bytes rather than an int64.
        var path = await WriteTimestampsAsync(
            TimeUnit.Microsecond, [1_000_000L, 3_000_000L], promoted: ["t"]);

        Assert.True(await MightContainAsync(path, LiteralValue.Of(Epoch.AddSeconds(3))));
        Assert.False(await MightContainAsync(path, LiteralValue.Of(Epoch.AddSeconds(2))));
        Assert.True(await MightContainAsync(path, LiteralValue.Of(Epoch.AddSeconds(2)), useBloom: false));
    }

    [Fact]
    public async Task ThePromotedAndOrdinaryCarriersAgreeOnWhatIsPresent()
    {
        // Same timestamps, two physical layouts, one answer. The bytes hashed differ entirely; the
        // observable result must not.
        long[] values = [1_000_000L, 3_000_000L];
        var ordinary = await WriteTimestampsAsync(TimeUnit.Microsecond, values);
        var promoted = await WriteTimestampsAsync(TimeUnit.Microsecond, values, promoted: ["t"]);

        foreach (int second in new[] { 1, 2, 3 })
        {
            var literal = LiteralValue.Of(Epoch.AddSeconds(second));
            Assert.Equal(
                await MightContainAsync(ordinary, literal),
                await MightContainAsync(promoted, literal));
        }
    }
}
