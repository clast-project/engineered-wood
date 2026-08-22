// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Expressions;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using TimeUnit = Apache.Arrow.Types.TimeUnit;

namespace EngineeredWood.Tests.Parquet.Data;

/// <summary>
/// Row-group statistics bounds for sub-millisecond timestamps used to be wrong in the direction that
/// loses data. <c>ParquetStatisticsAccessor</c> converted every timestamp bound through
/// <c>DateTimeOffset.FromUnixTimeMilliseconds</c>, so a MICROS or NANOS column had everything below a
/// millisecond truncated toward zero — a max bound of 1500 µs came back as 0 ms.
///
/// A max bound that is too SMALL is not a rounding blemish: a predicate of <c>t &gt; 0.5ms</c> compares
/// against it, concludes the row group cannot match, and prunes rows that genuinely do. These pin the
/// rule that replaced it — a bound may only ever move OUTWARD, and a bound that cannot be represented
/// at all is dropped rather than clamped.
/// </summary>
public sealed class TimestampBoundPrecisionTests : IDisposable
{
    private readonly string _tempDir;

    public TimestampBoundPrecisionTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-ts-bounds-" + Guid.NewGuid().ToString("N")[..8]);
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

    private async Task<(LiteralValue? Min, LiteralValue? Max)> BoundsAsync(TimeUnit unit, params long[] values)
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

        await using (var file = new LocalSequentialFile(path))
        {
            await using var writer = new ParquetFileWriter(file, options: new ParquetWriteOptions());
            await writer.WriteRowGroupAsync(new RecordBatch(schema, [array], values.Length));
        }

        await using var input = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(input, ownsFile: false);
        var metadata = await reader.ReadMetadataAsync();
        var accessor = new ParquetStatisticsAccessor(await reader.GetSchemaAsync());

        return (accessor.GetMinValue(metadata.RowGroups[0], "t"),
                accessor.GetMaxValue(metadata.RowGroups[0], "t"));
    }

    private static DateTimeOffset Utc(long ticksSinceEpoch) =>
        new DateTimeOffset(621_355_968_000_000_000L + ticksSinceEpoch, TimeSpan.Zero);

    [Fact]
    public async Task MicrosecondBoundsAreExact()
    {
        // 1 µs is 10 ticks, so nothing needs to round at all — the old code lost these entirely.
        var (min, max) = await BoundsAsync(TimeUnit.Microsecond, 500L, 1500L);

        Assert.Equal(Utc(5_000), min?.AsDateTimeOffset);
        Assert.Equal(Utc(15_000), max?.AsDateTimeOffset);
    }

    [Fact]
    public async Task MillisecondBoundsAreExact()
    {
        var (min, max) = await BoundsAsync(TimeUnit.Millisecond, -3L, 7L);

        Assert.Equal(Utc(-30_000), min?.AsDateTimeOffset);
        Assert.Equal(Utc(70_000), max?.AsDateTimeOffset);
    }

    [Fact]
    public async Task NanosecondBoundsRoundOutward()
    {
        // 1 tick is 100 ns, so 150 ns and 250 ns both fall between ticks. The min must round DOWN and the
        // max UP; rounding either the other way would exclude a value the file actually holds.
        var (min, max) = await BoundsAsync(TimeUnit.Nanosecond, 150L, 250L);

        Assert.Equal(Utc(1), min?.AsDateTimeOffset);  // floor(150/100) = 1
        Assert.Equal(Utc(3), max?.AsDateTimeOffset);  // ceil(250/100)  = 3
    }

    [Fact]
    public async Task NegativeNanosecondBoundsRoundOutwardToo()
    {
        // Pre-epoch, where truncation toward zero rounds the opposite way and the signs matter.
        var (min, max) = await BoundsAsync(TimeUnit.Nanosecond, -250L, -150L);

        Assert.Equal(Utc(-3), min?.AsDateTimeOffset);  // floor(-250/100) = -3
        Assert.Equal(Utc(-1), max?.AsDateTimeOffset);  // ceil(-150/100)  = -1
    }

    [Fact]
    public async Task BoundsThatCannotBeRepresentedAreDroppedRatherThanClamped()
    {
        // MILLIS spans far past year 9999. A clamped bound is indistinguishable from a real endpoint and
        // would prune on a value the file never contained, so there must be no bound at all.
        var (min, max) = await BoundsAsync(TimeUnit.Millisecond, long.MinValue / 2, long.MaxValue / 2);

        Assert.Null(min);
        Assert.Null(max);
    }

    [Fact]
    public async Task ARepresentableBoundSurvivesEvenWhenItsPartnerDoesNot()
    {
        var (min, max) = await BoundsAsync(TimeUnit.Millisecond, 0L, long.MaxValue / 2);

        Assert.Equal(Utc(0), min?.AsDateTimeOffset);
        Assert.Null(max);
    }

    [Theory]
    [InlineData(TimeUnit.Millisecond)]
    [InlineData(TimeUnit.Microsecond)]
    [InlineData(TimeUnit.Nanosecond)]
    public async Task TheBoundsAlwaysContainEveryValue(TimeUnit unit)
    {
        // The invariant the whole fix exists for, stated directly: whatever rounding happens, every value
        // in the column must still fall inside the range the footer advertises.
        long[] values = [-1_234_567L, -1L, 0L, 1L, 999L, 1_000L, 1_001L, 7_654_321L];
        var (min, max) = await BoundsAsync(unit, values);

        Assert.NotNull(min);
        Assert.NotNull(max);

        long ticksPerUnit = unit switch
        {
            TimeUnit.Millisecond => 10_000L,
            TimeUnit.Microsecond => 10L,
            _ => 1L,
        };

        foreach (long v in values)
        {
            // Nanoseconds are the only unit that cannot land on a tick, so compare in nanoseconds.
            long valueNanos = unit == TimeUnit.Nanosecond ? v : v * ticksPerUnit * 100L;
            long minNanos = ((min!.Value.AsDateTimeOffset).Ticks - 621_355_968_000_000_000L) * 100L;
            long maxNanos = ((max!.Value.AsDateTimeOffset).Ticks - 621_355_968_000_000_000L) * 100L;

            Assert.True(minNanos <= valueNanos, $"min bound excludes {v}");
            Assert.True(maxNanos >= valueNanos, $"max bound excludes {v}");
        }
    }
}
