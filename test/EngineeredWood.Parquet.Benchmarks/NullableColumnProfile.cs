// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

#if NET8_0_OR_GREATER
using System.Buffers;
using System.Diagnostics;
using System.Globalization;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Compression;
using EngineeredWood.IO;
using EngineeredWood.Parquet;

namespace EngineeredWood.Benchmarks;

/// <summary>
/// What nullability costs end to end: one column per file, written and read in memory and uncompressed,
/// declared non-nullable (the baseline) and nullable with 0%, 1 in 7 and half of its rows null.
/// </summary>
/// <remarks>
/// The nulls are placed at random (fixed seed), the case a per-value branch predicts worst.
/// Reports the best of <c>rounds</c> warm runs per cell.
/// Run with: dotnet run -c Release -f net10.0 -- nullable-profile [rounds] [rows] [kind...]
/// or <c>nullable-loop kind share seconds read|write</c> to spin one cell for a profiler.
/// </remarks>
internal static class NullableColumnProfile
{
    private static readonly string[] AllKinds = ["int64", "double", "string", "dictstring"];

    public static async Task<int> RunAsync(string[] args)
    {
        if (args[0] == "nullable-loop")
            return await LoopAsync(args);

        int rounds = args.Length > 1 ? int.Parse(args[1], CultureInfo.InvariantCulture) : 15;
        int rows = args.Length > 2 ? int.Parse(args[2], CultureInfo.InvariantCulture) : 1_000_000;
        string[] kinds = args.Length > 3 ? args[3..] : AllKinds;

        Console.WriteLine($"{rows:N0} rows, one column, uncompressed, in memory; best of {rounds} warm runs (ms).");
        Console.WriteLine("| Column | Nulls | Write ms | vs base | Read ms | vs base | File bytes |");
        Console.WriteLine("|---|---|---|---|---|---|---|");
        foreach (string kind in kinds)
        {
            double baseWrite = 0, baseRead = 0;
            foreach (double? share in new double?[] { null, 0.0, 1.0 / 7, 0.5 })
            {
                var batch = Build(kind, rows, share);
                var options = Options(kind);
                byte[] file = await WriteAsync(batch, options);
                double write = Best(rounds, () => DiscardWriteAsync(batch, options));
                double read = Best(rounds, () => ReadAsync(file));
                if (share is null)
                {
                    baseWrite = write;
                    baseRead = read;
                }

                string label = share is null ? "not nullable" : $"{share.Value * 100:F0}%";
                Console.WriteLine(
                    $"| {kind} | {label} | {write:F2} | {Ratio(write, baseWrite)} | {read:F2} | {Ratio(read, baseRead)} | {file.Length:N0} |");
            }
        }

        return 0;
    }

    private static async Task<int> LoopAsync(string[] args)
    {
        string kind = args[1];
        double? share = args[2] == "none" ? null : double.Parse(args[2], CultureInfo.InvariantCulture);
        double seconds = double.Parse(args[3], CultureInfo.InvariantCulture);
        bool read = args[4] == "read";
        var batch = Build(kind, 1_000_000, share);
        var options = Options(kind);
        byte[] file = await WriteAsync(batch, options);
        var clock = Stopwatch.StartNew();
        int n = 0;
        while (clock.Elapsed.TotalSeconds < seconds)
        {
            if (read)
                await ReadAsync(file);
            else
                await DiscardWriteAsync(batch, options);
            n++;
        }

        Console.WriteLine($"{n} iterations, {clock.Elapsed.TotalMilliseconds / n:F2} ms each");
        return 0;
    }

    private static string Ratio(double value, double baseline) =>
        baseline == 0 ? "" : (value / baseline).ToString("0.00x", CultureInfo.InvariantCulture);

    private static ParquetWriteOptions Options(string kind) => ParquetWriteOptions.Default with
    {
        Compression = CompressionCodec.Uncompressed,
        DictionaryEnabled = kind == "dictstring",
    };

    internal static RecordBatch Build(string kind, int rows, double? nullShare)
    {
        var random = new Random(42);
        bool IsNull() => nullShare is { } s && random.NextDouble() < s;
        IArrowArray array;
        IArrowType type;
        switch (kind)
        {
            case "int64":
            {
                var b = new Int64Array.Builder();
                for (int i = 0; i < rows; i++)
                {
                    if (IsNull()) b.AppendNull();
                    else b.Append(random.NextInt64());
                }

                array = b.Build();
                type = Int64Type.Default;
                break;
            }
            case "double":
            {
                var b = new DoubleArray.Builder();
                for (int i = 0; i < rows; i++)
                {
                    if (IsNull()) b.AppendNull();
                    else b.Append(random.NextDouble());
                }

                array = b.Build();
                type = DoubleType.Default;
                break;
            }
            case "string" or "dictstring":
            {
                var b = new StringArray.Builder();
                for (int i = 0; i < rows; i++)
                {
                    if (IsNull()) b.AppendNull();
                    else b.Append(kind == "string" ? "value-" + random.NextInt64().ToString(CultureInfo.InvariantCulture)
                        : "category-" + random.Next(1000).ToString(CultureInfo.InvariantCulture));
                }

                array = b.Build();
                type = StringType.Default;
                break;
            }
            default:
                throw new ArgumentException($"Unknown kind '{kind}'.");
        }

        var field = new Field("c", type, nullable: nullShare is not null);
        return new RecordBatch(new Apache.Arrow.Schema([field], null), [array], rows);
    }

    private static double Best(int rounds, Func<Task> action)
    {
        double best = double.MaxValue;
        for (int i = -3; i < rounds; i++)
        {
            var clock = Stopwatch.StartNew();
            action().GetAwaiter().GetResult();
            double ms = clock.Elapsed.TotalMilliseconds;
            if (i >= 0)
                best = Math.Min(best, ms);
        }

        return best;
    }

    private static async ValueTask<byte[]> WriteAsync(RecordBatch batch, ParquetWriteOptions options)
    {
        var file = new MemorySequentialFile();
        await using (var writer = new ParquetFileWriter(file, ownsFile: false, options))
        {
            await writer.WriteRowGroupAsync(batch);
            await writer.CloseAsync();
        }

        return file.ToArray();
    }

    private static async Task DiscardWriteAsync(RecordBatch batch, ParquetWriteOptions options)
    {
        await using var writer = new ParquetFileWriter(new CountingSequentialFile(), ownsFile: true, options);
        await writer.WriteRowGroupAsync(batch);
        await writer.CloseAsync();
    }

    private static async Task ReadAsync(byte[] file)
    {
        using var reader = new ParquetFileReader(new MemoryRandomAccessFile(file), ownsFile: true);
        await foreach (var batch in reader.ReadAllAsync())
            batch.Dispose();
    }

    private sealed class MemorySequentialFile : ISequentialFile
    {
        private readonly MemoryStream _stream = new();

        public long Position => _stream.Length;

        public ValueTask WriteAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
        {
            _stream.Write(data.Span);
            return default;
        }

        public ValueTask FlushAsync(CancellationToken cancellationToken = default) => default;

        public ValueTask CompleteAsync(CancellationToken cancellationToken = default) => default;

        public ValueTask DisposeAsync() => default;

        public void Dispose()
        {
        }

        public byte[] ToArray() => _stream.ToArray();
    }

    private sealed class MemoryRandomAccessFile(byte[] data) : IRandomAccessFile
    {
        public ValueTask<long> GetLengthAsync(CancellationToken cancellationToken = default) => new(data.LongLength);

        public ValueTask<IMemoryOwner<byte>> ReadAsync(FileRange range, CancellationToken cancellationToken = default) =>
            new(new Owner(new Memory<byte>(data, (int)range.Offset, (int)range.Length)));

        public async ValueTask<IReadOnlyList<IMemoryOwner<byte>>> ReadRangesAsync(
            IReadOnlyList<FileRange> ranges, CancellationToken cancellationToken = default)
        {
            var results = new IMemoryOwner<byte>[ranges.Count];
            for (int i = 0; i < ranges.Count; i++)
                results[i] = await ReadAsync(ranges[i], cancellationToken).ConfigureAwait(false);
            return results;
        }

        public void Dispose()
        {
        }

        public ValueTask DisposeAsync() => default;

        private sealed class Owner(Memory<byte> memory) : IMemoryOwner<byte>
        {
            public Memory<byte> Memory => memory;

            public void Dispose()
            {
            }
        }
    }
}
#endif
