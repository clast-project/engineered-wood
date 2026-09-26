// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

#if NET8_0_OR_GREATER
using System.Buffers;
using System.Diagnostics;
using System.Globalization;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.Benchmarks;

/// <summary>
/// A batched read's cost with the page map built from the OffsetIndex against the same data
/// without an index, whose map comes from reading each chunk to scan its page headers. Reports
/// the median time of alternating rounds, and the requests and bytes each read issues.
/// </summary>
/// <remarks>Run with: dotnet run -c Release -f net10.0 -- pagemap-ab [rounds] [schemas...]</remarks>
internal static class PageMapReadAb
{
    public static async Task<int> RunAsync(string[] args)
    {
        int rounds = args.Length > 1 ? int.Parse(args[1], CultureInfo.InvariantCulture) : 25;
        string[] schemas = args.Length > 2 ? args[2..] : ["plain", "dictionary", "strings"];
        string dir = Path.Combine(Path.GetTempPath(), "ew-pagemap-ab-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);

        try
        {
            Console.WriteLine($"{PageIndexWorkload.Rows:N0} rows, one row group; median of {rounds} alternating rounds after 3 warm-up rounds.");
            Console.WriteLine("| Schema | Pages | Batch | ms headers | ms index | Time | Requests headers | index | Bytes headers | index | File |");
            Console.WriteLine("|---|---|---|---|---|---|---|---|---|---|---|");
            foreach (string schema in schemas)
            {
                var batch = PageIndexWorkload.Build(schema);
                foreach (var version in new[] { DataPageVersion.V1, DataPageVersion.V2 })
                {
                    string off = Path.Combine(dir, $"{schema}-{version}-off.parquet");
                    string on = Path.Combine(dir, $"{schema}-{version}-on.parquet");
                    await WriteAsync(batch, off, PageIndexWorkload.Options(version, PageIndexWorkload.DefaultPageSize, writePageIndex: false));
                    await WriteAsync(batch, on, PageIndexWorkload.Options(version, PageIndexWorkload.DefaultPageSize, writePageIndex: true));

                    foreach (var (label, options) in new[]
                    {
                        ("64Ki rows", new ParquetReadOptions { BatchSize = 65_536 }),
                        ("8 MiB", new ParquetReadOptions { MaxBatchByteSize = 8L * 1024 * 1024 }),
                    })
                    {
                        var msOff = new List<double>();
                        var msOn = new List<double>();
                        var ratios = new List<double>();
                        (int Requests, long Bytes) ioOff = default, ioOn = default;
                        for (int round = -3; round < rounds; round++)
                        {
                            bool offFirst = round % 2 == 0;
                            var a = offFirst ? await ReadAsync(off, options) : default;
                            var b = await ReadAsync(on, options);
                            if (!offFirst)
                                a = await ReadAsync(off, options);

                            (ioOff, ioOn) = ((a.Requests, a.Bytes), (b.Requests, b.Bytes));
                            if (round < 0)
                                continue;
                            msOff.Add(a.Ms);
                            msOn.Add(b.Ms);
                            ratios.Add(b.Ms / a.Ms);
                        }

                        Console.WriteLine(
                            $"| {schema} | {version} | {label} | {Median(msOff):F1} | {Median(msOn):F1} | {(Median(ratios) - 1) * 100:+0;-0;0}% " +
                            $"| {ioOff.Requests} | {ioOn.Requests} | {ioOff.Bytes:N0} | {ioOn.Bytes:N0} | {new FileInfo(on).Length:N0} |");
                    }
                }
            }
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }

        return 0;
    }

    private static async Task WriteAsync(Apache.Arrow.RecordBatch batch, string path, ParquetWriteOptions options)
    {
        await using var file = new LocalSequentialFile(path);
        await using var writer = new ParquetFileWriter(file, ownsFile: false, options);
        await writer.WriteRowGroupAsync(batch);
        await writer.CloseAsync();
    }

    private static async Task<(double Ms, int Requests, long Bytes)> ReadAsync(string path, ParquetReadOptions options)
    {
        await using var file = new CountingFile(new LocalRandomAccessFile(path));
        var clock = Stopwatch.StartNew();
        using (var reader = new ParquetFileReader(file, ownsFile: false, options))
        {
            await foreach (var batch in reader.ReadAllAsync())
                batch.Dispose();
        }

        return (clock.Elapsed.TotalMilliseconds, file.Requests, file.Bytes);
    }

    private static double Median(List<double> values)
    {
        var sorted = values.OrderBy(v => v).ToList();
        return sorted[sorted.Count / 2];
    }

    /// <summary>Counts requests (a ReadRangesAsync is one) and the bytes they ask for.</summary>
    private sealed class CountingFile(IRandomAccessFile inner) : IRandomAccessFile
    {
        private int _requests;
        private long _bytes;

        public int Requests => _requests;

        public long Bytes => Interlocked.Read(ref _bytes);

        public ValueTask<long> GetLengthAsync(CancellationToken cancellationToken = default) =>
            inner.GetLengthAsync(cancellationToken);

        public ValueTask<IMemoryOwner<byte>> ReadAsync(FileRange range, CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref _requests);
            Interlocked.Add(ref _bytes, range.Length);
            return inner.ReadAsync(range, cancellationToken);
        }

        public ValueTask<IReadOnlyList<IMemoryOwner<byte>>> ReadRangesAsync(
            IReadOnlyList<FileRange> ranges, CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref _requests);
            foreach (var range in ranges)
                Interlocked.Add(ref _bytes, range.Length);
            return inner.ReadRangesAsync(ranges, cancellationToken);
        }

        public ValueTask DisposeAsync() => inner.DisposeAsync();

        public void Dispose() => inner.Dispose();
    }
}
#endif
