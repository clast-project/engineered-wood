// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

#if NET8_0_OR_GREATER
using System.Buffers;
using System.Diagnostics;
using System.Globalization;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.Benchmarks;

/// <summary>
/// What <see cref="ParquetReadOptions.FilterUseDictionaries"/> costs and saves on a filtered
/// <see cref="ParquetFileReader.ReadAllAsync"/>, measured to decide whether it can default on (#57).
/// Reports the median time of alternating off/on rounds, and the requests, bytes and row groups each
/// read issues.
/// </summary>
/// <remarks>
/// <para>Every row group holds the same smallest and largest key, so statistics never decide an
/// equality and every case reaches the dictionary step. The cases:</para>
/// <list type="bullet">
/// <item><b>in every group</b>: the value is in every group's dictionary, so the dictionary rules
/// nothing out and each group pays one extra request for nothing. The worst case.</item>
/// <item><b>in one group</b>: the value is in one group's dictionary; the others are skipped.</item>
/// <item><b>plain key</b>: the key column is not dictionary-encoded, so no chunk is asked and the
/// option should cost nothing.</item>
/// </list>
/// <para>Local disk hides what matters most on object storage: each asked row group costs one more
/// request, issued before that group's data read. <c>latencyMs</c> delays every request by that much
/// to model a store's time to first byte.</para>
/// <para><c>cloud</c> reads through <see cref="CoalescingFileReader"/>, as the S3, Azure and GCS readers
/// do, and delays each GET it issues rather than each call: that reader merges only nearby ranges and
/// sends the rest as concurrent GETs. The table then reports both calls and GETs.</para>
/// <para><c>bloom</c> measures <see cref="ParquetReadOptions.FilterUseBloomFilters"/> the same way instead:
/// the key carries a Bloom filter, and dictionary pruning is off on both sides so that only the Bloom
/// step differs. The third case is then a key with no Bloom filter.</para>
/// Run with: dotnet run -c Release -f net10.0 -- dictpruning-ab [rounds] [latencyMs] [direct|cloud] [dictionary|bloom]
/// </remarks>
internal static class DictionaryPruningAb
{
    private const int RowGroups = 32;
    private const int RowsPerGroup = 65_536;
    private const int KeysPerGroup = 64;

    public static async Task<int> RunAsync(string[] args)
    {
        int rounds = args.Length > 1 ? int.Parse(args[1], CultureInfo.InvariantCulture) : 15;
        int latencyMs = args.Length > 2 ? int.Parse(args[2], CultureInfo.InvariantCulture) : 0;
        bool cloud = args.Length > 3 && args[3] == "cloud";
        bool bloom = args.Length > 4 && args[4] == "bloom";
        string dir = Path.Combine(Path.GetTempPath(), "ew-dictpruning-ab-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);

        try
        {
            var batch = Build();
            // The source under test present in one file and absent from the other.
            string dictionaryFile = Path.Combine(dir, "with-source.parquet");
            string plainFile = Path.Combine(dir, "without-source.parquet");
            await WriteAsync(batch, dictionaryFile, dictionary: true, bloomFilter: bloom);
            await WriteAsync(batch, plainFile, dictionary: bloom, bloomFilter: false);

            Console.WriteLine(
                $"{RowGroups} row groups x {RowsPerGroup:N0} rows, key + 2 plain payload columns; " +
                $"median of {rounds} alternating rounds after 3 warm-up rounds; {latencyMs} ms added per " +
                (cloud ? "GET, through CoalescingFileReader" : "request") + "; measuring " +
                (bloom ? "Bloom filters (dictionaries off both sides)." : "dictionaries."));
            Console.WriteLine($"Files: with the source {new FileInfo(dictionaryFile).Length:N0} bytes, without {new FileInfo(plainFile).Length:N0} bytes.");
            Console.WriteLine("| Case | ms off | ms on | Time | Calls off | on | GETs off | on | Bytes off | on | Row groups read off | on |");
            Console.WriteLine("|---|---|---|---|---|---|---|---|---|---|---|---|");

            var cases = new[]
            {
                ("in every group", dictionaryFile, "common"),
                ("in one group", dictionaryFile, Key(7, 3)),
                (bloom ? "no Bloom filter, in one group" : "plain key, in one group", plainFile, Key(7, 3)),
            };

            // Unmeasured passes over every case first. Without them the first case measured read about
            // twice as slow as the same read later in the run, whichever case came first: the decode
            // loops were still on tier-0 code (DOTNET_TieredCompilation=0 made it vanish). One pass was
            // not enough.
            for (int pass = 0; pass < 10; pass++)
            {
                foreach (var (_, path, key) in cases)
                {
                    var filter = Ex.Equal("key", key);
                    await ReadAsync(path, Options(filter, bloom, on: false), latencyMs: 0, cloud);
                    await ReadAsync(path, Options(filter, bloom, on: true), latencyMs: 0, cloud);
                }
            }

            foreach (var (label, path, key) in cases)
            {
                var filter = Ex.Equal("key", key);
                var off = Options(filter, bloom, on: false);
                var on = Options(filter, bloom, on: true);

                var msOff = new List<double>();
                var msOn = new List<double>();
                var ratios = new List<double>();
                Result a = default, b = default;
                for (int round = -3; round < rounds; round++)
                {
                    bool offFirst = round % 2 == 0;
                    if (offFirst)
                        a = await ReadAsync(path, off, latencyMs, cloud);
                    b = await ReadAsync(path, on, latencyMs, cloud);
                    if (!offFirst)
                        a = await ReadAsync(path, off, latencyMs, cloud);

                    if (round < 0)
                        continue;
                    msOff.Add(a.Ms);
                    msOn.Add(b.Ms);
                    ratios.Add(b.Ms / a.Ms);
                }

                Console.WriteLine(
                    $"| {label} | {Median(msOff):F1} | {Median(msOn):F1} | {(Median(ratios) - 1) * 100:+0;-0;0}% " +
                    $"| {a.Calls} | {b.Calls} | {a.Gets} | {b.Gets} | {a.Bytes:N0} | {b.Bytes:N0} | {a.RowGroups} | {b.RowGroups} |");
            }
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }

        return 0;
    }

    private static string Key(int group, int i) => $"k{group:D2}-{i:D2}";

    /// <summary>
    /// Group g holds its own 64 keys, "common", and the file-wide extremes "a-min" and "z-max"; two
    /// payload columns of random longs and doubles make each group's data read a real cost.
    /// </summary>
    private static RecordBatch Build()
    {
        var random = new Random(57);
        var keys = new StringArray.Builder();
        var longs = new Int64Array.Builder();
        var doubles = new DoubleArray.Builder();
        for (int g = 0; g < RowGroups; g++)
        {
            for (int r = 0; r < RowsPerGroup; r++)
            {
                keys.Append((r % (KeysPerGroup + 3)) switch
                {
                    KeysPerGroup => "common",
                    KeysPerGroup + 1 => "a-min",
                    KeysPerGroup + 2 => "z-max",
                    var i => Key(g, i),
                });
                longs.Append(random.NextInt64());
                doubles.Append(random.NextDouble());
            }
        }

        var schema = new Schema.Builder()
            .Field(new Field("key", StringType.Default, false))
            .Field(new Field("l", Int64Type.Default, false))
            .Field(new Field("d", DoubleType.Default, false))
            .Build();
        return new RecordBatch(schema, [keys.Build(), longs.Build(), doubles.Build()], RowGroups * RowsPerGroup);
    }

    /// <summary>
    /// The read options for one side. Every option is explicit: dictionary pruning defaults to on, so
    /// leaving it out would measure on against on.
    /// </summary>
    private static ParquetReadOptions Options(EngineeredWood.Expressions.Predicate filter, bool bloom, bool on) =>
        bloom
            ? new ParquetReadOptions { Filter = filter, FilterUseDictionaries = false, FilterUseBloomFilters = on }
            : new ParquetReadOptions { Filter = filter, FilterUseDictionaries = on, FilterUseBloomFilters = false };

    private static async Task WriteAsync(RecordBatch batch, string path, bool dictionary, bool bloomFilter)
    {
        var options = new ParquetWriteOptions
        {
            RowGroupMaxRows = RowsPerGroup,
            // The payload columns never dictionary-encode (random values); only the key's setting varies.
            ColumnDictionaryEnabled = new Dictionary<string, bool> { ["key"] = dictionary },
            BloomFilterColumns = bloomFilter ? new HashSet<string> { "key" } : null,
        };
        await using var file = new LocalSequentialFile(path);
        await using var writer = new ParquetFileWriter(file, ownsFile: false, options);
        await writer.WriteRowGroupAsync(batch);
        await writer.CloseAsync();
    }

    private readonly record struct Result(double Ms, int Calls, int Gets, long Bytes, int RowGroups);

    private static async Task<Result> ReadAsync(string path, ParquetReadOptions options, int latencyMs, bool cloud)
    {
        // Direct: every call is one request and pays the delay. Cloud: calls go through the coalescer,
        // and each GET it sends to the transport pays the delay.
        await using var transport = new LatencyCountingFile(new LocalRandomAccessFile(path), latencyMs);
        await using var calls = cloud ? new LatencyCountingFile(new CoalescingFileReader(transport), 0) : null;
        IRandomAccessFile file = calls ?? transport;
        var clock = Stopwatch.StartNew();
        int batches = 0;
        using (var reader = new ParquetFileReader(file, ownsFile: false, options))
        {
            await foreach (var batch in reader.ReadAllAsync())
            {
                batches++; // one batch per row group: no BatchSize is set
                batch.Dispose();
            }
        }

        return new Result(clock.Elapsed.TotalMilliseconds, (calls ?? transport).Requests, transport.Requests,
            transport.Bytes, batches);
    }

    private static double Median(List<double> values)
    {
        var sorted = values.OrderBy(v => v).ToList();
        return sorted[sorted.Count / 2];
    }
}
#endif
