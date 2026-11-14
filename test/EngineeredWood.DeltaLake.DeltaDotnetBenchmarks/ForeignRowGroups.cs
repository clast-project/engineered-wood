// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Diagnostics;
using Apache.Arrow;
using DeltaLake.Table;
using EwDeltaTable = EngineeredWood.DeltaLake.Table.DeltaTable;
using EwOptions = EngineeredWood.DeltaLake.Table.DeltaTableOptions;

namespace DeltaDotnetComparison;

/// <summary>
/// #878 part 2: scan Delta tables whose one data file is a single ~1 GB row group written by pyarrow
/// (<c>gen_foreign_rowgroups.py</c>), under EW's default options, with no read budget (one read at a time), and
/// with a 256 MiB budget, and through delta-dotnet's DataFusion scan. Reports time, the largest batch (rows and
/// Arrow buffer bytes), and the process's peak private bytes, sampled every 10 ms.
///
/// <code>dotnet run -c Release -- --rg878 [root|-] [table,...] [config,...]</code>
///
/// <para>The tables are read from whichever store <c>EWDD_STORAGE</c> names (see <see cref="BenchStorage"/>). On
/// Azure, upload them first with <c>--rg878-upload &lt;local root&gt; [table,...]</c>; both modes then default to
/// <c>&lt;prefix&gt;/rg878</c>. <c>-</c> for the root means that default.</para>
/// </summary>
internal static class ForeignRowGroups
{
    public static async Task<int> RunAsync(string[] args)
    {
        string root = args.Length > 0 && args[0] != "-" ? args[0] : DefaultRoot;
        string[] tables = args.Length > 1 ? args[1].Split(',') : ["fat_str", "skew_str", "wide_num"];
        (string Name, EwOptions? Options)[] configs =
        [
            ("ew-default", new EwOptions()),
            ("ew-serial", new EwOptions { MaxConcurrentReads = 1 }),
            ("ew-256MiB", new EwOptions { MaxBufferedReadBytes = 256L << 20 }),
            ("dd-select*", null),
        ];
        if (args.Length > 2)
            configs = configs.Where(c => args[2].Split(',').Contains(c.Name)).ToArray();

        Console.WriteLine("table\tconfig\tseconds\trows\tbatches\tmaxBatchRows\tmaxBatchMiB\tpeakPrivateMiB\terror");
        int failures = 0;
        foreach (string name in tables)
        {
            string path = BenchStorage.Current.Combine(root, name);
            foreach (var (config, options) in configs)
            {
                GC.Collect();
                GC.WaitForPendingFinalizers();
                GC.Collect();
                using var sampler = new PeakSampler();
                var stats = new ScanStats();
                var watch = Stopwatch.StartNew();
                string error = "";
                try
                {
                    if (options is null)
                    {
                        using var table = await Dd.OpenAsync(path, engine: Dd.TunedEngine);
                        await foreach (var batch in table.QueryAsync(new SelectQuery("SELECT * FROM deltatable"), CancellationToken.None))
                            stats.Add(batch);
                    }
                    else
                    {
                        await using var table = await EwDeltaTable.OpenAsync(Ew.Fs(path), options);
                        await foreach (var batch in table.ReadAllAsync())
                            stats.Add(batch);
                    }
                }
                catch (Exception e)
                {
                    error = $"{e.GetType().Name}: {e.Message.Split('\n')[0]}";
                    if (Environment.GetEnvironmentVariable("EWDD_RG878_STACK") == "1")
                        Console.Error.WriteLine(e);
                    failures++;
                }

                watch.Stop();
                Console.WriteLine(
                    $"{name}\t{config}\t{watch.Elapsed.TotalSeconds:F2}\t{stats.Rows}\t{stats.Batches}\t{stats.MaxRows}\t"
                    + $"{stats.MaxBytes / (1 << 20)}\t{sampler.PeakBytes / (1 << 20)}\t{error}");
            }
        }

        return failures == 0 ? 0 : 1;
    }

    /// <summary>Where the tables are: <c>%TEMP%/ew-rg878</c> on local disk, else <c>rg878</c> under the store's root.</summary>
    private static string DefaultRoot => BenchStorage.Current is LocalStorage
        ? Path.Combine(Path.GetTempPath(), "ew-rg878")
        : BenchStorage.Current.Combine(BenchStorage.Current.Root, "rg878");

    /// <summary>
    /// Copies the local tables under <c>args[0]</c> to <see cref="DefaultRoot"/> on the configured store, streamed
    /// through EW's file system (a data file can be over 2 GiB). The commit goes last, so a table whose commit is
    /// there is complete and is skipped.
    /// </summary>
    public static async Task<int> UploadAsync(string[] args)
    {
        if (BenchStorage.Current is LocalStorage)
        {
            Console.Error.WriteLine("--rg878-upload copies the local tables to a remote store: set EWDD_STORAGE=azure.");
            return 2;
        }
        string local = args.Length > 0 ? args[0] : Path.Combine(Path.GetTempPath(), "ew-rg878");
        string[] tables = args.Length > 1 ? args[1].Split(',') : ["fat_str", "skew_str", "wide_num", "huge_str"];
        const string commit = "_delta_log/00000000000000000000.json";
        var buffer = new byte[8 << 20];
        foreach (string name in tables)
        {
            string source = Path.Combine(local, name);
            var fs = BenchStorage.Current.FileSystem(BenchStorage.Current.Combine(DefaultRoot, name));
            if (await fs.ExistsAsync(commit))
            {
                Console.WriteLine($"{name}: already uploaded");
                continue;
            }

            var files = Directory.GetFiles(source, "*", SearchOption.AllDirectories)
                .Select(f => Path.GetRelativePath(source, f).Replace(Path.DirectorySeparatorChar, '/'))
                .OrderBy(f => f == commit ? 1 : 0)
                .ToList();
            var watch = Stopwatch.StartNew();
            long bytes = 0;
            foreach (string file in files)
            {
                await using var input = File.OpenRead(Path.Combine(source, file));
                await using var output = await fs.CreateAsync(file, overwrite: true);
                int read;
                while ((read = await input.ReadAsync(buffer)) > 0)
                {
                    await output.WriteAsync(buffer.AsMemory(0, read));
                    bytes += read;
                }
                // Publishes the file: disposing one that was never completed discards what was written.
                await output.CompleteAsync();
            }
            Console.WriteLine($"{name}: {bytes / (1 << 20)} MiB in {watch.Elapsed.TotalSeconds:F0} s");
        }

        return 0;
    }

    private sealed class ScanStats
    {
        public long Rows, Batches, MaxRows, MaxBytes;

        public void Add(RecordBatch batch)
        {
            Rows += batch.Length;
            Batches++;
            MaxRows = Math.Max(MaxRows, batch.Length);
            long bytes = 0;
            for (int c = 0; c < batch.ColumnCount; c++)
                bytes += Bytes(batch.Column(c).Data);
            MaxBytes = Math.Max(MaxBytes, bytes);
            batch.Dispose();
        }

        private static long Bytes(ArrayData data)
        {
            long bytes = 0;
            foreach (var buffer in data.Buffers ?? [])
                bytes += buffer.Length;
            foreach (var child in data.Children ?? [])
                bytes += child is null ? 0 : Bytes(child);
            return bytes;
        }
    }

    /// <summary>The process's private bytes, sampled on a background thread until disposed.</summary>
    private sealed class PeakSampler : IDisposable
    {
        private readonly CancellationTokenSource _stop = new();
        private readonly Thread _thread;
        private long _peak;

        public PeakSampler()
        {
            _thread = new Thread(() =>
            {
                using var process = Process.GetCurrentProcess();
                while (!_stop.IsCancellationRequested)
                {
                    process.Refresh();
                    _peak = Math.Max(_peak, process.PrivateMemorySize64);
                    Thread.Sleep(10);
                }
            }) { IsBackground = true };
            _thread.Start();
        }

        public long PeakBytes => Interlocked.Read(ref _peak);

        public void Dispose()
        {
            _stop.Cancel();
            _thread.Join();
        }
    }
}
