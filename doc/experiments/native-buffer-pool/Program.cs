// Native buffer pool A/B under a Windows job-object commit limit.
//   poolbench gen <dir>                      write fixtures (ParquetSharp, uncompressed, 1 row group)
//   poolbench launch <limitMB> <dir> [reps]  put this process in a job with a per-process commit limit,
//                                            then run every (fixture, mode, workload) in its own child
//   poolbench run <fixture> <mode> <workload> one measurement (child)
using System.Diagnostics;
using System.Runtime.InteropServices;
using Apache.Arrow;
using EngineeredWood.Arrow;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using PS = ParquetSharp;



switch (args[0])
{
    case "gen": Gen(args[1], args.Length > 2 ? int.Parse(args[2]) : 1 << 20); break;
    case "launch": Launch(long.Parse(args[1]), args[2], args.Length > 3 ? int.Parse(args[3]) : 1); break;
    case "run": await Run(args[1], args[2], args[3]); break;
    default: throw new ArgumentException(args[0]);
}

static void Gen(string dir, int Rows)
{
    Directory.CreateDirectory(dir);
    var rng = new Random(42);

    void Write<T>(string name, T[] values, bool dictionary)
    {
        var props = new PS.WriterPropertiesBuilder().Compression(PS.Compression.Uncompressed);
        props = dictionary ? props.EnableDictionary() : props.DisableDictionary();
        using var writer = new PS.ParquetFileWriter(Path.Combine(dir, name + ".parquet"),
            new PS.Column[] { new PS.Column<T>("v") }, props.Build());
        using var rg = writer.AppendRowGroup();
        using (var col = rg.NextColumn().LogicalWriter<T>())
            col.WriteBatch(values);
        writer.Close();
    }

    Write("int32_w11", Enumerable.Range(0, Rows).Select(_ => rng.Next(2000)).ToArray(), dictionary: true);
    Write("int32_plain", Enumerable.Range(0, Rows).Select(_ => rng.Next()).ToArray(), dictionary: false);
    Write("double_plain", Enumerable.Range(0, Rows).Select(_ => rng.NextDouble()).ToArray(), dictionary: false);
    Write("int64_opt", Enumerable.Range(0, Rows).Select(_ => rng.Next(10) == 0 ? (long?)null : rng.NextInt64()).ToArray(), dictionary: false);
    Write("string_plain", Enumerable.Range(0, Rows).Select(_ =>
    {
        int len = 8 + rng.Next(17);
        return string.Create(len, rng, (span, r) => { for (int i = 0; i < span.Length; i++) span[i] = (char)('a' + r.Next(26)); });
    }).ToArray(), dictionary: false);
    Console.WriteLine($"fixtures written to {dir}");
}

static void Launch(long limitMb, string dir, int reps)
{
    Job.ConfineSelf(limitMb << 20);
    string self = Environment.ProcessPath!;
    string[] modes = ["off", "classic", "commit"];
    string[] workloads = ["steady", "window8"];
    var fixtures = Directory.GetFiles(dir, "*.parquet").OrderBy(f => f).ToArray();
    Console.WriteLine($"job commit limit {limitMb} MiB per process; {reps} rep(s)");
    for (int rep = 0; rep < reps; rep++)
    foreach (var workload in workloads)
    foreach (var fixture in fixtures)
    foreach (var mode in modes)
    {
        var psi = new ProcessStartInfo(self) { RedirectStandardOutput = true, RedirectStandardError = true };
        foreach (var a in new[] { "run", fixture, mode, workload })
            psi.ArgumentList.Add(a);
        using var p = Process.Start(psi)!;
        string stdout = p.StandardOutput.ReadToEnd();
        string stderr = p.StandardError.ReadToEnd();
        p.WaitForExit();
        var line = stdout.Split('\n').FirstOrDefault(l => l.StartsWith("RESULT"));
        Console.WriteLine(line?.TrimEnd() ?? $"FAILED {Path.GetFileNameWithoutExtension(fixture)} {mode} {workload} exit={p.ExitCode} {stderr.Split('\n').FirstOrDefault(l => l.Contains("Exception"))?.Trim()}");
    }
}

static async Task Run(string fixture, string modeName, string workload)
{
    NativeBufferPool.Mode = modeName switch
    {
        "off" => NativePoolMode.Off,
        "classic" => NativePoolMode.Classic,
        "commit" => NativePoolMode.Commit,
        _ => throw new ArgumentException(modeName),
    };
    int window = workload switch { "steady" => 1, "window8" => 8, _ => throw new ArgumentException(workload) };

    using var reader = new ParquetFileReader(new LocalRandomAccessFile(fixture));
    var live = new Queue<RecordBatch>();
    ulong checksum = 0;

    async Task<double> ReadOnce()
    {
        long t0 = Stopwatch.GetTimestamp();
        var batch = await reader.ReadRowGroupAsync(0);
        double ms = Stopwatch.GetElapsedTime(t0).TotalMilliseconds;
        live.Enqueue(batch);
        if (live.Count > window)
            live.Dequeue().Dispose();
        return ms;
    }

    // Checksum the first read: every mode must return the same bytes.
    {
        var first = await reader.ReadRowGroupAsync(0);
        checksum = Checksum(first.Column(0).Data);
        first.Dispose();
    }

    // Warm for 3 s so tier-1 code is what we time.
    var warm = Stopwatch.StartNew();
    while (warm.Elapsed < TimeSpan.FromSeconds(3))
        await ReadOnce();

    var times = new List<double>();
    var sw = Stopwatch.StartNew();
    while (sw.Elapsed < TimeSpan.FromSeconds(3) || times.Count < 40)
        times.Add(await ReadOnce());

    using var proc = Process.GetCurrentProcess();
    proc.Refresh();
    long endCommit = proc.PagedMemorySize64;
    long peakCommit = proc.PeakPagedMemorySize64;
    while (live.Count > 0)
        live.Dequeue().Dispose();

    times.Sort();
    double median = times[times.Count / 2];
    double p90 = times[(int)(times.Count * 0.9)];
    Console.WriteLine(string.Join(' ',
        "RESULT", Path.GetFileNameWithoutExtension(fixture), modeName, workload,
        $"median_ms={median:F3}", $"p90_ms={p90:F3}", $"n={times.Count}",
        $"peak_commit_mb={peakCommit / 1048576.0:F1}", $"end_commit_mb={endCommit / 1048576.0:F1}",
        $"hits={NativeBufferPool.Hits}", $"misses={NativeBufferPool.Misses}", $"trims={NativeBufferPool.PressureTrims}",
        $"cap_mb={NativeBufferPool.MaxRetainedBytes >> 20}", $"job_limit_mb={NativeBufferPool.CommitLimitBytes >> 20}",
        $"gc_limit_mb={GC.GetGCMemoryInfo().TotalAvailableMemoryBytes >> 20}", $"checksum={checksum:x16}"));
}

static ulong Checksum(ArrayData data)
{
    ulong h = 1469598103934665603;
    foreach (var buffer in data.Buffers)
        foreach (byte b in buffer.Span.Slice(0, Math.Min(buffer.Length, 1 << 24)))
            h = (h ^ b) * 1099511628211;
    return h ^ (ulong)data.Length ^ ((ulong)data.NullCount << 40);
}

static class Job
{
    [DllImport("kernel32.dll", SetLastError = true)] static extern nint CreateJobObjectW(nint attrs, nint name);
    [DllImport("kernel32.dll", SetLastError = true)] static extern unsafe int SetInformationJobObject(nint job, int cls, void* info, uint len);
    [DllImport("kernel32.dll", SetLastError = true)] static extern int AssignProcessToJobObject(nint job, nint process);
    [DllImport("kernel32.dll")] static extern nint GetCurrentProcess();

    [StructLayout(LayoutKind.Sequential)]
    struct ExtendedLimitInformation
    {
        public long PerProcessUserTimeLimit, PerJobUserTimeLimit;
        public uint LimitFlags;
        public nuint MinimumWorkingSetSize, MaximumWorkingSetSize;
        public uint ActiveProcessLimit;
        public nuint Affinity;
        public uint PriorityClass, SchedulingClass;
        public ulong R1, R2, R3, R4, R5, R6;
        public nuint ProcessMemoryLimit, JobMemoryLimit, PeakProcessMemoryUsed, PeakJobMemoryUsed;
    }

    public static unsafe void ConfineSelf(long processLimit)
    {
        nint job = CreateJobObjectW(0, 0);
        if (job == 0) throw new InvalidOperationException($"CreateJobObject {Marshal.GetLastPInvokeError()}");
        var info = new ExtendedLimitInformation
        {
            LimitFlags = 0x100 /* PROCESS_MEMORY */ | 0x2000 /* KILL_ON_JOB_CLOSE */,
            ProcessMemoryLimit = (nuint)processLimit,
        };
        if (SetInformationJobObject(job, 9, &info, (uint)sizeof(ExtendedLimitInformation)) == 0)
            throw new InvalidOperationException($"SetInformationJobObject {Marshal.GetLastPInvokeError()}");
        if (AssignProcessToJobObject(job, GetCurrentProcess()) == 0)
            throw new InvalidOperationException($"AssignProcessToJobObject {Marshal.GetLastPInvokeError()}");
    }
}
