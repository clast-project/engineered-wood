// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System;
using System.Diagnostics;
using System.IO;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Threading;

namespace EngineeredWood.Tests.Diagnostics
{

/// <summary>
/// <para>MEASUREMENT ONLY (see the draft PR this ships in). When <c>EW_THREADPOOL_SAMPLE_DIR</c> is set, every
/// test assembly starts a sampler as it loads, writing one CSV row every 250 ms to
/// <c>&lt;dir&gt;/&lt;assembly&gt;_&lt;framework&gt;_&lt;pid&gt;.csv</c>. Unset, it does nothing.</para>
///
/// <para>The question: do net472 test runs stall on CI because the .NET Framework thread pool injects threads
/// too slowly (about one per 500 ms once past its minimum) while the 4-vCPU runner is busy? The direct signal
/// is <b>probe latency</b>: each sample queues a no-op work item and records how long it waits to run. A
/// healthy pool runs it within milliseconds. A starved one leaves it queued for seconds while
/// <c>busy_workers</c> sits at <c>min_workers</c> and creeps up.</para>
///
/// <para>The sampler runs on its own dedicated thread, never the pool, so a starved pool cannot delay the
/// measurement of its own starvation.</para>
/// </summary>
internal static class ThreadPoolSampler
{
    private const string DirEnvVar = "EW_THREADPOOL_SAMPLE_DIR";
    private const int IntervalMs = 250;

    private static long _probeQueuedTicks;   // Stopwatch ticks when the outstanding probe was queued; 0 = none
    private static long _lastLatencyTicks = -1;

    // CS0436: on net472, test assemblies that are friends of EngineeredWood.Core also see Core's own internal
    // polyfill of this attribute; the local one below covers the assemblies that are not. Either works.
#pragma warning disable CS0436
    [ModuleInitializer]
#pragma warning restore CS0436
    internal static void Start()
    {
        string? dir = Environment.GetEnvironmentVariable(DirEnvVar);
        if (string.IsNullOrEmpty(dir))
            return;

        try
        {
            Directory.CreateDirectory(dir);
            string assembly = typeof(ThreadPoolSampler).Assembly.GetName().Name ?? "unknown";
            string framework = Framework();
            int pid = Process.GetCurrentProcess().Id;
            string path = Path.Combine(dir, $"{assembly}_{framework}_{pid}.csv");
            var thread = new Thread(() => Run(path)) { IsBackground = true, Name = "ThreadPoolSampler" };
            thread.Start();
        }
        catch
        {
            // Measurement must never break a test run.
        }
    }

    private static string Framework()
    {
#if NETFRAMEWORK
        return "net472";
#else
        return "net" + Environment.Version.Major + "." + Environment.Version.Minor;
#endif
    }

    private static void Run(string path)
    {
        try
        {
            using var writer = new StreamWriter(path, append: false) { AutoFlush = true };
            writer.WriteLine(
                "utc,elapsed_ms,busy_workers,min_workers,max_workers,busy_io,pool_threads,pending_items,"
                + "probe_latency_ms,probe_outstanding_ms,process_threads,cpu_ms,working_set_mb,managed_mb,gen0,gen1,gen2");

            var clock = Stopwatch.StartNew();
            var process = Process.GetCurrentProcess();
            while (true)
            {
                writer.WriteLine(Sample(clock, process));
                Thread.Sleep(IntervalMs);
            }
        }
        catch
        {
            // Measurement must never break a test run.
        }
    }

    private static string Sample(Stopwatch clock, Process process)
    {
        ThreadPool.GetAvailableThreads(out int availWorkers, out int availIo);
        ThreadPool.GetMinThreads(out int minWorkers, out _);
        ThreadPool.GetMaxThreads(out int maxWorkers, out int maxIo);

        // Queue a probe unless the previous one is still waiting; report the outstanding one's age either way.
        long now = Stopwatch.GetTimestamp();
        long queued = Interlocked.Read(ref _probeQueuedTicks);
        double outstandingMs = 0;
        if (queued == 0)
        {
            Interlocked.Exchange(ref _probeQueuedTicks, now);
            ThreadPool.UnsafeQueueUserWorkItem(_ =>
            {
                long start = Interlocked.Exchange(ref _probeQueuedTicks, 0);
                Interlocked.Exchange(ref _lastLatencyTicks, Stopwatch.GetTimestamp() - start);
            }, null);
        }
        else
        {
            outstandingMs = (now - queued) * 1000.0 / Stopwatch.Frequency;
        }

        long latencyTicks = Interlocked.Read(ref _lastLatencyTicks);
        string latency = latencyTicks < 0 ? "" : F(latencyTicks * 1000.0 / Stopwatch.Frequency);

        process.Refresh();
#if NETFRAMEWORK
        string poolThreads = "", pending = "";
#else
        string poolThreads = ThreadPool.ThreadCount.ToString(CultureInfo.InvariantCulture);
        string pending = ThreadPool.PendingWorkItemCount.ToString(CultureInfo.InvariantCulture);
#endif
        return string.Join(",",
            DateTime.UtcNow.ToString("HH:mm:ss.fff", CultureInfo.InvariantCulture),
            clock.ElapsedMilliseconds.ToString(CultureInfo.InvariantCulture),
            (maxWorkers - availWorkers).ToString(CultureInfo.InvariantCulture),
            minWorkers.ToString(CultureInfo.InvariantCulture),
            maxWorkers.ToString(CultureInfo.InvariantCulture),
            (maxIo - availIo).ToString(CultureInfo.InvariantCulture),
            poolThreads,
            pending,
            latency,
            F(outstandingMs),
            process.Threads.Count.ToString(CultureInfo.InvariantCulture),
            ((long)process.TotalProcessorTime.TotalMilliseconds).ToString(CultureInfo.InvariantCulture),
            (process.WorkingSet64 >> 20).ToString(CultureInfo.InvariantCulture),
            (GC.GetTotalMemory(false) >> 20).ToString(CultureInfo.InvariantCulture),
            GC.CollectionCount(0).ToString(CultureInfo.InvariantCulture),
            GC.CollectionCount(1).ToString(CultureInfo.InvariantCulture),
            GC.CollectionCount(2).ToString(CultureInfo.InvariantCulture));
    }

    private static string F(double d) => d.ToString("0.0", CultureInfo.InvariantCulture);
}
}

#if NETFRAMEWORK
namespace System.Runtime.CompilerServices
{
    /// <summary>Polyfill: the compiler honours this attribute by name on frameworks that lack it.</summary>
    [AttributeUsage(AttributeTargets.Method, Inherited = false)]
    internal sealed class ModuleInitializerAttribute : Attribute
    {
    }
}
#endif
