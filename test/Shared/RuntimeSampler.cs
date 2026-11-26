// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System;
using System.Collections;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Runtime;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;

namespace EngineeredWood.Tests.Diagnostics
{
    /// <summary>
    /// <para>MEASUREMENT ONLY (see the draft PR this ships in). When <c>EW_RUNTIME_SAMPLE_DIR</c> is set, every
    /// test assembly starts a sampler as it loads. Unset, it does nothing.</para>
    ///
    /// <para>The question: why does a test process on .NET 11 get a small fraction of the CPU its .NET 10 twin
    /// gets on the same CI runner at the same moment (#964: 0.1-0.5 cores against about 1.5), stretching
    /// DeltaLake.Tests from about 30 s to about 200 s with the same CPU time and the same GC counts? Two
    /// candidates:</para>
    /// <list type="bullet">
    /// <item>The OS deprioritizes the process: priority class, power throttling (EcoQoS), affinity.
    /// <c>&lt;file&gt;.info.txt</c> records those, and the runtime's GC configuration, once at start.</item>
    /// <item>The runtime stops it: long GC suspensions, which halt every managed thread, the sampler's own
    /// included. <c>gc_pause_ms</c> (cumulative, .NET 7+) measures that directly.</item>
    /// </list>
    /// <para><c>sampler_lag_ms</c>, how late the sampler's dedicated thread woke from its 250 ms sleep, shows a
    /// whole-process stall whatever its cause. Set against the growth of <c>gc_pause_ms</c> over the same
    /// interval, it tells the two candidates apart.</para>
    /// </summary>
    internal static class RuntimeSampler
    {
        private const string DirEnvVar = "EW_RUNTIME_SAMPLE_DIR";
        private const int IntervalMs = 250;

        private static long _probeQueuedTicks;
        private static long _lastLatencyTicks = -1;

        // CS0436: on net472, test assemblies that are friends of EngineeredWood.Core also see Core's own
        // internal polyfill of this attribute; the local one below covers the others. Either works.
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
                string assembly = typeof(RuntimeSampler).Assembly.GetName().Name ?? "unknown";
                int pid = Process.GetCurrentProcess().Id;
                string path = Path.Combine(dir, $"{assembly}_{Framework()}_{pid}.csv");
                File.WriteAllText(path + ".info.txt", Info());
                var thread = new Thread(() => Run(path)) { IsBackground = true, Name = "RuntimeSampler" };
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

        [StructLayout(LayoutKind.Sequential)]
        private struct PowerThrottlingState
        {
            public uint Version;
            public uint ControlMask;
            public uint StateMask;
        }

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern bool GetProcessInformation(IntPtr process, int infoClass, ref PowerThrottlingState info, int size);

        [DllImport("kernel32.dll")]
        private static extern uint GetPriorityClass(IntPtr process);

        private static string Info()
        {
            var sb = new StringBuilder();
            var p = Process.GetCurrentProcess();
            sb.AppendLine("runtime: " + RuntimeInformation.FrameworkDescription);
            sb.AppendLine("process: " + p.MainModule?.FileName);
            try { sb.AppendLine($"priority class: {p.PriorityClass} (raw 0x{GetPriorityClass(p.Handle):X}), base priority {p.BasePriority}"); }
            catch (Exception e) { sb.AppendLine("priority class: " + e.Message); }
            try
            {
                var s = new PowerThrottlingState { Version = 1 };
                bool ok = GetProcessInformation(p.Handle, 4, ref s, Marshal.SizeOf(typeof(PowerThrottlingState)));
                sb.AppendLine($"power throttling: ok={ok} err={Marshal.GetLastWin32Error()} control=0x{s.ControlMask:X} state=0x{s.StateMask:X}");
            }
            catch (Exception e) { sb.AppendLine("power throttling: " + e.Message); }
            try { sb.AppendLine($"affinity: 0x{(long)p.ProcessorAffinity:X}"); } catch (Exception e) { sb.AppendLine("affinity: " + e.Message); }
            sb.AppendLine("ProcessorCount: " + Environment.ProcessorCount);
            sb.AppendLine($"GC: server={GCSettings.IsServerGC} latency={GCSettings.LatencyMode}");
#if !NETFRAMEWORK
            var info = GC.GetGCMemoryInfo();
            sb.AppendLine($"GC memory: total available {info.TotalAvailableMemoryBytes >> 20} MB, high-load threshold {info.HighMemoryLoadThresholdBytes >> 20} MB");
            foreach (string key in new[] { "System.GC.Concurrent", "System.GC.Server", "System.GC.RetainVM", "System.GC.ConserveMemory",
                         "System.GC.DynamicAdaptationMode", "System.GC.HeapCount", "System.GC.HeapHardLimit", "System.Threading.ThreadPool.MinThreads" })
                sb.AppendLine($"AppContext {key} = {AppContext.GetData(key) ?? "(unset)"}");
#endif
            foreach (DictionaryEntry e in Environment.GetEnvironmentVariables())
            {
                string k = (string)e.Key;
                if (k.StartsWith("DOTNET_", StringComparison.OrdinalIgnoreCase) || k.StartsWith("COMPlus_", StringComparison.OrdinalIgnoreCase))
                    sb.AppendLine($"env {k} = {e.Value}");
            }

            return sb.ToString();
        }

        private static void Run(string path)
        {
            try
            {
                using var writer = new StreamWriter(path, append: false) { AutoFlush = true };
                writer.WriteLine(
                    "utc,elapsed_ms,sampler_lag_ms,cpu_ms,gc_pause_ms,alloc_mb,managed_mb,working_set_mb,gen0,gen1,gen2,"
                    + "busy_workers,min_workers,probe_latency_ms,probe_outstanding_ms,process_threads");

                var clock = Stopwatch.StartNew();
                var process = Process.GetCurrentProcess();
                long last = 0;
                while (true)
                {
                    long now = clock.ElapsedMilliseconds;
                    long lag = last == 0 ? 0 : Math.Max(0, now - last - IntervalMs);
                    last = now;
                    writer.WriteLine(Sample(clock, process, lag));
                    Thread.Sleep(IntervalMs);
                }
            }
            catch
            {
                // Measurement must never break a test run.
            }
        }

        private static string Sample(Stopwatch clock, Process process, long lag)
        {
            ThreadPool.GetAvailableThreads(out int availWorkers, out _);
            ThreadPool.GetMinThreads(out int minWorkers, out _);
            ThreadPool.GetMaxThreads(out int maxWorkers, out _);

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
            string gcPause = "", alloc = "";
#else
            string gcPause = F(GC.GetTotalPauseDuration().TotalMilliseconds);
            string alloc = (GC.GetTotalAllocatedBytes(false) >> 20).ToString(CultureInfo.InvariantCulture);
#endif
            return string.Join(",",
                DateTime.UtcNow.ToString("HH:mm:ss.fff", CultureInfo.InvariantCulture),
                clock.ElapsedMilliseconds.ToString(CultureInfo.InvariantCulture),
                lag.ToString(CultureInfo.InvariantCulture),
                ((long)process.TotalProcessorTime.TotalMilliseconds).ToString(CultureInfo.InvariantCulture),
                gcPause,
                alloc,
                (GC.GetTotalMemory(false) >> 20).ToString(CultureInfo.InvariantCulture),
                (process.WorkingSet64 >> 20).ToString(CultureInfo.InvariantCulture),
                GC.CollectionCount(0).ToString(CultureInfo.InvariantCulture),
                GC.CollectionCount(1).ToString(CultureInfo.InvariantCulture),
                GC.CollectionCount(2).ToString(CultureInfo.InvariantCulture),
                (maxWorkers - availWorkers).ToString(CultureInfo.InvariantCulture),
                minWorkers.ToString(CultureInfo.InvariantCulture),
                latency,
                F(outstandingMs),
                process.Threads.Count.ToString(CultureInfo.InvariantCulture));
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
