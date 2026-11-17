// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Diagnostics;
using Xunit;

namespace EngineeredWood.Tests;

/// <summary>
/// Asserts that an operation is fast, in a way a busy machine cannot fail.
/// </summary>
/// <remarks>
/// <para>
/// For the tests that guard against a superlinear cost on a huge input: a million-digit parse, a
/// literal with a scale of 50 million. The regressions they exist to catch take seconds to hours;
/// a healthy run takes a few milliseconds once warm. The budget is therefore generous, and what
/// made these tests flaky was never the code under test but two things the old single timed run
/// also counted:
/// </para>
/// <list type="bullet">
/// <item>JIT and first-touch costs. Measured on .NET 10 and .NET Framework, the first case of
/// each of these tests took 80-350 ms and every later case 1-5 ms on the same input.</item>
/// <item>A machine shared with other work. CI runs the test assemblies in parallel, and a
/// 260 ms first run was measured there at 1035 ms, against a 1000 ms budget.</item>
/// </list>
/// <para>
/// So the operation runs once untimed, then <see cref="Attempts"/> times, and the fastest attempt
/// is judged. Contention only ever adds time, so the minimum is the honest estimate of the code's
/// own cost, and one stalled attempt no longer fails the test. A regression of the kind these
/// tests catch is slow on every attempt.
/// </para>
/// <para>
/// The operation must be safe to repeat: pure, or doing its own setup. Assertions inside it run
/// on every attempt, which is harmless.
/// </para>
/// </remarks>
internal static class Timing
{
    /// <summary>How many timed runs follow the untimed warm-up.</summary>
    public const int Attempts = 3;

    /// <summary>The fastest of <see cref="Attempts"/> timed runs, after one untimed run.</summary>
    public static TimeSpan Fastest(Action operation)
    {
        operation();

        var fastest = TimeSpan.MaxValue;
        for (var attempt = 0; attempt < Attempts; attempt++)
        {
            var watch = Stopwatch.StartNew();
            operation();
            watch.Stop();

            if (watch.Elapsed < fastest)
                fastest = watch.Elapsed;
        }

        return fastest;
    }

    /// <summary>Fails unless the fastest timed run of <paramref name="operation"/> beats <paramref name="budget"/>.</summary>
    public static void AssertFasterThan(TimeSpan budget, Action operation)
    {
        var fastest = Fastest(operation);
        Assert.True(
            fastest < budget,
            $"took {fastest.TotalMilliseconds:F0} ms at best of {Attempts} warm runs, against {budget.TotalMilliseconds:F0} ms");
    }

    /// <summary>The usual budget: under a second, at best of several warm runs.</summary>
    public static void AssertQuick(Action operation) => AssertFasterThan(TimeSpan.FromSeconds(1), operation);
}
