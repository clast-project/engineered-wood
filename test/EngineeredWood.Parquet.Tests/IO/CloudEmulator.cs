// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Tests.IO;

/// <summary>
/// <para>The availability gate for the cloud-backend integration tests. Each of the three suites
/// probes its emulator in <c>InitializeAsync</c> and then calls <see cref="Require"/> as the first
/// statement of every test, with the test marked <c>[SkippableFact]</c>.</para>
///
/// <para><b>Why this exists.</b> These tests used to gate with <c>if (!_available) return;</c>, and an
/// early return is a PASS. CI starts no emulators and runs on windows-latest, where a <c>services:</c>
/// block is not even available — so 36 tests covering <c>AzureTableFileSystem</c>,
/// <c>S3TableFileSystem</c> and <c>GcsTableFileSystem</c> reported green on every run having never
/// opened a socket. That is not a small reporting nit: <c>ITableFileSystem</c> is what
/// <c>EngineeredWood.DeltaLake</c>, <c>EngineeredWood.Iceberg</c> and <c>EngineeredWood.Lance.Table</c>
/// all commit through, so three formats were trusting backends with zero executed coverage. See
/// issue #79.</para>
///
/// <para><b>The skip is half the mechanism.</b> It makes a run state what it did, but an honest green
/// is still a green — CI can skip all 36 and pass. <see cref="RequireEnvVar"/> is the other half: set
/// it in any job that starts emulators and absence becomes a failure naming the emulator and the
/// reason the probe gave. This mirrors <c>EW_REQUIRE_*</c> on the Delta interop tiers, deliberately:
/// the skip answers "what did this run do", the variable answers "was this job allowed to tolerate
/// that".</para>
/// </summary>
internal static class CloudEmulator
{
    /// <summary>Set to <c>1</c> in a job that starts the emulators, so an unreachable one fails
    /// rather than quietly skipping the backend it covers.</summary>
    public const string RequireEnvVar = "EW_REQUIRE_CLOUD_EMULATORS";

    /// <summary>
    /// Skips the calling test when <paramref name="available"/> is false — or throws, when
    /// <see cref="RequireEnvVar"/> says this job depended on the emulator being there.
    /// </summary>
    /// <param name="emulator">What to start, phrased so the message is actionable on its own
    /// (e.g. "Azurite on 127.0.0.1:10000").</param>
    /// <param name="available">The suite's probe result.</param>
    /// <param name="reason">What the probe actually failed with. Worth capturing rather than
    /// reporting a bare "unavailable": the failure is as often a rejected API version or a bad
    /// credential as it is nothing listening, and those need different fixes.</param>
    public static void Require(string emulator, bool available, string? reason)
    {
        if (available)
            return;

        string detail = string.IsNullOrWhiteSpace(reason) ? "(no diagnostic captured)" : reason!;

        if (Environment.GetEnvironmentVariable(RequireEnvVar) == "1")
        {
            throw new InvalidOperationException(
                $"{RequireEnvVar}=1 but {emulator} is unreachable: {detail}");
        }

        // Always skips here; Skip exposes only the two conditional forms, so the condition is
        // restated rather than an unconditional Skip.Always being called.
        Skip.IfNot(available, $"{emulator} is not running: {detail}");
    }
}
