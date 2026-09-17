// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Diagnostics;
using System.Runtime.InteropServices;

namespace EngineeredWood.Vortex.Tests.TestHelpers;

/// <summary>
/// Locates this project's source tree and the binaries of the Rust crate at
/// <c>test/EngineeredWood.Vortex.Tests/Rust</c>, which link against the pinned upstream vortex
/// release and act as the reference implementation for cross-validation.
/// </summary>
internal static class RustTools
{
    /// <summary>
    /// The EngineeredWood.Vortex.Tests project directory, found by walking up from the test
    /// assembly (which lives under its <c>bin/</c>). Null if the tests run from a copy detached
    /// from the source tree.
    /// </summary>
    public static string? ProjectDirectory { get; } = FindProjectDirectory();

    private static string? FindProjectDirectory()
    {
        var dir = AppContext.BaseDirectory;
        for (int i = 0; i < 8 && dir is not null; i++)
        {
            if (File.Exists(Path.Combine(dir, "EngineeredWood.Vortex.Tests.csproj")))
                return dir;
            dir = Path.GetDirectoryName(dir.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar));
        }
        return null;
    }

    /// <summary>The path to <paramref name="name"/>'s release build, or null if it isn't built.</summary>
    public static string? Find(string name)
    {
        if (ProjectDirectory is null) return null;
        var exe = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? name + ".exe" : name;
        var path = Path.Combine(ProjectDirectory, "Rust", "target", "release", exe);
        return File.Exists(path) ? path : null;
    }

    /// <summary>
    /// The path to <paramref name="name"/>. Skips the calling test when it isn't built — or
    /// throws, when <paramref name="requireEnvVar"/> is <c>1</c> because the job built it on
    /// purpose. The caller must be a <c>[SkippableFact]</c> or <c>[SkippableTheory]</c>.
    /// </summary>
    public static string Require(string name, string requireEnvVar)
    {
        var path = Find(name);
        if (path is not null)
            return path;

        string build = $"cd test/EngineeredWood.Vortex.Tests/Rust && cargo build --release --bin {name}";
        if (Environment.GetEnvironmentVariable(requireEnvVar) == "1")
            throw new InvalidOperationException($"{requireEnvVar}=1 but {name} is not built ({build}).");

        Skip.If(true, $"{name} is not built ({build}).");
        return null!;
    }

    /// <summary>Runs a tool to completion, capturing both output streams.</summary>
    public static (int ExitCode, string Stdout, string Stderr) Run(string tool, params string[] args)
    {
        var psi = new ProcessStartInfo
        {
            FileName = tool,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
        };
#if NETCOREAPP2_1_OR_GREATER
        foreach (var arg in args) psi.ArgumentList.Add(arg);
#else
        psi.Arguments = string.Join(" ", args.Select(a => "\"" + a.Replace("\"", "\\\"") + "\""));
#endif
        using var p = Process.Start(psi)!;
        // Drain stderr concurrently: reading one stream to the end while the other fills its pipe
        // buffer deadlocks.
        var stderr = p.StandardError.ReadToEndAsync();
        var stdout = p.StandardOutput.ReadToEnd();
        p.WaitForExit();
        return (p.ExitCode, stdout, stderr.Result);
    }
}
