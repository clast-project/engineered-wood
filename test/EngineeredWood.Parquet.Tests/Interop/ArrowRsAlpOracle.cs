// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Diagnostics;
using System.Runtime.InteropServices;

namespace EngineeredWood.Tests.Parquet.Interop;

/// <summary>
/// Drives <c>parquet-alp-oracle</c>, the Rust binary at <c>test/EngineeredWood.Parquet.Tests/Rust</c>
/// that reads and writes ALP-encoded Parquet with arrow-rs.
/// </summary>
/// <remarks>
/// <para><b>Why a Rust binary and not <see cref="ExternalParquetReaders"/>.</b> arrow-rs 60.0.0 is
/// the only ALP implementation so far, and the newest datafusion Python release (55.0.0) is still
/// built on arrow-rs 59. This is a stopgap until a datafusion release on arrow-rs 60 or later lets
/// ALP join that tier (#550), after which this file and the crate go away.</para>
///
/// <para><b>Setup.</b> <c>cd test/EngineeredWood.Parquet.Tests/Rust &amp;&amp; cargo build --release
/// --locked</c>. Without the binary the tests skip; <c>EW_REQUIRE_PARQUET_ALP_ORACLE=1</c> (set by
/// the <c>parquet-alp-interop</c> CI job) turns that into a failure, so a green run proves the
/// tier ran.</para>
/// </remarks>
internal static class ArrowRsAlpOracle
{
    private const string Name = "parquet-alp-oracle";
    private const string RequireEnvVar = "EW_REQUIRE_PARQUET_ALP_ORACLE";

    /// <summary>The oracle's path. Skips the calling test when it isn't built, or throws when
    /// <c>EW_REQUIRE_PARQUET_ALP_ORACLE=1</c>. The caller must be skippable.</summary>
    public static string Require()
    {
        var path = Find();
        if (path is not null)
            return path;

        string build = "cd test/EngineeredWood.Parquet.Tests/Rust && cargo build --release --locked";
        if (Environment.GetEnvironmentVariable(RequireEnvVar) == "1")
            throw new InvalidOperationException($"{RequireEnvVar}=1 but {Name} is not built ({build}).");

        Skip.If(true, $"{Name} is not built ({build}).");
        return null!;
    }

    /// <summary>Writes the oracle's fixed ALP file (FLOAT <c>f</c>, DOUBLE <c>d</c>) to
    /// <paramref name="path"/>.</summary>
    public static void Write(string oracle, string path) => Run(oracle, "write", path);

    /// <summary>
    /// Reads <paramref name="path"/> with arrow-rs. Returns each column chunk's encodings, keyed by
    /// <c>(row group, column)</c>, and each column's values as IEEE 754 bit patterns in lowercase
    /// hex (or <c>"null"</c>), in file order.
    /// </summary>
    public static (Dictionary<(int RowGroup, string Column), string[]> Encodings, List<List<string>> Values)
        Read(string oracle, string path)
    {
        var encodings = new Dictionary<(int, string), string[]>();
        var values = new List<List<string>>();
        foreach (var line in Run(oracle, "read", path).Split('\n'))
        {
            var parts = line.TrimEnd('\r').Split(' ');
            switch (parts[0])
            {
                case "ENCODINGS":
                    encodings[(int.Parse(parts[1]), parts[2])] = parts[3].Split(',');
                    break;
                case "V":
                    int column = int.Parse(parts[1]);
                    while (values.Count <= column) values.Add([]);
                    values[column].Add(parts[3]);
                    break;
            }
        }
        return (encodings, values);
    }

    private static string? Find()
    {
        var dir = AppContext.BaseDirectory;
        for (int i = 0; i < 8 && dir is not null; i++)
        {
            if (File.Exists(Path.Combine(dir, "EngineeredWood.Parquet.Tests.csproj")))
            {
                var exe = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? Name + ".exe" : Name;
                var path = Path.Combine(dir, "Rust", "target", "release", exe);
                return File.Exists(path) ? path : null;
            }
            dir = Path.GetDirectoryName(dir.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar));
        }
        return null;
    }

    private static string Run(string oracle, params string[] args)
    {
        var psi = new ProcessStartInfo
        {
            FileName = oracle,
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
        if (p.ExitCode != 0)
            throw new InvalidOperationException(
                $"{Name} {string.Join(" ", args)} failed (exit {p.ExitCode}): {stderr.Result}");
        return stdout;
    }
}
