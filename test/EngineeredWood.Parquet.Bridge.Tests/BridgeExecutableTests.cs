// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text.Json;

namespace EngineeredWood.Tests.Parquet.Bridge;

/// <summary>
/// The same contract through the real executable rather than in process. Parquity spawns this as a
/// child and reads its streams, so the split between stdout and stderr, and the exit code the
/// process actually returns, have to hold outside the test host.
/// </summary>
public class BridgeExecutableTests : BridgeHarness
{
    private static string Executable()
    {
        string name = RuntimeInformation.IsOSPlatform(OSPlatform.Windows)
            ? "ew-parquet-bridge.exe"
            : "ew-parquet-bridge";
        string path = Path.Combine(AppContext.BaseDirectory, name);
        Assert.True(File.Exists(path), $"the bridge executable is not beside its tests: {path}");
        return path;
    }

    private static async Task<(int ExitCode, string Stdout, string Stderr)> RunProcessAsync(
        params string[] args)
    {
        var start = new ProcessStartInfo(Executable())
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            RedirectStandardInput = true,
            UseShellExecute = false,
        };
        foreach (var argument in args)
            start.ArgumentList.Add(argument);

        using var process = Process.Start(start)
            ?? throw new InvalidOperationException("the bridge did not start");
        process.StandardInput.Close();
        var stdout = process.StandardOutput.ReadToEndAsync();
        var stderr = process.StandardError.ReadToEndAsync();
        await process.WaitForExitAsync().ConfigureAwait(false);
        return (process.ExitCode, await stdout.ConfigureAwait(false), await stderr.ConfigureAwait(false));
    }

    [Fact]
    public async Task TheProcessAnswersInfoOnStdoutAndSaysNothingOnStderr()
    {
        var (exitCode, stdout, stderr) = await RunProcessAsync("info");

        Assert.Equal(0, exitCode);
        Assert.Empty(stderr);
        using var document = JsonDocument.Parse(stdout);
        Assert.Equal(
            EngineeredWood.Parquet.Bridge.BridgeProgram.Protocol,
            document.RootElement.GetProperty("protocol").GetString());
    }

    [Fact]
    public async Task TheProcessReturnsTwoForARequestItCannotUnderstand()
    {
        var (exitCode, stdout, _) = await RunProcessAsync("bogus");

        Assert.Equal(2, exitCode);
        using var document = JsonDocument.Parse(stdout);
        Assert.Equal("ERROR", document.RootElement.GetProperty("status").GetString());
    }

    [Fact]
    public async Task TheProcessReturnsOneWhenTheImplementationFails()
    {
        string corrupt = Path_("corrupt.parquet");
        await File.WriteAllTextAsync(corrupt, "not parquet");

        var (exitCode, stdout, _) = await RunProcessAsync(
            "read", "--parquet", corrupt, "--arrow", Path_("out.arrow"));

        Assert.Equal(1, exitCode);
        using var document = JsonDocument.Parse(stdout);
        Assert.Equal("ERROR", document.RootElement.GetProperty("status").GetString());
    }

    [Fact]
    public async Task TheProcessRoundTripsATableAndExitsCleanly()
    {
        string source = Path_("in.arrow");
        string parquet = Path_("out.parquet");
        string target = Path_("out.arrow");
        await WriteArrowAsync(BridgeContractTests.SingleColumnBatch(), source);

        var written = await RunProcessAsync("write", "--arrow", source, "--parquet", parquet);
        Assert.Equal(0, written.ExitCode);
        Assert.Equal("{\"status\":\"OK\"}", written.Stdout);
        Assert.Empty(written.Stderr);

        var read = await RunProcessAsync("read", "--parquet", parquet, "--arrow", target);
        Assert.Equal(0, read.ExitCode);

        var (_, batches) = await ReadArrowAsync(target);
        Assert.Equal(2, batches.Single().Length);
    }
}
