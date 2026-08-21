// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Ipc;

// The bridge writes its control messages to Console.Out, which is process-wide, so the tests that
// capture it must not run concurrently with one another.
[assembly: CollectionBehavior(DisableTestParallelization = true)]

namespace EngineeredWood.Tests.Parquet.Bridge;

/// <summary>
/// Drives <see cref="EngineeredWood.Parquet.Bridge.BridgeProgram"/> in process and captures the
/// single JSON control message it writes, which is the whole of what a caller may rely on.
/// </summary>
public abstract class BridgeHarness : IDisposable
{
    private readonly string _tempDir;

    protected BridgeHarness()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-bridge-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
        GC.SuppressFinalize(this);
    }

    /// <summary>A path inside this test's own directory.</summary>
    /// <param name="name">The file name.</param>
    protected string Path_(string name) => Path.Combine(_tempDir, name);

    /// <summary>Runs one operation and returns its exit code with everything it wrote to stdout.</summary>
    /// <param name="args">The operation and its arguments.</param>
    protected static async Task<BridgeResult> RunAsync(params string[] args)
    {
        var original = Console.Out;
        using var buffer = new StringWriter();
        Console.SetOut(buffer);
        try
        {
            int exitCode = await EngineeredWood.Parquet.Bridge.BridgeProgram.RunAsync(args)
                .ConfigureAwait(false);
            return new BridgeResult(exitCode, buffer.ToString());
        }
        finally
        {
            Console.SetOut(original);
        }
    }

    /// <summary>Writes a batch as the Arrow IPC file format the contract exchanges.</summary>
    /// <param name="batch">The batch to write.</param>
    /// <param name="path">The destination path.</param>
    protected static async Task WriteArrowAsync(RecordBatch batch, string path)
    {
        await using var output = File.Create(path);
        using var writer = new ArrowFileWriter(output, batch.Schema);
        await writer.WriteStartAsync().ConfigureAwait(false);
        await writer.WriteRecordBatchAsync(batch).ConfigureAwait(false);
        await writer.WriteEndAsync().ConfigureAwait(false);
    }

    /// <summary>Writes only a schema, which is how a zero-row table crosses the boundary.</summary>
    /// <param name="schema">The schema to write.</param>
    /// <param name="path">The destination path.</param>
    protected static async Task WriteArrowSchemaAsync(Apache.Arrow.Schema schema, string path)
    {
        await using var output = File.Create(path);
        using var writer = new ArrowFileWriter(output, schema);
        await writer.WriteStartAsync().ConfigureAwait(false);
        await writer.WriteEndAsync().ConfigureAwait(false);
    }

    /// <summary>Reads an Arrow IPC file back, returning its schema and every batch it holds.</summary>
    /// <param name="path">The file to read.</param>
    protected static async Task<(Apache.Arrow.Schema Schema, List<RecordBatch> Batches)> ReadArrowAsync(
        string path)
    {
        await using var input = File.OpenRead(path);
        using var reader = new ArrowFileReader(input);
        var batches = new List<RecordBatch>();
        RecordBatch? batch;
        while ((batch = await reader.ReadNextRecordBatchAsync().ConfigureAwait(false)) is not null)
            batches.Add(batch);
        var schema = reader.Schema ?? batches.FirstOrDefault()?.Schema
            ?? throw new InvalidOperationException($"no schema in {path}");
        return (schema, batches);
    }
}

/// <summary>One invocation's exit code and control message.</summary>
/// <param name="ExitCode">The process exit code the contract defines.</param>
/// <param name="Stdout">Everything written to stdout, which must be exactly one JSON object.</param>
public readonly record struct BridgeResult(int ExitCode, string Stdout)
{
    /// <summary>Parses stdout, asserting that it is exactly one JSON object and nothing else.</summary>
    public JsonElement Control()
    {
        // Trailing text would break a caller reading the stream, so the parse is deliberately
        // strict about consuming the whole of it rather than just the leading value.
        var reader = new Utf8JsonReader(
            System.Text.Encoding.UTF8.GetBytes(Stdout),
            new JsonReaderOptions { CommentHandling = JsonCommentHandling.Disallow });
        Assert.True(JsonDocument.TryParseValue(ref reader, out var document), "stdout is not JSON");

        // A JsonDocument holds pooled buffers, so it has to be returned even though what escapes
        // here is an element. Clone is what makes that safe: it detaches the value from the
        // document's memory, so the returned element outlives the dispose below. Removing either
        // half breaks the other.
        using var owned = document!;
        Assert.False(reader.Read(), "stdout carries more than one JSON value");
        return owned.RootElement.Clone();
    }

    /// <summary>The <c>kind</c> and <c>detail</c> of an error control message.</summary>
    public (string Kind, string Detail) Error()
    {
        var control = Control();
        Assert.Equal("ERROR", control.GetProperty("status").GetString());
        return (
            control.GetProperty("kind").GetString() ?? string.Empty,
            control.GetProperty("detail").GetString() ?? string.Empty);
    }
}
