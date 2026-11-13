// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using EngineeredWood.Arrow;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// Compaction reshapes the batches it reads in a pipeline of its own, and releases what it drops (#855, #857 review
/// round 3): a dropped column's decode, a widened column's narrow values.
/// <para>Measured against a control, not against zero: the writer keeps the batches it is handed and never disposes
/// them, so a compaction always leaves the decode of the columns it writes for the collector. A compaction must leave
/// no more than that: exactly what compacting the same rows without the dropped column leaves, and less than compacting
/// them unwidened.</para>
/// </summary>
[Collection(NativeMemoryCollection.Name)]
public class CompactionRawReleaseTests : IDisposable
{
    private const int Rows = 8_000;
    private static readonly CompactionOptions AlwaysCompact = new() { MinFileSize = long.MaxValue, TargetFileSize = long.MaxValue };
    private readonly string _tempDir;

    public CompactionRawReleaseTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-compaction-release-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        try { Directory.Delete(_tempDir, true); } catch { }
    }

    [Theory]
    [InlineData("name")]
    [InlineData("id")]
    public async Task CompactingADroppedColumn_LeavesNoMoreThanWithoutIt(string mapping)
    {
        var config = new Dictionary<string, string> { ["delta.columnMapping.mode"] = mapping };
        var dropped = await CreateAsync("dropped", config, withGone: true);
        await using (var table = await DeltaTable.OpenAsync(dropped))
            await table.DropColumnAsync("gone");
        var control = await CreateAsync("control", config, withGone: false);

        Assert.Equal(await LeftAfterCompactingAsync(control), await LeftAfterCompactingAsync(dropped));
    }

    [Fact]
    public async Task CompactingAWidenedColumn_ReleasesItsNarrowValues()
    {
        var config = new Dictionary<string, string> { [EngineeredWood.DeltaLake.Schema.TypeWidening.EnableKey] = "true" };
        var widened = await CreateAsync("widened", config, withGone: false);
        var control = await CreateAsync("control", config, withGone: false);
        // One more row each, widening `id` to long in one table and not in the other.
        foreach (var (fs, idType) in new[] { (widened, (IArrowType)Int64Type.Default), (control, Int32Type.Default) })
        {
            await using var table = await DeltaTable.OpenAsync(fs);
            var schema = new Apache.Arrow.Schema.Builder()
                .Field(new Field("id", idType, true))
                .Field(new Field("tag", StringType.Default, true)).Build();
            await table.WriteAsync(
                [new RecordBatch(schema, schema.FieldsList.Select(f => ArrowCompute.MakeNullArray(f.DataType, 1)), 1)],
                mergeSchema: true);
        }

        // The widened compaction writes its ids as new (managed) long arrays and releases the narrow ones it read; the
        // control writes the ints it read, which the writer keeps.
        Assert.True(await LeftAfterCompactingAsync(widened) < await LeftAfterCompactingAsync(control));
    }

    [Fact]
    public async Task CompactingAFixedSizeBinaryColumn_KeepsTheValuesItWrites()
    {
        // Files a foreign writer left with a FIXED_LEN_BYTE_ARRAY column: compaction's normalization rewrites it as
        // binary over a window of the values it read, so releasing what the written batch drops must keep them
        // (#857 review round 4).
        var dir = Path.Combine(_tempDir, "fsb");
        Directory.CreateDirectory(dir);
        var fs = new LocalTableFileSystem(dir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int32Type.Default, false))
            .Field(new Field("b", new FixedSizeBinaryType(4), true)).Build();
#pragma warning disable EWDELTA0001 // codec seam is experimental
        var hostOptions = DeltaTableOptions.Default with { DataFileWriter = new VerbatimWriter(dir) };
#pragma warning restore EWDELTA0001
        await using (var host = await DeltaTable.CreateAsync(fs, schema, hostOptions))
        {
            for (int part = 0; part < 2; part++)
            {
                var ids = new Int32Array.Builder();
                var bytes = new byte[Rows / 2 * 4];
                for (int i = 0; i < Rows / 2; i++)
                {
                    int id = part * Rows / 2 + i;
                    ids.Append(id);
                    BitConverter.GetBytes(id).CopyTo(bytes, i * 4);
                }

                var b = new FixedSizeBinaryArray(new ArrayData(
                    new FixedSizeBinaryType(4), Rows / 2, 0, 0, [ArrowBuffer.Empty, new ArrowBuffer(bytes)]));
                await host.WriteAsync([new RecordBatch(schema, [ids.Build(), b], Rows / 2)]);
            }
        }

        // Batched, so the reader hands out slices of each decoded row group at nonzero offsets: a window then is not the
        // raw buffer's own memory.
        await using var table = await DeltaTable.OpenAsync(fs, new DeltaTableOptions
        {
            MaxConcurrentReads = 1, ParquetReadOptions = new ParquetReadOptions { BatchSize = 1_000 },
        });
        Assert.NotNull(await table.CompactAsync(AlwaysCompact));

        int seen = 0;
        await foreach (var batch in table.ReadAllAsync())
        {
            using (batch)
            {
                var ids = (Int32Array)batch.Column(batch.Schema.GetFieldIndex("id"));
                var values = (BinaryArray)batch.Column(batch.Schema.GetFieldIndex("b"));
                for (int i = 0; i < batch.Length; i++, seen++)
                    Assert.Equal(ids.GetValue(i)!.Value, BitConverter.ToInt32(values.GetBytes(i).ToArray(), 0));
            }
        }
        Assert.Equal(Rows, seen);
    }

    /// <summary>Writes the batches it is handed as they are, as a foreign writer would.</summary>
#pragma warning disable EWDELTA0001 // codec seam is experimental
    private sealed class VerbatimWriter(string root) : IDataFileWriter
#pragma warning restore EWDELTA0001
    {
        public async ValueTask<long> WriteAsync(
            IAsyncEnumerable<RecordBatch> batches, string relativePath, CancellationToken cancellationToken)
        {
            string path = Path.Combine(root, relativePath);
            Directory.CreateDirectory(Path.GetDirectoryName(path)!);
            await using (var file = new LocalSequentialFile(path))
            {
                await using var writer = new ParquetFileWriter(file, ownsFile: false);
                await foreach (var b in batches.WithCancellation(cancellationToken))
                    await writer.WriteRowGroupAsync(b, cancellationToken);
                await writer.CloseAsync(cancellationToken);
            }
            return new FileInfo(path).Length;
        }
    }

    // A table of `Rows` rows in two files: an int `id` (every seventh NULL), a string `tag` and, `withGone`, a string
    // column to drop.
    private async Task<LocalTableFileSystem> CreateAsync(
        string name, Dictionary<string, string> configuration, bool withGone)
    {
        var dir = Path.Combine(_tempDir, name);
        Directory.CreateDirectory(dir);
        var fs = new LocalTableFileSystem(dir);
        var builder = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int32Type.Default, true))
            .Field(new Field("tag", StringType.Default, true));
        if (withGone)
            builder.Field(new Field("gone", StringType.Default, true));
        var schema = builder.Build();

        await using var table = await DeltaTable.CreateAsync(fs, schema, configuration: configuration);
        for (int part = 0; part < 2; part++)
        {
            var ids = new Int32Array.Builder();
            var tags = new StringArray.Builder();
            var gone = new StringArray.Builder();
            for (int i = part * Rows / 2; i < (part + 1) * Rows / 2; i++)
            {
                if (i % 7 == 0) ids.AppendNull(); else ids.Append(i);
                tags.Append("t" + (i % 4));
                gone.Append("a dropped column's value " + i);
            }

            IArrowArray[] columns = withGone ? [ids.Build(), tags.Build(), gone.Build()] : [ids.Build(), tags.Build()];
            await table.WriteAsync([new RecordBatch(schema, columns, Rows / 2)]);
        }

        return fs;
    }

    // The native bytes a compaction of the table leaves alive, before any collection.
    private static async Task<long> LeftAfterCompactingAsync(LocalTableFileSystem fs)
    {
        for (int i = 0; i < 3; i++)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
        }

        long before = NativeMemoryTracker.LiveBytes;
        await using (var table = await DeltaTable.OpenAsync(fs, new DeltaTableOptions { MaxConcurrentReads = 1 }))
            Assert.NotNull(await table.CompactAsync(AlwaysCompact));
        return NativeMemoryTracker.LiveBytes - before;
    }
}
