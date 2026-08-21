// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Compression;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.Tests.Parquet;

/// <summary>
/// Parquet has no fixed-size list, so one is written as an ordinary three-level LIST — the same
/// thing PyArrow, Polars and DuckDB put on disk — and the width lives only in Arrow.
/// </summary>
/// <remarks>
/// The case that matters is a null slot. A variable-size list reaches its child through offsets and
/// consumes child positions sequentially; a fixed-size list's null slot still occupies its full
/// width of child positions, so consuming sequentially shifts every value after the first null.
/// Most of this file exists to hold that line.
/// </remarks>
public class FixedSizeListWriteTests : IDisposable
{
    private readonly string _tempDir;

    public FixedSizeListWriteTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-fsl-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
        GC.SuppressFinalize(this);
    }

    private static readonly ParquetWriteOptions Uncompressed =
        new() { Compression = CompressionCodec.Uncompressed };

    /// <summary>Builds a fixed-size list of int32, where a null slot is written as null.</summary>
    private static FixedSizeListArray Build(int width, params int?[]?[] slots)
    {
        var type = new FixedSizeListType(new Field("item", Int32Type.Default, nullable: true), width);
        var values = new Int32Array.Builder();
        var validity = new ArrowBuffer.BitmapBuilder();
        foreach (var slot in slots)
        {
            validity.Append(slot is not null);
            for (int index = 0; index < width; index++)
            {
                if (slot is null || slot[index] is null)
                    values.AppendNull();
                else
                    values.Append(slot[index]!.Value);
            }
        }

        return new FixedSizeListArray(
            type, slots.Length, values.Build(), validity.Build(), validity.UnsetBitCount);
    }

    private async Task<IReadOnlyList<int?[]?>> RoundTripAsync(int width, params int?[]?[] slots)
    {
        string path = Path.Combine(_tempDir, $"fsl-{Guid.NewGuid().ToString("N")[..8]}.parquet");
        var array = Build(width, slots);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("v", array.Data.DataType, nullable: true))
            .Build();

        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false, Uncompressed))
        {
            await writer.WriteRowGroupAsync(new RecordBatch(schema, [array], slots.Length));
            await writer.CloseAsync();
        }

        await using var readFile = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(readFile, ownsFile: false);
        var read = (ListArray)(await reader.ReadRowGroupAsync(0)).Column(0);
        var items = (Int32Array)read.Values;
        var offsets = read.ValueOffsets;

        var observed = new List<int?[]?>();
        for (int slot = 0; slot < read.Length; slot++)
        {
            if (read.IsNull(slot))
            {
                observed.Add(null);
                continue;
            }

            int start = offsets[slot];
            int length = offsets[slot + 1] - start;
            observed.Add([.. Enumerable.Range(start, length).Select(items.GetValue)]);
        }

        return observed;
    }

    [Fact]
    public async Task AFixedSizeListIsWrittenAsAnOrdinaryList()
    {
        string path = Path.Combine(_tempDir, "shape.parquet");
        var array = Build(2, [1, 2], [3, 4]);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("v", array.Data.DataType, nullable: true))
            .Build();

        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false, Uncompressed))
        {
            await writer.WriteRowGroupAsync(new RecordBatch(schema, [array], 2));
            await writer.CloseAsync();
        }

        await using var readFile = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(readFile, ownsFile: false);

        // An ordinary list on the way back, not a fixed-size one: the width is not in the format,
        // and reconstructing it from ARROW:schema is what makes PyArrow fail on its own files.
        var batch = await reader.ReadRowGroupAsync(0);
        Assert.IsType<ListType>(batch.Schema.FieldsList[0].DataType);
        Assert.IsType<ListArray>(batch.Column(0));
    }

    [Fact]
    public async Task ANullSlotDoesNotShiftTheValuesAfterIt()
    {
        // The whole point. A null slot occupies its full width of child positions, so a writer that
        // consumes the child sequentially puts 5 and 6 where 3 and 4 belong.
        var observed = await RoundTripAsync(2, [1, 2], null, [5, 6]);

        Assert.Equal(3, observed.Count);
        Assert.Equal([1, 2], observed[0]);
        Assert.Null(observed[1]);
        Assert.Equal([5, 6], observed[2]);
    }

    [Fact]
    public async Task ALeadingNullSlotDoesNotShiftEither()
    {
        var observed = await RoundTripAsync(2, null, [3, 4]);

        Assert.Null(observed[0]);
        Assert.Equal([3, 4], observed[1]);
    }

    [Fact]
    public async Task ConsecutiveNullSlotsAccumulateTheirFullWidth()
    {
        var observed = await RoundTripAsync(3, null, null, [7, 8, 9]);

        Assert.Null(observed[0]);
        Assert.Null(observed[1]);
        Assert.Equal([7, 8, 9], observed[2]);
    }

    [Fact]
    public async Task ANullElementInsideASlotIsNotANullSlot()
    {
        // A slot that is present but holds a null element keeps its width and stays non-null; only
        // the element is missing.
        var observed = await RoundTripAsync(2, [1, null], [null, 4]);

        Assert.Equal([1, null], observed[0]);
        Assert.Equal([null, 4], observed[1]);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(5)]
    public async Task TheWidthDoesNotHaveToBeTwo(int width)
    {
        int?[] slot = [.. Enumerable.Range(10, width).Select(value => (int?)value)];
        var observed = await RoundTripAsync(width, slot, null, slot);

        Assert.Equal(slot, observed[0]);
        Assert.Null(observed[1]);
        Assert.Equal(slot, observed[2]);
    }
}
