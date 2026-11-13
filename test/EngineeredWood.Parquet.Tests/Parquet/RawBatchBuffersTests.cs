// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using EngineeredWood.Arrow;
using EngineeredWood.Parquet;

namespace EngineeredWood.Tests.Parquet;

/// <summary>
/// What <see cref="RawBatchBuffers.ReleaseAllBut"/> keeps: every raw buffer whose allocation the output still reads,
/// including through a buffer of its own over part of that memory (#857 review round 4).
/// </summary>
public class RawBatchBuffersTests
{
    [Fact]
    public void AnOutputBufferOverPartOfARawBuffer_KeepsIt()
    {
        var raw = FixedSizeBinaryBatch(out var values);
        // Rows 1 and 2, in a buffer of their own over the raw values' memory, as compaction's normalization builds it.
        var window = new ArrowBuffer(values.Memory.Slice(4, 8));
        var output = Batch(new FixedSizeBinaryArray(
            new ArrayData(new FixedSizeBinaryType(4), 2, 0, 0, [ArrowBuffer.Empty, window])));

        RawBatchBuffers.Of(raw).ReleaseAllBut(output);

        // A released buffer's memory can't be reached; a kept one still reads its values.
        Assert.Equal(new byte[] { 2, 2, 2, 2, 3, 3, 3, 3 }, window.Span.ToArray());
        Assert.Equal(16, values.Memory.Length);
    }

    [Fact]
    public void ARawBufferTheOutputDoesNotRead_IsReleased()
    {
        var raw = FixedSizeBinaryBatch(out var values);
        var output = Batch(new FixedSizeBinaryArray(
            new ArrayData(new FixedSizeBinaryType(4), 1, 0, 0, [ArrowBuffer.Empty, new ArrowBuffer(new byte[4])])));

        RawBatchBuffers.Of(raw).ReleaseAllBut(output);

        Assert.ThrowsAny<Exception>(() => values.Memory.Length);
    }

    // Four 4-byte values (1,1,1,1 / 2,2,2,2 / ...) in a native buffer.
    private static RecordBatch FixedSizeBinaryBatch(out ArrowBuffer values)
    {
        var native = new NativeBuffer<byte>(16);
        for (int i = 0; i < 16; i++)
            native.Span[i] = (byte)(i / 4 + 1);
        values = native.Build();
        return Batch(new FixedSizeBinaryArray(
            new ArrayData(new FixedSizeBinaryType(4), 4, 0, 0, [ArrowBuffer.Empty, values])));
    }

    private static RecordBatch Batch(IArrowArray column) =>
        new(new Apache.Arrow.Schema([new Field("b", column.Data.DataType, true)], null), [column], column.Length);
}
