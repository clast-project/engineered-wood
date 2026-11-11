// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers;
using Apache.Arrow;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;
using EngineeredWood.Parquet;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// <see cref="ByteArrayRepresentation.Apply"/> over the batches a codec-seam host may hand back (#822 review round 1):
/// one array for several columns, or arrays over the same buffers. Every conversion reads its source before any source
/// is released, and each set of buffers is released once. The allocator overwrites its bytes when released, so a read
/// after release returns wrong values rather than stale right ones.
/// </summary>
public class ByteArrayRepresentationTests
{
    private sealed class PoisoningAllocator : MemoryAllocator
    {
        private int _live;
        private int _doubleReleases;

        public int Live => Volatile.Read(ref _live);
        public int DoubleReleases => Volatile.Read(ref _doubleReleases);

        protected override IMemoryOwner<byte> AllocateInternal(int length, out int bytesAllocated)
        {
            bytesAllocated = length;
            Interlocked.Increment(ref _live);
            return new Owner(this, new byte[length]);
        }

        private sealed class Owner(PoisoningAllocator allocator, byte[] bytes) : IMemoryOwner<byte>
        {
            private int _disposed;

            public Memory<byte> Memory => bytes;

            public void Dispose()
            {
                if (Interlocked.Exchange(ref _disposed, 1) != 0)
                {
                    Interlocked.Increment(ref allocator._doubleReleases);
                    return;
                }

                bytes.AsSpan().Fill(0xEE);
                Interlocked.Decrement(ref allocator._live);
            }
        }
    }

    private static readonly string[] Values = ["alpha", "beta", null!, "a value long enough not to be inlined in a view"];

    private static StringArray Strings(MemoryAllocator allocator)
    {
        var builder = new StringArray.Builder();
        foreach (var value in Values)
        {
            if (value is null) builder.AppendNull();
            else builder.Append(value);
        }

        return builder.Build(allocator);
    }

    private static string?[] Read(IArrowArray array) =>
        [.. Enumerable.Range(0, array.Length).Select(i => array.IsNull(i) ? null : array switch
        {
            StringArray a => a.GetString(i),
            StringViewArray a => a.GetString(i),
            LargeStringArray a => a.GetString(i),
            _ => throw new NotSupportedException(array.GetType().Name),
        })];

    [Theory]
    [InlineData(ByteArrayOutputKind.ViewType)]
    [InlineData(ByteArrayOutputKind.LargeOffsets)]
    public void ColumnsOverTheSameBuffersAreAllConvertedAndReleasedOnce(ByteArrayOutputKind kind)
    {
        var allocator = new PoisoningAllocator();
        var shared = Strings(allocator);
        var slice = (StringArray)shared.Slice(0, shared.Length); // a second array over the same buffers
        var schema = new Apache.Arrow.Schema(
            [new Field("a", StringType.Default, true), new Field("b", StringType.Default, true), new Field("c", StringType.Default, true)],
            null);
        var batch = new RecordBatch(schema, [shared, shared, slice], shared.Length);

        using var result = ByteArrayRepresentation.Apply(batch, kind);

        for (int c = 0; c < result.ColumnCount; c++)
            Assert.Equal(Values, Read(result.Column(c)));
        Assert.Equal(0, allocator.Live);           // the shared buffers were released...
        Assert.Equal(0, allocator.DoubleReleases); // ...once
    }

    [Fact]
    public void LargeStringsKeepTheirBytes()
    {
        var source = Strings(MemoryAllocator.Default.Value);
        var batch = new RecordBatch(new Apache.Arrow.Schema([new Field("s", StringType.Default, true)], null), [source], source.Length);
        using var result = ByteArrayRepresentation.Apply(batch, ByteArrayOutputKind.LargeOffsets);
        var large = Assert.IsType<LargeStringArray>(result.Column(0));
        Assert.Equal(LargeStringType.Default.TypeId, result.Schema.FieldsList[0].DataType.TypeId);
        Assert.Equal(Values, Read(large));
    }
}
