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

    private static StringViewArray Views(MemoryAllocator allocator)
    {
        var builder = new StringViewArray.Builder();
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

    /// <summary>
    /// A nested column's converted child is released too, not only a top-level string column (review round 3): the
    /// struct is rebuilt around the copy and its unconverted child, and every buffer the original held is released once
    /// the result is.
    /// </summary>
    [Fact]
    public void ANestedColumnsReplacedChildIsReleased()
    {
        var allocator = new PoisoningAllocator();
        var strings = Strings(allocator);
        var ints = new Int32Array.Builder().Append(1).Append(2).Append(3).Append(4).Build(allocator);
        var type = new StructType([new Field("s", StringType.Default, true), new Field("n", Int32Type.Default, false)]);
        var column = new StructArray(type, strings.Length, [strings, ints], ArrowBuffer.Empty, 0);
        var batch = new RecordBatch(new Apache.Arrow.Schema([new Field("st", type, true)], null), [column], column.Length);
        int before = allocator.Live;

        using (var result = ByteArrayRepresentation.Apply(batch, ByteArrayOutputKind.ViewType))
        {
            var converted = (StructArray)result.Column(0);
            Assert.Equal(Values, Read(converted.Fields[0]));
            Assert.Equal([1, 2, 3, 4], ((Int32Array)converted.Fields[1]).Values.ToArray());
            Assert.True(allocator.Live < before, "the replaced string child was not released");
        }

        Assert.Equal(0, allocator.Live);
        Assert.Equal(0, allocator.DoubleReleases);
    }

    /// <summary>
    /// A conversion over arrays it borrows, as a row mask's (review round 3), releases none of them: the reader gathers
    /// its rows from the same arrays afterwards.
    /// </summary>
    [Fact]
    public void ANonOwningConversionLeavesTheSourceIntact()
    {
        var allocator = new PoisoningAllocator();
        var source = Strings(allocator);
        var batch = new RecordBatch(new Apache.Arrow.Schema([new Field("s", StringType.Default, true)], null), [source], source.Length);
        int before = allocator.Live;

        using (var result = ByteArrayRepresentation.Apply(batch, ByteArrayOutputKind.ViewType, borrowed: batch))
            Assert.Equal(Values, Read(result.Column(0)));

        Assert.Equal(before, allocator.Live);
        Assert.Equal(Values, Read(source)); // still readable: not poisoned
        source.Dispose();
        Assert.Equal(0, allocator.Live);
    }

    /// <summary>
    /// What a row mask's reshaping built is released once the mask has answered (review round 4): an array it created
    /// (a partition value, say) and the conversion of it, but neither the reader's array it borrowed nor a column the
    /// answer shares buffers with.
    /// </summary>
    [Fact]
    public void AMasksReshapedBatchReleasesOnlyWhatItBuilt()
    {
        var readerAllocator = new PoisoningAllocator();
        var builtAllocator = new PoisoningAllocator();
        var raw = Views(readerAllocator); // decoded as the scan asks: the conversion leaves it as it is
        var flag = new BooleanArray.Builder().Append(true).Append(false).Append(true).Append(true).Build(readerAllocator);
        var borrowed = new RecordBatch(
            new Apache.Arrow.Schema([new Field("s", StringViewType.Default, true), new Field("flag", BooleanType.Default, false)], null),
            [raw, flag], raw.Length);

        // The reshaped batch: the reader's columns, plus a partition column the reshaping built, as plain utf8.
        var partition = Strings(builtAllocator);
        var shaped = new RecordBatch(
            new Apache.Arrow.Schema([.. borrowed.Schema.FieldsList, new Field("p", StringType.Default, true)], null),
            [raw, flag, partition], raw.Length);
        int readerBefore = readerAllocator.Live;

        // Converted with the reader's batch borrowed: the built partition column is replaced, and released.
        var converted = ByteArrayRepresentation.Apply(shaped, ByteArrayOutputKind.ViewType, borrowed);
        Assert.Equal(0, builtAllocator.Live);
        Assert.Equal(Values, Read(converted.Column(2)));

        // The answer is the reader's own flag column (WHERE flag), so it stays too.
        ByteArrayRepresentation.ReleaseReshaped(converted, borrowed, flag);

        Assert.Equal(readerBefore, readerAllocator.Live);
        Assert.Equal(Values, Read(raw));
        Assert.Equal([true, false, true, true], Enumerable.Range(0, 4).Select(i => flag.GetValue(i)!.Value));
        Assert.Equal(0, readerAllocator.DoubleReleases + builtAllocator.DoubleReleases);
    }

    /// <summary>
    /// <see cref="ByteArrayRepresentation.ReleaseReshaped"/> releases a column the reshaping built that kept its
    /// representation, too: a widened column or a backfilled NULL is the mask's alone once it has answered.
    /// </summary>
    [Fact]
    public void AMasksBuiltColumnIsReleasedWhetherOrNotItWasConverted()
    {
        var readerAllocator = new PoisoningAllocator();
        var builtAllocator = new PoisoningAllocator();
        var raw = new Int32Array.Builder().Append(1).Append(2).Build(readerAllocator);
        var widened = new Int64Array.Builder().Append(1).Append(2).Build(builtAllocator);
        var borrowed = new RecordBatch(new Apache.Arrow.Schema([new Field("n", Int32Type.Default, false)], null), [raw], 2);
        var shaped = new RecordBatch(new Apache.Arrow.Schema([new Field("n", Int64Type.Default, false)], null), [widened], 2);
        int readerBefore = readerAllocator.Live;

        ByteArrayRepresentation.ReleaseReshaped(shaped, borrowed, answer: null);

        Assert.Equal(0, builtAllocator.Live);
        Assert.Equal(readerBefore, readerAllocator.Live);
    }

    /// <summary>
    /// A mask may answer with a column the reshaping built (<c>WHERE flag</c> over a boolean partition column): that
    /// column is returned, so it is not released.
    /// </summary>
    [Fact]
    public void AMasksAnswerIsNotReleasedEvenWhenItWasBuilt()
    {
        var readerAllocator = new PoisoningAllocator();
        var builtAllocator = new PoisoningAllocator();
        var raw = new Int32Array.Builder().Append(1).Append(2).Build(readerAllocator);
        var partitionFlag = new BooleanArray.Builder().Append(true).Append(false).Build(builtAllocator);
        var borrowed = new RecordBatch(new Apache.Arrow.Schema([new Field("n", Int32Type.Default, false)], null), [raw], 2);
        var shaped = new RecordBatch(
            new Apache.Arrow.Schema([new Field("n", Int32Type.Default, false), new Field("flag", BooleanType.Default, false)], null),
            [raw, partitionFlag], 2);
        int builtBefore = builtAllocator.Live;

        ByteArrayRepresentation.ReleaseReshaped(shaped, borrowed, partitionFlag);

        Assert.Equal(builtBefore, builtAllocator.Live);
        Assert.Equal([true, false], Enumerable.Range(0, 2).Select(i => partitionFlag.GetValue(i)!.Value));
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
