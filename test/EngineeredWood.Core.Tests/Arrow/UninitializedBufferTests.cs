// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Arrow;

namespace EngineeredWood.Core.Tests.Arrow;

/// <summary>
/// The gathers and widenings allocate their output buffers WITHOUT zero-fill, on the contract that every
/// element is written before the array is handed out. A site that breaks that contract emits whatever the
/// heap last held there.
///
/// <para>The rest of the suite cannot catch that. Freshly demanded pages are zero, so a buffer with a hole in
/// it reads back as 0 — which for most of these tests is indistinguishable from the right answer, and passes.
/// These tests first DIRTY the heap with a recognisable pattern and free it, so an allocation that reuses
/// those pages starts non-zero, and only then run the operation.</para>
///
/// <para>That makes them probabilistic in the direction that is safe: a hole may go unseen on some run, but
/// a failure is always real. They are worth their keep because the alternative failure mode is silent
/// garbage in a written file.</para>
/// </summary>
public class UninitializedBufferTests
{
    /// <summary>Rows per array — large enough to clear the runtime's zero-fill threshold comfortably.</summary>
    private const int Rows = 64 * 1024;

    /// <summary>
    /// Churns the heap with 0xCC-filled blocks and drops them, so the next allocation of a similar size is
    /// likely to land on pages holding that pattern rather than on fresh zeroed ones.
    /// </summary>
    private static void DirtyHeap()
    {
        for (int i = 0; i < 8; i++)
        {
            var block = new byte[Rows * 8];
            block.AsSpan().Fill(0xCC);
            GC.KeepAlive(block);
        }

        GC.Collect();
        GC.WaitForPendingFinalizers();
    }

    private static int[] ReverseIndices(int count)
    {
        var indices = new int[count];
        for (int i = 0; i < count; i++)
            indices[i] = count - 1 - i;
        return indices;
    }

    [Fact]
    public void Widen_WritesEveryOutputSlot()
    {
        var bytes = new byte[Rows * sizeof(short)];
        var src = System.Runtime.InteropServices.MemoryMarshal.Cast<byte, short>(bytes.AsSpan());
        for (int i = 0; i < Rows; i++)
            src[i] = (short)(i % 4001 - 2000);

        var source = ArrowArrayFactory.BuildArray(new ArrayData(
            Int16Type.Default, Rows, 0, 0, new[] { ArrowBuffer.Empty, new ArrowBuffer(bytes) }));

        DirtyHeap();
        var widened = Assert.IsType<Int32Array>(ArrowCompute.Widen(source, Int32Type.Default));

        for (int i = 0; i < Rows; i++)
            Assert.Equal(i % 4001 - 2000, widened.GetValue(i));
    }

    [Fact]
    public void Widen_WritesSlotsUnderNullsToo()
    {
        // The widening loop converts every row including the null ones, rather than skipping them — which is
        // exactly what lets its output buffer skip the zero-fill. If it ever started skipping nulls, those
        // slots would hold heap garbage instead of a defined value.
        var bytes = new byte[Rows * sizeof(int)];
        var src = System.Runtime.InteropServices.MemoryMarshal.Cast<byte, int>(bytes.AsSpan());
        for (int i = 0; i < Rows; i++)
            src[i] = i;

        var bitmap = new ArrowBuffer.BitmapBuilder(Rows);
        for (int i = 0; i < Rows; i++)
            bitmap.Append(i % 3 != 0);

        var source = ArrowArrayFactory.BuildArray(new ArrayData(
            Int32Type.Default, Rows, Rows / 3 + 1, 0,
            new[] { bitmap.Build(), new ArrowBuffer(bytes) }));

        DirtyHeap();
        var widened = Assert.IsType<Int64Array>(ArrowCompute.Widen(source, Int64Type.Default));

        for (int i = 0; i < Rows; i++)
        {
            if (i % 3 == 0)
                Assert.True(widened.IsNull(i));
            else
                Assert.Equal(i, widened.GetValue(i));
        }
    }

    [Fact]
    public void TakeString_WritesEveryValueByteAndOffset()
    {
        var values = new string[Rows];
        for (int i = 0; i < Rows; i++)
            values[i] = new string((char)('a' + i % 26), i % 7 + 1);

        var source = RawArrays.VarBinary(StringType.Default, values, large: false);
        int[] indices = ReverseIndices(Rows);

        DirtyHeap();
        var result = Assert.IsType<StringArray>(ArrowCompute.Take(source, indices));

        for (int i = 0; i < Rows; i++)
            Assert.Equal(values[indices[i]], result.GetString(i));
    }

    [Fact]
    public void TakeStringWithNulls_LeavesNoGapBetweenSlices()
    {
        // A null contributes a zero-length slice, so the kept rows' bytes must still tile the value buffer end
        // to end. A gap would be uninitialized and would land inside some other row's string.
        var values = new string[Rows];
        var valid = new bool[Rows];
        for (int i = 0; i < Rows; i++)
        {
            values[i] = new string((char)('a' + i % 26), i % 5 + 1);
            valid[i] = i % 4 != 0;
        }

        var source = RawArrays.VarBinary(StringType.Default, values, large: false, valid);
        int[] indices = ReverseIndices(Rows);

        DirtyHeap();
        var result = Assert.IsType<StringArray>(ArrowCompute.Take(source, indices));

        for (int i = 0; i < Rows; i++)
        {
            int r = indices[i];
            if (valid[r])
                Assert.Equal(values[r], result.GetString(i));
            else
                Assert.Null(result.GetString(i));
        }
    }

    [Fact]
    public void TakeList_WritesEveryOffsetAndChildPosition()
    {
        var child = new int[Rows * 2];
        for (int i = 0; i < child.Length; i++)
            child[i] = i;

        var offsets = new int[Rows + 1];
        for (int i = 0; i <= Rows; i++)
            offsets[i] = i * 2;

        var source = RawArrays.List(
            new ListType(new Field("item", Int32Type.Default, true)),
            offsets,
            RawArrays.Fixed(Int32Type.Default, child));

        int[] indices = ReverseIndices(Rows);

        DirtyHeap();
        var result = Assert.IsType<ListArray>(ArrowCompute.Take(source, indices));

        Assert.Equal(Rows, result.Length);
        for (int i = 0; i < Rows; i++)
        {
            int r = indices[i];
            var slice = Assert.IsType<Int32Array>(result.GetSlicedValues(i));
            Assert.Equal(2, slice.Length);
            Assert.Equal(r * 2, slice.GetValue(0));
            Assert.Equal(r * 2 + 1, slice.GetValue(1));
        }
    }

    [Fact]
    public void TakeFixedWidth_NullSlotsStayZeroRatherThanHoldingHeapGarbage()
    {
        // TakeFixedWidth deliberately keeps its ZEROED allocation: its null path writes nothing, so the slot
        // is whatever the buffer arrived holding. Zeroed that is a defined value; uninitialized it would be
        // leftover heap contents that a downstream encoder or statistics pass could read and write to a file.
        var bytes = new byte[Rows * sizeof(long)];
        var src = System.Runtime.InteropServices.MemoryMarshal.Cast<byte, long>(bytes.AsSpan());
        for (int i = 0; i < Rows; i++)
            src[i] = unchecked(0x1122334455667788L + i);

        var bitmap = new ArrowBuffer.BitmapBuilder(Rows);
        for (int i = 0; i < Rows; i++)
            bitmap.Append(i % 2 == 0);

        var source = ArrowArrayFactory.BuildArray(new ArrayData(
            Int64Type.Default, Rows, Rows / 2, 0,
            new[] { bitmap.Build(), new ArrowBuffer(bytes) }));

        DirtyHeap();
        var result = Assert.IsType<Int64Array>(ArrowCompute.Take(source, ReverseIndices(Rows)));

        var raw = System.Runtime.InteropServices.MemoryMarshal
            .Cast<byte, long>(result.Data.Buffers[1].Span);
        for (int i = 0; i < Rows; i++)
        {
            if (result.IsNull(i))
                Assert.Equal(0L, raw[i]);
        }
    }

    [Fact]
    public void TakeBoolean_FalseAndNullBitsStayClear()
    {
        // The boolean gather only ever SETS bits, so its values bitmap must stay a zeroed allocation.
        bool[] values = new bool[Rows];
        bool[] valid = new bool[Rows];
        for (int i = 0; i < Rows; i++)
        {
            values[i] = i % 3 == 0;
            valid[i] = i % 5 != 0;
        }

        var source = RawArrays.Boolean(values, valid);

        DirtyHeap();
        var result = Assert.IsType<BooleanArray>(ArrowCompute.Take(source, ReverseIndices(Rows)));

        int[] indices = ReverseIndices(Rows);
        for (int i = 0; i < Rows; i++)
        {
            int r = indices[i];
            if (!valid[r])
                Assert.Null(result.GetValue(i));
            else
                Assert.Equal(values[r], result.GetValue(i));
        }
    }
}
