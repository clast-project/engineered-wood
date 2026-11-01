// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

#if NET8_0_OR_GREATER
using System.Buffers;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Apache.Arrow;

namespace EngineeredWood.Arrow;

/// <summary>
/// EXPERIMENT: a process-wide pool of native blocks behind <see cref="NativeBuffer{T}"/>, so a
/// column buffer released by a disposed Arrow array is reused by the next read instead of being
/// handed back to the OS and page-faulted in again.
/// </summary>
/// <remarks>
/// Blocks at or below <see cref="MinPooledBytes"/> are not pooled (the CRT heap already caches
/// them). Larger requests round up to a power-of-two size class; the caller still sees exactly
/// the length it asked for. Retained (idle) bytes are capped by <see cref="MaxRetainedBytes"/>.
/// </remarks>
public static class NativeBufferPool
{
    /// <summary>Whether new <see cref="NativeBuffer{T}"/>s draw from the pool. Off by default.</summary>
    public static bool Enabled { get; set; } = Environment.GetEnvironmentVariable("EW_NATIVE_POOL") == "1";

    /// <summary>Most idle bytes the pool keeps; a block returned beyond this is freed.</summary>
    public static long MaxRetainedBytes { get; set; } = 256L << 20;

    internal const int MinPooledBytes = 64 * 1024;
    private const int MaxClass = 30;
    private const nuint Alignment = 64;

    private static readonly Stack<nint>[] s_free = CreateClasses();
    private static long s_retained;
    private static long s_hits;
    private static long s_misses;

    /// <summary>Rents served from an idle block.</summary>
    public static long Hits => Volatile.Read(ref s_hits);

    /// <summary>Rents that had to allocate.</summary>
    public static long Misses => Volatile.Read(ref s_misses);

    /// <summary>Idle bytes currently held.</summary>
    public static long RetainedBytes => Volatile.Read(ref s_retained);

    private static Stack<nint>[] CreateClasses()
    {
        var classes = new Stack<nint>[MaxClass + 1];
        for (int i = 0; i < classes.Length; i++)
            classes[i] = new Stack<nint>();
        return classes;
    }

    /// <summary>The size class for a request, or -1 when it is not pooled.</summary>
    internal static int ClassFor(int length)
    {
        if (length <= MinPooledBytes)
            return -1;
        int cls = BitOperations.Log2((uint)length - 1) + 1;
        return cls > MaxClass ? -1 : cls;
    }

    internal static unsafe void* Rent(int cls)
    {
        var stack = s_free[cls];
        lock (stack)
        {
            if (stack.Count > 0)
            {
                Interlocked.Add(ref s_retained, -(1L << cls));
                Interlocked.Increment(ref s_hits);
                return (void*)stack.Pop();
            }
        }
        Interlocked.Increment(ref s_misses);
        return NativeMemory.AlignedAlloc((nuint)1 << cls, Alignment);
    }

    internal static unsafe void Return(void* pointer, int cls)
    {
        long size = 1L << cls;
        if (Interlocked.Add(ref s_retained, size) <= MaxRetainedBytes)
        {
            var stack = s_free[cls];
            lock (stack)
                stack.Push((nint)pointer);
            return;
        }
        Interlocked.Add(ref s_retained, -size);
        NativeMemory.AlignedFree(pointer);
    }

    /// <summary>Frees every idle block.</summary>
    public static unsafe void Trim()
    {
        for (int cls = 0; cls < s_free.Length; cls++)
        {
            var stack = s_free[cls];
            lock (stack)
            {
                while (stack.Count > 0)
                {
                    NativeMemory.AlignedFree((void*)stack.Pop());
                    Interlocked.Add(ref s_retained, -(1L << cls));
                }
            }
        }
    }

    internal static unsafe void* AllocateUnpooled(int length)
        => NativeMemory.AlignedAlloc((nuint)Math.Max(length, 1), Alignment);

    internal static unsafe void FreeUnpooled(void* pointer) => NativeMemory.AlignedFree(pointer);

    /// <summary>Arrow's ownership-taking constructor is internal; reach it for the experiment.</summary>
    [UnsafeAccessor(UnsafeAccessorKind.Constructor)]
    internal static extern ArrowBuffer CreateOwnedArrowBuffer(IMemoryOwner<byte> memoryOwner);
}

/// <summary>A native block that returns to <see cref="NativeBufferPool"/> when disposed.</summary>
internal sealed unsafe class PooledMemoryManager : MemoryManager<byte>
{
    private void* _pointer;
    private int _length;
    private int _class;
    private int _pinCount;

    public PooledMemoryManager(int length)
    {
        _class = NativeBufferPool.ClassFor(length);
        _pointer = _class < 0 ? NativeBufferPool.AllocateUnpooled(length) : NativeBufferPool.Rent(_class);
        _length = length;
        NativeMemoryTracker.OnTrack(length);
    }

    public int Length => _length;

    public override Span<byte> GetSpan()
    {
        if (_pointer == null)
            throw new ObjectDisposedException(nameof(PooledMemoryManager));
        return new Span<byte>(_pointer, _length);
    }

    public override MemoryHandle Pin(int elementIndex = 0)
    {
        Interlocked.Increment(ref _pinCount);
        return new MemoryHandle((byte*)_pointer + elementIndex, pinnable: this);
    }

    public override void Unpin() => Interlocked.Decrement(ref _pinCount);

    /// <summary>Resizes, preserving the first min(old, new) bytes.</summary>
    public void Reallocate(int newLength)
    {
        if (Volatile.Read(ref _pinCount) > 0)
            throw new InvalidOperationException("Cannot reallocate a native buffer that is currently pinned.");

        int oldLength = _length;
        int capacity = _class < 0 ? oldLength : 1 << _class;
        if (newLength <= capacity)
        {
            _length = newLength;
            NativeMemoryTracker.OnTrack(newLength - oldLength);
            return;
        }

        int newClass = NativeBufferPool.ClassFor(newLength);
        void* newPointer = newClass < 0 ? NativeBufferPool.AllocateUnpooled(newLength) : NativeBufferPool.Rent(newClass);
        Buffer.MemoryCopy(_pointer, newPointer, newLength, Math.Min(oldLength, newLength));
        Release(_pointer, _class);
        _pointer = newPointer;
        _class = newClass;
        _length = newLength;
        NativeMemoryTracker.OnTrack(newLength - oldLength);
    }

    protected override void Dispose(bool disposing)
    {
        if (_pointer == null)
            return;
        if (disposing && _pinCount > 0)
            throw new InvalidOperationException("cannot free native memory while it is pinned");
        Release(_pointer, _class);
        _pointer = null;
        NativeMemoryTracker.OnTrack(-_length);
    }

    private static void Release(void* pointer, int cls)
    {
        if (cls < 0)
            NativeBufferPool.FreeUnpooled(pointer);
        else
            NativeBufferPool.Return(pointer, cls);
    }
}
#endif
