// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Memory;

namespace EngineeredWood.Arrow;

/// <summary>
/// Forwards <see cref="INativeAllocationTracker.Track"/> notifications from
/// <see cref="Apache.Arrow.Memory.NativeBuffer{TItem,TTracker}"/> into the static
/// <see cref="NativeMemoryTracker"/> counters. Stateless struct so the JIT can
/// specialise and avoid virtual dispatch.
/// </summary>
internal struct EngineeredWoodAllocationTracker : INativeAllocationTracker
{
    public void Track(int count, long bytes) => NativeMemoryTracker.OnTrack(bytes);
}

/// <summary>
/// A pre-sized native-memory buffer for accumulating column values.
/// Values are written into the buffer via <see cref="Span"/>; calling <see cref="Build"/>
/// transfers ownership to an <see cref="ArrowBuffer"/> (zero-copy).
/// </summary>
/// <remarks>
/// Thin facade over <see cref="Apache.Arrow.Memory.NativeBuffer{TItem,TTracker}"/>
/// (Apache.Arrow 23.0.0+). Exists to (1) keep <c>zeroFill</c> defaulting to <c>true</c>
/// and amortised 2x grow semantics that match the rest of the codebase, and (2) wire in
/// our <see cref="NativeMemoryTracker"/> so existing benchmarks and diagnostics keep
/// working.
/// </remarks>
internal sealed class NativeBuffer<T> : IDisposable where T : unmanaged
{
    private Apache.Arrow.Memory.NativeBuffer<T, EngineeredWoodAllocationTracker>? _inner;

    /// <summary>Creates a native buffer sized for <paramref name="elementCount"/> elements of <typeparamref name="T"/>.</summary>
    /// <param name="elementCount">Number of elements.</param>
    /// <param name="zeroFill">If true, the buffer is zeroed. Set to false when the caller
    /// will overwrite all bytes (e.g. value buffers for non-nullable columns).</param>
    public NativeBuffer(int elementCount, bool zeroFill = true)
    {
        _inner = new Apache.Arrow.Memory.NativeBuffer<T, EngineeredWoodAllocationTracker>(
            elementCount, zeroFill, default);
    }

    /// <summary>Number of <typeparamref name="T"/> elements that fit in the buffer.</summary>
    public int Length => _inner!.Length;

    /// <summary>Gets a <see cref="Span{T}"/> over the native buffer.</summary>
    public Span<T> Span => _inner!.Span;

    /// <summary>Gets a <see cref="Span{T}"/> over the raw bytes of the native buffer.</summary>
    public Span<byte> ByteSpan => _inner!.ByteSpan;

    /// <summary>
    /// Transfers ownership to an <see cref="ArrowBuffer"/>. This instance becomes unusable.
    /// </summary>
    public ArrowBuffer Build(int usedBytes = -1)
    {
        var inner = _inner ?? throw new ObjectDisposedException(nameof(NativeBuffer<T>));
        _inner = null;
        return inner.Build();
    }

    /// <summary>
    /// Grows the buffer to hold at least <paramref name="newElementCount"/> elements,
    /// preserving existing data. Newly added bytes are not zero-filled — callers that need
    /// zeroed memory must overwrite or zero them explicitly.
    /// </summary>
    public void Grow(int newElementCount)
    {
        var inner = _inner ?? throw new ObjectDisposedException(nameof(NativeBuffer<T>));
        if (newElementCount <= inner.Length)
            return;

        // Exponential growth (2x) to amortise repeated grows.
        int newCount = Math.Max(newElementCount, checked(inner.Length * 2));
        inner.Grow(newCount, zeroFill: false);
    }

    public void Dispose()
    {
        _inner?.Dispose();
        _inner = null;
    }
}

/// <summary>
/// Thread-safe tracker for native (unmanaged) memory allocated by
/// <see cref="NativeBuffer{T}"/>. Provides live and peak counters for use in
/// benchmarks and diagnostics.
/// </summary>
public static class NativeMemoryTracker
{
    private static long s_liveBytes;
    private static long s_peakBytes;
    private static long s_totalAllocated;

    /// <summary>Current live (allocated but not yet freed) native bytes.</summary>
    public static long LiveBytes => Volatile.Read(ref s_liveBytes);

    /// <summary>Peak live native bytes since the last reset.</summary>
    public static long PeakBytes => Volatile.Read(ref s_peakBytes);

    /// <summary>Total native bytes allocated since the last reset (cumulative, does not decrease on free).</summary>
    public static long TotalAllocated => Volatile.Read(ref s_totalAllocated);

    /// <summary>Resets all counters to zero. Call before a measurement interval.</summary>
    public static void Reset()
    {
        Volatile.Write(ref s_liveBytes, 0);
        Volatile.Write(ref s_peakBytes, 0);
        Volatile.Write(ref s_totalAllocated, 0);
    }

    /// <summary>
    /// Forwarded from <see cref="EngineeredWoodAllocationTracker.Track"/>. Positive
    /// <paramref name="bytes"/> are allocations (or grow deltas); negative are frees
    /// (or shrink deltas).
    /// </summary>
    internal static void OnTrack(long bytes)
    {
        if (bytes > 0)
        {
            Interlocked.Add(ref s_totalAllocated, bytes);
            long live = Interlocked.Add(ref s_liveBytes, bytes);
            UpdatePeak(live);
        }
        else if (bytes < 0)
        {
            Interlocked.Add(ref s_liveBytes, bytes);
        }
    }

    private static void UpdatePeak(long candidate)
    {
        long current = Volatile.Read(ref s_peakBytes);
        while (candidate > current)
        {
            long prev = Interlocked.CompareExchange(ref s_peakBytes, candidate, current);
            if (prev == current)
                break;
            current = prev;
        }
    }
}
