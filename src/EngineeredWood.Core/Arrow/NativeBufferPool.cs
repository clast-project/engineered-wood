// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

#if NET8_0_OR_GREATER
using System.Buffers;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Apache.Arrow;

namespace EngineeredWood.Arrow;

/// <summary>EXPERIMENT: how <see cref="NativeBuffer{T}"/> gets its memory.</summary>
public enum NativePoolMode
{
    /// <summary>No pool: every buffer is allocated fresh and freed on dispose (main's behaviour).</summary>
    Off = 0,

    /// <summary>
    /// The first prototype: power-of-two blocks from <see cref="NativeMemory.AlignedAlloc"/>, each fully
    /// committed, kept whole while idle.
    /// </summary>
    Classic = 1,

    /// <summary>
    /// Commit-aware (Windows only; elsewhere behaves as <see cref="Classic"/>): each block reserves its
    /// power-of-two size class but commits only the pages its buffer asked for, and idle blocks count against
    /// the retention cap by their committed bytes.
    /// </summary>
    Commit = 2,
}

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
    /// <summary>Which pool new <see cref="NativeBuffer{T}"/>s draw from. Off by default.</summary>
    public static NativePoolMode Mode { get; set; } = ParseMode(Environment.GetEnvironmentVariable("EW_NATIVE_POOL"));

    /// <summary>Whether new <see cref="NativeBuffer{T}"/>s draw from a pool.</summary>
    public static bool Enabled
    {
        get => Mode != NativePoolMode.Off;
        set => Mode = value ? NativePoolMode.Classic : NativePoolMode.Off;
    }

    /// <summary>The process commit limit found at startup, or 0 when there is none.</summary>
    public static long CommitLimitBytes { get; } = WindowsJob.ProcessCommitLimit();

    /// <summary>
    /// Most idle bytes the pool keeps; a block returned beyond this is freed. Defaults to 256 MiB, or, in a
    /// process with a commit limit, a quarter of what that limit leaves outside the GC heap's hard limit,
    /// whichever is smaller. <c>EW_NATIVE_POOL_CAP_MB</c> overrides it.
    /// </summary>
    public static long MaxRetainedBytes { get; set; } = DefaultMaxRetained();

    internal const int MinPooledBytes = 64 * 1024;
    private const int MaxClass = 30;
    private const nuint Alignment = 64;
    internal const int PageSize = 4096;

    private static readonly Stack<nint>[] s_free = CreateClasses<nint>();
    private static readonly Stack<(nint Base, int Committed)>[] s_reserved = CreateClasses<(nint, int)>();
    private static long s_retained;
    private static long s_hits;
    private static long s_misses;
    private static long s_trims;

    /// <summary>Rents served from an idle block.</summary>
    public static long Hits => Volatile.Read(ref s_hits);

    /// <summary>Rents that had to allocate.</summary>
    public static long Misses => Volatile.Read(ref s_misses);

    /// <summary>Times a failed commit made the pool free its idle blocks and retry.</summary>
    public static long PressureTrims => Volatile.Read(ref s_trims);

    /// <summary>Idle bytes currently held (committed bytes, in <see cref="NativePoolMode.Commit"/>).</summary>
    public static long RetainedBytes => Volatile.Read(ref s_retained);

    internal static bool CommitModeSupported => OperatingSystem.IsWindows();

    private static NativePoolMode ParseMode(string? value) => value switch
    {
        "1" or "classic" => NativePoolMode.Classic,
        "2" or "commit" => NativePoolMode.Commit,
        _ => NativePoolMode.Off,
    };

    private static long DefaultMaxRetained()
    {
        if (long.TryParse(Environment.GetEnvironmentVariable("EW_NATIVE_POOL_CAP_MB"), out long mb))
            return mb << 20;
        const long Default = 256L << 20;
        long limit = CommitLimitBytes;
        if (limit <= 0)
            return Default;
        long gcLimit = GC.GetGCMemoryInfo().TotalAvailableMemoryBytes;
        long nativeShare = gcLimit > 0 && gcLimit < limit ? limit - gcLimit : limit / 4;
        return Math.Min(Default, nativeShare / 4);
    }

    private static Stack<TItem>[] CreateClasses<TItem>()
    {
        var classes = new Stack<TItem>[MaxClass + 1];
        for (int i = 0; i < classes.Length; i++)
            classes[i] = new Stack<TItem>();
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

    internal static int PageRound(long length) => checked((int)((length + PageSize - 1) & ~(long)(PageSize - 1)));

    // ---- Classic: fully committed power-of-two blocks ----

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

    // ---- Commit: reserve the class, commit what is asked for ----

    /// <summary>
    /// Rents a block of class <paramref name="cls"/> whose committed prefix covers <paramref name="length"/>.
    /// <paramref name="staleBytes"/> is how many leading bytes may hold an earlier owner's data; the rest are
    /// freshly committed, so zero.
    /// </summary>
    internal static unsafe void* RentCommitted(int cls, int length, out int committed, out int staleBytes)
    {
        nint block = 0;
        int had = 0;
        var stack = s_reserved[cls];
        lock (stack)
        {
            if (stack.Count > 0)
                (block, had) = stack.Pop();
        }

        if (block != 0)
        {
            Interlocked.Add(ref s_retained, -had);
            Interlocked.Increment(ref s_hits);
        }
        else
        {
            Interlocked.Increment(ref s_misses);
            block = WindowsJob.Reserve((nuint)1 << cls);
            if (block == 0)
                throw new OutOfMemoryException($"Could not reserve {1L << cls} bytes of address space.");
        }

        committed = had;
        try
        {
            Recommit(block, ref committed, length);
        }
        catch
        {
            WindowsJob.Release(block);
            throw;
        }
        staleBytes = Math.Min(had, length);
        return (void*)block;
    }

    /// <summary>Committed bytes a block may hold beyond what its buffer needs before the tail is decommitted.</summary>
    private static int SlackAllowed(int need) => Math.Max(MinPooledBytes, need / 8);

    /// <summary>
    /// Brings a block's committed prefix to cover <paramref name="length"/> bytes: commits the missing pages,
    /// or decommits a tail that exceeds <see cref="SlackAllowed"/>.
    /// </summary>
    internal static void Recommit(nint block, ref int committed, int length)
    {
        int need = PageRound(length);
        if (need > committed)
        {
            if (!WindowsJob.Commit(block + committed, (nuint)(need - committed)))
            {
                // At the commit limit: give back every idle block, then try once more.
                Interlocked.Increment(ref s_trims);
                Trim();
                if (!WindowsJob.Commit(block + committed, (nuint)(need - committed)))
                    throw new OutOfMemoryException($"Could not commit {need - committed} bytes (commit limit reached).");
            }
            committed = need;
        }
        else if (committed - need > SlackAllowed(need))
        {
            WindowsJob.Decommit(block + need, (nuint)(committed - need));
            committed = need;
        }
    }

    internal static void ReturnCommitted(nint block, int cls, int committed)
    {
        if (Interlocked.Add(ref s_retained, committed) <= MaxRetainedBytes)
        {
            var stack = s_reserved[cls];
            lock (stack)
                stack.Push((block, committed));
            return;
        }
        Interlocked.Add(ref s_retained, -committed);
        WindowsJob.Release(block);
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

            var reserved = s_reserved[cls];
            lock (reserved)
            {
                while (reserved.Count > 0)
                {
                    var (block, committed) = reserved.Pop();
                    WindowsJob.Release(block);
                    Interlocked.Add(ref s_retained, -committed);
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
    private Block _block;
    private int _length;
    private int _pinCount;

    /// <summary>Where a buffer's bytes live, and how they go back.</summary>
    private struct Block
    {
        public void* Pointer;
        public int Class;       // -1 = unpooled
        public bool CommitMode;
        public int Committed;   // commit mode only

        public readonly int Capacity(int length) => Class < 0 ? length : 1 << Class;

        /// <summary>Allocates a block for <paramref name="length"/> bytes; zeroes them when asked.</summary>
        public static Block Allocate(int length, bool zeroFill)
        {
            var block = new Block { Class = NativeBufferPool.ClassFor(length) };
            block.CommitMode = block.Class >= 0
                && NativeBufferPool.Mode == NativePoolMode.Commit
                && NativeBufferPool.CommitModeSupported;

            if (block.CommitMode)
            {
                block.Pointer = NativeBufferPool.RentCommitted(block.Class, length, out block.Committed, out int stale);
                // Freshly committed pages are already zero; only the reused prefix needs clearing.
                if (zeroFill)
                    new Span<byte>(block.Pointer, stale).Clear();
            }
            else
            {
                block.Pointer = block.Class < 0
                    ? NativeBufferPool.AllocateUnpooled(length)
                    : NativeBufferPool.Rent(block.Class);
                if (zeroFill)
                    new Span<byte>(block.Pointer, length).Clear();
            }
            return block;
        }

        public readonly void Release()
        {
            if (CommitMode)
                NativeBufferPool.ReturnCommitted((nint)Pointer, Class, Committed);
            else if (Class < 0)
                NativeBufferPool.FreeUnpooled(Pointer);
            else
                NativeBufferPool.Return(Pointer, Class);
        }
    }

    /// <param name="length">Bytes the caller will see.</param>
    /// <param name="zeroFill">Whether those bytes must read as zero.</param>
    public PooledMemoryManager(int length, bool zeroFill)
    {
        _block = Block.Allocate(length, zeroFill);
        _length = length;
        NativeMemoryTracker.OnTrack(length);
    }

    public int Length => _length;

    public override Span<byte> GetSpan()
    {
        if (_block.Pointer == null)
            throw new ObjectDisposedException(nameof(PooledMemoryManager));
        return new Span<byte>(_block.Pointer, _length);
    }

    public override MemoryHandle Pin(int elementIndex = 0)
    {
        Interlocked.Increment(ref _pinCount);
        return new MemoryHandle((byte*)_block.Pointer + elementIndex, pinnable: this);
    }

    public override void Unpin() => Interlocked.Decrement(ref _pinCount);

    /// <summary>Resizes, preserving the first min(old, new) bytes. Bytes past the old length are unspecified.</summary>
    public void Reallocate(int newLength)
    {
        if (Volatile.Read(ref _pinCount) > 0)
            throw new InvalidOperationException("Cannot reallocate a native buffer that is currently pinned.");

        int oldLength = _length;
        if (newLength <= _block.Capacity(oldLength))
        {
            if (_block.CommitMode)
                NativeBufferPool.Recommit((nint)_block.Pointer, ref _block.Committed, newLength);
        }
        else
        {
            var grown = Block.Allocate(newLength, zeroFill: false);
            Buffer.MemoryCopy(_block.Pointer, grown.Pointer, newLength, Math.Min(oldLength, newLength));
            _block.Release();
            _block = grown;
        }
        _length = newLength;
        NativeMemoryTracker.OnTrack(newLength - oldLength);
    }

    /// <summary>
    /// Shrinks to <paramref name="usedBytes"/>, for <see cref="NativeBuffer{T}.Build"/>. In commit mode the
    /// tail is decommitted in place; otherwise the used bytes move to a block of their own size.
    /// </summary>
    public void TrimTo(int usedBytes)
    {
        if (Volatile.Read(ref _pinCount) > 0)
            throw new InvalidOperationException("Cannot trim a native buffer that is currently pinned.");
        if (_block.CommitMode || NativeBufferPool.ClassFor(usedBytes) == _block.Class)
        {
            Reallocate(usedBytes);
            return;
        }

        var trimmed = Block.Allocate(usedBytes, zeroFill: false);
        new Span<byte>(_block.Pointer, usedBytes).CopyTo(new Span<byte>(trimmed.Pointer, usedBytes));
        _block.Release();
        _block = trimmed;
        NativeMemoryTracker.OnTrack(usedBytes - _length);
        _length = usedBytes;
    }

    protected override void Dispose(bool disposing)
    {
        if (_block.Pointer == null)
            return;
        if (disposing && _pinCount > 0)
            throw new InvalidOperationException("cannot free native memory while it is pinned");
        _block.Release();
        _block.Pointer = null;
        NativeMemoryTracker.OnTrack(-_length);
    }
}

/// <summary>Win32 virtual-memory and job-object calls for <see cref="NativePoolMode.Commit"/>.</summary>
internal static class WindowsJob
{
    private const uint MEM_COMMIT = 0x1000;
    private const uint MEM_RESERVE = 0x2000;
    private const uint MEM_DECOMMIT = 0x4000;
    private const uint MEM_RELEASE = 0x8000;
    private const uint PAGE_NOACCESS = 0x01;
    private const uint PAGE_READWRITE = 0x04;
    private const int JobObjectExtendedLimitInformation = 9;
    private const uint JOB_OBJECT_LIMIT_PROCESS_MEMORY = 0x100;
    private const uint JOB_OBJECT_LIMIT_JOB_MEMORY = 0x200;

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern nint VirtualAlloc(nint address, nuint size, uint allocationType, uint protect);

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern int VirtualFree(nint address, nuint size, uint freeType);

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern unsafe int QueryInformationJobObject(
        nint job, int infoClass, void* info, uint length, out uint returnLength);

    [StructLayout(LayoutKind.Sequential)]
    internal struct ExtendedLimitInformation
    {
        public long PerProcessUserTimeLimit;
        public long PerJobUserTimeLimit;
        public uint LimitFlags;
        public nuint MinimumWorkingSetSize;
        public nuint MaximumWorkingSetSize;
        public uint ActiveProcessLimit;
        public nuint Affinity;
        public uint PriorityClass;
        public uint SchedulingClass;
        public ulong ReadOperationCount, WriteOperationCount, OtherOperationCount;
        public ulong ReadTransferCount, WriteTransferCount, OtherTransferCount;
        public nuint ProcessMemoryLimit;
        public nuint JobMemoryLimit;
        public nuint PeakProcessMemoryUsed;
        public nuint PeakJobMemoryUsed;
    }

    public static nint Reserve(nuint size) => VirtualAlloc(0, size, MEM_RESERVE, PAGE_NOACCESS);

    public static bool Commit(nint address, nuint size) => VirtualAlloc(address, size, MEM_COMMIT, PAGE_READWRITE) != 0;

    public static void Decommit(nint address, nuint size)
    {
        if (VirtualFree(address, size, MEM_DECOMMIT) == 0)
            throw new InvalidOperationException($"VirtualFree(MEM_DECOMMIT) failed: {Marshal.GetLastPInvokeError()}");
    }

    public static void Release(nint address)
    {
        if (VirtualFree(address, 0, MEM_RELEASE) == 0)
            throw new InvalidOperationException($"VirtualFree(MEM_RELEASE) failed: {Marshal.GetLastPInvokeError()}");
    }

    /// <summary>The tightest commit limit of the job this process runs in, or 0 when there is none.</summary>
    public static long ProcessCommitLimit()
    {
        if (!OperatingSystem.IsWindows() || !TryQuery(out var info))
            return 0;
        long limit = 0;
        if ((info.LimitFlags & JOB_OBJECT_LIMIT_PROCESS_MEMORY) != 0)
            limit = (long)info.ProcessMemoryLimit;
        if ((info.LimitFlags & JOB_OBJECT_LIMIT_JOB_MEMORY) != 0)
            limit = limit == 0 ? (long)info.JobMemoryLimit : Math.Min(limit, (long)info.JobMemoryLimit);
        return limit;
    }

    /// <summary>Queries the current process's job (the zero handle means "the caller's job").</summary>
    public static unsafe bool TryQuery(out ExtendedLimitInformation info)
    {
        ExtendedLimitInformation local = default;
        bool ok = QueryInformationJobObject(
            0, JobObjectExtendedLimitInformation, &local, (uint)sizeof(ExtendedLimitInformation), out _) != 0;
        info = local;
        return ok;
    }
}
#endif
