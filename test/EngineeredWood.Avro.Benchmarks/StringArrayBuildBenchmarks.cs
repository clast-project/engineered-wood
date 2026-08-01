// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;
using BenchmarkDotNet.Attributes;
using EngineeredWood.Arrow;

namespace EngineeredWood.Avro.Benchmarks;

/// <summary>
/// Compares ways of turning a list of strings into a <see cref="StringArray"/> — the shape of
/// <c>EnumBuilder.BuildDictionary</c>, and of any code that materialises a dictionary, a checkpoint
/// column, or a projected string column.
///
/// <para>The baseline is <see cref="StringArray.Builder"/> with one <c>Append</c> per value. Its
/// per-value work is already tight (Arrow measures the UTF-8 length and encodes straight into the
/// value buffer, no temporary array), so the cost being attacked here is structural:</para>
/// <list type="bullet">
///   <item>Both the offset and value buffers start at capacity 8 and double from there.</item>
///   <item><c>Build()</c> allocates each buffer afresh from the <see cref="MemoryAllocator"/> —
///   which zero-fills it — and then copies into it, overwriting every byte it just zeroed.</item>
/// </list>
///
/// <para>Every variant disposes what it builds. That matters: Arrow's buffers are native memory
/// behind a ref-counted handle, and leaving them to accumulate makes the builder look far worse
/// than it is (roughly 2.4x, measured) because <c>GC.AddMemoryPressure</c> drives extra
/// collections. Pricing the free symmetrically is the only fair comparison.</para>
///
/// <para>Inputs are read through <c>IReadOnlyList&lt;string&gt;</c> over a <c>List&lt;string&gt;</c>,
/// matching what <c>AvroSchemaParser</c> produces — an array would hand every variant a faster loop
/// than the real call site gets.</para>
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 7)]
public class StringArrayBuildBenchmarks
{
    private static readonly System.Text.Encoding Utf8 = new System.Text.UTF8Encoding(false);

    private IReadOnlyList<string> _symbols = null!;

    /// <summary>How many distinct values are in the array.</summary>
    [Params(8, 128, 4096)]
    public int SymbolCount;

    /// <summary>Characters per value (not bytes — see <see cref="Charset"/>).</summary>
    [Params(4, 32, 256)]
    public int SymbolLength;

    /// <summary>
    /// "Ascii" is one byte per char, which is all a spec-conformant Avro enum symbol can be.
    /// "Mixed" averages ~2 bytes per char, standing in for dictionaries from other sources where
    /// the UTF-8 length diverges from the UTF-16 length.
    /// </summary>
    [Params("Ascii", "Mixed")]
    public string Charset = "Ascii";

    [GlobalSetup]
    public void Setup()
    {
        var rng = new Random(20260801);
        var symbols = new List<string>(SymbolCount);
        for (int i = 0; i < SymbolCount; i++)
            symbols.Add(MakeSymbol(rng, SymbolLength, Charset == "Ascii"));
        _symbols = symbols;

        // A wrong-but-fast candidate is worthless, so prove they all agree before timing them.
        VerifyEquivalent();
    }

    private static string MakeSymbol(Random rng, int length, bool ascii)
    {
        const string AsciiAlphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_";
        // Latin-1 supplement and Greek are 2-byte UTF-8; CJK is 3-byte. Mixing them keeps the
        // byte-per-char ratio away from both 1 (where GetByteCount is trivially predictable) and 3.
        const string WideAlphabet = "àéîõüßΑΒΓΔΕΖабвгд漢字日本語";

        var alphabet = ascii ? AsciiAlphabet : WideAlphabet;
        var chars = new char[length];
        for (int i = 0; i < length; i++)
            chars[i] = alphabet[rng.Next(alphabet.Length)];
        return new string(chars);
    }

    private void VerifyEquivalent()
    {
        using var builder = BuildWithArrowBuilder();
        using var reserved = BuildWithReserve();
        using var range = BuildWithAppendRange();
        using var exact = BuildTwoPassManaged();
        using var upper = BuildSinglePassUpperBound();
        using var native = BuildNativeExact();
        StringArray[] all = [builder, reserved, range, exact, upper, native];

        foreach (var candidate in all)
        {
            if (candidate.Length != _symbols.Count)
                throw new InvalidOperationException($"length {candidate.Length} != {_symbols.Count}");
            for (int i = 0; i < _symbols.Count; i++)
            {
                if (!string.Equals(candidate.GetString(i), _symbols[i], StringComparison.Ordinal))
                    throw new InvalidOperationException($"candidate differs at {i}");
            }
        }
    }

    // ─── Benchmarks: build, release, return a cheap digest so nothing accumulates ───

    /// <summary>What <c>EnumBuilder.BuildDictionary</c> does today.</summary>
    [Benchmark(Baseline = true)]
    public int ArrowBuilder()
    {
        using var array = BuildWithArrowBuilder();
        return array.Length;
    }

    /// <summary>
    /// Same builder, told the value count up front. Arrow forwards that same number to the value
    /// buffer as a *byte* count, so the payload stays under-reserved — see the
    /// <c>TODO: [ARROW-9366]</c> still standing in BinaryArray.BuilderBase on arrow-dotnet main.
    /// </summary>
    [Benchmark]
    public int ArrowBuilderReserved()
    {
        using var array = BuildWithReserve();
        return array.Length;
    }

    /// <summary>
    /// Arrow's own bulk path, which pre-measures total UTF-8 length and reserves all three buffers.
    /// Takes that branch only because the input is an <c>ICollection&lt;string&gt;</c>.
    /// </summary>
    [Benchmark]
    public int ArrowAppendRange()
    {
        using var array = BuildWithAppendRange();
        return array.Length;
    }

    /// <summary>Exact-size two-pass build over managed arrays — GC-heap buffers, not Arrow-aligned.</summary>
    [Benchmark]
    public int TwoPassManaged()
    {
        using var array = BuildTwoPassManaged();
        return array.Length;
    }

    /// <summary>Single encode pass into a worst-case-sized managed buffer, then sliced to fit.</summary>
    [Benchmark]
    public int SinglePassUpperBound()
    {
        using var array = BuildSinglePassUpperBound();
        return array.Length;
    }

    /// <summary>Exact-size two-pass build into native 64-byte-aligned buffers, transferred zero-copy.</summary>
    [Benchmark]
    public int NativeExact()
    {
        using var array = BuildNativeExact();
        return array.Length;
    }

    // ─── Implementations ───

    private StringArray BuildWithArrowBuilder()
    {
        var b = new StringArray.Builder();
        foreach (var sym in _symbols)
            b.Append(sym);
        return b.Build();
    }

    private StringArray BuildWithReserve()
    {
        var b = new StringArray.Builder().Reserve(_symbols.Count);
        foreach (var sym in _symbols)
            b.Append(sym);
        return b.Build();
    }

    private StringArray BuildWithAppendRange()
    {
        var b = new StringArray.Builder();
        b.AppendRange(_symbols);
        return b.Build();
    }

    private StringArray BuildTwoPassManaged()
    {
        int n = _symbols.Count;
        var offsetBytes = new byte[(n + 1) * sizeof(int)];
        var offsets = MemoryMarshal.Cast<byte, int>(offsetBytes.AsSpan());

        int total = 0;
        for (int i = 0; i < n; i++)
        {
            offsets[i] = total;
            total += Utf8.GetByteCount(_symbols[i]);
        }
        offsets[n] = total;

        var values = new byte[total];
        int pos = 0;
        for (int i = 0; i < n; i++)
            pos += EncodeInto(_symbols[i], values, pos);

        return new StringArray(n,
            new ArrowBuffer(offsetBytes), new ArrowBuffer(values),
            ArrowBuffer.Empty, nullCount: 0, offset: 0);
    }

    /// <summary>
    /// Allocates the UTF-8 worst case (3 bytes/char) so each string is traversed once, then hands
    /// Arrow a slice rather than copying down to size. Watch the large-object-heap threshold: the
    /// 3x over-allocation crosses 85 KB well before the exact-size build does, and once it does,
    /// the Gen2 cost dwarfs the saved encode pass.
    /// </summary>
    private StringArray BuildSinglePassUpperBound()
    {
        int n = _symbols.Count;
        var offsetBytes = new byte[(n + 1) * sizeof(int)];
        var offsets = MemoryMarshal.Cast<byte, int>(offsetBytes.AsSpan());

        // string.Length is a field read, not a traversal — this loop is not a second pass.
        int totalChars = 0;
        for (int i = 0; i < n; i++)
            totalChars += _symbols[i].Length;

        var values = new byte[Utf8.GetMaxByteCount(totalChars)];
        int pos = 0;
        for (int i = 0; i < n; i++)
        {
            offsets[i] = pos;
            pos += EncodeInto(_symbols[i], values, pos);
        }
        offsets[n] = pos;

        return new StringArray(n,
            new ArrowBuffer(offsetBytes),
            new ArrowBuffer(new ReadOnlyMemory<byte>(values, 0, pos)),
            ArrowBuffer.Empty, nullCount: 0, offset: 0);
    }

    /// <summary>
    /// The <c>StringColumn</c> shape from the Delta checkpoint writer, sized exactly instead of
    /// grown. <c>zeroFill: false</c> because both buffers are fully overwritten below — that is
    /// precisely the dead work <c>ArrowBuffer.Builder.Build()</c> cannot avoid.
    /// </summary>
    private StringArray BuildNativeExact()
    {
        int n = _symbols.Count;
        var offsets = new NativeBuffer<int>(n + 1, zeroFill: false);
        NativeBuffer<byte>? data = null;
        try
        {
            var offsetSpan = offsets.Span;
            int total = 0;
            for (int i = 0; i < n; i++)
            {
                offsetSpan[i] = total;
                total += Utf8.GetByteCount(_symbols[i]);
            }
            offsetSpan[n] = total;

            data = new NativeBuffer<byte>(Math.Max(total, 1), zeroFill: false);
            var dataSpan = data.ByteSpan;
            int pos = 0;
            for (int i = 0; i < n; i++)
                pos += EncodeInto(_symbols[i], dataSpan, pos);

            // Build() transfers ownership to the ArrowBuffer; the locals must not be disposed after.
            var arrayData = new ArrayData(StringType.Default, n, nullCount: 0, offset: 0,
                [ArrowBuffer.Empty, offsets.Build(), data.Build()]);
            offsets = null!;
            data = null;
            return new StringArray(arrayData);
        }
        finally
        {
            offsets?.Dispose();
            data?.Dispose();
        }
    }

    private static int EncodeInto(string value, byte[] destination, int offset)
    {
#if NET
        return Utf8.GetBytes(value.AsSpan(), destination.AsSpan(offset));
#else
        return Utf8.GetBytes(value, 0, value.Length, destination, offset);
#endif
    }

    private static int EncodeInto(string value, Span<byte> destination, int offset)
    {
#if NET
        return Utf8.GetBytes(value.AsSpan(), destination.Slice(offset));
#else
        // netstandard2.0 has no span-based GetBytes; stage through a pooled array. This is a real
        // cost of the native path on net472, not a measurement artefact — the managed variants can
        // target their byte[] directly and skip it.
        byte[] staging = System.Buffers.ArrayPool<byte>.Shared.Rent(Utf8.GetMaxByteCount(value.Length));
        try
        {
            int written = Utf8.GetBytes(value, 0, value.Length, staging, 0);
            staging.AsSpan(0, written).CopyTo(destination.Slice(offset));
            return written;
        }
        finally
        {
            System.Buffers.ArrayPool<byte>.Shared.Return(staging);
        }
#endif
    }
}

/// <summary>
/// Decomposes the fixed per-buffer cost behind <see cref="StringArrayBuildBenchmarks"/>, so the
/// floor can be attributed rather than guessed at.
///
/// <para><c>ArrowBuffer.Builder&lt;T&gt;.Build()</c> goes through <see cref="MemoryAllocator"/>,
/// which over-allocates for 64-byte alignment, zero-fills, and reports GC pressure — then copies
/// on top of the zeros. <c>Apache.Arrow.Memory.NativeBuffer</c> (public since 23.0.0) skips the GC
/// bookkeeping, lets the caller suppress the zero-fill, and transfers ownership without a copy.
/// Running both against a raw malloc/free pair and a managed array shows which of those layers
/// actually costs anything, and at what size.</para>
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 7)]
public class ArrowAllocatorFloorBenchmarks
{
    /// <summary>
    /// 64 B is what a freshly built tiny buffer rounds up to, 1 KB is a small dictionary's payload,
    /// 64 KB is the last size below the large-object-heap threshold.
    /// </summary>
    [Params(64, 1024, 65536)]
    public int Bytes;

    private static readonly MemoryAllocator Allocator = MemoryAllocator.Default.Value;

    /// <summary>Raw unmanaged allocation: no alignment slack, no zeroing, no GC bookkeeping.</summary>
    [Benchmark(Baseline = true)]
    public int RawAllocHGlobal()
    {
        nint p = Marshal.AllocHGlobal(Bytes);
        Marshal.FreeHGlobal(p);
        return Bytes;
    }

    /// <summary>What <c>Build()</c> does per buffer today: align, zero-fill, report GC pressure.</summary>
    [Benchmark]
    public int ArrowMemoryAllocator()
    {
        var owner = Allocator.Allocate(Bytes);
        owner.Dispose();
        return Bytes;
    }

    /// <summary>The public native buffer, zero-filled — isolates the fill against the variant below.</summary>
    [Benchmark]
    public int NativeBufferZeroed()
    {
        var buffer = new NativeBuffer<byte>(Bytes, zeroFill: true);
        buffer.Dispose();
        return Bytes;
    }

    /// <summary>The same, with the fill suppressed — what a fill-once caller should be paying.</summary>
    [Benchmark]
    public int NativeBufferUnzeroed()
    {
        var buffer = new NativeBuffer<byte>(Bytes, zeroFill: false);
        buffer.Dispose();
        return Bytes;
    }

    /// <summary>A managed array of the same size, for reference.</summary>
    [Benchmark]
    public int ManagedArray() => new byte[Bytes].Length;
}
