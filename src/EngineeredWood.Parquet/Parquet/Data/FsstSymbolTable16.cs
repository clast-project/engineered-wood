// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// The 16-bit code variant of <see cref="FsstSymbolTable"/> — the proposal's
/// <c>SymbolTableType.FSST_16</c>.
/// </summary>
/// <remarks>
/// <para>Body layout (§3.3, <c>SymbolTableType.FSST_16</c>):</para>
/// <code>
/// +------------------+---------------------------+---------------------+
/// |   symbol_count   |     length_histogram      |     symbol_data     |
/// |  (2 bytes, u16)  |    (32 bytes, u16[16])    |     (variable)      |
/// +------------------+---------------------------+---------------------+
/// </code>
/// <para>Everything is wider than the 8-bit variant: a <c>u16</c> count, sixteen <c>u16</c>
/// histogram slots covering lengths 1..16, <c>symbol_data</c> starting at offset 34, and a
/// code stream of little-endian <c>u16</c> codes where 65,535 is the escape marker followed
/// by one literal byte written as a <c>u16</c> (§4.7, §8.3).</para>
/// <para><b>Reading is liberal, writing is conservative.</b> <see cref="Parse"/> honours all
/// 16 histogram slots, which is what the spec settled on: §1.2 originally said symbols were
/// 1..8 bytes while §3.3, §3.5 and §3.6 described 16, and the author has since clarified that
/// FSST_16 symbols may be 1..16. That disagreement never reached the wire format anyway —
/// §3.3 gives the histogram its 16 slots unconditionally, so a table holding only short symbols
/// is simply one whose entries for lengths 9..16 are zero, which §3.6's reconstruction loop
/// reads without noticing. <see cref="TryTrain"/> emits nothing longer than
/// <see cref="TrainedMaxSymbolLength"/>, which the spec permits and which is a tuning choice
/// rather than a conformance one.</para>
/// </remarks>
internal sealed class FsstSymbolTable16 : FsstSymbolTable
{
    /// <summary>Codes 0..65534 are symbols; 65535 is reserved as the escape marker.</summary>
    public const int MaxSymbols = 65535;

    /// <summary>
    /// Histogram slots, and the width of a <c>symbol_data</c> slot in the raw table — symbols
    /// may be 1..16 bytes as far as a <em>reader</em> is concerned (§3.3, §3.5, §3.6).
    /// </summary>
    public const int MaxSymbolLength = 16;

    /// <summary>
    /// Longest symbol this writer will train. The spec allows up to
    /// <see cref="MaxSymbolLength"/>; emitting less is conformant, because a table whose
    /// histogram entries for lengths 9..16 are zero is an ordinary FSST_16 table.
    /// </summary>
    /// <remarks>
    /// <b>Measured, not cautious — do not raise this to 16 without re-measuring.</b> A longer
    /// cap is not reliably better: on a URL corpus, ratio runs 3.54x at 14, 2.01x at 15 and
    /// 1.41x at 16 — worse than this cap's 1.88x — at an essentially unchanged symbol count.
    /// A greedy trainer allowed *longer* symbols should not lose 60%, since it can still choose
    /// shorter ones, so the erratic response looks like cap sensitivity in Clast.Fsst's FSST16
    /// trainer. 8 is the stable choice until that is understood; the table of measurements is
    /// in <c>doc/parquet-fsst.md</c>.
    /// </remarks>
    public const int TrainedMaxSymbolLength = 8;

    /// <summary>Code that introduces a literal byte rather than a symbol.</summary>
    public const ushort EscapeCode = 0xFFFF;

    /// <summary>
    /// Bytes of fixed header: <c>symbol_count</c> plus <c>length_histogram</c>, so
    /// <c>symbol_data</c> begins at offset 34.
    /// </summary>
    public const int BodyHeaderSize = 2 + (2 * MaxSymbolLength);

    /// <summary>Per-code symbol length, ascending — the spec's code order.</summary>
    private readonly byte[] _lengths;

    /// <summary>Per-code symbol bytes in 16-byte slots, matching Clast.Fsst's raw layout.</summary>
    private readonly byte[] _symbols;

    /// <summary>Set on the write path only: the trained table the compressor needs.</summary>
    private readonly Clast.Fsst.SymbolTable16? _trained;

    /// <summary>
    /// Set on the write path only, and only when the trainer's own code order was not already
    /// ascending by length: trained code → spec code.
    /// </summary>
    private readonly ushort[]? _remap;

    private Clast.Fsst.Fsst16Decoder? _decoder;

    private FsstSymbolTable16(
        byte[] lengths, byte[] symbols, Clast.Fsst.SymbolTable16? trained, ushort[]? remap)
    {
        _lengths = lengths;
        _symbols = symbols;
        _trained = trained;
        _remap = remap;
    }

    /// <inheritdoc/>
#pragma warning disable EWPARQUET0003 // The enum value names a wire constant; writing one is the caller's opt-in.
    public override SymbolTableType Type => SymbolTableType.Fsst16;
#pragma warning restore EWPARQUET0003

    /// <inheritdoc/>
    public override int SymbolCount => _lengths.Length;

    /// <inheritdoc/>
    public override int SerializedSize
    {
        get
        {
            int size = BodyHeaderSize;
            foreach (byte len in _lengths) size += len;
            return size;
        }
    }

    /// <summary>
    /// Trains a symbol table over <paramref name="values"/>, or returns <see langword="null"/>
    /// when no usable table comes out — an empty corpus, or a table the 16-bit code space
    /// cannot address. Callers fall back to another encoding rather than failing the write.
    /// </summary>
    public static FsstSymbolTable16? TryTrain(ReadOnlySpan<byte[]> values)
    {
        if (values.Length == 0)
            return null;

        Clast.Fsst.SymbolTable16 trained;
        try
        {
            trained = Clast.Fsst.Fsst16Encoder.BuildSymbolTable(
                values, maxSymbolLength: TrainedMaxSymbolLength);
        }
        catch (Exception)
        {
            // Training is a heuristic over the caller's bytes; a corpus it cannot handle is a
            // reason to pick another encoding, not to fail the write.
            return null;
        }

        int count = trained.SymbolCount;

        // The escape marker owns code 65535, so a table that fills the code space has nowhere
        // to put its last symbol. Clast.Fsst caps itself below this; the check is here so a
        // future version relaxing that cap degrades to another encoding rather than writing
        // codes a reader would have to interpret as escapes.
        if (count <= 0 || count > MaxSymbols)
            return null;

        var rawLengths = new byte[count];
        var rawSymbols = new byte[count * MaxSymbolLength];
        trained.ExportRaw(rawLengths, rawSymbols);

        foreach (byte len in rawLengths)
        {
            if (len is < 1 or > MaxSymbolLength)
                return null;
        }

        ArrangeByLength(rawLengths, rawSymbols, out var lengths, out var symbols, out var remap);
        return new FsstSymbolTable16(lengths, symbols, trained, remap);
    }

    /// <summary>
    /// Puts the trained table into the ascending-length code order §3.3 requires, producing a
    /// remap from trained code to spec code. <paramref name="remap"/> is
    /// <see langword="null"/> — and the outputs alias the inputs — when the table was already
    /// in that order, which is the only case that happens in practice.
    /// </summary>
    /// <remarks>
    /// <para>Clast.Fsst's 16-bit trainer assigns codes in ascending length order already, so
    /// the renumbering path below never runs against it — measured over 48 trained tables, not
    /// one came back out of order. It is kept because the histogram <em>is</em> the length
    /// information: a trainer that quietly stopped promising that order would otherwise produce
    /// tables no reader could cut apart, and the failure would be silent corruption rather than
    /// an error.</para>
    /// <para>That is also why this is a separate method rather than an inline branch — it is
    /// the one piece of this class that production data cannot reach, so a test has to reach it
    /// directly instead.</para>
    /// </remarks>
    internal static void ArrangeByLength(
        byte[] rawLengths, byte[] rawSymbols,
        out byte[] lengths, out byte[] symbols, out ushort[]? remap)
    {
        int count = rawLengths.Length;

        bool ascending = true;
        for (int code = 1; code < count; code++)
        {
            if (rawLengths[code] < rawLengths[code - 1])
            {
                ascending = false;
                break;
            }
        }

        if (ascending)
        {
            lengths = rawLengths;
            symbols = rawSymbols;
            remap = null;
            return;
        }

        Span<int> histogram = stackalloc int[MaxSymbolLength + 1];
        foreach (byte len in rawLengths)
            histogram[len]++;

        Span<int> nextCode = stackalloc int[MaxSymbolLength + 1];
        int running = 0;
        for (int len = 1; len <= MaxSymbolLength; len++)
        {
            nextCode[len] = running;
            running += histogram[len];
        }

        lengths = new byte[count];
        symbols = new byte[count * MaxSymbolLength];

        // Indexed by trained code, so it must span the whole code space; the escape marker maps
        // to itself because it is not a symbol and must survive Compress untouched.
        var built = new ushort[MaxSymbols + 1];
        built[EscapeCode] = EscapeCode;

        for (int oldCode = 0; oldCode < count; oldCode++)
        {
            byte len = rawLengths[oldCode];
            int newCode = nextCode[len]++;
            lengths[newCode] = len;
            rawSymbols.AsSpan(oldCode * MaxSymbolLength, MaxSymbolLength)
                .CopyTo(symbols.AsSpan(newCode * MaxSymbolLength, MaxSymbolLength));
            built[oldCode] = (ushort)newCode;
        }

        remap = built;
    }

    /// <inheritdoc/>
    public override int Serialize(Span<byte> destination)
    {
        int size = SerializedSize;
        if (destination.Length < size)
            throw new ArgumentException(
                $"FSST_16 symbol table needs {size} bytes, got {destination.Length}.", nameof(destination));

        BinaryPrimitives.WriteUInt16LittleEndian(destination, (ushort)SymbolCount);

        var histogram = destination.Slice(2, 2 * MaxSymbolLength);
        histogram.Clear();
        foreach (byte len in _lengths)
        {
            var slot = histogram.Slice((len - 1) * 2, 2);
            BinaryPrimitives.WriteUInt16LittleEndian(
                slot, (ushort)(BinaryPrimitives.ReadUInt16LittleEndian(slot) + 1));
        }

        // Symbols are already in ascending-length code order, so a straight walk emits the
        // ascending length blocks the histogram describes.
        int pos = BodyHeaderSize;
        for (int code = 0; code < _lengths.Length; code++)
        {
            byte len = _lengths[code];
            _symbols.AsSpan(code * MaxSymbolLength, len).CopyTo(destination.Slice(pos, len));
            pos += len;
        }

        return pos;
    }

    /// <summary>
    /// Reads a symbol table page body, validating every invariant §3.7 requires a reader
    /// to check.
    /// </summary>
    public static FsstSymbolTable16 Parse(ReadOnlySpan<byte> body)
    {
        if (body.Length < BodyHeaderSize)
            throw new ParquetFormatException(
                $"FSST_16 symbol table page is {body.Length} bytes, too small for the " +
                $"{BodyHeaderSize}-byte header.");

        int symbolCount = BinaryPrimitives.ReadUInt16LittleEndian(body);

        // Widened deliberately: §3.7 requires arithmetic wide enough that a hostile histogram
        // cannot wrap the sum past the length check. Sixteen u16 slots can describe just over
        // 16 MB of symbol data, which overflows nothing here but would in 32-bit arithmetic
        // once multiplied out by a caller reusing this shape.
        long histogramSum = 0;
        long symbolDataSize = 0;
        for (int i = 0; i < MaxSymbolLength; i++)
        {
            int n = BinaryPrimitives.ReadUInt16LittleEndian(body.Slice(2 + (i * 2), 2));
            histogramSum += n;
            symbolDataSize += (long)n * (i + 1);
        }

        if (histogramSum != symbolCount)
            throw new ParquetFormatException(
                $"FSST_16 symbol table length_histogram sums to {histogramSum} but symbol_count " +
                $"is {symbolCount}.");

        long expectedLength = BodyHeaderSize + symbolDataSize;
        if (body.Length != expectedLength)
            throw new ParquetFormatException(
                $"FSST_16 symbol table page is {body.Length} bytes but its histogram describes " +
                $"{expectedLength} ({symbolDataSize} bytes of symbol data plus a {BodyHeaderSize}-byte header).");

        var lengths = new byte[symbolCount];
        var symbols = new byte[symbolCount * MaxSymbolLength];

        int code = 0;
        int pos = BodyHeaderSize;
        for (int i = 0; i < MaxSymbolLength; i++)
        {
            int len = i + 1;
            for (int n = BinaryPrimitives.ReadUInt16LittleEndian(body.Slice(2 + (i * 2), 2)); n > 0; n--)
            {
                lengths[code] = (byte)len;
                body.Slice(pos, len).CopyTo(symbols.AsSpan(code * MaxSymbolLength, len));
                pos += len;
                code++;
            }
        }

        return new FsstSymbolTable16(lengths, symbols, trained: null, remap: null);
    }

    /// <inheritdoc/>
    /// <remarks>
    /// Every byte escaping to a two-byte marker plus a two-byte literal (§5.3) — 4x, where the
    /// 8-bit variant's worst case is 2x.
    /// </remarks>
    public override long MaxCompressedLength(long rawLength) => rawLength * 4;

    /// <inheritdoc/>
    public override int Compress(ReadOnlySpan<byte> value, Span<byte> destination)
    {
        if (_trained is null)
            throw new InvalidOperationException(
                "This FSST_16 symbol table was parsed from a page and cannot compress; train one instead.");

        if (value.Length == 0)
            return 0;

        if (!Clast.Fsst.Fsst16Encoder.TryCompress(_trained, value, destination, out int written))
            throw new InvalidOperationException(
                $"FSST_16 compression needed more than the {destination.Length} bytes provided " +
                $"for a {value.Length}-byte value.");

        if ((written & 1) != 0)
            throw new InvalidOperationException(
                $"FSST_16 compression produced {written} bytes, which is not a whole number of " +
                "16-bit codes.");

        // Renumber trained codes into the spec's ascending-length codes. Escapes are copied
        // through with their literal, which must not be remapped — it is a raw byte widened to
        // 16 bits, not a code.
        var remap = _remap;
        if (remap != null)
        {
            for (int i = 0; i < written; i += 2)
            {
                var slot = destination.Slice(i, 2);
                ushort c = BinaryPrimitives.ReadUInt16LittleEndian(slot);
                if (c == EscapeCode)
                    i += 2;
                else
                    BinaryPrimitives.WriteUInt16LittleEndian(slot, remap[c]);
            }
        }

        return written;
    }

    /// <inheritdoc/>
    public override void ValidateCodeStream(ReadOnlySpan<byte> codes)
    {
        if ((codes.Length & 1) != 0)
            throw new ParquetFormatException(
                $"FSST_16 value is {codes.Length} bytes, which is not a whole number of 16-bit codes.");

        int symbolCount = SymbolCount;
        int i = 0;
        while (i < codes.Length)
        {
            ushort c = BinaryPrimitives.ReadUInt16LittleEndian(codes.Slice(i, 2));
            if (c == EscapeCode)
            {
                if (i + 4 > codes.Length)
                    throw new ParquetFormatException(
                        "FSST_16 value ends with an escape marker and no literal.");

                ushort literal = BinaryPrimitives.ReadUInt16LittleEndian(codes.Slice(i + 2, 2));
                if (literal > byte.MaxValue)
                    throw new ParquetFormatException(
                        $"FSST_16 escape literal is {literal}, which is not a byte value (§8.3).");

                i += 4;
            }
            else
            {
                if (c >= symbolCount)
                    throw new ParquetFormatException(
                        $"FSST_16 value uses symbol code {c} but the table has only {symbolCount} symbols.");
                i += 2;
            }
        }
    }

    /// <inheritdoc/>
    public override int MaxDecompressedLength(int compressedLength) =>
        Clast.Fsst.Fsst16Decoder.MaxDecompressedLength(compressedLength);

    /// <inheritdoc/>
    public override bool TryDecompressBatch(
        ReadOnlySpan<byte> compressedData,
        ReadOnlySpan<int> compressedLengths,
        Span<byte> destination,
        Span<int> destinationOffsets,
        out int totalWritten) =>
        Decoder.TryDecompressBatch(
            compressedData, compressedLengths, destination, destinationOffsets, out totalWritten);

    /// <summary>Decoder over this table's symbols, built once and reused across pages.</summary>
    private Clast.Fsst.Fsst16Decoder Decoder =>
        _decoder ??= Clast.Fsst.Fsst16Decoder.FromSymbols(_lengths, _symbols);
}
