// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// An FSST symbol table in the wire form the Parquet FSST proposal defines for a
/// SYMBOL_TABLE_PAGE body — one table shared by every FSST data page in a column chunk.
/// </summary>
/// <remarks>
/// <para>Body layout (§3.3, <c>SymbolTableType.FSST</c>):</para>
/// <code>
/// +------------------+---------------------------+---------------------+
/// |   symbol_count   |     length_histogram      |     symbol_data     |
/// |   (1 byte, u8)   |     (8 bytes, u8[8])      |     (variable)      |
/// +------------------+---------------------------+---------------------+
/// </code>
/// <para><c>length_histogram[i]</c> is the number of symbols of length <c>i+1</c>, so the
/// lengths are not stored per symbol: codes are assigned in ascending length order and the
/// histogram is what lets a reader cut <c>symbol_data</c> back into symbols. That ordering
/// is the reason this type exists rather than the raw table Clast.Fsst hands back — the
/// training algorithm emits codes in gain order, so <see cref="TryTrain"/> renumbers them
/// and carries a remap that <see cref="Compress"/> applies to every emitted code.</para>
/// <para>Codes 0..254 are symbols and 255 is the escape marker, which is followed by one
/// literal byte. This is the same convention Clast.Fsst uses internally, so escapes pass
/// through the remap untouched.</para>
/// </remarks>
internal sealed class FsstSymbolTable
{
    /// <summary>Codes 0..254 are symbols; 255 is reserved as the escape marker.</summary>
    public const int MaxSymbols = 255;

    /// <summary>Symbols are 1..8 bytes for the 8-bit code variant.</summary>
    public const int MaxSymbolLength = 8;

    /// <summary>Code that introduces a one-byte literal rather than a symbol.</summary>
    public const byte EscapeCode = 0xFF;

    /// <summary>Bytes of fixed header: <c>symbol_count</c> plus <c>length_histogram</c>.</summary>
    public const int BodyHeaderSize = 1 + MaxSymbolLength;

    /// <summary>Per-code symbol length, ascending — the spec's code order.</summary>
    private readonly byte[] _lengths;

    /// <summary>Per-code symbol bytes in 8-byte slots, matching Clast.Fsst's raw layout.</summary>
    private readonly byte[] _symbols;

    /// <summary>Set on the write path only: the trained table the compressor needs.</summary>
    private readonly Clast.Fsst.SymbolTable? _trained;

    /// <summary>Set on the write path only: trained code → spec code.</summary>
    private readonly byte[]? _remap;

    private Clast.Fsst.FsstDecoder? _decoder;

    private FsstSymbolTable(
        byte[] lengths, byte[] symbols, Clast.Fsst.SymbolTable? trained, byte[]? remap)
    {
        _lengths = lengths;
        _symbols = symbols;
        _trained = trained;
        _remap = remap;
    }

    /// <summary>Number of symbols in the table.</summary>
    public int SymbolCount => _lengths.Length;

    /// <summary>Size of the serialized page body in bytes.</summary>
    public int SerializedSize
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
    /// when no usable table comes out — an empty corpus, or a table the 8-bit code space
    /// cannot address. Callers fall back to another encoding rather than failing the write.
    /// </summary>
    public static FsstSymbolTable? TryTrain(ReadOnlySpan<byte[]> values)
    {
        if (values.Length == 0)
            return null;

        Clast.Fsst.SymbolTable trained;
        try
        {
            trained = Clast.Fsst.FsstEncoder.BuildSymbolTable(values, zeroTerminated: false);
        }
        catch (Exception)
        {
            // Training is a heuristic over the caller's bytes; a corpus it cannot handle is a
            // reason to pick another encoding, not to fail the write.
            return null;
        }

        int count = trained.SymbolCount;

        // The escape marker owns code 255, so a table that fills the code space has nowhere to
        // put its last symbol. Clast.Fsst caps itself below this; the check is here so a future
        // version relaxing that cap degrades to another encoding instead of writing bad codes.
        if (count <= 0 || count > MaxSymbols)
            return null;

        var rawLengths = new byte[count];
        var rawSymbols = new byte[count * MaxSymbolLength];
        trained.ExportRaw(rawLengths, rawSymbols);

        // Codes are assigned in ascending length order (§3.3). Counting-sort the trained codes
        // by length — stable within a length, so the order is a pure function of the table.
        Span<int> histogram = stackalloc int[MaxSymbolLength + 1];
        foreach (byte len in rawLengths)
        {
            if (len is < 1 or > MaxSymbolLength)
                return null;
            histogram[len]++;
        }

        Span<int> nextCode = stackalloc int[MaxSymbolLength + 1];
        int running = 0;
        for (int len = 1; len <= MaxSymbolLength; len++)
        {
            nextCode[len] = running;
            running += histogram[len];
        }

        var lengths = new byte[count];
        var symbols = new byte[count * MaxSymbolLength];
        var remap = new byte[256];
        remap[EscapeCode] = EscapeCode;

        for (int oldCode = 0; oldCode < count; oldCode++)
        {
            byte len = rawLengths[oldCode];
            int newCode = nextCode[len]++;
            lengths[newCode] = len;
            rawSymbols.AsSpan(oldCode * MaxSymbolLength, MaxSymbolLength)
                .CopyTo(symbols.AsSpan(newCode * MaxSymbolLength, MaxSymbolLength));
            remap[oldCode] = (byte)newCode;
        }

        return new FsstSymbolTable(lengths, symbols, trained, remap);
    }

    /// <summary>
    /// Writes the symbol table page body. <paramref name="destination"/> must be at least
    /// <see cref="SerializedSize"/> bytes.
    /// </summary>
    public int Serialize(Span<byte> destination)
    {
        int size = SerializedSize;
        if (destination.Length < size)
            throw new ArgumentException(
                $"FSST symbol table needs {size} bytes, got {destination.Length}.", nameof(destination));

        destination[0] = (byte)SymbolCount;

        var histogram = destination.Slice(1, MaxSymbolLength);
        histogram.Clear();
        foreach (byte len in _lengths)
            histogram[len - 1]++;

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

    /// <summary>Serializes the symbol table page body into a new array.</summary>
    public byte[] Serialize()
    {
        var body = new byte[SerializedSize];
        Serialize(body);
        return body;
    }

    /// <summary>
    /// Reads a symbol table page body, validating every invariant §3.7 requires a reader
    /// to check.
    /// </summary>
    public static FsstSymbolTable Parse(ReadOnlySpan<byte> body)
    {
        if (body.Length < BodyHeaderSize)
            throw new ParquetFormatException(
                $"FSST symbol table page is {body.Length} bytes, too small for the {BodyHeaderSize}-byte header.");

        int symbolCount = body[0];

        // Widened deliberately: §3.7 requires arithmetic wide enough that a hostile histogram
        // cannot wrap the sum past the length check.
        long histogramSum = 0;
        long symbolDataSize = 0;
        for (int i = 0; i < MaxSymbolLength; i++)
        {
            int n = body[1 + i];
            histogramSum += n;
            symbolDataSize += (long)n * (i + 1);
        }

        if (histogramSum != symbolCount)
            throw new ParquetFormatException(
                $"FSST symbol table length_histogram sums to {histogramSum} but symbol_count is {symbolCount}.");

        long expectedLength = BodyHeaderSize + symbolDataSize;
        if (body.Length != expectedLength)
            throw new ParquetFormatException(
                $"FSST symbol table page is {body.Length} bytes but its histogram describes " +
                $"{expectedLength} ({symbolDataSize} bytes of symbol data plus a {BodyHeaderSize}-byte header).");

        var lengths = new byte[symbolCount];
        var symbols = new byte[symbolCount * MaxSymbolLength];

        int code = 0;
        int pos = BodyHeaderSize;
        for (int i = 0; i < MaxSymbolLength; i++)
        {
            int len = i + 1;
            for (int n = body[1 + i]; n > 0; n--)
            {
                lengths[code] = (byte)len;
                body.Slice(pos, len).CopyTo(symbols.AsSpan(code * MaxSymbolLength, len));
                pos += len;
                code++;
            }
        }

        return new FsstSymbolTable(lengths, symbols, trained: null, remap: null);
    }

    /// <summary>
    /// Upper bound on the compressed size of a <paramref name="rawLength"/>-byte input:
    /// every byte escaping to a two-byte sequence (§5.3).
    /// </summary>
    /// <remarks>
    /// Takes and returns <see cref="long"/> so the doubling cannot silently wrap for a
    /// column chunk over 1 GB — callers bound the result before allocating.
    /// </remarks>
    public static long MaxCompressedLength(long rawLength) => rawLength * 2;

    /// <summary>
    /// Compresses one value into <paramref name="destination"/> and returns the byte count.
    /// Write path only — requires a table from <see cref="TryTrain"/>.
    /// </summary>
    public int Compress(ReadOnlySpan<byte> value, Span<byte> destination)
    {
        if (_trained is null || _remap is null)
            throw new InvalidOperationException(
                "This FSST symbol table was parsed from a page and cannot compress; train one instead.");

        if (value.Length == 0)
            return 0;

        if (!Clast.Fsst.FsstEncoder.TryCompress(_trained, value, destination, out int written))
            throw new InvalidOperationException(
                $"FSST compression needed more than the {destination.Length} bytes provided " +
                $"for a {value.Length}-byte value.");

        // Renumber trained codes into the spec's ascending-length codes. Escapes are copied
        // through with their literal, which must not be remapped — it is a raw byte, not a code.
        var remap = _remap;
        for (int i = 0; i < written; i++)
        {
            byte c = destination[i];
            if (c == EscapeCode)
                i++;
            else
                destination[i] = remap[c];
        }

        return written;
    }

    /// <summary>
    /// Validates one value's code stream: every code must name a symbol this table has, and
    /// an escape must have its literal (§5.2, §8.3).
    /// </summary>
    public void ValidateCodeStream(ReadOnlySpan<byte> codes)
    {
        int symbolCount = SymbolCount;
        int i = 0;
        while (i < codes.Length)
        {
            byte c = codes[i];
            if (c == EscapeCode)
            {
                if (i + 1 >= codes.Length)
                    throw new ParquetFormatException(
                        "FSST value ends with an escape marker and no literal byte.");
                i += 2;
            }
            else
            {
                if (c >= symbolCount)
                    throw new ParquetFormatException(
                        $"FSST value uses symbol code {c} but the table has only {symbolCount} symbols.");
                i++;
            }
        }
    }

    /// <summary>Decoder over this table's symbols, built once and reused across pages.</summary>
    public Clast.Fsst.FsstDecoder Decoder =>
        _decoder ??= Clast.Fsst.FsstDecoder.FromSymbols(_lengths, _symbols);
}
