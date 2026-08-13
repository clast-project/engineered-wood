// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// How a FSST data page encodes its offset array (the page header's
/// <c>offset_encoding</c> byte).
/// </summary>
internal enum FsstOffsetEncoding : byte
{
    /// <summary>One little-endian int32 per value.</summary>
    Plain = 0,

    /// <summary>DELTA_BINARY_PACKED over the end offsets.</summary>
    DeltaBinaryPacked = 1,
}

/// <summary>
/// A whole column chunk's values compressed against one trained symbol table.
/// </summary>
/// <remarks>
/// <para>FSST's phase-1 design puts the symbol table in a page shared by the whole column
/// chunk (§1.4), so the table cannot be known until every value has been seen. Compression
/// therefore happens once, here, rather than per page: the pages that follow are slices of
/// <see cref="Payload"/>, which also makes the "is FSST actually smaller?" question §7.5
/// asks answerable before a single page is written.</para>
/// </remarks>
internal sealed class FsstCompressedColumn
{
    /// <summary>
    /// Largest array the runtime will allocate (<c>Array.MaxLength</c>, spelled out because
    /// netstandard2.0 does not expose it).
    /// </summary>
    private const long MaxArrayLength = 0x7FFFFFC7;

    private FsstCompressedColumn(FsstSymbolTable table, byte[] payload, int[] endOffsets)
    {
        Table = table;
        Payload = payload;
        EndOffsets = endOffsets;
    }

    /// <summary>The trained table, to be written as the chunk's symbol table page.</summary>
    public FsstSymbolTable Table { get; }

    /// <summary>Every value's compressed bytes, concatenated.</summary>
    public byte[] Payload { get; }

    /// <summary>Cumulative end offsets into <see cref="Payload"/>; <c>n + 1</c> entries.</summary>
    public int[] EndOffsets { get; }

    /// <summary>Total compressed bytes across the chunk.</summary>
    public int CompressedLength => EndOffsets[EndOffsets.Length - 1];

    /// <summary>
    /// Trains a symbol table over <paramref name="values"/> and compresses them, or returns
    /// <see langword="null"/> when FSST is not worth using for this column chunk — no
    /// trainable corpus, or output that does not beat storing the bytes as they are.
    /// </summary>
#pragma warning disable EWPARQUET0003 // Caller has opted in via the experimental enum value; internal dispatch should not re-warn.
    public static FsstCompressedColumn? TryCompress(
        ReadOnlySpan<byte[]> values, SymbolTableType symbolTableType)
    {
        FsstSymbolTable? table = symbolTableType switch
        {
            SymbolTableType.Fsst => FsstSymbolTable8.TryTrain(values),
            SymbolTableType.Fsst16 => FsstSymbolTable16.TryTrain(values),
            _ => null,
        };

        if (table is null)
            return null;

        long rawBytes = 0;
        foreach (var value in values)
            rawBytes += value.Length;

        // §5.3: worst case is every byte escaping — to a two-byte sequence for 8-bit codes, a
        // four-byte one for 16-bit. Accumulated and scaled in long, because an int would wrap
        // negative somewhere past a 1 GB chunk and turn a size question into an allocation
        // crash. A chunk whose worst case will not fit an array is simply one FSST declines —
        // the caller already has a fallback for that, and it is the same answer as "FSST did
        // not shrink this".
        long worstCase = table.MaxCompressedLength(rawBytes);
        if (worstCase > MaxArrayLength)
            return null;

        var payload = new byte[(int)worstCase];
        var endOffsets = new int[values.Length + 1];

        int written = 0;
        for (int i = 0; i < values.Length; i++)
        {
            written += table.Compress(values[i], payload.AsSpan(written));
            endOffsets[i + 1] = written;
        }

        // §7.5: fall back rather than write a page FSST made bigger. The symbol table page
        // rides along on every chunk that uses the encoding, so it counts against the win.
        if (written + table.SerializedSize >= rawBytes)
            return null;

        return new FsstCompressedColumn(table, payload, endOffsets);
    }
#pragma warning restore EWPARQUET0003
}

/// <summary>
/// Builds the values section of an FSST data page from a slice of an already-compressed
/// column. Mirror of <see cref="FsstPageDecoder"/>.
/// </summary>
/// <remarks>
/// Page body (§4.3):
/// <code>
/// +-------------+----------------------+---------------------------------+
/// | FSST header |     Offset array     |          Data section           |
/// |  (9 bytes)  |      (variable)      |           (variable)            |
/// +-------------+----------------------+---------------------------------+
/// </code>
/// The offset array holds one cumulative END offset per value, measured from the start of
/// this page's data section, which is what gives a reader O(1) access to any single value.
/// The symbol table is not part of the page — it lives in the chunk's SYMBOL_TABLE_PAGE.
/// </remarks>
internal static class FsstPageEncoder
{
    /// <summary>offset_encoding (1) + num_values (4) + offset_array_length (4).</summary>
    public const int HeaderSize = 9;

    /// <summary>
    /// Encodes the <paramref name="count"/> values starting at <paramref name="valueIndex"/>
    /// into one FSST page body.
    /// </summary>
    public static byte[] Encode(FsstCompressedColumn column, int valueIndex, int count)
    {
        var columnOffsets = column.EndOffsets;
        int payloadStart = columnOffsets[valueIndex];
        int payloadEnd = columnOffsets[valueIndex + count];
        int payloadLength = payloadEnd - payloadStart;

        // Offsets are relative to this page's data section, not the column's payload.
        var endOffsets = new int[count];
        for (int i = 0; i < count; i++)
            endOffsets[i] = columnOffsets[valueIndex + 1 + i] - payloadStart;

        // The header carries offset_encoding precisely so the writer can choose per page.
        // Delta beats plain on any page whose values are short enough that four bytes of
        // offset per value is a real cost — which is most of them, but it is measured here
        // rather than assumed.
        byte[] offsetBytes = EncodePlainOffsets(endOffsets);
        var offsetEncoding = FsstOffsetEncoding.Plain;

        var deltaEncoder = new DeltaBinaryPackedEncoder(Math.Max(64, count));
        deltaEncoder.EncodeInt32s(endOffsets);
        if (deltaEncoder.Length < offsetBytes.Length)
        {
            offsetBytes = deltaEncoder.ToArray();
            offsetEncoding = FsstOffsetEncoding.DeltaBinaryPacked;
        }

        var page = new byte[HeaderSize + offsetBytes.Length + payloadLength];
        page[0] = (byte)offsetEncoding;
        BinaryPrimitives.WriteInt32LittleEndian(page.AsSpan(1, 4), count);
        BinaryPrimitives.WriteInt32LittleEndian(page.AsSpan(5, 4), offsetBytes.Length);
        offsetBytes.CopyTo(page.AsSpan(HeaderSize));
        column.Payload.AsSpan(payloadStart, payloadLength)
            .CopyTo(page.AsSpan(HeaderSize + offsetBytes.Length));

        return page;
    }

    private static byte[] EncodePlainOffsets(ReadOnlySpan<int> endOffsets)
    {
        var bytes = new byte[endOffsets.Length * 4];
        for (int i = 0; i < endOffsets.Length; i++)
            BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(i * 4, 4), endOffsets[i]);
        return bytes;
    }
}
