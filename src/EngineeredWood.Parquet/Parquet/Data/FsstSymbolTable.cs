// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// An FSST symbol table in the wire form the Parquet FSST proposal defines for a
/// SYMBOL_TABLE_PAGE body — one table shared by every FSST data page in a column chunk.
/// </summary>
/// <remarks>
/// <para>The proposal defines two code widths (§2.3): <c>FSST</c> with 8-bit codes and
/// <c>FSST_16</c> with 16-bit ones. They share more than they differ — §4.3's section list,
/// §4.4's 9-byte page header and §4.5's end-offset array are the same bytes either way, and
/// only the symbol table body and the code stream fork. This base class is what carries that:
/// the reader threads one <see cref="FsstSymbolTable"/> through every page-decoding signature
/// and never asks which width it holds. The virtual call lands once per page, not per value.</para>
/// <para>In both widths the body stores a <c>length_histogram</c> rather than a per-symbol
/// length: codes are assigned in ascending length order and the histogram is what lets a
/// reader cut <c>symbol_data</c> back into symbols. Keeping to that order is each subclass's
/// job, because the training algorithm underneath is under no obligation to produce it.</para>
/// </remarks>
internal abstract class FsstSymbolTable
{
    /// <summary>The <c>SymbolTableType</c> this table is written as (§2.3).</summary>
#pragma warning disable EWPARQUET0003 // The enum names a wire constant; opting in is the public caller's choice, not this dispatch's.
    public abstract SymbolTableType Type { get; }
#pragma warning restore EWPARQUET0003

    /// <summary>Number of symbols in the table.</summary>
    public abstract int SymbolCount { get; }

    /// <summary>Size of the serialized page body in bytes.</summary>
    public abstract int SerializedSize { get; }

    /// <summary>
    /// Writes the symbol table page body. <paramref name="destination"/> must be at least
    /// <see cref="SerializedSize"/> bytes.
    /// </summary>
    public abstract int Serialize(Span<byte> destination);

    /// <summary>Serializes the symbol table page body into a new array.</summary>
    public byte[] Serialize()
    {
        var body = new byte[SerializedSize];
        Serialize(body);
        return body;
    }

    /// <summary>
    /// Upper bound on the compressed size of a <paramref name="rawLength"/>-byte input (§5.3).
    /// </summary>
    /// <remarks>
    /// Takes and returns <see cref="long"/> so the multiplication cannot silently wrap for a
    /// column chunk over 1 GB — callers bound the result before allocating.
    /// </remarks>
    public abstract long MaxCompressedLength(long rawLength);

    /// <summary>
    /// Compresses one value into <paramref name="destination"/> and returns the byte count.
    /// Write path only — requires a trained table, not one parsed from a page.
    /// </summary>
    public abstract int Compress(ReadOnlySpan<byte> value, Span<byte> destination);

    /// <summary>
    /// Validates one value's code stream: every code must name a symbol this table has, and
    /// an escape must have its literal (§5.2, §8.3).
    /// </summary>
    public abstract void ValidateCodeStream(ReadOnlySpan<byte> codes);

    /// <summary>
    /// Upper bound on the decompressed size of <paramref name="compressedLength"/> bytes of
    /// code stream.
    /// </summary>
    public abstract int MaxDecompressedLength(int compressedLength);

    /// <summary>
    /// Decompresses a page's values in one call, filling <paramref name="destinationOffsets"/>
    /// with the per-value boundaries the caller turns into an Arrow offsets buffer.
    /// </summary>
    public abstract bool TryDecompressBatch(
        ReadOnlySpan<byte> compressedData,
        ReadOnlySpan<int> compressedLengths,
        Span<byte> destination,
        Span<int> destinationOffsets,
        out int totalWritten);

    /// <summary>
    /// Reads a symbol table page body of the given type, validating every invariant §3.7
    /// requires a reader to check.
    /// </summary>
#pragma warning disable EWPARQUET0003 // Reading a page another writer produced is not an opt-in to writing one.
    public static FsstSymbolTable Parse(SymbolTableType type, ReadOnlySpan<byte> body)
    {
        return type switch
        {
            SymbolTableType.Fsst => FsstSymbolTable8.Parse(body),
            SymbolTableType.Fsst16 => FsstSymbolTable16.Parse(body),
            _ => throw new NotSupportedException(
                $"Symbol table type '{type}' is not defined by the FSST proposal; only FSST " +
                "(8-bit codes) and FSST_16 (16-bit codes) exist."),
        };
    }
#pragma warning restore EWPARQUET0003
}
