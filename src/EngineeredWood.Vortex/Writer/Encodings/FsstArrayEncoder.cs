// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using Apache.Arrow;
using Clast.Fsst;
using EngineeredWood.Encodings;

namespace EngineeredWood.Vortex.Writer.Encodings;

/// <summary>
/// Inverse of <see cref="EngineeredWood.Vortex.Encodings.FsstArrayDecoder"/>:
/// emits a <c>vortex.fsst</c> ArrayNode subtree for repetitive
/// <see cref="StringArray"/> + <see cref="BinaryArray"/> columns. FSST
/// (Fast Static Symbol Table) trains a 1-byte-per-code symbol table on the
/// input bytes, then expresses each value as a sequence of symbol codes.
/// Substrings appearing as symbols shrink to a single byte each; bytes
/// outside the table escape to a literal.
///
/// <para>Wire shape: 3 buffers (symbols at N×8 bytes, symbol_lengths at N×1
/// bytes, compressed_codes_bytes), 2 children (uncompressed_lengths: per-row
/// raw byte length; codes_offsets: VarBin-style cumulative offsets into the
/// codes buffer with <c>n+1</c> entries), or 3 children when nullable
/// (validity bitmap appended). Metadata
/// <c>FSSTMetadata { uncompressed_lengths_ptype, codes_offsets_ptype }</c>.</para>
///
/// <para>Scope: nullable + non-nullable, sliced + non-sliced StringArray
/// AND BinaryArray. FSST trains on raw bytes regardless of UTF-8-ness, so
/// the encode path is identical for both — the column's schema-level
/// dtype determines how the reader interprets the decompressed bytes.
/// Floats deferred.</para>
///
/// <para>Symbol-table extraction uses the public
/// <see cref="SymbolTable.SymbolCount"/> + <see cref="SymbolTable.ExportRaw"/>
/// pair (Clast.Fsst 0.1.3+); the latter is symmetric with
/// <see cref="FsstDecoder.FromSymbols"/> so the round-trip is just
/// <c>ExportRaw(lengths, symbols)</c> → ship buffers → <c>FromSymbols(lengths, symbols)</c>.</para>
/// </summary>
internal static class FsstArrayEncoder
{
    /// <summary>Minimum row count + total-bytes thresholds for the gate. Below
    /// these the symbol-table overhead dwarfs any compression win.</summary>
    private const int MinRows = 32;
    private const int MinTotalBytes = 256;

    public static bool IsApplicable(IArrowArray array)
    {
        // BinaryArray is the base of StringArray; FSST trains on raw bytes
        // regardless of UTF-8-ness, so both shapes flow through the same
        // path. The schema-level dtype determines how the reader interprets
        // the decompressed bytes.
        if (array is not BinaryArray s) return false;
        var data = s.Data;
        int n = s.Length;
        if (n < MinRows) return false;
        int nullCount = data.GetNullCount();
        if (nullCount == n) return false; // all-null — nothing to train

        // Cheap totals probe — skip to BuildSymbolTable + CompressBatch only
        // when there's enough data to make compression plausible. Null rows
        // contribute 0 bytes (the reader's offsets[i] == offsets[i+1] for
        // null rows), so they don't help.
        int total = 0;
        for (int i = 0; i < n; i++) total += s.GetValueLength(i);
        if (total < MinTotalBytes) return false;

        // Build a tentative symbol table on the non-null rows. Null rows
        // would feed empty byte arrays, which the cwida training algorithm
        // tolerates but skews the histogram toward shorter symbols.
        var rowBytes = ExtractRowBytes(s, n);
        var trainingRows = nullCount == 0
            ? rowBytes
            : ExtractNonNullRows(rowBytes, data.Buffers[0].Span, data.Offset, n);
        SymbolTable table;
        try
        {
            table = FsstEncoder.BuildSymbolTable(trainingRows, zeroTerminated: false);
        }
        catch
        {
            return false;
        }
        var (compressed, _) = FsstEncoder.CompressBatch(table, rowBytes);
        long fsstBytes = compressed.LongLength
            + (long)table.SymbolCount * 9 // symbols (8) + symbol_lengths (1) per slot
            + (long)(n + 1) * SmallestUIntElemSize(compressed.Length) // codes_offsets
            + (long)n * SmallestUIntElemSize(MaxRowLength(rowBytes)) // uncompressed_lengths
            + (nullCount > 0 ? (long)(n + 7) / 8 : 0); // validity bitmap
        return fsstBytes * 3 / 2 < total;
    }

    public static int Emit(
        SegmentBuilder sb, IArrowArray array, EncodingIndices idx, int? statsTicket = null)
    {
        if (array is not BinaryArray s)
            throw new NotSupportedException(
                $"vortex.fsst writer requires StringArray or BinaryArray, got {array.GetType().Name}.");
        var data = s.Data;

        int n = s.Length;
        int nullCount = data.GetNullCount();
        bool hasNulls = nullCount > 0;
        var rowBytes = ExtractRowBytes(s, n);

        // 1. Train on non-null rows only (empty rows would skew the
        //    histogram toward short symbols), then compress every row
        //    including null ones — null rows compress to zero bytes since
        //    we feed them as empty arrays.
        var trainingRows = hasNulls
            ? ExtractNonNullRows(rowBytes, data.Buffers[0].Span, data.Offset, n)
            : rowBytes;
        var table = FsstEncoder.BuildSymbolTable(trainingRows, zeroTerminated: false);
        var (compressed, perRowCompressedLengths) = FsstEncoder.CompressBatch(table, rowBytes);

        // 2. Extract the symbol table into vortex's wire shape:
        //    symbols buffer = N × 8 bytes (each Symbol.Val packed LE)
        //    symbol_lengths buffer = N bytes (one length per code)
        var (symbolsBuffer, symbolLengths) = ExtractSymbols(table);

        // 3. Build cumulative codes_offsets (VarBin convention: n+1 entries).
        //    Pick smallest unsigned width that covers the compressed total.
        int totalCompressed = compressed.Length;
        byte codesOffsetsPtype = SmallestUIntPtypeFor(totalCompressed);
        var codesOffsetsArr = BuildOffsets(perRowCompressedLengths, n, codesOffsetsPtype);

        // 4. Build per-row uncompressed_lengths child.
        int maxRowLen = MaxRowLength(rowBytes);
        byte uncompressedLensPtype = SmallestUIntPtypeFor(maxRowLen);
        var uncompressedLensArr = BuildUncompressedLengths(rowBytes, n, uncompressedLensPtype);

        // 5. Register buffers.
        ushort symbolsBufIdx = sb.AddBuffer(symbolsBuffer, alignmentExponent: 3); // align 8 — symbols are u64
        ushort symbolLengthsBufIdx = sb.AddBuffer(symbolLengths, alignmentExponent: 0);
        ushort codesBufIdx = sb.AddBuffer(compressed, alignmentExponent: 0);

        // 6. Encode children. Both go through dispatch with compress=true so
        //    the typically-small monotonic offsets and tightly-bounded
        //    uncompressed_lengths can land on bitpacked / FoR.
        int uncompressedLensTicket = ArrayEncoderDispatch.Emit(
            sb, uncompressedLensArr, idx, statsTicket: null, compress: true);
        int codesOffsetsTicket = ArrayEncoderDispatch.Emit(
            sb, codesOffsetsArr, idx, statsTicket: null, compress: true);

        // 7. Metadata.
        var metadataBytes = SerializeMetadata(uncompressedLensPtype, codesOffsetsPtype);
        int metadataTicket = sb.Builder.WriteByteVector(metadataBytes);

        // Optional 3rd child: validity bitmap as a vortex.bool node.
        var bufIdxs = new[] { symbolsBufIdx, symbolLengthsBufIdx, codesBufIdx };
        int[] children;
        if (hasNulls)
        {
            var validityBitmap = EncoderHelpers.ExtractValidityBitmap(
                data.Buffers[0].Span, srcBitOffset: data.Offset, rowCount: n);
            ushort validityBufIdx = sb.AddBuffer(validityBitmap, alignmentExponent: 0);
            int validityNodeTicket = ArrayNodeEmitter.EmitWithSingleBuffer(
                sb.Builder, idx.Bool, validityBufIdx);
            children = new[] { uncompressedLensTicket, codesOffsetsTicket, validityNodeTicket };
        }
        else
        {
            children = new[] { uncompressedLensTicket, codesOffsetsTicket };
        }

        return statsTicket is null
            ? ArrayNodeEmitter.EmitWithMetadataBuffersAndChildren(
                sb.Builder, idx.FsstString, bufIdxs, metadataTicket, children)
            : ArrayNodeEmitter.EmitWithMetadataBuffersChildrenAndStats(
                sb.Builder, idx.FsstString, bufIdxs, metadataTicket, children, statsTicket.Value);
    }

    private static byte[][] ExtractRowBytes(BinaryArray s, int n)
    {
        var rows = new byte[n][];
        for (int i = 0; i < n; i++)
        {
            // For null rows, hand the compressor an empty byte[] — it
            // contributes 0 bytes to the codes buffer, and the corresponding
            // codes_offsets entry advances by 0, exactly what the reader
            // expects for null rows (validity child masks the empty range).
            if (!s.IsValid(i))
            {
                rows[i] = System.Array.Empty<byte>();
                continue;
            }
            // GetValueLength + GetBytes go through Apache.Arrow's typed
            // accessor, which already honors data.Offset on sliced inputs.
            int len = s.GetValueLength(i);
            rows[i] = s.GetBytes(i).ToArray();
            // Defensive: GetBytes can return a span larger than len when the
            // offsets buffer's slice covers padding. Trim to the typed length.
            if (rows[i].Length != len)
                rows[i] = rows[i].AsSpan(0, len).ToArray();
        }
        return rows;
    }

    /// <summary>
    /// Returns just the non-null rows from <paramref name="rowBytes"/>,
    /// suitable as a training sample. Validity is read from the underlying
    /// bitmap at bit position <c>offset + i</c> (logical row <c>i</c> in the
    /// slice).
    /// </summary>
    private static byte[][] ExtractNonNullRows(
        byte[][] rowBytes, ReadOnlySpan<byte> validity, int offset, int n)
    {
        var nonNull = new List<byte[]>(n);
        for (int i = 0; i < n; i++)
        {
            int bit = offset + i;
            if ((validity[bit >> 3] & (1 << (bit & 7))) != 0)
                nonNull.Add(rowBytes[i]);
        }
        return nonNull.ToArray();
    }

    /// <summary>
    /// Extracts the symbol table into vortex's wire layout via Clast.Fsst's
    /// public <see cref="SymbolTable.ExportRaw"/>: <c>lengths</c> ends up with
    /// <c>SymbolCount</c> bytes (one length per code), <c>symbols</c> with
    /// <c>SymbolCount × 8</c> bytes (each symbol's bytes packed LE,
    /// zero-padded). Parameter order matches <see cref="FsstDecoder.FromSymbols"/>.
    /// </summary>
    private static (byte[] Symbols, byte[] Lengths) ExtractSymbols(SymbolTable table)
    {
        int n = table.SymbolCount;
        var symbols = new byte[n * 8];
        var lengths = new byte[n];
        table.ExportRaw(lengths, symbols);
        return (symbols, lengths);
    }

    /// <summary>
    /// Builds the <c>codes_offsets</c> child as VarBin-style cumulative
    /// offsets — n+1 entries where offsets[0] = 0 and offsets[i+1] = offsets[i]
    /// + perRowCompressedLengths[i]. Width is the smallest unsigned ptype
    /// that covers the total compressed size.
    /// </summary>
    private static IArrowArray BuildOffsets(int[] perRowLengths, int n, byte ptype)
    {
        int len = n + 1;
        int elemSize = ElemSizeForPtype(ptype);
        var bytes = new byte[(long)len * elemSize];
        long cumulative = 0;
        for (int i = 0; i <= n; i++)
        {
            WriteUnsigned(bytes.AsSpan(i * elemSize, elemSize), (ulong)cumulative, elemSize);
            if (i < n) cumulative += perRowLengths[i];
        }
        return BuildUnsignedArray(bytes, len, elemSize);
    }

    private static IArrowArray BuildUncompressedLengths(byte[][] rowBytes, int n, byte ptype)
    {
        int elemSize = ElemSizeForPtype(ptype);
        var bytes = new byte[(long)n * elemSize];
        for (int i = 0; i < n; i++)
            WriteUnsigned(bytes.AsSpan(i * elemSize, elemSize), (ulong)rowBytes[i].Length, elemSize);
        return BuildUnsignedArray(bytes, n, elemSize);
    }

    private static int MaxRowLength(byte[][] rowBytes)
    {
        int max = 0;
        for (int i = 0; i < rowBytes.Length; i++)
            if (rowBytes[i].Length > max) max = rowBytes[i].Length;
        return max;
    }

    private static int SmallestUIntElemSize(int max)
    {
        if (max <= byte.MaxValue) return 1;
        if (max <= ushort.MaxValue) return 2;
        if ((uint)max <= uint.MaxValue) return 4;
        return 8;
    }

    private static byte SmallestUIntPtypeFor(int max) => SmallestUIntElemSize(max) switch
    {
        1 => 0, // U8
        2 => 1, // U16
        4 => 2, // U32
        _ => 3, // U64
    };

    private static int ElemSizeForPtype(byte ptype) => ptype switch
    {
        0 => 1,
        1 => 2,
        2 => 4,
        3 => 8,
        _ => throw new NotSupportedException(),
    };

    private static void WriteUnsigned(Span<byte> dest, ulong value, int elemSize)
    {
        switch (elemSize)
        {
            case 1: dest[0] = (byte)value; break;
            case 2: BinaryPrimitives.WriteUInt16LittleEndian(dest, (ushort)value); break;
            case 4: BinaryPrimitives.WriteUInt32LittleEndian(dest, (uint)value); break;
            case 8: BinaryPrimitives.WriteUInt64LittleEndian(dest, value); break;
            default: throw new NotSupportedException();
        }
    }

    private static IArrowArray BuildUnsignedArray(byte[] bytes, int len, int elemSize)
    {
        var buf = new ArrowBuffer(bytes);
        return elemSize switch
        {
            1 => new UInt8Array(buf, ArrowBuffer.Empty, len, 0, 0),
            2 => new UInt16Array(buf, ArrowBuffer.Empty, len, 0, 0),
            4 => new UInt32Array(buf, ArrowBuffer.Empty, len, 0, 0),
            8 => new UInt64Array(buf, ArrowBuffer.Empty, len, 0, 0),
            _ => throw new NotSupportedException(),
        };
    }

    /// <summary>
    /// FSSTMetadata { uncompressed_lengths_ptype: PType (tag 1),
    /// codes_offsets_ptype: PType (tag 2) }. Both proto enums.
    /// </summary>
    private static byte[] SerializeMetadata(byte uncompressedLensPtype, byte codesOffsetsPtype)
    {
        Span<byte> tmp = stackalloc byte[8];
        int pos = 0;
        tmp[pos++] = 0x08; // tag 1, varint
        pos += Varint.WriteUnsigned(tmp.Slice(pos), uncompressedLensPtype);
        tmp[pos++] = 0x10; // tag 2, varint
        pos += Varint.WriteUnsigned(tmp.Slice(pos), codesOffsetsPtype);
        return tmp.Slice(0, pos).ToArray();
    }
}
