// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using System.IO;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Vortex.Encodings;
using EngineeredWood.Vortex.Format;
using EngineeredWood.Vortex.Layouts;
using EngineeredWood.Vortex.Writer.Encodings;

namespace EngineeredWood.Vortex.Writer;

/// <summary>
/// Vortex writer. Produces a single Vortex file from one or more Arrow
/// <see cref="RecordBatch"/>es. Single-batch files use a
/// <c>vortex.struct(vortex.flat × N)</c> layout; multi-batch files use
/// <c>vortex.struct(vortex.chunked(vortex.flat × M) × N)</c>.
///
/// <para>Usage:
/// <code>
/// using var w = new VortexFileWriter(stream, schema);
/// w.WriteBatch(batch1);
/// w.WriteBatch(batch2);
/// w.Close(); // (Dispose also closes if not already)
/// </code></para>
///
/// <para>File layout:
/// <c>[VTXF magic][batch_0 segments][batch_1 segments]...[DType FB][Layout FB][Footer FB][Postscript FB][EndOfFile struct]</c>
/// where EndOfFile = <c>version:u16 | postscript_len:u16 | "VTXF"</c>.</para>
///
/// <para>Phase 2 scope: primitive (Int8..Int64, UInt8..UInt64, Float32/64, Bool)
/// and Utf8/Binary columns, nullable + non-nullable, sliced inputs. Multi-batch
/// streaming. Compressing encodings, lists, decimals, dicts, and zone-pruning
/// stats are deferred to later phases.</para>
/// </summary>
public sealed class VortexFileWriter : IDisposable
{
    // Array-spec registry constants.
    private const ushort PrimitiveEncodingIdx = 0;
    private const ushort BoolEncodingIdx = 1;
    private const ushort VarBinEncodingIdx = 2;
    private const ushort ListEncodingIdx = 3;
    private const ushort FixedSizeListEncodingIdx = 4;
    private const ushort BitPackedEncodingIdx = 5;
    private const ushort DecimalEncodingIdx = 6;
    private const ushort ConstantEncodingIdx = 7;
    private const ushort ForEncodingIdx = 8;
    private const ushort DeltaEncodingIdx = 9;
    private const ushort DictEncodingIdx = 10;
    private const ushort RleEncodingIdx = 11;
    private const ushort StructEncodingIdx = 12;
    private const ushort AlpEncodingIdx = 13;
    private const ushort RunEndEncodingIdx = 14;
    private const ushort SparseEncodingIdx = 15;
    private const ushort FsstStringEncodingIdx = 16;
    private const ushort AlpRdEncodingIdx = 17;
    private const ushort VarBinViewEncodingIdx = 18;
    private const ushort PcoEncodingIdx = 19;
    private static readonly EncodingIndices Indices = new(
        Primitive: PrimitiveEncodingIdx,
        Bool: BoolEncodingIdx,
        VarBin: VarBinEncodingIdx,
        List: ListEncodingIdx,
        FixedSizeList: FixedSizeListEncodingIdx,
        BitPacked: BitPackedEncodingIdx,
        Decimal: DecimalEncodingIdx,
        Constant: ConstantEncodingIdx,
        For: ForEncodingIdx,
        Delta: DeltaEncodingIdx,
        Dict: DictEncodingIdx,
        Rle: RleEncodingIdx,
        Struct_: StructEncodingIdx,
        Alp: AlpEncodingIdx,
        RunEnd: RunEndEncodingIdx,
        Sparse: SparseEncodingIdx,
        FsstString: FsstStringEncodingIdx,
        AlpRd: AlpRdEncodingIdx,
        VarBinView: VarBinViewEncodingIdx,
        Pco: PcoEncodingIdx);

    // Layout-spec registry constants.
    private const ushort FlatLayoutIdx = 0;
    private const ushort StructLayoutIdx = 1;
    private const ushort ChunkedLayoutIdx = 2;
    private const ushort StatsLayoutIdx = 3;

    private readonly Stream _stream;
    private readonly Apache.Arrow.Schema _schema;
    private readonly SegmentWriter _sw;
    /// <summary>One per column; each entry collects (segment_idx) per WriteBatch call.</summary>
    private readonly List<uint>[] _columnSegmentsByBatch;
    /// <summary>One per column; the zones-table schema scheme for this column's type.</summary>
    private readonly ZoneStatScheme[] _columnStatScheme;
    /// <summary>One per column; per-batch stats blob accumulator.</summary>
    private readonly List<BatchStats>[] _columnBatchStats;
    private readonly List<ulong> _batchRowCounts = new();
    private readonly bool _compress;
    private readonly bool _preferVarBinView;
    private readonly bool _preserveStats;
    private readonly bool _preferPco;
    private bool _closed;

    /// <summary>
    /// Per-batch stats captured during <see cref="WriteBatch"/> for the
    /// zones-table emission at <see cref="Close"/>. Fields are populated
    /// based on the column's <see cref="ZoneStatScheme"/>:
    /// <list type="bullet">
    ///   <item>NullCountOnly: just <see cref="NullCount"/>.</item>
    ///   <item>IntFull: <see cref="IsConstant"/>, <see cref="IsSorted"/>,
    ///     <see cref="IsStrictSorted"/>, <see cref="MinBytes"/>,
    ///     <see cref="MaxBytes"/>, <see cref="SumBytes"/>,
    ///     <see cref="NullCount"/>.</item>
    ///   <item>FloatStandard: <see cref="MinBytes"/>, <see cref="MaxBytes"/>,
    ///     <see cref="SumBytes"/>, <see cref="NullCount"/>,
    ///     <see cref="NaNCount"/>. Sortedness flags are skipped — NaN
    ///     ordering would make them unreliable.</item>
    /// </list>
    /// Byte-array fields are LE-encoded raw values of the column's natural
    /// width (Min/Max) or a 64-bit accumulator (Sum: i64 / u64 / f64). A
    /// <c>null</c> byte array means "no value in this batch" and the
    /// corresponding zones-table cell will have its validity bit cleared.
    /// </summary>
    private readonly struct BatchStats
    {
        public ulong NullCount { get; init; }
        public ulong NaNCount { get; init; }
        /// <summary>
        /// Sum of all Arrow buffer byte lengths (recursively across child
        /// data). Approximates vortex's <c>Stat::UncompressedSizeInBytes</c>
        /// which materializes the canonical form and reports its nbytes —
        /// for the Apache.Arrow shapes we accept (primitive, bool, varbin,
        /// list, struct, ...), the input array IS the canonical form, so
        /// summing its buffers is exact.
        /// </summary>
        public ulong UncompressedSizeInBytes { get; init; }
        public byte[]? MinBytes { get; init; }
        public byte[]? MaxBytes { get; init; }
        public byte[]? SumBytes { get; init; }
        public bool? IsConstant { get; init; }
        public bool? IsSorted { get; init; }
        public bool? IsStrictSorted { get; init; }
    }

    /// <summary>
    /// Stat schema for a column's per-zone table. Determines the column set
    /// in the auxiliary zones array AND the bitset that lands in the
    /// vortex.stats layout's metadata. Per upstream's `present_stats` rules
    /// the order is sorted ascending by Stat enum value:
    /// IsConstant=0, IsSorted=1, IsStrictSorted=2, Max=3, Min=4, Sum=5,
    /// NullCount=6, UncompressedSizeInBytes=7, NaNCount=8.
    /// </summary>
    private enum ZoneStatScheme
    {
        /// <summary>
        /// bitset = [0xC0, 0x00] — bits 6 (NullCount), 7 (UncompressedSizeInBytes).
        /// zones struct = { null_count: u64, uncompressed_size_in_bytes: u64 }.
        /// </summary>
        NullCountOnly,

        /// <summary>
        /// bitset = [0xFF, 0x00] — bits 0..7 (IsConstant, IsSorted,
        /// IsStrictSorted, Max, Min, Sum, NullCount, UncompressedSizeInBytes).
        /// zones struct = { is_constant: bool?, is_sorted: bool?,
        ///                  is_strict_sorted: bool?, max: T?,
        ///                  max_is_truncated: bool, min: T?,
        ///                  min_is_truncated: bool, sum: i64?/u64?,
        ///                  null_count: u64, uncompressed_size_in_bytes: u64 }.
        /// </summary>
        IntFull,

        /// <summary>
        /// bitset = [0xF8, 0x01] — bits 3..7 (Max, Min, Sum, NullCount,
        /// UncompressedSizeInBytes) plus bit 8 (NaNCount).
        /// zones struct = { max: T?, max_is_truncated: bool,
        ///                  min: T?, min_is_truncated: bool, sum: f64?,
        ///                  null_count: u64, uncompressed_size_in_bytes: u64,
        ///                  nan_count: u64 }.
        /// Sortedness flags are excluded for floats — NaN comparisons
        /// would make them unreliable, matching upstream's
        /// <c>ComputeIntOrdering</c> short-circuit on float types.
        /// </summary>
        FloatStandard,
    }

    /// <summary>
    /// Begins a Vortex file. The <paramref name="schema"/> is fixed for the
    /// lifetime of the writer; subsequent <see cref="WriteBatch"/> calls must
    /// pass batches with a structurally-equal schema.
    /// </summary>
    /// <param name="compress">When true, opt eligible columns into compressing
    /// encodings (bitpacked, FoR, dict, ALP, FSST, runend, sparse, etc.).
    /// Default <c>false</c>.</param>
    /// <param name="preferVarBinView">When true, string columns that fall
    /// through the compressing-encoding chain (no constant / dict / FSST hit)
    /// land on <c>vortex.varbinview</c> instead of <c>vortex.varbin</c>.
    /// Useful for cross-tool interop with consumers that prefer Arrow's
    /// BinaryView shape; for short-string columns vortex.varbin is more
    /// compact (4 + len bytes/row vs varbinview's 16 + len bytes/row).</param>
    /// <param name="preserveStats">When true, wrap each column in a
    /// <c>vortex.stats</c> layout that carries a per-zone stats table
    /// (currently just <c>null_count</c> per zone). Lets pruning-aware
    /// readers skip whole zones without decoding them. Falls back to the
    /// non-zoned chunked layout when batch row counts aren't uniform (vortex
    /// requires all zones except the last to share <c>zone_len</c>).</param>
    /// <param name="preferPco">When true, route eligible numeric columns
    /// (i16/u16/i32/u32/i64/u64/f32/f64) through <c>vortex.pco</c> instead
    /// of the format-specific compressing chain (ALP / ALP-RD / RLE / FoR /
    /// bitpacked). Pco's per-chunk mode-search (Classic / IntMult / FloatMult /
    /// FloatQuant) tends to beat the specific encoders on noisy real-world
    /// numeric data; trades slower decode for typically tighter output.
    /// Constant, dict, and FSST still take precedence — those strictly
    /// subsume pco for their niches. Has no effect when
    /// <paramref name="compress"/> is <c>false</c>.</param>
    public VortexFileWriter(
        Stream stream, Apache.Arrow.Schema schema,
        bool compress = false, bool preferVarBinView = false,
        bool preserveStats = false, bool preferPco = false)
    {
        _compress = compress;
        _preferVarBinView = preferVarBinView;
        _preserveStats = preserveStats;
        _preferPco = preferPco;
        _stream = stream ?? throw new ArgumentNullException(nameof(stream));
        _schema = schema ?? throw new ArgumentNullException(nameof(schema));
        _sw = new SegmentWriter(_stream);
        int nFields = schema.FieldsList.Count;
        _columnSegmentsByBatch = new List<uint>[nFields];
        _columnBatchStats = new List<BatchStats>[nFields];
        _columnStatScheme = new ZoneStatScheme[nFields];
        for (int i = 0; i < nFields; i++)
        {
            _columnSegmentsByBatch[i] = new List<uint>();
            _columnBatchStats[i] = new List<BatchStats>();
            _columnStatScheme[i] = SchemeForType(schema.FieldsList[i].DataType);
        }

        // Leading VTXF magic.
        var magicBytes = new byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(magicBytes, VortexFileFormat.MagicLE);
        _sw.WriteRaw(magicBytes);
    }

    /// <summary>Encodes <paramref name="batch"/> into one segment per column and appends them.</summary>
    public void WriteBatch(RecordBatch batch)
    {
        if (batch is null) throw new ArgumentNullException(nameof(batch));
        if (_closed) throw new InvalidOperationException("Writer has been closed.");
        if (batch.Schema.FieldsList.Count != _schema.FieldsList.Count)
            throw new ArgumentException(
                $"Batch has {batch.Schema.FieldsList.Count} columns but writer was opened with {_schema.FieldsList.Count}.");

        for (int i = 0; i < _schema.FieldsList.Count; i++)
        {
            var col = batch.Column(i);
            var sb = new SegmentBuilder();

            // Compute and emit per-column stats at the top-level ArrayNode.
            // Recursive child encoders skip stats (statsTicket=null) — adding
            // stats at every level would be wasteful and semantically wrong
            // for unrelated children (e.g., the offsets array's null_count
            // isn't the parent's).
            var statsValues = ArrayStatsComputer.Compute(col);
            int? statsTicket = ArrayStatsEmitter.Emit(sb.Builder, statsValues);

            int rootTicket = ArrayEncoderDispatch.Emit(
                sb, col, Indices, statsTicket, _compress, statsValues,
                _preferVarBinView, _preferPco);
            byte[] bytes = sb.FinishSegment(rootTicket);
            uint segIdx = _sw.AppendSegment(bytes, alignmentExponent: 0);
            _columnSegmentsByBatch[i].Add(segIdx);
            // Capture this batch's stats blob so we can build a per-zone
            // stats table at Close. Different schemes populate different
            // fields — see ComputeBatchStats / ZoneStatScheme.
            _columnBatchStats[i].Add(ComputeBatchStats(col, _columnStatScheme[i]));
        }
        _batchRowCounts.Add(checked((ulong)batch.Length));
    }

    /// <summary>
    /// Returns true iff the buffered batches form a valid zoned layout —
    /// every non-final batch has the same row count and the final batch's
    /// row count is &lt;= that. Vortex's zoned-layout spec requires zones
    /// of identical length except for the trailing partial zone.
    /// </summary>
    private bool CanZoneBatches()
    {
        int n = _batchRowCounts.Count;
        if (n == 0) return false;
        if (n == 1) return true;
        ulong first = _batchRowCounts[0];
        for (int b = 1; b < n - 1; b++)
            if (_batchRowCounts[b] != first) return false;
        return _batchRowCounts[n - 1] <= first;
    }

    private static ZoneStatScheme SchemeForType(Apache.Arrow.Types.IArrowType type) => type switch
    {
        Apache.Arrow.Types.Int8Type or Apache.Arrow.Types.Int16Type
            or Apache.Arrow.Types.Int32Type or Apache.Arrow.Types.Int64Type
            or Apache.Arrow.Types.UInt8Type or Apache.Arrow.Types.UInt16Type
            or Apache.Arrow.Types.UInt32Type or Apache.Arrow.Types.UInt64Type
            => ZoneStatScheme.IntFull,
        Apache.Arrow.Types.FloatType or Apache.Arrow.Types.DoubleType
            => ZoneStatScheme.FloatStandard,
        _ => ZoneStatScheme.NullCountOnly,
    };

    /// <summary>
    /// Returns the bitset bytes for the layout-metadata's `present_stats`
    /// field. Layout matches upstream's `as_stat_bitset_bytes`: 9-bit
    /// packed field (Stat enum values 0..8), 2 bytes total.
    /// </summary>
    private static byte[] BitsetForScheme(ZoneStatScheme scheme) => scheme switch
    {
        // Bits 6 (NullCount), 7 (UncompressedSizeInBytes) = 0xC0.
        ZoneStatScheme.NullCountOnly => new byte[] { 0xC0, 0x00 },
        // Bits 0..7 (IsConstant, IsSorted, IsStrictSorted, Max, Min, Sum, NullCount, UncompressedSizeInBytes) = 0xFF.
        ZoneStatScheme.IntFull => new byte[] { 0xFF, 0x00 },
        // Bits 3..7 (Max, Min, Sum, NullCount, UncompressedSizeInBytes) = 0xF8, plus bit 8 (NaNCount) = 0x01.
        ZoneStatScheme.FloatStandard => new byte[] { 0xF8, 0x01 },
        _ => throw new NotSupportedException(),
    };

    private static int ByteWidthForIntType(Apache.Arrow.Types.IArrowType type) => type switch
    {
        Apache.Arrow.Types.Int8Type or Apache.Arrow.Types.UInt8Type => 1,
        Apache.Arrow.Types.Int16Type or Apache.Arrow.Types.UInt16Type => 2,
        Apache.Arrow.Types.Int32Type or Apache.Arrow.Types.UInt32Type => 4,
        Apache.Arrow.Types.Int64Type or Apache.Arrow.Types.UInt64Type => 8,
        _ => throw new NotSupportedException(),
    };

    /// <summary>
    /// Computes a per-batch <see cref="BatchStats"/> blob for the given
    /// column based on its <see cref="ZoneStatScheme"/>. Walks the column
    /// once for integer / float schemes; NullCountOnly just reads the
    /// already-computed null count from <c>Data</c>. UncompressedSizeInBytes
    /// is computed for all schemes since it's a fast byte-counting walk
    /// that doesn't depend on the column's element type.
    /// </summary>
    private static BatchStats ComputeBatchStats(Apache.Arrow.IArrowArray col, ZoneStatScheme scheme)
    {
        var data = ((Apache.Arrow.Array)col).Data;
        ulong nullCount = (ulong)data.GetNullCount();
        ulong uncompressedSize = ComputeUncompressedSize(data);
        BatchStats baseStats = scheme switch
        {
            ZoneStatScheme.NullCountOnly => new BatchStats { NullCount = nullCount },
            ZoneStatScheme.IntFull => ComputeIntStats(col, data, nullCount),
            ZoneStatScheme.FloatStandard => ComputeFloatStats(col, data, nullCount),
            _ => throw new NotSupportedException(),
        };
        return new BatchStats
        {
            NullCount = baseStats.NullCount,
            NaNCount = baseStats.NaNCount,
            UncompressedSizeInBytes = uncompressedSize,
            MinBytes = baseStats.MinBytes,
            MaxBytes = baseStats.MaxBytes,
            SumBytes = baseStats.SumBytes,
            IsConstant = baseStats.IsConstant,
            IsSorted = baseStats.IsSorted,
            IsStrictSorted = baseStats.IsStrictSorted,
        };
    }

    /// <summary>
    /// Sum of all Arrow buffer byte lengths, recursing through child data.
    /// Approximates vortex's <c>Stat::UncompressedSizeInBytes</c>.
    /// </summary>
    private static ulong ComputeUncompressedSize(Apache.Arrow.ArrayData data)
    {
        ulong size = 0;
        if (data.Buffers is not null)
            foreach (var buf in data.Buffers) size += (ulong)buf.Length;
        // Apache.Arrow.NET leaves Children null for primitive arrays — only
        // nested types (struct, list, FSL, dictionary) populate it.
        if (data.Children is not null)
            foreach (var child in data.Children)
                if (child is not null) size += ComputeUncompressedSize(child);
        return size;
    }

    /// <summary>
    /// Single-pass compute of {Min, Max, Sum, IsConstant, IsSorted,
    /// IsStrictSorted, NullCount} for an integer column. Sum widens to i64
    /// (signed) or u64 (unsigned); arithmetic is unchecked, matching
    /// upstream's "sum may wrap silently" convention. Empty/all-null
    /// batches return null Min/Max/Sum bytes — the zones-table cells get
    /// their validity bit cleared.
    /// </summary>
    private static BatchStats ComputeIntStats(
        Apache.Arrow.IArrowArray col, Apache.Arrow.ArrayData data, ulong nullCount)
    {
        int n = col.Length;
        bool hasNulls = nullCount > 0;
        var validity = hasNulls ? data.Buffers[0].Span : default;
        int off = data.Offset;
        bool isSigned = col is Apache.Arrow.Int8Array or Apache.Arrow.Int16Array
            or Apache.Arrow.Int32Array or Apache.Arrow.Int64Array;
        int byteWidth = ByteWidthForIntType(data.DataType);
        var src = data.Buffers[1].Span.Slice(off * byteWidth, n * byteWidth);

        bool any = false;
        long sMin = long.MaxValue, sMax = long.MinValue, sSum = 0;
        ulong uMin = ulong.MaxValue, uMax = ulong.MinValue, uSum = 0;
        long sFirst = 0, sPrev = 0;
        ulong uFirst = 0, uPrev = 0;
        bool isConstant = true, isSorted = true, isStrictSorted = true;

        for (int i = 0; i < n; i++)
        {
            if (hasNulls)
            {
                int gb = off + i;
                if ((validity[gb >> 3] & (1 << (gb & 7))) == 0) continue;
            }
            int p = i * byteWidth;
            if (isSigned)
            {
                long v = byteWidth switch
                {
                    1 => (sbyte)src[p],
                    2 => System.Buffers.Binary.BinaryPrimitives.ReadInt16LittleEndian(src.Slice(p, 2)),
                    4 => System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(src.Slice(p, 4)),
                    8 => System.Buffers.Binary.BinaryPrimitives.ReadInt64LittleEndian(src.Slice(p, 8)),
                    _ => throw new NotSupportedException(),
                };
                unchecked { sSum += v; }
                if (!any)
                {
                    sMin = sMax = sFirst = sPrev = v;
                    any = true;
                }
                else
                {
                    if (v < sMin) sMin = v;
                    if (v > sMax) sMax = v;
                    if (v != sFirst) isConstant = false;
                    if (v < sPrev) { isSorted = false; isStrictSorted = false; }
                    else if (v == sPrev) isStrictSorted = false;
                    sPrev = v;
                }
            }
            else
            {
                ulong v = byteWidth switch
                {
                    1 => src[p],
                    2 => System.Buffers.Binary.BinaryPrimitives.ReadUInt16LittleEndian(src.Slice(p, 2)),
                    4 => System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(src.Slice(p, 4)),
                    8 => System.Buffers.Binary.BinaryPrimitives.ReadUInt64LittleEndian(src.Slice(p, 8)),
                    _ => throw new NotSupportedException(),
                };
                unchecked { uSum += v; }
                if (!any)
                {
                    uMin = uMax = uFirst = uPrev = v;
                    any = true;
                }
                else
                {
                    if (v < uMin) uMin = v;
                    if (v > uMax) uMax = v;
                    if (v != uFirst) isConstant = false;
                    if (v < uPrev) { isSorted = false; isStrictSorted = false; }
                    else if (v == uPrev) isStrictSorted = false;
                    uPrev = v;
                }
            }
        }

        if (!any)
        {
            // Empty / all-null batch: Min/Max/Sum cells are null. Sortedness
            // and is_constant for an empty range are vacuously "true" but
            // we set them null since there's no data to characterise.
            return new BatchStats { NullCount = nullCount };
        }

        var minBytes = new byte[byteWidth];
        var maxBytes = new byte[byteWidth];
        var sumBytes = new byte[8];
        if (isSigned)
        {
            WriteIntLE(minBytes, sMin, byteWidth);
            WriteIntLE(maxBytes, sMax, byteWidth);
            System.Buffers.Binary.BinaryPrimitives.WriteInt64LittleEndian(sumBytes, sSum);
        }
        else
        {
            WriteIntLE(minBytes, unchecked((long)uMin), byteWidth);
            WriteIntLE(maxBytes, unchecked((long)uMax), byteWidth);
            System.Buffers.Binary.BinaryPrimitives.WriteUInt64LittleEndian(sumBytes, uSum);
        }
        return new BatchStats
        {
            NullCount = nullCount,
            MinBytes = minBytes,
            MaxBytes = maxBytes,
            SumBytes = sumBytes,
            IsConstant = isConstant,
            IsSorted = isSorted,
            IsStrictSorted = isStrictSorted,
        };
    }

    /// <summary>
    /// Single-pass compute of {Min, Max, Sum, NullCount, NaNCount} for a
    /// float column. NaNs are excluded from Min/Max/Sum but counted in
    /// NaNCount. Sum is accumulated in f64 regardless of input width.
    /// Sortedness flags are skipped — NaN ordering would make them
    /// unreliable, matching upstream's behavior.
    /// </summary>
    private static BatchStats ComputeFloatStats(
        Apache.Arrow.IArrowArray col, Apache.Arrow.ArrayData data, ulong nullCount)
    {
        int n = col.Length;
        bool hasNulls = nullCount > 0;
        var validity = hasNulls ? data.Buffers[0].Span : default;
        int off = data.Offset;
        bool isF32 = col is Apache.Arrow.FloatArray;
        int byteWidth = isF32 ? 4 : 8;
        var src = data.Buffers[1].Span.Slice(off * byteWidth, n * byteWidth);

        bool any = false;
        double dMin = 0, dMax = 0, dSum = 0;
        ulong nanCount = 0;
        for (int i = 0; i < n; i++)
        {
            if (hasNulls)
            {
                int gb = off + i;
                if ((validity[gb >> 3] & (1 << (gb & 7))) == 0) continue;
            }
            double v;
            if (isF32)
            {
                int bits = System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(
                    src.Slice(i * 4, 4));
                v = Int32BitsToSingle(bits);
            }
            else
            {
                long bits = System.Buffers.Binary.BinaryPrimitives.ReadInt64LittleEndian(
                    src.Slice(i * 8, 8));
                v = BitConverter.Int64BitsToDouble(bits);
            }
            if (double.IsNaN(v)) { nanCount++; continue; }
            dSum += v;
            if (!any) { dMin = dMax = v; any = true; }
            else { if (v < dMin) dMin = v; if (v > dMax) dMax = v; }
        }

        if (!any)
            return new BatchStats { NullCount = nullCount, NaNCount = nanCount };

        var minBytes = new byte[byteWidth];
        var maxBytes = new byte[byteWidth];
        var sumBytes = new byte[8];
        if (isF32)
        {
            System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(
                minBytes, SingleToInt32Bits((float)dMin));
            System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(
                maxBytes, SingleToInt32Bits((float)dMax));
        }
        else
        {
            System.Buffers.Binary.BinaryPrimitives.WriteInt64LittleEndian(
                minBytes, BitConverter.DoubleToInt64Bits(dMin));
            System.Buffers.Binary.BinaryPrimitives.WriteInt64LittleEndian(
                maxBytes, BitConverter.DoubleToInt64Bits(dMax));
        }
        System.Buffers.Binary.BinaryPrimitives.WriteInt64LittleEndian(
            sumBytes, BitConverter.DoubleToInt64Bits(dSum));
        return new BatchStats
        {
            NullCount = nullCount,
            NaNCount = nanCount,
            MinBytes = minBytes,
            MaxBytes = maxBytes,
            SumBytes = sumBytes,
        };
    }

    private static void WriteIntLE(Span<byte> dest, long value, int byteWidth)
    {
        switch (byteWidth)
        {
            case 1: dest[0] = (byte)value; break;
            case 2: System.Buffers.Binary.BinaryPrimitives.WriteInt16LittleEndian(dest, (short)value); break;
            case 4: System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(dest, (int)value); break;
            case 8: System.Buffers.Binary.BinaryPrimitives.WriteInt64LittleEndian(dest, value); break;
            default: throw new NotSupportedException();
        }
    }

    /// <summary>netstandard2.0-compat shims for the f32 ↔ i32 bitcast.</summary>
    private static int SingleToInt32Bits(float f)
    {
#if NET6_0_OR_GREATER
        return BitConverter.SingleToInt32Bits(f);
#else
        return BitConverter.ToInt32(BitConverter.GetBytes(f), 0);
#endif
    }

    private static float Int32BitsToSingle(int bits)
    {
#if NET6_0_OR_GREATER
        return BitConverter.Int32BitsToSingle(bits);
#else
        return BitConverter.ToSingle(BitConverter.GetBytes(bits), 0);
#endif
    }

    /// <summary>
    /// Builds and appends a per-column zones segment. Dispatches on the
    /// column's <see cref="ZoneStatScheme"/>:
    /// <list type="bullet">
    ///   <item>NullCountOnly: 1-field struct.</item>
    ///   <item>IntFull: 9-field struct (sorted [IsConstant, IsSorted,
    ///     IsStrictSorted, Max, MaxIsTruncated, Min, MinIsTruncated, Sum,
    ///     NullCount]).</item>
    ///   <item>FloatStandard: 7-field struct ([Max, MaxIsTruncated, Min,
    ///     MinIsTruncated, Sum, NullCount, NaNCount]).</item>
    /// </list>
    /// </summary>
    private uint EmitZonesSegment(int columnIdx)
    {
        var stats = _columnBatchStats[columnIdx];
        return _columnStatScheme[columnIdx] switch
        {
            ZoneStatScheme.NullCountOnly => EmitZonesSegmentNullCountOnly(stats),
            ZoneStatScheme.IntFull => EmitZonesSegmentIntFull(stats, columnIdx),
            ZoneStatScheme.FloatStandard => EmitZonesSegmentFloatStandard(stats, columnIdx),
            _ => throw new NotSupportedException(),
        };
    }

    private uint EmitZonesSegmentNullCountOnly(List<BatchStats> stats)
    {
        var sb = new SegmentBuilder();
        // Order (sorted by Stat enum): null_count(6), uncompressed_size_in_bytes(7).
        int ncTicket = EmitNullCountChild(sb, stats);
        int sizeTicket = EmitUncompressedSizeChild(sb, stats);
        int structTicket = ArrayNodeEmitter.EmitWithChildrenOnly(
            sb.Builder, StructEncodingIdx, new[] { ncTicket, sizeTicket });
        return _sw.AppendSegment(sb.FinishSegment(structTicket), alignmentExponent: 0);
    }

    private uint EmitZonesSegmentIntFull(List<BatchStats> stats, int columnIdx)
    {
        int byteWidth = ByteWidthForIntType(_schema.FieldsList[columnIdx].DataType);
        bool isSigned = _schema.FieldsList[columnIdx].DataType is Apache.Arrow.Types.Int8Type
            or Apache.Arrow.Types.Int16Type or Apache.Arrow.Types.Int32Type
            or Apache.Arrow.Types.Int64Type;
        var sb = new SegmentBuilder();

        // Order (sorted by Stat enum, with Min/Max followed by their truncation flags):
        // is_constant(0), is_sorted(1), is_strict_sorted(2), max(3), max_is_truncated,
        // min(4), min_is_truncated, sum(5), null_count(6), uncompressed_size_in_bytes(7).
        int isConstantTicket = EmitNullableBool(sb, stats, s => s.IsConstant);
        int isSortedTicket = EmitNullableBool(sb, stats, s => s.IsSorted);
        int isStrictSortedTicket = EmitNullableBool(sb, stats, s => s.IsStrictSorted);
        int maxTicket = EmitNullablePrimitiveColumn(sb, stats, s => s.MaxBytes, byteWidth);
        int maxTruncTicket = EmitAllFalseBool(sb, stats.Count);
        int minTicket = EmitNullablePrimitiveColumn(sb, stats, s => s.MinBytes, byteWidth);
        int minTruncTicket = EmitAllFalseBool(sb, stats.Count);
        // Sum is i64 (signed) or u64 (unsigned); both are 8-byte primitives.
        // The dtype proto distinction lives in the layout's column_dtype, not
        // the array layout — they share wire shape.
        _ = isSigned;
        int sumTicket = EmitNullablePrimitiveColumn(sb, stats, s => s.SumBytes, 8);
        int nullCountTicket = EmitNullCountChild(sb, stats);
        int sizeTicket = EmitUncompressedSizeChild(sb, stats);

        int structTicket = ArrayNodeEmitter.EmitWithChildrenOnly(
            sb.Builder, StructEncodingIdx,
            new[] { isConstantTicket, isSortedTicket, isStrictSortedTicket,
                    maxTicket, maxTruncTicket, minTicket, minTruncTicket,
                    sumTicket, nullCountTicket, sizeTicket });
        return _sw.AppendSegment(sb.FinishSegment(structTicket), alignmentExponent: 0);
    }

    private uint EmitZonesSegmentFloatStandard(List<BatchStats> stats, int columnIdx)
    {
        int byteWidth = _schema.FieldsList[columnIdx].DataType is Apache.Arrow.Types.FloatType ? 4 : 8;
        var sb = new SegmentBuilder();

        // Order: max(3), max_is_truncated, min(4), min_is_truncated, sum(5),
        // null_count(6), uncompressed_size_in_bytes(7), nan_count(8).
        int maxTicket = EmitNullablePrimitiveColumn(sb, stats, s => s.MaxBytes, byteWidth);
        int maxTruncTicket = EmitAllFalseBool(sb, stats.Count);
        int minTicket = EmitNullablePrimitiveColumn(sb, stats, s => s.MinBytes, byteWidth);
        int minTruncTicket = EmitAllFalseBool(sb, stats.Count);
        int sumTicket = EmitNullablePrimitiveColumn(sb, stats, s => s.SumBytes, 8);
        int nullCountTicket = EmitNullCountChild(sb, stats);
        int sizeTicket = EmitUncompressedSizeChild(sb, stats);
        int nanCountTicket = EmitNanCountChild(sb, stats);

        int structTicket = ArrayNodeEmitter.EmitWithChildrenOnly(
            sb.Builder, StructEncodingIdx,
            new[] { maxTicket, maxTruncTicket, minTicket, minTruncTicket,
                    sumTicket, nullCountTicket, sizeTicket, nanCountTicket });
        return _sw.AppendSegment(sb.FinishSegment(structTicket), alignmentExponent: 0);
    }

    /// <summary>
    /// Emits the per-zone <c>null_count: u64</c> child (always non-null).
    /// </summary>
    private static int EmitNullCountChild(SegmentBuilder sb, List<BatchStats> stats)
    {
        int n = stats.Count;
        var bytes = new byte[(long)n * 8];
        var span = System.Runtime.InteropServices.MemoryMarshal.Cast<byte, ulong>(bytes.AsSpan());
        for (int i = 0; i < n; i++) span[i] = stats[i].NullCount;
        ushort buf = sb.AddBuffer(bytes, alignmentExponent: 3);
        return ArrayNodeEmitter.EmitWithSingleBuffer(sb.Builder, PrimitiveEncodingIdx, buf);
    }

    /// <summary>
    /// Emits the per-zone <c>uncompressed_size_in_bytes: u64</c> child
    /// (always non-null).
    /// </summary>
    private static int EmitUncompressedSizeChild(SegmentBuilder sb, List<BatchStats> stats)
    {
        int n = stats.Count;
        var bytes = new byte[(long)n * 8];
        var span = System.Runtime.InteropServices.MemoryMarshal.Cast<byte, ulong>(bytes.AsSpan());
        for (int i = 0; i < n; i++) span[i] = stats[i].UncompressedSizeInBytes;
        ushort buf = sb.AddBuffer(bytes, alignmentExponent: 3);
        return ArrayNodeEmitter.EmitWithSingleBuffer(sb.Builder, PrimitiveEncodingIdx, buf);
    }

    /// <summary>
    /// Emits the per-zone <c>nan_count: u64</c> child (always non-null;
    /// 0 for batches with no NaNs).
    /// </summary>
    private static int EmitNanCountChild(SegmentBuilder sb, List<BatchStats> stats)
    {
        int n = stats.Count;
        var bytes = new byte[(long)n * 8];
        var span = System.Runtime.InteropServices.MemoryMarshal.Cast<byte, ulong>(bytes.AsSpan());
        for (int i = 0; i < n; i++) span[i] = stats[i].NaNCount;
        ushort buf = sb.AddBuffer(bytes, alignmentExponent: 3);
        return ArrayNodeEmitter.EmitWithSingleBuffer(sb.Builder, PrimitiveEncodingIdx, buf);
    }

    /// <summary>
    /// Emits a nullable bool child. Each cell's validity bit is cleared
    /// when <paramref name="getter"/> returns null for that batch (e.g.
    /// empty/all-null batch's IsConstant is undefined).
    /// </summary>
    private static int EmitNullableBool(
        SegmentBuilder sb, List<BatchStats> stats, Func<BatchStats, bool?> getter)
    {
        int n = stats.Count;
        var values = new byte[(n + 7) / 8];
        var validity = new byte[(n + 7) / 8];
        int nullCount = 0;
        for (int i = 0; i < n; i++)
        {
            var v = getter(stats[i]);
            if (v is null) { nullCount++; continue; }
            validity[i >> 3] |= (byte)(1 << (i & 7));
            if (v.Value) values[i >> 3] |= (byte)(1 << (i & 7));
        }
        ushort valBuf = sb.AddBuffer(values, alignmentExponent: 0);
        if (nullCount == 0)
            return ArrayNodeEmitter.EmitWithSingleBuffer(sb.Builder, BoolEncodingIdx, valBuf);
        ushort validityBuf = sb.AddBuffer(validity, alignmentExponent: 0);
        int validityNode = ArrayNodeEmitter.EmitWithSingleBuffer(sb.Builder, BoolEncodingIdx, validityBuf);
        return ArrayNodeEmitter.EmitWithBufferAndChildren(
            sb.Builder, BoolEncodingIdx, valBuf, new[] { validityNode });
    }

    /// <summary>
    /// Emits a non-nullable bool child whose values are all <c>false</c>.
    /// Used for <c>min_is_truncated</c> / <c>max_is_truncated</c>: the
    /// writer never truncates min/max so these are always all-zero.
    /// </summary>
    private static int EmitAllFalseBool(SegmentBuilder sb, int n)
    {
        var bytes = new byte[(n + 7) / 8];
        ushort buf = sb.AddBuffer(bytes, alignmentExponent: 0);
        return ArrayNodeEmitter.EmitWithSingleBuffer(sb.Builder, BoolEncodingIdx, buf);
    }

    /// <summary>
    /// Emits a nullable typed-primitive column. For each zone, when
    /// <paramref name="getter"/> returns null the value slot is zeroed and
    /// the validity bit is cleared.
    /// </summary>
    private static int EmitNullablePrimitiveColumn(
        SegmentBuilder sb, List<BatchStats> stats, Func<BatchStats, byte[]?> getter, int byteWidth)
    {
        int n = stats.Count;
        var values = new byte[(long)n * byteWidth];
        var validity = new byte[(n + 7) / 8];
        int nullCount = 0;
        for (int i = 0; i < n; i++)
        {
            var v = getter(stats[i]);
            if (v is null) { nullCount++; continue; }
            validity[i >> 3] |= (byte)(1 << (i & 7));
            v.AsSpan().CopyTo(values.AsSpan(i * byteWidth, byteWidth));
        }
        byte alignExp = byteWidth switch { 1 => 0, 2 => 1, 4 => 2, 8 => 3, _ => 0 };
        ushort valBuf = sb.AddBuffer(values, alignmentExponent: alignExp);
        if (nullCount == 0)
            return ArrayNodeEmitter.EmitWithSingleBuffer(sb.Builder, PrimitiveEncodingIdx, valBuf);
        ushort validityBuf = sb.AddBuffer(validity, alignmentExponent: 0);
        int validityNode = ArrayNodeEmitter.EmitWithSingleBuffer(sb.Builder, BoolEncodingIdx, validityBuf);
        return ArrayNodeEmitter.EmitWithBufferAndChildren(
            sb.Builder, PrimitiveEncodingIdx, valBuf, new[] { validityNode });
    }

    /// <summary>
    /// Finalizes the file: emits DType, Layout, Footer, Postscript, EndOfFile.
    /// Idempotent — calling twice is a no-op. Disposing also calls Close.
    /// </summary>
    public void Close()
    {
        if (_closed) return;
        _closed = true;

        ulong totalRows = 0;
        for (int b = 0; b < _batchRowCounts.Count; b++) totalRows += _batchRowCounts[b];

        // 1. DType.
        var dtypeBytes = DTypeSerializer.SerializeSchema(_schema);
        var dtypeBlock = _sw.AppendPostscriptBlock(dtypeBytes);

        // 2. Layout.
        // Strategy:
        //   - Single batch: vortex.struct(vortex.flat × N).
        //   - Multi batch:  vortex.struct(vortex.chunked(vortex.flat × M) × N).
        //   - With preserveStats AND zone-eligible batch sizes: each column is
        //     additionally wrapped in vortex.stats(data, zones) so a
        //     pruning-aware reader can skip whole zones via the zones table.
        // preserveStats falls back to the non-zoned chunked layout when
        // batch row counts aren't uniform (vortex requires all zones except
        // the last to share zone_len).
        bool zoned = _preserveStats && _columnSegmentsByBatch.Length > 0 && CanZoneBatches();
        byte[] layoutBytes;
        if (_batchRowCounts.Count == 0)
        {
            throw new InvalidOperationException(
                "Cannot finalize a Vortex file with zero batches written; write at least one batch first.");
        }

        if (zoned)
        {
            // Build per-column zones segment (one row per batch with the
            // batch's null_count). Returns the segment index for each column.
            var perColumnZonesSeg = new uint[_columnSegmentsByBatch.Length];
            for (int c = 0; c < perColumnZonesSeg.Length; c++)
                perColumnZonesSeg[c] = EmitZonesSegment(c);

            int zoneLen = checked((int)_batchRowCounts[0]);
            int numZones = _batchRowCounts.Count;
            var perColumnSegByBatch = new uint[_columnSegmentsByBatch.Length][];
            var perColumnBitset = new byte[_columnSegmentsByBatch.Length][];
            for (int i = 0; i < perColumnSegByBatch.Length; i++)
            {
                perColumnSegByBatch[i] = _columnSegmentsByBatch[i].ToArray();
                perColumnBitset[i] = BitsetForScheme(_columnStatScheme[i]);
            }
            layoutBytes = LayoutSerializer.SerializeStructStatsChunked(
                StructLayoutIdx, StatsLayoutIdx, ChunkedLayoutIdx, FlatLayoutIdx,
                totalRows, zoneLen, numZones,
                _batchRowCounts, perColumnSegByBatch, perColumnZonesSeg, perColumnBitset);
        }
        else if (_batchRowCounts.Count == 1)
        {
            var perColumnSeg = new uint[_columnSegmentsByBatch.Length];
            for (int i = 0; i < perColumnSeg.Length; i++)
                perColumnSeg[i] = _columnSegmentsByBatch[i][0];
            layoutBytes = LayoutSerializer.SerializeStructFlat(
                StructLayoutIdx, FlatLayoutIdx, totalRows, perColumnSeg);
        }
        else
        {
            var perColumnSegByBatch = new uint[_columnSegmentsByBatch.Length][];
            for (int i = 0; i < perColumnSegByBatch.Length; i++)
                perColumnSegByBatch[i] = _columnSegmentsByBatch[i].ToArray();
            layoutBytes = LayoutSerializer.SerializeStructChunked(
                StructLayoutIdx, ChunkedLayoutIdx, FlatLayoutIdx,
                totalRows, _batchRowCounts, perColumnSegByBatch);
        }
        var layoutBlock = _sw.AppendPostscriptBlock(layoutBytes);

        // 3. Footer.
        var footerBytes = FooterSerializer.Serialize(
            arraySpecs: new[]
            {
                VortexArrayEncodings.Primitive,
                VortexArrayEncodings.Bool,
                VortexArrayEncodings.VarBin,
                VortexArrayEncodings.List,
                VortexArrayEncodings.FixedSizeList,
                VortexArrayEncodings.FastlanesBitPacked,
                VortexArrayEncodings.Decimal,
                VortexArrayEncodings.Constant,
                VortexArrayEncodings.FastlanesFor,
                VortexArrayEncodings.FastlanesDelta,
                VortexArrayEncodings.Dict,
                VortexArrayEncodings.FastlanesRle,
                VortexArrayEncodings.Struct_,
                VortexArrayEncodings.Alp,
                VortexArrayEncodings.RunEnd,
                VortexArrayEncodings.Sparse,
                VortexArrayEncodings.FsstString,
                VortexArrayEncodings.AlpRD,
                VortexArrayEncodings.VarBinView,
                VortexArrayEncodings.Pco,
            },
            layoutSpecs: new[] { VortexLayoutEncodings.Flat, VortexLayoutEncodings.Struct, VortexLayoutEncodings.Chunked, VortexLayoutEncodings.Stats },
            segmentSpecs: _sw.SegmentSpecs);
        var footerBlock = _sw.AppendPostscriptBlock(footerBytes);

        // 4. Postscript.
        var postscriptBytes = PostscriptSerializer.Serialize(dtypeBlock, layoutBlock, footerBlock);
        if (postscriptBytes.Length > VortexFileFormat.MaxPostscriptLen)
            throw new InvalidOperationException(
                $"Postscript size {postscriptBytes.Length} exceeds the format maximum ({VortexFileFormat.MaxPostscriptLen}).");
        _sw.WriteRaw(postscriptBytes);

        // 5. EndOfFile struct.
        var eof = new byte[VortexFileFormat.EndOfFileSize];
        BinaryPrimitives.WriteUInt16LittleEndian(eof.AsSpan(0), 1);
        BinaryPrimitives.WriteUInt16LittleEndian(eof.AsSpan(2), (ushort)postscriptBytes.Length);
        BinaryPrimitives.WriteUInt32LittleEndian(eof.AsSpan(4), VortexFileFormat.MagicLE);
        _sw.WriteRaw(eof);
    }

    public void Dispose()
    {
        // Don't let a finalize-time exception (e.g., "no batches") mask the
        // original exception that caused the using-block to unwind. Swallow
        // anything from Close so the caller sees the root cause.
        try { Close(); }
        catch { _closed = true; }
    }

    /// <summary>One-shot convenience: writes <paramref name="batch"/> as a single-batch file.</summary>
    public static void Write(
        Stream stream, RecordBatch batch,
        bool compress = false, bool preferVarBinView = false,
        bool preserveStats = false, bool preferPco = false)
    {
        if (batch is null) throw new ArgumentNullException(nameof(batch));
        using var writer = new VortexFileWriter(
            stream, batch.Schema, compress, preferVarBinView, preserveStats, preferPco);
        writer.WriteBatch(batch);
        writer.Close();
    }
}
