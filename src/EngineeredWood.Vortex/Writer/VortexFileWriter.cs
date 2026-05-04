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
        VarBinView: VarBinViewEncodingIdx);

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
    /// <summary>One per column; per-batch null counts for the zoned-stats layout.</summary>
    private readonly List<ulong>[] _columnNullCountsByBatch;
    private readonly List<ulong> _batchRowCounts = new();
    private readonly bool _compress;
    private readonly bool _preferVarBinView;
    private readonly bool _preserveStats;
    private bool _closed;

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
    public VortexFileWriter(
        Stream stream, Apache.Arrow.Schema schema,
        bool compress = false, bool preferVarBinView = false,
        bool preserveStats = false)
    {
        _compress = compress;
        _preferVarBinView = preferVarBinView;
        _preserveStats = preserveStats;
        _stream = stream ?? throw new ArgumentNullException(nameof(stream));
        _schema = schema ?? throw new ArgumentNullException(nameof(schema));
        _sw = new SegmentWriter(_stream);
        _columnSegmentsByBatch = new List<uint>[schema.FieldsList.Count];
        _columnNullCountsByBatch = new List<ulong>[schema.FieldsList.Count];
        for (int i = 0; i < _columnSegmentsByBatch.Length; i++)
        {
            _columnSegmentsByBatch[i] = new List<uint>();
            _columnNullCountsByBatch[i] = new List<ulong>();
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
                sb, col, Indices, statsTicket, _compress, statsValues, _preferVarBinView);
            byte[] bytes = sb.FinishSegment(rootTicket);
            uint segIdx = _sw.AppendSegment(bytes, alignmentExponent: 0);
            _columnSegmentsByBatch[i].Add(segIdx);
            // Capture this batch's null_count so we can build a per-zone stats
            // table at Close. Apache.Arrow's typed array exposes NullCount via
            // the underlying Data; using GetNullCount handles the lazy-count
            // case for sliced columns even though we don't slice at the
            // top-level here.
            _columnNullCountsByBatch[i].Add((ulong)((Apache.Arrow.Array)col).Data.GetNullCount());
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

    /// <summary>
    /// Builds and appends the per-column zones segment carrying the per-zone
    /// null_count table. The segment holds a <c>vortex.struct</c> ArrayNode
    /// with a single <c>null_count</c> u64 child of length = batch count.
    /// Returns the segment index.
    /// </summary>
    private uint EmitZonesSegment(int columnIdx)
    {
        var counts = _columnNullCountsByBatch[columnIdx];
        int numZones = counts.Count;
        var bytes = new byte[(long)numZones * 8];
        var span = System.Runtime.InteropServices.MemoryMarshal.Cast<byte, ulong>(bytes.AsSpan());
        for (int i = 0; i < numZones; i++) span[i] = counts[i];
        var nullCountArr = new Apache.Arrow.UInt64Array(
            new Apache.Arrow.ArrowBuffer(bytes), Apache.Arrow.ArrowBuffer.Empty, numZones, 0, 0);

        var sb = new SegmentBuilder();
        // The zones table is a vortex.struct with one u64 child. We hand-emit
        // both ArrayNodes here rather than going through StructArrayEncoder
        // (which expects an Apache.Arrow.StructArray) — the wire shape is
        // simple enough that hand-rolling is clearer than constructing an
        // Apache.Arrow StructArray just to throw it away.
        ushort u64BufIdx = sb.AddBuffer(bytes, alignmentExponent: 3);
        int u64Ticket = ArrayNodeEmitter.EmitWithSingleBuffer(sb.Builder, PrimitiveEncodingIdx, u64BufIdx);
        int structTicket = ArrayNodeEmitter.EmitWithChildrenOnly(
            sb.Builder, StructEncodingIdx, new[] { u64Ticket });
        byte[] segBytes = sb.FinishSegment(structTicket);
        return _sw.AppendSegment(segBytes, alignmentExponent: 0);
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
            for (int i = 0; i < perColumnSegByBatch.Length; i++)
                perColumnSegByBatch[i] = _columnSegmentsByBatch[i].ToArray();
            layoutBytes = LayoutSerializer.SerializeStructStatsChunked(
                StructLayoutIdx, StatsLayoutIdx, ChunkedLayoutIdx, FlatLayoutIdx,
                totalRows, zoneLen, numZones,
                _batchRowCounts, perColumnSegByBatch, perColumnZonesSeg);
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
        bool preserveStats = false)
    {
        if (batch is null) throw new ArgumentNullException(nameof(batch));
        using var writer = new VortexFileWriter(
            stream, batch.Schema, compress, preferVarBinView, preserveStats);
        writer.WriteBatch(batch);
        writer.Close();
    }
}
