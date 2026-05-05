// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using EngineeredWood.Compression;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;
using EngineeredWood.Vortex.Encodings;
using EngineeredWood.Vortex.Format;
using EngineeredWood.Vortex.Layouts;
using EngineeredWood.Vortex.Schema;

namespace EngineeredWood.Vortex;

/// <summary>
/// File-level Vortex reader. Validates the leading and trailing <c>'VTXF'</c>
/// magic, parses the EndOfFile struct + postscript, materializes the footer
/// registry (array specs, layout specs, segment specs), and stages the raw
/// bytes for the dtype and layout segments.
///
/// <para>Phase 1 scope: container open only. Schema (DType→Arrow) and layout
/// traversal land in subsequent chunks. Encryption is rejected.</para>
/// </summary>
public sealed class VortexFileReader : IAsyncDisposable, IDisposable
{
    private readonly IRandomAccessFile _reader;
    private readonly bool _ownsReader;
    private readonly string[] _arraySpecs;
    private readonly string[] _layoutSpecs;
    private readonly SegmentLocator[] _segmentSpecs;
    private readonly byte[] _dtypeBytes;
    private readonly byte[] _layoutBytes;
    private bool _disposed;

    /// <summary>The file format version reported in the EndOfFile struct (currently always 1).</summary>
    public ushort FormatVersion { get; }

    /// <summary>Total file length in bytes.</summary>
    public long FileLength { get; }

    /// <summary>
    /// Arrow schema derived from the file's root <see cref="DType"/>.
    /// The root must be a Struct; non-struct roots are rejected at open time.
    /// </summary>
    public Apache.Arrow.Schema Schema { get; }

    /// <summary>Total number of logical rows reported by the root layout.</summary>
    public long NumberOfRows => checked((long)RootLayout.RowCount);

    /// <summary>Materialized root layout tree. Internal until a stable API surface is defined.</summary>
    internal VortexLayout RootLayout { get; }

    private readonly ColumnPlan[] _columnPlans;
    internal IReadOnlyList<ColumnPlan> ColumnPlans => _columnPlans;

    internal IReadOnlyList<string> ArraySpecs => _arraySpecs;
    internal IReadOnlyList<string> LayoutSpecs => _layoutSpecs;
    internal IReadOnlyList<SegmentLocator> SegmentSpecs => _segmentSpecs;

    /// <summary>Decompressed bytes of the root <c>DType</c> FlatBuffer.</summary>
    internal ReadOnlyMemory<byte> DTypeBytes => _dtypeBytes;

    /// <summary>Decompressed bytes of the root <c>Layout</c> FlatBuffer.</summary>
    internal ReadOnlyMemory<byte> LayoutBytes => _layoutBytes;

    private VortexFileReader(
        IRandomAccessFile reader,
        bool ownsReader,
        ushort formatVersion,
        long fileLength,
        Apache.Arrow.Schema schema,
        VortexLayout rootLayout,
        ColumnPlan[] columnPlans,
        string[] arraySpecs,
        string[] layoutSpecs,
        SegmentLocator[] segmentSpecs,
        byte[] dtypeBytes,
        byte[] layoutBytes)
    {
        _reader = reader;
        _ownsReader = ownsReader;
        FormatVersion = formatVersion;
        FileLength = fileLength;
        Schema = schema;
        RootLayout = rootLayout;
        _columnPlans = columnPlans;
        _arraySpecs = arraySpecs;
        _layoutSpecs = layoutSpecs;
        _segmentSpecs = segmentSpecs;
        _dtypeBytes = dtypeBytes;
        _layoutBytes = layoutBytes;
    }

    public static async Task<VortexFileReader> OpenAsync(
        string path,
        bool useLargeList = false,
        CancellationToken cancellationToken = default)
    {
        var reader = new LocalRandomAccessFile(path);
        try
        {
            return await OpenAsync(reader, ownsReader: true, useLargeList, cancellationToken)
                .ConfigureAwait(false);
        }
        catch
        {
            reader.Dispose();
            throw;
        }
    }

    public static async Task<VortexFileReader> OpenAsync(
        IRandomAccessFile reader,
        bool ownsReader = false,
        bool useLargeList = false,
        CancellationToken cancellationToken = default)
    {
        long fileLength = await reader.GetLengthAsync(cancellationToken).ConfigureAwait(false);
        if (fileLength < VortexFileFormat.MinFileSize)
            throw new VortexFormatException(
                $"File is too small to be a Vortex file: {fileLength} bytes (minimum {VortexFileFormat.MinFileSize}).");

        int tailSize = (int)Math.Min(fileLength, VortexFileFormat.DefaultTailReadSize);
        long tailOffset = fileLength - tailSize;

        using IMemoryOwner<byte> tailOwner = await reader
            .ReadAsync(new FileRange(tailOffset, tailSize), cancellationToken)
            .ConfigureAwait(false);
        ReadOnlyMemory<byte> tail = tailOwner.Memory.Slice(0, tailSize);
        ReadOnlySpan<byte> tailSpan = tail.Span;

        // EndOfFile: version:u16 | postscript_len:u16 | 'VTXF'
        var eof = tailSpan.Slice(tailSize - VortexFileFormat.EndOfFileSize);
        if (BinaryPrimitives.ReadUInt32LittleEndian(eof.Slice(4)) != VortexFileFormat.MagicLE)
            throw new VortexFormatException("Missing 'VTXF' magic at end of file.");

        var version = BinaryPrimitives.ReadUInt16LittleEndian(eof);
        var postscriptLen = BinaryPrimitives.ReadUInt16LittleEndian(eof.Slice(2));
        if (postscriptLen == 0 || postscriptLen > VortexFileFormat.MaxPostscriptLen)
            throw new VortexFormatException(
                $"Postscript length {postscriptLen} is out of range (1..{VortexFileFormat.MaxPostscriptLen}).");

        long postscriptStart = fileLength - VortexFileFormat.EndOfFileSize - postscriptLen;
        if (postscriptStart < VortexFileFormat.LeadingMagicSize)
            throw new VortexFormatException(
                $"Postscript would start at file offset {postscriptStart}, before the leading magic.");

        // The postscript is part of the tail by construction (postscript ≤ 65528,
        // tail ≥ 65536 when the file is at least that big; for smaller files the
        // tail covers the whole file).
        if (postscriptStart < tailOffset)
            throw new VortexFormatException(
                $"Postscript at {postscriptStart} is outside the {tailSize}-byte tail starting at {tailOffset}. " +
                "Vortex requires the postscript to fit in the initial tail read.");

        var postscriptOffsetInTail = checked((int)(postscriptStart - tailOffset));
        var postscript = Postscript.ReadRoot(tailSpan.Slice(postscriptOffsetInTail, postscriptLen));

        if (!postscript.Layout.IsPresent)
            throw new VortexFormatException("Postscript is missing the required 'layout' segment pointer.");
        if (!postscript.Footer.IsPresent)
            throw new VortexFormatException("Postscript is missing the required 'footer' segment pointer.");

        // Materialize each postscript-resident segment locator. Encryption is rejected.
        SegmentLocator? dtypeLoc = postscript.DType.IsPresent ? FromPostscriptSegment(postscript.DType) : null;
        SegmentLocator footerLoc = FromPostscriptSegment(postscript.Footer);
        SegmentLocator layoutLoc = FromPostscriptSegment(postscript.Layout);

        // Verify leading magic. The tail covers the whole file when fileLength <= tailSize;
        // otherwise we need a 4-byte read at offset 0.
        if (tailOffset == 0)
        {
            ValidateLeadingMagic(tailSpan);
        }
        else
        {
            using var headOwner = await reader.ReadAsync(
                new FileRange(0, VortexFileFormat.LeadingMagicSize), cancellationToken)
                .ConfigureAwait(false);
            ValidateLeadingMagic(headOwner.Memory.Span);
        }

        // Fetch + decompress the DType, Footer, Layout segments. All three are typically
        // small; we read each one separately rather than coalescing for clarity.
        byte[] footerBytes = await ReadSegmentAsync(
            reader, footerLoc, tail, tailOffset, label: "Footer", cancellationToken)
            .ConfigureAwait(false);
        byte[] layoutBytes = await ReadSegmentAsync(
            reader, layoutLoc, tail, tailOffset, label: "Layout", cancellationToken)
            .ConfigureAwait(false);
        byte[] dtypeBytes = dtypeLoc is { } dl
            ? await ReadSegmentAsync(reader, dl, tail, tailOffset, label: "DType", cancellationToken)
                .ConfigureAwait(false)
            : Array.Empty<byte>();

        // Parse Footer → materialize the registries.
        var footer = Footer.ReadRoot(footerBytes);
        var arraySpecs = new string[footer.ArraySpecs.Length];
        for (int i = 0; i < arraySpecs.Length; i++)
            arraySpecs[i] = footer.ArraySpecId(i);
        var layoutSpecs = new string[footer.LayoutSpecs.Length];
        for (int i = 0; i < layoutSpecs.Length; i++)
            layoutSpecs[i] = footer.LayoutSpecId(i);

        var compressionSpecCount = footer.CompressionSpecs.Length;
        var encryptionSpecCount = footer.EncryptionSpecs.Length;

        var segCount = footer.SegmentSpecs.Length;
        var segmentSpecs = new SegmentLocator[segCount];
        for (int i = 0; i < segCount; i++)
        {
            var s = footer.SegmentSpec(i);
            CompressionCodec codec = s.CompressionIndex == 0
                ? CompressionCodec.Uncompressed
                : ResolveFooterCodec(s.CompressionIndex, compressionSpecCount, footer);

            if (s.EncryptionIndex != 0)
                throw new VortexFormatException(
                    $"Encrypted segment_specs[{i}] (encryption_idx={s.EncryptionIndex}) is not supported.");
            _ = encryptionSpecCount; // currently no encryption support; silence unused warning

            segmentSpecs[i] = new SegmentLocator(
                s.Offset, s.Length, s.AlignmentExponent, codec);
        }

        // Materialize the Arrow schema. The DType segment is required for the
        // Arrow-facing API — non-struct roots are rejected here.
        if (dtypeBytes.Length == 0)
            throw new VortexFormatException(
                "File has no DType segment; the Arrow-facing reader requires a Struct root dtype.");
        var dtype = DType.ReadRoot(dtypeBytes);
        var schema = VortexSchemaConverter.ToArrowSchema(dtype, useLargeList);

        // Materialize the layout tree. layoutBytes can never be empty — the
        // postscript checks that the layout segment is required.
        var rootLayout = VortexLayoutParser.Parse(layoutBytes, layoutSpecs);
        var columnPlans = LayoutPlanner.Plan(schema, rootLayout);

        return new VortexFileReader(
            reader, ownsReader, version, fileLength, schema, rootLayout, columnPlans,
            arraySpecs, layoutSpecs, segmentSpecs,
            dtypeBytes, layoutBytes);
    }

    private static SegmentLocator FromPostscriptSegment(PostscriptSegment seg)
    {
        if (seg.Encryption.IsPresent)
            throw new VortexFormatException("Encrypted postscript segments are not supported.");
        var scheme = seg.Compression.IsPresent
            ? seg.Compression.Scheme
            : CompressionScheme.None;
        return new SegmentLocator(
            seg.Offset, seg.Length, seg.AlignmentExponent,
            SegmentLocator.MapScheme(scheme));
    }

    private static CompressionCodec ResolveFooterCodec(
        byte compressionIndex, int compressionSpecCount, Footer footer)
    {
        // compression_specs is dictionary-encoded; index 0 is reserved for None
        // (i.e., "no compression"). Index 1 is the first actual entry.
        if (compressionIndex > compressionSpecCount)
            throw new VortexFormatException(
                $"segment_specs references compression index {compressionIndex} but only {compressionSpecCount} compression_specs are defined.");
        var spec = new CompressionSpec(footer.CompressionSpecs.Table(compressionIndex - 1));
        return SegmentLocator.MapScheme(spec.Scheme);
    }

    private static void ValidateLeadingMagic(ReadOnlySpan<byte> headBytes)
    {
        if (BinaryPrimitives.ReadUInt32LittleEndian(headBytes) != VortexFileFormat.MagicLE)
            throw new VortexFormatException("Missing 'VTXF' magic at start of file.");
    }

    /// <summary>
    /// Reads a postscript-resident segment, slicing from the cached tail when
    /// possible. Decompresses according to <see cref="SegmentLocator.Codec"/>.
    /// </summary>
    private static async Task<byte[]> ReadSegmentAsync(
        IRandomAccessFile reader,
        SegmentLocator loc,
        ReadOnlyMemory<byte> tail,
        long tailOffset,
        string label,
        CancellationToken cancellationToken)
    {
        if (loc.Length == 0)
            return Array.Empty<byte>();
        if ((long)loc.Offset + loc.Length > tailOffset + tail.Length)
        {
            // Not entirely in the tail — fetch from file.
            using var owner = await reader.ReadAsync(
                new FileRange(checked((long)loc.Offset), checked((int)loc.Length)), cancellationToken)
                .ConfigureAwait(false);
            return Decompress(owner.Memory.Span, loc, label);
        }

        var startInTail = checked((int)((long)loc.Offset - tailOffset));
        return Decompress(tail.Span.Slice(startInTail, checked((int)loc.Length)), loc, label);
    }

    private static byte[] Decompress(ReadOnlySpan<byte> compressed, SegmentLocator loc, string label)
    {
        if (loc.Codec == CompressionCodec.Uncompressed)
            return compressed.ToArray();

        // The Vortex spec does not carry an uncompressed length on a segment, so
        // we have no upfront size for decompression. Wire this up alongside the
        // first fixture that exercises it.
        throw new NotSupportedException(
            $"Decompressing the {label} segment with codec {loc.Codec} is not yet implemented. " +
            "Add support and a fixture that exercises it.");
    }

    /// <summary>
    /// Reads and decodes one top-level column from the file as a single
    /// Apache Arrow array, materialising every chunk that backs the
    /// column. For multi-chunk columns each chunk is read in order and
    /// the results are concatenated via
    /// <see cref="Apache.Arrow.ArrowArrayConcatenator"/>.
    ///
    /// <para>Use this when you only need one column from a multi-column
    /// file — it avoids the per-batch cost of decoding every column on
    /// each chunk that <see cref="ReadAllAsync(System.Threading.CancellationToken)"/>
    /// pays. For column projection across multiple columns / chunked
    /// streaming, use the
    /// <see cref="ReadAllAsync(System.Collections.Generic.IReadOnlyList{int}, System.Threading.CancellationToken)"/>
    /// projection overload instead.</para>
    /// </summary>
    public async Task<Apache.Arrow.IArrowArray> ReadColumnAsync(
        int fieldIndex, CancellationToken cancellationToken = default)
    {
        if ((uint)fieldIndex >= (uint)_columnPlans.Length)
            throw new ArgumentOutOfRangeException(nameof(fieldIndex), fieldIndex,
                $"fieldIndex must be in [0, {_columnPlans.Length}).");

        var plan = _columnPlans[fieldIndex];
        if (plan.ChunkCount == 0)
            throw new VortexFormatException(
                $"Column {fieldIndex} ({Schema.FieldsList[fieldIndex].Name}) has no chunks.");
        if (plan.ChunkCount == 1)
            return await ReadPlanChunkAsync(plan, chunkIndex: 0, cancellationToken).ConfigureAwait(false);

        var pieces = new Apache.Arrow.IArrowArray[plan.ChunkCount];
        for (int i = 0; i < plan.ChunkCount; i++)
            pieces[i] = await ReadPlanChunkAsync(plan, i, cancellationToken).ConfigureAwait(false);
        return Apache.Arrow.ArrowArrayConcatenator.Concatenate(pieces);
    }

    /// <summary>
    /// Streams the entire file as Arrow <see cref="RecordBatch"/>es, one per
    /// chunk in the layout tree. For files without chunking (the common case
    /// today) this yields exactly one batch with all rows.
    /// </summary>
    public IAsyncEnumerable<Apache.Arrow.RecordBatch> ReadAllAsync(
        CancellationToken cancellationToken = default)
        => ReadAllCoreAsync(acceptedZones: null, columnIndices: null, cancellationToken);

    /// <summary>
    /// Streams the file as Arrow <see cref="RecordBatch"/>es, pruned by
    /// <paramref name="predicate"/> against the per-zone stats. Predicates
    /// are conservative — zones whose stats prove no row can match are
    /// skipped at decode time; zones with missing stats or matching ranges
    /// are kept and the caller is expected to apply a row-level filter.
    ///
    /// <para>Equivalent to:
    /// <c>ReadAllAsync(await predicate.EvaluateZonesAsync(this, ...))</c>
    /// — the predicate evaluates lazily against each column's
    /// <see cref="GetZoneStatsAsync"/> result.</para>
    /// </summary>
    public async IAsyncEnumerable<Apache.Arrow.RecordBatch> ReadAllAsync(
        Predicate predicate,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (predicate is null) throw new ArgumentNullException(nameof(predicate));
        await foreach (var batch in ReadAllWithPredicateAsync(
            predicate, columnIndices: null, cancellationToken).ConfigureAwait(false))
            yield return batch;
    }

    /// <summary>
    /// Streams the file as Arrow <see cref="RecordBatch"/>es, optionally
    /// filtered by zone. When <paramref name="acceptedZones"/> is non-null,
    /// only chunks whose zone index is in the set are decoded — letting
    /// callers prune whole zones based on the per-column stats returned
    /// from <see cref="GetZoneStatsAsync"/>.
    ///
    /// <para>For files written with <c>preserveStats: true</c> using uniform
    /// batch sizes, chunk index == zone index. For non-zoned files
    /// <paramref name="acceptedZones"/> still filters the chunk index but
    /// the caller has no per-zone stats to drive the decision.</para>
    /// </summary>
    public IAsyncEnumerable<Apache.Arrow.RecordBatch> ReadAllAsync(
        ISet<int>? acceptedZones,
        CancellationToken cancellationToken = default)
        => ReadAllCoreAsync(acceptedZones, columnIndices: null, cancellationToken);

    /// <summary>
    /// Streams a column-projected view of the file: each emitted
    /// <see cref="RecordBatch"/> contains only the columns whose indices
    /// appear in <paramref name="columnIndices"/>, in the order given.
    /// Skipped columns are never decoded — useful when the caller only
    /// needs a few columns from a wide file.
    /// </summary>
    public IAsyncEnumerable<Apache.Arrow.RecordBatch> ReadAllAsync(
        IReadOnlyList<int> columnIndices,
        CancellationToken cancellationToken = default)
    {
        ValidateColumnIndices(columnIndices);
        return ReadAllCoreAsync(acceptedZones: null, columnIndices, cancellationToken);
    }

    /// <summary>
    /// Streams a column-projected view of the file, pruned by
    /// <paramref name="predicate"/>. The predicate evaluates against the
    /// underlying zone stats — its referenced columns don't need to be in
    /// <paramref name="columnIndices"/>.
    /// </summary>
    public async IAsyncEnumerable<Apache.Arrow.RecordBatch> ReadAllAsync(
        IReadOnlyList<int> columnIndices,
        Predicate predicate,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (predicate is null) throw new ArgumentNullException(nameof(predicate));
        ValidateColumnIndices(columnIndices);
        await foreach (var batch in ReadAllWithPredicateAsync(
            predicate, columnIndices, cancellationToken).ConfigureAwait(false))
            yield return batch;
    }

    /// <summary>
    /// Streams a column-projected view of the file, optionally filtered by
    /// zone. <paramref name="acceptedZones"/> follows the same convention
    /// as <see cref="ReadAllAsync(ISet{int}?, CancellationToken)"/>:
    /// non-null limits the chunk indices decoded.
    /// </summary>
    public IAsyncEnumerable<Apache.Arrow.RecordBatch> ReadAllAsync(
        IReadOnlyList<int> columnIndices,
        ISet<int>? acceptedZones,
        CancellationToken cancellationToken = default)
    {
        ValidateColumnIndices(columnIndices);
        return ReadAllCoreAsync(acceptedZones, columnIndices, cancellationToken);
    }

    private void ValidateColumnIndices(IReadOnlyList<int> columnIndices)
    {
        if (columnIndices is null) throw new ArgumentNullException(nameof(columnIndices));
        if (columnIndices.Count == 0)
            throw new ArgumentException(
                "Column projection list is empty; specify at least one column index.",
                nameof(columnIndices));
        for (int i = 0; i < columnIndices.Count; i++)
        {
            int idx = columnIndices[i];
            if ((uint)idx >= (uint)_columnPlans.Length)
                throw new ArgumentOutOfRangeException(nameof(columnIndices),
                    $"columnIndices[{i}] = {idx} is outside [0, {_columnPlans.Length}).");
        }
    }

    private async IAsyncEnumerable<Apache.Arrow.RecordBatch> ReadAllWithPredicateAsync(
        Predicate predicate, IReadOnlyList<int>? columnIndices,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        if (_columnPlans.Length == 0) yield break;
        int totalZones = _columnPlans[0].ChunkCount;
        var accepted = await predicate.EvaluateZonesAsync(this, totalZones, cancellationToken)
            .ConfigureAwait(false);
        await foreach (var batch in ReadAllCoreAsync(accepted, columnIndices, cancellationToken)
            .ConfigureAwait(false))
            yield return batch;
    }

    /// <summary>
    /// Shared chunk-streaming loop. <paramref name="columnIndices"/>
    /// defaults (null) to "all columns in schema order"; otherwise only the
    /// listed columns are decoded and the emitted batch's schema is
    /// projected to that subset. <paramref name="acceptedZones"/> defaults
    /// (null) to "all chunks"; otherwise only chunks whose index is in the
    /// set are emitted.
    /// </summary>
    private async IAsyncEnumerable<Apache.Arrow.RecordBatch> ReadAllCoreAsync(
        ISet<int>? acceptedZones,
        IReadOnlyList<int>? columnIndices,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        if (_columnPlans.Length == 0)
            yield break;

        int chunkCount = _columnPlans[0].ChunkCount;
        for (int i = 1; i < _columnPlans.Length; i++)
        {
            if (_columnPlans[i].ChunkCount != chunkCount)
                throw new NotSupportedException(
                    $"Column {i} has {_columnPlans[i].ChunkCount} chunks but column 0 has {chunkCount}. " +
                    "Per-column chunking with mismatched chunk counts is not yet supported.");
        }

        // Pre-compute the projected schema once. ValidateColumnIndices
        // ensures the list is non-empty, so colsToRead >= 1 — the
        // first-column rowCount fallback is always reachable.
        var (projectedSchema, projectedIndices) = columnIndices is null
            ? (Schema, null)
            : ProjectSchema(columnIndices);

        int colsToRead = projectedIndices?.Length ?? _columnPlans.Length;

        for (int chunkIdx = 0; chunkIdx < chunkCount; chunkIdx++)
        {
            if (acceptedZones is not null && !acceptedZones.Contains(chunkIdx)) continue;
            var arrays = new Apache.Arrow.IArrowArray[colsToRead];
            int rowCount = -1;
            for (int outIdx = 0; outIdx < colsToRead; outIdx++)
            {
                int srcIdx = projectedIndices is null ? outIdx : projectedIndices[outIdx];
                arrays[outIdx] = await ReadPlanChunkAsync(_columnPlans[srcIdx], chunkIdx, cancellationToken)
                    .ConfigureAwait(false);
                var len = arrays[outIdx].Length;
                if (rowCount < 0) rowCount = len;
                else if (len != rowCount)
                    throw new VortexFormatException(
                        $"Chunk {chunkIdx}: first read column has {rowCount} rows but column index {srcIdx} has {len}.");
            }
            yield return new Apache.Arrow.RecordBatch(projectedSchema, arrays, rowCount);
        }
    }

    private (Apache.Arrow.Schema, int[]) ProjectSchema(IReadOnlyList<int> columnIndices)
    {
        var projectedFields = new Apache.Arrow.Field[columnIndices.Count];
        var indices = new int[columnIndices.Count];
        for (int i = 0; i < columnIndices.Count; i++)
        {
            int srcIdx = columnIndices[i];
            projectedFields[i] = Schema.FieldsList[srcIdx];
            indices[i] = srcIdx;
        }
        var projectedSchema = new Apache.Arrow.Schema(projectedFields, metadata: null);
        return (projectedSchema, indices);
    }

    private async Task<Apache.Arrow.IArrowArray> ReadPlanChunkAsync(
        ColumnPlan plan, int chunkIndex, CancellationToken cancellationToken)
    {
        switch (plan)
        {
            case FlatColumnPlan flat:
                return await ReadFlatChunkAsync(flat, chunkIndex, cancellationToken).ConfigureAwait(false);
            case DictColumnPlan dict:
                {
                    // Dict reconstruction: read the values dict (single chunk
                    // for now — dict layouts always have 1 chunk in the values
                    // sub-plan) and the per-row codes for this chunk index.
                    var values = await ReadPlanChunkAsync(dict.Values, 0, cancellationToken)
                        .ConfigureAwait(false);
                    var codes = await ReadPlanChunkAsync(dict.Codes, chunkIndex, cancellationToken)
                        .ConfigureAwait(false);
                    return DictReconstructor.Reconstruct(plan.ArrowType, values, codes);
                }
            default:
                throw new NotSupportedException(
                    $"Column plan type {plan.GetType().Name} is not yet supported.");
        }
    }

    /// <summary>
    /// Returns the per-zone stats table for column <paramref name="fieldIndex"/>,
    /// or <c>null</c> if the column wasn't written with a <c>vortex.stats</c>
    /// (zoned) layout. Materializes the stats segment lazily — the file open
    /// path doesn't decode it.
    ///
    /// <para>Use the returned <see cref="ZoneStats"/> to derive a
    /// <see cref="HashSet{T}"/> of accepted zone indices, then pass that set
    /// to <see cref="ReadAllAsync(System.Collections.Generic.IReadOnlySet{int}?, CancellationToken)"/>
    /// to skip whole zones at decode time.</para>
    /// </summary>
    public async Task<ZoneStats?> GetZoneStatsAsync(
        int fieldIndex, CancellationToken cancellationToken = default)
    {
        if ((uint)fieldIndex >= (uint)_columnPlans.Length)
            throw new ArgumentOutOfRangeException(nameof(fieldIndex), fieldIndex,
                $"fieldIndex must be in [0, {_columnPlans.Length}).");
        var plan = _columnPlans[fieldIndex];
        if (plan.ZoneInfo is null) return null;
        var zi = plan.ZoneInfo;

        var locator = _segmentSpecs[(int)zi.ZonesSegmentRef];
        using var owner = await _reader.ReadAsync(
            new FileRange(checked((long)locator.Offset), checked((int)locator.Length)),
            cancellationToken).ConfigureAwait(false);
        var compressed = owner.Memory.Span;
        var raw = locator.Codec == CompressionCodec.Uncompressed
            ? compressed
            : throw new NotSupportedException(
                $"Decompressing the zones segment with codec {locator.Codec} is not yet implemented.");
        var serialized = SerializedArray.Parse(raw);

        // The zones segment contains a vortex.struct ArrayNode whose fields
        // correspond to PresentStats (with min/max followed by their
        // truncation flag). Build the matching Arrow struct dtype, decode,
        // then map fields back to ZoneStats.
        var structType = ZoneStatsLayout.BuildStructType(plan.ArrowType, zi.PresentStats);
        var structArray = (Apache.Arrow.StructArray)ArrayDecoder.Decode(
            serialized, _arraySpecs, structType, zi.ZoneCount);
        return ZoneStatsLayout.FromStruct(structArray, plan.ArrowType, zi);
    }

    private async Task<Apache.Arrow.IArrowArray> ReadFlatChunkAsync(
        FlatColumnPlan plan, int chunkIndex, CancellationToken cancellationToken)
    {
        var chunk = plan.Chunks[chunkIndex];
        var locator = _segmentSpecs[(int)chunk.SegmentRef];

        using var owner = await _reader.ReadAsync(
            new FileRange(checked((long)locator.Offset), checked((int)locator.Length)),
            cancellationToken).ConfigureAwait(false);

        var compressed = owner.Memory.Span;
        var raw = locator.Codec == CompressionCodec.Uncompressed
            ? compressed
            : throw new NotSupportedException(
                $"Decompressing segments with codec {locator.Codec} is not yet implemented.");

        var serialized = SerializedArray.Parse(raw);
        return ArrayDecoder.Decode(serialized, _arraySpecs, plan.ArrowType, checked((long)chunk.RowCount));
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        if (_ownsReader) _reader.Dispose();
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;
        if (_ownsReader) await _reader.DisposeAsync().ConfigureAwait(false);
    }
}
