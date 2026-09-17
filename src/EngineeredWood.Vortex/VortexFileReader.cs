// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using EngineeredWood.Compression;
using EngineeredWood.Expressions;
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

        // The postscript is part of the tail by construction (postscript ≤ 65527,
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
        if ((long)loc.Offset < tailOffset || (long)loc.Offset + loc.Length > tailOffset + tail.Length)
        {
            // Not entirely in the tail (it can start before the tail's first
            // byte as well as end past its last) — fetch from file.
            using var owner = await reader.ReadAsync(
                new FileRange(checked((long)loc.Offset), checked((int)loc.Length)), cancellationToken)
                .ConfigureAwait(false);
            return Decompress(owner.Memory.Span, loc, label);
        }

        var startInTail = checked((int)((long)loc.Offset - tailOffset));
        return Decompress(tail.Span.Slice(startInTail, checked((int)loc.Length)), loc, label);
    }

    /// <summary>
    /// Segment compression is a reserved field in Vortex's footer and postscript: the format
    /// names the codecs, but no upstream writer sets it (vortex 0.86) and a segment carries no
    /// uncompressed length to decompress to. There is nothing to implement until the format
    /// defines it, so a file that sets it is refused.
    /// </summary>
    private static NotSupportedException UndefinedSegmentCompression(string label, CompressionCodec codec) =>
        new($"The {label} segment declares {codec} compression, which the Vortex format reserves but does " +
            "not define: no writer produces it, and a segment records no uncompressed length.");

    private static byte[] Decompress(ReadOnlySpan<byte> compressed, SegmentLocator loc, string label)
    {
        if (loc.Codec == CompressionCodec.Uncompressed)
            return compressed.ToArray();

        throw UndefinedSegmentCompression(label, loc.Codec);
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
    /// Streams the entire file as Arrow <see cref="Apache.Arrow.RecordBatch"/>es, one per
    /// chunk in the layout tree. For files without chunking (the common case
    /// today) this yields exactly one batch with all rows.
    /// </summary>
    public IAsyncEnumerable<Apache.Arrow.RecordBatch> ReadAllAsync(
        CancellationToken cancellationToken = default)
        => ReadAllCoreAsync(acceptedZones: null, columnIndices: null, rowOffset: 0, rowCount: long.MaxValue, cancellationToken);

    /// <summary>
    /// Streams the file as Arrow <see cref="Apache.Arrow.RecordBatch"/>es, pruned by
    /// <paramref name="predicate"/> (from the shared
    /// <see cref="EngineeredWood.Expressions"/> library) against the per-zone
    /// stats. Predicates are conservative — zones whose stats prove no row
    /// can match are skipped at decode time; zones with missing stats or
    /// matching ranges are kept and the caller is expected to apply a
    /// row-level filter.
    ///
    /// <para>The predicate's <see cref="UnboundReference"/> nodes are matched
    /// against the file's <see cref="Schema"/> by name; references to columns
    /// not in the schema cause the affected zone to be kept conservatively.
    /// Build predicates with <c>EngineeredWood.Expressions.Expressions</c>
    /// (e.g. <c>Expressions.GreaterThanOrEqual("v", LiteralValue.Of(100L))</c>)
    /// or a higher-level translator (Spark SQL, Substrait, etc.).</para>
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
    /// Streams the file as Arrow <see cref="Apache.Arrow.RecordBatch"/>es, optionally
    /// filtered by zone. When <paramref name="acceptedZones"/> is non-null,
    /// only rows in zones whose index is in the set are decoded — letting
    /// callers prune whole zones based on the per-column stats returned
    /// from <see cref="GetZoneStatsAsync"/>.
    ///
    /// <para>Zone <c>z</c> covers rows <c>[z * ZoneLen, (z + 1) * ZoneLen)</c>
    /// of <see cref="ZoneStats.ZoneLen"/>, whatever the columns' chunking. For
    /// files written with <c>preserveStats: true</c> that is one chunk per zone.
    /// For files without zone maps <paramref name="acceptedZones"/> filters
    /// chunk indices of the first column, but the caller has no per-zone stats
    /// to drive the decision.</para>
    /// </summary>
    public IAsyncEnumerable<Apache.Arrow.RecordBatch> ReadAllAsync(
        ISet<int>? acceptedZones,
        CancellationToken cancellationToken = default)
        => ReadAllCoreAsync(acceptedZones, columnIndices: null, rowOffset: 0, rowCount: long.MaxValue, cancellationToken);

    /// <summary>
    /// Streams a column-projected view of the file: each emitted
    /// <see cref="Apache.Arrow.RecordBatch"/> contains only the columns whose indices
    /// appear in <paramref name="columnIndices"/>, in the order given.
    /// Skipped columns are never decoded — useful when the caller only
    /// needs a few columns from a wide file.
    /// </summary>
    public IAsyncEnumerable<Apache.Arrow.RecordBatch> ReadAllAsync(
        IReadOnlyList<int> columnIndices,
        CancellationToken cancellationToken = default)
    {
        ValidateColumnIndices(columnIndices);
        return ReadAllCoreAsync(acceptedZones: null, columnIndices, rowOffset: 0, rowCount: long.MaxValue, cancellationToken);
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
        return ReadAllCoreAsync(acceptedZones, columnIndices, rowOffset: 0, rowCount: long.MaxValue, cancellationToken);
    }

    /// <summary>
    /// Streams a row-range slice of the file: only logical rows in
    /// <c>[rowOffset, rowOffset + rowCount)</c> are yielded. Chunks fully
    /// outside the range incur zero I/O — they're identified by per-chunk
    /// row counts already known at open time, so they're skipped without
    /// reading any segments. Boundary chunks (at most two) are decoded
    /// fully and then sliced via <see cref="Apache.Arrow.RecordBatch.Slice(int, int)"/>,
    /// which is an O(1) per-array offset/length adjustment.
    ///
    /// <para>Composes with column projection and zone-pruning predicates;
    /// when both are supplied the predicate runs first (over zone stats),
    /// then the row-range further trims the surviving chunks.</para>
    ///
    /// <para>If <paramref name="rowOffset"/> is past the end of the file,
    /// no batches are yielded. <paramref name="rowCount"/> is clamped to
    /// what's available, so passing <see cref="long.MaxValue"/> reads
    /// from <paramref name="rowOffset"/> to end.</para>
    /// </summary>
    public IAsyncEnumerable<Apache.Arrow.RecordBatch> ReadAllAsync(
        long rowOffset,
        long rowCount,
        IReadOnlyList<int>? columnIndices = null,
        Predicate? predicate = null,
        CancellationToken cancellationToken = default)
    {
        if (rowOffset < 0)
            throw new ArgumentOutOfRangeException(nameof(rowOffset), rowOffset, "rowOffset must be ≥ 0.");
        if (rowCount < 0)
            throw new ArgumentOutOfRangeException(nameof(rowCount), rowCount, "rowCount must be ≥ 0.");
        if (columnIndices is not null) ValidateColumnIndices(columnIndices);
        return ReadAllRangedAsync(rowOffset, rowCount, columnIndices, predicate, cancellationToken);
    }

    private async IAsyncEnumerable<Apache.Arrow.RecordBatch> ReadAllRangedAsync(
        long rowOffset, long rowCount,
        IReadOnlyList<int>? columnIndices,
        Predicate? predicate,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        IReadOnlyList<RowRange>? acceptedRanges = null;
        if (predicate is not null && _columnPlans.Length > 0)
        {
            acceptedRanges = await EvaluatePredicateRangesAsync(predicate, cancellationToken)
                .ConfigureAwait(false);
        }
        await foreach (var batch in ReadRangesAsync(
            acceptedRanges, columnIndices, rowOffset, rowCount, cancellationToken).ConfigureAwait(false))
            yield return batch;
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
        var accepted = await EvaluatePredicateRangesAsync(predicate, cancellationToken)
            .ConfigureAwait(false);
        await foreach (var batch in ReadRangesAsync(accepted, columnIndices, rowOffset: 0, rowCount: long.MaxValue, cancellationToken)
            .ConfigureAwait(false))
            yield return batch;
    }

    /// <summary>
    /// Walks <paramref name="predicate"/>, pre-loads the per-zone stats for
    /// every column it references (one <see cref="GetZoneStatsAsync"/> call
    /// per referenced column), then iterates the zones and runs the shared
    /// <see cref="StatisticsEvaluator"/> against a per-zone cursor. Returns
    /// the row ranges of the zones that aren't proven to contain no matches,
    /// or null to read everything.
    /// </summary>
    /// <remarks>
    /// The cursor addresses every column's stats by one zone index, which is
    /// only meaningful when the referenced columns share a zone length. They
    /// always do for files from this writer and from upstream's default
    /// strategy. When they don't, or when no referenced column has zone
    /// stats, see <see cref="EvaluateWithoutZoneStats"/>.
    /// </remarks>
    private async Task<IReadOnlyList<RowRange>?> EvaluatePredicateRangesAsync(
        Predicate predicate, CancellationToken cancellationToken)
    {
        // Collect referenced column names once. Predicates over columns that
        // aren't in the schema, or that the predicate doesn't reference at
        // all, don't trigger any stats fetch.
        var referenced = VortexZoneStatsAccessor.CollectReferencedColumns(predicate);
        var statsByColumn = new Dictionary<string, ZoneStats?>(referenced.Count, StringComparer.Ordinal);
        int zoneLen = 0, zoneCount = 0;
        foreach (var name in referenced)
        {
            int idx = FindFieldIndex(name);
            // Unresolvable reference → null entry → accessor returns null for
            // min/max/etc. → evaluator stays at Unknown → zone kept.
            var stats = idx < 0
                ? null
                : await GetZoneStatsAsync(idx, cancellationToken).ConfigureAwait(false);
            statsByColumn[name] = stats;
            if (stats is null) continue;
            if (zoneLen != 0 && (stats.ZoneLen != zoneLen || stats.ZoneCount != zoneCount))
                return EvaluateWithoutZoneStats(predicate);
            zoneLen = stats.ZoneLen;
            zoneCount = stats.ZoneCount;
        }
        if (zoneLen == 0)
            return EvaluateWithoutZoneStats(predicate);

        var accessor = new VortexZoneStatsAccessor(Schema);
        var cursor = new VortexZoneCursor(statsByColumn);
        var accepted = new List<int>();
        for (int z = 0; z < zoneCount; z++)
        {
            cursor.ZoneIndex = z;
            var result = StatisticsEvaluator.Evaluate(predicate, cursor, accessor);
            if (result != FilterResult.AlwaysFalse)
                accepted.Add(z);
        }
        return ZonesToRowRanges(accepted, zoneLen);
    }

    /// <summary>
    /// Evaluates <paramref name="predicate"/> once with no stats for any column,
    /// for when there are no per-zone stats to evaluate it against. The answer
    /// then can't depend on the data, so it holds for the whole file: nothing to
    /// read when the predicate is <see cref="FilterResult.AlwaysFalse"/> (as
    /// <see cref="Expressions.Expressions.False"/> is), everything otherwise.
    /// </summary>
    private IReadOnlyList<RowRange>? EvaluateWithoutZoneStats(Predicate predicate)
    {
        var cursor = new VortexZoneCursor(
            new Dictionary<string, ZoneStats?>(StringComparer.Ordinal));
        var result = StatisticsEvaluator.Evaluate(
            predicate, cursor, new VortexZoneStatsAccessor(Schema));
        return result == FilterResult.AlwaysFalse ? System.Array.Empty<RowRange>() : null;
    }

    /// <summary>
    /// Zone <c>z</c> of a zone map with length <paramref name="zoneLen"/> covers rows
    /// <c>[z * zoneLen, (z + 1) * zoneLen)</c>, the last zone ending with the file.
    /// </summary>
    private List<RowRange> ZonesToRowRanges(IEnumerable<int> zones, int zoneLen)
    {
        long total = NumberOfRows;
        var ranges = new List<RowRange>();
        foreach (var z in zones)
        {
            if (z < 0) continue;
            long start = (long)z * zoneLen;
            if (start >= total) continue;
            ranges.Add(new RowRange(start, Math.Min(start + zoneLen, total)));
        }
        return ranges;
    }

    /// <summary>
    /// Resolves caller-supplied zone indices (from <see cref="GetZoneStatsAsync"/>)
    /// to row ranges. A file with no zone maps has no zones, so the indices are
    /// taken as chunk indices of the first column, which is what they meant for
    /// the chunk-per-zone files this writer produces.
    /// </summary>
    private List<RowRange> CallerZonesToRowRanges(ISet<int> zones)
    {
        int zoneLen = 0;
        foreach (var plan in _columnPlans)
        {
            if (plan.ZoneInfo is null) continue;
            if (zoneLen != 0 && plan.ZoneInfo.ZoneLen != zoneLen)
                throw new NotSupportedException(
                    "Zone indices are ambiguous: the file's columns have zone maps with different zone lengths.");
            zoneLen = plan.ZoneInfo.ZoneLen;
        }
        if (zoneLen != 0)
            return ZonesToRowRanges(zones.OrderBy(z => z), zoneLen);

        var ranges = new List<RowRange>();
        if (_columnPlans.Length == 0) return ranges;
        var starts = ChunkStarts(_columnPlans[0]);
        for (int c = 0; c < starts.Length - 1; c++)
        {
            if (zones.Contains(c))
                ranges.Add(new RowRange(starts[c], starts[c + 1]));
        }
        return ranges;
    }

    private int FindFieldIndex(string name)
    {
        var fields = Schema.FieldsList;
        for (int i = 0; i < fields.Count; i++)
        {
            if (string.Equals(fields[i].Name, name, StringComparison.Ordinal))
                return i;
        }
        return -1;
    }

    /// <summary>
    /// Streams the file with <paramref name="acceptedZones"/> (null for all
    /// rows) resolved to row ranges; see <see cref="ReadRangesAsync"/>.
    /// </summary>
    private IAsyncEnumerable<Apache.Arrow.RecordBatch> ReadAllCoreAsync(
        ISet<int>? acceptedZones,
        IReadOnlyList<int>? columnIndices,
        long rowOffset,
        long rowCount,
        CancellationToken cancellationToken)
        => ReadRangesAsync(
            acceptedZones is null ? null : CallerZonesToRowRanges(acceptedZones),
            columnIndices, rowOffset, rowCount, cancellationToken);

    /// <summary>
    /// Shared streaming loop. <paramref name="columnIndices"/> defaults
    /// (null) to "all columns in schema order"; otherwise only the listed
    /// columns are decoded and the emitted batch's schema is projected to
    /// that subset. <paramref name="acceptedRanges"/> defaults (null) to
    /// "all rows"; otherwise only rows inside the ranges are emitted.
    /// <paramref name="rowOffset"/> + <paramref name="rowCount"/> further
    /// trim them to a logical row range.
    /// </summary>
    /// <remarks>
    /// Columns may be chunked independently (upstream's writer does), so a
    /// batch ends wherever any read column's chunk ends, as well as at the
    /// edges of the accepted ranges. When every read column shares its
    /// chunk boundaries and whole chunks are accepted, that is one batch per
    /// chunk. A chunk is decoded only if an emitted row falls inside it, and
    /// only once however many batches it is sliced into.
    /// </remarks>
    private async IAsyncEnumerable<Apache.Arrow.RecordBatch> ReadRangesAsync(
        IReadOnlyList<RowRange>? acceptedRanges,
        IReadOnlyList<int>? columnIndices,
        long rowOffset,
        long rowCount,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        if (_columnPlans.Length == 0)
            yield break;
        if (rowCount == 0)
            yield break;

        // Pre-compute the projected schema once. ValidateColumnIndices
        // ensures the list is non-empty, so colsToRead >= 1.
        var (projectedSchema, projectedIndices) = columnIndices is null
            ? (Schema, null)
            : ProjectSchema(columnIndices);

        int colsToRead = projectedIndices?.Length ?? _columnPlans.Length;
        long totalRows = NumberOfRows;
        var columns = new ColumnCursor[colsToRead];
        for (int outIdx = 0; outIdx < colsToRead; outIdx++)
        {
            int srcIdx = projectedIndices is null ? outIdx : projectedIndices[outIdx];
            var starts = ChunkStarts(_columnPlans[srcIdx]);
            if (starts[starts.Length - 1] != totalRows)
                throw new VortexFormatException(
                    $"Column {srcIdx} has {starts[starts.Length - 1]} rows but the file has {totalRows}.");
            columns[outIdx] = new ColumnCursor(_columnPlans[srcIdx], srcIdx, starts);
        }

        // Saturating end: rowOffset + rowCount may overflow long.MaxValue
        // when the caller passes long.MaxValue (= "to end"). Saturate to
        // long.MaxValue so the boundary check stays correct.
        long rangeEnd = rowOffset > long.MaxValue - rowCount ? long.MaxValue : rowOffset + rowCount;
        var ranges = NormalizeRanges(
            acceptedRanges ?? new[] { new RowRange(0, totalRows) },
            Math.Min(rowOffset, totalRows), Math.Min(rangeEnd, totalRows));

        foreach (var range in ranges)
        {
            long pos = range.Start;
            while (pos < range.End)
            {
                long segEnd = range.End;
                foreach (var col in columns)
                    segEnd = Math.Min(segEnd, col.SeekChunkEnd(pos));

                var arrays = new Apache.Arrow.IArrowArray[colsToRead];
                for (int outIdx = 0; outIdx < colsToRead; outIdx++)
                {
                    arrays[outIdx] = await columns[outIdx]
                        .SliceAsync(this, pos, segEnd, cancellationToken)
                        .ConfigureAwait(false);
                }
                yield return new Apache.Arrow.RecordBatch(
                    projectedSchema, arrays, checked((int)(segEnd - pos)));
                pos = segEnd;
            }
        }
    }

    /// <summary>
    /// Clips <paramref name="ranges"/> to <c>[start, end)</c>, then sorts and
    /// merges them so adjacent accepted zones are read as one span.
    /// </summary>
    private static List<RowRange> NormalizeRanges(IEnumerable<RowRange> ranges, long start, long end)
    {
        var clipped = new List<RowRange>();
        foreach (var r in ranges)
        {
            long s = Math.Max(r.Start, start), e = Math.Min(r.End, end);
            if (s < e) clipped.Add(new RowRange(s, e));
        }
        clipped.Sort((a, b) => a.Start.CompareTo(b.Start));

        var merged = new List<RowRange>(clipped.Count);
        foreach (var r in clipped)
        {
            if (merged.Count > 0 && r.Start <= merged[merged.Count - 1].End)
            {
                var last = merged[merged.Count - 1];
                merged[merged.Count - 1] = new RowRange(last.Start, Math.Max(last.End, r.End));
            }
            else
            {
                merged.Add(r);
            }
        }
        return merged;
    }

    /// <summary>Row offsets of each chunk's start, plus the column's total row count.</summary>
    private static long[] ChunkStarts(ColumnPlan plan)
    {
        var starts = new long[plan.ChunkCount + 1];
        for (int c = 0; c < plan.ChunkCount; c++)
            starts[c + 1] = starts[c] + checked((long)plan.ChunkRowCount(c));
        return starts;
    }

    /// <summary>A half-open range of logical rows.</summary>
    private readonly record struct RowRange(long Start, long End);

    /// <summary>
    /// Tracks one column while <see cref="ReadRangesAsync"/> walks forward
    /// through the file, holding the most recently decoded chunk.
    /// </summary>
    private sealed class ColumnCursor
    {
        private readonly ColumnPlan _plan;
        private readonly int _fieldIndex;
        private readonly long[] _starts;
        private int _chunk;
        private int _decodedChunk = -1;
        private Apache.Arrow.IArrowArray? _decoded;

        public ColumnCursor(ColumnPlan plan, int fieldIndex, long[] starts)
        {
            _plan = plan;
            _fieldIndex = fieldIndex;
            _starts = starts;
        }

        /// <summary>
        /// Moves to the chunk holding row <paramref name="pos"/> (rows only
        /// move forward) and returns the row where that chunk ends.
        /// </summary>
        public long SeekChunkEnd(long pos)
        {
            while (_starts[_chunk + 1] <= pos)
                _chunk++;
            return _starts[_chunk + 1];
        }

        /// <summary>
        /// Returns rows <c>[start, end)</c>, which must lie in the current chunk.
        /// </summary>
        public async Task<Apache.Arrow.IArrowArray> SliceAsync(
            VortexFileReader reader, long start, long end, CancellationToken cancellationToken)
        {
            if (_decodedChunk != _chunk)
            {
                _decoded = null;
                var array = await reader.ReadPlanChunkAsync(_plan, _chunk, cancellationToken)
                    .ConfigureAwait(false);
                long expected = _starts[_chunk + 1] - _starts[_chunk];
                if (array.Length != expected)
                    throw new VortexFormatException(
                        $"Column {_fieldIndex} chunk {_chunk}: decoded {array.Length} rows, layout says {expected}.");
                _decoded = array;
                _decodedChunk = _chunk;
            }

            int offset = checked((int)(start - _starts[_chunk]));
            int length = checked((int)(end - start));
            return offset == 0 && length == _decoded!.Length
                ? _decoded
                : Apache.Arrow.ArrowArrayFactory.Slice(_decoded, offset, length);
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
    /// or <c>null</c> if the column wasn't written with a zoned layout
    /// (<c>vortex.stats</c> or <c>vortex.zoned</c>) whose zone map this reader understands. Materializes the stats segment lazily — the file open
    /// path doesn't decode it.
    ///
    /// <para>Use the returned <see cref="ZoneStats"/> to derive a
    /// <see cref="HashSet{T}"/> of accepted zone indices, then pass that set
    /// to <see cref="ReadAllAsync(ISet{int}, CancellationToken)"/>
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
            : throw UndefinedSegmentCompression("zones", locator.Codec);
        var serialized = SerializedArray.Parse(raw);

        // The zones segment contains a vortex.struct ArrayNode whose fields
        // correspond to PresentStats (with min/max followed by their
        // truncation flag). Build the matching Arrow struct dtype, decode,
        // then map fields back to ZoneStats.
        var structType = zi.Aggregates is null
            ? ZoneStatsLayout.BuildStructType(plan.ArrowType, zi.PresentStats)
            : ZonedZoneMap.BuildStructType(plan.ArrowType, zi.Aggregates)!;
        var structArray = (Apache.Arrow.StructArray)ArrayDecoder.Decode(
            serialized, _arraySpecs, structType, zi.ZoneCount);
        return zi.Aggregates is null
            ? ZoneStatsLayout.FromStruct(structArray, plan.ArrowType, zi)
            : ZonedZoneMap.FromStruct(structArray, plan.ArrowType, zi);
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
            : throw UndefinedSegmentCompression("data", locator.Codec);

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
