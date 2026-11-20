// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using Clast.BloomFilter;
using EngineeredWood.Arrow;
using EngineeredWood.Compression;
using EngineeredWood.Parquet.BloomFilter;
using EngineeredWood.Parquet.Metadata;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// Encodes one leaf column's chunk of a row group into Parquet pages: <see cref="Append"/> encodes
/// values into pages and holds them compressed, and <see cref="Finish"/> returns the chunk, with its
/// dictionary page, statistics, page index and Bloom filter.
/// </summary>
/// <remarks>
/// <para>This is the one column-encoding core of #936. Every per-column knob — encodings, the
/// dictionary's first-page test and partial fallback, page size, compression, statistics, Bloom
/// filters, the page index and size statistics — is applied here, so a front end that feeds a row
/// group to it (<see cref="ParquetFileWriter"/>'s whole-batch and streaming row groups; a permuting writer later)
/// only decides what to append.</para>
/// <para>A chunk takes any number of appends (#936 step 2). The open page carries over from one to the next, the
/// dictionary keeps its table, statistics and the Bloom filter accumulate, so a chunk does not depend on how its
/// values were batched; what each piece of state does across appends is set out on the fields below. A chunk of ONE
/// append is cut, encoded and decided exactly as the whole-array writer this replaced, byte for byte.</para>
/// <para>Not thread-safe. Appends and the finish of one chunk run in order; different chunks run in
/// parallel. No state is kept in the thread-static encode buffers between calls, so consecutive calls
/// may run on different threads.</para>
/// </remarks>
internal sealed class ColumnChunkWriter
{
    /// <summary>
    /// Result of encoding a column chunk.
    /// </summary>
    internal readonly struct ColumnChunkResult
    {
        /// <summary>
        /// The pages that precede the data pages — the dictionary page or the FSST symbol table page —
        /// or empty. They are written to the file ahead of <see cref="Data"/>. They are kept apart because
        /// the dictionary page can only be written once every value of the chunk is known, after the data
        /// pages that use it.
        /// </summary>
        public ArraySegment<byte> PrefixData { get; init; }

        /// <summary>The data pages, which follow <see cref="PrefixData"/> in the file.</summary>
        public required ArraySegment<byte> Data { get; init; }
        public required ColumnMetaData MetaData { get; init; }

        /// <summary>
        /// Size of the dictionary page in bytes (including header), or 0 if no dictionary.
        /// Used by the caller to calculate DictionaryPageOffset vs DataPageOffset.
        /// </summary>
        public int DictionaryPageSize { get; init; }

        /// <summary>
        /// Size of the FSST symbol table page in bytes (including header), or 0 if the column
        /// chunk is not FSST-encoded. Like the dictionary page it precedes the data pages, so
        /// the caller adds it when computing DataPageOffset.
        /// </summary>
        public int SymbolTablePageSize { get; init; }

        /// <summary>
        /// Serialized Bloom filter block (Thrift header + bitset), or null if not enabled.
        /// </summary>
        public byte[]? BloomFilterData { get; init; }

        /// <summary>
        /// The chunk's page index, with page offsets relative to the chunk's start (the start of
        /// <see cref="PrefixData"/>), or null when <see cref="ParquetWriteOptions.WritePageIndex"/> is off.
        /// </summary>
        public ChunkPageIndex? PageIndex { get; init; }
    }

    [ThreadStatic]
    private static byte[]? t_compressBuffer;

    /// <summary>
    /// The level entries an undecided dictionary may hold before <see cref="Detach"/> takes its first-page test
    /// early. The test is otherwise taken at <see cref="DictionaryEncoder.FirstPageBytes"/> of values, which a
    /// column of mostly nulls may never reach; holding its appends until the row group closed would hold all of it.
    /// </summary>
    internal const int HeldLevelLimit = 1 << 20;

    // ───── The chunk being written ─────
    //
    // A chunk takes any number of appends (#936 step 2). What each piece of state does across them follows the
    // decision 5 table in #937; a single append is exactly the whole-array writer this replaced, byte for byte.

    private readonly IReadOnlyList<string> _pathInSchema;
    private readonly PhysicalType _physicalType;
    private readonly int _typeLength;
    private readonly int _maxDefLevel;
    private readonly int _maxRepLevel;

    /// <summary>The write options with this column's own overrides folded in.</summary>
    private readonly ParquetWriteOptions _options;

    /// <summary>
    /// The distinct values a Bloom filter is sized for, when the front end knows more than the first append does:
    /// a split-block filter is sized before its first insert. Null sizes it from the append it is first filled by,
    /// which is exact for a chunk of one append.
    /// </summary>
    private readonly long? _bloomFilterExpectedValues;

    private int _appendCount;
    private bool _finished;

    /// <summary>
    /// The data pages, compressed, as they were written. Their offsets are relative to the first data page
    /// until <see cref="Finish"/> knows how long the prefix is.
    /// </summary>
    private ColumnChunkOutput? _output;

    /// <summary>
    /// The pages that go ahead of the data pages: the FSST symbol table page, written when the chunk's table
    /// is trained, or the dictionary page, written by <see cref="Finish"/> once the dictionary is complete.
    /// </summary>
    private ColumnChunkOutput? _prefix;

    private int _symbolTablePageSize;

    /// <summary>The chunk's encodings, in the order they first appear (a set; enumerated in insertion order).</summary>
    private readonly HashSet<Encoding> _encodings = new();

    /// <summary>Level encoders, reused for every page of the chunk.</summary>
    private RleBitPackedEncoder? _defEncoder;
    private RleBitPackedEncoder? _repEncoder;

    /// <summary>Per page, as pages are emitted; also folds the pages' bounds into the chunk's.</summary>
    private PageIndexCollector? _pageIndex;

    private StatisticsOrder _statisticsOrder;

    /// <summary>The values' Arrow type as they reach the file, after the rewrites in <see cref="Append"/>.</summary>
    private IArrowType? _writtenType;

    private int _levelCount;
    private int _nonNullCount;

    /// <summary>SizeStatistics' unencoded_byte_array_data_bytes for the chunk (#890): summed per page.</summary>
    private long _unencodedBytes;

    /// <summary>
    /// The chunk's dictionary while it is tried or kept: its hash table persists across appends, and each page's
    /// index bit width is the one in force when that page is emitted (the format records it per page). Null when the
    /// column has none: disabled, not applicable, or given up on whole. Once <see cref="DictionaryEncoder.DictionaryBuilder.Frozen"/>,
    /// the pages are in the column's own encoding, and the dictionary only still has its page to write.
    /// </summary>
    private DictionaryEncoder.DictionaryBuilder? _dictionary;

    /// <summary>A whole chunk's dictionary handed over already built (<see cref="AppendDictionaryEncoded"/>).</summary>
    private DictionaryEncoder.DictionaryResult? _prebuiltDictionary;

    /// <summary>The prebuilt dictionary's entries, for the page bounds.</summary>
    private DictionaryEncoder.IDictionaryEntries? _prebuiltEntries;

    /// <summary>
    /// Whether FLOAT/DOUBLE statistics come from the dictionary too, counting NaNs through its indices:
    /// for a chunk appended already dictionary-encoded, which leaves no values to scan.
    /// </summary>
    private bool _dictionaryStatisticsForFloats;

    /// <summary>Whether any dictionary-encoded data page has been started: the dictionary page is then written.</summary>
    private bool _dictionaryPagesStarted;

    /// <summary>Whether pages in the column's own encoding have been started.</summary>
    private bool _plainPagesStarted;

    /// <summary>
    /// Appends whose values the dictionary has entries for but has not yet tested (its first-page test is taken at
    /// <see cref="DictionaryEncoder.FirstPageBytes"/> of values). They are written once it decides: as dictionary
    /// pages if it is kept, in the column's own encoding if it is declined.
    /// </summary>
    private List<Segment>? _held;
    private long _heldLevels;
    private long _heldValues;
    private long _heldPlainBytes;

    /// <summary>
    /// The open page: the level entries after the last page emitted, carried over to the next append so that page
    /// boundaries do not depend on batch boundaries. At most a page, kept as the pieces the appends left, and joined
    /// once, when the page is emitted: an append that does not fill the page adds a piece and copies nothing else, so
    /// small batches cost what large ones do per value.
    /// </summary>
    private List<Segment>? _open;
    private int _openLevels;
    private int _openRows;
    private int _openNonNull;

    /// <summary>Whether <see cref="_open"/> is a dictionary-encoded page.</summary>
    private bool _tailDictionary;

    /// <summary>
    /// The chunk's FSST symbol table: trained on the first values the chunk writes in its own encoding (retried on later
    /// appends while FSST declines and less than <see cref="DictionaryEncoder.FirstPageBytes"/> has been tried), and
    /// reused for the rest, whose values its escapes keep correct.
    /// </summary>
    private FsstSymbolTable? _fsstTable;
    private bool _fsstTrained;
    private long _fsstTriedBytes;

    /// <summary>
    /// Whether the statistics have to be scanned from the values, because no page index folds their bounds: it is
    /// off, or the chunk gets no ColumnIndex. A dictionary that covers the chunk supplies them otherwise.
    /// </summary>
    private bool _scanStatistics;

    /// <summary>The bounds scanned from the values, append by append.</summary>
    private BoundsFold? _scannedBounds;

    /// <summary>
    /// The Bloom filter of a chunk that is not wholly dictionary-encoded, filled from each append's values.
    /// One that is gets its filter from the dictionary at <see cref="Finish"/> instead.
    /// </summary>
    private SplitBlockBloomFilterBuilder? _bloomFilter;

    /// <summary>The bounds of dictionary-encoded pages, from the entries their indices reference.</summary>
    private DictionaryPageBounds? _dictionaryBounds;

    /// <summary>The dictionary index encoder, for pages at <see cref="_indexBitWidth"/>.</summary>
    private RleBitPackedEncoder? _indexEncoder;
    private int _indexBitWidth;

    /// <summary>Each dictionary entry's length, for SizeStatistics (#890), as far as entries have been measured.</summary>
    private int[]? _entryLengths;
    private int _entryLengthCount;

    /// <summary>Opens a column chunk for one leaf column.</summary>
    /// <param name="maxDefLevel">The leaf's maximum definition level: 0 or 1 for a flat column.</param>
    /// <param name="maxRepLevel">The leaf's maximum repetition level: 0 for a column that is not repeated.</param>
    /// <param name="bloomFilterExpectedValues">
    /// The distinct values to size a Bloom filter for, for a chunk of several appends; see
    /// <see cref="_bloomFilterExpectedValues"/>.
    /// </param>
    public ColumnChunkWriter(
        IReadOnlyList<string> pathInSchema,
        PhysicalType physicalType,
        int typeLength,
        int maxDefLevel,
        int maxRepLevel,
        ParquetWriteOptions options,
        long? bloomFilterExpectedValues = null)
    {
        _pathInSchema = pathInSchema;
        _physicalType = physicalType;
        _typeLength = typeLength;
        _maxDefLevel = maxDefLevel;
        _maxRepLevel = maxRepLevel;
        _options = ResolveColumnOptions(pathInSchema, options);
        _bloomFilterExpectedValues = bloomFilterExpectedValues;
    }

    /// <summary>
    /// Encodes an Arrow array as a Parquet column chunk (flat column, single def level).
    /// </summary>
    public static ColumnChunkResult WriteColumn(
        IArrowArray array,
        IReadOnlyList<string> pathInSchema,
        PhysicalType physicalType,
        int typeLength,
        bool isNullable,
        ParquetWriteOptions options)
    {
        var writer = new ColumnChunkWriter(pathInSchema, physicalType, typeLength, isNullable ? 1 : 0, 0, options);
        writer.AppendFlat(array);
        return writer.Finish();
    }

    /// <summary>
    /// Encodes an Arrow array as a Parquet column chunk with pre-computed def/rep levels
    /// (for nested columns).
    /// </summary>
    public static ColumnChunkResult WriteColumn(
        IArrowArray array,
        IReadOnlyList<string> pathInSchema,
        PhysicalType physicalType,
        int typeLength,
        int maxDefLevel,
        int maxRepLevel,
        int[] defLevels,
        int[]? repLevels,
        int nonNullCount,
        int levelCount,
        ParquetWriteOptions options)
    {
        var writer = new ColumnChunkWriter(pathInSchema, physicalType, typeLength, maxDefLevel, maxRepLevel, options);
        writer.Append(array, defLevels, repLevels, nonNullCount, levelCount);
        return writer.Finish();
    }

    /// <summary>
    /// The options with this column's overrides for compression, level, byte-array encoding and dictionary
    /// folded in, or the options themselves when it has none.
    /// </summary>
    private static ParquetWriteOptions ResolveColumnOptions(IReadOnlyList<string> pathInSchema, ParquetWriteOptions options)
    {
        var columnCodec = options.GetCodec(pathInSchema);
        var columnLevel = options.GetCompressionLevel(pathInSchema);
        var columnByteArrayEncoding = options.GetByteArrayEncoding(pathInSchema);
        bool columnDictionary = options.GetDictionaryEnabled(pathInSchema);
        if (columnCodec != options.Compression
            || columnLevel != options.CompressionLevel
            || columnByteArrayEncoding != options.ByteArrayEncoding
            || columnDictionary != options.DictionaryEnabled)
        {
            options = options with
            {
                Compression = columnCodec,
                CompressionLevel = columnLevel,
                ByteArrayEncoding = columnByteArrayEncoding,
                // Folded into the per-column options rather than passed alongside them, so everything
                // downstream — DictionaryEncoder above all — reads one resolved DictionaryEnabled and
                // cannot consult the file-wide one by accident.
                DictionaryEnabled = columnDictionary,
            };
        }

        return options;
    }

    /// <summary>
    /// Appends a flat column's values, deriving its definition levels from the array: every row holds a
    /// value unless the column is nullable (<c>maxDefLevel</c> 1) and the row is null.
    /// </summary>
    public void AppendFlat(IArrowArray array)
    {
        int rowCount = array.Length;

        // Extract definition levels for nullable columns
        int[]? defLevels = null;
        int nonNullCount = rowCount;

        if (_maxDefLevel > 0)
        {
            defLevels = new int[rowCount];

            if (array is RunEndEncodedArray ree)
            {
                nonNullCount = FillRunEndEncodedDefLevels(ree, defLevels);
            }
            else if (array is Apache.Arrow.Array plain)
            {
                nonNullCount = FillDefLevelsFromValidity(plain, defLevels);
            }
            else
            {
                // NullArray, UnionArray and ExtensionArray answer IsNull their own way.
                nonNullCount = 0;
                for (int i = 0; i < rowCount; i++)
                {
                    bool isNull = array.IsNull(i);
                    defLevels[i] = isNull ? 0 : 1;
                    if (!isNull) nonNullCount++;
                }
            }
        }

        Append(array, defLevels, null, nonNullCount, rowCount);
    }

    /// <summary>
    /// A flat column's definition levels, read from the validity bitmap a byte at a time: the same answer
    /// as <see cref="Apache.Arrow.Array.IsNull"/> row by row, which reads the bitmap only when the array
    /// has a null and a bitmap, and otherwise reports every row valid. Returns the non-null count.
    /// </summary>
    private static int FillDefLevelsFromValidity(Apache.Arrow.Array array, int[] defLevels)
    {
        var bitmap = array.NullBitmapBuffer.Span;
        if (array.NullCount == 0 || bitmap.IsEmpty)
        {
            defLevels.AsSpan().Fill(1);
            return defLevels.Length;
        }

        return BitmapHelper.ExpandToLevels(bitmap, array.Offset, defLevels);
    }

    /// <summary>
    /// Appends values with their levels, and encodes them into pages. The last page stays open for the next append.
    /// </summary>
    /// <remarks>
    /// The writer may keep references to <paramref name="array"/> and the level arrays — the open page, and values
    /// an undecided dictionary holds — until <see cref="Detach"/> or <see cref="Finish"/>. A front end whose caller
    /// may reuse or free them after the append calls <see cref="Detach"/> first.
    /// </remarks>
    /// <param name="array">One slot per level entry; slots at levels that hold no value are ignored.</param>
    /// <param name="defLevels">Definition levels, one per entry, or null for a required column.</param>
    /// <param name="repLevels">Repetition levels, or null for a column that is not repeated. The entries must be whole records.</param>
    /// <param name="nonNullCount">Entries at the maximum definition level: the values.</param>
    /// <param name="levelCount">Level entries: rows, for a column that is not repeated.</param>
    public void Append(IArrowArray array, int[]? defLevels, int[]? repLevels, int nonNullCount, int levelCount)
    {
        EnsureRoomFor(levelCount);
        bool first = BeginAppend();
        if (_prebuiltDictionary is not null)
            throw new InvalidOperationException("A chunk appended already dictionary-encoded takes no other append.");

        var options = _options;
        var physicalType = _physicalType;
        int typeLength = _typeLength;
        int maxDefLevel = _maxDefLevel;

        // Normalize def levels for value-encoding and dictionary methods:
        // these check defLevels[i] == 0 for null, which only works when maxDefLevel <= 1.
        int[]? valueDefLevels = NormalizeDefLevels(defLevels, maxDefLevel);

        // A run-end encoded column is brought into a shape the rest of this method can read before any of
        // it runs: its window compacted away (the encoders read the children directly, and a sliced view
        // leaves rows in them that the array does not expose), and — for FLOAT/DOUBLE — expanded outright,
        // since those are always full-scanned for the NaN count below and there is no run-aware form of
        // that scan. Everything after this point sees either a whole-window run-end encoded column or a
        // plain one.
        if (array is RunEndEncodedArray reeColumn)
        {
            array = physicalType is PhysicalType.Float or PhysicalType.Double || ValueType(reeColumn) is HalfFloatType
                ? RunEndEncoding.Expand(RunEndEncoding.Compact(reeColumn))
                : RunEndEncoding.Compact(reeColumn);
        }

        // Int8/UInt8/Int16/UInt16 are written as the 4-byte Int32 physical type, but every value extraction
        // below (DictionaryEncoder, PLAIN, the V2 encoders, StatisticsCollector) reinterprets the raw Arrow
        // value buffer AT THE PHYSICAL WIDTH — reading a 1-byte buffer as int packs four rows into each
        // value: silent corruption when the buffer's 64-byte padding hides the overrun, an out-of-range
        // read when it does not. Widen once here so every consumer downstream sees a 4-byte buffer, the
        // same single-normalization shape as the FLBA byte reversal below.
        if (physicalType == PhysicalType.Int32
            && ValueType(array) is Int8Type or UInt8Type or Int16Type or UInt16Type)
        {
            // Sign- vs zero-extension follows the source type; nulls and row positions are preserved.
            array = MapValues(array, a => ArrowCompute.Widen(a, Int32Type.Default));
        }

        // A Decimal128/256 of precision <= 18 is written as INT32/INT64 (#504). Narrowed to Decimal32/64 here,
        // before the statistics order is read off the type, it takes the integer path end to end.
        if (physicalType is PhysicalType.Int32 or PhysicalType.Int64
            && DecimalNarrowing.IsWideDecimal(ValueType(array)))
        {
            var target = DecimalNarrowing.EncodingType(ValueType(array), physicalType);
            array = MapValues(array, a => DecimalNarrowing.ToInteger(a, target));
        }

        // An Arrow timestamp promoted to the extended-precision carrier becomes twelve little-endian bytes
        // here, for the same reason the decimal reversal below happens here: everything downstream --
        // dictionary, page encoders, statistics, bloom filter -- must see the bytes that reach the file.
        bool extendedTimestamp = physicalType == PhysicalType.FixedLenByteArray
            && options.IsExtendedTimestampColumn(_pathInSchema);

        if (first)
        {
            // Read off the Arrow type before the rewrites below replace it with the bytes that reach the file.
            _statisticsOrder = StatisticsOrders.For(ValueType(array), physicalType, extendedTimestamp);

            // Null when page indexes are off, which is what keeps every page loop below free of their cost.
            _pageIndex = options.WritePageIndex
                ? NewPageIndexCollector(ValueType(array), physicalType, typeLength, _statisticsOrder, _pathInSchema, options)
                : null;
            BeginStatistics();
            _dictionary = DictionaryEncoder.DictionaryBuilder.Create(physicalType, typeLength, options);
        }

        // Only when the values are still timestamps. The buffered writer encodes at accumulation time --
        // its encoders dispatch on the Arrow type, so they have to see the carrier -- and then reaches
        // this method through its dictionary-fallback path with an array that is already twelve-byte.
        // Encoding it twice reads those bytes back as int64 and produces a well-formed, wrong file.
        if (extendedTimestamp && ValueType(array) is TimestampType)
        {
            array = MapValues(array, ExtendedTimestamp.EncodeColumn);
        }

        // For decimal FLBA types, narrow to the column's width and reverse from Arrow little-endian to Parquet
        // big-endian. This must happen before encoding/dictionary/statistics so all downstream code sees the
        // bytes that reach the file; the statistics order was read off the decimal type above.
        if (physicalType == PhysicalType.FixedLenByteArray && DecimalNarrowing.IsWideDecimal(ValueType(array)))
        {
            array = MapValues(array, a => DecimalNarrowing.ToBigEndian(a, typeLength));
        }

        _writtenType ??= ValueType(array);
        _levelCount += levelCount;
        _nonNullCount += nonNullCount;

        var segment = new Segment
        {
            Array = array,
            Def = defLevels,
            Rep = repLevels,
            ValueDef = valueDefLevels,
            Start = 0,
            End = levelCount,
            NonNull = nonNullCount,
            Borrowed = true,
        };

        if (_dictionary is { Declined: false, Frozen: false })
            AppendToDictionary(segment);
        else
            WritePlainSegment(segment);

        // Roots the caller's array across the whole append. Everything above — the dictionary encoder, the
        // plain/V2 encoders, the statistics scan, the Bloom filter — reads raw spans off its buffers. A span
        // does not keep the array alive, so without this the buffers of a caller-built array can be finalized
        // mid-encode. See doc/arrow-span-lifetime.md.
        GC.KeepAlive(array);
    }

    /// <summary>
    /// Feeds an append to the dictionary being tried or kept, and writes what that decides: held while the dictionary
    /// is undecided, dictionary pages once it is kept, the column's own encoding once it is declined or full.
    /// </summary>
    private void AppendToDictionary(Segment segment)
    {
        var dictionary = _dictionary!;
        var feed = dictionary.Feed(segment.Array!, segment.ValueDef, segment.NonNull);
        if (feed.Outcome == DictionaryEncoder.DictionaryFeed.UseRows)
        {
            segment.Array = RunEndEncoding.Expand((RunEndEncodedArray)segment.Array!);
            feed = dictionary.Feed(segment.Array, segment.ValueDef, segment.NonNull);
        }

        segment.Indices = feed.Indices;
        segment.Runs = feed.Runs;

        switch (feed.Outcome)
        {
            case DictionaryEncoder.DictionaryFeed.Pending:
                (_held ??= new()).Add(segment);
                _heldLevels += segment.Levels;
                _heldValues += segment.NonNull;
                _heldPlainBytes += PlainValueBytes(segment.Array!, segment.NonNull, _physicalType, _typeLength);
                break;

            case DictionaryEncoder.DictionaryFeed.Accepted:
                WriteHeld(dictionary: true);
                WriteDictionarySegment(segment, segment.End);
                break;

            case DictionaryEncoder.DictionaryFeed.Full:
                AppendToFullDictionary(segment, feed.FullAt);
                break;

            default: // Declined
                _dictionary = null;
                WriteHeld(dictionary: false);
                WritePlainSegment(segment);
                break;
        }
    }

    /// <summary>
    /// An append that filled the dictionary partway: the levels before the cut are dictionary-encoded, and the rest of
    /// the chunk follows in the column's own encoding, as parquet-cpp and parquet-mr write a column whose dictionary
    /// fills.
    /// </summary>
    private void AppendToFullDictionary(Segment segment, int cut)
    {
        // The cut moves back to the start of a record, so no record is split between the two encodings.
        var repLevels = segment.Rep;
        while (cut > 0 && repLevels is not null && repLevels[cut] != 0)
            cut--;

        // In the chunk's first append, a cut at the very start is no dictionary at all. FSST trains one symbol table
        // per chunk, written ahead of the data pages, so a column encoded with it falls back whole there too. Later,
        // dictionary pages have been written: the cut stands, and the tail takes the column's own encoding without FSST.
        if (_appendCount == 1 && (cut == 0 || UsesFsst(_physicalType, _options)))
        {
            _dictionary = null;
            if (segment.Array is RunEndEncodedArray)
                segment.Array = RunEndEncoding.Expand((RunEndEncodedArray)segment.Array);
            WritePlainSegment(segment);
            return;
        }

        bool earlierAppends = _appendCount > 1;
        WriteHeld(dictionary: true);
        // Scanned whole below, with the levels past the cut.
        WriteDictionarySegment(segment, cut, scan: false);
        // The dictionary pages end at the cut.
        FlushTail();

        // Nothing past the dictionary has a run-aware path, so the tail of a run-end encoded append is expanded. The
        // dictionary pages above read only its entries.
        if (segment.Array is RunEndEncodedArray ree)
            segment.Array = RunEndEncoding.Expand(ree);

        // The appends before this one were dictionary-encoded whole, so neither the statistics scan nor the Bloom
        // filter saw their values: the frozen dictionary's entries are those values. This append is taken whole below,
        // as a chunk of one append always was.
        if (earlierAppends)
            AddFrozenDictionaryToStatisticsAndBloomFilter();
        ScanStatistics(segment);
        AddToBloomFilter(segment);

        int valuesBeforeCut = _maxDefLevel > 0 ? CountNonNull(segment.Def!, 0, cut, _maxDefLevel) : cut;
        WritePlainRange(segment, cut, valuesBeforeCut);
    }

    /// <summary>
    /// Writes the appends an undecided dictionary held, now that it has decided: as dictionary pages, or in the
    /// column's own encoding.
    /// </summary>
    private void WriteHeld(bool dictionary)
    {
        if (_held is null)
            return;

        var held = _held;
        _held = null;
        _heldLevels = 0;
        _heldValues = 0;
        _heldPlainBytes = 0;
        foreach (var segment in held)
        {
            if (dictionary)
                WriteDictionarySegment(segment, segment.End);
            else
                WritePlainSegment(segment);
        }
    }

    /// <summary>
    /// Writes a whole append in the column's own encoding: PLAIN for V1, type-aware for V2, FSST where the chunk
    /// trained a table. Its values are scanned for the statistics and hashed into the Bloom filter as they go.
    /// </summary>
    private void WritePlainSegment(Segment segment)
    {
        // Nothing past the dictionary has a run-aware path — the PLAIN and V2 encoders, the statistics
        // scan and the page-size estimate all read one value slot per row. A column the dictionary
        // declined is therefore expanded here and encoded exactly as a plain one would be. This costs the
        // memory the encoding exists to save, which is why it is the fallback rather than the route:
        // in practice a run-encoded column is low-cardinality by construction and the dictionary takes it.
        if (segment.Array is RunEndEncodedArray declined)
            segment.Array = RunEndEncoding.Expand(declined);

        ScanStatistics(segment);
        AddToBloomFilter(segment);
        WritePlainRange(segment, segment.Start, segment.ValueStart);
    }

    /// <summary>The statistics scan of a whole append, when nothing else will supply them.</summary>
    /// <remarks>
    /// A dictionary that covers the chunk supplies the bounds of every column but FLOAT/DOUBLE, whose NaN count counts
    /// values, so those are always scanned (or folded by the page index).
    /// </remarks>
    private void ScanStatistics(Segment segment, bool dictionaryEncoded = false)
    {
        if (!_scanStatistics || segment.NonNull == 0)
            return;
        if (dictionaryEncoded && !StatisticsCollector.IsFloatingPoint(_physicalType, _statisticsOrder))
            return;

        var bounds = StatisticsCollector.ComputePageBounds(
            segment.Array!, _physicalType, _typeLength, segment.ValueDef, segment.Start, segment.Levels,
            _options.FloatingPointOrder == FloatingPointColumnOrder.Ieee754TotalOrder, _statisticsOrder);
        _scannedBounds!.Add(bounds);
    }

    /// <summary>A whole append's values into the Bloom filter of a chunk that is not wholly dictionary-encoded.</summary>
    private void AddToBloomFilter(Segment segment)
    {
        if (!_options.HasBloomFilter(_pathInSchema))
            return;

        _bloomFilter ??= NewBloomFilterBuilder(_bloomFilterExpectedValues ?? segment.NonNull, _options);
        BloomFilterArrowEncoder.AddArrowValues(_bloomFilter, segment.Array!, _physicalType, _typeLength);
    }

    /// <summary>
    /// A dictionary that filled after earlier appends: its entries are exactly the values those appends brought, so
    /// they stand in for them in the statistics and the Bloom filter. The same set of values sets the same bits.
    /// </summary>
    private void AddFrozenDictionaryToStatisticsAndBloomFilter()
    {
        var dictionary = _dictionary!;
        if (_scanStatistics && dictionary.Count > 0
            && !StatisticsCollector.IsFloatingPoint(_physicalType, _statisticsOrder))
        {
            var bounds = new DictionaryPageBounds(
                dictionary, _physicalType, _statisticsOrder,
                _options.FloatingPointOrder == FloatingPointColumnOrder.Ieee754TotalOrder);
            bounds.Begin();
            for (int entry = 0; entry < dictionary.Count; entry++)
                bounds.Add(entry, 1);
            _scannedBounds!.Add(bounds.End());
        }

        if (_options.HasBloomFilter(_pathInSchema))
        {
            _bloomFilter ??= NewBloomFilterBuilder(_bloomFilterExpectedValues ?? dictionary.Count, _options);
            BloomFilterArrowEncoder.AddDictionaryValues(
                _bloomFilter, dictionary.ToPage(), dictionary.Count, _physicalType, _typeLength);
        }
    }

    private void BeginStatistics()
    {
        // A page index folds every page's bounds, even after it drops its ColumnIndex for an all-NaN page. Without
        // one, the values are scanned append by append and the scans merged.
        _scanStatistics = _options.GetWriteStatistics(_pathInSchema) && _pageIndex is not { FoldsBounds: true };
        if (_scanStatistics)
        {
            _scannedBounds = new BoundsFold(
                _physicalType, _statisticsOrder,
                _options.FloatingPointOrder == FloatingPointColumnOrder.Ieee754TotalOrder);
        }
    }

    /// <summary>
    /// Appends values that are already dictionary-encoded, as the buffered writer builds them: the whole
    /// chunk's dictionary and indices. Statistics and the Bloom filter come from the dictionary, since there
    /// are no values to scan. The chunk takes no other append.
    /// </summary>
    /// <param name="arrowType">The values' Arrow type, for the statistics order.</param>
    internal void AppendDictionaryEncoded(
        DictionaryEncoder.DictionaryResult dictResult,
        IArrowType arrowType,
        int[]? defLevels,
        int[]? repLevels,
        int nonNullCount,
        int levelCount)
    {
        if (dictResult.PartialLength is not null)
            throw new ArgumentException("A dictionary appended whole must cover the chunk.", nameof(dictResult));
        if (_appendCount > 0)
            throw new InvalidOperationException("A chunk appended already dictionary-encoded takes no other append.");

        EnsureRoomFor(levelCount);
        BeginAppend();

        _statisticsOrder = StatisticsOrders.For(
            arrowType, _physicalType,
            _physicalType == PhysicalType.FixedLenByteArray && _options.IsExtendedTimestampColumn(_pathInSchema));
        _pageIndex = _options.WritePageIndex
            ? NewPageIndexCollector(arrowType, _physicalType, _typeLength, _statisticsOrder, _pathInSchema, _options)
            : null;
        _writtenType = arrowType;
        _prebuiltDictionary = dictResult;
        _prebuiltEntries = new DictionaryEncoder.PageEntries(
            dictResult.DictionaryPageData, dictResult.DictionaryCount, _physicalType, _typeLength);
        // FLOAT/DOUBLE take the index-aware overload: the bounds come from the dictionary entries either
        // way, but nan_count counts VALUES, so it has to see the indices. Append full-scans the Arrow array
        // for the same reason; here there is no array left to scan.
        _dictionaryStatisticsForFloats = true;

        _levelCount += levelCount;
        _nonNullCount += nonNullCount;

        WriteDictionarySegment(new Segment
        {
            Array = null,
            Def = defLevels,
            Rep = repLevels,
            ValueDef = null,
            Start = 0,
            End = levelCount,
            NonNull = nonNullCount,
            Indices = dictResult.Indices,
            Runs = dictResult.IndexRuns,
            Borrowed = true,
        }, levelCount, scan: false); // no values to scan: Finish takes the statistics from the dictionary and indices
    }

    /// <summary>
    /// Refuses an append that would take the chunk past <see cref="int.MaxValue"/> level entries, before any of it is
    /// taken: the counts the chunk's metadata, pages and buffers are built on are 32-bit, and wrapped they would be
    /// written as a well-formed, wrong <c>num_values</c>. A row group whose leaves repeat that much must be cut sooner.
    /// </summary>
    private void EnsureRoomFor(int levelCount)
    {
        if (_finished)
            throw new InvalidOperationException("The column chunk has already been finished.");
        if ((long)_levelCount + levelCount > int.MaxValue)
        {
            throw new InvalidOperationException(
                $"Column '{string.Join(".", _pathInSchema)}' would hold more than {int.MaxValue} level entries in one "
                + "column chunk; cut row groups sooner (RowGroupMaxRows, or FlushRowGroupAsync).");
        }
    }

    /// <returns>Whether this is the chunk's first append.</returns>
    private bool BeginAppend()
    {
        if (_finished)
            throw new InvalidOperationException("The column chunk has already been finished.");

        if (_appendCount++ > 0)
            return false;

        // Reusable encoders for def/rep levels across pages
        int maxLiteralGroups = _options.MaxLiteralGroups;
        _defEncoder = _maxDefLevel > 0 ? new RleBitPackedEncoder(BitWidth(_maxDefLevel), maxLiteralGroups: maxLiteralGroups) : null;
        _repEncoder = _maxRepLevel > 0 ? new RleBitPackedEncoder(BitWidth(_maxRepLevel), maxLiteralGroups: maxLiteralGroups) : null;
        return true;
    }

    /// <summary>
    /// Copies what the chunk still holds of the arrays it was given — the open page, and the appends an undecided
    /// dictionary holds — so that the caller may reuse or free them. A dictionary that has held more than
    /// <see cref="HeldLevelLimit"/> level entries without deciding takes its test now instead.
    /// </summary>
    public void Detach()
    {
        if (_finished)
            return;

        if (_held is not null && _heldLevels > HeldLevelLimit)
        {
            bool kept = _dictionary!.Decide();
            if (!kept)
                _dictionary = null;
            WriteHeld(kept);
        }

        if (_held is not null)
        {
            for (int i = 0; i < _held.Count; i++)
            {
                if (_held[i].Borrowed)
                    _held[i] = _held[i].Copy(withArray: true, withIndices: true);
            }
        }

        if (_open is not null)
        {
            for (int i = 0; i < _open.Count; i++)
            {
                if (_open[i].Borrowed)
                    _open[i] = _open[i].Copy(withArray: !_tailDictionary, withIndices: _tailDictionary);
            }
        }
    }

    /// <summary>
    /// The chunk's size as it would be written now, estimated without encoding anything more: the pages written,
    /// and the dictionary page, open page and held values at their uncompressed size. Values an undecided dictionary
    /// holds count at the smaller of their two encodings, since its test keeps the smaller. Levels not yet in a page,
    /// open or held, count a quarter byte each, generously: without them a column of nulls estimated at nothing while
    /// its dictionary held up to <see cref="HeldLevelLimit"/> of them, and never took part in a byte cut (#938 review).
    /// </summary>
    public long EstimatedSize
    {
        get
        {
            long size = (_output?.Length ?? 0) + (_prefix?.Length ?? 0);
            bool hasLevels = _maxDefLevel > 0 || _maxRepLevel > 0;
            if (_held is not null)
            {
                long dictionaryEncoded = _dictionary!.ByteCount
                    + (_heldValues * DictionaryEncoder.GetIndexBitWidth(_dictionary.Count) + 7) / 8;
                size += Math.Min(_heldPlainBytes, dictionaryEncoded);
                if (hasLevels)
                    size += _heldLevels / 4;
            }
            else if (_dictionary is { Declined: false } dictionary)
            {
                size += dictionary.ByteCount;
            }
            if (_open is not null)
            {
                size += _tailDictionary
                    ? ((long)_openNonNull * DictionaryEncoder.GetIndexBitWidth(DictionaryCount) + 7) / 8
                    : TailPlainBytes(_openNonNull);
                if (hasLevels)
                    size += _openLevels / 4;
            }
            return size;
        }
    }

    /// <summary>A PLAIN-size estimate of <paramref name="values"/> values of the open page.</summary>
    private long TailPlainBytes(int values) => _physicalType switch
    {
        PhysicalType.Boolean => (values + 7) / 8,
        PhysicalType.Int32 or PhysicalType.Float => 4L * values,
        PhysicalType.Int64 or PhysicalType.Double => 8L * values,
        PhysicalType.Int96 => 12L * values,
        PhysicalType.FixedLenByteArray => (long)_typeLength * values,
        _ => _open?[0].Array is { } array && array.Length > 0
            ? (long)EstimateAvgByteArraySize(array) * values
            : 32L * values,
    };

    /// <summary>
    /// Ends the chunk: writes its dictionary page, and returns the chunk with its metadata, statistics,
    /// page index and Bloom filter. The writer can take nothing more afterwards.
    /// </summary>
    public ColumnChunkResult Finish()
    {
        if (_finished)
            throw new InvalidOperationException("The column chunk has already been finished.");
        if (_appendCount == 0)
            throw new InvalidOperationException("A column chunk needs an append before it can be finished.");

        // A dictionary still undecided takes its test over everything it was given.
        if (_held is not null)
        {
            bool kept = _dictionary!.Decide();
            if (!kept)
                _dictionary = null;
            WriteHeld(kept);
        }

        FlushTail();
        _finished = true;

        var options = _options;
        var physicalType = _physicalType;
        var output = _output ?? NewOutput(0);

        // The dictionary page goes ahead of the data pages that use it, so it is written last and placed first.
        int dictionaryPageSize = 0;
        if (_dictionaryPagesStarted)
        {
            byte[] dictionaryPage = _prebuiltDictionary?.DictionaryPageData ?? _dictionary!.ToPage();
            _prefix ??= new ColumnChunkOutput(
                Compressor.GetMaxCompressedLength(options.Compression, dictionaryPage.Length)
                    + PageHeaderAllowance,
                options);
            dictionaryPageSize = WriteDictionaryPage(_prefix, dictionaryPage, DictionaryCount, options);
        }

        var prefix = _prefix;
        var prefixData = prefix?.Data ?? default;

        // encoding_stats in order of first appearance: the prefix's pages come first.
        PageEncodingStats[] encodingStats;
        if (prefix is null)
        {
            encodingStats = output.EncodingStats.ToArray();
        }
        else
        {
            encodingStats = new PageEncodingStats[prefix.EncodingStats.Count + output.EncodingStats.Count];
            for (int i = 0; i < prefix.EncodingStats.Count; i++)
                encodingStats[i] = prefix.EncodingStats[i];
            for (int i = 0; i < output.EncodingStats.Count; i++)
                encodingStats[prefix.EncodingStats.Count + i] = output.EncodingStats[i];
        }

        var metadata = new ColumnMetaData
        {
            Type = physicalType,
            Encodings = _encodings.ToArray(),
            PathInSchema = _pathInSchema is string[] arr ? arr : _pathInSchema.ToArray(),
            Codec = options.Compression,
            NumValues = _levelCount,
            TotalUncompressedSize = (prefix?.TotalUncompressedSize ?? 0) + output.TotalUncompressedSize,
            TotalCompressedSize = (prefix?.TotalCompressedSize ?? 0) + output.TotalCompressedSize,
            EncodingStats = encodingStats,
            DataPageOffset = 0, // set by caller
            DictionaryPageOffset = dictionaryPageSize > 0 ? 0 : null, // set by caller
            SymbolTablePageOffset = _symbolTablePageSize > 0 ? 0 : null, // set by caller
            SymbolTablePageLength = _symbolTablePageSize > 0 ? _symbolTablePageSize : null,
            UnencodedByteArrayDataBytes = RecordsUnencodedBytes(physicalType, options) ? _unencodedBytes : null,
        };

        // A dictionary that covers the whole chunk: every data page is dictionary-encoded.
        bool wholeDictionary = _prebuiltDictionary is not null
            || (_dictionary is { Decided: true, Frozen: false, Declined: false } && _dictionaryPagesStarted);

        // See ParquetWriteOptions.WriteStatistics: off means no Statistics at all, not merely no bounds.
        if (options.GetWriteStatistics(_pathInSchema))
        {
            // The chunk's statistics come from the dictionary entries when one covers the chunk (O(unique)
            // vs O(total)), or else from the page index, which found every page's bounds with the same scans:
            // folding them gives these statistics exactly. Only when neither can were the values scanned.
            // FLOAT/DOUBLE columns are always scanned (or folded) so the NaN count covers every value, not
            // just the distinct dictionary entries.
            bool floatingPointTotalOrder = options.FloatingPointOrder == FloatingPointColumnOrder.Ieee754TotalOrder;
            bool isFloatingPoint = StatisticsCollector.IsFloatingPoint(physicalType, _statisticsOrder);
            int nullCount = _levelCount - _nonNullCount;

            Statistics stats;
            if (wholeDictionary && (!isFloatingPoint || _dictionaryStatisticsForFloats))
            {
                stats = isFloatingPoint
                    ? StatisticsCollector.ComputeFloatingPointFromDictEntries(
                        _prebuiltDictionary!.Value, physicalType, nullCount, floatingPointTotalOrder)
                    : StatisticsCollector.ComputeFromDictEntries(
                        _prebuiltDictionary?.DictionaryPageData ?? _dictionary!.ToPage(), DictionaryCount,
                        physicalType, _typeLength, nullCount, _statisticsOrder);
            }
            else if (_nonNullCount == 0)
            {
                // No values ⇒ no NaN. nan_count is mandatory for FP columns even at zero.
                stats = new Statistics { NullCount = nullCount, NanCount = isFloatingPoint ? 0L : null };
            }
            else
            {
                stats = _pageIndex?.ChunkStatistics(_nonNullCount, _levelCount)
                    ?? _scannedBounds?.ToStatistics(nullCount)
                    ?? throw new InvalidOperationException("The chunk's statistics were neither folded nor scanned.");
            }

            stats = StatisticsCollector.ApplyTypeOrderZeroRule(stats, physicalType, floatingPointTotalOrder, _statisticsOrder);
            metadata.Statistics = DropDeprecatedMinMaxIfMisordered(stats, _writtenType!, physicalType);
        }

        byte[]? bloomFilterData = null;
        if (options.HasBloomFilter(_pathInSchema))
        {
            var bloomFilter = wholeDictionary
                ? DictionaryBloomFilter(
                    _prebuiltDictionary?.DictionaryPageData ?? _dictionary!.ToPage(), DictionaryCount,
                    physicalType, _typeLength, options)
                : _bloomFilter ?? NewBloomFilterBuilder(_bloomFilterExpectedValues ?? 0, options);
            bloomFilterData = BloomFilterSerializer.Serialize(bloomFilter.ToArray());
        }

        var result = new ColumnChunkResult
        {
            PrefixData = prefixData,
            Data = output.Data,
            MetaData = metadata,
            DictionaryPageSize = dictionaryPageSize,
            SymbolTablePageSize = _symbolTablePageSize,
            BloomFilterData = bloomFilterData,
            PageIndex = _pageIndex?.Build(pageOffsetBase: prefixData.Count),
        };

        // What the chunk held is the result's now; the rest can go.
        _output = null;
        _prefix = null;
        _dictionary = null;
        _prebuiltDictionary = null;
        _prebuiltEntries = null;
        _dictionaryBounds = null;
        _bloomFilter = null;
        _pageIndex = null;
        _scannedBounds = null;
        _defEncoder = null;
        _repEncoder = null;
        _held = null;
        _open = null;
        return result;
    }

    /// <summary>The entries the dictionary page holds, and the index bit width of a page emitted now is for.</summary>
    private int DictionaryCount => _prebuiltDictionary?.DictionaryCount ?? _dictionary!.Count;

    private DictionaryEncoder.IDictionaryEntries DictionaryEntries =>
        _prebuiltEntries ?? (DictionaryEncoder.IDictionaryEntries)_dictionary!;

    private ColumnChunkOutput NewOutput(int capacity) => _output = new ColumnChunkOutput(capacity, _options);

    // ───── Pages ─────

    /// <summary>
    /// Level entries of one append, or a part of one: the open page, or the values a dictionary holds. Values are
    /// numbered within the arrays, from 0 at level 0.
    /// </summary>
    private sealed class Segment
    {
        /// <summary>One slot per level entry. Null only for a dictionary-encoded segment the writer copied.</summary>
        public IArrowArray? Array;
        public int[]? Def;
        public int[]? Rep;

        /// <summary>The definition levels as 0/1 (see <see cref="NormalizeDefLevels"/>); may be <see cref="Def"/> itself.</summary>
        public int[]? ValueDef;

        /// <summary>The level positions [<see cref="Start"/>, <see cref="End"/>) the segment covers.</summary>
        public int Start, End;

        /// <summary>The number of the first value at or after <see cref="Start"/>.</summary>
        public int ValueStart;

        /// <summary>Values in [<see cref="Start"/>, <see cref="End"/>).</summary>
        public int NonNull;

        /// <summary>Each value's dictionary entry, by value number, or null.</summary>
        public int[]? Indices;

        /// <summary>The same in run form, for a whole run-end encoded append (<see cref="Start"/> 0).</summary>
        public DictionaryEncoder.DictionaryIndexRuns? Runs;

        /// <summary>The values FSST-compressed, by value number less <see cref="FsstBase"/>.</summary>
        public FsstCompressedColumn? Fsst;
        public int FsstBase;

        /// <summary>Whether the arrays may be the caller's (see <see cref="ColumnChunkWriter.Detach"/>).</summary>
        public bool Borrowed;

        public int Levels => End - Start;

        /// <summary>The segment with its own copies of what it covers, renumbered from level 0.</summary>
        public Segment Copy(bool withArray, bool withIndices)
        {
            var copy = new Segment
            {
                Start = 0,
                End = Levels,
                ValueStart = 0,
                NonNull = NonNull,
                Def = Slice(Def, Start, Levels),
                Rep = Slice(Rep, Start, Levels),
            };
            copy.ValueDef = ReferenceEquals(ValueDef, Def) ? copy.Def : Slice(ValueDef, Start, Levels);

            if (withArray && Array is not null)
            {
                var rows = new int[Levels];
                for (int i = 0; i < rows.Length; i++)
                    rows[i] = Start + i;
                copy.Array = ArrowCompute.Take(Array, rows);
            }

            if (withIndices && (Indices is not null || Runs is not null))
            {
                if (Runs is { } runs && Start == 0 && withArray)
                {
                    copy.Runs = runs; // the writer's own, and whole
                }
                else
                {
                    copy.Indices = new int[NonNull];
                    ReadIndices(ValueStart, copy.Indices);
                }
            }

            return copy;
        }

        /// <summary>The entries of the values numbered from <paramref name="first"/>, into <paramref name="destination"/>.</summary>
        public void ReadIndices(int first, Span<int> destination)
        {
            if (Runs is { } runs)
            {
                var cursor = new IndexRunCursor(runs);
                cursor.Skip(first);
                cursor.Read(destination);
            }
            else
            {
                Indices.AsSpan(first, destination.Length).CopyTo(destination);
            }
        }

        private static int[]? Slice(int[]? levels, int start, int length) =>
            levels?.AsSpan(start, length).ToArray();
    }

    /// <summary>
    /// Writes a segment's level positions [<see cref="Segment.Start"/>, <paramref name="end"/>) as RLE_DICTIONARY
    /// pages. The dictionary page is written by <see cref="Finish"/>.
    /// </summary>
    private void WriteDictionarySegment(Segment segment, int end, bool scan = true)
    {
        if (!_dictionaryPagesStarted)
        {
            _dictionaryPagesStarted = true;
            // FSST trains one symbol table per chunk, written ahead of the data pages; a chunk with a dictionary
            // page has no place for both.
            _fsstTrained = true;

            int bitWidth = DictionaryEncoder.GetIndexBitWidth(DictionaryCount);
            int valuesPerPage = Math.Max(1, _options.DataPageSize / Math.Max(1, (bitWidth + 7) / 8));
            // The dictionary page is not in this stream (see _prefix), so it is not counted in its capacity.
            _output ??= NewOutput(EstimateDictionaryColumnSize(
                dictionaryBytes: 0, segment.Levels,
                LayOutPages(segment.Rep, segment.End, valuesPerPage, _options.DataPageRowCountLimit),
                bitWidth, _maxDefLevel, _maxRepLevel, _options.Compression));

            _encodings.Add(Encoding.Plain);
            _encodings.Add(Encoding.RleDictionary);
            if (_maxDefLevel > 0 || _maxRepLevel > 0)
                _encodings.Add(Encoding.Rle);
        }

        if (scan)
            ScanStatistics(segment, dictionaryEncoded: true);

        if (_open is not null && !_tailDictionary)
            FlushTail();

        var cursor = segment.Runs is { } runs ? new IndexRunCursor(runs) : default;
        WritePages(segment, segment.Start, end, segment.ValueStart, dictionary: true, ref cursor);
    }

    /// <summary>
    /// Writes a segment's level positions [<paramref name="start"/>, <see cref="Segment.End"/>) in the column's own
    /// encoding (PLAIN for V1, type-aware for V2), training the chunk's FSST symbol table first when it uses one.
    /// <paramref name="start"/> must begin a record; its first value is value <paramref name="valueStart"/>.
    /// </summary>
    private void WritePlainRange(Segment segment, int start, int valueStart)
    {
        var options = _options;
        var physicalType = _physicalType;

        if (!_plainPagesStarted)
        {
            _plainPagesStarted = true;

            // FSST is the one encoding option a V1 page cannot honour: it trains a per-chunk symbol table
            // written as its own page, and only the V2 page writer carries one. Every other option now
            // applies to both page versions (#270), so quietly writing DELTA_LENGTH_BYTE_ARRAY here would
            // leave exactly one place where an encoding is accepted and discarded. Say so instead.
            if (options.DataPageVersion == DataPageVersion.V1
                && physicalType == PhysicalType.ByteArray
#pragma warning disable EWPARQUET0003 // Reporting the caller's own opt-in back to them; not a new use.
                && options.ByteArrayEncoding is ByteArrayEncoding.Fsst or ByteArrayEncoding.Fsst16)
#pragma warning restore EWPARQUET0003
            {
                throw new NotSupportedException(
                    $"FSST requires {nameof(DataPageVersion)}.{nameof(DataPageVersion.V2)}: it writes a "
                    + "per-column-chunk symbol table page that V1 data pages have no place for. Set "
                    + $"{nameof(ParquetWriteOptions.DataPageVersion)} to V2, or choose another "
                    + $"{nameof(ByteArrayEncoding)} for this column.");
            }

            // After a dictionary that filled up, the stream sized for its pages grows to take these.
            if (_output is null)
            {
                int levels = segment.End - start;
                int nonNull = segment.NonNull - (valueStart - segment.ValueStart);
                NewOutput(EstimateColumnSize(
                    levels, nonNull, PlainValueBytes(segment.Array!, nonNull, physicalType, _typeLength),
                    LayOutPages(segment.Rep, levels, EstimatedValuesPerPage(segment.Array!), options.DataPageRowCountLimit),
                    physicalType, _typeLength,
                    _maxDefLevel, _maxRepLevel, options.ByteArrayEncoding == ByteArrayEncoding.DeltaByteArray,
                    options.Compression));
            }

            if (_maxDefLevel > 0 || _maxRepLevel > 0)
                _encodings.Add(Encoding.Rle);
        }

        if (_open is not null && _tailDictionary)
            FlushTail();

        // FSST trains one symbol table per column chunk (§1.4), on the first values the chunk writes in its own
        // encoding, and compresses each append whole up front: the pages below are slices of the result. Values
        // FSST declined (nothing to train on, or no size win) are written DELTA_LENGTH_BYTE_ARRAY, and the next
        // append is tried, until a table is trained or FirstPageBytes of values have been tried: a first append of a
        // few rows is no sample. A table trained later still precedes every data page, so any page may use it.
        int nonNullInRange = segment.NonNull - (valueStart - segment.ValueStart);
        if (UsesFsst(physicalType, options) && nonNullInRange > 0)
        {
            if (!_fsstTrained)
            {
                var values = ExtractDenseByteArrays(segment, start, segment.End);
                foreach (var value in values)
                    _fsstTriedBytes += value.Length;
                _fsstTrained = _fsstTriedBytes >= DictionaryEncoder.FirstPageBytes;

                var column = FsstCompressedColumn.TryCompress(values, FsstTableType(options));
                if (column is not null)
                {
                    _fsstTrained = true;
                    _fsstTable = column.Table;
                    _prefix = new ColumnChunkOutput(PageHeaderAllowance, options);
                    _symbolTablePageSize = WriteSymbolTablePage(_prefix, column.Table, options);
                    segment.Fsst = column;
                    segment.FsstBase = valueStart;
                }
            }
            else if (_fsstTable is not null)
            {
                segment.Fsst = FsstCompressedColumn.Compress(
                    _fsstTable, ExtractDenseByteArrays(segment, start, segment.End));
                segment.FsstBase = valueStart;
            }
        }

        var cursor = default(IndexRunCursor);
        WritePages(segment, start, segment.End, valueStart, dictionary: false, ref cursor);
    }

    /// <summary>The page length, in level entries, a plain page of this array's values is cut at.</summary>
    private int EstimatedValuesPerPage(IArrowArray array) =>
        Math.Max(1, EstimateValuesPerPage(array, _physicalType, _typeLength, _options.DataPageSize));

    /// <summary>The page length, in level entries, a dictionary-encoded page is cut at: by the bit width in force.</summary>
    private int DictionaryValuesPerPage()
    {
        int bitWidth = DictionaryEncoder.GetIndexBitWidth(DictionaryCount);
        int bytesPerIndex = Math.Max(1, (bitWidth + 7) / 8);
        return Math.Max(1, _options.DataPageSize / bytesPerIndex);
    }

    /// <summary>
    /// Cuts level positions [<paramref name="start"/>, <paramref name="end"/>) of a segment into pages, the open page
    /// first: full pages are emitted, and what is left, less than a page, stays open (<see cref="_open"/>).
    /// </summary>
    private void WritePages(
        Segment segment, int start, int end, int valueIndex, bool dictionary, ref IndexRunCursor cursor)
    {
        int valuesPerPage = dictionary ? DictionaryValuesPerPage() : EstimatedValuesPerPage(segment.Array!);
        int offset = start;

        if (_open is not null && offset < end)
            CompleteOpenPage(segment, ref offset, end, ref valueIndex, valuesPerPage, dictionary, ref cursor);

        var repLevels = segment.Rep;
        int? rowLimit = _options.DataPageRowCountLimit;
        while (offset < end)
        {
            if (!PageFull(repLevels, offset, end, valuesPerPage, rowLimit))
            {
                HoldOpenPage(segment, offset, end, valueIndex, dictionary, ref cursor);
                return;
            }

            int pageValues = RecordAlignedPageLength(
                repLevels, offset, PageTargetLength(repLevels, offset, valuesPerPage, end, rowLimit), end);
            int pageNonNull = _maxDefLevel > 0
                ? CountNonNull(segment.Def!, offset, pageValues, _maxDefLevel)
                : pageValues;

            EmitPage(segment, offset, pageValues, pageNonNull, valueIndex, dictionary, ref cursor);
            offset += pageValues;
            valueIndex += pageNonNull;
        }
    }

    /// <summary>
    /// Whether level positions [<paramref name="offset"/>, <paramref name="end"/>) fill a page: as many level
    /// entries as a page takes, or as many rows as <paramref name="rowLimit"/> allows. Less is an open page.
    /// </summary>
    private static bool PageFull(int[]? repLevels, int offset, int end, int valuesPerPage, int? rowLimit)
    {
        int length = end - offset;
        if (length >= valuesPerPage)
            return true;
        if (rowLimit is not int limit)
            return false;
        if (repLevels is null)
            return length >= limit;

        int rows = 0;
        for (int i = offset; i < end; i++)
        {
            if (repLevels[i] == 0 && ++rows >= limit)
                return true;
        }
        return false;
    }

    /// <summary>Leaves [<paramref name="offset"/>, <paramref name="end"/>) open, for the next append or <see cref="Finish"/>.</summary>
    private void HoldOpenPage(Segment segment, int offset, int end, int valueIndex, bool dictionary, ref IndexRunCursor cursor)
    {
        int nonNull = _maxDefLevel > 0 ? CountNonNull(segment.Def!, offset, end - offset, _maxDefLevel) : end - offset;
        var tail = new Segment
        {
            Array = segment.Array,
            Def = segment.Def,
            Rep = segment.Rep,
            ValueDef = segment.ValueDef,
            Start = offset,
            End = end,
            ValueStart = valueIndex,
            NonNull = nonNull,
            Indices = segment.Indices,
            Fsst = segment.Fsst,
            FsstBase = segment.FsstBase,
            Borrowed = segment.Borrowed,
        };

        // Run-form entries are read out for the open page alone: at most a page of them.
        if (dictionary && segment.Runs is not null)
        {
            var indices = new int[nonNull];
            var peek = cursor;
            peek.Read(indices);
            tail.Indices = indices;
            tail.ValueStart = 0;
            tail.Start = offset;
        }

        (_open ??= new()).Add(tail);
        _openLevels += tail.Levels;
        _openRows += PageRows(tail.Rep, tail.Start, tail.Levels, _maxRepLevel);
        _openNonNull += nonNull;
        _tailDictionary = dictionary;
    }

    /// <summary>
    /// Completes the open page with the level entries that follow it, from <paramref name="offset"/> on, and emits it
    /// once it is full. Entries that do not fill it are added to it as another piece. A page that fills is joined from
    /// its pieces and the entries that complete it: a copy of at most a page, once per page.
    /// </summary>
    private void CompleteOpenPage(
        Segment segment, ref int offset, int end, ref int valueIndex, int valuesPerPage, bool dictionary,
        ref IndexRunCursor cursor)
    {
        int? rowLimit = _options.DataPageRowCountLimit;

        // A page target smaller than the open page (a later append's values are larger): the open page is a page.
        if (_openLevels >= valuesPerPage || (rowLimit is int limit && _openRows >= limit))
        {
            FlushTail();
            return;
        }

        if (!FillsOpenPage(segment, offset, end, valuesPerPage, rowLimit))
        {
            // Everything that follows fits in the open page, which stays open.
            HoldOpenPage(segment, offset, end, valueIndex, dictionary, ref cursor);
            AdvancePast(segment, ref offset, ref valueIndex, end - offset, dictionary, ref cursor);
            return;
        }

        int openLevels = _openLevels;
        int take = Math.Min(end - offset, valuesPerPage - openLevels);
        // One entry past the page, when there is one, so that the record alignment can see where the next record starts.
        int lookahead = offset + take < end ? 1 : 0;
        var joined = JoinOpen(segment, offset, take + lookahead, valueIndex, dictionary, ref cursor);
        int pageEnd = openLevels + take;

        int pageValues = RecordAlignedPageLength(
            joined.Rep, 0, PageTargetLength(joined.Rep, 0, valuesPerPage, pageEnd, rowLimit), joined.End);
        int pageNonNull = _maxDefLevel > 0
            ? CountNonNull(joined.Def!, 0, pageValues, _maxDefLevel)
            : pageValues;
        var noRuns = default(IndexRunCursor);
        EmitPage(joined, 0, pageValues, pageNonNull, 0, dictionary, ref noRuns);
        ClearOpenPage();

        AdvancePast(segment, ref offset, ref valueIndex, pageValues - openLevels, dictionary, ref cursor);
    }

    /// <summary>
    /// Whether the open page and level positions [<paramref name="offset"/>, <paramref name="end"/>) fill a page, as
    /// <see cref="PageFull"/> decides it for one segment.
    /// </summary>
    private bool FillsOpenPage(Segment segment, int offset, int end, int valuesPerPage, int? rowLimit)
    {
        if (_openLevels + (end - offset) >= valuesPerPage)
            return true;
        if (rowLimit is not int limit)
            return false;
        if (segment.Rep is null)
            return _openRows + (end - offset) >= limit;

        int rows = _openRows;
        for (int i = offset; i < end; i++)
        {
            if (segment.Rep[i] == 0 && ++rows >= limit)
                return true;
        }
        return false;
    }

    private void ClearOpenPage()
    {
        _open = null;
        _openLevels = 0;
        _openRows = 0;
        _openNonNull = 0;
    }

    /// <summary>Moves past <paramref name="levels"/> level entries of a segment, and their values.</summary>
    private void AdvancePast(
        Segment segment, ref int offset, ref int valueIndex, int levels, bool dictionary, ref IndexRunCursor cursor)
    {
        int nonNull = _maxDefLevel > 0 ? CountNonNull(segment.Def!, offset, levels, _maxDefLevel) : levels;
        if (dictionary && segment.Runs is not null)
            cursor.Skip(nonNull);
        offset += levels;
        valueIndex += nonNull;
    }

    /// <summary>
    /// The open page's pieces followed by <paramref name="levels"/> level entries of <paramref name="segment"/> from
    /// <paramref name="offset"/> (none when it is null), as one segment of their own.
    /// </summary>
    private Segment JoinOpen(
        Segment? segment, int offset, int levels, int valueIndex, bool dictionary, ref IndexRunCursor cursor)
    {
        var pieces = _open!;
        int nextNonNull = segment is null ? 0
            : _maxDefLevel > 0 ? CountNonNull(segment.Def!, offset, levels, _maxDefLevel) : levels;
        int total = _openLevels + levels;
        var joined = new Segment
        {
            Start = 0,
            End = total,
            ValueStart = 0,
            NonNull = _openNonNull + nextNonNull,
            Def = _maxDefLevel > 0 ? new int[total] : null,
            Rep = _maxRepLevel > 0 ? new int[total] : null,
        };
        joined.ValueDef = _maxDefLevel switch
        {
            0 => null,
            1 => joined.Def,
            _ => new int[total],
        };

        int position = 0;
        foreach (var piece in pieces)
        {
            CopyLevels(piece, piece.Start, piece.Levels, joined, position);
            position += piece.Levels;
        }
        if (segment is not null)
            CopyLevels(segment, offset, levels, joined, position);

        if (dictionary)
        {
            var indices = new int[joined.NonNull];
            int value = 0;
            foreach (var piece in pieces)
            {
                piece.ReadIndices(piece.ValueStart, indices.AsSpan(value, piece.NonNull));
                value += piece.NonNull;
            }
            if (segment is not null)
            {
                if (segment.Runs is not null)
                {
                    var peek = cursor;
                    peek.Read(indices.AsSpan(value));
                }
                else
                {
                    segment.Indices.AsSpan(valueIndex, nextNonNull).CopyTo(indices.AsSpan(value));
                }
            }
            joined.Indices = indices;
        }
        else
        {
            var parts = new List<IArrowArray>(pieces.Count + 1);
            foreach (var piece in pieces)
                parts.Add(TakeRows(piece.Array!, piece.Start, piece.Levels));
            if (segment is not null)
                parts.Add(TakeRows(segment.Array!, offset, levels));
            joined.Array = parts.Count == 1 ? parts[0] : ArrowCompute.Concatenate(parts);
        }

        return joined;
    }

    private void CopyLevels(Segment from, int start, int length, Segment to, int position)
    {
        if (to.Def is not null)
            from.Def.AsSpan(start, length).CopyTo(to.Def.AsSpan(position));
        if (to.Rep is not null)
            from.Rep.AsSpan(start, length).CopyTo(to.Rep.AsSpan(position));
        if (_maxDefLevel > 1)
            from.ValueDef.AsSpan(start, length).CopyTo(to.ValueDef.AsSpan(position));
    }

    private static int[]? JoinLevels(int[]? first, int firstStart, int firstLength, int[]? second, int secondStart, int secondLength)
    {
        if (first is null && second is null)
            return null;
        var joined = new int[firstLength + secondLength];
        first.AsSpan(firstStart, firstLength).CopyTo(joined);
        second.AsSpan(secondStart, secondLength).CopyTo(joined.AsSpan(firstLength));
        return joined;
    }

    /// <summary>A copy of rows [<paramref name="start"/>, +<paramref name="length"/>) of an array, at offset 0.</summary>
    private static IArrowArray TakeRows(IArrowArray array, int start, int length)
    {
        if (start == 0 && length == array.Length && array.Data.Offset == 0)
            return array;
        var rows = new int[length];
        for (int i = 0; i < length; i++)
            rows[i] = start + i;
        return ArrowCompute.Take(array, rows);
    }

    /// <summary>Emits the open page, if there is one: its one piece as it is, or its pieces joined.</summary>
    private void FlushTail()
    {
        if (_open is null)
            return;

        var noRuns = default(IndexRunCursor);
        var page = _open.Count == 1 ? _open[0] : JoinOpen(null, 0, 0, 0, _tailDictionary, ref noRuns);
        ClearOpenPage();
        EmitPage(page, page.Start, page.Levels, page.NonNull, page.ValueStart, _tailDictionary, ref noRuns);
    }

    /// <summary>
    /// Emits one data page: level positions [<paramref name="offset"/>, +<paramref name="pageValues"/>) of a segment,
    /// whose first value is value <paramref name="valueIndex"/>.
    /// </summary>
    private void EmitPage(
        Segment segment, int offset, int pageValues, int pageNonNull, int valueIndex, bool dictionary,
        ref IndexRunCursor cursor)
    {
        if (dictionary)
            EmitDictionaryPage(segment, offset, pageValues, pageNonNull, valueIndex, ref cursor);
        else
            EmitValuePage(segment, offset, pageValues, pageNonNull, valueIndex);
    }

    /// <summary>A data page in the column's own encoding.</summary>
    private void EmitValuePage(Segment segment, int offset, int pageValues, int pageNonNull, int valueIndex)
    {
        var options = _options;
        var physicalType = _physicalType;
        int typeLength = _typeLength;
        var array = segment.Array!;
        var output = _output!;
        var pageIndex = _pageIndex;

        // A copied open page lost its slice of the compressed values; compress it again with the chunk's table.
        if (_fsstTable is not null && segment.Fsst is null && pageNonNull > 0)
        {
            segment.Fsst = FsstCompressedColumn.Compress(
                _fsstTable, ExtractDenseByteArrays(segment, segment.Start, segment.End));
            segment.FsstBase = segment.ValueStart;
        }

        Encoding pageEncoding;
        EmittedPage page;
        if (options.DataPageVersion == DataPageVersion.V2)
        {
            page = WriteDataPageV2(output, array, offset, pageValues, pageNonNull,
                physicalType, typeLength, _maxDefLevel, _maxRepLevel, segment.Def, segment.Rep,
                segment.ValueDef, options, _defEncoder, _repEncoder,
                segment.Fsst, valueIndex - segment.FsstBase, out pageEncoding);
        }
        else
        {
            page = WriteDataPageV1(output, array, offset, pageValues, pageNonNull,
                physicalType, typeLength, _maxDefLevel, _maxRepLevel, segment.Def, segment.Rep,
                segment.ValueDef, options, _defEncoder, _repEncoder, out pageEncoding);
        }

        long? pageUnencodedBytes = RecordsUnencodedBytes(physicalType, options)
            ? ByteArrayDataBytes(array, offset, pageValues, segment.ValueDef)
            : null;
        _unencodedBytes += pageUnencodedBytes ?? 0;

        if (pageIndex != null)
        {
            // The same scans the chunk statistics run, over this page's level positions.
            var bounds = pageIndex.FoldsBounds && pageNonNull > 0
                ? StatisticsCollector.ComputePageBounds(
                    array, physicalType, typeLength, segment.ValueDef, offset, pageValues,
                    pageIndex.FloatingPointTotalOrder, pageIndex.Order)
                : default;
            pageIndex.AddPage(page, PageRows(segment.Rep, offset, pageValues, _maxRepLevel),
                pageValues - pageNonNull, pageNonNull, bounds, pageUnencodedBytes);
        }

        _encodings.Add(pageEncoding);
    }

    /// <summary>An RLE_DICTIONARY data page, its indices at the bit width the dictionary has now.</summary>
    private void EmitDictionaryPage(
        Segment segment, int offset, int pageValues, int pageNonNull, int valueIndex, ref IndexRunCursor cursor)
    {
        var options = _options;
        var pageIndex = _pageIndex;
        var output = _output!;
        bool runs = segment.Runs is not null;

        int bitWidth = DictionaryEncoder.GetIndexBitWidth(DictionaryCount);
        if (_indexEncoder is null || _indexBitWidth != bitWidth)
        {
            _indexEncoder = new RleBitPackedEncoder(bitWidth, maxLiteralGroups: options.MaxLiteralGroups);
            _indexBitWidth = bitWidth;
        }
        var indexEncoder = _indexEncoder;

        // Before the indices are encoded, which advances the run cursor: bounds read a copy of it.
        PageBounds bounds = default;
        if (pageIndex is { FoldsBounds: true } && pageNonNull > 0)
        {
            var dictionaryBounds = _dictionaryBounds ??= new DictionaryPageBounds(
                DictionaryEntries, _physicalType, pageIndex.Order, pageIndex.FloatingPointTotalOrder);
            dictionaryBounds.Begin();
            if (!runs)
            {
                dictionaryBounds.Add(segment.Indices.AsSpan(valueIndex, pageNonNull));
            }
            else
            {
                var peek = cursor;
                peek.VisitNext(pageNonNull, dictionaryBounds);
            }

            bounds = dictionaryBounds.End();
        }

        // SizeStatistics' unencoded_byte_array_data_bytes, per page and for the chunk (#890): each value
        // counts its entry's length, so the entries are measured once and the indices summed.
        // Also before the indices are encoded, for the same reason.
        long? pageUnencodedBytes = null;
        if (RecordsUnencodedBytes(_physicalType, options))
        {
            int[] entryLengths = EntryLengths();
            pageUnencodedBytes = !runs
                ? SumEntryLengths(segment.Indices.AsSpan(valueIndex, pageNonNull), entryLengths)
                : cursor.SumEntryLengths(pageNonNull, entryLengths);
            _unencodedBytes += pageUnencodedBytes.Value;
        }

        // Encoded up front rather than inside each page writer: both need the same bytes in the same
        // thread-static buffer, and only here is it known which form the indices arrived in.
        int valuesLen = !runs
            ? EncodeDictionaryIndicesToBuffer(
                segment.Indices.AsSpan(valueIndex, pageNonNull), pageNonNull, bitWidth, indexEncoder)
            : EncodeDictionaryIndexRunsToBuffer(
                ref cursor, pageNonNull, bitWidth, indexEncoder);

        EmittedPage page = options.DataPageVersion == DataPageVersion.V2
            ? WriteDictDataPageV2(output, valuesLen, offset, pageValues, pageNonNull,
                _maxDefLevel, _maxRepLevel, segment.Def, segment.Rep, options, _defEncoder, _repEncoder)
            : WriteDictDataPageV1(output, valuesLen, offset, pageValues, pageNonNull,
                _maxDefLevel, _maxRepLevel, segment.Def, segment.Rep, options, _defEncoder, _repEncoder);

        pageIndex?.AddPage(page, PageRows(segment.Rep, offset, pageValues, _maxRepLevel),
            pageValues - pageNonNull, pageNonNull, bounds, pageUnencodedBytes);
    }

    /// <summary>Each dictionary entry's length, measured as far as the dictionary has entries.</summary>
    private int[] EntryLengths()
    {
        var entries = DictionaryEntries;
        int count = entries.Count;
        if (_entryLengths is null || _entryLengths.Length < count)
            System.Array.Resize(ref _entryLengths, Math.Max(count, (_entryLengths?.Length ?? 0) * 2));
        for (; _entryLengthCount < count; _entryLengthCount++)
            _entryLengths[_entryLengthCount] = entries.Entry(_entryLengthCount).Length;
        return _entryLengths;
    }

    /// <summary>
    /// A collector for one chunk's page index. The ColumnIndex follows the chunk statistics: a column
    /// without them gets none, and neither does INT96 (its order is undefined). The OffsetIndex is
    /// written regardless.
    /// </summary>
    private static PageIndexCollector NewPageIndexCollector(
        Apache.Arrow.Types.IArrowType valueType, PhysicalType physicalType, int typeLength,
        StatisticsOrder order, IReadOnlyList<string> pathInSchema, ParquetWriteOptions options) =>
        new(physicalType, typeLength, order,
            floatingPointTotalOrder: options.FloatingPointOrder == FloatingPointColumnOrder.Ieee754TotalOrder,
            truncateLength: options.PageIndexTruncateLength,
            utf8: valueType is StringType or LargeStringType or StringViewType,
            writeColumnIndex: options.GetWriteStatistics(pathInSchema)
                && physicalType != PhysicalType.Int96,
            recordsUnencodedBytes: RecordsUnencodedBytes(physicalType, options));

    /// <summary>
    /// Whether a chunk records its BYTE_ARRAY value bytes (SizeStatistics' unencoded_byte_array_data_bytes, #890):
    /// a BYTE_ARRAY chunk under <see cref="ParquetWriteOptions.WriteSizeStatistics"/>.
    /// </summary>
    private static bool RecordsUnencodedBytes(PhysicalType physicalType, ParquetWriteOptions options) =>
        physicalType == PhysicalType.ByteArray && options.WriteSizeStatistics;

    /// <summary>Rows that start in a page: every level entry for a flat column, rep = 0 for a repeated one.</summary>
    private static int PageRows(int[]? repLevels, int offset, int pageValues, int maxRepLevel) =>
        maxRepLevel > 0 ? CountRows(repLevels, offset, pageValues, maxRepLevel) : pageValues;

    /// <summary>
    /// A page's length in level entries before record alignment: what <paramref name="valuesPerPage"/>
    /// allows, shortened to <paramref name="rowLimit"/> rows. For a repeated leaf the row cut falls
    /// where the first record past the limit begins, which is already a record boundary.
    /// </summary>
    /// <param name="levelCount">Level entries in the whole chunk.</param>
    private static int PageTargetLength(int[]? repLevels, int offset, int valuesPerPage, int levelCount, int? rowLimit)
    {
        int target = Math.Min(valuesPerPage, levelCount - offset);
        if (rowLimit is not int limit)
            return target;
        if (repLevels is null)
            return Math.Min(target, limit);

        int rows = 0;
        for (int i = offset; i < offset + target; i++)
        {
            if (repLevels[i] == 0 && ++rows > limit)
                return i - offset;
        }

        return target;
    }

    /// <summary>
    /// Moves a page's end onto a record boundary, so that every page of a repeated leaf begins at
    /// repetition level 0. V2 data pages require that, and so does an OffsetIndex for V1 pages (#389).
    /// </summary>
    /// <remarks>
    /// A cut that falls inside a record backs up to that record's start, which keeps the page within
    /// its target. Only when the record began the page, and so is larger than a page on its own, does
    /// the cut move forward to the record's end instead: that record gets an oversized page to itself,
    /// as in parquet-mr and arrow-rs. Each level entry is scanned at most twice across the chunk.
    /// </remarks>
    /// <param name="targetLength">The page length in level entries before alignment; at least 1.</param>
    /// <param name="levelCount">Level entries in the whole chunk.</param>
    private static int RecordAlignedPageLength(int[]? repLevels, int offset, int targetLength, int levelCount)
    {
        int end = offset + targetLength;
        if (repLevels is null || end >= levelCount || repLevels[end] == 0)
            return targetLength;

        int recordStart = end - 1;
        while (recordStart > offset && repLevels[recordStart] != 0)
            recordStart--;
        if (recordStart > offset)
            return recordStart - offset;

        while (end < levelCount && repLevels[end] != 0)
            end++;
        return end - offset;
    }

    /// <summary>
    /// Whether this column chunk should attempt FSST. V2 pages only — a V1 page always writes
    /// PLAIN — and BYTE_ARRAY only, which is all §1.3 defines.
    /// </summary>
    private static bool UsesFsst(PhysicalType physicalType, ParquetWriteOptions options) =>
        options.DataPageVersion == DataPageVersion.V2
        && physicalType == PhysicalType.ByteArray
#pragma warning disable EWPARQUET0003 // Caller has opted in via the experimental enum value; internal dispatch should not re-warn.
        && options.ByteArrayEncoding is ByteArrayEncoding.Fsst or ByteArrayEncoding.Fsst16;
#pragma warning restore EWPARQUET0003

    /// <summary>
    /// Which symbol table the chunk trains. Both widths are written as
    /// <see cref="Encoding.Fsst"/>; the SYMBOL_TABLE_PAGE header is what tells them apart
    /// (§2.3), so the choice never reaches the data pages.
    /// </summary>
#pragma warning disable EWPARQUET0003 // Caller has opted in via the experimental enum value; internal dispatch should not re-warn.
    private static SymbolTableType FsstTableType(ParquetWriteOptions options) =>
        options.ByteArrayEncoding == ByteArrayEncoding.Fsst16
            ? SymbolTableType.Fsst16
            : SymbolTableType.Fsst;
#pragma warning restore EWPARQUET0003

    /// <summary>
    /// Materializes the non-null values at level positions [<paramref name="start"/>, <paramref name="end"/>) so
    /// the symbol table can be trained over them, or they can be compressed with it. Reads the Arrow buffers the
    /// same way the DELTA_LENGTH_BYTE_ARRAY path does.
    /// </summary>
    private static byte[][] ExtractDenseByteArrays(Segment segment, int start, int end)
    {
        var data = segment.Array!.Data;
        ReadOnlySpan<int> arrowOffsets = MemoryMarshal.Cast<byte, int>(data.Buffers[1].Span);
        ReadOnlySpan<byte> arrowData = data.Buffers[2].Span;
        var defLevels = segment.ValueDef;

        int count = 0;
        for (int i = start; i < end; i++)
        {
            if (defLevels == null || defLevels[i] != 0)
                count++;
        }

        var values = new byte[count][];
        int idx = 0;
        for (int i = start; i < end; i++)
        {
            if (defLevels != null && defLevels[i] == 0) continue;
            int valueStart = arrowOffsets[i];
            int len = arrowOffsets[i + 1] - valueStart;
            values[idx++] = arrowData.Slice(valueStart, len).ToArray();
        }

        return values;
    }

    private static int WriteSymbolTablePage(
        ColumnChunkOutput output,
        FsstSymbolTable table,
        ParquetWriteOptions options)
    {
        byte[] body = table.Serialize();

        int compressedLen = CompressTo(body, options);
        bool isCompressed = options.Compression != CompressionCodec.Uncompressed;

        var pageHeader = new PageHeader
        {
            Type = PageType.SymbolTablePage,
            UncompressedPageSize = body.Length,
            CompressedPageSize = compressedLen,
            SymbolTablePageHeader = new SymbolTablePageHeader
            {
                Type = table.Type,
                IsCompressed = isCompressed,
            },
        };

        return output.EmitPage(pageHeader, t_compressBuffer.AsSpan(0, compressedLen)).Size;
    }

    /// <summary>
    /// Writes a dictionary-encoded column from a pre-built <see cref="DictionaryEncoder.DictionaryResult"/>.
    /// Used by <see cref="BufferedParquetWriter"/> which builds dictionaries incrementally.
    /// </summary>
    internal static ColumnChunkResult WriteDictionaryColumnFromResult(
        DictionaryEncoder.DictionaryResult dictResult,
        int rowCount,
        int nonNullCount,
        IReadOnlyList<string> pathInSchema,
        PhysicalType physicalType,
        int typeLength,
        Apache.Arrow.Types.IArrowType arrowType,
        int maxDefLevel,
        int maxRepLevel,
        int[]? defLevels,
        int[]? repLevels,
        ParquetWriteOptions options)
    {
        var writer = new ColumnChunkWriter(pathInSchema, physicalType, typeLength, maxDefLevel, maxRepLevel, options);
        writer.AppendDictionaryEncoded(dictResult, arrowType, defLevels, repLevels, nonNullCount, rowCount);
        return writer.Finish();
    }

    private static int WriteDictionaryPage(
        ColumnChunkOutput output,
        byte[] dictData,
        int dictionaryCount,
        ParquetWriteOptions options)
    {
        int compressedLen = CompressTo(dictData, options);

        var pageHeader = new PageHeader
        {
            Type = PageType.DictionaryPage,
            UncompressedPageSize = dictData.Length,
            CompressedPageSize = compressedLen,
            DictionaryPageHeader = new DictionaryPageHeader
            {
                NumValues = dictionaryCount,
                Encoding = Encoding.Plain,
            },
        };

        return output.EmitPage(pageHeader, t_compressBuffer.AsSpan(0, compressedLen)).Size;
    }

    /// <param name="uncompressedValuesSize">
    /// Bytes of encoded dictionary indices already sitting in the thread-static values buffer — see the
    /// page loop in <see cref="EmitDictionaryPage"/>, which encodes them.
    /// </param>
    private static EmittedPage WriteDictDataPageV2(
        ColumnChunkOutput output,
        int uncompressedValuesSize, int rowOffset, int numValues, int nonNullCount,
        int maxDefLevel, int maxRepLevel, int[]? defLevels, int[]? repLevels,
        ParquetWriteOptions options,
        RleBitPackedEncoder? defEncoder, RleBitPackedEncoder? repEncoder)
    {
        // Encode repetition levels (RLE, no length prefix for V2)
        int repLevelLen = 0;
        if (repEncoder != null)
        {
            repEncoder.Reset();
            repEncoder.Encode(repLevels.AsSpan(rowOffset, numValues));
            repLevelLen = repEncoder.Length;
        }

        // Encode definition levels (RLE, no length prefix for V2)
        int defLevelLen = 0;
        if (defEncoder != null)
        {
            defEncoder.Reset();
            defEncoder.Encode(defLevels.AsSpan(rowOffset, numValues));
            defLevelLen = defEncoder.Length;
        }

        // Compress values section only (V2: levels are uncompressed)
        int compressedValuesLen = CompressTo(
            t_valuesBuffer.AsSpan(0, uncompressedValuesSize), options);

        int numRows = maxRepLevel > 0 ? CountRows(repLevels, rowOffset, numValues, maxRepLevel) : numValues;

        var pageHeader = new PageHeader
        {
            Type = PageType.DataPageV2,
            UncompressedPageSize = repLevelLen + defLevelLen + uncompressedValuesSize,
            CompressedPageSize = repLevelLen + defLevelLen + compressedValuesLen,
            DataPageHeaderV2 = new DataPageHeaderV2
            {
                NumValues = numValues,
                NumNulls = numValues - nonNullCount,
                NumRows = numRows,
                Encoding = Encoding.RleDictionary,
                DefinitionLevelsByteLength = defLevelLen,
                RepetitionLevelsByteLength = repLevelLen,
                IsCompressed = options.Compression != CompressionCodec.Uncompressed,
            },
        };

        return output.EmitPage(
            pageHeader,
            repLevelLen > 0 ? repEncoder!.WrittenSpan : default,
            defLevelLen > 0 ? defEncoder!.WrittenSpan : default,
            t_compressBuffer.AsSpan(0, compressedValuesLen));
    }

    /// <param name="valuesLen">
    /// Bytes of encoded dictionary indices already sitting in the thread-static values buffer — see the
    /// page loop in <see cref="EmitDictionaryPage"/>, which encodes them.
    /// </param>
    private static EmittedPage WriteDictDataPageV1(
        ColumnChunkOutput output,
        int valuesLen, int rowOffset, int numValues, int nonNullCount,
        int maxDefLevel, int maxRepLevel, int[]? defLevels, int[]? repLevels,
        ParquetWriteOptions options,
        RleBitPackedEncoder? defEncoder, RleBitPackedEncoder? repEncoder)
    {
        // Build uncompressed body: rep levels + def levels + values
        // All concatenated and compressed together for V1
        int repRleLen = 0, defRleLen = 0;

        if (repEncoder != null)
        {
            repEncoder.Reset();
            repEncoder.Encode(repLevels.AsSpan(rowOffset, numValues));
            repRleLen = repEncoder.Length;
        }

        if (defEncoder != null)
        {
            defEncoder.Reset();
            defEncoder.Encode(defLevels.AsSpan(rowOffset, numValues));
            defRleLen = defEncoder.Length;
        }

        int repPrefixedLen = repRleLen > 0 ? 4 + repRleLen : 0;
        int defPrefixedLen = defRleLen > 0 ? 4 + defRleLen : 0;
        int uncompressedBodySize = repPrefixedLen + defPrefixedLen + valuesLen;
        byte[] uncompressedBody = new byte[uncompressedBodySize];
        int bPos = 0;

        if (repRleLen > 0)
        {
            BinaryPrimitives.WriteInt32LittleEndian(uncompressedBody.AsSpan(bPos), repRleLen);
            bPos += 4;
            repEncoder!.WrittenSpan.CopyTo(uncompressedBody.AsSpan(bPos));
            bPos += repRleLen;
        }
        if (defRleLen > 0)
        {
            BinaryPrimitives.WriteInt32LittleEndian(uncompressedBody.AsSpan(bPos), defRleLen);
            bPos += 4;
            defEncoder!.WrittenSpan.CopyTo(uncompressedBody.AsSpan(bPos));
            bPos += defRleLen;
        }
        t_valuesBuffer.AsSpan(0, valuesLen).CopyTo(uncompressedBody.AsSpan(bPos));

        int compressedLen = CompressTo(uncompressedBody, options);

        var pageHeader = new PageHeader
        {
            Type = PageType.DataPage,
            UncompressedPageSize = uncompressedBodySize,
            CompressedPageSize = compressedLen,
            DataPageHeader = new DataPageHeader
            {
                NumValues = numValues,
                Encoding = Encoding.RleDictionary,
                DefinitionLevelEncoding = Encoding.Rle,
                RepetitionLevelEncoding = Encoding.Rle,
            },
        };

        return output.EmitPage(pageHeader, t_compressBuffer.AsSpan(0, compressedLen));
    }

    /// <summary>
    /// Encodes dictionary indices (1-byte bit width + RLE/BP hybrid) into the
    /// thread-static values buffer. Returns the number of bytes written.
    /// </summary>
    private static int EncodeDictionaryIndicesToBuffer(
        ReadOnlySpan<int> indices, int nonNullCount, int bitWidth,
        RleBitPackedEncoder indexEncoder)
    {
        if (nonNullCount == 0)
            return EmptyIndexBuffer(bitWidth);

        indexEncoder.Reset();
        indexEncoder.Encode(indices);
        return FinishIndexBuffer(bitWidth, indexEncoder);
    }

    /// <summary>
    /// The run-form twin of <see cref="EncodeDictionaryIndicesToBuffer"/>: encodes the next
    /// <paramref name="nonNullCount"/> indices straight out of the runs, advancing
    /// <paramref name="cursor"/> past them. Produces the same bytes the per-row form would, since the
    /// RLE/bit-packing hybrid is itself run-driven — it just skips rediscovering the runs.
    /// </summary>
    private static int EncodeDictionaryIndexRunsToBuffer(
        ref IndexRunCursor cursor, int nonNullCount, int bitWidth, RleBitPackedEncoder indexEncoder)
    {
        if (nonNullCount == 0)
            return EmptyIndexBuffer(bitWidth);

        indexEncoder.Reset();
        indexEncoder.BeginRuns();
        cursor.EncodeNext(nonNullCount, indexEncoder);
        indexEncoder.EndRuns();
        return FinishIndexBuffer(bitWidth, indexEncoder);
    }

    /// <summary>A page with no values: the bit-width byte and nothing else.</summary>
    private static int EmptyIndexBuffer(int bitWidth)
    {
        EnsureValuesBuffer(1);
        t_valuesBuffer![0] = (byte)bitWidth;
        return 1;
    }

    /// <summary>Prefixes the encoded indices with the bit-width byte the page format requires.</summary>
    private static int FinishIndexBuffer(int bitWidth, RleBitPackedEncoder indexEncoder)
    {
        int totalLen = 1 + indexEncoder.Length;
        EnsureValuesBuffer(totalLen);
        t_valuesBuffer![0] = (byte)bitWidth;
        indexEncoder.WrittenSpan.CopyTo(t_valuesBuffer.AsSpan(1));
        return totalLen;
    }

    /// <summary>
    /// A position within run-form dictionary indices, so consecutive pages can each take their share of a
    /// run that straddles them.
    /// </summary>
    private struct IndexRunCursor
    {
        private readonly int[] _values;
        private readonly int[] _lengths;
        private int _run;
        private int _taken; // rows already consumed from _run

        public IndexRunCursor(DictionaryEncoder.DictionaryIndexRuns runs)
        {
            _values = runs.Values;
            _lengths = runs.Lengths;
            _run = 0;
            _taken = 0;
        }

        /// <summary>
        /// Feeds the next <paramref name="count"/> indices to <paramref name="bounds"/>, a run at a time.
        /// Advances this cursor, so call it on a copy to leave the original where it was.
        /// </summary>
        public void VisitNext(int count, DictionaryPageBounds bounds)
        {
            while (count > 0 && _run < _values.Length)
            {
                int take = Math.Min(_lengths[_run] - _taken, count);
                bounds.Add(_values[_run], take);

                _taken += take;
                count -= take;

                if (_taken == _lengths[_run])
                {
                    _run++;
                    _taken = 0;
                }
            }
        }

        /// <summary>
        /// The summed <paramref name="entryLengths"/> of the next <paramref name="count"/> indices, a run
        /// at a time. Reads without advancing: the cursor is a struct, and this works on its own copy.
        /// </summary>
        public readonly long SumEntryLengths(int count, int[] entryLengths)
        {
            long total = 0;
            int run = _run, taken = _taken;
            while (count > 0 && run < _values.Length)
            {
                int take = Math.Min(_lengths[run] - taken, count);
                total += (long)take * entryLengths[_values[run]];
                count -= take;
                run++;
                taken = 0;
            }

            return total;
        }

        /// <summary>Moves past the next <paramref name="count"/> indices.</summary>
        public void Skip(int count)
        {
            while (count > 0 && _run < _values.Length)
            {
                int take = Math.Min(_lengths[_run] - _taken, count);
                _taken += take;
                count -= take;
                if (_taken == _lengths[_run])
                {
                    _run++;
                    _taken = 0;
                }
            }
        }

        /// <summary>Reads the next indices into <paramref name="destination"/>, one per value, advancing past them.</summary>
        public void Read(Span<int> destination)
        {
            int position = 0;
            while (position < destination.Length && _run < _values.Length)
            {
                int take = Math.Min(_lengths[_run] - _taken, destination.Length - position);
                destination.Slice(position, take).Fill(_values[_run]);
                position += take;
                _taken += take;
                if (_taken == _lengths[_run])
                {
                    _run++;
                    _taken = 0;
                }
            }
        }

        /// <summary>Feeds the next <paramref name="count"/> indices to <paramref name="encoder"/> as runs.</summary>
        public void EncodeNext(int count, RleBitPackedEncoder encoder)
        {
            while (count > 0 && _run < _values.Length)
            {
                int take = Math.Min(_lengths[_run] - _taken, count);
                encoder.AppendRun(_values[_run], take);

                _taken += take;
                count -= take;

                if (_taken == _lengths[_run])
                {
                    _run++;
                    _taken = 0;
                }
            }
        }
    }

    [ThreadStatic]
    private static byte[]? t_valuesBuffer;

    private static void EnsureValuesBuffer(int size)
    {
        if (t_valuesBuffer == null || t_valuesBuffer.Length < size)
            t_valuesBuffer = new byte[Math.Max(size, 4096)];
    }

    // ───── PLAIN encoding path ─────

    /// <summary>
    /// The bytes of the values at level positions [<paramref name="offset"/>, +<paramref name="numValues"/>)
    /// of a BYTE_ARRAY column, length prefixes excluded: a page's <c>unencoded_byte_array_data_bytes</c>.
    /// Indexed as the page encoders index it, one slot per level, nulls (<paramref name="defLevels"/> 0)
    /// skipped.
    /// </summary>
    private static long ByteArrayDataBytes(IArrowArray array, int offset, int numValues, int[]? defLevels)
    {
        ReadOnlySpan<int> offsets = MemoryMarshal.Cast<byte, int>(array.Data.Buffers[1].Span);
        if (defLevels == null)
            return (long)offsets[offset + numValues] - offsets[offset];

        long total = 0;
        for (int i = offset, end = offset + numValues; i < end; i++)
        {
            if (defLevels[i] != 0)
                total += offsets[i + 1] - offsets[i];
        }

        return total;
    }

    /// <summary>Each entry's length in a PLAIN BYTE_ARRAY dictionary page, whose entries each follow a 4-byte length.</summary>
    private static int[] DictionaryEntryLengths(byte[] dictionaryPage, int count)
    {
        var lengths = new int[count];
        int position = 0;
        for (int i = 0; i < count; i++)
        {
            lengths[i] = BinaryPrimitives.ReadInt32LittleEndian(dictionaryPage.AsSpan(position));
            position += 4 + lengths[i];
        }

        return lengths;
    }

    private static long SumEntryLengths(ReadOnlySpan<int> indices, int[] entryLengths)
    {
        long total = 0;
        foreach (int index in indices)
            total += entryLengths[index];
        return total;
    }

    private static int CountNonNull(int[] defLevels, int offset, int count, int maxDefLevel)
    {
        var span = defLevels.AsSpan(offset, count);
        int nonNull = 0;

        foreach (int v in span)
            nonNull += (v >= maxDefLevel) ? 1 : 0;

        return nonNull;
    }

    private static EmittedPage WriteDataPageV2(
        ColumnChunkOutput output,
        IArrowArray array, int offset, int numValues, int nonNullCount,
        PhysicalType physicalType, int typeLength, int maxDefLevel, int maxRepLevel,
        int[]? defLevels, int[]? repLevels, int[]? valueDefLevels,
        ParquetWriteOptions options,
        RleBitPackedEncoder? defEncoder, RleBitPackedEncoder? repEncoder,
        FsstCompressedColumn? fsstColumn, int valueIndex,
        out Encoding valueEncoding)
    {
        // Encode repetition levels (RLE, no length prefix for V2)
        int repLevelLen = 0;
        if (repEncoder != null)
        {
            repEncoder.Reset();
            repEncoder.Encode(repLevels.AsSpan(offset, numValues));
            repLevelLen = repEncoder.Length;
        }

        // Encode definition levels (RLE, no length prefix for V2)
        int defLevelLen = 0;
        if (defEncoder != null)
        {
            defEncoder.Reset();
            defEncoder.Encode(defLevels.AsSpan(offset, numValues));
            defLevelLen = defEncoder.Length;
        }

        // Encode values into reusable buffer with type-aware V2 encoding
        int uncompressedValuesSize = EncodeValuesToBuffer(
            array, offset, numValues, nonNullCount, physicalType, typeLength,
            valueDefLevels, options, fsstColumn, valueIndex, out valueEncoding);

        // Compress values section only (V2: levels are uncompressed)
        int compressedValuesLen = CompressTo(
            t_valuesBuffer.AsSpan(0, uncompressedValuesSize), options);

        int numNulls = numValues - nonNullCount;
        int numRows = maxRepLevel > 0 ? CountRows(repLevels, offset, numValues, maxRepLevel) : numValues;

        var pageHeader = new PageHeader
        {
            Type = PageType.DataPageV2,
            UncompressedPageSize = repLevelLen + defLevelLen + uncompressedValuesSize,
            CompressedPageSize = repLevelLen + defLevelLen + compressedValuesLen,
            DataPageHeaderV2 = new DataPageHeaderV2
            {
                NumValues = numValues,
                NumNulls = numNulls,
                NumRows = numRows,
                Encoding = valueEncoding,
                DefinitionLevelsByteLength = defLevelLen,
                RepetitionLevelsByteLength = repLevelLen,
                IsCompressed = options.Compression != CompressionCodec.Uncompressed,
            },
        };

        return output.EmitPage(
            pageHeader,
            repLevelLen > 0 ? repEncoder!.WrittenSpan : default,
            defLevelLen > 0 ? defEncoder!.WrittenSpan : default,
            t_compressBuffer.AsSpan(0, compressedValuesLen));
    }

    private static EmittedPage WriteDataPageV1(
        ColumnChunkOutput output,
        IArrowArray array, int offset, int numValues, int nonNullCount,
        PhysicalType physicalType, int typeLength, int maxDefLevel, int maxRepLevel,
        int[]? defLevels, int[]? repLevels, int[]? valueDefLevels,
        ParquetWriteOptions options,
        RleBitPackedEncoder? defEncoder, RleBitPackedEncoder? repEncoder,
        out Encoding valueEncoding)
    {
        // Encode levels
        int repRleLen = 0, defRleLen = 0;

        if (repEncoder != null)
        {
            repEncoder.Reset();
            repEncoder.Encode(repLevels.AsSpan(offset, numValues));
            repRleLen = repEncoder.Length;
        }

        if (defEncoder != null)
        {
            defEncoder.Reset();
            defEncoder.Encode(defLevels.AsSpan(offset, numValues));
            defRleLen = defEncoder.Length;
        }

        // Encode values with the same type-aware dispatch the V2 path uses (use normalized
        // valueDefLevels for null detection). A V1 page's values section is byte-for-byte what a V2
        // page's would be — the page header carries the encoding either way — so the only thing that
        // was ever V1-specific here is the level framing and the compression boundary below.
        // FSST is passed as absent: it is refused for V1 in WriteNonDictionaryPages before reaching here.
        int valuesLength = EncodeValuesToBuffer(
            array, offset, numValues, nonNullCount, physicalType, typeLength,
            valueDefLevels, options, fsstColumn: null, valueIndex: 0, out valueEncoding);
        // A view into the shared encode buffer, not a copy — valid only until the next call that
        // writes t_valuesBuffer, and consumed below when the body is assembled. The staging through
        // `uncompressedBody` is what the V1 page format costs: levels sit IN FRONT of the values and
        // the whole thing is compressed as one unit, so the values cannot be compressed where they
        // lie. The V2 path has no such constraint and calls CompressTo on this buffer directly.
        var valuesBytes = t_valuesBuffer.AsSpan(0, valuesLength);

        // Concatenate levels + values, then compress together (V1)
        int repPrefixedLen = repRleLen > 0 ? 4 + repRleLen : 0;
        int defPrefixedLen = defRleLen > 0 ? 4 + defRleLen : 0;
        int uncompressedBodySize = repPrefixedLen + defPrefixedLen + valuesBytes.Length;
        byte[] uncompressedBody = new byte[uncompressedBodySize];
        int bPos = 0;

        if (repRleLen > 0)
        {
            BinaryPrimitives.WriteInt32LittleEndian(uncompressedBody.AsSpan(bPos), repRleLen);
            bPos += 4;
            repEncoder!.WrittenSpan.CopyTo(uncompressedBody.AsSpan(bPos));
            bPos += repRleLen;
        }
        if (defRleLen > 0)
        {
            BinaryPrimitives.WriteInt32LittleEndian(uncompressedBody.AsSpan(bPos), defRleLen);
            bPos += 4;
            defEncoder!.WrittenSpan.CopyTo(uncompressedBody.AsSpan(bPos));
            bPos += defRleLen;
        }
        valuesBytes.CopyTo(uncompressedBody.AsSpan(bPos));

        int compressedLen = CompressTo(uncompressedBody, options);

        var pageHeader = new PageHeader
        {
            Type = PageType.DataPage,
            UncompressedPageSize = uncompressedBodySize,
            CompressedPageSize = compressedLen,
            DataPageHeader = new DataPageHeader
            {
                NumValues = numValues,
                Encoding = valueEncoding,
                DefinitionLevelEncoding = Encoding.Rle,
                RepetitionLevelEncoding = Encoding.Rle,
            },
        };

        return output.EmitPage(pageHeader, t_compressBuffer.AsSpan(0, compressedLen));
    }

    // ───── Compression ─────

    /// <summary>
    /// Compresses data into the thread-static compression buffer.
    /// Returns the number of compressed bytes written.
    /// For Uncompressed codec, copies data into the buffer and returns data.Length.
    /// </summary>
    private static int CompressTo(ReadOnlySpan<byte> data, ParquetWriteOptions options)
    {
        // An EMPTY section still needs a VALID compressed stream: a 0-byte payload declared compressed is
        // rejected by strict decoders (snappy's empty stream is the single 0x00 length varint — SQL Server
        // fails an all-null V2 page's zero-byte values section with "Corrupt snappy compressed data";
        // Delta CHECKPOINT files are full of all-null column chunks). Let the codec encode emptiness.
        int maxLen = Compressor.GetMaxCompressedLength(options.Compression, data.Length);
        EnsureCompressBuffer(maxLen);
        return Compressor.Compress(
            options.Compression, data, t_compressBuffer!.AsSpan(0, maxLen),
            options.CompressionLevel, options.CustomCompressionLevel);
    }

    private static void EnsureCompressBuffer(int size)
    {
        if (t_compressBuffer == null || t_compressBuffer.Length < size)
            t_compressBuffer = new byte[Math.Max(size, 4096)];
    }

    // ───── V2 type-aware encoding dispatch ─────

    /// <summary>
    /// Encodes values into <see cref="t_valuesBuffer"/> using type-aware V2 encoding.
    /// Returns the number of bytes written and the encoding used.
    /// </summary>
    private static int EncodeValuesToBuffer(
        IArrowArray array, int offset, int numValues, int nonNullCount,
        PhysicalType physicalType, int typeLength, int[]? defLevels,
        ParquetWriteOptions options, FsstCompressedColumn? fsstColumn, int valueIndex,
        out Encoding encoding)
    {
        bool useDba = options.ByteArrayEncoding == ByteArrayEncoding.DeltaByteArray;
        bool usePlainBa = options.ByteArrayEncoding == ByteArrayEncoding.Plain;
        bool useAlp = options.FloatingPointEncoding == FloatingPointEncoding.Alp;
        bool usePlainFp = options.FloatingPointEncoding == FloatingPointEncoding.Plain;
#pragma warning disable EWPARQUET0005 // Caller has opted in via the experimental enum value; internal dispatch should not re-warn.
        bool usePfor = options.IntegerEncoding == IntegerEncoding.Pfor;
#pragma warning restore EWPARQUET0005
        bool usePlainInt = options.IntegerEncoding == IntegerEncoding.Plain;

        if (nonNullCount == 0)
        {
            // Zero defined values (an all-null page — e.g. every row's struct ancestor is null). The V2 delta
            // encodings (DELTA_BINARY_PACKED / DELTA_LENGTH_BYTE_ARRAY / ...) require a non-empty HEADER even
            // for zero values — declaring them with a 0-byte payload makes readers fail with buffer underruns.
            // PLAIN is the only encoding whose empty representation is valid for every type.
            encoding = Encoding.Plain;
            EnsureValuesBuffer(0);
            return 0;
        }

        encoding = EncodingStrategyResolver.GetValueEncoding(
            physicalType, options.ByteArrayEncoding, options.FloatingPointEncoding, options.IntegerEncoding);

        // FSST is decided per column chunk, not per page: when the chunk declined it (nothing
        // trainable, or no size win) the resolver's answer is stale and the page falls back.
        bool useFsst = fsstColumn != null && UsesFsst(physicalType, options);
#pragma warning disable EWPARQUET0003 // Caller has opted in via the experimental enum value; internal dispatch should not re-warn.
        if (!useFsst && encoding == Encoding.Fsst)
#pragma warning restore EWPARQUET0003
            encoding = Encoding.DeltaLengthByteArray;

        return physicalType switch
        {
            PhysicalType.ByteArray when useFsst =>
                EncodeFsstToBuffer(fsstColumn!, valueIndex, nonNullCount),
            PhysicalType.Boolean => EncodeBooleanValuesRleToBuffer(array, offset, numValues, nonNullCount, defLevels, options.MaxLiteralGroups),
            PhysicalType.Int32 when usePfor => EncodePforInt32ToBuffer(
                array, offset, numValues, nonNullCount, defLevels, ref encoding),
            PhysicalType.Int32 when usePlainInt => EncodePlainToBuffer(array, offset, numValues, nonNullCount, physicalType, typeLength, defLevels),
            PhysicalType.Int32 => EncodeDeltaInt32ToBuffer(array, offset, numValues, nonNullCount, defLevels),
            PhysicalType.Int64 when usePfor => EncodePforInt64ToBuffer(
                array, offset, numValues, nonNullCount, defLevels, ref encoding),
            PhysicalType.Int64 when usePlainInt => EncodePlainToBuffer(array, offset, numValues, nonNullCount, physicalType, typeLength, defLevels),
            PhysicalType.Int64 => EncodeDeltaInt64ToBuffer(array, offset, numValues, nonNullCount, defLevels),
            PhysicalType.Float when useAlp => EncodeAlpSingleToBuffer(
                array, offset, numValues, nonNullCount, defLevels, ref encoding),
            PhysicalType.Float when usePlainFp => EncodePlainToBuffer(array, offset, numValues, nonNullCount, physicalType, typeLength, defLevels),
            PhysicalType.Float => EncodeBssSingleToBuffer(array, offset, numValues, nonNullCount, defLevels),
            PhysicalType.Double when useAlp => EncodeAlpDoubleToBuffer(
                array, offset, numValues, nonNullCount, defLevels, ref encoding),
            PhysicalType.Double when usePlainFp => EncodePlainToBuffer(array, offset, numValues, nonNullCount, physicalType, typeLength, defLevels),
            PhysicalType.Double => EncodeBssDoubleToBuffer(array, offset, numValues, nonNullCount, defLevels),
            PhysicalType.ByteArray when usePlainBa => EncodePlainToBuffer(array, offset, numValues, nonNullCount, physicalType, typeLength, defLevels),
            PhysicalType.ByteArray when useDba => EncodeDbaByteArrayToBuffer(array, offset, numValues, nonNullCount, defLevels),
            PhysicalType.ByteArray => EncodeDlbaToBuffer(array, offset, numValues, nonNullCount, defLevels),
            PhysicalType.FixedLenByteArray when useDba => EncodeDbaFlbaToBuffer(array, offset, numValues, nonNullCount, defLevels, typeLength),
            _ => EncodePlainToBuffer(array, offset, numValues, nonNullCount, physicalType, typeLength, defLevels),
        };
    }

    private static int EncodeFsstToBuffer(
        FsstCompressedColumn fsstColumn, int valueIndex, int nonNullCount)
    {
        byte[] page = FsstPageEncoder.Encode(fsstColumn, valueIndex, nonNullCount);
        EnsureValuesBuffer(page.Length);
        page.CopyTo(t_valuesBuffer!, 0);
        return page.Length;
    }

    /// <summary>
    /// ALP for a FLOAT page, falling back to PLAIN when ALP would not shrink it.
    /// </summary>
    /// <remarks>
    /// ALP cannot fit every column, and when it does not fit it is not merely unhelpful but
    /// actively worse: values that do not survive the spec's decode formula are stored whole
    /// alongside a position, costing half again what writing them plainly would. MEASURED, that is
    /// two-decimal FLOAT values past about 2^16, where the format's 24-bit mantissa cannot reproduce
    /// them at all, and DOUBLE coordinate columns with no decimal structure. Declining per page
    /// keeps the guarantee the FSST writer already makes: choosing this encoding cannot make a file
    /// bigger than not choosing it.
    /// </remarks>
    private static int EncodeAlpSingleToBuffer(
        IArrowArray array, int offset, int numValues, int nonNullCount, int[]? defLevels,
        ref Encoding encoding)
    {
        ReadOnlySpan<float> source = MemoryMarshal.Cast<byte, float>(array.Data.Buffers[1].Span);
        float[] dense;
        ReadOnlySpan<float> values;
        if (defLevels == null)
        {
            values = source.Slice(offset, numValues);
            dense = [];
        }
        else
        {
            dense = ExtractNonNullValues<float>(array, offset, numValues, nonNullCount, defLevels);
            values = dense;
        }

        byte[] page = AlpEncoder.EncodeFloats(values);
        if (page.Length >= nonNullCount * sizeof(float))
        {
            // Written from the dense values already in hand. Going back through
            // EncodePlainToBuffer would walk the definition levels to compact them a second time
            // and allocate a buffer to do it, on a path that runs for every page of any column ALP
            // does not suit.
            encoding = Encoding.Plain;
            EnsureValuesBuffer(nonNullCount * sizeof(float));
            return PlainEncoder.EncodeFloats(values, t_valuesBuffer!);
        }

        EnsureValuesBuffer(page.Length);
        page.CopyTo(t_valuesBuffer!, 0);
        return page.Length;
    }

    /// <summary>
    /// ALP for a DOUBLE page. See <see cref="EncodeAlpSingleToBuffer"/> for why it may decline.
    /// </summary>
    private static int EncodeAlpDoubleToBuffer(
        IArrowArray array, int offset, int numValues, int nonNullCount, int[]? defLevels,
        ref Encoding encoding)
    {
        ReadOnlySpan<double> source = MemoryMarshal.Cast<byte, double>(array.Data.Buffers[1].Span);
        double[] dense;
        ReadOnlySpan<double> values;
        if (defLevels == null)
        {
            values = source.Slice(offset, numValues);
            dense = [];
        }
        else
        {
            dense = ExtractNonNullValues<double>(array, offset, numValues, nonNullCount, defLevels);
            values = dense;
        }

        byte[] page = AlpEncoder.EncodeDoubles(values);
        if (page.Length >= nonNullCount * sizeof(double))
        {
            // See EncodeAlpSingleToBuffer: written from the dense values rather than compacting a
            // second time.
            encoding = Encoding.Plain;
            EnsureValuesBuffer(nonNullCount * sizeof(double));
            return PlainEncoder.EncodeDoubles(values, t_valuesBuffer!);
        }

        EnsureValuesBuffer(page.Length);
        page.CopyTo(t_valuesBuffer!, 0);
        return page.Length;
    }


    private static int EncodeBooleanValuesRleToBuffer(
        IArrowArray array, int offset, int numValues, int nonNullCount, int[]? defLevels,
        int maxLiteralGroups)
    {
        var boolArray = (BooleanArray)array;
        var values = new int[nonNullCount];
        int idx = 0;
        for (int i = 0; i < numValues; i++)
        {
            if (defLevels == null || defLevels[offset + i] != 0)
                values[idx++] = boolArray.GetValue(offset + i) == true ? 1 : 0;
        }

        var encoder = new RleBitPackedEncoder(1, maxLiteralGroups: maxLiteralGroups);
        encoder.Encode(values);

        // 4-byte LE length prefix + RLE data
        int totalLen = 4 + encoder.Length;
        EnsureValuesBuffer(totalLen);
        BinaryPrimitives.WriteInt32LittleEndian(t_valuesBuffer, encoder.Length);
        encoder.WrittenSpan.CopyTo(t_valuesBuffer.AsSpan(4));
        return totalLen;
    }

    private static int EncodeDeltaInt32ToBuffer(
        IArrowArray array, int offset, int numValues, int nonNullCount, int[]? defLevels)
    {
        // Encodes straight into t_valuesBuffer, sized for the worst case, so a page allocates no
        // growing buffer of its own and its bytes are not copied afterwards.
        EnsureValuesBuffer(DeltaBinaryPackedEncoder.MaxEncodedLength(nonNullCount, 32));
        var encoder = new DeltaBinaryPackedEncoder(t_valuesBuffer!);
        // The encoder reads past the null slots itself, so a page with nulls is not compacted first.
        encoder.EncodeInt32s(
            MemoryMarshal.Cast<byte, int>(array.Data.Buffers[1].Span).Slice(offset, numValues),
            defLevels == null ? default : defLevels.AsSpan(offset, numValues),
            nonNullCount);

        t_valuesBuffer = encoder.Buffer;
        return encoder.Length;
    }

    private static int EncodeDeltaInt64ToBuffer(
        IArrowArray array, int offset, int numValues, int nonNullCount, int[]? defLevels)
    {
        EnsureValuesBuffer(DeltaBinaryPackedEncoder.MaxEncodedLength(nonNullCount, 64));
        var encoder = new DeltaBinaryPackedEncoder(t_valuesBuffer!);
        encoder.EncodeInt64s(
            MemoryMarshal.Cast<byte, long>(array.Data.Buffers[1].Span).Slice(offset, numValues),
            defLevels == null ? default : defLevels.AsSpan(offset, numValues),
            nonNullCount);

        t_valuesBuffer = encoder.Buffer;
        return encoder.Length;
    }

    /// <summary>
    /// Encodes an INT32 page as PFOR, falling back to PLAIN when PFOR did not shrink it.
    /// </summary>
    /// <remarks>
    /// The fallback is what makes <see cref="IntegerEncoding.Pfor"/> safe to turn on: uniformly
    /// random integers have no outliers to exploit and no frame worth subtracting, so the cost
    /// model lands on the full width and the page comes out larger than PLAIN by the size of its
    /// headers. Pages within one column chunk may therefore differ in encoding, which is what
    /// the format allows for.
    /// </remarks>
    private static int EncodePforInt32ToBuffer(
        IArrowArray array, int offset, int numValues, int nonNullCount, int[]? defLevels,
        ref Encoding encoding)
    {
        ReadOnlySpan<int> source = MemoryMarshal.Cast<byte, int>(array.Data.Buffers[1].Span);
        int[] dense;
        ReadOnlySpan<int> values;
        if (defLevels == null)
        {
            values = source.Slice(offset, numValues);
            dense = [];
        }
        else
        {
            dense = ExtractNonNullValues<int>(array, offset, numValues, nonNullCount, defLevels);
            values = dense;
        }

        byte[] page = PforEncoder.EncodeInt32s(values);
        if (page.Length >= nonNullCount * sizeof(int))
        {
            // Written from the dense values rather than compacting a second time.
            encoding = Encoding.Plain;
            EnsureValuesBuffer(nonNullCount * sizeof(int));
            return PlainEncoder.EncodeInt32s(values, t_valuesBuffer!);
        }

        EnsureValuesBuffer(page.Length);
        page.CopyTo(t_valuesBuffer!, 0);
        return page.Length;
    }

    /// <inheritdoc cref="EncodePforInt32ToBuffer"/>
    private static int EncodePforInt64ToBuffer(
        IArrowArray array, int offset, int numValues, int nonNullCount, int[]? defLevels,
        ref Encoding encoding)
    {
        ReadOnlySpan<long> source = MemoryMarshal.Cast<byte, long>(array.Data.Buffers[1].Span);
        long[] dense;
        ReadOnlySpan<long> values;
        if (defLevels == null)
        {
            values = source.Slice(offset, numValues);
            dense = [];
        }
        else
        {
            dense = ExtractNonNullValues<long>(array, offset, numValues, nonNullCount, defLevels);
            values = dense;
        }

        byte[] page = PforEncoder.EncodeInt64s(values);
        if (page.Length >= nonNullCount * sizeof(long))
        {
            encoding = Encoding.Plain;
            EnsureValuesBuffer(nonNullCount * sizeof(long));
            return PlainEncoder.EncodeInt64s(values, t_valuesBuffer!);
        }

        EnsureValuesBuffer(page.Length);
        page.CopyTo(t_valuesBuffer!, 0);
        return page.Length;
    }

    private static int EncodeBssSingleToBuffer(
        IArrowArray array, int offset, int numValues, int nonNullCount, int[]? defLevels)
    {
        int totalLen = nonNullCount * sizeof(float);
        EnsureValuesBuffer(totalLen);

        if (defLevels == null)
        {
            var span = MemoryMarshal.Cast<byte, float>(array.Data.Buffers[1].Span).Slice(offset, numValues);
            ByteStreamSplitEncoder.EncodeFloatsTo(span, t_valuesBuffer!);
        }
        else
        {
            var valueBuffer = MemoryMarshal.Cast<byte, float>(array.Data.Buffers[1].Span);
            ByteStreamSplitEncoder.EncodeFloatsTo(valueBuffer, offset, numValues, nonNullCount, defLevels, t_valuesBuffer!);
        }

        return totalLen;
    }

    private static int EncodeBssDoubleToBuffer(
        IArrowArray array, int offset, int numValues, int nonNullCount, int[]? defLevels)
    {
        int totalLen = nonNullCount * sizeof(double);
        EnsureValuesBuffer(totalLen);

        if (defLevels == null)
        {
            var span = MemoryMarshal.Cast<byte, double>(array.Data.Buffers[1].Span).Slice(offset, numValues);
            ByteStreamSplitEncoder.EncodeDoublesTo(span, t_valuesBuffer!);
        }
        else
        {
            var valueBuffer = MemoryMarshal.Cast<byte, double>(array.Data.Buffers[1].Span);
            ByteStreamSplitEncoder.EncodeDoublesTo(valueBuffer, offset, numValues, nonNullCount, defLevels, t_valuesBuffer!);
        }

        return totalLen;
    }

    private static int EncodeDlbaToBuffer(
        IArrowArray array, int offset, int numValues, int nonNullCount, int[]? defLevels)
    {
        var data = array.Data;
        ReadOnlySpan<int> arrowOffsets = MemoryMarshal.Cast<byte, int>(data.Buffers[1].Span);
        ReadOnlySpan<byte> arrowData = data.Buffers[2].Span;

        // Compute lengths and total data size in one pass
        var lengths = new int[nonNullCount];
        int totalDataLen = 0;
        int idx = 0;
        for (int i = 0; i < numValues; i++)
        {
            if (defLevels != null && defLevels[offset + i] == 0) continue;
            int start = arrowOffsets[offset + i];
            int len = arrowOffsets[offset + i + 1] - start;
            lengths[idx++] = len;
            totalDataLen += len;
        }

        // Delta-encode lengths
        EnsureValuesBuffer(DeltaBinaryPackedEncoder.MaxEncodedLength(nonNullCount, 32) + totalDataLen);
        var deltaEncoder = new DeltaBinaryPackedEncoder(t_valuesBuffer!);
        deltaEncoder.EncodeInt32s(lengths);
        t_valuesBuffer = deltaEncoder.Buffer;

        // Write delta_lengths + compact_data into values buffer
        int totalLen = deltaEncoder.Length + totalDataLen;

        // Copy non-null byte data directly
        int pos = deltaEncoder.Length;
        for (int i = 0; i < numValues; i++)
        {
            if (defLevels != null && defLevels[offset + i] == 0) continue;
            int start = arrowOffsets[offset + i];
            int len = arrowOffsets[offset + i + 1] - start;
            arrowData.Slice(start, len).CopyTo(t_valuesBuffer.AsSpan(pos));
            pos += len;
        }

        return totalLen;
    }

    private static int EncodeDbaByteArrayToBuffer(
        IArrowArray array, int offset, int numValues, int nonNullCount, int[]? defLevels)
    {
        var data = array.Data;
        ReadOnlySpan<int> arrowOffsets = MemoryMarshal.Cast<byte, int>(data.Buffers[1].Span);
        ReadOnlySpan<byte> arrowData = data.Buffers[2].Span;

        // Compute total data bytes for buffer sizing
        int totalDataBytes = 0;
        for (int i = 0; i < numValues; i++)
        {
            if (defLevels != null && defLevels[offset + i] == 0) continue;
            totalDataBytes += arrowOffsets[offset + i + 1] - arrowOffsets[offset + i];
        }

        int maxSize = DeltaByteArrayEncoder.EstimateMaxSize(nonNullCount, totalDataBytes);
        EnsureValuesBuffer(maxSize);

        return DeltaByteArrayEncoder.Encode(
            arrowOffsets, arrowData, offset, numValues, nonNullCount, defLevels, t_valuesBuffer!);
    }

    private static int EncodeDbaFlbaToBuffer(
        IArrowArray array, int offset, int numValues, int nonNullCount,
        int[]? defLevels, int typeLength)
    {
        var valueBuffer = array.Data.Buffers[1].Span;
        int totalDataBytes = nonNullCount * typeLength;

        int maxSize = DeltaByteArrayEncoder.EstimateMaxSize(nonNullCount, totalDataBytes);
        EnsureValuesBuffer(maxSize);

        return DeltaByteArrayEncoder.EncodeFixed(
            valueBuffer, typeLength, offset, numValues, nonNullCount, defLevels, t_valuesBuffer!);
    }

    private static int EncodePlainToBuffer(
        IArrowArray array, int offset, int numValues, int nonNullCount,
        PhysicalType physicalType, int typeLength, int[]? defLevels)
    {
        byte[] valuesBytes = EncodeValues(array, offset, numValues, nonNullCount,
            physicalType, typeLength, defLevels);
        EnsureValuesBuffer(valuesBytes.Length);
        valuesBytes.CopyTo(t_valuesBuffer.AsSpan());
        return valuesBytes.Length;
    }

    private static T[] ExtractNonNullValues<T>(
        IArrowArray array, int offset, int numValues, int nonNullCount, int[]? defLevels)
        where T : unmanaged
    {
        var valueBuffer = MemoryMarshal.Cast<byte, T>(array.Data.Buffers[1].Span);

        if (defLevels == null)
        {
            var result = new T[numValues];
            valueBuffer.Slice(offset, numValues).CopyTo(result);
            return result;
        }

        var values = new T[nonNullCount];
        PresentValues.Gather(valueBuffer.Slice(offset, numValues), defLevels.AsSpan(offset, numValues), values);
        return values;
    }

    // ───── PLAIN encoding (V1 and FLBA fallback) ─────

    private static byte[] EncodeValues(
        IArrowArray array, int offset, int numValues, int nonNullCount,
        PhysicalType physicalType, int typeLength, int[]? defLevels)
    {
        if (nonNullCount == 0)
            return [];

        return physicalType switch
        {
            PhysicalType.Boolean => EncodeBooleanValues(array, offset, numValues, nonNullCount, defLevels),
            PhysicalType.Int32 => EncodeFixedValues<int>(array, offset, numValues, nonNullCount, defLevels),
            PhysicalType.Int64 => EncodeFixedValues<long>(array, offset, numValues, nonNullCount, defLevels),
            PhysicalType.Float => EncodeFixedValues<float>(array, offset, numValues, nonNullCount, defLevels),
            PhysicalType.Double => EncodeFixedValues<double>(array, offset, numValues, nonNullCount, defLevels),
            PhysicalType.ByteArray => EncodeByteArrayValues(array, offset, numValues, nonNullCount, defLevels),
            PhysicalType.FixedLenByteArray => EncodeFixedLenByteArrayValues(
                array, offset, numValues, nonNullCount, defLevels, typeLength),
            _ => throw new NotSupportedException($"Unsupported physical type: {physicalType}"),
        };
    }

    private static byte[] EncodeBooleanValues(
        IArrowArray array, int offset, int numValues, int nonNullCount, int[]? defLevels)
    {
        var boolArray = (BooleanArray)array;
        var nonNullValues = new bool[nonNullCount];
        int idx = 0;

        for (int i = 0; i < numValues; i++)
        {
            if (defLevels == null || defLevels[offset + i] != 0)
                nonNullValues[idx++] = boolArray.GetValue(offset + i) ?? false;
        }

        var encoded = new byte[(nonNullCount + 7) / 8];
        PlainEncoder.EncodeBooleans(nonNullValues, encoded);
        return encoded;
    }

    private static byte[] EncodeFixedValues<T>(
        IArrowArray array, int offset, int numValues, int nonNullCount, int[]? defLevels)
        where T : unmanaged
    {
        int elementSize = Marshal.SizeOf<T>();

        if (defLevels == null)
        {
            // Required column: direct copy from Arrow value buffer
            var valueSpan = MemoryMarshal.Cast<byte, T>(array.Data.Buffers[1].Span).Slice(offset, numValues);
            var result = new byte[numValues * elementSize];
            MemoryMarshal.AsBytes(valueSpan).CopyTo(result);
            return result;
        }

        // Optional column: the non-null values, gathered straight into the encoded bytes
        var encoded = new byte[nonNullCount * elementSize];
        PresentValues.Gather(
            MemoryMarshal.Cast<byte, T>(array.Data.Buffers[1].Span).Slice(offset, numValues),
            defLevels.AsSpan(offset, numValues),
            MemoryMarshal.Cast<byte, T>(encoded.AsSpan()));
        return encoded;
    }

    private static byte[] EncodeByteArrayValues(
        IArrowArray array, int offset, int numValues, int nonNullCount, int[]? defLevels)
    {
        var data = array.Data;
        ReadOnlySpan<int> arrowOffsets;
        ReadOnlySpan<byte> arrowData;

        if (array is StringArray || array is BinaryArray)
        {
            arrowOffsets = MemoryMarshal.Cast<byte, int>(data.Buffers[1].Span);
            arrowData = data.Buffers[2].Span;
        }
        else
        {
            throw new NotSupportedException(
                $"Unsupported Arrow array type for BYTE_ARRAY: {array.GetType().Name}");
        }

        // Calculate total size
        int totalSize = 0;
        for (int i = 0; i < numValues; i++)
        {
            if (defLevels != null && defLevels[offset + i] == 0)
                continue;
            int idx = offset + i;
            int len = arrowOffsets[idx + 1] - arrowOffsets[idx];
            totalSize += 4 + len;
        }

        var result = new byte[totalSize];
        int pos = 0;

        for (int i = 0; i < numValues; i++)
        {
            if (defLevels != null && defLevels[offset + i] == 0)
                continue;

            int idx = offset + i;
            int start = arrowOffsets[idx];
            int len = arrowOffsets[idx + 1] - start;

            BinaryPrimitives.WriteInt32LittleEndian(result.AsSpan(pos), len);
            pos += 4;
            arrowData.Slice(start, len).CopyTo(result.AsSpan(pos));
            pos += len;
        }

        return result;
    }

    private static byte[] EncodeFixedLenByteArrayValues(
        IArrowArray array, int offset, int numValues, int nonNullCount,
        int[]? defLevels, int typeLength)
    {
        var data = array.Data;
        var valueBuffer = data.Buffers[1].Span;

        if (defLevels == null)
        {
            var result = new byte[numValues * typeLength];
            valueBuffer.Slice(offset * typeLength, numValues * typeLength).CopyTo(result);
            return result;
        }

        var encoded = new byte[nonNullCount * typeLength];
        int pos = 0;
        for (int i = 0; i < numValues; i++)
        {
            if (defLevels[offset + i] == 0) continue;
            int idx = offset + i;
            valueBuffer.Slice(idx * typeLength, typeLength).CopyTo(encoded.AsSpan(pos));
            pos += typeLength;
        }

        return encoded;
    }

    private static int EstimateValuesPerPage(
        IArrowArray array, PhysicalType physicalType, int typeLength, int targetPageSize)
    {
        int bytesPerValue = physicalType switch
        {
            PhysicalType.Boolean => 1, // conservative
            PhysicalType.Int32 or PhysicalType.Float => 4,
            PhysicalType.Int64 or PhysicalType.Double => 8,
            PhysicalType.Int96 => 12,
            PhysicalType.FixedLenByteArray => typeLength > 0 ? typeLength : 16,
            PhysicalType.ByteArray => EstimateAvgByteArraySize(array),
            _ => 8,
        };

        if (bytesPerValue == 0) bytesPerValue = 1;
        return Math.Max(1, targetPageSize / bytesPerValue);
    }

    private static int EstimateAvgByteArraySize(IArrowArray array)
    {
        if (array.Length == 0) return 32;

        var data = array.Data;
        if (data.Buffers.Length >= 3)
        {
            var offsets = MemoryMarshal.Cast<byte, int>(data.Buffers[1].Span);
            int totalDataBytes = offsets[array.Length] - offsets[0];
            int avgLen = totalDataBytes / Math.Max(1, array.Length);
            return 4 + avgLen;
        }

        return 32;
    }

    // The chunk stream is sized for an incompressible chunk: an estimate below what it needs costs a
    // doubling and a copy of the whole stream, which is more than the memory the headroom holds back
    // from a chunk that compresses. Each estimate below counts the pages the writer will cut, because
    // at small DataPageRowCountLimit values the per-page costs — headers, DELTA_BINARY_PACKED stream
    // headers and miniblock padding — outweigh the values, and a repeated column's pages are cut short
    // at record boundaries (#522 review). Every per-page cost is bounded by a term in the values plus a
    // constant per page, which holds however unevenly the pages are cut.

    // A data page header, which carries no statistics: measured at 22–28 bytes, with room for a CRC
    // and larger sizes.
    private const int PageHeaderAllowance = 40;

    /// <summary>
    /// The initial capacity of a non-dictionary chunk's stream.
    /// </summary>
    /// <param name="levelCount">Levels in the chunk: rows, for a column that is not repeated.</param>
    /// <param name="nonNullCount">Values in the chunk.</param>
    /// <param name="plainValueBytes">The values' PLAIN size, from <see cref="PlainValueBytes"/>.</param>
    /// <param name="pages">The pages the writer will cut, from <see cref="LayOutPages"/>.</param>
    internal static int EstimateColumnSize(
        int levelCount, int nonNullCount, long plainValueBytes, PageLayout pages,
        PhysicalType physicalType, int typeLength, int maxDefLevel, int maxRepLevel,
        bool deltaByteArray, CompressionCodec codec)
    {
        long bytes = plainValueBytes
            + pages.Count * PageHeaderAllowance
            + LevelBytes(levelCount, pages, maxDefLevel, allDefined: nonNullCount == levelCount)
            + LevelBytes(levelCount, pages, maxRepLevel, allDefined: false);

        // DELTA_BINARY_PACKED at full width is larger than the raw values (+1.8% for INT32). A
        // BYTE_ARRAY's lengths are its PLAIN size's 4-byte prefixes, which DELTA_LENGTH_BYTE_ARRAY
        // packs the same way, and DELTA_BYTE_ARRAY packs a second stream of them. On a
        // FIXED_LEN_BYTE_ARRAY, DELTA_BYTE_ARRAY adds both streams, each in [0, typeLength].
        if (physicalType is PhysicalType.Int32 or PhysicalType.Int64)
        {
            bytes += DeltaOverheadBytes(nonNullCount, pages, physicalType == PhysicalType.Int32 ? 32 : 64);
        }
        else if (physicalType == PhysicalType.ByteArray)
        {
            bytes += DeltaOverheadBytes(nonNullCount, pages, 32);
            if (deltaByteArray)
                bytes += 4L * nonNullCount + DeltaOverheadBytes(nonNullCount, pages, 32);
        }
        else if (physicalType == PhysicalType.FixedLenByteArray && deltaByteArray)
        {
            int lengthBits = BitWidth(2 * typeLength);
            bytes += 2 * (((long)nonNullCount * lengthBits + 7) / 8
                + DeltaOverheadBytes(nonNullCount, pages, lengthBits));
        }

        return WithMargin(bytes + CodecBytes(bytes, pages, codec));
    }

    /// <summary>
    /// The values' size as PLAIN writes them: a BYTE_ARRAY is its bytes behind a 4-byte length.
    /// </summary>
    internal static long PlainValueBytes(
        IArrowArray array, int nonNullCount, PhysicalType physicalType, int typeLength)
    {
        if (physicalType == PhysicalType.ByteArray)
        {
            var data = array.Data;
            long dataBytes;
            if (data.DataType is BinaryType or StringType)
            {
                var offsets = MemoryMarshal.Cast<byte, int>(data.Buffers[1].Span);
                dataBytes = offsets[data.Offset + data.Length] - (long)offsets[data.Offset];
            }
            else
            {
                // Large and view layouts: the average EstimateValuesPerPage uses, which reads 32-bit
                // offsets whatever the layout, so it is kept from going negative.
                dataBytes = (long)nonNullCount * Math.Max(0, EstimateAvgByteArraySize(array) - 4);
            }
            return dataBytes + 4L * nonNullCount;
        }

        int bytesPerValue = physicalType switch
        {
            PhysicalType.Boolean => 1,
            PhysicalType.Int32 or PhysicalType.Float => 4,
            PhysicalType.Int64 or PhysicalType.Double => 8,
            PhysicalType.Int96 => 12,
            PhysicalType.FixedLenByteArray => typeLength > 0 ? typeLength : 16,
            _ => 8,
        };
        return (long)nonNullCount * bytesPerValue;
    }

    /// <summary>
    /// The initial capacity of a dictionary-encoded chunk's stream: the dictionary page, and every
    /// row's index and levels at the hybrid encoding's largest.
    /// </summary>
    /// <remarks>An index is counted for every level, null or not.</remarks>
    internal static int EstimateDictionaryColumnSize(
        int dictionaryBytes, int levelCount, PageLayout pages,
        int indexBitWidth, int maxDefLevel, int maxRepLevel, CompressionCodec codec)
    {
        long bytes = PageHeaderAllowance + dictionaryBytes
            // Each data page leads its indices with their bit width.
            + pages.Count * (PageHeaderAllowance + 1) + HybridBytes(levelCount, pages, indexBitWidth)
            + LevelBytes(levelCount, pages, maxDefLevel, allDefined: false)
            + LevelBytes(levelCount, pages, maxRepLevel, allDefined: false);
        return WithMargin(bytes + CodecBytes(bytes, pages, codec));
    }

    /// <summary>How many data pages a chunk is cut into, and the most levels any one of them holds.</summary>
    internal readonly record struct PageLayout(long Count, int MaxLength);

    /// <summary>
    /// The pages the write loops will cut, found the way they cut them: <see cref="PageTargetLength"/>,
    /// then <see cref="RecordAlignedPageLength"/>.
    /// </summary>
    /// <remarks>
    /// A column that is not repeated is cut into equal pages and needs no walk. A repeated column's
    /// cut backs up to the start of the record it falls in, so its pages can be shorter than the
    /// target, and more of them; a record larger than a page gets an oversized page to itself.
    /// </remarks>
    internal static PageLayout LayOutPages(int[]? repLevels, int levelCount, int valuesPerPage, int? rowLimit)
    {
        if (repLevels is null)
        {
            int length = Math.Max(1, Math.Min(valuesPerPage, rowLimit ?? int.MaxValue));
            return new PageLayout(
                Math.Max(1, (levelCount + (long)length - 1) / length),
                Math.Max(1, Math.Min(length, levelCount)));
        }

        long count = 0;
        int maxLength = 1;
        for (int offset = 0; offset < levelCount;)
        {
            int length = RecordAlignedPageLength(
                repLevels, offset,
                PageTargetLength(repLevels, offset, valuesPerPage, levelCount, rowLimit),
                levelCount);
            count++;
            maxLength = Math.Max(maxLength, length);
            offset += length;
        }
        return new PageLayout(Math.Max(1, count), maxLength);
    }

    /// <summary>
    /// What the pages' DELTA_BINARY_PACKED streams add to <paramref name="valueCount"/> values at
    /// <paramref name="bitWidth"/>. The values themselves are the caller's to count.
    /// </summary>
    /// <remarks>
    /// Per page: a header of block size, miniblock count, value count and first value; per 128
    /// deltas, a min-delta varint and four bit widths; and the last miniblock padded to 32 deltas,
    /// up to 31 values of padding. A page of one value has no deltas, so none of the last two.
    /// </remarks>
    private static long DeltaOverheadBytes(long valueCount, PageLayout pages, int bitWidth)
    {
        int varintBytes = (bitWidth + 7) / 7; // a zigzag value of bitWidth bits
        long header = 2 + 1 + VarintLength((uint)pages.MaxLength) + varintBytes;
        if (pages.MaxLength < 2)
            return pages.Count * header;

        // Blocks: at most valueCount / 128 across the chunk, plus a partial one per page.
        long blockBytes = varintBytes + 4;
        return valueCount / 128 * blockBytes
            + pages.Count * (header + blockBytes + 31L * bitWidth / 8);
    }

    /// <summary>
    /// The pages' levels at the hybrid encoding's largest, behind the 4-byte length a V1 page gives
    /// them.
    /// </summary>
    /// <remarks>
    /// When every level is the maximum, a page's levels are one run: a header and one value.
    /// </remarks>
    private static long LevelBytes(int levelCount, PageLayout pages, int maxLevel, bool allDefined)
    {
        if (maxLevel == 0)
            return 0;
        int bitWidth = BitWidth(maxLevel);
        return allDefined
            ? pages.Count * (4 + 5 + (bitWidth + 7) / 8)
            : pages.Count * 4 + HybridBytes(levelCount, pages, bitWidth);
    }

    /// <summary>
    /// The most bytes the RLE/bit-packed hybrid spends on <paramref name="count"/> values cut into
    /// <paramref name="pages"/>.
    /// </summary>
    /// <remarks>
    /// Bounded as if <see cref="ParquetWriteOptions.BatchBitPackedRuns"/> were off, where every 8-value
    /// bit-packed group carries its own 1-byte header: a bit per value over the width. Batched, a literal
    /// run of g groups costs g bytes per width and at most g headers, so no more. That holds for the
    /// width-1 packed path too, though there a stream can outgrow its unbatched encoding: its RLE runs
    /// cover 8 or more values and cost 2 bytes under 64 values and 3 under 8,192, never more than the
    /// 2 bytes per 8 values allowed here, and the header that resumes literals after one belongs to that
    /// literal run. A run of 8 or more repeats, a 1-byte header and one value, is never more. Each page
    /// can end on a partial group.
    /// </remarks>
    private static long HybridBytes(long count, PageLayout pages, int bitWidth) =>
        bitWidth == 0 ? 0 : (count / 8 + pages.Count) * (1 + bitWidth);

    private static int VarintLength(uint value)
    {
        int length = 1;
        while (value >= 0x80)
        {
            value >>= 7;
            length++;
        }
        return length;
    }

    /// <summary>
    /// What <paramref name="codec"/> adds to <paramref name="bytes"/> of incompressible pages: framing
    /// on each page, which a page of a few bytes cannot cover in proportion, and for Gzip a share of
    /// the bytes.
    /// </summary>
    /// <remarks>
    /// The framing is what each codec was measured to add to a 2-byte page, with room: Gzip 20 bytes
    /// of header and trailer, Zstd 8, Brotli 3, Snappy 1, LZ4 none. zlib-ng, which .NET's GZipStream
    /// uses from .NET 9 on, writes incompressible input at Fastest as static-Huffman literals of up to
    /// 9 bits: up to 1/8 more, and 5.5% more for uniformly random bytes. Other codecs' expansion of
    /// large incompressible input is within the 1/64 that <see cref="WithMargin"/> adds.
    /// </remarks>
    private static long CodecBytes(long bytes, PageLayout pages, CompressionCodec codec) => codec switch
    {
        CompressionCodec.Uncompressed => 0,
        CompressionCodec.Snappy or CompressionCodec.Lz4 or CompressionCodec.Brotli => pages.Count * 8,
        CompressionCodec.Zstd => pages.Count * 16,
        CompressionCodec.Gzip or CompressionCodec.Deflate => pages.Count * 24 + bytes / 8,
        _ => pages.Count * 32,
    };

    // The 1/64 covers the rest of codec framing. Capped so a huge chunk over-estimated (booleans,
    // nulls) cannot reserve gigabytes up front.
    private static int WithMargin(long bytes) =>
        (int)Math.Min(bytes + bytes / 64 + 256, 256L * 1024 * 1024);

    /// <summary>
    /// Drops the DEPRECATED <c>min</c>/<c>max</c> fields on the column types where signed byte ordering
    /// is not the logical ordering.
    /// </summary>
    /// <remarks>
    /// Those fields are defined with SIGNED byte ordering. Our values are computed with the CORRECT
    /// logical ordering (they mirror min_value/max_value), so a legacy reader that honors the deprecated
    /// fields under signed semantics would mis-prune UTF-8 / unsigned / decimal-FLBA columns. parquet-mr's
    /// rule: emit the deprecated fields only where signed ordering IS the logical ordering (booleans,
    /// signed ints incl. date/time/timestamp, floats); drop them elsewhere.
    /// </remarks>
    private static Statistics DropDeprecatedMinMaxIfMisordered(
        Statistics stats, Apache.Arrow.Types.IArrowType type, PhysicalType physicalType)
    {
        if (SignedOrderMatchesLogical(type, physicalType))
            return stats;

        return new Statistics
        {
            NullCount = stats.NullCount,
            MinValue = stats.MinValue,
            MaxValue = stats.MaxValue,
            IsMinValueExact = stats.IsMinValueExact,
            IsMaxValueExact = stats.IsMaxValueExact,
            NanCount = stats.NanCount,
            DistinctCount = stats.DistinctCount,
        };
    }

    // True when the type's SIGNED byte ordering equals its logical ordering — the precondition for emitting
    // the deprecated Statistics.min/max fields (see DropDeprecatedMinMaxIfMisordered).
    //
    // The real precondition is narrower than "this Arrow type is signed": it is that StatisticsCollector
    // compared these values with a TYPED comparator. It does that only for BOOLEAN/INT32/INT64/FLOAT/DOUBLE;
    // BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY columns are compared as bytes (see StatisticsOrder), which is
    // not the signed order the deprecated fields promise. So the physical type has to be part of the
    // answer wherever an Arrow type can arrive on more than one physical width.
    // Internal rather than private so the gate can be pinned directly. The FIXED_LEN_BYTE_ARRAY answer is
    // latent until an Arrow TimestampType can map to that physical type, so there is no end-to-end write
    // that reaches it yet — a unit test is the only thing that keeps the fix from silently regressing.
    internal static bool SignedOrderMatchesLogical(
        Apache.Arrow.Types.IArrowType type, PhysicalType physicalType) => type switch
    {
        BooleanType => true,
        Int8Type or Int16Type or Int32Type or Int64Type => true,
        FloatType or DoubleType => true,
        // FLOAT16 is FLBA(2): its bounds are ordered numerically (StatisticsOrder.Float16), which no
        // signed comparison of the two bytes reproduces, so it gets only min_value/max_value.
        HalfFloatType => false,
        // Date/Time/Duration only ever arrive on INT32/INT64, so the collector used a typed comparator.
        Date32Type or Date64Type or Time32Type or Time64Type or DurationType => true,
        // TIMESTAMP is the one that can also arrive on FIXED_LEN_BYTE_ARRAY. There the collector compares
        // bytes unsigned-lexicographically, which is not the signed order these deprecated fields promise —
        // and a wrong bound in the footer is a wrong prune, not a cosmetic defect.
        TimestampType => physicalType == PhysicalType.Int64,
        Decimal32Type or Decimal64Type => true, // INT32/INT64 physical — signed numeric ordering
        _ => false, // UTF-8 strings/binary (unsigned lexical), unsigned ints, decimal FLBA, nested, ...
    };

    /// <summary>
    /// Fills a run-end encoded column's definition levels, a run at a time, and returns its non-null row
    /// count.
    ///
    /// <para>The nulls come from each run's VALUE. A run-end encoded array has no validity bitmap of its
    /// own, so <c>array.IsNull(row)</c> — what the per-row loop this replaces would call — answers false
    /// for every row of a column that is entirely null, which would write null rows as whatever bytes sat
    /// in the value slot.</para>
    /// </summary>
    private static int FillRunEndEncodedDefLevels(RunEndEncodedArray array, int[] defLevels)
    {
        var values = array.Values;
        int row = 0;
        int nonNullCount = 0;

        foreach (var run in RunEndEncoding.EnumerateRuns(array))
        {
            if (!values.IsNull(run.PhysicalIndex))
            {
                defLevels.AsSpan(row, run.Length).Fill(1);
                nonNullCount += run.Length;
            }

            row += run.Length;
        }

        return nonNullCount;
    }

    /// <summary>
    /// The type whose values a column holds — its own, or for a run-end encoded column the type of its
    /// values child, since that is what lands in the file.
    /// </summary>
    private static IArrowType ValueType(IArrowArray array) =>
        array.Data.DataType is RunEndEncodedType ree ? ree.ValuesDataType : array.Data.DataType;

    /// <summary>
    /// Applies a whole-column transform where it belongs: to the values child of a run-end encoded column
    /// (one value per run — the transform never sees the rows), or to a plain column directly. Requires a
    /// compacted array, so the runs the values pair with are all of them.
    /// </summary>
    private static IArrowArray MapValues(IArrowArray array, Func<IArrowArray, IArrowArray> transform) =>
        array is RunEndEncodedArray ree
            ? new RunEndEncodedArray(ree.RunEnds, transform(ree.Values))
            : transform(array);

    /// <summary>A filter sized for <paramref name="distinctValues"/> under the options' FPP and size cap.</summary>
    private static SplitBlockBloomFilterBuilder NewBloomFilterBuilder(long distinctValues, ParquetWriteOptions options) =>
        new(SplitBlockBloomFilterBuilder.OptimalNumBytes(
            (int)Math.Min(int.MaxValue, distinctValues), options.BloomFilterFpp, options.BloomFilterMaxBytes));

    /// <summary>
    /// The Bloom filter of a dictionary-encoded chunk, sized exactly (the dictionary holds the distinct
    /// count) and filled by hashing each dictionary entry once.
    /// </summary>
    private static SplitBlockBloomFilterBuilder DictionaryBloomFilter(
        byte[] dictionaryPage, int dictionaryCount, PhysicalType physicalType, int typeLength,
        ParquetWriteOptions options)
    {
        var builder = NewBloomFilterBuilder(dictionaryCount, options);
        BloomFilterArrowEncoder.AddDictionaryValues(builder, dictionaryPage, dictionaryCount, physicalType, typeLength);
        return builder;
    }

    /// <summary>
    /// For nested columns (maxDefLevel &gt; 1), normalizes def levels to 0/1
    /// so value encoding methods can check != 0 for presence.
    /// Returns null if defLevels is null. Returns defLevels unchanged if maxDefLevel &lt;= 1.
    /// </summary>
    private static int[]? NormalizeDefLevels(int[]? defLevels, int maxDefLevel)
    {
        if (defLevels == null)
            return null;
        if (maxDefLevel == 0)
            return null; // fully required column — no nulls possible
        if (maxDefLevel == 1)
            return defLevels; // already 0/1

        // Multi-level: normalize to 0 (absent) / 1 (present)
        var normalized = new int[defLevels.Length];
        for (int i = 0; i < defLevels.Length; i++)
            normalized[i] = defLevels[i] >= maxDefLevel ? 1 : 0;
        return normalized;
    }

    /// <summary>Minimum bit width to represent values 0..maxValue.</summary>
    private static int BitWidth(int maxValue) =>
#if NET8_0_OR_GREATER
        maxValue <= 0 ? 0 : (32 - int.LeadingZeroCount(maxValue));
#else
        maxValue <= 0 ? 0 : (32 - BitPolyfills.LeadingZeroCount((uint)maxValue));
#endif

    /// <summary>
    /// Counts the number of rows in a page by counting entries where repLevel &lt; maxRepLevel
    /// (i.e., rep level 0 starts a new row at the top level).
    /// </summary>
    private static int CountRows(int[]? repLevels, int offset, int numValues, int maxRepLevel)
    {
        if (repLevels == null) return numValues;
        int rows = 0;
        for (int i = offset; i < offset + numValues; i++)
        {
            if (repLevels[i] == 0)
                rows++;
        }
        return rows;
    }
}
