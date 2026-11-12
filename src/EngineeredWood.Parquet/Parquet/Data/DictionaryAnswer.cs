// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Parquet.Metadata;
using EngineeredWood.Parquet.Schema;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// A value-wise predicate's answers for one dictionary-encoded column chunk, from evaluating it once over the
/// chunk's dictionary (#502 phase 4b, doc/row-level-filtering.md §6): the chunk is decoded to its indices only,
/// and each row's answer is its index's entry in a lookup table. The column's values are never built.
/// </summary>
/// <remarks>
/// <para>A chunk that falls back from its dictionary partway through is answered too (#804). Writers do this once
/// the dictionary passes a size limit (parquet-mr, arrow-cpp and arrow-rs all do). Each page that is not
/// dictionary-encoded has its values decoded apart, and the predicates are asked over them
/// (<see cref="AnswerValues"/>). Their answers join the lookup table as entries past the dictionary's, and the
/// page's values take those entries as their indices. So <see cref="Map"/> treats both kinds of page alike.</para>
/// <para>One instance per chunk read. <see cref="Prepare"/> evaluates the dictionary; when it declines (no
/// dictionary, one no smaller than the rows it would answer, or a predicate that raises or answers wrongly
/// over it), the chunk is read as values and the predicate asked row by row as usual. So an error raised by an
/// entry no row uses (an ANSI cast of a value since deleted) never fails a read, and one a row's value raises
/// recurs on that row.</para>
/// <para>The answers stand in for the column in the decoded batch, as a boolean column whose field
/// <see cref="IsAnswerField"/> recognizes; <see cref="BatchSelector"/> applies them as the predicate's mask. Only
/// a column no other predicate reads and the read drops is answered, so nothing else ever sees them.</para>
/// </remarks>
internal sealed class DictionaryAnswer
{
    private const byte False = 0, True = 1, Null = 2;

    /// <summary>The field metadata key marking a column of answers.</summary>
    private const string AnswerKey = "EngineeredWood.DictionaryAnswer";

    private static readonly Dictionary<string, string> AnswerMetadata = new() { [AnswerKey] = "1" };

    /// <summary>The field the indices are built as.</summary>
    internal static readonly Field IndexField = new("indices", Int32Type.Default, nullable: true);

    private readonly IReadOnlyList<RowPredicate> _predicates;
    private readonly ColumnDescriptor _column;
    private readonly Field _field;
    private readonly Func<Field, IArrowArray, (Field Field, IArrowArray Array)> _restore;
    private readonly int _rows;
    private readonly bool _keepValues;
    private bool _prepared;
    private byte _nullAnswer;

    // The table as two bits per entry, for Map's inner loop: TRUE, and known (not NULL). And whether any entry, or the
    // NULL row, answers NULL, which is what decides whether the answers need a validity bitmap at all.
    private byte[] _trueBits = [];
    private byte[] _knownBits = [];
    private bool _anyNull;

    // The dictionary's entries. Past them are the answers for the values on fallback pages decoded since the last
    // Map (#804). Map takes them, and the next batch starts afresh.
    private int _entries;
    private int _extra;
    private bool _extraAnyNull;

    /// <param name="predicates">The predicates, value-wise, over this one column: a run whose answers are their Kleene AND
    /// (see <see cref="AnswerPlan"/>).</param>
    /// <param name="column">The column: a flat leaf.</param>
    /// <param name="field">The column's Arrow field as the read builds it.</param>
    /// <param name="restore">What the read does to a built column before it is returned (the declared time
    /// units), so the predicate sees the dictionary as it would see the rows.</param>
    /// <param name="rows">The rows the answers are for: a dictionary at least this large is not evaluated.</param>
    /// <param name="keepValues">The read returns the column: its values are decoded too (#502 phase 4d).</param>
    internal DictionaryAnswer(
        IReadOnlyList<RowPredicate> predicates, ColumnDescriptor column, Field field,
        Func<Field, IArrowArray, (Field Field, IArrowArray Array)> restore, int rows, bool keepValues = false)
    {
        _predicates = predicates;
        _column = column;
        _field = field;
        _restore = restore;
        _rows = rows;
        _keepValues = keepValues;
    }

    /// <summary>
    /// Whether the read returns the column, so its values are decoded as well as its indices, and the answers come
    /// beside them, as the column <see cref="AnswerColumnName"/> names, rather than in their place.
    /// </summary>
    internal bool KeepsValues => _keepValues;

    /// <summary>
    /// The name of the column holding a returned column's answers, which the read drops. It starts with a NUL, so it
    /// is unlikely to be a real column's, but it is not reserved: NUL is legal in a Parquet field name. A caller must
    /// check it against the file's columns, as <see cref="BatchSelector"/> does, and not answer on a collision.
    /// </summary>
    internal static string AnswerColumnName(string column) => "\0answer:" + column;

    /// <summary>
    /// Whether a chunk may be answered from its dictionary: it has dictionary-encoded data pages, and possibly pages
    /// that fell back from the dictionary too (#804). With <c>encoding_stats</c>, those decide. Without them, the
    /// chunk's encodings (writers list a dictionary encoding when any page uses one) or its dictionary page offset do.
    /// A chunk that turns out to have no dictionary page declines at its first data page.
    /// </summary>
    internal static bool MayAnswer(ColumnMetaData meta)
    {
        if (meta.EncodingStats is { Count: > 0 } stats)
        {
            bool dictionaryPage = false, dictionaryData = false;
            foreach (var entry in stats)
            {
                if (entry.Count <= 0)
                    continue;
                if (entry.PageType == PageType.DictionaryPage)
                    dictionaryPage = true;
                else if (entry.PageType is PageType.DataPage or PageType.DataPageV2
                         && entry.Encoding is Encoding.RleDictionary or Encoding.PlainDictionary)
                    dictionaryData = true;
            }

            return dictionaryPage && dictionaryData;
        }

        if (meta.DictionaryPageOffset is not null)
            return true;
        foreach (var encoding in meta.Encodings)
        {
            if (encoding is Encoding.RleDictionary or Encoding.PlainDictionary)
                return true;
        }

        return false;
    }

    /// <summary>
    /// The share of a chunk's rows on pages that fell back from the dictionary, from its bytes (<paramref name="chunk"/>,
    /// the whole chunk), by walking the page headers without decoding a page. Zero, with no walk, when the chunk's
    /// <c>encoding_stats</c> prove it wholly dictionary-encoded. Null when it holds no data page, or a header does
    /// not decode or declares a payload the chunk cannot hold; the read then reports the corrupt page itself.
    /// </summary>
    internal static double? FallbackRowShare(ColumnMetaData meta, ReadOnlySpan<byte> chunk)
    {
        if (MembershipPredicateEvaluator.IsWhollyDictionaryEncoded(meta))
            return 0;

        long fallback = 0, total = 0, values = 0;
        try
        {
            // Until the chunk's values are all counted, as the read stops: bytes may follow the last data page.
            for (int offset = 0; offset < chunk.Length && values < meta.NumValues;)
            {
                var header = PageHeaderDecoder.Decode(chunk.Slice(offset), out int consumed);

                // The decoder takes any size. A negative one would step back, or not move at all, and loop forever
                // (review round 1). One past the chunk's end is as corrupt. PageReader refuses both the same way.
                int payload = offset + consumed;
                if (header.CompressedPageSize < 0 || header.CompressedPageSize > chunk.Length - payload)
                    return null;
                offset = payload + header.CompressedPageSize;

                // A flat column's V1 page holds one value, NULL or not, per row.
                var (rows, pageValues, encoding) = header switch
                {
                    { DataPageHeader: { } v1 } => (v1.NumValues, v1.NumValues, v1.Encoding),
                    { DataPageHeaderV2: { } v2 } => (v2.NumRows, v2.NumValues, v2.Encoding),
                    _ => (0, 0, Encoding.Plain),
                };
                values += pageValues;
                total += rows;
                if (!IsDictionaryEncoding(encoding))
                    fallback += rows;
            }
        }
        catch (Exception ex) when (ex is not (OutOfMemoryException or OperationCanceledException))
        {
            return null;
        }

        return total == 0 ? null : (double)fallback / total;
    }

    /// <summary>
    /// The share of a chunk's rows on pages that fell back from the dictionary, from its page map. A map read from the
    /// page headers knows each page's encoding. A map built from the OffsetIndex knows only each page's rows. Writers
    /// fall back once per chunk and never return to the dictionary, so the dictionary-encoded data pages that
    /// <c>encoding_stats</c> counts are the first ones. Null without <c>encoding_stats</c> for such a map, or for a
    /// map with no rows.
    /// </summary>
    internal static double? FallbackRowShare(ColumnMetaData meta, ColumnPageMap map)
    {
        if (MembershipPredicateEvaluator.IsWhollyDictionaryEncoded(meta))
            return 0;
        if (map.TotalRows == 0)
            return null;

        if (map.HeadersResolved)
        {
            long fallback = 0;
            foreach (var page in map.Pages)
            {
                if (!IsDictionaryEncoding(page.Encoding))
                    fallback += page.NumRows;
            }

            return (double)fallback / map.TotalRows;
        }

        if (meta.EncodingStats is not { Count: > 0 } stats)
            return null;

        // The counts come from the footer, and a corrupt one can hold counts whose sum overflows an int (review round
        // 1). The sum is a long, so the index stays in range. The footer decoder drops a list holding a negative count;
        // one is skipped here too, as MayAnswer skips it.
        long dictionaryPages = 0;
        foreach (var entry in stats)
        {
            if (entry.Count > 0 && entry.PageType is PageType.DataPage or PageType.DataPageV2
                && IsDictionaryEncoding(entry.Encoding))
            {
                dictionaryPages += entry.Count;
            }
        }

        return dictionaryPages >= map.Pages.Length
            ? 0
            : (double)(map.TotalRows - map.CumulativeRows[(int)dictionaryPages]) / map.TotalRows;
    }

    private static bool IsDictionaryEncoding(Encoding encoding) =>
        encoding is Encoding.RleDictionary or Encoding.PlainDictionary;

    /// <summary>Whether <see cref="Prepare"/> has built the lookup table.</summary>
    internal bool IsPrepared => _prepared;

    /// <summary>The field of a column of answers named <paramref name="name"/>.</summary>
    internal static Field AnswerField(string name) => new(name, BooleanType.Default, nullable: true, AnswerMetadata);

    /// <summary>Whether <paramref name="field"/> is a column of answers rather than the column's values.</summary>
    internal static bool IsAnswerField(Field field) =>
        field.HasMetadata && field.Metadata.TryGetValue(AnswerKey, out var value) && value == "1";

    /// <summary>A build state collecting a chunk's indices for <paramref name="answer"/>.</summary>
    internal static ColumnBuildState IndexState(ColumnDescriptor column, int capacity, DictionaryAnswer answer) =>
        new(PhysicalType.Int32, column.MaxDefinitionLevel, maxRepLevel: 0, capacity, columnPath: column.DottedPath)
        {
            DictionaryIndices = true,
            Answerer = answer,
        };

    /// <summary>
    /// Raised while a chunk is decoded when a page that fell back from the dictionary cannot be answered: its values
    /// cannot be built apart, or a predicate raises over them. The chunk's rows are then read as values and the
    /// predicates asked over the live ones, as when the dictionary itself declines. It is not a decode failure.
    /// </summary>
    internal sealed class DeclinedException(Exception? inner = null)
        : Exception("A page that fell back from the dictionary could not be answered.", inner);

    /// <summary>A build state for the <paramref name="count"/> non-NULL values of a page that fell back from the
    /// dictionary, which are decoded apart for <see cref="AnswerValues"/>.</summary>
    internal ColumnBuildState FallbackValueState(int count) =>
        new(_column.PhysicalType, maxDefLevel: 0, maxRepLevel: 0, Math.Max(1, count),
            ByteArrayOutputOf(_field.DataType), _column.DottedPath, ExtendedTimestamp.DeclaredUnit(_column));

    /// <summary>
    /// Answers the <paramref name="count"/> non-NULL values in <paramref name="values"/>, which were decoded from a
    /// page that fell back from the dictionary (#804), by asking every predicate over them. Their answers join the
    /// lookup table as entries of their own, and <paramref name="indices"/> receives those entries in order, so
    /// <see cref="Map"/> maps them as it maps the dictionary's. Raises <see cref="DeclinedException"/> when the values
    /// cannot be built, or when a predicate raises over them or answers wrongly; the chunk is then read as values.
    /// </summary>
    internal void AnswerValues(ColumnBuildState values, int count, Span<int> indices)
    {
        if (!_prepared)
            throw new InvalidOperationException("The dictionary has not been evaluated.");
        if (count == 0)
            return;

        RecordBatch batch;
        try
        {
            var (field, array) = _restore(_field, ArrowArrayBuilder.Build(values, _field, count));
            batch = new RecordBatch(new Apache.Arrow.Schema([field], null), [array], count);
        }
        catch (Exception ex) when (ex is not (OutOfMemoryException or OperationCanceledException))
        {
            throw new DeclinedException(ex);
        }

        // The answers go straight into the table, as entries past the dictionary's and earlier pages'.
        int first = _entries + _extra;
        EnsureEntries(first + count);
        bool answered, anyNull;
        try
        {
            answered = Evaluate(batch, count, _trueBits.AsSpan(first, count), _knownBits.AsSpan(first, count), out anyNull);
        }
        finally
        {
            batch.Dispose();
        }

        if (!answered)
            throw new DeclinedException();

        _extraAnyNull |= anyNull;
        for (int i = 0; i < count; i++)
            indices[i] = first + i;
        _extra += count;
    }

    private void EnsureEntries(int entries)
    {
        if (_trueBits.Length >= entries)
            return;

        int size = Math.Max(entries, _trueBits.Length * 2);
        System.Array.Resize(ref _trueBits, size);
        System.Array.Resize(ref _knownBits, size);
    }

    /// <summary>
    /// Evaluates every predicate over <paramref name="dictionary"/>'s values, and over one NULL when the column is
    /// nullable, and keeps their Kleene AND per entry. False, leaving the chunk to be read as values, when there is no
    /// dictionary, when it holds at least as many entries as the rows to answer, or when a predicate raises or returns
    /// a mask of the wrong length.
    /// </summary>
    internal bool Prepare(DictionaryDecoder? dictionary)
    {
        _prepared = false;
        if (dictionary is null || dictionary.Count >= _rows)
            return false;

        int entries = dictionary.Count;
        bool nullable = _column.MaxDefinitionLevel > 0;
        int rows = entries + (nullable ? 1 : 0);
        RecordBatch batch;
        try
        {
            using var state = new ColumnBuildState(
                _column.PhysicalType, _column.MaxDefinitionLevel, maxRepLevel: 0, Math.Max(1, rows),
                ByteArrayOutputOf(_field.DataType), _column.DottedPath, ExtendedTimestamp.DeclaredUnit(_column));
            if (nullable)
            {
                var levels = state.ReserveDefLevels(rows);
                levels.Slice(0, entries).Fill((byte)_column.MaxDefinitionLevel);
                levels[entries] = 0;
            }

            var indices = new int[entries];
            for (int i = 0; i < entries; i++)
                indices[i] = i;
            ColumnChunkReader.WriteDictionaryValues(_column, dictionary, indices, state);
            var (field, array) = _restore(_field, ArrowArrayBuilder.Build(state, _field, rows));
            batch = new RecordBatch(new Apache.Arrow.Schema([field], null), [array], rows);
        }
        catch (Exception ex) when (ex is not (OutOfMemoryException or OperationCanceledException))
        {
            // Building every entry can fail where building only the rows' would not (review round 1): an INT96
            // entry out of nanosecond range that no decoded row uses. The rows decide, as for a predicate's error.
            return false;
        }

        var trueBits = new byte[rows];
        var knownBits = new byte[rows];
        bool answered;
        try
        {
            answered = Evaluate(batch, rows, trueBits, knownBits, out _);
        }
        finally
        {
            batch.Dispose();
        }

        if (!answered)
            return false;

        // The NULL row's answer apart. Past the entries, the table's slots are free for fallback pages' values.
        _nullAnswer = !nullable ? Null
            : knownBits[entries] == 0 ? Null
            : trueBits[entries] == 1 ? True
            : False;
        _entries = entries;
        _extra = 0;
        _extraAnyNull = false;
        _trueBits = trueBits;
        _knownBits = knownBits;
        _anyNull = knownBits.AsSpan(0, entries).IndexOf((byte)0) >= 0;
        _prepared = true;
        return true;
    }

    /// <summary>
    /// Every predicate's answers over the <paramref name="rows"/> rows of <paramref name="batch"/>, combined per row by
    /// Kleene AND: FALSE if any answers FALSE, else NULL if any answers NULL, else TRUE. Each row's answer is written as
    /// two bits, one byte each: <paramref name="trueBits"/> (TRUE) and <paramref name="knownBits"/> (not NULL), the
    /// form <see cref="Map"/> reads. False when a predicate raises or returns a mask of the wrong length; the rows then
    /// decide.
    /// </summary>
    private bool Evaluate(RecordBatch batch, int rows, Span<byte> trueBits, Span<byte> knownBits, out bool anyNull)
    {
        anyNull = false;

        // Every row starts TRUE. Each predicate's answer is ANDed in: the row stays TRUE while every answer is TRUE,
        // and is known while every answer is known, or once any is FALSE (Kleene: FALSE wins, then NULL).
        trueBits.Fill(1);
        knownBits.Fill(1);
        byte[]? falseSeen = _predicates.Count > 1 ? new byte[rows] : null;
        foreach (var predicate in _predicates)
        {
            BooleanArray mask;
            try
            {
                mask = predicate.Evaluate(batch);
            }
            catch (Exception ex) when (ex is not (OutOfMemoryException or OperationCanceledException))
            {
                // A value no live row holds can raise (a cast of a value since deleted): the rows decide.
                return false;
            }

            if (mask is null)
                return false;
            try
            {
                if (mask.Length != rows)
                    return false;

                // Straight from the mask's bitmaps: a fallback page puts every value through here (#804).
                var values = mask.ValueBuffer.Span;
                var validity = mask.NullCount > 0 ? mask.NullBitmapBuffer.Span : default;
                int offset = mask.Offset;
                for (int i = 0; i < rows; i++)
                {
                    int bit = offset + i;
                    int known = validity.IsEmpty ? 1 : (validity[bit >> 3] >> (bit & 7)) & 1;
                    int isTrue = (values[bit >> 3] >> (bit & 7)) & known & 1; // a NULL's value bit means nothing
                    trueBits[i] &= (byte)isTrue;
                    knownBits[i] &= (byte)known;
                    if (falseSeen is not null)
                        falseSeen[i] |= (byte)(known & (isTrue ^ 1));
                }
            }
            finally
            {
                BatchSelector.Release(mask, batch);
            }
        }

        // A FALSE from any predicate decides the row, whatever another answered NULL.
        if (falseSeen is not null)
        {
            for (int i = 0; i < rows; i++)
                knownBits[i] |= falseSeen[i];
        }

        anyNull = knownBits.IndexOf((byte)0) >= 0;
        return true;
    }

    /// <summary>
    /// The answer for each of <paramref name="rows"/> rows, from the dictionary indices of its non-NULL values
    /// (<paramref name="denseIndices"/>, in row order) and, for a nullable column, every row's definition level: a row
    /// below <paramref name="maxDefinitionLevel"/> is NULL and gets the NULL row's answer. One pass, eight rows to a
    /// byte, without spreading the indices into an array of their own first (#502 phase 5d).
    /// </summary>
    internal BooleanArray Map(
        ReadOnlySpan<int> denseIndices, ReadOnlySpan<byte> definitionLevels, int maxDefinitionLevel, int rows)
    {
        if (!_prepared)
            throw new InvalidOperationException("The dictionary has not been evaluated.");

        bool nullRows = denseIndices.Length != rows;
        if (nullRows && definitionLevels.Length < rows)
            throw new InvalidOperationException($"{denseIndices.Length} values for {rows} rows, with no definition levels.");

        int bytes = (rows + 7) / 8;
        var values = new byte[bytes];
        byte nullTrue = _nullAnswer == True ? (byte)1 : (byte)0;
        byte nullKnown = _nullAnswer == Null ? (byte)0 : (byte)1;
        bool needValidity = _anyNull || _extraAnyNull || (nullRows && nullKnown == 0);
        var validity = needValidity ? new byte[bytes] : null;
        var trueBits = _trueBits;
        var knownBits = _knownBits;
        int known = 0;

        if (!nullRows)
        {
            // Every row has a value: its answer is its entry's.
            for (int b = 0, row = 0; b < bytes; b++)
            {
                int end = Math.Min(8, rows - row);
                int v = 0, k = 0;
                for (int bit = 0; bit < end; bit++, row++)
                {
                    int index = denseIndices[row];
                    v |= trueBits[index] << bit;
                    k |= knownBits[index] << bit;
                }

                values[b] = (byte)v;
                if (validity is not null)
                {
                    validity[b] = (byte)k;
                    known += BitsSet(k);
                }
            }
        }
        else
        {
            for (int b = 0, row = 0, next = 0; b < bytes; b++)
            {
                int end = Math.Min(8, rows - row);
                int v = 0, k = 0;
                for (int bit = 0; bit < end; bit++, row++)
                {
                    if (definitionLevels[row] == maxDefinitionLevel)
                    {
                        int index = denseIndices[next++];
                        v |= trueBits[index] << bit;
                        k |= knownBits[index] << bit;
                    }
                    else
                    {
                        v |= nullTrue << bit;
                        k |= nullKnown << bit;
                    }
                }

                values[b] = (byte)v;
                if (validity is not null)
                {
                    validity[b] = (byte)k;
                    known += BitsSet(k);
                }
            }
        }

        // The fallback pages' entries were for these rows only (#804).
        _extra = 0;
        _extraAnyNull = false;

        int nulls = validity is null ? 0 : rows - known;
        return new BooleanArray(
            new ArrowBuffer(values), nulls == 0 ? ArrowBuffer.Empty : new ArrowBuffer(validity!), rows, nulls, 0);
    }

    // The set bits of one byte's worth of answers.
    private static int BitsSet(int b)
    {
#if NET
        return System.Numerics.BitOperations.PopCount((uint)b);
#else
        // netstandard2.0's BitOperations (from a polyfill package) has no PopCount: the SWAR count.
        b -= (b >> 1) & 0x55;
        b = (b & 0x33) + ((b >> 2) & 0x33);
        return (b + (b >> 4)) & 0x0F;
#endif
    }

    private static ByteArrayOutputKind ByteArrayOutputOf(IArrowType type) => type switch
    {
        StringViewType or BinaryViewType => ByteArrayOutputKind.ViewType,
        LargeStringType or LargeBinaryType => ByteArrayOutputKind.LargeOffsets,
        _ => ByteArrayOutputKind.Default,
    };
}
