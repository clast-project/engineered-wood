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
    private byte[]? _table;
    private byte _nullAnswer;

    // The table as two bits per entry, for Map's inner loop: TRUE, and known (not NULL). And whether any entry, or the
    // NULL row, answers NULL, which is what decides whether the answers need a validity bitmap at all.
    private byte[] _trueBits = [];
    private byte[] _knownBits = [];
    private bool _anyNull;

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

    /// <summary>Whether <see cref="Prepare"/> has built the lookup table.</summary>
    internal bool IsPrepared => _table is not null;

    /// <summary>The field of a column of answers named <paramref name="name"/>.</summary>
    internal static Field AnswerField(string name) => new(name, BooleanType.Default, nullable: true, AnswerMetadata);

    /// <summary>Whether <paramref name="field"/> is a column of answers rather than the column's values.</summary>
    internal static bool IsAnswerField(Field field) =>
        field.HasMetadata && field.Metadata.TryGetValue(AnswerKey, out var value) && value == "1";

    /// <summary>A build state collecting a chunk's indices.</summary>
    internal static ColumnBuildState IndexState(ColumnDescriptor column, int capacity) =>
        new(PhysicalType.Int32, column.MaxDefinitionLevel, maxRepLevel: 0, capacity, columnPath: column.DottedPath)
        {
            DictionaryIndices = true,
        };

    /// <summary>
    /// Evaluates every predicate over <paramref name="dictionary"/>'s values, and over one NULL when the column is
    /// nullable, and keeps their Kleene AND per entry. False, leaving the chunk to be read as values, when there is no
    /// dictionary, when it holds at least as many entries as the rows to answer, or when a predicate raises or returns
    /// a mask of the wrong length.
    /// </summary>
    internal bool Prepare(DictionaryDecoder? dictionary)
    {
        _table = null;
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

        try
        {
            // Every entry starts TRUE; each predicate's answer is ANDed in (Kleene: FALSE wins, then NULL).
            var answers = new byte[rows];
            answers.AsSpan().Fill(True);
            foreach (var predicate in _predicates)
            {
                BooleanArray mask;
                try
                {
                    mask = predicate.Evaluate(batch);
                }
                catch (Exception ex) when (ex is not (OutOfMemoryException or OperationCanceledException))
                {
                    // An entry no row may use can raise (a cast of a value since deleted): the rows decide.
                    return false;
                }

                if (mask is null)
                    return false;
                try
                {
                    if (mask.Length != rows)
                        return false;
                    for (int i = 0; i < rows; i++)
                        answers[i] = And(answers[i], AnswerOf(mask, i));
                }
                finally
                {
                    BatchSelector.Release(mask, batch);
                }
            }

            _table = answers.AsSpan(0, entries).ToArray();
            _nullAnswer = nullable ? answers[entries] : Null;
            _trueBits = new byte[entries];
            _knownBits = new byte[entries];
            _anyNull = false;
            for (int i = 0; i < entries; i++)
            {
                _trueBits[i] = _table[i] == True ? (byte)1 : (byte)0;
                _knownBits[i] = _table[i] == Null ? (byte)0 : (byte)1;
                _anyNull |= _table[i] == Null;
            }

            return true;
        }
        finally
        {
            batch.Dispose();
        }
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
        if (_table is null)
            throw new InvalidOperationException("The dictionary has not been evaluated.");

        bool nullRows = denseIndices.Length != rows;
        if (nullRows && definitionLevels.Length < rows)
            throw new InvalidOperationException($"{denseIndices.Length} values for {rows} rows, with no definition levels.");

        int bytes = (rows + 7) / 8;
        var values = new byte[bytes];
        byte nullTrue = _nullAnswer == True ? (byte)1 : (byte)0;
        byte nullKnown = _nullAnswer == Null ? (byte)0 : (byte)1;
        bool needValidity = _anyNull || (nullRows && nullKnown == 0);
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

    // Kleene AND of two answers.
    private static byte And(byte a, byte b) => a == False || b == False ? False : a == Null || b == Null ? Null : True;

    private static byte AnswerOf(BooleanArray mask, int i) => mask.GetValue(i) switch
    {
        null => Null,
        true => True,
        false => False,
    };

    private static ByteArrayOutputKind ByteArrayOutputOf(IArrowType type) => type switch
    {
        StringViewType or BinaryViewType => ByteArrayOutputKind.ViewType,
        LargeStringType or LargeBinaryType => ByteArrayOutputKind.LargeOffsets,
        _ => ByteArrayOutputKind.Default,
    };
}
