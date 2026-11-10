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

    private readonly RowPredicate _predicate;
    private readonly ColumnDescriptor _column;
    private readonly Field _field;
    private readonly Func<Field, IArrowArray, (Field Field, IArrowArray Array)> _restore;
    private readonly int _rows;
    private byte[]? _table;
    private byte _nullAnswer;

    /// <param name="predicate">The predicate: value-wise, over this one column.</param>
    /// <param name="column">The column: a flat leaf.</param>
    /// <param name="field">The column's Arrow field as the read builds it.</param>
    /// <param name="restore">What the read does to a built column before it is returned (the declared time
    /// units), so the predicate sees the dictionary as it would see the rows.</param>
    /// <param name="rows">The rows the answers are for: a dictionary at least this large is not evaluated.</param>
    internal DictionaryAnswer(
        RowPredicate predicate, ColumnDescriptor column, Field field,
        Func<Field, IArrowArray, (Field Field, IArrowArray Array)> restore, int rows)
    {
        _predicate = predicate;
        _column = column;
        _field = field;
        _restore = restore;
        _rows = rows;
    }

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
    /// Evaluates the predicate over <paramref name="dictionary"/>'s values, and over one NULL when the column is
    /// nullable. False, leaving the chunk to be read as values, when there is no dictionary, when it holds at
    /// least as many entries as the rows to answer, or when the predicate raises or returns a mask of the wrong
    /// length.
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
        using (var state = new ColumnBuildState(
                   _column.PhysicalType, _column.MaxDefinitionLevel, maxRepLevel: 0, Math.Max(1, rows),
                   ByteArrayOutputOf(_field.DataType), _column.DottedPath, ExtendedTimestamp.DeclaredUnit(_column)))
        {
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

        try
        {
            BooleanArray mask;
            try
            {
                mask = _predicate.Evaluate(batch);
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

                var table = new byte[entries];
                for (int i = 0; i < entries; i++)
                    table[i] = AnswerOf(mask, i);
                _nullAnswer = nullable ? AnswerOf(mask, entries) : Null;
                _table = table;
                return true;
            }
            finally
            {
                BatchSelector.Release(mask, batch);
            }
        }
        finally
        {
            batch.Dispose();
        }
    }

    /// <summary>The answer for each row of <paramref name="indices"/> (NULL for a NULL row): a column of answers.</summary>
    internal BooleanArray Map(Int32Array indices)
    {
        var table = _table ?? throw new InvalidOperationException("The dictionary has not been evaluated.");
        int length = indices.Length;
        int bytes = (length + 7) / 8;
        var values = new byte[bytes];
        var validity = new byte[bytes];
        int nulls = 0;
        var keys = indices.Values;
        bool anyNullRow = indices.NullCount != 0;
        for (int i = 0; i < length; i++)
        {
            byte answer = anyNullRow && indices.IsNull(i) ? _nullAnswer : table[keys[i]];
            if (answer == Null)
            {
                nulls++;
                continue;
            }

            validity[i >> 3] |= (byte)(1 << (i & 7));
            if (answer == True)
                values[i >> 3] |= (byte)(1 << (i & 7));
        }

        return new BooleanArray(
            new ArrowBuffer(values), nulls == 0 ? ArrowBuffer.Empty : new ArrowBuffer(validity), length, nulls, 0);
    }

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
