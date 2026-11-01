// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake.Table;

/// <summary>What a <see cref="MergeAction"/> does to the row its clause applies to.</summary>
public enum MergeActionKind
{
    /// <summary>Replace the matched target row's columns with the source row's (<c>UPDATE SET</c>).</summary>
    Update,

    /// <summary>Delete the target row (<c>DELETE</c>).</summary>
    Delete,

    /// <summary>Insert the source row (<c>INSERT *</c>).</summary>
    Insert,
}

/// <summary>
/// The action a clause of a MERGE takes: see <see cref="MergeOptions"/>. Built from the static members, which
/// correspond to Spark's <c>UPDATE SET *</c>, <c>UPDATE SET c = source.c, ...</c>, <c>DELETE</c> and
/// <c>INSERT *</c>.
/// </summary>
public sealed class MergeAction
{
    private MergeAction(MergeActionKind kind, IReadOnlyList<string>? columns)
    {
        Kind = kind;
        Columns = columns;
    }

    /// <summary>What the action does.</summary>
    public MergeActionKind Kind { get; }

    /// <summary>For an update, the columns it sets from the source row, or null for every column the source
    /// carries (<c>UPDATE SET *</c>). Null for the other kinds.</summary>
    public IReadOnlyList<string>? Columns { get; }

    /// <summary>
    /// Sets every column from the source row (<c>UPDATE SET *</c>). As in Spark, the source must carry every
    /// column of the table; <see cref="UpdateColumns"/> sets fewer.
    /// </summary>
    public static MergeAction Update { get; } = new(MergeActionKind.Update, null);

    /// <summary>Sets only <paramref name="columns"/> from the source row
    /// (<c>UPDATE SET c = source.c, ...</c>); the others keep the target row's values. Each must be a table
    /// column the source carries.</summary>
    public static MergeAction UpdateColumns(params string[] columns) =>
        new(MergeActionKind.Update, CheckColumns(columns, "An update must set"));

    /// <summary>Deletes the target row.</summary>
    public static MergeAction Delete { get; } = new(MergeActionKind.Delete, null);

    /// <summary>
    /// Inserts the source row (<c>INSERT *</c>). As in Spark, the source must carry every column of the table;
    /// <see cref="InsertColumns"/> inserts fewer.
    /// </summary>
    public static MergeAction Insert { get; } = new(MergeActionKind.Insert, null);

    /// <summary>
    /// Inserts only <paramref name="columns"/> of the source row (<c>INSERT (c, ...) VALUES (source.c, ...)</c>).
    /// The other columns are written as an append writes a column it omits: NULL, or the generated or identity
    /// value. Each must be a table column the source carries.
    /// </summary>
    public static MergeAction InsertColumns(params string[] columns) =>
        new(MergeActionKind.Insert, CheckColumns(columns, "An insert must write"));

    private static string[] CheckColumns(string[] columns, string what)
    {
        if (columns is null)
            throw new ArgumentNullException(nameof(columns));
        if (columns.Length == 0)
            throw new ArgumentException($"{what} at least one column.", nameof(columns));
        foreach (string column in columns)
        {
            if (string.IsNullOrEmpty(column))
                throw new ArgumentException("A column name cannot be null or empty.", nameof(columns));
        }
        if (columns.Distinct(StringComparer.Ordinal).Count() != columns.Length)
            throw new ArgumentException("A column is named twice.", nameof(columns));
        return columns.ToArray();
    }

    /// <inheritdoc/>
    public override string ToString() =>
        Columns is null ? Kind.ToString() : $"{Kind}({string.Join(", ", Columns)})";
}

/// <summary>
/// A key-based MERGE: <c>MERGE INTO target USING source ON target.k1 = source.k1 AND ...</c> followed by at most
/// one clause of each kind. See <see cref="DeltaTable.MergeAsync"/>.
/// </summary>
/// <remarks>
/// <para><b>Conditions.</b> Each clause may carry a condition. A condition names the target row's columns as
/// <c>target.c</c> and the source row's as <c>source.c</c> (references with those two parts, as
/// <see cref="Expressions.UnboundReference.FromParts"/> builds them or the SQL parser parses <c>source.c</c>).
/// A column name only one side has may also be written bare. The <see cref="WhenNotMatchedCondition"/> sees only
/// the source row and the <see cref="WhenNotMatchedBySourceCondition"/> only the target row. As in SQL, a row is
/// acted on only when its condition is TRUE: NULL is not.</para>
///
/// <para><b>Matching.</b> A target row matches a source row when every key is equal on both, and a NULL key
/// matches nothing (SQL <c>=</c>, not <c>&lt;=&gt;</c>). A source row matched by any target row is never
/// inserted, whether or not <see cref="WhenMatchedCondition"/> holds for it. A target row whose keys two source
/// rows equal is an error (<c>DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE</c>), whatever the clause
/// conditions say, unless the only matched action is an unconditional delete or the MERGE only inserts. That is
/// delta-spark's rule (it counts the ON matches alone), checked against Spark 4.0 / Delta 4.0.</para>
/// </remarks>
public sealed class MergeOptions
{
    /// <summary>
    /// The columns the ON condition equates: <c>target.k = source.k</c> for each. Each must be a top-level
    /// column of the table with a primitive type, carried by the source under the same name.
    /// </summary>
    public IReadOnlyList<string> Keys { get; init; } = [];

    /// <summary>What to do to a target row a source row matches: <see cref="MergeAction.Update"/>,
    /// <see cref="MergeAction.UpdateColumns"/> or <see cref="MergeAction.Delete"/>. Null leaves matched rows
    /// alone (an insert-only MERGE).</summary>
    public MergeAction? WhenMatched { get; init; }

    /// <summary>A condition over the matched pair (<c>target.c</c> and <c>source.c</c>) that must be TRUE for
    /// <see cref="WhenMatched"/> to act. Null always acts.</summary>
    public Expressions.Predicate? WhenMatchedCondition { get; init; }

    /// <summary>What to do with a source row no target row matches: <see cref="MergeAction.Insert"/>,
    /// <see cref="MergeAction.InsertColumns"/> or null (no insert).</summary>
    public MergeAction? WhenNotMatched { get; init; }

    /// <summary>A condition over the unmatched source row that must be TRUE for it to be inserted.</summary>
    public Expressions.Predicate? WhenNotMatchedCondition { get; init; }

    /// <summary>What to do to a target row no source row matches: <see cref="MergeAction.Delete"/> or null.
    /// Setting it makes the MERGE read every file of the table, since any row may be unmatched.</summary>
    public MergeAction? WhenNotMatchedBySource { get; init; }

    /// <summary>A condition over the unmatched target row that must be TRUE for it to be deleted.</summary>
    public Expressions.Predicate? WhenNotMatchedBySourceCondition { get; init; }
}

/// <summary>
/// What a MERGE did, named as Spark names the MERGE's <c>operationMetrics</c>.
/// </summary>
/// <param name="NumSourceRows">The rows the source held.</param>
/// <param name="NumTargetRowsInserted">Source rows inserted by the not-matched clause.</param>
/// <param name="NumTargetRowsUpdated">Target rows the matched clause updated.</param>
/// <param name="NumTargetRowsMatchedDeleted">Target rows the matched clause deleted.</param>
/// <param name="NumTargetRowsNotMatchedBySourceDeleted">Target rows the not-matched-by-source clause deleted.</param>
/// <param name="NumTargetRowsCopied">Unchanged target rows rewritten because their file held a changed row.</param>
/// <param name="NumTargetFilesAdded">Data files the MERGE added: rewrites and inserts.</param>
/// <param name="NumTargetFilesRemoved">Data files the MERGE removed.</param>
public sealed record MergeMetrics(
    long NumSourceRows,
    long NumTargetRowsInserted,
    long NumTargetRowsUpdated,
    long NumTargetRowsMatchedDeleted,
    long NumTargetRowsNotMatchedBySourceDeleted,
    long NumTargetRowsCopied,
    long NumTargetFilesAdded,
    long NumTargetFilesRemoved)
{
    /// <summary>Target rows deleted by either clause.</summary>
    public long NumTargetRowsDeleted => NumTargetRowsMatchedDeleted + NumTargetRowsNotMatchedBySourceDeleted;

    /// <summary>Target rows the matched clause updated: the only clause that updates here, so
    /// <see cref="NumTargetRowsUpdated"/> under the name Spark also reports it by.</summary>
    public long NumTargetRowsMatchedUpdated => NumTargetRowsUpdated;

    /// <summary>The rows the MERGE wrote to data files: updated, copied and inserted.</summary>
    public long NumOutputRows => NumTargetRowsUpdated + NumTargetRowsCopied + NumTargetRowsInserted;
}
