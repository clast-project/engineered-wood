// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake.Table;

/// <summary>What a <see cref="MergeAction"/> does to the row its clause applies to.</summary>
public enum MergeActionKind
{
    /// <summary>Change the target row's columns (<c>UPDATE SET</c>).</summary>
    Update,

    /// <summary>Delete the target row (<c>DELETE</c>).</summary>
    Delete,

    /// <summary>Insert a row built from the source row (<c>INSERT</c>).</summary>
    Insert,
}

/// <summary>
/// One assignment of an <see cref="MergeAction.UpdateSet"/> or <see cref="MergeAction.InsertValues"/>: the table
/// column <see cref="Column"/> gets the value of <see cref="Value"/>.
/// </summary>
/// <remarks>
/// <para>The value names the target row's columns as <c>target.c</c> and the source row's as <c>source.c</c>,
/// as a clause condition does (see <see cref="MergeOptions"/>). An insert sees only the source row, and a
/// not-matched-by-source update only the target row. Every assignment of an action reads the row as it was
/// before the action: <c>SET a = b, b = a</c> swaps them.</para>
/// <para>The value is converted to the column's type as Spark's MERGE converts it, with an ANSI <c>CAST</c>:
/// <c>'42'</c> into an <c>int</c> column is 42, <c>'abc'</c> is an error (<c>CAST_INVALID_INPUT</c>), and a value
/// the column cannot hold is <c>DELTA_CAST_OVERFLOW_IN_TABLE_WRITE</c>. A <c>timestamp_ntz</c>, struct, array or
/// map column takes only a value already of its type.</para>
/// </remarks>
public sealed class MergeAssignment
{
    /// <summary>Assigns <paramref name="value"/> to the table column <paramref name="column"/>.</summary>
    public MergeAssignment(string column, Expressions.Expression value)
    {
        if (string.IsNullOrEmpty(column))
            throw new ArgumentException("A column name cannot be null or empty.", nameof(column));
        Column = column;
        Value = value ?? throw new ArgumentNullException(nameof(value));
    }

    /// <summary>The table column assigned: a top-level column, by name, compared ignoring case as Spark does.</summary>
    public string Column { get; }

    /// <summary>The value assigned to it.</summary>
    public Expressions.Expression Value { get; }

    /// <summary>Lets an assignment be written as a tuple: <c>("count", expression)</c>.</summary>
    public static implicit operator MergeAssignment((string Column, Expressions.Expression Value) assignment) =>
        new(assignment.Column, assignment.Value);

    /// <inheritdoc/>
    public override string ToString() => $"{Column} = {Value}";
}

/// <summary>
/// A clause of a MERGE: the action it takes and an optional condition (<see cref="When"/>); see
/// <see cref="MergeOptions"/>. Built from the static members, which correspond to Spark's <c>UPDATE SET *</c>,
/// <c>UPDATE SET c = source.c, ...</c>, <c>UPDATE SET c = expression, ...</c>, <c>DELETE</c>, <c>INSERT *</c>,
/// <c>INSERT (c, ...) VALUES (source.c, ...)</c> and <c>INSERT (c, ...) VALUES (expression, ...)</c>.
/// </summary>
public sealed class MergeAction
{
    private MergeAction(
        MergeActionKind kind, IReadOnlyList<string>? columns, IReadOnlyList<MergeAssignment>? assignments,
        Expressions.Predicate? condition)
    {
        Kind = kind;
        Columns = columns;
        Assignments = assignments;
        Condition = condition;
    }

    /// <summary>What the action does.</summary>
    public MergeActionKind Kind { get; }

    /// <summary>For an update or insert that copies source columns, the columns it copies, or null for every column
    /// of the table (<c>*</c>). Null for a delete and for an action with <see cref="Assignments"/>.</summary>
    public IReadOnlyList<string>? Columns { get; }

    /// <summary>For an <see cref="UpdateSet"/> or <see cref="InsertValues"/>, its assignments, in order; null
    /// otherwise.</summary>
    public IReadOnlyList<MergeAssignment>? Assignments { get; }

    /// <summary>The condition that must be TRUE for this clause to apply, or null for an unconditional clause.
    /// Set with <see cref="When"/>.</summary>
    public Expressions.Predicate? Condition { get; }

    /// <summary>
    /// This action, applied only to the rows for which <paramref name="condition"/> is TRUE
    /// (<c>WHEN MATCHED AND condition THEN ...</c>). A row it does not apply to goes on to the next clause of the
    /// same kind.
    /// </summary>
    /// <exception cref="InvalidOperationException">The action already has a condition.</exception>
    public MergeAction When(Expressions.Predicate condition)
    {
        if (condition is null)
            throw new ArgumentNullException(nameof(condition));
        if (Condition is not null)
        {
            throw new InvalidOperationException(
                $"This MERGE action already has the condition {Condition}; combine the two with AND instead.");
        }
        return new MergeAction(Kind, Columns, Assignments, condition);
    }

    /// <summary>
    /// Sets every column from the source row (<c>UPDATE SET *</c>). As in Spark, the source must carry every
    /// column of the table; <see cref="UpdateColumns"/> sets fewer.
    /// </summary>
    public static MergeAction Update { get; } = new(MergeActionKind.Update, null, null, null);

    /// <summary>Sets only <paramref name="columns"/> from the source row
    /// (<c>UPDATE SET c = source.c, ...</c>); the others keep the target row's values. Each must be a table
    /// column the source carries.</summary>
    public static MergeAction UpdateColumns(params string[] columns) =>
        new(MergeActionKind.Update, CheckColumns(columns, "An update must set"), null, null);

    /// <summary>
    /// Sets each assignment's column to its value (<c>UPDATE SET c = expression, ...</c>); the other columns keep
    /// the target row's values. A matched update's values may read both rows, a not-matched-by-source update's only
    /// the target row. See <see cref="MergeAssignment"/>.
    /// </summary>
    public static MergeAction UpdateSet(params MergeAssignment[] assignments) =>
        new(MergeActionKind.Update, null, CheckAssignments(assignments, "An update must set",
            "DELTA_CONFLICT_SET_COLUMN: the update sets"), null);

    /// <summary>Deletes the target row.</summary>
    public static MergeAction Delete { get; } = new(MergeActionKind.Delete, null, null, null);

    /// <summary>
    /// Inserts the source row (<c>INSERT *</c>). As in Spark, the source must carry every column of the table;
    /// <see cref="InsertColumns"/> inserts fewer.
    /// </summary>
    public static MergeAction Insert { get; } = new(MergeActionKind.Insert, null, null, null);

    /// <summary>
    /// Inserts only <paramref name="columns"/> of the source row (<c>INSERT (c, ...) VALUES (source.c, ...)</c>).
    /// The other columns are written as an append writes a column it omits: NULL, or the generated or identity
    /// value. Each must be a table column the source carries.
    /// </summary>
    public static MergeAction InsertColumns(params string[] columns) =>
        new(MergeActionKind.Insert, CheckColumns(columns, "An insert must write"), null, null);

    /// <summary>
    /// Inserts a row whose columns are the assignments' values (<c>INSERT (c, ...) VALUES (expression, ...)</c>),
    /// which may read only the source row. The other columns are written as an append writes a column it omits:
    /// NULL, or the generated or identity value. See <see cref="MergeAssignment"/>.
    /// </summary>
    public static MergeAction InsertValues(params MergeAssignment[] assignments) =>
        new(MergeActionKind.Insert, null, CheckAssignments(assignments, "An insert must write",
            "DELTA_DUPLICATE_COLUMNS_ON_INSERT: the insert writes"), null);

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

    // Column names compare ignoring case, as Spark resolves them: SET a = 1, A = 2 sets one column twice.
    private static MergeAssignment[] CheckAssignments(MergeAssignment[] assignments, string what, string twice)
    {
        if (assignments is null)
            throw new ArgumentNullException(nameof(assignments));
        if (assignments.Length == 0)
            throw new ArgumentException($"{what} at least one column.", nameof(assignments));
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var assignment in assignments)
        {
            if (assignment is null)
                throw new ArgumentException("An assignment is null.", nameof(assignments));
            if (!seen.Add(assignment.Column))
                throw new ArgumentException($"{twice} column '{assignment.Column}' twice.", nameof(assignments));
        }
        return assignments.ToArray();
    }

    /// <inheritdoc/>
    public override string ToString()
    {
        string action = Assignments is not null
            ? $"{Kind}({string.Join(", ", Assignments)})"
            : Columns is null ? Kind.ToString() : $"{Kind}({string.Join(", ", Columns)})";
        return Condition is null ? action : $"{action} when {Condition}";
    }
}

/// <summary>
/// A key-based MERGE: <c>MERGE INTO target USING source ON target.k1 = source.k1 AND ...</c> followed by its
/// clauses. See <see cref="DeltaTable.MergeAsync"/>.
/// </summary>
/// <remarks>
/// <para><b>Clauses.</b> Each of <see cref="WhenMatched"/>, <see cref="WhenNotMatched"/> and
/// <see cref="WhenNotMatchedBySource"/> holds any number of clauses, tried in order: a row takes the first clause
/// of its kind whose condition is TRUE (<see cref="MergeAction.When"/>), and a row no clause takes is left alone
/// (a matched or not-matched-by-source target row is kept as it is, an unmatched source row is not inserted).
/// As in Spark, only the last clause of a kind may be unconditional.</para>
///
/// <para><b>Conditions and values.</b> A condition, or an assigned value (<see cref="MergeAssignment"/>), names
/// the target row's columns as <c>target.c</c> and the source row's as <c>source.c</c> (references with those two
/// parts, as <see cref="Expressions.UnboundReference.FromParts"/> builds them or the SQL parser parses
/// <c>source.c</c>). A column name only one side has may also be written bare. A not-matched clause sees only the
/// source row and a not-matched-by-source clause only the target row. As in SQL, a row is acted on only when its
/// condition is TRUE: NULL is not.</para>
///
/// <para><b>Matching.</b> A target row matches a source row when every key is equal on both, and a NULL key
/// matches nothing (SQL <c>=</c>, not <c>&lt;=&gt;</c>). A source row matched by any target row is never
/// inserted, whether or not any matched clause applies to it. A target row whose keys two source rows equal is an
/// error (<c>DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE</c>), whatever the clause conditions say,
/// unless the only matched clause is one unconditional delete or the MERGE only inserts. That is delta-spark's rule
/// (it counts the ON matches alone), checked against Spark 4.0 / Delta 4.0.</para>
/// </remarks>
public sealed class MergeOptions
{
    /// <summary>
    /// The columns the ON condition equates: <c>target.k = source.k</c> for each. Each must be a top-level
    /// column of the table with a primitive type, carried by the source under the same name.
    /// </summary>
    public IReadOnlyList<string> Keys { get; init; } = [];

    /// <summary>The clauses for a target row a source row matches, in order: <see cref="MergeAction.Update"/>,
    /// <see cref="MergeAction.UpdateColumns"/>, <see cref="MergeAction.UpdateSet"/> or
    /// <see cref="MergeAction.Delete"/>, each with an optional condition over the matched pair. Empty leaves
    /// matched rows alone (an insert-only MERGE).</summary>
    public IReadOnlyList<MergeAction> WhenMatched { get; init; } = [];

    /// <summary>The clauses for a source row no target row matches, in order: <see cref="MergeAction.Insert"/>,
    /// <see cref="MergeAction.InsertColumns"/> or <see cref="MergeAction.InsertValues"/>, each with an optional
    /// condition over the source row. Empty inserts nothing.</summary>
    public IReadOnlyList<MergeAction> WhenNotMatched { get; init; } = [];

    /// <summary>The clauses for a target row no source row matches, in order: <see cref="MergeAction.UpdateSet"/>
    /// or <see cref="MergeAction.Delete"/>, each with an optional condition over the target row. Any clause here
    /// makes the MERGE read every file of the table, since any row may be unmatched.</summary>
    public IReadOnlyList<MergeAction> WhenNotMatchedBySource { get; init; } = [];

    /// <summary>
    /// Evolves the table's schema to take the source columns the MERGE writes, in the same commit: Spark's MERGE
    /// with <c>spark.databricks.delta.schema.autoMerge.enabled</c>, or <c>withSchemaEvolution()</c> on its builder.
    /// Off by default, as in Spark: a typo in a column name should not silently add a column.
    /// </summary>
    /// <remarks>
    /// <para>As measured against Spark 4.0 / Delta 4.0:</para>
    /// <list type="bullet">
    /// <item>A source column is written by an update or insert that copies every column
    /// (<see cref="MergeAction.Update"/>, <see cref="MergeAction.Insert"/>), names it in a column list, or assigns a
    /// table column of the same name. Only those evolve the schema; a column only a condition reads, or one no
    /// clause writes, does not.</item>
    /// <item>A written source column the table lacks is added at the end, nullable, with the source column's type;
    /// a value assigned to it is cast to that type. A new field of a struct column is added too. Rows the MERGE does
    /// not write read NULL for them.</item>
    /// <item>A written source column wider than the table's (a <c>long</c> into an <c>int</c> column) widens the
    /// column when the table enables <c>delta.enableTypeWidening</c>, with the changes a <c>mergeSchema</c> write
    /// makes. The type is the source column's, not the assigned value's.</item>
    /// <item>The schema evolves even when no row reaches the clause that writes the column: the MERGE then commits
    /// the new schema alone.</item>
    /// <item>A column the table has and the source lacks is left as it is by an update that copies every column,
    /// and is written as an append writes it by such an insert; a field of a struct column the source lacks is kept
    /// by an update and NULL in an insert. Without evolution such a MERGE is refused.</item>
    /// </list>
    /// <para>Inside a transaction it composes with merging appends, and is refused alongside a schema change
    /// staged any other way, as <see cref="DeltaTransaction.WriteAsync"/> with <c>mergeSchema</c> is. A new field
    /// inside an array element or a map key or value is refused, as a <c>mergeSchema</c> write refuses it, though
    /// Spark adds one. A source column whose name differs from a table column's only in case is refused rather than
    /// matched or added.</para>
    /// </remarks>
    public bool MergeSchema { get; init; }
}

/// <summary>
/// What a MERGE did, named as Spark names the MERGE's <c>operationMetrics</c>.
/// </summary>
/// <param name="NumSourceRows">The rows the source held.</param>
/// <param name="NumTargetRowsInserted">Source rows inserted by a not-matched clause.</param>
/// <param name="NumTargetRowsMatchedUpdated">Target rows a matched clause updated.</param>
/// <param name="NumTargetRowsNotMatchedBySourceUpdated">Target rows a not-matched-by-source clause updated.</param>
/// <param name="NumTargetRowsMatchedDeleted">Target rows a matched clause deleted.</param>
/// <param name="NumTargetRowsNotMatchedBySourceDeleted">Target rows a not-matched-by-source clause deleted.</param>
/// <param name="NumTargetRowsCopied">Unchanged target rows rewritten because their file held a changed row.</param>
/// <param name="NumTargetFilesAdded">Data files the MERGE added: rewrites and inserts.</param>
/// <param name="NumTargetFilesRemoved">Data files the MERGE removed.</param>
public sealed record MergeMetrics(
    long NumSourceRows,
    long NumTargetRowsInserted,
    long NumTargetRowsMatchedUpdated,
    long NumTargetRowsNotMatchedBySourceUpdated,
    long NumTargetRowsMatchedDeleted,
    long NumTargetRowsNotMatchedBySourceDeleted,
    long NumTargetRowsCopied,
    long NumTargetFilesAdded,
    long NumTargetFilesRemoved)
{
    /// <summary>Target rows updated by any clause.</summary>
    public long NumTargetRowsUpdated => NumTargetRowsMatchedUpdated + NumTargetRowsNotMatchedBySourceUpdated;

    /// <summary>Target rows deleted by any clause.</summary>
    public long NumTargetRowsDeleted => NumTargetRowsMatchedDeleted + NumTargetRowsNotMatchedBySourceDeleted;

    /// <summary>The rows the MERGE wrote to data files: updated, copied and inserted.</summary>
    public long NumOutputRows => NumTargetRowsUpdated + NumTargetRowsCopied + NumTargetRowsInserted;
}
