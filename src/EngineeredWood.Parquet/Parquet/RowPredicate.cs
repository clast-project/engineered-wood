// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;

namespace EngineeredWood.Parquet;

/// <summary>
/// One conjunct of a <see cref="RowFilter"/>: the columns it reads, and a function from them to a mask.
/// </summary>
/// <remarks>
/// The reader never looks inside <see cref="Evaluate"/>, so a predicate can be anything that answers a
/// batch: an expression evaluator, a host engine's join filter, a probabilistic membership test.
/// </remarks>
public sealed class RowPredicate
{
    /// <summary>Creates a predicate over <paramref name="columns"/>.</summary>
    /// <param name="columns">Top-level column names, at least one, without repeats.</param>
    /// <param name="evaluate">The predicate; see <see cref="Evaluate"/>.</param>
    /// <exception cref="ArgumentException"><paramref name="columns"/> is empty or names a column twice.</exception>
    public RowPredicate(IReadOnlyList<string> columns, Func<RecordBatch, BooleanArray> evaluate)
    {
        if (columns is null)
            throw new ArgumentNullException(nameof(columns));
        if (columns.Count == 0)
            throw new ArgumentException("A row predicate reads at least one column.", nameof(columns));
        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var name in columns)
        {
            if (name is null)
                throw new ArgumentException("A column name is null.", nameof(columns));
            if (!seen.Add(name))
                throw new ArgumentException($"Column '{name}' is named twice.", nameof(columns));
        }

        Columns = columns.ToArray();
        Evaluate = evaluate ?? throw new ArgumentNullException(nameof(evaluate));
    }

    /// <summary>
    /// The top-level columns the predicate reads. The batch passed to <see cref="Evaluate"/> holds exactly
    /// these, in this order, whether or not the read projects them.
    /// </summary>
    public IReadOnlyList<string> Columns { get; }

    /// <summary>
    /// Answers each row of a batch: a mask of the batch's length. FALSE drops the row; NULL keeps it for the
    /// predicates after this one but out of the result (see <see cref="RowFilter"/>).
    /// </summary>
    /// <remarks>
    /// <para>May be called concurrently, since row groups are read in parallel. The batch belongs to the
    /// reader: do not keep it or dispose it. The mask may be one of the batch's own columns (a boolean
    /// column answering for itself); otherwise the reader takes ownership of it and disposes it.</para>
    /// <para>An exception propagates out of the read, at the position of the batch that raised it, as it
    /// would from a filter applied to the batches afterwards.</para>
    /// </remarks>
    public Func<RecordBatch, BooleanArray> Evaluate { get; }

    /// <summary>
    /// True when a row's answer depends only on that row's values of <see cref="Columns"/>, and on nothing
    /// else (not its position, not the other rows of the batch), and is deterministic. The reader may then
    /// answer a dictionary-encoded column chunk by evaluating the dictionary once. False by default.
    /// </summary>
    public bool ValueWise { get; init; }
}
