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
    /// <para>How rows are cut into batches is the reader's choice, and differs between reads (a batch size, a
    /// parallel read, two passes): a row's answer must not depend on which other rows share its batch.</para>
    /// </remarks>
    public Func<RecordBatch, BooleanArray> Evaluate { get; }

    /// <summary>
    /// True when a row's answer depends only on that row's values of <see cref="Columns"/>, and on nothing
    /// else (not its position, not the other rows of the batch), and is deterministic. The reader may then
    /// answer a dictionary-encoded column chunk by evaluating the dictionary once. False by default.
    /// </summary>
    public bool ValueWise { get; init; }

    /// <summary>
    /// True when this predicate begins a new condition rather than continuing the one before it: the rows an
    /// earlier predicate answered NULL for are dropped before this one is asked, instead of staying live for it.
    /// </summary>
    /// <remarks>
    /// A <see cref="RowFilter"/> keeps a NULL row live so that the conjuncts split out of one expression keep its
    /// three-valued AND (a later conjunct still evaluates, and can still raise on, a row an earlier one answered
    /// NULL for). A separate condition after them, a Delta host mask, must never see such a row: for the
    /// expression, NULL already decided it.
    /// </remarks>
    internal bool DropsEarlierNulls { get; init; }

    /// <summary>
    /// The predicate's answer for EVERY row of a batch, rows no longer live included, when it can be computed with no
    /// row's value able to raise; null to decline, having raised nothing a value causes (#502 phase 5a). Set, it lets
    /// the reader ask the predicate about a batch whose rows are mostly still live without first gathering those rows
    /// (<see cref="BatchSelector.DefaultWholeBatchLiveShare"/>): the answers for rows an earlier predicate already dropped are
    /// ignored. A decline falls back to <see cref="Evaluate"/> over the live rows. Same batch contract as
    /// <see cref="Evaluate"/>.
    /// </summary>
    internal Func<RecordBatch, BooleanArray?>? EvaluateWholeBatch { get; init; }
}
