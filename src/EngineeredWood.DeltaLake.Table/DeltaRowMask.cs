// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// A host's own row condition for a scan (<see cref="DeltaReadOptions.RowMasks"/>): the logical columns it reads,
/// and a function from them to a mask. A host engine's runtime join filter is the case it is for: a membership test
/// of a key column against the join's build side, applied while the probe side is read.
/// </summary>
/// <remarks>
/// <para>The scan never looks inside <see cref="Mask"/>, so a mask may over-approximate (a Bloom filter's false
/// positives): the rows it keeps are returned, and the host rechecks them as its join would anyway.</para>
/// <para>With the built-in reader the mask runs inside the Parquet read, after <see cref="DeltaReadOptions.Filter"/>
/// when <see cref="DeltaReadOptions.FilterRows"/> is set, and only over the rows still live, so the other columns of
/// the rows it rejects can go undecoded. Through a <see cref="DeltaTableOptions.DataFileReader"/> it runs on each
/// batch the reader returns.</para>
/// </remarks>
public sealed class DeltaRowMask
{
    /// <summary>Creates a mask over <paramref name="columns"/>.</summary>
    /// <param name="columns">Top-level logical column names of the table, at least one, without repeats.</param>
    /// <param name="mask">The mask; see <see cref="Mask"/>.</param>
    /// <exception cref="ArgumentException"><paramref name="columns"/> is empty or names a column twice.</exception>
    public DeltaRowMask(IReadOnlyList<string> columns, Func<RecordBatch, BooleanArray> mask)
    {
        if (columns is null)
            throw new ArgumentNullException(nameof(columns));
        if (columns.Count == 0)
            throw new ArgumentException("A row mask reads at least one column.", nameof(columns));
        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var name in columns)
        {
            if (name is null)
                throw new ArgumentException("A column name is null.", nameof(columns));
            if (!seen.Add(name))
                throw new ArgumentException($"Column '{name}' is named twice.", nameof(columns));
        }

        Columns = columns.ToArray();
        Mask = mask ?? throw new ArgumentNullException(nameof(mask));
    }

    /// <summary>
    /// The top-level logical columns the mask reads. The batch passed to <see cref="Mask"/> holds exactly these, in
    /// this order, whether or not the scan projects them, and each as the scan returns it: a partition column carries
    /// the file's partition value, and a column added after a file was written is NULL for that file's rows. A name
    /// the table doesn't have is refused when the scan starts.
    /// <para>Each column has the table's logical type, but not always one Arrow representation of it: a string or
    /// binary column read from the file follows <see cref="EngineeredWood.Parquet.ParquetReadOptions.ByteArrayOutput"/> (<see cref="DeltaTableOptions.ParquetReadOptions"/>) (a view or
    /// large-offset array), while a partition value or a backfilled NULL is a plain array, as in the scan's own
    /// batches. A mask over such a column should accept any of them.</para>
    /// </summary>
    public IReadOnlyList<string> Columns { get; }

    /// <summary>
    /// Answers each row of a batch: a mask of the batch's length. TRUE keeps the row; FALSE or NULL drops it.
    /// </summary>
    /// <remarks>
    /// <para>May be called concurrently, since files and row groups are read in parallel. The batch belongs to the
    /// scan: do not keep it or dispose it. The scan takes ownership of the returned mask, unless it is one of the
    /// batch's own columns.</para>
    /// <para>An exception propagates out of the scan.</para>
    /// <para>How rows are cut into batches is the scan's choice, and differs between reads: a row's answer must
    /// not depend on which other rows share its batch. A mask that reads no column the file holds (only partition
    /// columns, or columns added after the file was written) sees the same values on every row of the file, and is
    /// asked once for the whole file, over a single row.</para>
    /// </remarks>
    public Func<RecordBatch, BooleanArray> Mask { get; }

    /// <summary>
    /// True when a row's answer depends only on that row's values of <see cref="Columns"/>, and on nothing else (not
    /// its position, not the other rows of the batch), and is deterministic: a membership test of a key against a
    /// join's build side is. False by default.
    /// </summary>
    /// <remarks>
    /// With the built-in reader, a value-wise mask over one column the scan does not return may then be asked once
    /// per dictionary-encoded column chunk, over the chunk's dictionary (and one NULL when the column is nullable),
    /// instead of over its rows; each row takes its value's answer. So the mask may see values no live row holds
    /// (a deleted row's), and an exception it raises over the dictionary makes the scan ask the rows instead.
    /// </remarks>
    public bool ValueWise { get; init; }
}
