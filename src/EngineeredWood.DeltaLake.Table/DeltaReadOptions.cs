// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// Everything a <see cref="DeltaTable.ReadAsync"/> call can vary: projection, pruning, which version, and
/// which per-row metadata columns to append. One options object rather than a method per combination —
/// <see cref="DeltaRowMetadata"/> is <see cref="FlagsAttribute"/>, so two metadata kinds cost one pass
/// instead of two reads of the table.
/// </summary>
public sealed record DeltaReadOptions
{
    /// <summary>
    /// Logical column names to project. Null reads all columns. The emitted columns follow the TABLE's
    /// field order, not the order named here, and an unknown name is ignored rather than erroring —
    /// <see cref="DeltaTable.GetReadSchema"/> reports exactly what will come back.
    /// </summary>
    public IReadOnlyList<string>? Columns { get; init; }

    /// <summary>
    /// Superset-safe pruning by partition values and column statistics: a file is skipped only when its
    /// statistics in the log PROVE no row can match, and within a surviving file so is a Parquet row group
    /// whose footer statistics prove the same, or whose dictionary pages do (unless
    /// <see cref="Parquet.ParquetReadOptions.FilterUseDictionaries"/> is turned off), or whose Bloom filter
    /// does (when <see cref="Parquet.ParquetReadOptions.FilterUseBloomFilters"/> is set), and within a
    /// surviving row group so are the pages whose page-index bounds prove it (unless
    /// <see cref="Parquet.ParquetReadOptions.FilterUsePageIndex"/> is turned off), all on the table's
    /// <see cref="DeltaTableOptions.ParquetReadOptions"/>. Row groups and pages are pruned only by the built-in
    /// reader: a <see cref="DeltaTableOptions.DataFileReader"/> gets file pruning alone, because the codec seam
    /// hides the footer. The reader does NOT re-apply this per row, so
    /// surviving batches still contain non-matching rows — filter them yourself. Metadata columns are
    /// unaffected: a row keeps its file position and row id however many rows before it were skipped.
    /// </summary>
    public Expressions.Predicate? Filter { get; init; }

    /// <summary>
    /// When true, <see cref="Filter"/> is also applied to rows, exactly: a scan returns only the rows it answers
    /// TRUE for (a NULL answer drops the row, as a SQL <c>WHERE</c> does). False (the default) keeps the superset
    /// <see cref="Filter"/> describes.
    /// </summary>
    /// <remarks>
    /// <para>The filter is evaluated with the same Spark-configured rules as a predicate DELETE or UPDATE, against
    /// each row's logical values (partition values, schema-evolution NULLs and widened types included). It is
    /// refused before any file is read when Spark's analyzer would refuse it, and a value that fails to evaluate (an
    /// ANSI cast error, say) raises from the scan. A row a deletion vector deletes is never evaluated.</para>
    /// <para>With the built-in reader the evaluation runs inside the Parquet read, where it can avoid decoding the
    /// other columns of rows it rejects (#502); through a <see cref="DeltaTableOptions.DataFileReader"/> it runs on
    /// each batch the reader returns. Columns the filter reads but <see cref="Columns"/> does not name are read for
    /// it and not returned. Metadata columns follow the rows: a row keeps its file position and row id.</para>
    /// </remarks>
    public bool FilterRows { get; init; }

    /// <summary>
    /// Rows of the scan's result to skip before the first one returned. 0 (the default) skips none.
    /// </summary>
    /// <remarks>
    /// <para>The result's order is the scan's: files in path order, row groups and rows in file order, whatever
    /// <see cref="DeltaTableOptions.MaxConcurrentReads"/> is. An offset past the last row returns nothing.</para>
    /// <para>Applied to the rows the scan returns, so with a <see cref="Filter"/> only when it is exact
    /// (<see cref="FilterRows"/>); with a superset filter the rows skipped would depend on which ones pruning happened
    /// to keep, and setting an offset or a limit then throws. <see cref="RowMasks"/> apply first.</para>
    /// <para>With no row condition (no exact filter, no mask) and the built-in reader, each row group's live rows
    /// are known from the file's footer and deletion vector, so the row groups the offset covers are not read at
    /// all. Otherwise skipped rows are read and evaluated to know which they are. Metadata columns are those of the
    /// rows returned.</para>
    /// </remarks>
    /// <exception cref="ArgumentOutOfRangeException">Negative.</exception>
    public long Offset { get; init; }

    /// <summary>
    /// The most rows the scan returns, after <see cref="Offset"/>. Null (the default) returns every row; 0 returns
    /// none and reads nothing.
    /// </summary>
    /// <remarks>
    /// <para>The scan stops reading once the rows are returned. A consumer that stops enumerating early gets that
    /// too, but only after the scan has read ahead (<see cref="DeltaTableOptions.MaxConcurrentReads"/> row groups);
    /// with a limit it reads ahead only what it needs. With no row condition and the built-in reader, the row groups
    /// that hold the rows are known from their footers and deletion vectors, and only those are read, in parallel.
    /// With an exact filter, masks or a <see cref="DeltaTableOptions.DataFileReader"/>, how many rows match is not
    /// known ahead: the scan reads one row group first (a whole window when there is an offset, which has to be read
    /// through anyway), then as many more as the rows still needed suggest, up to its window.</para>
    /// <para>Same conditions as <see cref="Offset"/>.</para>
    /// </remarks>
    /// <exception cref="ArgumentOutOfRangeException">Negative.</exception>
    public long? Limit { get; init; }

    /// <summary>
    /// The host's own row conditions (#502): a scan returns only the rows every mask answers TRUE for. Null or empty
    /// for none. They apply whether or not <see cref="FilterRows"/> is set, after the exact <see cref="Filter"/>
    /// when it is, in the order given. Each mask's columns are refused before any file is read unless the table has
    /// them; they are read for the mask and returned only if <see cref="Columns"/> names them.
    /// </summary>
    /// <remarks>
    /// A row a deletion vector deletes is never passed to a mask, and metadata columns follow the rows kept. See
    /// <see cref="DeltaRowMask"/> for what a mask is given and may assume.
    /// </remarks>
    public IReadOnlyList<DeltaRowMask>? RowMasks { get; init; }

    /// <summary>Which per-row metadata columns to append. Combinable; defaults to none.</summary>
    public DeltaRowMetadata Metadata { get; init; } = DeltaRowMetadata.None;

    /// <summary>
    /// Prefix for the <see cref="DeltaRowMetadata.Locator"/> and <see cref="DeltaRowMetadata.RowTracking"/>
    /// column names (see <see cref="DeltaMetadataColumns"/>). Change it when the table has columns of its
    /// own under the default — a collision is refused rather than shadowed, so there has to be somewhere to
    /// move them to. <see cref="DeltaRowMetadata.RowAddress"/> is not prefixed.
    /// </summary>
    public string MetadataPrefix { get; init; } = DeltaMetadataColumns.DefaultPrefix;

    /// <summary>
    /// Time travel: read this version instead of the latest. Null reads the current snapshot. Mutually
    /// exclusive with <see cref="Snapshot"/> — setting both throws rather than picking one.
    /// </summary>
    public long? AtVersion { get; init; }

    /// <summary>
    /// Read against a snapshot the caller already pinned — ordinarily
    /// <see cref="DeltaTransaction.Snapshot"/>, so that planning, reading, addressing and committing all name
    /// ONE version. Resolves with no I/O, unlike <see cref="AtVersion"/>.
    ///
    /// <para>This is what makes the host guide's "pin one version and use it everywhere" true at every step
    /// rather than at three of four. Without it a read inside a transaction silently follows
    /// <see cref="DeltaTable.CurrentSnapshot"/>, so the rows it returns — and any address minted from them —
    /// can come from a version the transaction is not validating against.</para>
    ///
    /// <para>Mutually exclusive with <see cref="AtVersion"/>; setting both throws.</para>
    /// </summary>
    public Snapshot.Snapshot? Snapshot { get; init; }
}
