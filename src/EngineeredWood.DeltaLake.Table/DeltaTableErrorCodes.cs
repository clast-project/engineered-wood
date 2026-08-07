// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// The <see cref="DeltaFormatException.ErrorCode"/> values raised by the table layer.
/// </summary>
/// <remarks>
/// <para>The companion to <see cref="DeltaErrorCodes"/> in the log layer. They share one flat
/// namespace of string values on purpose: a consumer switches on <c>ErrorCode</c> once, without
/// caring which assembly raised it, and the log layer does not have to know that this one exists.
/// That is the property an enum could not have given — it would have forced these conditions into
/// the log layer's type or handed callers two unrelated enums to switch on.</para>
///
/// <para>Names follow the same rules: delta-spark's verbatim where the condition genuinely
/// corresponds (verified against <c>error/delta-error-classes.json</c> in delta-spark 4.0.0), ours
/// otherwise, and semantically equivalent throw sites share a code. Nine codes cover seventeen
/// sites; the eighteenth is <see cref="DeltaTableNotFoundException"/>, which the log layer already
/// defines.</para>
///
/// <para>A caveat worth knowing when catching these. Several are really CALLER errors — asking to
/// repartition without a full overwrite, naming a column that is not in the table — and in a
/// green-field API would be <see cref="ArgumentException"/> rather than
/// <see cref="DeltaFormatException"/>. They are left as-is because changing the exception type is
/// breaking; the code at least lets a caller tell "you asked for something impossible" from "this
/// table is unreadable", which the type alone did not.</para>
/// </remarks>
public static class DeltaTableErrorCodes
{
    // ── Table configuration conflicts ──

    /// <summary>
    /// A table declares both clustering columns and partition columns, which are mutually exclusive.
    /// </summary>
    /// <remarks>
    /// No delta-spark equivalent. Its <c>DELTA_CLUSTERING_WITH_PARTITION_PREDICATE</c> and
    /// <c>DELTA_CLUSTERING_REPLACE_TABLE_WITH_PARTITIONED_TABLE</c> are different situations —
    /// OPTIMIZE predicates and REPLACE TABLE respectively — so neither name is honest here.
    /// </remarks>
    public const string ClusteringWithPartitioning = "DELTA_CLUSTERING_WITH_PARTITIONING";

    /// <summary>
    /// A caller-supplied <c>preAssignedSchema</c> cannot be used: it carries no column-mapping field
    /// ids for a table being created with column mapping, or it reuses ids the existing table has
    /// already spent.
    /// </summary>
    /// <remarks>
    /// No delta-spark equivalent — <c>preAssignedSchema</c> is an EngineeredWood affordance for hosts
    /// that write their own data files, and Spark has no corresponding entry point.
    /// </remarks>
    public const string InvalidPreAssignedSchema = "DELTA_INVALID_PREASSIGNED_SCHEMA";

    // ── Columns ──

    /// <summary>A named column is not in the table's schema.</summary>
    /// <remarks>delta-spark: <c>DELTA_COLUMN_NOT_FOUND</c> — "Unable to find the column
    /// &lt;columnName&gt; given [&lt;columnList&gt;]".</remarks>
    public const string ColumnNotFound = "DELTA_COLUMN_NOT_FOUND";

    /// <summary>
    /// A named column exists but is not one of the table's partition columns, in a position where
    /// only a partition column is meaningful.
    /// </summary>
    /// <remarks>
    /// delta-spark: <c>DELTA_INVALID_PARTITION_COLUMN</c> — "&lt;columnName&gt; is not a valid
    /// partition column in table &lt;tableName&gt;". Distinct from
    /// <see cref="ColumnNotFound"/> because the fix differs: the column is there, it is the wrong
    /// KIND of column.
    /// </remarks>
    public const string InvalidPartitionColumn = "DELTA_INVALID_PARTITION_COLUMN";

    // ── Time travel ──

    /// <summary>
    /// No commit could be resolved at or before the requested timestamp.
    /// </summary>
    /// <remarks>
    /// No delta-spark equivalent that fits. <c>DELTA_TIMESTAMP_GREATER_THAN_COMMIT</c> covers only
    /// the after-the-latest-version side, and <c>DELTA_NO_COMMITS_FOUND</c> means the log is empty.
    /// This fires on the earlier side, and also when in-commit timestamps are disabled so no
    /// timestamp can be resolved at all.
    /// </remarks>
    public const string NoCommitAtTimestamp = "DELTA_NO_COMMIT_AT_TIMESTAMP";

    // ── Writer enforcement ──

    /// <summary>
    /// The table sets <c>delta.appendOnly=true</c> and the operation would remove or change existing
    /// data.
    /// </summary>
    /// <remarks>delta-spark: <c>DELTA_CANNOT_MODIFY_APPEND_ONLY</c>.</remarks>
    public const string CannotModifyAppendOnly = "DELTA_CANNOT_MODIFY_APPEND_ONLY";

    /// <summary>
    /// The table declares a CHECK constraint, an invariant, or a generation expression that this
    /// writer cannot evaluate, so the write is refused rather than committing data that might
    /// violate it.
    /// </summary>
    /// <remarks>
    /// <para>No delta-spark equivalent, and the reason is instructive: Spark CAN evaluate these, so
    /// the condition does not arise there. Its <c>DELTA_GENERATED_COLUMNS_*</c> errors are about
    /// defining such expressions, not about a writer declining to honour one.</para>
    /// <para>This is a capability answer. The remedy is to write through an engine that evaluates
    /// the expression, not to change the table.</para>
    /// </remarks>
    public const string UnevaluableTableExpression = "DELTA_UNEVALUABLE_TABLE_EXPRESSION";

    // ── Write modes ──

    /// <summary>
    /// The requested combination of write mode and options is not valid for this table — a
    /// repartition outside a full overwrite, or a dynamic partition overwrite on an unpartitioned
    /// table or in an append-shaped call.
    /// </summary>
    /// <remarks>
    /// No delta-spark equivalent: its overwrite-mode errors (<c>DELTA_REPLACE_WHERE_IN_OVERWRITE</c>,
    /// <c>DELTA_OVERWRITE_SCHEMA_WITH_DYNAMIC_PARTITION_OVERWRITE</c>, …) are all phrased in terms of
    /// DataFrame writer options that EngineeredWood does not have, so borrowing one would point a
    /// reader at documentation for an API that is not this one.
    /// </remarks>
    public const string InvalidWriteMode = "DELTA_INVALID_WRITE_MODE";

    /// <summary>
    /// The data handed to a partition overwrite contains rows outside the partitions being replaced,
    /// which would silently write into partitions the caller did not name.
    /// </summary>
    /// <remarks>
    /// delta-spark's <c>DELTA_REPLACE_WHERE_MISMATCH</c> is the nearest thing and is deliberately NOT
    /// reused: it is about a <c>replaceWhere</c> predicate, an API this library does not expose, so
    /// the name would send a reader looking for something that is not here.
    /// </remarks>
    public const string DataOutsideTargetPartitions = "DELTA_DATA_OUTSIDE_TARGET_PARTITIONS";

    // ── Concurrency conditions only the table layer can raise ──
    //
    // Carried by DeltaConflictException, in the same flat namespace as DeltaErrorCodes' concurrency
    // codes — a caller switches on ErrorCode once and does not care which layer named the condition.
    // These two have no delta-spark equivalent because they are about mechanisms Spark does not have.

    /// <summary>
    /// Row-level reconciliation could not absorb a concurrent commit: the same ROW was concurrently
    /// deleted or updated, or a row's file was rewritten away and its stable id could not be resolved
    /// onto the replacement.
    /// </summary>
    /// <remarks>
    /// <para>Distinct from <see cref="DeltaErrorCodes.ConcurrentDeleteDelete"/>, and the distinction is
    /// the point: delete/delete is a FILE-granularity verdict, raised because two transactions touched
    /// the same file. This is raised only after row-granularity reconciliation was attempted and
    /// failed, which means the two transactions genuinely touched the same rows. A host that treats the
    /// two as one condition loses the ability to tell "we collided on a file" from "we collided on
    /// data".</para>
    /// <para>No delta-spark equivalent: deletion-vector union and stable-id remapping across a
    /// concurrent rewrite are this library's own reconciliation, so Spark has no error class for
    /// their failure.</para>
    /// </remarks>
    public const string RowLevelConflict = "DELTA_ROW_LEVEL_CONFLICT";

    /// <summary>
    /// A snapshot-coupled commit found the table had moved off the version it was pinned to. Its
    /// actions were computed against that exact version — deletion-vector ordinals and row positions
    /// resolve against a specific active-file set — so there is no other version at which they are
    /// correct, and the commit aborts rather than rebasing.
    /// </summary>
    /// <remarks>
    /// Raised by <see cref="DeltaTable.CommitDataFilesAsync"/> when <c>expectedVersion</c> is set. The
    /// condition resembles delta-spark's <c>DELTA_CONCURRENT_WRITE</c>, which is deliberately not
    /// reused: that one leaves the staged actions valid (<see cref="ConflictRecovery.Replay"/>) and
    /// this one does not, so sharing a code would erase exactly the difference a caller needs.
    /// </remarks>
    public const string StaleTransactionSnapshot = "DELTA_STALE_TRANSACTION_SNAPSHOT";
}
