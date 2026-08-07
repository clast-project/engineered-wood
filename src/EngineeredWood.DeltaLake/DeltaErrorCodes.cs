// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake;

/// <summary>
/// The stable identifiers carried by <see cref="DeltaFormatException.ErrorCode"/>.
/// </summary>
/// <remarks>
/// <para>These exist because the exception MESSAGE is not an identity. A caller that needs to tell
/// "there is no table here" from "this table needs a feature we lack" from "this log is corrupt" was
/// previously left matching on message substrings — which this repository's own tests already do in a
/// dozen places, so the prose is frozen API by accident. A code gives the condition a name and frees
/// the wording to change.</para>
///
/// <para>Strings rather than an enum, deliberately: the set grows, a string is self-describing in a
/// log or a telemetry field without a lookup table, there is no numbering to keep stable, and
/// <c>EngineeredWood.DeltaLake.Table</c> can publish its own codes into the same flat namespace
/// without the log layer having to know that it exists.</para>
///
/// <para>Several names are taken VERBATIM from delta-spark's error classes, verified against
/// <c>error/delta-error-classes.json</c> in delta-spark 4.0.0, so a host bridging engines can treat
/// them as the same condition. Each one is marked below. Where our condition merely resembles one of
/// Spark's, it gets our own name instead — an identical name would assert an equivalence we have not
/// checked.</para>
///
/// <para>Semantically equivalent throw sites SHARE a code. Thirteen of these cover forty-one sites:
/// the stack trace says which line, and a caller that needs the line rather than the condition is
/// not being served by an error code anyway.</para>
/// </remarks>
public static class DeltaErrorCodes
{
    // ── Table existence and log integrity ──

    /// <summary>
    /// The path holds no Delta table: the log names no version at all, by commit or checkpoint.
    /// Carried by <see cref="DeltaTableNotFoundException"/>.
    /// </summary>
    /// <remarks>delta-spark: <c>DELTA_PATH_DOES_NOT_EXIST</c> — "&lt;path&gt; doesn't exist, or is
    /// not a Delta table". EW is path-addressed, so this is the match rather than the catalog-oriented
    /// <c>DELTA_TABLE_NOT_FOUND</c>.</remarks>
    public const string PathDoesNotExist = "DELTA_PATH_DOES_NOT_EXIST";

    /// <summary>
    /// Replay cannot cover its whole range: a version between the starting point and the target is
    /// missing or unreadable, and no checkpoint covers it.
    /// </summary>
    /// <remarks>delta-spark: <c>DELTA_TRUNCATED_TRANSACTION_LOG</c> — "Unable to reconstruct state at
    /// version &lt;version&gt; as the transaction log has been truncated".</remarks>
    public const string TruncatedTransactionLog = "DELTA_TRUNCATED_TRANSACTION_LOG";

    /// <summary>
    /// A checkpoint the table depends on is in a form this implementation recognises but cannot
    /// decode, and no other route to the requested version exists.
    /// </summary>
    /// <remarks>
    /// <para>Distinct from <see cref="TruncatedTransactionLog"/>, which it would otherwise be
    /// mistaken for: nothing is missing from the log, and no amount of retention tuning will help.
    /// The remedy is a newer version of this library, or a writer that emits a form it reads.</para>
    /// <para>No delta-spark equivalent, and for the usual instructive reason: Spark decodes every
    /// checkpoint form the spec defines, so the condition does not arise there.</para>
    /// </remarks>
    public const string UnsupportedCheckpointFormat = "DELTA_UNSUPPORTED_CHECKPOINT_FORMAT";

    /// <summary>
    /// The log replayed to its end without yielding a protocol or a metadata action, so there is no
    /// table state to describe.
    /// </summary>
    /// <remarks>delta-spark: <c>DELTA_STATE_RECOVER_ERROR</c> — "The &lt;operation&gt; of your Delta
    /// table could not be recovered while Reconstructing version", where the operation is exactly
    /// Protocol or Metadata.</remarks>
    public const string StateRecoverError = "DELTA_STATE_RECOVER_ERROR";

    // ── Protocol and table features ──

    /// <summary>
    /// The table declares a reader or writer protocol version above what this implementation supports.
    /// </summary>
    /// <remarks>delta-spark: <c>DELTA_INVALID_PROTOCOL_VERSION</c>, which likewise covers the reader
    /// and writer cases with one code and distinguishes them in the message.</remarks>
    public const string InvalidProtocolVersion = "DELTA_INVALID_PROTOCOL_VERSION";

    /// <summary>The table requires named reader features this implementation does not support.</summary>
    /// <remarks>delta-spark: <c>DELTA_UNSUPPORTED_FEATURES_FOR_READ</c>.</remarks>
    public const string UnsupportedFeaturesForRead = "DELTA_UNSUPPORTED_FEATURES_FOR_READ";

    /// <summary>The table requires named writer features this implementation does not support.</summary>
    /// <remarks>delta-spark: <c>DELTA_UNSUPPORTED_FEATURES_FOR_WRITE</c>.</remarks>
    public const string UnsupportedFeaturesForWrite = "DELTA_UNSUPPORTED_FEATURES_FOR_WRITE";

    // ── Table configuration ──

    /// <summary>The table's schema or configuration violates the IcebergCompat rules it declares.</summary>
    /// <remarks>delta-spark: <c>DELTA_ICEBERG_COMPAT_VIOLATION</c>, also a single code for every
    /// validation rule, parameterised by version.</remarks>
    public const string IcebergCompatViolation = "DELTA_ICEBERG_COMPAT_VIOLATION";

    /// <summary>
    /// A write attempted to create or modify a domainMetadata entry in the system-reserved namespace.
    /// </summary>
    /// <remarks>No delta-spark equivalent: its <c>DELTA_DOMAIN_METADATA_NOT_SUPPORTED</c> is the
    /// different condition of the table feature being absent.</remarks>
    public const string SystemDomainModification = "DELTA_SYSTEM_DOMAIN_MODIFICATION";

    // ── Log decoding ──

    /// <summary>A commit or checkpoint line is not the JSON shape an action must have.</summary>
    /// <remarks>No delta-spark equivalent — Spark decodes the log through Spark SQL and fails
    /// differently.</remarks>
    public const string InvalidLogJson = "DELTA_INVALID_LOG_JSON";

    /// <summary>An action is missing a field the protocol marks required.</summary>
    /// <remarks>No delta-spark equivalent. The message names the field.</remarks>
    public const string MissingRequiredField = "DELTA_MISSING_REQUIRED_FIELD";

    /// <summary>Serialization was handed an action type this writer cannot emit.</summary>
    /// <remarks>
    /// No delta-spark equivalent. This one is arguably a caller error rather than a format error —
    /// see the note on the throw site.
    /// </remarks>
    public const string UnsupportedActionType = "DELTA_UNSUPPORTED_ACTION_TYPE";

    // ── Deletion vectors ──

    /// <summary>
    /// A deletion vector's stored bytes could not be decoded: bad Z85, a bad magic number, a
    /// truncated buffer, an impossible count, or an extent past the end of the file.
    /// </summary>
    /// <remarks>
    /// No delta-spark equivalent. Spark's <c>DELTA_DELETION_VECTOR_*</c> codes are write-time
    /// integrity checks (cardinality, checksum, size mismatch) rather than decode failures, so
    /// borrowing one of those names would claim a correspondence that does not hold.
    /// </remarks>
    public const string InvalidDeletionVector = "DELTA_INVALID_DELETION_VECTOR";

    /// <summary>
    /// A deletion vector names a storage type this implementation does not read. Distinct from
    /// <see cref="InvalidDeletionVector"/>: the bytes may be perfectly good, and the answer is a
    /// capability one rather than a corruption one.
    /// </summary>
    public const string UnsupportedDeletionVectorStorageType =
        "DELTA_UNSUPPORTED_DELETION_VECTOR_STORAGE_TYPE";
}
