// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake.RowTracking;

/// <summary>
/// Constants and utilities for Delta Lake row tracking.
/// Row tracking assigns stable, unique row IDs that persist across compaction.
/// </summary>
public static class RowTrackingConfig
{
    /// <summary>Table property to enable row tracking.</summary>
    public const string EnableKey = "delta.enableRowTracking";

    /// <summary>
    /// The column name for materialized row IDs in data files.
    /// This is a hidden metadata column, not part of the user-visible schema.
    /// </summary>
    public const string RowIdColumnName = "_metadata.row_id";

    /// <summary>Virtual column name exposed to readers for the row ID.</summary>
    public const string VirtualRowIdColumn = "_metadata.row_id";

    /// <summary>Virtual column name exposed to readers for the commit version.</summary>
    public const string VirtualRowCommitVersionColumn = "_metadata.row_commit_version";

    /// <summary>
    /// Returns true if row tracking is enabled for the table.
    /// </summary>
    public static bool IsEnabled(IReadOnlyDictionary<string, string>? configuration) =>
        configuration is not null &&
        configuration.TryGetValue(EnableKey, out string? value) &&
        string.Equals(value, "true", StringComparison.OrdinalIgnoreCase);

    /// <summary>The row-tracking domain-metadata domain name (spec).</summary>
    public const string DomainName = "delta.rowTracking";

    /// <summary>
    /// Table-property key naming the hidden physical column that MATERIALIZES row IDs (spec:
    /// <c>delta.rowTracking.materializedRowIdColumnName</c>). A conformant reader consults this to find the
    /// per-row row-id column that overrides <c>baseRowId + position</c> for rows moved by a rewrite. The name
    /// is fixed at table creation and never changes.
    /// </summary>
    public const string MaterializedRowIdColumnNameKey = "delta.rowTracking.materializedRowIdColumnName";

    /// <summary>
    /// Table-property key naming the hidden physical column that MATERIALIZES row commit versions (spec:
    /// <c>delta.rowTracking.materializedRowCommitVersionColumnName</c>). The commit-version counterpart of
    /// <see cref="MaterializedRowIdColumnNameKey"/>.
    /// </summary>
    public const string MaterializedRowCommitVersionColumnNameKey =
        "delta.rowTracking.materializedRowCommitVersionColumnName";

    /// <summary>
    /// Generates the two spec-required hidden physical column names for a table that is enabling row tracking.
    /// The names are UUID-suffixed so they can never collide with a user column; they are FIXED at enablement,
    /// stored in table metadata under <see cref="MaterializedRowIdColumnNameKey"/> /
    /// <see cref="MaterializedRowCommitVersionColumnNameKey"/>, and honored by every reader/writer thereafter.
    /// Fresh appends never write these columns (a row's id is <c>baseRowId + position</c>); they carry
    /// materialized values only for rows relocated by a copy-on-write rewrite.
    /// </summary>
    public static (string RowIdColumnName, string RowCommitVersionColumnName) GenerateMaterializedColumnNames() =>
        ($"_row_id_{Guid.NewGuid():N}", $"_row_commit_version_{Guid.NewGuid():N}");

    /// <summary>
    /// Returns the two hidden materialized physical column names stored at enablement, or <c>(null, null)</c>
    /// when row tracking is off / the names are absent. A rewrite (UPDATE / compaction) writes each relocated
    /// row's original id + commit version into these columns, and the read path strips them; both are keyed by
    /// the physical name (spec: <see cref="MaterializedRowIdColumnNameKey"/> /
    /// <see cref="MaterializedRowCommitVersionColumnNameKey"/>).
    /// </summary>
    public static (string? RowIdColumnName, string? RowCommitVersionColumnName) TryGetMaterializedColumnNames(
        IReadOnlyDictionary<string, string>? configuration)
    {
        if (configuration is null)
            return (null, null);
        configuration.TryGetValue(MaterializedRowIdColumnNameKey, out string? rowId);
        configuration.TryGetValue(MaterializedRowCommitVersionColumnNameKey, out string? rowCommitVersion);
        return (string.IsNullOrEmpty(rowId) ? null : rowId,
                string.IsNullOrEmpty(rowCommitVersion) ? null : rowCommitVersion);
    }

    /// <summary>
    /// Builds the <c>delta.rowTracking</c> domainMetadata action recording the new high-water mark. The
    /// domain stores the HIGHEST ASSIGNED row id (spec), while <paramref name="nextAvailableRowId"/> is
    /// engineered-wood's internal "next id to assign".
    /// </summary>
    public static Actions.DomainMetadata BuildHighWaterMarkAction(long nextAvailableRowId) => new()
    {
        Domain = DomainName,
        Configuration = "{\"rowIdHighWaterMark\":"
            + (nextAvailableRowId - 1).ToString(System.Globalization.CultureInfo.InvariantCulture) + "}",
        Removed = false,
    };

    /// <summary>
    /// Reads the highest-assigned row id from the <c>delta.rowTracking</c> domain metadata, or null when
    /// the domain is absent/removed/unparsable.
    /// </summary>
    public static long? TryReadHighWaterMark(
        IReadOnlyDictionary<string, Actions.DomainMetadata> domainMetadata)
    {
        if (!domainMetadata.TryGetValue(DomainName, out var dm) || dm.Removed || dm.Configuration is null)
            return null;
        try
        {
            using var doc = System.Text.Json.JsonDocument.Parse(dm.Configuration);
            if (doc.RootElement.TryGetProperty("rowIdHighWaterMark", out var v)
                && v.ValueKind == System.Text.Json.JsonValueKind.Number && v.TryGetInt64(out long h))
            {
                return h;
            }
        }
        catch (System.Text.Json.JsonException)
        {
        }
        return null;
    }

    /// <summary>
    /// Computes the row ID high water mark from the active file set.
    /// The next available row ID is <c>highWaterMark</c>.
    /// </summary>
    public static long ComputeHighWaterMark(
        IReadOnlyDictionary<string, Actions.AddFile> activeFiles)
    {
        long highWaterMark = 0;

        foreach (var addFile in activeFiles.Values)
        {
            if (addFile.BaseRowId.HasValue)
            {
                // Estimate row count from stats if available
                long numRows = EstimateRowCount(addFile);
                long fileEnd = addFile.BaseRowId.Value + numRows;
                if (fileEnd > highWaterMark)
                    highWaterMark = fileEnd;
            }
        }

        return highWaterMark;
    }

    /// <summary>
    /// Estimates the row count for a file from its stats JSON.
    /// Falls back to 0 if stats are not available.
    /// </summary>
    private static long EstimateRowCount(Actions.AddFile addFile)
    {
        if (addFile.Stats is null)
            return 0;

        try
        {
            using var doc = System.Text.Json.JsonDocument.Parse(addFile.Stats);
            if (doc.RootElement.TryGetProperty("numRecords", out var nr))
                return nr.GetInt64();
        }
        catch
        {
            // Stats are optional; if they can't be parsed, use 0
        }

        return 0;
    }
}
