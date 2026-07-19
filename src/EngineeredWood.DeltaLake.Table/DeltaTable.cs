// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.CompilerServices;
using Apache.Arrow;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Checkpoint;
using EngineeredWood.DeltaLake.DeletionVectors;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.DeltaLake.Snapshot;
using EngineeredWood.IO;
using EngineeredWood.Parquet;

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// Main entry point for Delta Lake table operations.
/// Supports reading and writing Arrow <see cref="RecordBatch"/> data,
/// time travel, compaction, and vacuum.
/// </summary>
public sealed class DeltaTable : IAsyncDisposable, IDisposable
{
    private readonly ITableFileSystem _fs;
    private readonly DeltaTableOptions _options;
    private readonly TransactionLog _log;
    private readonly CheckpointReader _checkpointReader;
    private readonly CheckpointWriter _checkpointWriter;
    private readonly DeletionVectorReader _dvReader;
    private Snapshot.Snapshot? _currentSnapshot;
    private bool _disposed;

    private DeltaTable(
        ITableFileSystem fileSystem,
        DeltaTableOptions options,
        Snapshot.Snapshot? snapshot)
    {
        _fs = fileSystem;
        _options = options;
        _log = new TransactionLog(fileSystem);
        _checkpointReader = new CheckpointReader(fileSystem);
        _dvReader = new DeletionVectorReader(fileSystem);
        _checkpointWriter = new CheckpointWriter(fileSystem, options.ParquetWriteOptions);
        _currentSnapshot = snapshot;
    }

    /// <summary>The current point-in-time table state.</summary>
    public Snapshot.Snapshot CurrentSnapshot =>
        _currentSnapshot ?? throw new InvalidOperationException("Table not initialized.");

    /// <summary>The Arrow schema of the table.</summary>
    public Apache.Arrow.Schema ArrowSchema => CurrentSnapshot.ArrowSchema;

    /// <summary>
    /// Opens an existing Delta table.
    /// </summary>
    public static async ValueTask<DeltaTable> OpenAsync(
        ITableFileSystem fileSystem,
        DeltaTableOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        options ??= DeltaTableOptions.Default;
        var log = new TransactionLog(fileSystem);

        long latestVersion = await log.GetLatestVersionAsync(cancellationToken)
            .ConfigureAwait(false);

        if (latestVersion < 0)
            throw new DeltaFormatException("No Delta table found (no commits in _delta_log/).");

        var checkpointReader = new CheckpointReader(fileSystem);
        var snapshot = await SnapshotBuilder.BuildAsync(
            log, checkpointReader, atVersion: null, cancellationToken)
            .ConfigureAwait(false);

        // Validate protocol compatibility
        ProtocolVersions.ValidateReadSupport(snapshot.Protocol);

        return new DeltaTable(fileSystem, options, snapshot);
    }

    /// <summary>
    /// Creates a new Delta table with the given Arrow schema.
    /// </summary>
    /// <param name="fileSystem">The filesystem rooted at the table directory.</param>
    /// <param name="schema">The Arrow schema for the table.</param>
    /// <param name="options">Table options.</param>
    /// <param name="partitionColumns">Ordered list of partition column names.</param>
    /// <param name="columnMappingMode">
    /// Column mapping mode. When set to <see cref="ColumnMappingMode.Name"/> or
    /// <see cref="ColumnMappingMode.Id"/>, the protocol is upgraded to
    /// Reader v2 / Writer v5 and column mapping metadata is assigned.
    /// </param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public static async ValueTask<DeltaTable> CreateAsync(
        ITableFileSystem fileSystem,
        Apache.Arrow.Schema schema,
        DeltaTableOptions? options = null,
        IReadOnlyList<string>? partitionColumns = null,
        ColumnMappingMode columnMappingMode = ColumnMappingMode.None,
        CancellationToken cancellationToken = default)
    {
        options ??= DeltaTableOptions.Default;
        var log = new TransactionLog(fileSystem);

        // Check that the table doesn't already exist
        long latestVersion = await log.GetLatestVersionAsync(cancellationToken)
            .ConfigureAwait(false);

        if (latestVersion >= 0)
            throw new InvalidOperationException("Delta table already exists.");

        // Convert Arrow schema to Delta schema
        var deltaSchema = SchemaConverter.FromArrowSchema(schema);

        // Set protocol versions based on column mapping mode
        int minReaderVersion = 1;
        int minWriterVersion = 2;
        Dictionary<string, string>? configuration = null;

        if (columnMappingMode != ColumnMappingMode.None)
        {
            minReaderVersion = 2;
            minWriterVersion = 5;

            // Assign column mapping IDs and physical names
            var (mappedSchema, maxId) = ColumnMapping.AssignColumnMapping(deltaSchema);
            deltaSchema = mappedSchema;

            string modeStr = columnMappingMode switch
            {
                ColumnMappingMode.Id => "id",
                ColumnMappingMode.Name => "name",
                _ => "none",
            };

            configuration = new Dictionary<string, string>
            {
                [ColumnMapping.ModeKey] = modeStr,
                [ColumnMapping.MaxColumnIdKey] = maxId.ToString(),
            };
        }

        // Schema-driven table features must be DECLARED at creation, else a strict reader (Spark,
        // delta-kernel) rejects the table with "feature enabled in metadata but not listed in protocol".
        var readerFeatures = new List<string>();
        var writerFeatures = new List<string>();

        // timestamp_ntz (a naive TIMESTAMP column) is itself a table feature: the spec requires the
        // 'timestampNtz' READER+WRITER feature whenever the schema contains the type, at any nesting depth.
        if (SchemaUsesTimestampNtz(deltaSchema))
        {
            minReaderVersion = 3;
            minWriterVersion = 7;
            readerFeatures.Add("timestampNtz");
            writerFeatures.Add("timestampNtz");
        }

        // Identity columns (delta.identity.* field metadata) are a WRITER-only feature ('identityColumns',
        // legacy writer v6) — readers see an ordinary long column — so the reader version is untouched.
        if (deltaSchema.Fields.Any(IdentityColumn.IsIdentityColumn))
        {
            minWriterVersion = 7;
            writerFeatures.Add("identityColumns");
        }

        // Column mapping is BOTH a reader and writer feature. Once any other feature has forced
        // table-features mode (reader v3 / writer v7) it MUST be listed in BOTH lists too — a v7 protocol
        // with no columnMapping entry reads as "column mapping not supported". Absent any other feature,
        // the legacy versioning set above (reader v2 / writer v5, no lists) is what Spark itself writes.
        if (columnMappingMode != ColumnMappingMode.None &&
            (minReaderVersion >= 3 || minWriterVersion >= 7))
        {
            minReaderVersion = 3;
            minWriterVersion = 7;
            readerFeatures.Add("columnMapping");
            writerFeatures.Add("columnMapping");
        }

        string schemaString = DeltaSchemaSerializer.Serialize(deltaSchema);

        var actions = new List<DeltaAction>
        {
            new ProtocolAction
            {
                MinReaderVersion = minReaderVersion,
                MinWriterVersion = minWriterVersion,
                ReaderFeatures = readerFeatures.Count > 0 ? readerFeatures : null,
                WriterFeatures = writerFeatures.Count > 0 ? writerFeatures : null,
            },
            new MetadataAction
            {
                Id = Guid.NewGuid().ToString(),
                Format = Format.Parquet,
                SchemaString = schemaString,
                PartitionColumns = partitionColumns ?? [],
                Configuration = configuration,
                CreatedTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            },
        };

        // The creation commit gets a commitInfo like every other commit, so version 0 is dated and named in
        // the history (and resolvable by timestamp time travel) rather than being the one silent version.
        var createActions = Log.InCommitTimestamp.EnsureCommitInfo(
            actions, configuration, "CREATE TABLE");
        await log.WriteCommitAsync(0, createActions, cancellationToken).ConfigureAwait(false);

        var snapshot = await SnapshotBuilder.BuildAsync(
            log, checkpointReader: null, atVersion: 0, cancellationToken)
            .ConfigureAwait(false);

        return new DeltaTable(fileSystem, options, snapshot);
    }

    /// <summary>
    /// Opens an existing Delta table, or creates a new one if it doesn't exist.
    /// </summary>
    public static async ValueTask<DeltaTable> OpenOrCreateAsync(
        ITableFileSystem fileSystem,
        Apache.Arrow.Schema schema,
        DeltaTableOptions? options = null,
        IReadOnlyList<string>? partitionColumns = null,
        CancellationToken cancellationToken = default)
    {
        options ??= DeltaTableOptions.Default;
        var log = new TransactionLog(fileSystem);

        long latestVersion = await log.GetLatestVersionAsync(cancellationToken)
            .ConfigureAwait(false);

        if (latestVersion >= 0)
            return await OpenAsync(fileSystem, options, cancellationToken)
                .ConfigureAwait(false);

        return await CreateAsync(fileSystem, schema, options, partitionColumns,
            cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Refreshes the snapshot to the latest version.
    /// </summary>
    public async ValueTask RefreshAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        _currentSnapshot = await SnapshotBuilder.UpdateAsync(
            CurrentSnapshot, _log, cancellationToken).ConfigureAwait(false);
    }

    #region Schema Evolution

    /// <summary>
    /// Schema evolution — appends a nullable column. Writes a metadata-only commit (a new
    /// <see cref="MetadataAction"/> whose schema = the current schema ++ <paramref name="newColumn"/>); NO data
    /// files are rewritten. Old files lack the column, so the read path backfills it as all-NULL. The column
    /// must be nullable (existing rows have no value for it). On a column-mapping table the new field is
    /// assigned a fresh column id (maxColumnId + 1) and physical name — recursively, so a struct/array/map
    /// column arrives with ids on every descendant — and <c>delta.columnMapping.maxColumnId</c> is bumped.
    /// Returns the new version.
    /// </summary>
    public async ValueTask<long> AddColumnAsync(
        Field newColumn, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ProtocolVersions.ValidateWriteSupport(CurrentSnapshot.Protocol);

        if (!newColumn.IsNullable)
            throw new InvalidOperationException(
                $"ADD COLUMN '{newColumn.Name}' must be nullable — existing rows have no value for a new column.");

        var snapshot = CurrentSnapshot;
        var config = snapshot.Metadata.Configuration;
        var mappingMode = ColumnMapping.GetMode(config);

        foreach (var f in snapshot.Schema.Fields)
        {
            if (string.Equals(f.Name, newColumn.Name, StringComparison.Ordinal))
                throw new InvalidOperationException($"Column '{newColumn.Name}' already exists.");
        }

        // Convert the incoming Arrow field to a Delta field (via a one-field schema — reuses the type mapping).
        var newDeltaField = SchemaConverter.FromArrowSchema(
            new Apache.Arrow.Schema([newColumn], null)).Fields[0];

        string newSchemaString;
        var newConfig = config;
        if (mappingMode == ColumnMappingMode.None)
        {
            // Plain table: append the field; old files backfill NULL on read.
            var fields = new List<StructField>(snapshot.Schema.Fields) { newDeltaField };
            newSchemaString = DeltaSchemaSerializer.Serialize(new StructType { Fields = fields });
        }
        else
        {
            // Column-mapping table: assign the new field a fresh column id + physical name RECURSIVELY (the
            // create-time assigner), so a struct/array/map-typed column arrives with ids on every descendant —
            // a top-level-only assignment would commit spec-violating metadata that strict readers reject.
            // Existing fields keep their id/physicalName; maxColumnId advances past the last assigned id.
            var (mappedField, lastId) = AssignMappedField(snapshot.Schema, config, newDeltaField);
            var fields = new List<StructField>(snapshot.Schema.Fields) { mappedField };
            newSchemaString = DeltaSchemaSerializer.Serialize(new StructType { Fields = fields });
            var cfg = config is null
                ? new Dictionary<string, string>()
                : config.ToDictionary(kv => kv.Key, kv => kv.Value);
            cfg[ColumnMapping.MaxColumnIdKey] = lastId.ToString();
            newConfig = cfg;
        }

        // Adding a column whose type requires a schema-driven table feature (timestampNtz) to a table whose
        // protocol lacks it needs a protocol upgrade in the SAME commit — otherwise the committed schema
        // declares a type the protocol doesn't advertise, and strict readers reject the table.
        var protocolUpgrade =
            UpgradeProtocolForFeatures(snapshot.Protocol, RequiredSchemaFeatures(newDeltaField.Type));

        return await CommitMetadataOnlyAsync(
            snapshot,
            snapshot.Metadata with { SchemaString = newSchemaString, Configuration = newConfig },
            "ADD COLUMNS",
            cancellationToken,
            protocolUpgrade).ConfigureAwait(false);
    }

    /// <summary>
    /// Renames a column as a metadata-only commit (a new <c>metaData</c> action changing only the field's
    /// logical name; NO data files are rewritten). ONLY supported on a <b>column-mapping</b> table: the field
    /// keeps its <c>delta.columnMapping.id</c> + <c>physicalName</c>, so existing data files (stored under the
    /// physical name, or matched by field id in id mode) are read unchanged under the new logical name. A
    /// non-mapping table would have to rewrite every file (the logical name IS the physical parquet name), so
    /// it is rejected. Throws if <paramref name="oldName"/> is absent or <paramref name="newName"/> exists.
    /// Returns the new version.
    /// </summary>
    public async ValueTask<long> RenameColumnAsync(
        string oldName, string newName, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ProtocolVersions.ValidateWriteSupport(CurrentSnapshot.Protocol);

        var snapshot = CurrentSnapshot;
        if (ColumnMapping.GetMode(snapshot.Metadata.Configuration) == ColumnMappingMode.None)
        {
            throw new InvalidOperationException(
                "RENAME COLUMN requires column mapping (enable it at table creation) — a plain table would need "
                + "to rewrite every data file since the logical name is the physical parquet column name.");
        }

        var schema = snapshot.Schema;
        StructField? target = null;
        foreach (var f in schema.Fields)
        {
            if (string.Equals(f.Name, newName, StringComparison.Ordinal))
                throw new InvalidOperationException($"Column '{newName}' already exists.");
            if (string.Equals(f.Name, oldName, StringComparison.Ordinal))
                target = f;
        }
        if (target is null)
            throw new InvalidOperationException($"Column '{oldName}' does not exist.");

        var newFields = new List<StructField>(schema.Fields.Count);
        foreach (var f in schema.Fields)
        {
            newFields.Add(ReferenceEquals(f, target)
                ? new StructField
                {
                    Name = newName, Type = f.Type, Nullable = f.Nullable, Metadata = f.Metadata,
                }
                : f);
        }
        string newSchemaString = DeltaSchemaSerializer.Serialize(new StructType { Fields = newFields });

        // metaData.partitionColumns holds LOGICAL names (Spark convention) — renaming a partition column must
        // update it too, else the reader/writer treat the renamed column as an ordinary data column.
        var newPartitionColumns = snapshot.Metadata.PartitionColumns;
        if (newPartitionColumns.Contains(oldName))
        {
            newPartitionColumns = newPartitionColumns
                .Select(pc => string.Equals(pc, oldName, StringComparison.Ordinal) ? newName : pc)
                .ToList();
        }

        return await CommitMetadataOnlyAsync(
            snapshot,
            snapshot.Metadata with
            {
                SchemaString = newSchemaString,
                PartitionColumns = newPartitionColumns,
            },
            "RENAME COLUMN",
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Drops a column as a metadata-only commit (a new <c>metaData</c> action removing the field; NO data files
    /// are rewritten — old files still carry the physical column, which the read path reconciles away against
    /// the current schema). ONLY supported on a <b>column-mapping</b> table: without mapping, dropping a column
    /// would require rewriting every data file, and the name could not be safely reused. The dropped field's
    /// column id is retired (maxColumnId is NOT decremented), so a later ADD COLUMN never reuses it. Throws if
    /// the column is absent, is a partition column, or is the table's only column. Returns the new version.
    /// </summary>
    public async ValueTask<long> DropColumnAsync(
        string name, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ProtocolVersions.ValidateWriteSupport(CurrentSnapshot.Protocol);

        var snapshot = CurrentSnapshot;
        if (ColumnMapping.GetMode(snapshot.Metadata.Configuration) == ColumnMappingMode.None)
        {
            throw new InvalidOperationException(
                "DROP COLUMN requires column mapping (enable it at table creation) — a plain table would need "
                + "to rewrite every data file since the logical name is the physical parquet column name.");
        }
        foreach (var pc in snapshot.Metadata.PartitionColumns)
        {
            if (string.Equals(pc, name, StringComparison.Ordinal))
                throw new InvalidOperationException($"Cannot drop partition column '{name}'.");
        }

        var newFields = new List<StructField>(snapshot.Schema.Fields.Count);
        bool found = false;
        foreach (var f in snapshot.Schema.Fields)
        {
            if (string.Equals(f.Name, name, StringComparison.Ordinal)) { found = true; continue; }
            newFields.Add(f);
        }
        if (!found)
            throw new InvalidOperationException($"Column '{name}' does not exist.");
        if (newFields.Count == 0)
            throw new InvalidOperationException("Cannot drop the table's only column.");

        string newSchemaString = DeltaSchemaSerializer.Serialize(new StructType { Fields = newFields });

        return await CommitMetadataOnlyAsync(
            snapshot,
            snapshot.Metadata with { SchemaString = newSchemaString },
            "DROP COLUMNS",
            cancellationToken).ConfigureAwait(false);
    }

    // Commits a metaData action (the shape every metadata-only schema change takes), optionally preceded by a
    // protocol upgrade in the SAME commit, and refreshes.
    private async ValueTask<long> CommitMetadataOnlyAsync(
        Snapshot.Snapshot snapshot,
        MetadataAction newMetadata,
        string operation,
        CancellationToken cancellationToken,
        ProtocolAction? protocolUpgrade = null)
    {
        var actionList = new List<DeltaAction>();
        if (protocolUpgrade is not null)
            actionList.Add(protocolUpgrade);
        actionList.Add(newMetadata);

        var actions = Log.InCommitTimestamp.EnsureCommitInfo(
            actionList, snapshot.Metadata.Configuration, operation);

        long newVersion = snapshot.Version + 1;
        await _log.WriteCommitAsync(newVersion, actions, cancellationToken).ConfigureAwait(false);

        _currentSnapshot = await SnapshotBuilder.UpdateAsync(
            snapshot, _log, cancellationToken).ConfigureAwait(false);

        return newVersion;
    }

    /// <summary>True when the type contains a <c>timestamp_ntz</c> column at any nesting depth.</summary>
    private static bool SchemaUsesTimestampNtz(DeltaDataType type) => type switch
    {
        PrimitiveType p => string.Equals(p.TypeName, "timestamp_ntz", StringComparison.Ordinal),
        StructType st => st.Fields.Any(f => SchemaUsesTimestampNtz(f.Type)),
        ArrayType at => SchemaUsesTimestampNtz(at.ElementType),
        MapType mt => SchemaUsesTimestampNtz(mt.KeyType) || SchemaUsesTimestampNtz(mt.ValueType),
        _ => false,
    };

    /// <summary>
    /// The schema-driven reader+writer table features <paramref name="type"/> requires per the Delta spec
    /// (currently <c>timestampNtz</c> for a naive timestamp; <c>variantType</c> joins this list when variant
    /// columns are supported).
    /// </summary>
    private static List<string> RequiredSchemaFeatures(DeltaDataType type)
    {
        var features = new List<string>();
        if (SchemaUsesTimestampNtz(type))
            features.Add("timestampNtz");
        return features;
    }

    /// <summary>
    /// Builds the protocol action that adds the given reader+writer features, or null when the current
    /// protocol already declares them all (or none are required). Upgrading a LEGACY-versioned protocol to
    /// table-features mode (reader 3 / writer 7) must enumerate every feature the legacy version implied,
    /// else those capabilities are silently lost on the upgraded table.
    /// </summary>
    private static ProtocolAction? UpgradeProtocolForFeatures(
        ProtocolAction current, IReadOnlyList<string> features)
    {
        var missing = features.Where(f =>
            current.ReaderFeatures?.Contains(f) != true
            || current.WriterFeatures?.Contains(f) != true).ToList();
        if (missing.Count == 0)
            return null;

        var writerFeatures = new List<string>(
            current.WriterFeatures ?? LegacyWriterFeatures(current.MinWriterVersion));
        var readerFeatures = new List<string>(
            current.ReaderFeatures ?? LegacyReaderFeatures(current.MinReaderVersion));
        foreach (var feature in missing)
        {
            if (!writerFeatures.Contains(feature))
                writerFeatures.Add(feature);
            if (!readerFeatures.Contains(feature))
                readerFeatures.Add(feature);
        }

        return new ProtocolAction
        {
            MinReaderVersion = 3,
            MinWriterVersion = 7,
            ReaderFeatures = readerFeatures,
            WriterFeatures = writerFeatures,
        };
    }

    /// <summary>Writer features implied by a legacy writer version (Delta spec upgrade table).</summary>
    private static List<string> LegacyWriterFeatures(int minWriterVersion)
    {
        var features = new List<string>();
        if (minWriterVersion >= 2) { features.Add("appendOnly"); features.Add("invariants"); }
        if (minWriterVersion >= 3) { features.Add("checkConstraints"); }
        if (minWriterVersion >= 4) { features.Add("changeDataFeed"); features.Add("generatedColumns"); }
        if (minWriterVersion >= 5) { features.Add("columnMapping"); }
        if (minWriterVersion >= 6) { features.Add("identityColumns"); }
        return features;
    }

    /// <summary>Reader features implied by a legacy reader version (Delta spec upgrade table).</summary>
    private static List<string> LegacyReaderFeatures(int minReaderVersion)
    {
        var features = new List<string>();
        if (minReaderVersion >= 2) { features.Add("columnMapping"); }
        return features;
    }

    // Assigns column-mapping metadata (id + physical name) to a NEW field being added to a mapped table —
    // recursively, via the create-time assigner, so struct/array/map descendants all get their own ids. Ids
    // continue past the table's current maxColumnId (schema-derived OR the config key, whichever is higher).
    // Returns the mapped field + the last assigned id (the new maxColumnId).
    private static (StructField Field, int LastId) AssignMappedField(
        StructType baseSchema, IReadOnlyDictionary<string, string>? config, StructField field)
    {
        int maxId = ColumnMapping.GetMaxColumnId(baseSchema);
        if (config is not null && config.TryGetValue(ColumnMapping.MaxColumnIdKey, out var maxStr)
            && int.TryParse(maxStr, out var cfgMax))
        {
            maxId = Math.Max(maxId, cfgMax);
        }
        var (assigned, lastId) = ColumnMapping.AssignColumnMapping(
            new StructType { Fields = [field] }, maxId);
        return (assigned.Fields[0], lastId);
    }

    #endregion

    #region Domain Metadata

    /// <summary>
    /// Gets all active domain metadata entries.
    /// </summary>
    public IReadOnlyDictionary<string, DomainMetadata> GetDomainMetadata() =>
        CurrentSnapshot.DomainMetadata;

    /// <summary>
    /// Gets the configuration for a specific domain, or null if not set.
    /// </summary>
    public string? GetDomainMetadata(string domain) =>
        CurrentSnapshot.DomainMetadata.TryGetValue(domain, out var dm) ? dm.Configuration : null;

    /// <summary>
    /// Sets domain metadata. User domains are unrestricted; system domains
    /// (starting with <c>delta.</c>) can only be modified by implementations
    /// that understand them.
    /// </summary>
    public async ValueTask<long> SetDomainMetadataAsync(
        string domain, string configuration,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ProtocolVersions.ValidateWriteSupport(CurrentSnapshot.Protocol);
        DomainMetadataValidation.ValidateUserModification(domain);

        IReadOnlyList<DeltaAction> actions = new List<DeltaAction>
        {
            new DomainMetadata
            {
                Domain = domain,
                Configuration = configuration,
                Removed = false,
            },
        };

        actions = Log.InCommitTimestamp.EnsureCommitInfo(
            actions, CurrentSnapshot.Metadata.Configuration, "SET DOMAIN METADATA");

        long newVersion = CurrentSnapshot.Version + 1;
        await _log.WriteCommitAsync(newVersion, actions, cancellationToken)
            .ConfigureAwait(false);

        _currentSnapshot = await SnapshotBuilder.UpdateAsync(
            CurrentSnapshot, _log, cancellationToken).ConfigureAwait(false);

        return newVersion;
    }

    /// <summary>
    /// Removes domain metadata by setting a tombstone. User domains are unrestricted;
    /// system domains can only be removed by implementations that understand them.
    /// </summary>
    public async ValueTask<long> RemoveDomainMetadataAsync(
        string domain,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ProtocolVersions.ValidateWriteSupport(CurrentSnapshot.Protocol);
        DomainMetadataValidation.ValidateUserModification(domain);

        if (!CurrentSnapshot.DomainMetadata.ContainsKey(domain))
            throw new InvalidOperationException(
                $"Domain '{domain}' does not exist in the table metadata.");

        IReadOnlyList<DeltaAction> actions = new List<DeltaAction>
        {
            new DomainMetadata
            {
                Domain = domain,
                Configuration = "",
                Removed = true,
            },
        };

        actions = Log.InCommitTimestamp.EnsureCommitInfo(
            actions, CurrentSnapshot.Metadata.Configuration, "REMOVE DOMAIN METADATA");

        long newVersion = CurrentSnapshot.Version + 1;
        await _log.WriteCommitAsync(newVersion, actions, cancellationToken)
            .ConfigureAwait(false);

        _currentSnapshot = await SnapshotBuilder.UpdateAsync(
            CurrentSnapshot, _log, cancellationToken).ConfigureAwait(false);

        return newVersion;
    }

    #endregion

    /// <summary>
    /// Gets a snapshot at a specific version (time travel).
    /// </summary>
    public async ValueTask<Snapshot.Snapshot> GetSnapshotAtVersionAsync(
        long version, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        return await SnapshotBuilder.BuildAsync(
            _log, _checkpointReader, atVersion: version, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Gets a snapshot at the latest version whose in-commit timestamp
    /// is at or before the specified timestamp.
    /// Requires <c>delta.enableInCommitTimestamps</c> to be enabled.
    /// </summary>
    public async ValueTask<Snapshot.Snapshot> GetSnapshotAtTimestampAsync(
        DateTimeOffset timestamp, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();

        long targetMs = timestamp.ToUnixTimeMilliseconds();

        // Scan commits to find the latest version at or before the target timestamp
        long? bestVersion = null;

        await foreach (long version in _log.ListVersionsAsync(0, cancellationToken)
            .ConfigureAwait(false))
        {
            var actions = await _log.ReadCommitAsync(version, cancellationToken)
                .ConfigureAwait(false);

            long? commitTs = Log.InCommitTimestamp.GetTimestampFromActions(actions);

            if (commitTs.HasValue && commitTs.Value <= targetMs)
                bestVersion = version;
            else if (commitTs.HasValue && commitTs.Value > targetMs)
                break; // Timestamps are monotonically increasing
        }

        if (bestVersion is null)
            throw new DeltaFormatException(
                "No commit found at or before the specified timestamp. " +
                "Ensure the table has in-commit timestamps enabled.");

        return await GetSnapshotAtVersionAsync(bestVersion.Value, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Reads data at the latest version whose in-commit timestamp
    /// is at or before the specified timestamp.
    /// </summary>
    public async IAsyncEnumerable<RecordBatch> ReadAtTimestampAsync(
        DateTimeOffset timestamp,
        IReadOnlyList<string>? columns = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        var snapshot = await GetSnapshotAtTimestampAsync(timestamp, cancellationToken)
            .ConfigureAwait(false);

        foreach (var addFile in snapshot.ActiveFiles.Values)
        {
            await foreach (var batch in ReadFileAsync(
                addFile, columns, snapshot, cancellationToken).ConfigureAwait(false))
            {
                yield return batch;
            }
        }
    }

    /// <summary>
    /// One row of the table's commit history. <see cref="TimestampMs"/> is the commit's inCommitTimestamp
    /// (or, without that feature, the commitInfo <c>timestamp</c> field) in epoch milliseconds — null only
    /// for a commit written before commitInfo became unconditional, or by a writer that omits it.
    /// <see cref="OperationParameters"/> is the raw JSON of <c>commitInfo.operationParameters</c>.
    /// </summary>
    public readonly record struct DeltaHistoryEntry(
        long Version, long? TimestampMs, string? Operation, string? OperationParameters);

    /// <summary>
    /// Enumerates the table's commit history — every version and its commitInfo — oldest first.
    /// Reads the Delta log only; no data files are opened.
    /// </summary>
    public async IAsyncEnumerable<DeltaHistoryEntry> GetHistoryAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        await foreach (long version in _log.ListVersionsAsync(0, cancellationToken).ConfigureAwait(false))
        {
            var actions = await _log.ReadCommitAsync(version, cancellationToken).ConfigureAwait(false);
            long? ts = null;
            string? op = null;
            string? opParams = null;
            foreach (var action in actions)
            {
                if (action is CommitInfo ci)
                {
                    // GetTimestamp prefers inCommitTimestamp and falls back to the standard `timestamp`.
                    ts = Log.InCommitTimestamp.GetTimestamp(ci);
                    if (ci.GetValue("operation") is { ValueKind: System.Text.Json.JsonValueKind.String } o)
                        op = o.GetString();
                    var p = ci.GetValue("operationParameters");
                    opParams = p.HasValue ? p.Value.GetRawText() : null;
                    break;
                }
            }
            yield return new DeltaHistoryEntry(version, ts, op, opParams);
        }
    }

    #region Delete and Update

    /// <summary>
    /// Deletes rows matching the predicate using deletion vectors.
    /// The predicate receives each batch (with logical column names) and returns
    /// a <see cref="BooleanArray"/> where <c>true</c> means the row should be deleted.
    /// Returns the number of rows deleted and the committed version.
    /// </summary>
    public async ValueTask<(long RowsDeleted, long Version)> DeleteAsync(
        Func<RecordBatch, BooleanArray> predicate,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ProtocolVersions.ValidateWriteSupport(CurrentSnapshot.Protocol);
        HonorWriterFeatures(isAppend: false); // DELETE is a data change

        var snapshot = CurrentSnapshot;
        var dvWriter = new DeletionVectors.DeletionVectorWriter(_fs);
        var actions = new List<DeltaAction>();
        long totalDeleted = 0;
        bool cdfEnabled = DeltaLake.ChangeDataFeed.CdfConfig.IsEnabled(
            snapshot.Metadata.Configuration);

        foreach (var addFile in snapshot.ActiveFiles.Values)
        {
            var rawDeletedRows = addFile.DeletionVector is not null
                ? await _dvReader.ReadAsync(addFile.DeletionVector, cancellationToken)
                    .ConfigureAwait(false)
                : new HashSet<long>();

            var newDeletedIndices = new List<long>();
            var deletedRowBatches = new List<RecordBatch>(); // For CDC
            long rowOffset = 0;

            await using var file = await _fs.OpenReadAsync(EngineeredWood.DeltaLake.DeltaPath.Decode(addFile.Path), cancellationToken)
                .ConfigureAwait(false);
            using var reader = new Parquet.ParquetFileReader(
                file, ownsFile: false, _options.ParquetReadOptions);

            var mappingMode = ColumnMapping.GetMode(snapshot.Metadata.Configuration);
            var physicalToLogical = ColumnMapping.BuildPhysicalToLogicalMap(
                snapshot.Schema, mappingMode);

            await foreach (var batch in reader.ReadAllAsync(
                cancellationToken: cancellationToken).ConfigureAwait(false))
            {
                var logicalBatch = ColumnMapping.RenameColumns(batch, physicalToLogical);
                if (ColumnMappingRecursive.HasNestedFields(snapshot.Schema))
                    logicalBatch = ColumnMappingRecursive.ToLogical(logicalBatch, snapshot.Schema, mappingMode);
                var mask = predicate(logicalBatch);
                var matchRows = new List<int>();

                for (int i = 0; i < batch.Length; i++)
                {
                    long absIdx = rowOffset + i;
                    if (rawDeletedRows.Contains(absIdx))
                        continue;

                    if (!mask.IsNull(i) && mask.GetValue(i) == true)
                    {
                        newDeletedIndices.Add(absIdx);
                        matchRows.Add(i);
                    }
                }

                // Collect deleted row data for CDC
                if (cdfEnabled && matchRows.Count > 0)
                    deletedRowBatches.Add(TakeRowsFromBatch(logicalBatch, matchRows));

                rowOffset += batch.Length;
            }

            if (newDeletedIndices.Count == 0)
                continue;

            var allDeleted = new HashSet<long>(rawDeletedRows);
            foreach (long idx in newDeletedIndices)
                allDeleted.Add(idx);

            totalDeleted += newDeletedIndices.Count;

            var newDv = await dvWriter.CreateAsync(
                allDeleted, allDeleted.Count, cancellationToken).ConfigureAwait(false);

            actions.Add(new RemoveFile
            {
                Path = addFile.Path,
                DeletionTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                DataChange = true,
                DeletionVector = addFile.DeletionVector,
            });

            actions.Add(addFile with
            {
                DeletionVector = newDv,
                DataChange = true,
            });

            // Write CDC file for deleted rows
            if (cdfEnabled)
            {
                foreach (var deletedBatch in deletedRowBatches)
                {
                    var cdcAction = await ChangeDataFeed.CdfWriter.WriteAsync(
                        _fs, deletedBatch, DeltaLake.ChangeDataFeed.CdfConfig.Delete,
                        addFile.PartitionValues, _options.ParquetWriteOptions,
                        cancellationToken).ConfigureAwait(false);
                    actions.Add(cdcAction);
                }
            }
        }

        if (actions.Count == 0)
            return (0, snapshot.Version);

        var finalActions = Log.InCommitTimestamp.EnsureCommitInfo(
            actions, snapshot.Metadata.Configuration, "DELETE");

        long newVersion = snapshot.Version + 1;
        await _log.WriteCommitAsync(newVersion, finalActions, cancellationToken)
            .ConfigureAwait(false);

        _currentSnapshot = await SnapshotBuilder.UpdateAsync(
            snapshot, _log, cancellationToken).ConfigureAwait(false);

        return (totalDeleted, newVersion);
    }

    /// <summary>
    /// Updates rows matching the predicate. The <paramref name="updater"/> function
    /// receives matching rows and returns modified rows. Non-matching rows are
    /// preserved unchanged. Affected files are rewritten.
    /// Returns the number of rows updated and the committed version.
    /// </summary>
    public async ValueTask<(long RowsUpdated, long Version)> UpdateAsync(
        Func<RecordBatch, BooleanArray> predicate,
        Func<RecordBatch, RecordBatch> updater,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ProtocolVersions.ValidateWriteSupport(CurrentSnapshot.Protocol);
        HonorWriterFeatures(isAppend: false); // UPDATE is a data change

        var snapshot = CurrentSnapshot;
        var actions = new List<DeltaAction>();
        long totalUpdated = 0;
        bool cdfEnabled = DeltaLake.ChangeDataFeed.CdfConfig.IsEnabled(
            snapshot.Metadata.Configuration);

        // ColumnMappingRecursive reads the physical names / field ids off the schema itself — no flat maps needed.
        var mappingMode = ColumnMapping.GetMode(snapshot.Metadata.Configuration);

        foreach (var addFile in snapshot.ActiveFiles.Values)
        {
            // Read file data with DV filtering
            var batches = new List<RecordBatch>();
            await foreach (var batch in ReadFileAsync(
                addFile, null, snapshot, cancellationToken).ConfigureAwait(false))
            {
                batches.Add(batch);
            }

            if (batches.Count == 0)
                continue;

            // Evaluate predicate and apply updates
            bool fileModified = false;
            var outputBatches = new List<RecordBatch>();
            var preimages = new List<RecordBatch>();
            var postimages = new List<RecordBatch>();

            foreach (var batch in batches)
            {
                var mask = predicate(batch);
                int matchCount = CountTrue(mask);

                if (matchCount == 0)
                {
                    outputBatches.Add(batch);
                    continue;
                }

                fileModified = true;
                totalUpdated += matchCount;

                var matchRows = new List<int>();
                var keepRows = new List<int>();

                for (int i = 0; i < batch.Length; i++)
                {
                    if (!mask.IsNull(i) && mask.GetValue(i) == true)
                        matchRows.Add(i);
                    else
                        keepRows.Add(i);
                }

                if (matchRows.Count > 0)
                {
                    var matchBatch = TakeRowsFromBatch(batch, matchRows);
                    var updatedBatch = updater(matchBatch);
                    outputBatches.Add(updatedBatch);

                    // Collect preimage and postimage for CDC
                    if (cdfEnabled)
                    {
                        preimages.Add(matchBatch);
                        postimages.Add(updatedBatch);
                    }
                }

                if (keepRows.Count > 0)
                    outputBatches.Add(TakeRowsFromBatch(batch, keepRows));
            }

            if (!fileModified)
                continue;

            // Write new file with all output batches
            string newFileName = $"{Guid.NewGuid():N}.parquet";
            long fileSize;

            // Physical names + parquet field ids at EVERY level (nested struct children included — the
            // top-level-only rename/stamp pair left them logical-named and id-less), then strip the internal
            // row-tracking column. Prepared up front so both the built-in and pluggable writers see the same
            // batches.
            var writeBatches = new List<RecordBatch>(outputBatches.Count);
            foreach (var batch in outputBatches)
            {
                var physicalBatch = ColumnMappingRecursive.ToPhysical(
                    batch, snapshot.Schema, mappingMode);
                var (cleanBatch, _) = RowTracking.RowTrackingWriter.StripRowIdColumn(physicalBatch);
                writeBatches.Add(cleanBatch);
            }

            if (_options.DataFileWriter is { } rewriteWriter)
            {
                fileSize = await rewriteWriter.WriteAsync(
                    writeBatches, newFileName, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                await using var file = await _fs.CreateAsync(
                    newFileName, cancellationToken: cancellationToken).ConfigureAwait(false);
                await using var writer = new Parquet.ParquetFileWriter(
                    file, ownsFile: false, _options.ParquetWriteOptions);

                foreach (var batch in writeBatches)
                {
                    await writer.WriteRowGroupAsync(batch, cancellationToken)
                        .ConfigureAwait(false);
                }

                await writer.DisposeAsync().ConfigureAwait(false);
                fileSize = file.Position;
            }

            string? stats = Stats.StatsCollector.Collect(outputBatches);

            // Remove old, add new
            actions.Add(new RemoveFile
            {
                Path = addFile.Path,
                DeletionTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                DataChange = true,
                ExtendedFileMetadata = true,
                PartitionValues = addFile.PartitionValues,
                Size = addFile.Size,
                // Keyed by (path, deletionVector) — see the Overwrite remove. The rewritten file already
                // has the DV's deletions applied, so the source must be removed under its DV-qualified key.
                DeletionVector = addFile.DeletionVector,
            });

            actions.Add(new AddFile
            {
                Path = EngineeredWood.DeltaLake.DeltaPath.Encode(newFileName),
                PartitionValues = addFile.PartitionValues,
                Size = fileSize,
                ModificationTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                DataChange = true,
                Stats = stats,
            });

            // Write CDC files for update preimage/postimage
            if (cdfEnabled)
            {
                foreach (var pre in preimages)
                {
                    var cdcAction = await ChangeDataFeed.CdfWriter.WriteAsync(
                        _fs, pre, DeltaLake.ChangeDataFeed.CdfConfig.UpdatePreimage,
                        addFile.PartitionValues, _options.ParquetWriteOptions,
                        cancellationToken).ConfigureAwait(false);
                    actions.Add(cdcAction);
                }
                foreach (var post in postimages)
                {
                    var cdcAction = await ChangeDataFeed.CdfWriter.WriteAsync(
                        _fs, post, DeltaLake.ChangeDataFeed.CdfConfig.UpdatePostimage,
                        addFile.PartitionValues, _options.ParquetWriteOptions,
                        cancellationToken).ConfigureAwait(false);
                    actions.Add(cdcAction);
                }
            }
        }

        if (actions.Count == 0)
            return (0, snapshot.Version);

        var finalActions = Log.InCommitTimestamp.EnsureCommitInfo(
            actions, snapshot.Metadata.Configuration, "UPDATE");

        long newVersion = snapshot.Version + 1;
        await _log.WriteCommitAsync(newVersion, finalActions, cancellationToken)
            .ConfigureAwait(false);

        _currentSnapshot = await SnapshotBuilder.UpdateAsync(
            snapshot, _log, cancellationToken).ConfigureAwait(false);

        return (totalUpdated, newVersion);
    }

    private static int CountTrue(BooleanArray mask)
    {
        int count = 0;
        for (int i = 0; i < mask.Length; i++)
            if (!mask.IsNull(i) && mask.GetValue(i) == true)
                count++;
        return count;
    }

    // The canonical identity of ONE partition (for dynamic partition overwrite set membership): the
    // sorted "key=value" pairs joined with U+0001, with every key translated to its PHYSICAL name when the
    // table has column mapping — so a physical-keyed entry (the spec convention) and a logical-keyed one
    // (older engineered-wood commits) canonicalize identically. A null value (Delta's "row is null in this
    // partition column") is marked distinctly from an empty string.
    internal static string CanonicalPartitionKey(
        IReadOnlyDictionary<string, string> values,
        IReadOnlyDictionary<string, string>? logicalToPhysical)
    {
        var parts = new List<string>(values.Count);
        foreach (var kv in values)
        {
            string key = logicalToPhysical is not null && logicalToPhysical.TryGetValue(kv.Key, out var phys)
                ? phys : kv.Key;
            parts.Add(key + "=" + (kv.Value is null ? "\u0000<null>" : kv.Value));
        }
        parts.Sort(StringComparer.Ordinal);
        return string.Join("\u0001", parts);
    }

    // True when `fileValues` matches every entry in `filter` (partition-overwrite file selection). A file matches
    // only if it carries each filter key with the exact same value (ordinal string compare — partition values are
    // stored as strings). Keys are validated to be partition columns before this is called. `filter` keys are the
    // user-facing LOGICAL names; under column mapping a file's partitionValues are keyed by the PHYSICAL name
    // (the Delta-spec convention — physical keys survive a partition-column rename), while files written before
    // that convention are logical-keyed — so each filter key is tried under BOTH names.
    private static bool PartitionValuesMatch(
        IReadOnlyDictionary<string, string> fileValues, IReadOnlyDictionary<string, string> filter,
        IReadOnlyDictionary<string, string>? logicalToPhysical = null)
    {
        foreach (var kv in filter)
        {
            if (!fileValues.TryGetValue(kv.Key, out var v)
                && (logicalToPhysical is null || !logicalToPhysical.TryGetValue(kv.Key, out var phys)
                    || !fileValues.TryGetValue(phys, out v)))
            {
                return false;
            }
            if (!string.Equals(v, kv.Value, StringComparison.Ordinal))
            {
                return false;
            }
        }
        return true;
    }

    private static RecordBatch TakeRowsFromBatch(RecordBatch batch, List<int> rows)
    {
        var columns = new IArrowArray[batch.ColumnCount];
        for (int col = 0; col < batch.ColumnCount; col++)
            columns[col] = DeletionVectors.DeletionVectorFilter.TakeRowsPublic(
                batch.Column(col), rows);
        return new RecordBatch(batch.Schema, columns, rows.Count);
    }

    #endregion

    /// <summary>
    /// Reads row-level changes between two versions using the Change Data Feed.
    /// Each batch includes <c>_change_type</c> ("insert", "delete", "update_preimage",
    /// "update_postimage"), <c>_commit_version</c>, and <c>_commit_timestamp</c> columns.
    /// For versions with CDC files, those are used directly. For versions without,
    /// changes are inferred from add/remove actions.
    /// </summary>
    public IAsyncEnumerable<RecordBatch> ReadChangesAsync(
        long startVersion, long endVersion,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        return ChangeDataFeed.CdfReader.ReadChangesAsync(
            _fs, _log, startVersion, endVersion,
            _options.ParquetReadOptions, cancellationToken);
    }

    /// <summary>
    /// Creates a log compaction file for a range of commits.
    /// Compacted files aggregate reconciled actions, allowing readers to
    /// skip individual commit files for faster snapshot construction.
    /// </summary>
    /// <param name="startVersion">Start of the commit range (inclusive).</param>
    /// <param name="endVersion">End of the commit range (inclusive). Must be greater than startVersion.</param>
    public async ValueTask CompactLogAsync(
        long startVersion, long endVersion,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        var logCompaction = new Log.LogCompaction(_fs, _log);
        await logCompaction.CompactRangeAsync(startVersion, endVersion, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Reads all data from the current snapshot as a stream of RecordBatches.
    /// </summary>
    public IAsyncEnumerable<RecordBatch> ReadAllAsync(
        IReadOnlyList<string>? columns = null,
        CancellationToken cancellationToken = default) =>
        ReadAllAsync(columns, filter: null, cancellationToken);

    /// <summary>
    /// Reads all data with an optional <see cref="EngineeredWood.Expressions.Predicate"/>
    /// filter. When set, files whose partition values or column statistics
    /// prove no rows can match are skipped before any data pages are read.
    /// The reader does NOT re-apply the predicate per row; callers wanting
    /// exact row-level filtering must do that on the returned batches.
    /// </summary>
    public async IAsyncEnumerable<RecordBatch> ReadAllAsync(
        IReadOnlyList<string>? columns,
        EngineeredWood.Expressions.Predicate? filter,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        var snapshot = CurrentSnapshot;
        var pruner = filter is null ? null : new DeltaFilePruner(
            snapshot.Schema, snapshot.Metadata.PartitionColumns);

        foreach (var addFile in snapshot.ActiveFiles.Values)
        {
            if (pruner is not null && !pruner.ShouldInclude(addFile, filter!))
                continue;

            await foreach (var batch in ReadFileAsync(
                addFile, columns, snapshot, cancellationToken).ConfigureAwait(false))
            {
                yield return batch;
            }
        }
    }

    /// <summary>
    /// Reads data from a specific version (time travel).
    /// </summary>
    public IAsyncEnumerable<RecordBatch> ReadAtVersionAsync(
        long version,
        IReadOnlyList<string>? columns = null,
        CancellationToken cancellationToken = default) =>
        ReadAtVersionAsync(version, columns, filter: null, cancellationToken);

    /// <summary>
    /// Reads data from a specific version with an optional filter predicate.
    /// See <see cref="ReadAllAsync(IReadOnlyList{string}, EngineeredWood.Expressions.Predicate, CancellationToken)"/>
    /// for filter semantics.
    /// </summary>
    public async IAsyncEnumerable<RecordBatch> ReadAtVersionAsync(
        long version,
        IReadOnlyList<string>? columns,
        EngineeredWood.Expressions.Predicate? filter,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        var snapshot = await GetSnapshotAtVersionAsync(version, cancellationToken)
            .ConfigureAwait(false);
        var pruner = filter is null ? null : new DeltaFilePruner(
            snapshot.Schema, snapshot.Metadata.PartitionColumns);

        foreach (var addFile in snapshot.ActiveFiles.Values)
        {
            if (pruner is not null && !pruner.ShouldInclude(addFile, filter!))
                continue;

            await foreach (var batch in ReadFileAsync(
                addFile, columns, snapshot, cancellationToken).ConfigureAwait(false))
            {
                yield return batch;
            }
        }
    }

    /// <summary>
    /// Writes RecordBatch data as a new commit.
    /// Returns the committed version number.
    /// </summary>
    /// <summary>
    /// Enforces the writer features a table ACTIVELY declares (Delta constraints are write-time only, so a
    /// violating commit would poison the table for every reader). <c>delta.appendOnly=true</c> blocks non-append
    /// data changes; <c>delta.constraints.*</c> / <c>delta.invariants</c> / <c>delta.generationExpression</c>
    /// carry arbitrary SQL this writer cannot evaluate, so an ACTIVE one rejects the write. A table that merely
    /// LISTS these features in its writer-v7 protocol (the common case) is unaffected.
    /// </summary>
    private void HonorWriterFeatures(bool isAppend)
    {
        var cfg = CurrentSnapshot.Metadata.Configuration;
        if (cfg is not null)
        {
            if (!isAppend && cfg.TryGetValue("delta.appendOnly", out var ao)
                && string.Equals(ao, "true", StringComparison.OrdinalIgnoreCase))
            {
                throw new DeltaFormatException(
                    "Table is append-only (delta.appendOnly=true): overwrite/delete/update are not permitted.");
            }
            foreach (var key in cfg.Keys)
            {
                if (key.StartsWith("delta.constraints.", StringComparison.Ordinal))
                {
                    throw new DeltaFormatException(
                        $"Table declares CHECK constraint '{key}' which this writer cannot evaluate; write rejected.");
                }
            }
        }
        foreach (var field in CurrentSnapshot.ArrowSchema.FieldsList)
        {
            if (field.Metadata is not null && field.Metadata.ContainsKey("delta.invariants"))
            {
                throw new DeltaFormatException(
                    $"Column '{field.Name}' declares an invariant expression this writer cannot evaluate; write rejected.");
            }
            if (field.Metadata is not null && field.Metadata.ContainsKey("delta.generationExpression"))
            {
                throw new DeltaFormatException(
                    $"Column '{field.Name}' declares a generation expression this writer cannot evaluate; write rejected.");
            }
        }
    }


    /// <summary>
    /// Writes RecordBatch data as a new commit.
    /// Returns the committed version number.
    /// <para><paramref name="repartitionTo"/> (Overwrite only): change the table's partition columns as part
    /// of the SAME atomic commit — the Delta-protocol-legal way to repartition (a new <c>metaData</c> with
    /// the new <c>partitionColumns</c> is only valid when every active file is removed in the same commit,
    /// which a full Overwrite does; Spark exposes this as <c>overwriteSchema=true</c> + a new
    /// <c>partitionBy</c>). The new data is Hive-split by the NEW columns. Ignored when equal to the current
    /// partitioning; empty list = departition.</para>
    /// </summary>
    public ValueTask<long> WriteAsync(
        IReadOnlyList<RecordBatch> batches,
        DeltaWriteMode mode = DeltaWriteMode.Append,
        CancellationToken cancellationToken = default,
        IReadOnlyList<string>? repartitionTo = null)
        => WriteCoreAsync(batches, mode, null, cancellationToken, repartitionTo: repartitionTo);

    /// <summary>
    /// Atomically overwrites one or more whole partitions in a SINGLE commit: removes exactly the active files
    /// whose partition values match every entry in <paramref name="overwritePartitions"/>, and adds
    /// <paramref name="batches"/> (which must fall within those partitions). This is delta-rs's static
    /// partition-overwrite / <c>replaceWhere</c>-on-partition-columns: the removal is file-exact (no rewrite)
    /// because the keys are partition columns, and the swap is one atomic Delta version. Files outside the target
    /// partitions are untouched. The keys must be partition columns of the table.
    /// </summary>
    public ValueTask<long> OverwritePartitionsAsync(
        IReadOnlyList<RecordBatch> batches,
        IReadOnlyDictionary<string, string> overwritePartitions,
        CancellationToken cancellationToken = default)
        => WriteCoreAsync(batches, DeltaWriteMode.Overwrite, overwritePartitions, cancellationToken);

    /// <summary>
    /// DYNAMIC partition overwrite (Spark <c>partitionOverwriteMode=dynamic</c>): atomically replaces exactly the
    /// partitions PRESENT IN <paramref name="batches"/> in a SINGLE commit — their currently-active files are
    /// removed and the new files added; partitions the input does not touch are unaffected. Unlike
    /// <see cref="OverwritePartitionsAsync"/> the target set is derived from the data, not supplied. Requires a
    /// partitioned table (throws otherwise — an unpartitioned "dynamic overwrite" would be a full replace in
    /// disguise; use Overwrite explicitly for that).
    /// </summary>
    public ValueTask<long> DynamicOverwriteAsync(
        IReadOnlyList<RecordBatch> batches,
        CancellationToken cancellationToken = default)
        => WriteCoreAsync(batches, DeltaWriteMode.Append, null, cancellationToken,
                          dynamicPartitionOverwrite: true);

    private async ValueTask<long> WriteCoreAsync(
        IReadOnlyList<RecordBatch> batches,
        DeltaWriteMode mode,
        IReadOnlyDictionary<string, string>? overwritePartitions,
        CancellationToken cancellationToken,
        bool dynamicPartitionOverwrite = false,
        IReadOnlyList<string>? repartitionTo = null)
    {
        ThrowIfDisposed();
        ProtocolVersions.ValidateWriteSupport(CurrentSnapshot.Protocol);
        // A dynamic partition overwrite removes files, so it is NOT an append for appendOnly enforcement.
        HonorWriterFeatures(mode == DeltaWriteMode.Append && !dynamicPartitionOverwrite);

        var snapshot = CurrentSnapshot;

        // Repartition-on-overwrite: changing partitionColumns is protocol-legal ONLY when every active file
        // is removed in the same commit — i.e. a FULL overwrite (a partition-scoped or dynamic overwrite
        // keeps files that would no longer conform to the new partition schema).
        bool repartitioned = false;
        if (repartitionTo is not null)
        {
            if (mode != DeltaWriteMode.Overwrite || overwritePartitions is { Count: > 0 } || dynamicPartitionOverwrite)
            {
                throw new DeltaFormatException(
                    "Repartitioning requires a FULL overwrite (the new partition schema is only valid when "
                    + "every active file is replaced in the same commit).");
            }
            foreach (var col in repartitionTo)
            {
                if (!snapshot.Schema.Fields.Any(f => f.Name == col))
                {
                    throw new DeltaFormatException(
                        $"Repartition: '{col}' is not a column of the table.");
                }
            }
            repartitioned = !repartitionTo.SequenceEqual(snapshot.Metadata.PartitionColumns);
        }

        if (dynamicPartitionOverwrite && snapshot.Metadata.PartitionColumns.Count == 0)
        {
            throw new DeltaFormatException(
                "Dynamic partition overwrite requires a partitioned table (the table has no partition columns).");
        }

        // A partition-overwrite: the filter keys MUST be partition columns so file-level removal is exact (a
        // data-column predicate could partially match a file → deleting the whole file would drop other rows).
        if (overwritePartitions is { Count: > 0 })
        {
            foreach (var key in overwritePartitions.Keys)
            {
                if (!snapshot.Metadata.PartitionColumns.Contains(key))
                {
                    throw new DeltaFormatException(
                        $"OverwritePartitions: '{key}' is not a partition column of the table " +
                        $"(partition columns: {string.Join(", ", snapshot.Metadata.PartitionColumns)}).");
                }
            }
        }

        // Iceberg compatibility: validate constraints before writing
        var icebergVersion = Schema.IcebergCompat.GetVersion(snapshot.Metadata.Configuration);
        if (icebergVersion != Schema.IcebergCompatVersion.None)
        {
            Schema.IcebergCompat.Validate(icebergVersion, snapshot.Metadata, snapshot.Protocol);
        }

        var actions = new List<DeltaAction>();

        // Column mapping: prepare logical-to-physical name mapping (also used to match/emit partitionValues,
        // which are keyed by the PHYSICAL column name under mapping — the Delta-spec convention).
        // A repartitioning overwrite splits by the NEW columns (the metaData swap is emitted below).
        var partitionColumns = repartitioned ? repartitionTo! : snapshot.Metadata.PartitionColumns;
        var mappingMode = ColumnMapping.GetMode(snapshot.Metadata.Configuration);
        var logicalToPhysical = ColumnMapping.BuildLogicalToPhysicalMap(
            snapshot.Schema, mappingMode);

        // Dynamic partition overwrite: collect the canonical partition keys the INPUT touches while writing;
        // the matching active files are removed after the write loop (one atomic commit).
        var touchedPartitions = dynamicPartitionOverwrite ? new HashSet<string>(StringComparer.Ordinal) : null;

        // For overwrite mode, remove existing files: ALL of them for a full overwrite, or only the files whose
        // partition values match `overwritePartitions` for an atomic partition-scoped overwrite (files outside
        // the target partitions are kept).
        if (mode == DeltaWriteMode.Overwrite)
        {
            long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            foreach (var existingFile in snapshot.ActiveFiles.Values)
            {
                if (overwritePartitions is { Count: > 0 } &&
                    !PartitionValuesMatch(existingFile.PartitionValues, overwritePartitions, logicalToPhysical))
                {
                    continue; // keep files outside the target partition(s)
                }
                actions.Add(new RemoveFile
                {
                    Path = existingFile.Path,
                    DeletionTimestamp = now,
                    DataChange = true,
                    ExtendedFileMetadata = true,
                    PartitionValues = existingFile.PartitionValues,
                    Size = existingFile.Size,
                    // Must match the ACTIVE (path, DV) entry: without the DV a remove of a
                    // deletion-vector-carrying file never reconciles and the file stays active forever
                    // (duplicated rows after an Overwrite of a DV-deleted table).
                    DeletionVector = existingFile.DeletionVector,
                });
            }
        }

        // Row tracking: prepare high water mark
        bool rowTrackingEnabled = DeltaLake.RowTracking.RowTrackingConfig.IsEnabled(
            snapshot.Metadata.Configuration);
        long nextRowId = rowTrackingEnabled ? snapshot.RowIdHighWaterMark : 0;
        long newVersion = snapshot.Version + 1;

        // Identity columns: prepare configs
        var identityConfigs = new Dictionary<string, IdentityColumnConfig>();
        foreach (var field in snapshot.Schema.Fields)
        {
            var config = IdentityColumn.GetConfig(field);
            if (config is not null)
                identityConfigs[field.Name] = config;
        }
        var allIdentityUpdates = new List<(string Name, long HighWaterMark)>();

        foreach (var batch in batches)
        {
            if (batch.Length == 0)
                continue;

            // Process identity columns: generate or validate values
            var processedBatch = batch;
            if (identityConfigs.Count > 0)
            {
                var (processed, updates) = IdentityColumns.IdentityColumnWriter.ProcessBatch(
                    batch, snapshot.Schema, ref identityConfigs);
                processedBatch = processed;
                allIdentityUpdates.AddRange(updates);
            }

            var partitions = Partitioning.PartitionUtils.SplitByPartition(
                processedBatch, partitionColumns);

            foreach (var (partValues, dataBatch) in partitions)
            {
                if (dataBatch.Length == 0)
                    continue;

                // Partition overwrite: the input must fall within the target partition(s) — otherwise we'd ADD
                // files in partitions we didn't clear, silently mixing overwrite + append semantics.
                if (overwritePartitions is { Count: > 0 } && !PartitionValuesMatch(partValues, overwritePartitions))
                {
                    throw new DeltaFormatException(
                        "OverwritePartitions: input data falls outside the target partition(s) " +
                        $"({string.Join(", ", overwritePartitions.Select(kv => kv.Key + "=" + kv.Value))}).");
                }

                // Rename logical columns to physical names + stamp field ids, at EVERY level (nested struct
                // children included — the top-level-only pair left them logical-named/id-less).
                var physicalBatch = ColumnMappingRecursive.ToPhysical(dataBatch, snapshot.Schema, mappingMode);

                // IcebergCompat: materialize partition columns into Parquet file
                if (Schema.IcebergCompat.RequiresPartitionMaterialization(icebergVersion) &&
                    partValues.Count > 0)
                {
                    physicalBatch = Partitioning.PartitionUtils.AppendPartitionColumns(
                        physicalBatch, partValues, snapshot.Schema, partitionColumns,
                        logicalToPhysical);
                }

                // (field ids already stamped recursively above; IcebergCompat-appended partition columns are
                // physical-named by AppendPartitionColumns and carry no mapping ids of their own)

                // Assign row IDs if row tracking is enabled
                long fileBaseRowId = nextRowId;
                if (rowTrackingEnabled)
                {
                    physicalBatch = RowTracking.RowTrackingWriter.AddRowIdColumn(
                        physicalBatch, fileBaseRowId);
                    nextRowId += dataBatch.Length;
                }

                // Under column mapping the tracked partitionValues are keyed by the PHYSICAL column name (Delta
                // spec: "track partition values with the physical name" — Spark does the same; physical keys
                // survive a partition-column RENAME, which never rewrites add actions). The Hive-style directory
                // follows the same (physical) keys; readers treat paths as opaque and take values from the log.
                var trackedPartValues = partValues;
                if (mappingMode != ColumnMappingMode.None && partValues.Count > 0)
                {
                    trackedPartValues = new Dictionary<string, string>(partValues.Count);
                    foreach (var kv in partValues)
                    {
                        trackedPartValues[logicalToPhysical.TryGetValue(kv.Key, out var p) ? p : kv.Key] = kv.Value;
                    }
                }

                touchedPartitions?.Add(CanonicalPartitionKey(trackedPartValues, logicalToPhysical));

                // Build file path: partition subdirectory + UUID filename
                string partDir = Partitioning.PartitionUtils.BuildPartitionPath(trackedPartValues);
                string fileName = string.IsNullOrEmpty(partDir)
                    ? $"{Guid.NewGuid():N}.parquet"
                    : $"{partDir}/{Guid.NewGuid():N}.parquet";

                long fileSize;

                if (_options.DataFileWriter is { } dataFileWriter)
                {
                    // Delegate the parquet bytes to the host writer; it places the file at the location the
                    // table filesystem maps `fileName` to and returns its byte size.
                    fileSize = await dataFileWriter.WriteAsync(
                        [physicalBatch], fileName, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    await using var file = await _fs.CreateAsync(
                        fileName, cancellationToken: cancellationToken).ConfigureAwait(false);
                    await using var writer = new ParquetFileWriter(
                        file, ownsFile: false, _options.ParquetWriteOptions);
                    await writer.WriteRowGroupAsync(physicalBatch, cancellationToken)
                        .ConfigureAwait(false);

                    // DisposeAsync writes the Parquet footer before we read Position
                    await writer.DisposeAsync().ConfigureAwait(false);
                    fileSize = file.Position;
                }

                // Collect stats from the data batch. Under column mapping the Delta-spec convention keys the
                // per-file stats by the PHYSICAL column names (matching what the streaming writer emits and what
                // spec readers use for data skipping) — collect over the top-level-renamed batch (stats cover
                // top-level primitives only, so the flat rename suffices).
                // IcebergCompat requires numRecords in stats regardless of options
                string? stats = null;
                if (_options.CollectStats ||
                    Schema.IcebergCompat.RequiresNumRecords(icebergVersion))
                    // Stats keys are PHYSICAL at every level under mapping (nested struct leaves included).
                    stats = CollectStats(ColumnMappingRecursive.ToPhysical(dataBatch, snapshot.Schema, mappingMode));

                actions.Add(new AddFile
                {
                    // add.path is the URL-encoded form of the on-disk relative path (spec / Spark).
                    Path = DeltaPath.Encode(fileName),
                    PartitionValues = trackedPartValues,
                    Size = fileSize,
                    ModificationTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                    DataChange = true,
                    Stats = stats,
                    BaseRowId = rowTrackingEnabled ? fileBaseRowId : null,
                    DefaultRowCommitVersion = rowTrackingEnabled ? newVersion : null,
                });
            }
        }

        // Dynamic partition overwrite: remove every currently-active file whose partition matches one the input
        // touched (canonical physical-keyed comparison, tolerating older logical-keyed commits). Files in
        // untouched partitions are kept. Same commit as the adds -> the swap is atomic per touched partition.
        if (touchedPartitions is { Count: > 0 })
        {
            long removeNow = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            foreach (var existingFile in snapshot.ActiveFiles.Values)
            {
                if (!touchedPartitions.Contains(CanonicalPartitionKey(existingFile.PartitionValues, logicalToPhysical)))
                    continue;
                actions.Add(new RemoveFile
                {
                    Path = existingFile.Path,
                    DeletionTimestamp = removeNow,
                    DataChange = true,
                    ExtendedFileMetadata = true,
                    PartitionValues = existingFile.PartitionValues,
                    Size = existingFile.Size,
                    DeletionVector = existingFile.DeletionVector, // match the active (path, DV) entry
                });
            }
        }

        // If identity columns were updated, emit metadata action with new HWMs. A commit must not carry two
        // conflicting metaData actions, so the identity metadata also carries a repartition's new
        // partitionColumns; a repartition WITHOUT identity updates emits its own metaData below.
        if (allIdentityUpdates.Count > 0)
        {
            var updatedSchema = snapshot.Schema;
            foreach (var (name, hwm) in allIdentityUpdates)
            {
                var updatedFields = updatedSchema.Fields.Select(f =>
                    f.Name == name ? IdentityColumn.UpdateHighWaterMark(f, hwm) : f).ToList();
                updatedSchema = new Schema.StructType { Fields = updatedFields };
            }

            string updatedSchemaString = DeltaSchemaSerializer.Serialize(updatedSchema);
            actions.Add(snapshot.Metadata with
            {
                SchemaString = updatedSchemaString,
                PartitionColumns = partitionColumns,
            });
        }
        else if (repartitioned)
        {
            // Repartition-on-overwrite: the new partitionColumns commit atomically with the full file swap —
            // every add in this commit already conforms to the new partition schema, every old file is
            // removed above, so no reader ever sees a nonconforming active file.
            actions.Add(snapshot.Metadata with { PartitionColumns = partitionColumns });
        }

        // Row tracking: persist the advanced high-water mark as the delta.rowTracking domainMetadata (the
        // spec-required source of truth; deriving it from active files alone under-counts after removes, so
        // a mixed writer could reassign already-used row ids).
        if (rowTrackingEnabled && nextRowId > snapshot.RowIdHighWaterMark)
        {
            actions.Add(DeltaLake.RowTracking.RowTrackingConfig.BuildHighWaterMarkAction(nextRowId));
        }

        // Prepend CommitInfo with inCommitTimestamp if enabled
        var finalActions = Log.InCommitTimestamp.EnsureCommitInfo(
            actions, snapshot.Metadata.Configuration, "WRITE");

        // Commit (newVersion computed earlier for row tracking)
        await _log.WriteCommitAsync(newVersion, finalActions, cancellationToken)
            .ConfigureAwait(false);

        // Refresh snapshot
        _currentSnapshot = await SnapshotBuilder.UpdateAsync(
            snapshot, _log, cancellationToken).ConfigureAwait(false);

        // Auto-checkpoint
        if (_options.CheckpointInterval > 0 &&
            newVersion % _options.CheckpointInterval == 0)
        {
            await _checkpointWriter.WriteCheckpointAsync(
                _currentSnapshot, cancellationToken).ConfigureAwait(false);
        }

        return newVersion;
    }

    /// <summary>
    /// Writes a stream of RecordBatch data as a new commit.
    /// </summary>
    public async ValueTask<long> WriteAsync(
        IAsyncEnumerable<RecordBatch> batches,
        DeltaWriteMode mode = DeltaWriteMode.Append,
        CancellationToken cancellationToken = default)
    {
        var batchList = new List<RecordBatch>();
        await foreach (var batch in batches.WithCancellation(cancellationToken)
            .ConfigureAwait(false))
        {
            batchList.Add(batch);
        }
        return await WriteAsync(batchList, mode, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Compacts small files into larger ones.
    /// Returns the committed version number, or null if no compaction was needed.
    /// </summary>
    public async ValueTask<long?> CompactAsync(
        CompactionOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ProtocolVersions.ValidateWriteSupport(CurrentSnapshot.Protocol);

        options ??= CompactionOptions.Default;
        var result = await Compaction.CompactionExecutor.ExecuteAsync(
            _fs, _log, CurrentSnapshot, options,
            _options.ParquetWriteOptions, _options.ParquetReadOptions,
            cancellationToken, _options.DataFileWriter, _options.DataFileReader)
            .ConfigureAwait(false);

        if (result.HasValue)
        {
            _currentSnapshot = await SnapshotBuilder.UpdateAsync(
                CurrentSnapshot, _log, cancellationToken).ConfigureAwait(false);
        }

        return result;
    }

    /// <summary>
    /// Deletes unreferenced data files older than the retention period.
    /// </summary>
    public async ValueTask<VacuumResult> VacuumAsync(
        TimeSpan? retentionPeriod = null,
        bool dryRun = false,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();

        // When vacuumProtocolCheck is enabled, validate we understand ALL features
        // before deleting any files — prevents deleting files needed by
        // features this implementation doesn't recognize.
        ProtocolVersions.ValidateVacuumSupport(CurrentSnapshot.Protocol);

        var retention = retentionPeriod ?? _options.VacuumRetention;
        return await Vacuum.VacuumExecutor.ExecuteAsync(
            _fs, _log, CurrentSnapshot, retention, dryRun, cancellationToken)
            .ConfigureAwait(false);
    }

    private async IAsyncEnumerable<RecordBatch> ReadFileAsync(
        AddFile addFile,
        IReadOnlyList<string>? columns,
        Snapshot.Snapshot snapshot,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var partitionColumns = snapshot.Metadata.PartitionColumns;
        bool hasPartitions = partitionColumns.Count > 0 &&
            addFile.PartitionValues.Count > 0;

        // Column mapping setup
        var mappingMode = ColumnMapping.GetMode(snapshot.Metadata.Configuration);
        bool isIdMode = mappingMode == ColumnMappingMode.Id;

        var logicalToPhysical = ColumnMapping.BuildLogicalToPhysicalMap(
            snapshot.Schema, mappingMode);
        var physicalToLogical = ColumnMapping.BuildPhysicalToLogicalMap(
            snapshot.Schema, mappingMode);
        var fieldIdToLogical = isIdMode
            ? ColumnMapping.BuildFieldIdToLogicalMap(snapshot.Schema)
            : null;

        // Load the deletion vector first — it is independent of the byte source.
        HashSet<long>? deletedRows = null;
        if (addFile.DeletionVector is not null)
        {
            deletedRows = await _dvReader.ReadAsync(
                addFile.DeletionVector, cancellationToken).ConfigureAwait(false);
        }

        if (_options.DataFileReader is { } dataFileReader)
        {
            // Pluggable codec read: raw physical batches in file order (DV rows included). Projection resolves
            // by PHYSICAL NAME in every mode — id-mode field-id resolution needs the parquet footer, which the
            // seam deliberately hides; Delta-spec files carry physicalName in BOTH modes, so name resolution is
            // exact for spec-written files. parquetSchema stays null, so the logical rename in the pipeline
            // falls to the (equivalent for spec files) name-based path.
            IReadOnlyList<string>? seamColumns = null;
            if (columns is not null)
            {
                var partSet = hasPartitions
                    ? new HashSet<string>(partitionColumns, StringComparer.Ordinal)
                    : new HashSet<string>();
                seamColumns = columns
                    .Where(c => !partSet.Contains(c))
                    .Select(c => logicalToPhysical.TryGetValue(c, out var p) ? p : c)
                    .ToList();
            }
            else if (hasPartitions)
            {
                var partSet = new HashSet<string>(partitionColumns, StringComparer.Ordinal);
                seamColumns = snapshot.Schema.Fields
                    .Where(f => !partSet.Contains(f.Name))
                    .Select(f => ColumnMapping.GetPhysicalName(f, mappingMode))
                    .ToList();
            }

            var seamBatches = dataFileReader.ReadAsync(
                EngineeredWood.DeltaLake.DeltaPath.Decode(addFile.Path), seamColumns, cancellationToken);
            await foreach (var processed in ProcessFileBatchesAsync(
                seamBatches, addFile, snapshot, columns, mappingMode, isIdMode, physicalToLogical,
                logicalToPhysical, fieldIdToLogical, parquetSchema: null, deletedRows, partitionColumns,
                hasPartitions, cancellationToken).ConfigureAwait(false))
            {
                yield return processed;
            }
            yield break;
        }

        // Open the file and read its Parquet schema for field_id resolution
        await using var file = await _fs.OpenReadAsync(EngineeredWood.DeltaLake.DeltaPath.Decode(addFile.Path), cancellationToken)
            .ConfigureAwait(false);
        using var reader = new ParquetFileReader(
            file, ownsFile: false, _options.ParquetReadOptions);

        Parquet.Schema.SchemaDescriptor? parquetSchema = null;

        // Determine which columns to request from the Parquet file
        IReadOnlyList<string>? fileColumns = null;

        if (isIdMode)
        {
            // In ID mode: resolve field_ids from the Parquet schema to column names
            parquetSchema = await reader.GetSchemaAsync(cancellationToken)
                .ConfigureAwait(false);

            if (columns is not null)
            {
                var partSet = hasPartitions
                    ? new HashSet<string>(partitionColumns, StringComparer.Ordinal)
                    : new HashSet<string>();
                var logicalToFieldId = ColumnMapping.BuildLogicalToFieldIdMap(snapshot.Schema);

                // Map logical names → field_ids → Parquet column names
                var fieldIds = columns
                    .Where(c => !partSet.Contains(c))
                    .Where(c => logicalToFieldId.ContainsKey(c))
                    .Select(c => logicalToFieldId[c])
                    .ToList();
                var resolved = parquetSchema.ResolveFieldIds(fieldIds);
                fileColumns = resolved.Where(n => n is not null).Select(n => n!).ToList();
            }
            // else: read all columns, rename by field_id after
        }
        else
        {
            // Name mode or None mode: translate by physical name
            fileColumns = columns;
            if (columns is not null)
            {
                var partSet = hasPartitions
                    ? new HashSet<string>(partitionColumns, StringComparer.Ordinal)
                    : new HashSet<string>();

                fileColumns = columns
                    .Where(c => !partSet.Contains(c))
                    .Select(c => logicalToPhysical.TryGetValue(c, out var p) ? p : c)
                    .ToList();
            }
            else if (hasPartitions)
            {
                var partSet = new HashSet<string>(partitionColumns, StringComparer.Ordinal);
                fileColumns = snapshot.Schema.Fields
                    .Where(f => !partSet.Contains(f.Name))
                    .Select(f => ColumnMapping.GetPhysicalName(f, mappingMode))
                    .ToList();
            }
        }

        if (parquetSchema is null && isIdMode)
        {
            parquetSchema = await reader.GetSchemaAsync(cancellationToken)
                .ConfigureAwait(false);
        }

        var builtinBatches = reader.ReadAllAsync(
            columnNames: fileColumns, cancellationToken: cancellationToken);
        await foreach (var processed in ProcessFileBatchesAsync(
            builtinBatches, addFile, snapshot, columns, mappingMode, isIdMode, physicalToLogical,
            logicalToPhysical, fieldIdToLogical, parquetSchema, deletedRows, partitionColumns,
            hasPartitions, cancellationToken).ConfigureAwait(false))
        {
            yield return processed;
        }
    }

    /// <summary>
    /// The per-batch read pipeline shared by the built-in <c>ParquetFileReader</c> and a pluggable
    /// <see cref="IDataFileReader"/>: everything ABOVE the raw decode. The source yields RAW batches —
    /// physical column names, file order, deletion-vector rows included — and this applies the logical
    /// rename, DV filtering, type widening, partition-column re-add, row-tracking strip, and the
    /// schema-evolution backfill. Position-keyed steps (DV filtering) depend on the source preserving
    /// file order, which is part of the <see cref="IDataFileReader"/> contract.
    /// </summary>
    private async IAsyncEnumerable<RecordBatch> ProcessFileBatchesAsync(
        IAsyncEnumerable<RecordBatch> source,
        AddFile addFile,
        Snapshot.Snapshot snapshot,
        IReadOnlyList<string>? columns,
        ColumnMappingMode mappingMode,
        bool isIdMode,
        Dictionary<string, string> physicalToLogical,
        Dictionary<string, string> logicalToPhysical,
        Dictionary<int, string>? fieldIdToLogical,
        Parquet.Schema.SchemaDescriptor? parquetSchema,
        HashSet<long>? deletedRows,
        IReadOnlyList<string> partitionColumns,
        bool hasPartitions,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        long batchStartRow = 0;

        await foreach (var batch in source.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            // Rename columns back to logical names (flat, top level), then recursively for nested struct
            // children (the flat renames leave them under their physical names).
            RecordBatch result;
            if (isIdMode && fieldIdToLogical is not null && parquetSchema is not null)
            {
                result = ColumnMapping.RenameByFieldId(batch, fieldIdToLogical, parquetSchema);
            }
            else
            {
                result = ColumnMapping.RenameColumns(batch, physicalToLogical);
            }
            if (ColumnMappingRecursive.HasNestedFields(snapshot.Schema))
            {
                result = ColumnMappingRecursive.ToLogical(result, snapshot.Schema, mappingMode);
            }

            // Apply deletion vector filtering
            if (deletedRows is not null)
            {
                result = DeletionVectorFilter.Filter(result, deletedRows, batchStartRow);
                batchStartRow += batch.Length;

                if (result.Length == 0)
                    continue; // All rows in this batch were deleted
            }

            // Apply type widening — convert narrow types from old files to current schema types
            if (Schema.TypeWidening.IsEnabled(snapshot.Metadata.Configuration) ||
                HasTypeChanges(snapshot.Schema))
            {
                var targetSchema = columns is not null
                    ? BuildProjectedSchema(snapshot.ArrowSchema, columns,
                        hasPartitions ? partitionColumns : null)
                    : BuildNonPartitionSchema(snapshot.ArrowSchema, partitionColumns);

                result = TypeWidening.ValueWidener.WidenBatch(result, targetSchema);
            }

            if (hasPartitions)
            {
                // Re-add partition columns as constant arrays
                var fullSchema = columns is not null
                    ? BuildProjectedSchema(snapshot.ArrowSchema, columns)
                    : snapshot.ArrowSchema;

                // partitionValues are keyed by the PHYSICAL column name under mapping (the spec convention),
                // while files written before that convention are logical-keyed — the map resolves both.
                result = Partitioning.PartitionUtils.AddPartitionColumns(
                    result, fullSchema, addFile.PartitionValues, partitionColumns, logicalToPhysical);
            }

            // Strip internal row tracking column if present
            var (cleanResult, _) = RowTracking.RowTrackingWriter.StripRowIdColumn(result);

            // Schema evolution: ADD/DROP COLUMN are metadata-only commits, so a file written before an ADD
            // lacks the column and one written before a DROP still carries it — reconcile every emitted batch
            // to the current schema's expected output columns (absent ones backfilled as typed all-NULL).
            var expectedFields = (columns is not null
                ? BuildProjectedSchema(snapshot.ArrowSchema, columns)
                : snapshot.ArrowSchema).FieldsList;
            cleanResult = SchemaEvolution.BackfillMissingColumns(cleanResult, expectedFields);

            yield return cleanResult;
        }
    }

    private static Apache.Arrow.Schema BuildProjectedSchema(
        Apache.Arrow.Schema fullSchema, IReadOnlyList<string> columns)
    {
        var colSet = new HashSet<string>(columns, StringComparer.Ordinal);
        var builder = new Apache.Arrow.Schema.Builder();
        foreach (var field in fullSchema.FieldsList)
        {
            if (colSet.Contains(field.Name))
                builder.Field(field);
        }
        return builder.Build();
    }

    private static Apache.Arrow.Schema BuildProjectedSchema(
        Apache.Arrow.Schema fullSchema, IReadOnlyList<string> columns,
        IReadOnlyList<string>? excludeColumns)
    {
        var colSet = new HashSet<string>(columns, StringComparer.Ordinal);
        var excludeSet = excludeColumns is not null
            ? new HashSet<string>(excludeColumns, StringComparer.Ordinal)
            : new HashSet<string>();
        var builder = new Apache.Arrow.Schema.Builder();
        foreach (var field in fullSchema.FieldsList)
        {
            if (colSet.Contains(field.Name) && !excludeSet.Contains(field.Name))
                builder.Field(field);
        }
        return builder.Build();
    }

    private static Apache.Arrow.Schema BuildNonPartitionSchema(
        Apache.Arrow.Schema fullSchema, IReadOnlyList<string> partitionColumns)
    {
        if (partitionColumns.Count == 0)
            return fullSchema;

        var partSet = new HashSet<string>(partitionColumns, StringComparer.Ordinal);
        var builder = new Apache.Arrow.Schema.Builder();
        foreach (var field in fullSchema.FieldsList)
        {
            if (!partSet.Contains(field.Name))
                builder.Field(field);
        }
        return builder.Build();
    }

    private static bool HasTypeChanges(EngineeredWood.DeltaLake.Schema.StructType schema)
    {
        foreach (var field in schema.Fields)
        {
            if (field.Metadata is not null &&
                field.Metadata.ContainsKey(EngineeredWood.DeltaLake.Schema.TypeWidening.TypeChangesKey))
                return true;
        }
        return false;
    }

    private static string? CollectStats(RecordBatch batch) =>
        Stats.StatsCollector.Collect(batch);

    private void ThrowIfDisposed()
    {
#if NET8_0_OR_GREATER
        ObjectDisposedException.ThrowIf(_disposed, this);
#else
        if (_disposed) throw new ObjectDisposedException(GetType().FullName);
#endif
    }

    public void Dispose()
    {
        _disposed = true;
    }

    public ValueTask DisposeAsync()
    {
        _disposed = true;
        return default;
    }
}
