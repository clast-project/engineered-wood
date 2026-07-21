// Copyright (c) clast-project. All rights reserved.
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
        _dataFileReadOptions = WithVariantExtension(options.ParquetReadOptions);
        _log = new TransactionLog(fileSystem);
        _checkpointReader = new CheckpointReader(fileSystem);
        _dvReader = new DeletionVectorReader(fileSystem);
        _checkpointWriter = new CheckpointWriter(fileSystem, options.ParquetWriteOptions);
        _currentSnapshot = snapshot;
    }

    /// <summary>
    /// The read options used for DATA files: the caller's options with the
    /// <c>arrow.parquet.variant</c> extension guaranteed to be registered.
    /// <para>Delta's <c>variant</c> type maps to that Arrow extension, and the parquet reader only
    /// materialises it (reassembling any shredding) when its registry knows it — with no registry a
    /// VARIANT-annotated group decodes as a bare <c>struct&lt;metadata, value&gt;</c>, which would not
    /// match the table's declared schema. Registering it is therefore a correctness requirement here,
    /// not a caller preference. A caller-supplied registry is CLONED rather than mutated, and any
    /// other extensions it carries are preserved.</para>
    /// <para>Applies to data files only; log and checkpoint parquet never contains variant.</para>
    /// </summary>
    private readonly ParquetReadOptions _dataFileReadOptions;

    private static ParquetReadOptions WithVariantExtension(ParquetReadOptions options)
    {
        var registry = options.ExtensionRegistry;
        if (registry is not null
            && registry.TryGetDefinition(VariantExtensionDefinition.Instance.ExtensionName, out _))
        {
            return options; // already registered — nothing to do
        }

        var augmented = registry?.Clone() ?? new ExtensionTypeRegistry();
        augmented.Register(VariantExtensionDefinition.Instance);
        return options with { ExtensionRegistry = augmented };
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
        IReadOnlyList<string>? clusteringColumns = null,
        bool enableDeletionVectors = false,
        bool enableRowTracking = false,
        CancellationToken cancellationToken = default)
    {
        options ??= DeltaTableOptions.Default;
        var log = new TransactionLog(fileSystem);

        // Liquid clustering and partitioning are mutually exclusive (Spark's CLUSTER BY REPLACES
        // PARTITIONED BY; no engine creates a table carrying both, so readers' clustering-info resolution
        // is undefined on the combination). A partitioned table can still be physically SORTED at write
        // time — it just must not DECLARE clustering.
        if (clusteringColumns is { Count: > 0 } && partitionColumns is { Count: > 0 })
        {
            throw new DeltaFormatException(
                "Liquid clustering and partitioning are mutually exclusive — a partitioned table cannot "
                + "declare clustering columns.");
        }

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

        // Deletion vectors are opt-in: set the table property so the DELETE path knows it may soft-delete
        // rows with a DV, and declare the reader+writer feature below so foreign readers apply them.
        if (enableDeletionVectors)
        {
            configuration ??= new Dictionary<string, string>();
            configuration[DeletionVectors.DeletionVectorConfig.EnableKey] = "true";
        }

        // Row tracking is opt-in: set the property and store the two spec-required hidden column names now
        // (they are fixed at enablement — a reader consults them to find the materialized id/version columns
        // an eventual rewrite writes). Fresh appends need neither column: a row's id is baseRowId + position.
        // The rowTracking + domainMetadata writer features are declared below.
        if (enableRowTracking)
        {
            configuration ??= new Dictionary<string, string>();
            configuration[DeltaLake.RowTracking.RowTrackingConfig.EnableKey] = "true";
            var (rowIdCol, rowCommitVersionCol) =
                DeltaLake.RowTracking.RowTrackingConfig.GenerateMaterializedColumnNames();
            configuration[DeltaLake.RowTracking.RowTrackingConfig.MaterializedRowIdColumnNameKey] = rowIdCol;
            configuration[DeltaLake.RowTracking.RowTrackingConfig.MaterializedRowCommitVersionColumnNameKey] =
                rowCommitVersionCol;
        }

        // The legacy protocol versions this table would carry if NO table feature forced it into
        // table-features mode. Captured before any feature escalates the versions below, because
        // switching to reader 3 / writer 7 means every capability the legacy versions IMPLIED must be
        // spelled out explicitly -- see the merge just before the ProtocolAction is built.
        int legacyReaderVersion = minReaderVersion;
        int legacyWriterVersion = minWriterVersion;

        // Schema-driven table features must be DECLARED at creation, else a strict reader (Spark,
        // delta-kernel) rejects the table with "feature enabled in metadata but not listed in protocol".
        var readerFeatures = new List<string>();
        var writerFeatures = new List<string>();

        // Schema-driven READER+WRITER features — currently 'timestampNtz' (a naive TIMESTAMP column) and
        // 'variantType' (a variant column), each required by the spec whenever the type appears at any
        // nesting depth. Both are reader-3 / writer-7 named features, so either puts the table in
        // table-features mode. This shares RequiredSchemaFeatures with the ALTER path
        // (AddColumnAsync/SetSchemaAsync) deliberately: when the two were separate, adding a type here
        // meant remembering to add it there too, and variant support was written against the ALTER path
        // while CREATE silently kept emitting a legacy protocol.
        foreach (var feature in RequiredSchemaFeatures(deltaSchema))
        {
            minReaderVersion = 3;
            minWriterVersion = 7;
            readerFeatures.Add(feature);
            writerFeatures.Add(feature);
        }

        // Deletion vectors — an opt-in reader+writer feature (reader 3 / writer 7). Declaring it is what
        // makes a conformant foreign reader APPLY the DVs a DELETE writes; without it they are silently
        // ignored (a reader returns rows the table considers deleted). The DELETE path refuses to write a DV
        // unless this feature is enabled — see ComputeDeleteActionsAsync.
        if (enableDeletionVectors)
        {
            minReaderVersion = 3;
            minWriterVersion = 7;
            readerFeatures.Add(DeletionVectors.DeletionVectorConfig.FeatureName);
            writerFeatures.Add(DeletionVectors.DeletionVectorConfig.FeatureName);
        }

        // Row tracking — a WRITER-only feature ('rowTracking', writer 7) that depends on the 'domainMetadata'
        // writer feature (the row-id high-water mark rides the delta.rowTracking system domain). Readers see
        // ordinary data plus optional add.baseRowId metadata, so the reader version is untouched. The append
        // path assigns baseRowId + defaultRowCommitVersion and advances the HWM domain; a copy-on-write rewrite
        // (UPDATE / OVERWRITE / compaction) materializes each moved row's original id + commit version.
        if (enableRowTracking)
        {
            minWriterVersion = 7;
            writerFeatures.Add("rowTracking");
            if (!writerFeatures.Contains("domainMetadata"))
                writerFeatures.Add("domainMetadata");
        }

        // Identity columns (delta.identity.* field metadata) are a WRITER-only feature ('identityColumns',
        // legacy writer v6) — readers see an ordinary long column — so the reader version is untouched.
        if (deltaSchema.Fields.Any(IdentityColumn.IsIdentityColumn))
        {
            minWriterVersion = 7;
            writerFeatures.Add("identityColumns");
        }

        // Clustered (liquid-clustering) table: a WRITER-only feature ('clustering' — readers read normally)
        // whose clustering-columns spec rides the delta.clustering system domain, so domainMetadata is a
        // dependency. The domain action joins commit 0 below. This library does not WRITE clustered layouts;
        // a clustering engine's OPTIMIZE (Spark) uses the declaration to (re)cluster.
        if (clusteringColumns is { Count: > 0 })
        {
            minWriterVersion = 7;
            writerFeatures.Add("clustering");
            if (!writerFeatures.Contains("domainMetadata"))
                writerFeatures.Add("domainMetadata");
        }

        // Column mapping is BOTH a reader and writer feature. Once any other feature has forced
        // table-features mode (reader v3 / writer v7) it MUST be listed in BOTH lists too — a v7 protocol
        // with no columnMapping entry reads as "column mapping not supported".
        //
        // Absent any other feature we emit the legacy pair (reader v2 / writer v5, no lists). NOTE that
        // this is NOT what Spark writes: measured against delta-spark 4.0.0, Spark emits a hybrid --
        // reader 2 (legacy) with writer 7 and writerFeatures [columnMapping, invariants, appendOnly].
        // Both are spec-legal and Spark reads ours (SparkInteropTests covers it); the difference is
        // cosmetic, because writer v5's extra implied features only impose obligations on tables that
        // actually declare a constraint or generated column, and HonorWriterFeatures already fails
        // closed on those. See doc/upstream-landing-notes.md for the full measurement.
        if (columnMappingMode != ColumnMappingMode.None &&
            (minReaderVersion >= 3 || minWriterVersion >= 7))
        {
            minReaderVersion = 3;
            minWriterVersion = 7;
            readerFeatures.Add("columnMapping");
            writerFeatures.Add("columnMapping");
        }

        // Table-features mode is all-or-nothing: at writer 7 / reader 3 there are no implicit
        // capabilities left, so every feature the LEGACY versions implied must be listed or the table
        // is self-inconsistent. Spark rejects it outright --
        //   DELTA_FEATURES_PROTOCOL_METADATA_MISMATCH: ... enabled in metadata but not listed in
        //   protocol: invariants
        // -- which is exactly what happened to every clustered table this library wrote, because
        // clustering forces writer 7 from a writer-2 baseline whose implied appendOnly/invariants were
        // then dropped on the floor. UpgradeProtocolForFeatures already does this for ALTER; creation
        // has to do it too.
        if (minWriterVersion >= 7)
        {
            foreach (string feature in LegacyWriterFeatures(legacyWriterVersion))
            {
                if (!writerFeatures.Contains(feature))
                    writerFeatures.Add(feature);
            }
        }

        if (minReaderVersion >= 3)
        {
            foreach (string feature in LegacyReaderFeatures(legacyReaderVersion))
            {
                if (!readerFeatures.Contains(feature))
                    readerFeatures.Add(feature);
            }
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

        if (clusteringColumns is { Count: > 0 })
        {
            actions.Add(BuildClusteringDomain(deltaSchema, clusteringColumns, columnMappingMode));
        }

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
        IReadOnlyList<string>? clusteringColumns = null,
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
            clusteringColumns: clusteringColumns,
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

    // ── Buffered-transaction schema seam ───────────────────────────────────────────────────────────────
    //
    // The Compute* family is the COMPUTE-ONLY counterpart of the schema ALTERs: each builds the metaData
    // (+ optional protocol upgrade) actions WITHOUT committing, so a buffered multi-statement transaction can
    // fuse a schema change with its data changes into ONE atomic commit (via CommitDataFilesAsync' extraActions).
    // Chained ALTERs pass the previous change's baseMetadata/baseProtocol so the second composes on the first's
    // PENDING schema — the fused commit then carries only the final metaData (a commit must not carry two).

    /// <summary>The deferred (compute-only) form of a schema change, for a buffered multi-statement transaction:
    /// <see cref="Actions"/> = the optional protocol upgrade + the new <c>metaData</c> action, to be fused into
    /// ONE commit via <see cref="CommitDataFilesAsync"/>' <c>extraActions</c>; <see cref="Metadata"/> /
    /// <see cref="ProtocolUpgrade"/> are the pending base for a CHAINED next change; <see cref="NewSchema"/> is
    /// the parsed new Delta schema (drives the caller's read overlays and schema-overridden writes).</summary>
    public readonly record struct DeferredSchemaChange(
        IReadOnlyList<DeltaAction> Actions,
        MetadataAction Metadata,
        ProtocolAction? ProtocolUpgrade,
        StructType NewSchema);

    /// <summary>
    /// The compute-only counterpart of <see cref="AddColumnAsync"/>: builds the metaData (+ protocol upgrade)
    /// actions for appending a nullable column WITHOUT committing. For CHAINED adds in one transaction pass the
    /// previous change's <paramref name="baseMetadata"/> / <paramref name="baseProtocol"/> so the second column
    /// composes on the first's pending schema/protocol. Pure computation, no IO.
    /// </summary>
    public DeferredSchemaChange ComputeAddColumn(
        Field newColumn, MetadataAction? baseMetadata = null, ProtocolAction? baseProtocol = null)
    {
        ThrowIfDisposed();
        ProtocolVersions.ValidateWriteSupport(CurrentSnapshot.Protocol);

        if (!newColumn.IsNullable)
            throw new InvalidOperationException(
                $"ADD COLUMN '{newColumn.Name}' must be nullable — existing rows have no value for a new column.");

        var snapshot = CurrentSnapshot;
        var baseMeta = baseMetadata ?? snapshot.Metadata;
        var baseSchema = baseMetadata is null
            ? snapshot.Schema
            : DeltaSchemaSerializer.Parse(baseMetadata.SchemaString);
        var config = baseMeta.Configuration;
        var mappingMode = ColumnMapping.GetMode(config);

        foreach (var f in baseSchema.Fields)
        {
            if (string.Equals(f.Name, newColumn.Name, StringComparison.Ordinal))
                throw new InvalidOperationException($"Column '{newColumn.Name}' already exists.");
        }

        var newDeltaField = SchemaConverter.FromArrowSchema(
            new Apache.Arrow.Schema([newColumn], null)).Fields[0];

        StructType newSchema;
        var newConfig = config;
        if (mappingMode == ColumnMappingMode.None)
        {
            newSchema = new StructType { Fields = new List<StructField>(baseSchema.Fields) { newDeltaField } };
        }
        else
        {
            // Fresh column id + physical name, recursively, continuing past the base's maxColumnId (the base
            // may itself be a pending change that already bumped it).
            var (mappedField, lastId) = AssignMappedField(baseSchema, config, newDeltaField);
            newSchema = new StructType { Fields = new List<StructField>(baseSchema.Fields) { mappedField } };
            var cfg = config is null
                ? new Dictionary<string, string>()
                : config.ToDictionary(kv => kv.Key, kv => kv.Value);
            cfg[ColumnMapping.MaxColumnIdKey] = lastId.ToString();
            newConfig = cfg;
        }

        var protocolUpgrade = UpgradeProtocolForFeatures(
            baseProtocol ?? snapshot.Protocol, RequiredSchemaFeatures(newDeltaField.Type));

        var metadata = baseMeta with
        {
            SchemaString = DeltaSchemaSerializer.Serialize(newSchema),
            Configuration = newConfig,
        };
        var actions = new List<DeltaAction>();
        if (protocolUpgrade is not null)
            actions.Add(protocolUpgrade);
        actions.Add(metadata);
        return new DeferredSchemaChange(actions, metadata, protocolUpgrade, newSchema);
    }

    /// <summary>The compute-only counterpart of <see cref="RenameColumnAsync"/> — for a buffered transaction.
    /// Requires column mapping (checked against the base config). The renamed field keeps its column id +
    /// physical name; a renamed PARTITION column also updates <c>metaData.partitionColumns</c>. No protocol
    /// change.</summary>
    public DeferredSchemaChange ComputeRenameColumn(
        string oldName, string newName, MetadataAction? baseMetadata = null)
    {
        ThrowIfDisposed();
        ProtocolVersions.ValidateWriteSupport(CurrentSnapshot.Protocol);

        var snapshot = CurrentSnapshot;
        var baseMeta = baseMetadata ?? snapshot.Metadata;
        var baseSchema = baseMetadata is null
            ? snapshot.Schema
            : DeltaSchemaSerializer.Parse(baseMetadata.SchemaString);
        if (ColumnMapping.GetMode(baseMeta.Configuration) == ColumnMappingMode.None)
        {
            throw new InvalidOperationException(
                "RENAME COLUMN requires column mapping (enable it at table creation) — a plain table would need "
                + "to rewrite every data file since the logical name is the physical parquet column name.");
        }

        StructField? target = null;
        foreach (var f in baseSchema.Fields)
        {
            if (string.Equals(f.Name, newName, StringComparison.Ordinal))
                throw new InvalidOperationException($"Column '{newName}' already exists.");
            if (string.Equals(f.Name, oldName, StringComparison.Ordinal))
                target = f;
        }
        if (target is null)
            throw new InvalidOperationException($"Column '{oldName}' does not exist.");

        var newFields = new List<StructField>(baseSchema.Fields.Count);
        foreach (var f in baseSchema.Fields)
        {
            newFields.Add(ReferenceEquals(f, target)
                ? new StructField { Name = newName, Type = f.Type, Nullable = f.Nullable, Metadata = f.Metadata }
                : f);
        }
        var newSchema = new StructType { Fields = newFields };

        var newPartitionColumns = baseMeta.PartitionColumns;
        if (newPartitionColumns.Contains(oldName))
        {
            newPartitionColumns = newPartitionColumns
                .Select(pc => string.Equals(pc, oldName, StringComparison.Ordinal) ? newName : pc)
                .ToList();
        }

        var metadata = baseMeta with
        {
            SchemaString = DeltaSchemaSerializer.Serialize(newSchema),
            PartitionColumns = newPartitionColumns,
        };
        return new DeferredSchemaChange(new List<DeltaAction> { metadata }, metadata, null, newSchema);
    }

    /// <summary>The compute-only counterpart of <see cref="DropColumnAsync"/> — for a buffered transaction.
    /// Requires column mapping; partition columns and the last column are rejected. No protocol change.</summary>
    public DeferredSchemaChange ComputeDropColumn(string name, MetadataAction? baseMetadata = null)
    {
        ThrowIfDisposed();
        ProtocolVersions.ValidateWriteSupport(CurrentSnapshot.Protocol);

        var snapshot = CurrentSnapshot;
        var baseMeta = baseMetadata ?? snapshot.Metadata;
        var baseSchema = baseMetadata is null
            ? snapshot.Schema
            : DeltaSchemaSerializer.Parse(baseMetadata.SchemaString);
        if (ColumnMapping.GetMode(baseMeta.Configuration) == ColumnMappingMode.None)
        {
            throw new InvalidOperationException(
                "DROP COLUMN requires column mapping (enable it at table creation) — a plain table would need "
                + "to rewrite every data file since the logical name is the physical parquet column name.");
        }
        foreach (var pc in baseMeta.PartitionColumns)
        {
            if (string.Equals(pc, name, StringComparison.Ordinal))
                throw new InvalidOperationException($"Cannot drop partition column '{name}'.");
        }

        var newFields = new List<StructField>(baseSchema.Fields.Count);
        bool found = false;
        foreach (var f in baseSchema.Fields)
        {
            if (string.Equals(f.Name, name, StringComparison.Ordinal)) { found = true; continue; }
            newFields.Add(f);
        }
        if (!found)
            throw new InvalidOperationException($"Column '{name}' does not exist.");
        if (newFields.Count == 0)
            throw new InvalidOperationException("Cannot drop the table's only column.");
        var newSchema = new StructType { Fields = newFields };

        var metadata = baseMeta with { SchemaString = DeltaSchemaSerializer.Serialize(newSchema) };
        return new DeferredSchemaChange(new List<DeltaAction> { metadata }, metadata, null, newSchema);
    }

    /// <summary>The compute-only counterpart of <see cref="AddFieldAsync"/> (nested ADD) — for a buffered
    /// transaction. <paramref name="containerPath"/> names the CONTAINING struct (every segment must resolve to
    /// a struct). For CHAINED changes pass the previous change's <paramref name="baseMetadata"/> /
    /// <paramref name="baseProtocol"/> so this composes on the pending schema/protocol. Under column mapping the
    /// new field gets fresh recursive ids continuing past the base's <c>maxColumnId</c>; it may carry a protocol
    /// upgrade for a schema-driven feature (timestampNtz / variantType). Pure computation, no IO.</summary>
    public DeferredSchemaChange ComputeAddField(
        IReadOnlyList<string> containerPath, Field newField,
        MetadataAction? baseMetadata = null, ProtocolAction? baseProtocol = null)
    {
        ThrowIfDisposed();
        ProtocolVersions.ValidateWriteSupport(CurrentSnapshot.Protocol);
        if (containerPath.Count == 0)
            throw new ArgumentException(
                "containerPath must name the containing struct column.", nameof(containerPath));
        if (!newField.IsNullable)
            throw new InvalidOperationException(
                $"ADD COLUMN '{PathText(containerPath)}.{newField.Name}' must be nullable — existing rows have "
                + "no value for a new field.");

        var snapshot = CurrentSnapshot;
        var baseMeta = baseMetadata ?? snapshot.Metadata;
        var baseSchema = baseMetadata is null
            ? snapshot.Schema
            : DeltaSchemaSerializer.Parse(baseMetadata.SchemaString);
        var config = baseMeta.Configuration;
        var mappingMode = ColumnMapping.GetMode(config);

        var newDeltaField = SchemaConverter.FromArrowSchema(
            new Apache.Arrow.Schema([newField], null)).Fields[0];

        var newConfig = config;
        if (mappingMode != ColumnMappingMode.None)
        {
            // Fresh recursive ids + physical names, continuing past the base's maxColumnId (the base may itself
            // be a pending change that already bumped it) — struct/array/map descendants each get their own id.
            var (mappedField, lastId) = AssignMappedField(baseSchema, config, newDeltaField);
            newDeltaField = mappedField;
            var cfg = config is null
                ? new Dictionary<string, string>()
                : config.ToDictionary(kv => kv.Key, kv => kv.Value);
            cfg[ColumnMapping.MaxColumnIdKey] = lastId.ToString();
            newConfig = cfg;
        }

        var addedField = newDeltaField;
        var newSchema = TransformStructAt(baseSchema, containerPath, 0, fields =>
        {
            foreach (var f in fields)
            {
                if (string.Equals(f.Name, addedField.Name, StringComparison.Ordinal))
                    throw new InvalidOperationException(
                        $"Field '{PathText(containerPath)}.{addedField.Name}' already exists.");
            }
            return new List<StructField>(fields) { addedField };
        });

        var protocolUpgrade = UpgradeProtocolForFeatures(
            baseProtocol ?? snapshot.Protocol, RequiredSchemaFeatures(newDeltaField.Type));

        var metadata = baseMeta with
        {
            SchemaString = DeltaSchemaSerializer.Serialize(newSchema),
            Configuration = newConfig,
        };
        var actions = new List<DeltaAction>();
        if (protocolUpgrade is not null)
            actions.Add(protocolUpgrade);
        actions.Add(metadata);
        return new DeferredSchemaChange(actions, metadata, protocolUpgrade, newSchema);
    }

    /// <summary>The compute-only counterpart of <see cref="RenameFieldAsync"/> (nested RENAME) — for a buffered
    /// transaction. <paramref name="fieldPath"/> is the FULL path of the field (length ≥ 2). Requires column
    /// mapping (the field keeps its id + physical name). No protocol change.</summary>
    public DeferredSchemaChange ComputeRenameField(
        IReadOnlyList<string> fieldPath, string newName, MetadataAction? baseMetadata = null)
    {
        ThrowIfDisposed();
        ProtocolVersions.ValidateWriteSupport(CurrentSnapshot.Protocol);
        if (fieldPath.Count < 2)
            throw new ArgumentException(
                "fieldPath must name a NESTED field (use ComputeRenameColumn for top-level columns).");

        var snapshot = CurrentSnapshot;
        var baseMeta = baseMetadata ?? snapshot.Metadata;
        var baseSchema = baseMetadata is null
            ? snapshot.Schema
            : DeltaSchemaSerializer.Parse(baseMetadata.SchemaString);
        if (ColumnMapping.GetMode(baseMeta.Configuration) == ColumnMappingMode.None)
        {
            throw new InvalidOperationException(
                "RENAME of a nested field requires column mapping (enable it at table creation) — a plain table "
                + "would need to rewrite every data file since the logical name is the physical parquet name.");
        }

        string oldName = fieldPath[fieldPath.Count - 1];
        var containerPath = fieldPath.Take(fieldPath.Count - 1).ToList();
        var newSchema = TransformStructAt(baseSchema, containerPath, 0, fields =>
        {
            StructField? target = null;
            foreach (var f in fields)
            {
                if (string.Equals(f.Name, newName, StringComparison.Ordinal))
                    throw new InvalidOperationException(
                        $"Field '{PathText(containerPath)}.{newName}' already exists.");
                if (string.Equals(f.Name, oldName, StringComparison.Ordinal))
                    target = f;
            }
            if (target is null)
                throw new InvalidOperationException($"Field '{PathText(fieldPath)}' does not exist.");
            var result = new List<StructField>(fields.Count);
            foreach (var f in fields)
            {
                result.Add(ReferenceEquals(f, target)
                    ? new StructField { Name = newName, Type = f.Type, Nullable = f.Nullable, Metadata = f.Metadata }
                    : f);
            }
            return result;
        });

        var metadata = baseMeta with { SchemaString = DeltaSchemaSerializer.Serialize(newSchema) };
        return new DeferredSchemaChange(new List<DeltaAction> { metadata }, metadata, null, newSchema);
    }

    /// <summary>The compute-only counterpart of <see cref="DropFieldAsync"/> (nested DROP) — for a buffered
    /// transaction. <paramref name="fieldPath"/> is the FULL path (length ≥ 2). Requires column mapping; the
    /// containing struct must not become empty; the retired id is never reused. No protocol change.</summary>
    public DeferredSchemaChange ComputeDropField(
        IReadOnlyList<string> fieldPath, MetadataAction? baseMetadata = null)
    {
        ThrowIfDisposed();
        ProtocolVersions.ValidateWriteSupport(CurrentSnapshot.Protocol);
        if (fieldPath.Count < 2)
            throw new ArgumentException(
                "fieldPath must name a NESTED field (use ComputeDropColumn for top-level columns).");

        var snapshot = CurrentSnapshot;
        var baseMeta = baseMetadata ?? snapshot.Metadata;
        var baseSchema = baseMetadata is null
            ? snapshot.Schema
            : DeltaSchemaSerializer.Parse(baseMetadata.SchemaString);
        if (ColumnMapping.GetMode(baseMeta.Configuration) == ColumnMappingMode.None)
        {
            throw new InvalidOperationException(
                "DROP of a nested field requires column mapping (enable it at table creation) — a plain table "
                + "would need to rewrite every data file since the logical name is the physical parquet name.");
        }

        string name = fieldPath[fieldPath.Count - 1];
        var containerPath = fieldPath.Take(fieldPath.Count - 1).ToList();
        var newSchema = TransformStructAt(baseSchema, containerPath, 0, fields =>
        {
            var result = new List<StructField>(fields.Count);
            bool found = false;
            foreach (var f in fields)
            {
                if (string.Equals(f.Name, name, StringComparison.Ordinal)) { found = true; continue; }
                result.Add(f);
            }
            if (!found)
                throw new InvalidOperationException($"Field '{PathText(fieldPath)}' does not exist.");
            if (result.Count == 0)
                throw new InvalidOperationException(
                    $"Cannot drop the only field of struct '{PathText(containerPath)}'.");
            return result;
        });

        var metadata = baseMeta with { SchemaString = DeltaSchemaSerializer.Serialize(newSchema) };
        return new DeferredSchemaChange(new List<DeltaAction> { metadata }, metadata, null, newSchema);
    }

    /// <summary>Reconciles a logically-named batch to <paramref name="expectedFields"/> — the public form of the
    /// read path's recursive schema-evolution reconcile: expected columns/struct members the batch lacks
    /// backfill as typed NULLs, extra ones drop, struct children recurse. A buffered transaction uses it to
    /// overlay its PENDING (uncommitted-ALTER) schema onto committed reads ("read your own schema").</summary>
    public static RecordBatch ReconcileBatchToFields(RecordBatch batch, IReadOnlyList<Field> expectedFields)
        => SchemaEvolution.BackfillMissingColumns(batch, expectedFields);

    /// <summary>
    /// Replaces the table's schema wholesale with <paramref name="newSchema"/> as a metadata-only commit (a new
    /// <c>metaData</c> action; no data files are rewritten). Unlike <see cref="AddColumnAsync"/> this can add,
    /// drop, or retype columns — the "schema overwrite" primitive a CREATE OR REPLACE uses (adopt exactly the
    /// incoming schema). Callers align the data (typically a paired <c>Overwrite</c> write that removes the
    /// old-schema files). On a column-mapping table fresh field ids are assigned (continuing past the current
    /// maxColumnId so ids are never reused across history). Returns the new version; a no-op (returns the current
    /// version) if the schema is already logically identical.
    /// </summary>
    public async ValueTask<long> SetSchemaAsync(
        Apache.Arrow.Schema newSchema, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ProtocolVersions.ValidateWriteSupport(CurrentSnapshot.Protocol);

        var snapshot = CurrentSnapshot;
        var config = snapshot.Metadata.Configuration;
        var mappingMode = ColumnMapping.GetMode(config);

        var newDeltaSchema = SchemaConverter.FromArrowSchema(newSchema);
        var newConfig = config;
        if (mappingMode != ColumnMappingMode.None)
        {
            // A column-mapping table's SchemaString always differs (ids/physical names the incoming Arrow schema
            // lacks), so compare the LOGICAL shape (names + types, ids stripped recursively) to no-op when nothing
            // actually changed — e.g. a fresh CTAS that just created the table with the right schema+mapping.
            if (string.Equals(LogicalSchemaString(snapshot.Schema),
                              LogicalSchemaString(newDeltaSchema), StringComparison.Ordinal))
            {
                return snapshot.Version;
            }
            // Full-replace adopts an arbitrary new schema, so assign FRESH field ids + physical names (continuing
            // from the current maxColumnId so ids are never reused) and bump maxColumnId. Sound for a REPLACE
            // because the old data files are removed by the paired Overwrite.
            int startId = ColumnMapping.GetMaxColumnId(snapshot.Schema);
            if (config is not null && config.TryGetValue(ColumnMapping.MaxColumnIdKey, out var maxStr)
                && int.TryParse(maxStr, out var cfgMax))
            {
                startId = Math.Max(startId, cfgMax);
            }
            var (mapped, newMax) = ColumnMapping.AssignColumnMapping(newDeltaSchema, startId);
            newDeltaSchema = mapped;
            var cfg = config is null
                ? new Dictionary<string, string>()
                : config.ToDictionary(kv => kv.Key, kv => kv.Value);
            cfg[ColumnMapping.MaxColumnIdKey] = newMax.ToString();
            newConfig = cfg;
        }

        string newSchemaString = DeltaSchemaSerializer.Serialize(newDeltaSchema);
        if (string.Equals(newSchemaString, snapshot.Metadata.SchemaString, StringComparison.Ordinal))
        {
            return snapshot.Version; // identical schema — nothing to commit
        }

        var protocolUpgrade = UpgradeProtocolForFeatures(snapshot.Protocol, RequiredSchemaFeatures(newDeltaSchema));

        return await CommitMetadataOnlyAsync(
            snapshot,
            snapshot.Metadata with { SchemaString = newSchemaString, Configuration = newConfig },
            "CHANGE COLUMNS",
            cancellationToken,
            protocolUpgrade).ConfigureAwait(false);
    }

    // The schema's LOGICAL signature — field names + types + nullability, with column-mapping metadata (ids /
    // physical names) stripped RECURSIVELY — so two schemas that differ only in assigned ids compare equal. Used
    // to no-op SetSchema on a column-mapping table when the logical shape is unchanged.
    private static string LogicalSchemaString(StructType schema)
        => DeltaSchemaSerializer.Serialize(StripMetadata(schema));

    private static StructType StripMetadata(StructType schema)
    {
        var stripped = new List<StructField>(schema.Fields.Count);
        foreach (var f in schema.Fields)
            stripped.Add(new StructField
            {
                Name = f.Name, Type = StripMetadata(f.Type), Nullable = f.Nullable, Metadata = null,
            });
        return new StructType { Fields = stripped };
    }

    private static DeltaDataType StripMetadata(DeltaDataType type) => type switch
    {
        StructType st => StripMetadata(st),
        ArrayType at => new ArrayType { ElementType = StripMetadata(at.ElementType), ContainsNull = at.ContainsNull },
        MapType mt => new MapType
        {
            KeyType = StripMetadata(mt.KeyType), ValueType = StripMetadata(mt.ValueType),
            ValueContainsNull = mt.ValueContainsNull,
        },
        _ => type,
    };

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

    /// <summary>
    /// Adds a nullable field INSIDE a nested struct column as a metadata-only commit — the nested analog of
    /// <see cref="AddColumnAsync"/>. <paramref name="containerPath"/> names the CONTAINING struct (top-level
    /// column first, e.g. <c>["s", "inner"]</c> adds a member to <c>s.inner</c>); every segment must resolve to
    /// a STRUCT. Old files lack the member — the read path reconciles it to a typed NULL child. On a
    /// column-mapping table the new field is assigned a fresh column id + physical name RECURSIVELY (a
    /// struct/array/map-typed member arrives with ids on every descendant) and <c>maxColumnId</c> is bumped. A
    /// type needing a schema-driven feature (timestampNtz / variantType) upgrades the protocol in the same
    /// commit. Returns the new version.
    /// </summary>
    public async ValueTask<long> AddFieldAsync(
        IReadOnlyList<string> containerPath, Field newField, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ProtocolVersions.ValidateWriteSupport(CurrentSnapshot.Protocol);
        if (containerPath.Count == 0)
            throw new ArgumentException(
                "containerPath must name the containing struct column.", nameof(containerPath));
        if (!newField.IsNullable)
            throw new InvalidOperationException(
                $"ADD COLUMN '{PathText(containerPath)}.{newField.Name}' must be nullable — existing rows have "
                + "no value for a new field.");

        var snapshot = CurrentSnapshot;
        var config = snapshot.Metadata.Configuration;
        var mappingMode = ColumnMapping.GetMode(config);

        var newDeltaField = SchemaConverter.FromArrowSchema(
            new Apache.Arrow.Schema([newField], null)).Fields[0];

        var newConfig = config;
        if (mappingMode != ColumnMappingMode.None)
        {
            // Recursive id + physical-name assignment (the create-time assigner) — a struct/array/map-typed
            // field gets ids on every descendant, exactly like at create; maxColumnId advances past them.
            var (mappedField, lastId) = AssignMappedField(snapshot.Schema, config, newDeltaField);
            newDeltaField = mappedField;
            var cfg = config is null
                ? new Dictionary<string, string>()
                : config.ToDictionary(kv => kv.Key, kv => kv.Value);
            cfg[ColumnMapping.MaxColumnIdKey] = lastId.ToString();
            newConfig = cfg;
        }

        var addedField = newDeltaField;
        var newSchema = TransformStructAt(snapshot.Schema, containerPath, 0, fields =>
        {
            foreach (var f in fields)
            {
                if (string.Equals(f.Name, addedField.Name, StringComparison.Ordinal))
                    throw new InvalidOperationException(
                        $"Field '{PathText(containerPath)}.{addedField.Name}' already exists.");
            }
            return new List<StructField>(fields) { addedField };
        });
        string newSchemaString = DeltaSchemaSerializer.Serialize(newSchema);

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
    /// Renames a field INSIDE a nested struct column as a metadata-only commit — the nested analog of
    /// <see cref="RenameColumnAsync"/>. <paramref name="fieldPath"/> is the FULL path of the field (length ≥ 2;
    /// use <see cref="RenameColumnAsync"/> for a top-level column). Requires column mapping — the field keeps
    /// its column id + physical name, so old files keep resolving under the new logical name. Returns the new
    /// version.
    /// </summary>
    public async ValueTask<long> RenameFieldAsync(
        IReadOnlyList<string> fieldPath, string newName, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ProtocolVersions.ValidateWriteSupport(CurrentSnapshot.Protocol);
        if (fieldPath.Count < 2)
            throw new ArgumentException(
                "fieldPath must name a NESTED field (use RenameColumnAsync for top-level columns).");

        var snapshot = CurrentSnapshot;
        if (ColumnMapping.GetMode(snapshot.Metadata.Configuration) == ColumnMappingMode.None)
        {
            throw new InvalidOperationException(
                "RENAME of a nested field requires column mapping (enable it at table creation) — a plain table "
                + "would need to rewrite every data file since the logical name is the physical parquet name.");
        }

        string oldName = fieldPath[fieldPath.Count - 1];
        var containerPath = fieldPath.Take(fieldPath.Count - 1).ToList();
        var newSchema = TransformStructAt(snapshot.Schema, containerPath, 0, fields =>
        {
            StructField? target = null;
            foreach (var f in fields)
            {
                if (string.Equals(f.Name, newName, StringComparison.Ordinal))
                    throw new InvalidOperationException(
                        $"Field '{PathText(containerPath)}.{newName}' already exists.");
                if (string.Equals(f.Name, oldName, StringComparison.Ordinal))
                    target = f;
            }
            if (target is null)
                throw new InvalidOperationException($"Field '{PathText(fieldPath)}' does not exist.");
            var result = new List<StructField>(fields.Count);
            foreach (var f in fields)
            {
                result.Add(ReferenceEquals(f, target)
                    ? new StructField { Name = newName, Type = f.Type, Nullable = f.Nullable, Metadata = f.Metadata }
                    : f);
            }
            return result;
        });
        string newSchemaString = DeltaSchemaSerializer.Serialize(newSchema);

        return await CommitMetadataOnlyAsync(
            snapshot,
            snapshot.Metadata with { SchemaString = newSchemaString },
            "RENAME COLUMN",
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Drops a field INSIDE a nested struct column as a metadata-only commit — the nested analog of
    /// <see cref="DropColumnAsync"/>. <paramref name="fieldPath"/> is the FULL path (length ≥ 2; use
    /// <see cref="DropColumnAsync"/> for a top-level column). Requires column mapping; the containing struct
    /// must not become empty; the retired column id is never reused (maxColumnId is not decremented). Old files
    /// still carry the physical column — readers reconcile it away. Returns the new version.
    /// </summary>
    public async ValueTask<long> DropFieldAsync(
        IReadOnlyList<string> fieldPath, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ProtocolVersions.ValidateWriteSupport(CurrentSnapshot.Protocol);
        if (fieldPath.Count < 2)
            throw new ArgumentException(
                "fieldPath must name a NESTED field (use DropColumnAsync for top-level columns).");

        var snapshot = CurrentSnapshot;
        if (ColumnMapping.GetMode(snapshot.Metadata.Configuration) == ColumnMappingMode.None)
        {
            throw new InvalidOperationException(
                "DROP of a nested field requires column mapping (enable it at table creation) — a plain table "
                + "would need to rewrite every data file since the logical name is the physical parquet name.");
        }

        string name = fieldPath[fieldPath.Count - 1];
        var containerPath = fieldPath.Take(fieldPath.Count - 1).ToList();
        var newSchema = TransformStructAt(snapshot.Schema, containerPath, 0, fields =>
        {
            var result = new List<StructField>(fields.Count);
            bool found = false;
            foreach (var f in fields)
            {
                if (string.Equals(f.Name, name, StringComparison.Ordinal)) { found = true; continue; }
                result.Add(f);
            }
            if (!found)
                throw new InvalidOperationException($"Field '{PathText(fieldPath)}' does not exist.");
            if (result.Count == 0)
                throw new InvalidOperationException(
                    $"Cannot drop the only field of struct '{PathText(containerPath)}'.");
            return result;
        });
        string newSchemaString = DeltaSchemaSerializer.Serialize(newSchema);

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

    /// <summary>
    /// The ALTER CLUSTER BY analog: declares, re-keys, or (null/empty) removes the table's clustering
    /// declaration — the <c>delta.clustering</c> domain — as ONE metadata commit. Callers supply LOGICAL
    /// column names (resolved to physical through the mapped schema). Declaring clustering on a PARTITIONED
    /// table throws (mutually exclusive). Upgrades the protocol with the WRITER-ONLY
    /// <c>clustering</c>/<c>domainMetadata</c> features when missing — the reader side is left untouched,
    /// since neither is a reader feature. <paramref name="extraActions"/> (e.g. a caller's table-property
    /// update) join the same commit. Returns the committed version, or the current one when there was
    /// nothing to change and no extra actions.
    /// </summary>
    public async ValueTask<long> SetClusteringColumnsAsync(
        IReadOnlyList<string>? logicalColumns,
        IReadOnlyList<DeltaAction>? extraActions = null,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ProtocolVersions.ValidateWriteSupport(CurrentSnapshot.Protocol);

        var snapshot = CurrentSnapshot;
        var actions = new List<DeltaAction>();

        if (logicalColumns is { Count: > 0 })
        {
            if (snapshot.Metadata.PartitionColumns.Count > 0)
            {
                throw new DeltaFormatException(
                    "Liquid clustering and partitioning are mutually exclusive — a partitioned table "
                    + "cannot declare clustering columns.");
            }

            var upgrade = UpgradeProtocolForWriterFeatures(
                snapshot.Protocol, ["clustering", "domainMetadata"]);
            if (upgrade is not null)
                actions.Add(upgrade);

            actions.Add(BuildClusteringDomain(
                snapshot.Schema, logicalColumns, ColumnMapping.GetMode(snapshot.Metadata.Configuration)));
        }
        else if (snapshot.DomainMetadata.ContainsKey(ClusteringDomain))
        {
            actions.Add(new DomainMetadata
            {
                Domain = ClusteringDomain,
                Configuration = "{}",
                Removed = true,
            });
        }

        if (extraActions is { Count: > 0 })
            actions.AddRange(extraActions);
        if (actions.Count == 0)
            return snapshot.Version; // nothing to change

        long newVersion = snapshot.Version + 1;
        var final = Log.InCommitTimestamp.EnsureCommitInfo(
            actions, snapshot.Metadata.Configuration,
            logicalColumns is { Count: > 0 } ? "SET SORTED BY" : "RESET SORTED BY");
        await _log.WriteCommitAsync(newVersion, final, cancellationToken).ConfigureAwait(false);

        _currentSnapshot = await SnapshotBuilder.UpdateAsync(snapshot, _log, cancellationToken)
            .ConfigureAwait(false);
        return newVersion;
    }

    /// <summary>The Delta system domain carrying a table's liquid-clustering column spec.</summary>
    private const string ClusteringDomain = "delta.clustering";

    // The clustering-columns spec, byte-shaped like Spark's own (each column a PATH array — these are
    // top-level names — plus the redundant domainName field Spark includes):
    //   {"clusteringColumns":[["a"],["b"]],"domainName":"delta.clustering"}
    // CRITICAL: the domain stores PHYSICAL names. OSS Delta's ClusteringColumnInfo resolves them against
    // the schema's physical names and None.get-crashes on a logical name under column mapping (observed
    // live on Fabric Spark 4.1, breaking DESCRIBE DETAIL and OPTIMIZE). Callers supply LOGICAL names,
    // resolved here through the already-mapping-assigned schema; without mapping physical == logical.
    private static DomainMetadata BuildClusteringDomain(
        Schema.StructType deltaSchema, IReadOnlyList<string> clusteringColumns, ColumnMappingMode mode)
    {
        var sb = new System.Text.StringBuilder("{\"clusteringColumns\":[");
        for (int i = 0; i < clusteringColumns.Count; i++)
        {
            var field = deltaSchema.Fields.FirstOrDefault(
                f => string.Equals(f.Name, clusteringColumns[i], StringComparison.OrdinalIgnoreCase));
            if (field is null)
            {
                throw new DeltaFormatException(
                    $"Clustering column '{clusteringColumns[i]}' is not a column of the table.");
            }
            string physical = ColumnMapping.GetPhysicalName(field, mode);
            if (i > 0)
                sb.Append(',');
            sb.Append("[\"").Append(physical.Replace("\\", "\\\\").Replace("\"", "\\\"")).Append("\"]");
        }
        sb.Append("],\"domainName\":\"").Append(ClusteringDomain).Append("\"}");

        return new DomainMetadata
        {
            Domain = ClusteringDomain,
            Configuration = sb.ToString(),
            Removed = false,
        };
    }

    /// <summary>
    /// Protocol upgrade for WRITER-ONLY features (clustering / domainMetadata): bumps minWriterVersion to 7
    /// with the legacy writer features enumerated, and appends the missing ones. The READER side is left
    /// exactly as it was — adding a writer-only feature to readerFeatures would wrongly lock readers out
    /// (a legacy reader-1 table stays reader-1 while becoming writer-7).
    /// </summary>
    private static ProtocolAction? UpgradeProtocolForWriterFeatures(
        ProtocolAction current, IReadOnlyList<string> features)
    {
        var missing = features.Where(f => current.WriterFeatures?.Contains(f) != true).ToList();
        if (missing.Count == 0)
            return null;

        var writerFeatures = new List<string>(
            current.WriterFeatures ?? LegacyWriterFeatures(current.MinWriterVersion));
        foreach (var feature in missing)
        {
            if (!writerFeatures.Contains(feature))
                writerFeatures.Add(feature);
        }

        return new ProtocolAction
        {
            MinReaderVersion = current.MinReaderVersion,
            MinWriterVersion = 7,
            ReaderFeatures = current.ReaderFeatures,
            WriterFeatures = writerFeatures,
        };
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
    /// True when <paramref name="type"/> contains a <c>variant</c> column at any nesting depth.
    /// </summary>
    private static bool SchemaUsesVariant(DeltaDataType type) => type switch
    {
        PrimitiveType p => string.Equals(p.TypeName, "variant", StringComparison.Ordinal),
        StructType st => st.Fields.Any(f => SchemaUsesVariant(f.Type)),
        ArrayType at => SchemaUsesVariant(at.ElementType),
        MapType mt => SchemaUsesVariant(mt.KeyType) || SchemaUsesVariant(mt.ValueType),
        _ => false,
    };

    /// <summary>
    /// The schema-driven reader+writer table features <paramref name="type"/> requires per the Delta spec:
    /// <c>timestampNtz</c> for a naive timestamp, <c>variantType</c> for a variant column. Both are
    /// reader-3 / writer-7 named features, so declaring either upgrades the table to table-features mode.
    /// </summary>
    private static List<string> RequiredSchemaFeatures(DeltaDataType type)
    {
        var features = new List<string>();
        if (SchemaUsesTimestampNtz(type))
            features.Add("timestampNtz");
        if (SchemaUsesVariant(type))
            features.Add("variantType");
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

    // Rebuilds the schema with the struct at `containerPath` transformed via `transform` on its field list
    // (every non-terminal segment must resolve to a struct field). Fields outside the path are untouched.
    private static StructType TransformStructAt(
        StructType current, IReadOnlyList<string> containerPath, int depth,
        Func<IReadOnlyList<StructField>, List<StructField>> transform)
    {
        if (depth == containerPath.Count)
            return new StructType { Fields = transform(current.Fields) };

        string segment = containerPath[depth];
        var newFields = new List<StructField>(current.Fields.Count);
        bool found = false;
        foreach (var f in current.Fields)
        {
            if (!found && string.Equals(f.Name, segment, StringComparison.Ordinal))
            {
                found = true;
                if (f.Type is not StructType st)
                    throw new InvalidOperationException(
                        $"'{PathText(containerPath.Take(depth + 1).ToList())}' is not a STRUCT column.");
                var newSt = TransformStructAt(st, containerPath, depth + 1, transform);
                newFields.Add(new StructField
                {
                    Name = f.Name, Type = newSt, Nullable = f.Nullable, Metadata = f.Metadata,
                });
            }
            else
            {
                newFields.Add(f);
            }
        }
        if (!found)
            throw new InvalidOperationException(
                $"Column '{PathText(containerPath.Take(depth + 1).ToList())}' does not exist.");
        return new StructType { Fields = newFields };
    }

    private static string PathText(IReadOnlyList<string> path) => string.Join(".", path);

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

    #region Transactions

    /// <summary>
    /// Begins an optimistic-concurrency transaction pinned to the current table version.
    ///
    /// <para>The returned <see cref="DeltaTransaction"/> records what it reads; on
    /// <see cref="DeltaTransaction.CommitAsync"/> it is validated against every commit that landed since
    /// this call. If none of them invalidated its reads it commits (rebasing onto the newer version if
    /// necessary); otherwise it aborts with a <see cref="DeltaConflictException"/> — first committer
    /// wins. Use this when a write depends on a read that a concurrent writer could invalidate; the
    /// auto-committing <see cref="DeleteAsync"/> / write methods are the single-shot equivalent.</para>
    /// </summary>
    public DeltaTransaction StartTransaction(
        IsolationLevel isolationLevel = IsolationLevel.WriteSerializable)
    {
        ThrowIfDisposed();
        return new DeltaTransaction(this, CurrentSnapshot, isolationLevel);
    }

    /// <summary>
    /// Runs the optimistic-concurrency commit loop for <paramref name="transaction"/>. A DELETE reads
    /// exactly the files it removes, so the removed paths are both the read-set (concurrentDeleteRead)
    /// and the planned removes (delete/delete).
    /// </summary>
    internal ValueTask<long> CommitTransactionAsync(
        DeltaTransaction transaction, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();

        var baseSnapshot = transaction.BaseSnapshot;

        // Every transactional operation is now rebase-safe under row tracking: a DELETE only edits deletion
        // vectors on EXISTING files (its re-add keeps that file's own baseRowId), or — when a file was
        // concurrently rewritten — remaps its rows by STABLE ROW ID onto the new files (Layer 3 B); an append
        // or an UPDATE's copy-on-write rewrite is a fresh (post-image) add whose baseRowId CommitOccAsync
        // re-derives against the advanced high-water mark on rebase. Overwrite modes are not stageable on a
        // transaction, so nothing here reads the whole active-file set (the one remaining non-rebase-safe case).
        var reads = new Concurrency.ReadSet
        {
            Files = transaction.RemovedPaths,
            Predicates = transaction.ReadPredicates,
        };

        return CommitOccAsync(
            baseSnapshot, transaction.DataActions, reads, transaction.RemovedPaths,
            transaction.IsolationLevel, transaction.Operation, rebaseSafe: true,
            cancellationToken,
            rowLevelDeletes: transaction.DvEdits);
    }

    /// <summary>Shared by blind-append commits, which plan no removes.</summary>
    private static readonly HashSet<string> NoRemovedPaths = new(StringComparer.Ordinal);

    /// <summary>
    /// The optimistic-concurrency commit loop shared by the transactional path, the auto-committing
    /// <see cref="DeleteAsync"/>, and single-shot appends. Attempts the commit at the version after
    /// <paramref name="baseSnapshot"/>; on a collision it reads the intervening commits, runs the
    /// <see cref="Concurrency.ConflictChecker"/> against <paramref name="reads"/> /
    /// <paramref name="plannedRemovePaths"/>, and either aborts (a real conflict) or — when
    /// <paramref name="rebaseSafe"/> — rebases onto the latest version and retries. A no-conflict rebase
    /// re-commits the staged actions verbatim, valid precisely because nothing the commit read or removed
    /// was touched.
    ///
    /// <para><paramref name="rebaseSafe"/> is <c>false</c> when the staged actions embed the attempted
    /// version — row tracking's <c>baseRowId</c> / <c>defaultRowCommitVersion</c> would be wrong after a
    /// rebase — so such a commit succeeds only uncontended and otherwise aborts rather than corrupt.</para>
    /// </summary>
    internal async ValueTask<long> CommitOccAsync(
        Snapshot.Snapshot baseSnapshot,
        IReadOnlyList<DeltaAction> dataActions,
        Concurrency.ReadSet reads,
        ISet<string> plannedRemovePaths,
        IsolationLevel isolationLevel,
        string operation,
        bool rebaseSafe,
        CancellationToken cancellationToken,
        IReadOnlyList<DeleteDvEdit>? rowLevelDeletes = null)
    {
        ThrowIfDisposed();

        if (dataActions.Count == 0)
            return baseSnapshot.Version; // nothing staged — no commit

        var pruner = new DeltaFilePruner(baseSnapshot.Schema, baseSnapshot.Metadata.PartitionColumns);
        bool rowLevel = rowLevelDeletes is { Count: > 0 };
        bool rowTrackingEnabled = DeltaLake.RowTracking.RowTrackingConfig.IsEnabled(
            baseSnapshot.Metadata.Configuration);

        // The actions actually written this attempt. Starts as the actions computed against the base
        // snapshot; a rebase replaces it with actions computed against the latest snapshot — a row-level
        // delete's deletion-vector-union/remap actions, and/or a re-derivation of row-tracking post-image ids.
        // Always sourced from the original `dataActions` (or the row-level resolution of them), never from a
        // prior rebase, so each retry rebases the STABLE staged work onto whatever the newest snapshot holds.
        var currentActions = dataActions;

        long attemptVersion = baseSnapshot.Version + 1;
        const int maxAttempts = 100;
        for (int attempt = 0; ; attempt++)
        {
            var finalActions = Log.InCommitTimestamp.EnsureCommitInfo(
                currentActions, baseSnapshot.Metadata.Configuration, operation);
            try
            {
                await _log.WriteCommitAsync(attemptVersion, finalActions, cancellationToken)
                    .ConfigureAwait(false);
                _currentSnapshot = await SnapshotBuilder.UpdateAsync(
                    CurrentSnapshot, _log, cancellationToken).ConfigureAwait(false);
                return attemptVersion;
            }
            catch (DeltaConflictException) when (attempt + 1 < maxAttempts)
            {
                // Row-level DELETE/DELETE reconciliation needs the concurrent files' current deletion vectors,
                // and a row-tracking rebase needs the advanced high-water mark, so both build the latest
                // snapshot; a plain (non-tracking) append/update rebase only needs the version.
                long latest;
                Snapshot.Snapshot? latestSnapshot = null;
                if (rowLevel || rowTrackingEnabled)
                {
                    latestSnapshot = await SnapshotBuilder.UpdateAsync(
                        baseSnapshot, _log, cancellationToken).ConfigureAwait(false);
                    latest = latestSnapshot.Version;
                }
                else
                {
                    latest = await _log.GetLatestVersionAsync(cancellationToken).ConfigureAwait(false);
                }

                var concurrent = new List<(long, IReadOnlyList<DeltaAction>)>();
                for (long v = baseSnapshot.Version + 1; v <= latest; v++)
                {
                    concurrent.Add((v,
                        await _log.ReadCommitAsync(v, cancellationToken).ConfigureAwait(false)));
                }

                // Row-level resolution runs BEFORE the checker so its verdict can ignore the reconciled
                // files: rebase each staged delete's deletion vector onto the file's current one (union the
                // rows). A null result is a genuine conflict — the same row was deleted concurrently, or the
                // file was rewritten away (compaction/update, out of scope for pure DV resolution).
                ISet<string>? resolvedPaths = null;
                if (rowLevel)
                {
                    var resolution = await ResolveRowLevelDeletesAsync(
                        baseSnapshot, latestSnapshot!, dataActions, rowLevelDeletes!, cancellationToken)
                        .ConfigureAwait(false);
                    if (resolution is null)
                        throw new DeltaConflictException(
                            "A concurrent commit deleted a row this delete also removed, or rewrote a file "
                            + "it targeted such that a row cannot be remapped; the delete conflicts at row "
                            + "level and must be retried.");

                    currentActions = resolution.Value.Actions;
                    resolvedPaths = resolution.Value.ResolvedPaths;
                }
                else
                {
                    currentActions = dataActions; // stable source; row-tracking ids re-derived below
                }

                var verdict = Concurrency.ConflictChecker.Check(
                    reads, plannedRemovePaths, pruner, isolationLevel, concurrent, resolvedPaths);
                if (verdict.HasConflict)
                    throw new DeltaConflictException(verdict.Message!);

                if (!rebaseSafe)
                {
                    throw new DeltaConflictException(
                        "A concurrent commit landed and this operation cannot be safely rebased onto it; "
                        + "retry the operation.");
                }

                // Re-derive row-tracking post-image ids against the snapshot we now land on (a concurrent
                // commit may have consumed row-id space). No-op for the row-level delete's own re-adds — they
                // keep their existing baseRowId (excluded by resolvedPaths / base-active membership).
                if (rowTrackingEnabled)
                    currentActions = RebaseRowTrackingAddIds(
                        currentActions, baseSnapshot, latestSnapshot!, latest + 1, resolvedPaths);

                attemptVersion = latest + 1; // no conflict — rebase and retry
            }
        }
    }

    /// <summary>
    /// Row-level DELETE/DELETE reconciliation (Databricks row-level concurrency, and beyond): rebase a losing
    /// delete onto the winner so two writers touching DISJOINT rows of the same data both land, instead of the
    /// second aborting at file granularity. Each file this delete touched is reconciled by one of two
    /// mechanisms, chosen by whether the file survived the concurrent commits:
    /// <list type="bullet">
    /// <item><b>DV union</b> — the file is still active in <paramref name="latestSnapshot"/>: rebuild its
    /// <see cref="RemoveFile"/>/<see cref="AddFile"/> pair against the file's CURRENT state, unioning the rows
    /// this delete removed into the file's current deletion vector (DV positions are stable across a
    /// concurrent DV-delete, so no row tracking is needed).</item>
    /// <item><b>Remap across a rewrite</b> (<see cref="RemapRowLevelDeletesAsync"/>) — the file was rewritten
    /// away by a concurrent compaction/UPDATE: relocate the deleted rows by STABLE ROW ID onto the new files
    /// (requires row tracking). Beyond Databricks, whose row-level concurrency still conflicts with a rewrite.</item>
    /// </list>
    /// Every other staged action (CDC files, a co-staged append) is preserved verbatim.
    ///
    /// <para>Returns <c>null</c> — a genuine conflict that must abort — when a row this delete removed was ALSO
    /// removed/updated by a concurrent commit (same-row conflict), or when a rewritten-away file's rows cannot
    /// be remapped (no row tracking, or a target row was concurrently deleted so its stable id is gone).</para>
    /// </summary>
    private async ValueTask<(List<DeltaAction> Actions, ISet<string> ResolvedPaths)?>
        ResolveRowLevelDeletesAsync(
            Snapshot.Snapshot baseSnapshot,
            Snapshot.Snapshot latestSnapshot,
            IReadOnlyList<DeltaAction> originalActions,
            IReadOnlyList<DeleteDvEdit> dvEdits,
            CancellationToken cancellationToken)
    {
        // A valid table has at most one active file per path (a DV update removes the old reconciliation
        // key and adds a new one with the same path), so path is a sufficient lookup key here.
        var activeByPath = new Dictionary<string, AddFile>(StringComparer.Ordinal);
        foreach (var file in latestSnapshot.ActiveFiles.Values)
            activeByPath[file.Path] = file;

        var editedPaths = new HashSet<string>(StringComparer.Ordinal);
        foreach (var edit in dvEdits)
            editedPaths.Add(edit.Path);

        // Split this delete's edits: files still active reconcile by DV union; files rewritten away need the
        // stable-row-id remap (Layer 3 B), which requires row tracking. Without it a rewritten-away file is a
        // genuine, unresolvable conflict (the strict pre-existing behavior).
        var unionEdits = new List<DeleteDvEdit>();
        var remapEdits = new List<DeleteDvEdit>();
        foreach (var edit in dvEdits)
            (activeByPath.ContainsKey(edit.Path) ? unionEdits : remapEdits).Add(edit);

        if (remapEdits.Count > 0
            && !DeltaLake.RowTracking.RowTrackingConfig.IsEnabled(latestSnapshot.Metadata.Configuration))
        {
            return null; // rewritten away, no stable ids to remap by
        }

        // Paths whose concurrent remove/re-add the checker must ignore: the source files we reconcile (both
        // union and remap), plus — added inside the remap — the NEW files it re-touches.
        var resolvedPaths = new HashSet<string>(editedPaths, StringComparer.Ordinal);

        // Keep everything except this delete's own remove/add of an edited file — those get rebuilt below.
        var result = new List<DeltaAction>();
        foreach (var action in originalActions)
        {
            if (action is RemoveFile remove && editedPaths.Contains(remove.Path))
                continue;
            if (action is AddFile add && editedPaths.Contains(add.Path))
                continue;
            result.Add(action);
        }

        var dvWriter = new DeletionVectors.DeletionVectorWriter(_fs);

        foreach (var edit in unionEdits)
        {
            var latestAdd = activeByPath[edit.Path];

            var concurrentDeleted = latestAdd.DeletionVector is not null
                ? await _dvReader.ReadAsync(latestAdd.DeletionVector, cancellationToken)
                    .ConfigureAwait(false)
                : new HashSet<long>();

            // If any row this delete removed is already deleted in the file's current DV, the same row was
            // deleted concurrently — a real row-level conflict.
            foreach (long row in edit.NewlyDeletedRows)
            {
                if (concurrentDeleted.Contains(row))
                    return null;
            }

            var union = new HashSet<long>(concurrentDeleted);
            foreach (long row in edit.NewlyDeletedRows)
                union.Add(row);

            var unionDv = await dvWriter.CreateAsync(union, union.Count, cancellationToken)
                .ConfigureAwait(false);

            result.Add(new RemoveFile
            {
                Path = latestAdd.Path,
                DeletionTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                DataChange = true,
                DeletionVector = latestAdd.DeletionVector,
            });

            result.Add(latestAdd with
            {
                DeletionVector = unionDv,
                DataChange = true,
            });
        }

        if (remapEdits.Count > 0)
        {
            var remapped = await RemapRowLevelDeletesAsync(
                baseSnapshot, latestSnapshot, remapEdits, resolvedPaths, cancellationToken)
                .ConfigureAwait(false);
            if (remapped is null)
                return null;
            result.AddRange(remapped);
        }

        return (result, resolvedPaths);
    }

    /// <summary>
    /// Layer 3 (B): relocate a losing DELETE's row intents ACROSS a concurrent rewrite (compaction /
    /// copy-on-write UPDATE) by STABLE ROW ID, so a delete whose target file was rewritten away still lands
    /// instead of aborting. Requires row tracking — the rows are followed by their stable id, not position.
    ///
    /// <list type="number">
    /// <item>Resolve each target row's stable id + ORIGINAL commit version from the tombstoned source file
    /// (read at <paramref name="baseSnapshot"/>, where those rows still live un-deleted — the parquet
    /// survives until VACUUM). The target rows are identified by their absolute in-file positions
    /// (<see cref="DeleteDvEdit.NewlyDeletedRows"/>).</item>
    /// <item>Locate those stable ids in the NEW files (active in <paramref name="latestSnapshot"/> but not in
    /// the base) — compaction-shaped files (<c>dataChange=false</c>) first, early-exit once all are found. The
    /// row's commit version is the concurrent-modification discriminator: a relocated-untouched row keeps its
    /// ORIGINAL version (compaction and a CoW pass-through both materialize it) ⇒ remap; a concurrently
    /// UPDATED row carries the rewrite's version ⇒ conflict; an id found nowhere was concurrently DELETED (a
    /// DV-deleted relocated row is filtered from the scan) ⇒ conflict.</item>
    /// <item>The found positions become <c>remove</c>/<c>add</c> deletion-vector pairs on the new files.</item>
    /// </list>
    /// Returns <c>null</c> on any row-level conflict (concurrent update/delete of a target row, or an
    /// unresolvable id). Adds each new file it re-touches to <paramref name="resolvedPaths"/> so the checker
    /// ignores that file's concurrent add.
    /// </summary>
    private async ValueTask<List<DeltaAction>?> RemapRowLevelDeletesAsync(
        Snapshot.Snapshot baseSnapshot,
        Snapshot.Snapshot latestSnapshot,
        IReadOnlyList<DeleteDvEdit> remapEdits,
        HashSet<string> resolvedPaths,
        CancellationToken cancellationToken)
    {
        var baseByPath = new Dictionary<string, AddFile>(StringComparer.Ordinal);
        foreach (var file in baseSnapshot.ActiveFiles.Values)
            baseByPath[file.Path] = file;

        // 1. Resolve the target rows' stable ids + original commit versions from the tombstoned sources.
        var targetVersions = new Dictionary<long, long>(); // stable row id -> original commit version
        foreach (var edit in remapEdits)
        {
            if (!baseByPath.TryGetValue(edit.Path, out var sourceAdd))
                return null; // the delete's source file is not in the base snapshot — cannot resolve

            var wantPositions = new HashSet<long>(edit.NewlyDeletedRows);
            var ids = new List<Int64Array?>();
            var vers = new List<Int64Array?>();
            var positions = new List<Int64Array?>();
            await foreach (var _ in ReadFileAsync(
                sourceAdd, null, baseSnapshot, cancellationToken, ids, vers, positions).ConfigureAwait(false))
            {
                // Only the row-aligned out-params matter here; the emitted user batches are discarded.
            }

            int resolved = 0;
            for (int bi = 0; bi < positions.Count; bi++)
            {
                var pA = positions[bi];
                var idA = bi < ids.Count ? ids[bi] : null;
                var vA = bi < vers.Count ? vers[bi] : null;
                if (pA is null)
                    continue;
                for (int i = 0; i < pA.Length; i++)
                {
                    long pos = pA.GetValue(i)!.Value;
                    if (!wantPositions.Contains(pos))
                        continue;
                    if (idA is null || idA.IsNull(i) || vA is null || vA.IsNull(i))
                        return null; // a target row has no stable id/version to remap by
                    targetVersions[idA.GetValue(i)!.Value] = vA.GetValue(i)!.Value;
                    resolved++;
                }
            }
            if (resolved != wantPositions.Count)
                return null; // some target rows could not be resolved
        }

        // 2. Locate the stable ids in the NEW files (active in latest, absent from base). A row concurrently
        //    DV-deleted in latest is filtered out on read, so it never appears here → falls to the not-found
        //    conflict below.
        var remaining = new HashSet<long>(targetVersions.Keys);
        var assignments = new Dictionary<string, (AddFile File, HashSet<long> Positions)>(StringComparer.Ordinal);
        var candidates = latestSnapshot.ActiveFiles.Values
            .Where(f => !baseByPath.ContainsKey(f.Path))
            .OrderBy(f => f.DataChange) // false (compaction) first
            .ToList();

        foreach (var cand in candidates)
        {
            if (remaining.Count == 0)
                break;

            var ids = new List<Int64Array?>();
            var vers = new List<Int64Array?>();
            var positions = new List<Int64Array?>();
            await foreach (var _ in ReadFileAsync(
                cand, null, latestSnapshot, cancellationToken, ids, vers, positions).ConfigureAwait(false))
            {
            }

            for (int bi = 0; bi < positions.Count; bi++)
            {
                var pA = positions[bi];
                var idA = bi < ids.Count ? ids[bi] : null;
                var vA = bi < vers.Count ? vers[bi] : null;
                if (pA is null || idA is null)
                    continue; // no resolvable ids in this batch — a fresh append can't hold our rows
                for (int i = 0; i < pA.Length; i++)
                {
                    if (idA.IsNull(i))
                        continue;
                    long stable = idA.GetValue(i)!.Value;
                    if (!remaining.Contains(stable))
                        continue;
                    long newVer = vA is not null && !vA.IsNull(i) ? vA.GetValue(i)!.Value : long.MaxValue;
                    if (newVer != targetVersions[stable])
                        return null; // the row was concurrently updated (its commit version advanced)
                    if (!assignments.TryGetValue(cand.Path, out var slot))
                        assignments[cand.Path] = slot = (cand, new HashSet<long>());
                    slot.Positions.Add(pA.GetValue(i)!.Value);
                    remaining.Remove(stable);
                }
            }
        }
        if (remaining.Count > 0)
            return null; // some rows were not found after the rewrite — concurrently deleted

        // 3. Build remove/add deletion-vector pairs on the new files.
        var dvWriter = new DeletionVectors.DeletionVectorWriter(_fs);
        var result = new List<DeltaAction>(assignments.Count * 2);
        foreach (var kv in assignments)
        {
            var (cand, positions) = kv.Value;
            resolvedPaths.Add(cand.Path); // checker: this new file's concurrent add is reconciled, not foreign

            var deleted = cand.DeletionVector is not null
                ? new HashSet<long>(await _dvReader.ReadAsync(cand.DeletionVector, cancellationToken)
                    .ConfigureAwait(false))
                : new HashSet<long>();
            foreach (long p in positions)
                deleted.Add(p);
            var newDv = await dvWriter.CreateAsync(deleted, deleted.Count, cancellationToken)
                .ConfigureAwait(false);

            result.Add(new RemoveFile
            {
                Path = cand.Path,
                DeletionTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                DataChange = true,
                DeletionVector = cand.DeletionVector,
            });
            result.Add(cand with
            {
                DeletionVector = newDv,
                DataChange = true,
            });
        }
        return result;
    }

    /// <summary>
    /// Re-derives the row-tracking ids of a rebasing transaction's POST-IMAGE adds against the snapshot it is
    /// now landing on. A fresh add (an append, or an UPDATE's copy-on-write rewrite output) reserved its
    /// <c>baseRowId</c> from the STALE base high-water mark; a concurrent commit that landed in between may
    /// have consumed row-id space, so committing verbatim would assign an already-used id. This reassigns each
    /// post-image add's <c>baseRowId</c> from <paramref name="latestSnapshot"/>'s high-water mark and its
    /// <c>defaultRowCommitVersion</c> to <paramref name="attemptVersion"/>, and rebuilds the
    /// <c>delta.rowTracking</c> high-water-mark domain to match — mirroring Spark's row-id reassignment on
    /// conflict resolution and pr-4's <c>RebaseDvDmlActionsAsync</c>.
    ///
    /// <para>A post-image add is a data-change <see cref="AddFile"/> carrying a <c>baseRowId</c> whose path is
    /// NOT active in <paramref name="baseSnapshot"/> and was NOT produced by the row-level DELETE resolution
    /// (<paramref name="resolvedPaths"/>). Those excluded adds — a DV re-union re-add of an existing file, or a
    /// remap re-add on a concurrently-rewritten file — already carry the correct (their own) <c>baseRowId</c>
    /// and must be left untouched.</para>
    /// </summary>
    private static List<DeltaAction> RebaseRowTrackingAddIds(
        IReadOnlyList<DeltaAction> actions,
        Snapshot.Snapshot baseSnapshot,
        Snapshot.Snapshot latestSnapshot,
        long attemptVersion,
        ISet<string>? resolvedPaths)
    {
        var baseActivePaths = new HashSet<string>(StringComparer.Ordinal);
        foreach (var f in baseSnapshot.ActiveFiles.Values)
            baseActivePaths.Add(f.Path);

        long nextRowId = latestSnapshot.RowIdHighWaterMark;
        bool changed = false;
        var result = new List<DeltaAction>(actions.Count);
        foreach (var action in actions)
        {
            switch (action)
            {
                case AddFile add when add.DataChange && add.BaseRowId is not null
                    && !baseActivePaths.Contains(add.Path)
                    && (resolvedPaths is null || !resolvedPaths.Contains(add.Path)):
                    result.Add(add with
                    {
                        BaseRowId = nextRowId,
                        DefaultRowCommitVersion = attemptVersion,
                    });
                    nextRowId += ColumnStats.Parse(add.Stats)?.NumRecords ?? 0;
                    changed = true;
                    break;

                case DomainMetadata dm when string.Equals(
                    dm.Domain, DeltaLake.RowTracking.RowTrackingConfig.DomainName, StringComparison.Ordinal):
                    changed = true; // drop; re-emitted below with the re-derived mark
                    break;

                default:
                    result.Add(action);
                    break;
            }
        }

        // Re-emit exactly one high-water-mark domain reflecting the reassigned ids. When nothing was
        // reassigned (no post-image add) this restores it at the unchanged mark — a harmless idempotent commit.
        if (changed)
            result.Add(DeltaLake.RowTracking.RowTrackingConfig.BuildHighWaterMarkAction(nextRowId));

        return result;
    }

    #endregion

    #region Delete and Update

    // The row-level predicate evaluator turning an analyzable Expressions.Predicate into the
    // Func<RecordBatch, BooleanArray> mask the DELETE/UPDATE machinery consumes. Stateless (no function
    // registry), so one shared instance is safe. Evaluates by LOGICAL column name, which is exactly what
    // the compute paths hand the predicate (they rename batches to logical names first).
    private static readonly Expressions.Arrow.ArrowRowEvaluator RowEvaluator = new();

    /// <summary>Adapts an analyzable predicate to the per-row mask delegate: a row is selected when the
    /// predicate evaluates to TRUE (SQL three-valued logic — NULL/unknown is not selected).</summary>
    internal static Func<RecordBatch, BooleanArray> MaskFor(Expressions.Predicate predicate) =>
        batch => RowEvaluator.EvaluatePredicate(predicate, batch);

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

        // Route the single-shot DELETE through the optimistic-concurrency loop: a DELETE that races a
        // concurrent commit rebases and retries (when nothing it read was removed) instead of failing on
        // the version collision, and aborts with a DeltaConflictException only on a real conflict
        // (delete/delete on the same file, or a concurrent metadata/protocol change). When no rows match,
        // nothing is staged and CommitAsync returns the unchanged read version. Write preconditions are
        // validated by the transaction's DeleteAsync (against the same pinned base snapshot).
        var transaction = StartTransaction();
        long rowsDeleted = await transaction.DeleteAsync(predicate, cancellationToken)
            .ConfigureAwait(false);
        long version = await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        return (rowsDeleted, version);
    }

    /// <summary>
    /// Deletes rows matching an analyzable <see cref="Expressions.Predicate"/>. Beyond the functional
    /// overload this gives the writer a predicate it can reason about: files whose statistics prove no row
    /// matches are skipped without being read, and — because the predicate is recorded as the operation's
    /// read-set — a concurrent commit that adds a file matching it is detected as a conflict
    /// (concurrentAppend). Under the default <see cref="IsolationLevel.WriteSerializable"/> a concurrent
    /// blind append is still exempt; under <see cref="IsolationLevel.Serializable"/> it conflicts.
    /// Returns the number of rows deleted and the committed version.
    /// </summary>
    public async ValueTask<(long RowsDeleted, long Version)> DeleteAsync(
        Expressions.Predicate predicate,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        var transaction = StartTransaction();
        long rowsDeleted = await transaction.DeleteAsync(predicate, cancellationToken)
            .ConfigureAwait(false);
        long version = await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        return (rowsDeleted, version);
    }

    /// <summary>The remove/add (and CDC) actions a DELETE produces, its removed-file paths, the row
    /// count, and the per-file row-level edits — everything a commit needs, but without committing. Shared
    /// by the auto-committing <see cref="DeleteAsync"/> and the transactional <see cref="DeltaTransaction"/>
    /// path.</summary>
    internal sealed record DeleteActions(
        IReadOnlyList<DeltaAction> DataActions, ISet<string> RemovedPaths, long TotalDeleted,
        IReadOnlyList<DeleteDvEdit> DvEdits);

    /// <summary>
    /// The rows one DELETE newly marked deleted in one file, by absolute row position. Deletion vectors
    /// mark rows in place — they never move a surviving row — so these positions stay valid even after a
    /// concurrent DV-delete of the same file. That stability is what lets row-level concurrency rebase a
    /// losing delete's deletion vector onto the winner's (union the two) instead of aborting: see
    /// <see cref="ResolveRowLevelDeletesAsync"/>.
    /// </summary>
    internal sealed record DeleteDvEdit(string Path, IReadOnlyList<long> NewlyDeletedRows);

    /// <summary>
    /// Computes the actions for a DELETE against <paramref name="snapshot"/> WITHOUT committing. The
    /// removed-file paths double as a transaction's read-set: a DELETE reads exactly the files it
    /// rewrites, so a concurrent commit that removed one of them is the conflict that must abort it.
    /// <para>When <paramref name="prunePredicate"/> is supplied (the analyzable-predicate overloads pass
    /// it), files whose statistics prove no row can match are skipped without being opened. This never
    /// changes the removed-file set — a pruned file could not have contained a matching row, so it would
    /// not have been rewritten anyway — it only avoids reading files that cannot contribute.</para>
    /// </summary>
    internal async ValueTask<DeleteActions> ComputeDeleteActionsAsync(
        Snapshot.Snapshot snapshot,
        Func<RecordBatch, BooleanArray> predicate,
        CancellationToken cancellationToken,
        Expressions.Predicate? prunePredicate = null)
    {
        var dvWriter = new DeletionVectors.DeletionVectorWriter(_fs);
        var actions = new List<DeltaAction>();
        var removedPaths = new HashSet<string>(StringComparer.Ordinal);
        var dvEdits = new List<DeleteDvEdit>();
        long totalDeleted = 0;
        bool cdfEnabled = DeltaLake.ChangeDataFeed.CdfConfig.IsEnabled(
            snapshot.Metadata.Configuration);
        // Deletion vectors are opt-in. When disabled, a DELETE may only remove WHOLE files (a clean
        // file/partition boundary); a partial match that would need a soft-delete throws below.
        bool deletionVectorsEnabled = DeletionVectors.DeletionVectorConfig.IsEnabled(
            snapshot.Metadata.Configuration);
        var pruner = prunePredicate is null ? null : new DeltaFilePruner(
            snapshot.Schema, snapshot.Metadata.PartitionColumns);

        foreach (var addFile in snapshot.ActiveFiles.Values)
        {
            if (pruner is not null && !pruner.ShouldInclude(addFile, prunePredicate!))
                continue; // stats prove no row here matches — nothing to delete, skip the read

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
                file, ownsFile: false, _dataFileReadOptions);

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

            // Whole-file delete: every physical row is now gone, so the file can be dropped outright — a
            // metadata-only remove needing no deletion vector, valid even when DVs are not enabled. When a
            // file is only PARTIALLY matched a soft-delete is unavoidable, which requires DVs; without them
            // enabled the delete is rejected rather than writing a vector a foreign reader would ignore.
            bool wholeFile = allDeleted.Count == rowOffset;

            if (!wholeFile && !deletionVectorsEnabled)
            {
                throw new InvalidOperationException(
                    "DELETE would soft-delete part of a data file, which requires deletion vectors. Create "
                    + "the table with DeltaTable.CreateAsync(..., enableDeletionVectors: true), or restrict "
                    + "the predicate so it removes whole files/partitions (which needs no deletion vector).");
            }

            actions.Add(new RemoveFile
            {
                Path = addFile.Path,
                DeletionTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                DataChange = true,
                DeletionVector = addFile.DeletionVector,
            });
            removedPaths.Add(addFile.Path);

            if (!wholeFile)
            {
                var newDv = await dvWriter.CreateAsync(
                    allDeleted, allDeleted.Count, cancellationToken).ConfigureAwait(false);

                actions.Add(addFile with
                {
                    DeletionVector = newDv,
                    DataChange = true,
                });

                // Record the exact rows this delete added (absolute positions), so a concurrent DV-delete
                // of the same file can be reconciled row-by-row rather than aborting the whole file. A
                // whole-file remove has no surviving rows to reconcile, so it records no edit (a concurrent
                // delete of that file is a genuine file-level conflict).
                dvEdits.Add(new DeleteDvEdit(addFile.Path, newDeletedIndices));
            }

            // Write CDC file for deleted rows
            if (cdfEnabled)
            {
                foreach (var deletedBatch in deletedRowBatches)
                {
                    var cdcAction = await ChangeDataFeed.CdfWriter.WriteAsync(
                        _fs, snapshot, deletedBatch, DeltaLake.ChangeDataFeed.CdfConfig.Delete,
                        addFile.PartitionValues, _options.ParquetWriteOptions,
                        cancellationToken).ConfigureAwait(false);
                    actions.Add(cdcAction);
                }
            }
        }

        return new DeleteActions(actions, removedPaths, totalDeleted, dvEdits);
    }

    /// <summary>
    /// Updates rows matching the predicate. The <paramref name="updater"/> function
    /// receives matching rows and returns modified rows. Non-matching rows are
    /// preserved unchanged. Affected files are rewritten.
    /// Returns the number of rows updated and the committed version.
    /// </summary>
    public ValueTask<(long RowsUpdated, long Version)> UpdateAsync(
        Func<RecordBatch, BooleanArray> predicate,
        Func<RecordBatch, RecordBatch> updater,
        CancellationToken cancellationToken = default)
        => UpdateCoreAsync(predicate, updater, prunePredicate: null, readPredicates: [], cancellationToken);

    /// <summary>
    /// Updates rows matching an analyzable <see cref="Expressions.Predicate"/>. As with the analyzable
    /// <see cref="DeleteAsync(Expressions.Predicate, CancellationToken)"/>, files whose statistics prove no
    /// row matches are skipped without being read, and the predicate becomes the operation's read-set so a
    /// concurrent commit adding a file that matches it is a conflict (concurrentAppend), precise to the
    /// isolation level. Returns the number of rows updated and the committed version.
    /// </summary>
    public ValueTask<(long RowsUpdated, long Version)> UpdateAsync(
        Expressions.Predicate predicate,
        Func<RecordBatch, RecordBatch> updater,
        CancellationToken cancellationToken = default)
        => UpdateCoreAsync(MaskFor(predicate), updater, prunePredicate: predicate,
            readPredicates: [predicate], cancellationToken);

    private async ValueTask<(long RowsUpdated, long Version)> UpdateCoreAsync(
        Func<RecordBatch, BooleanArray> predicate,
        Func<RecordBatch, RecordBatch> updater,
        Expressions.Predicate? prunePredicate,
        IReadOnlyList<Expressions.Predicate> readPredicates,
        CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        var snapshot = CurrentSnapshot;
        ValidateWritable(snapshot, isAppend: false); // UPDATE is a data change

        var plan = await ComputeUpdateActionsAsync(
            snapshot, predicate, updater, cancellationToken, prunePredicate).ConfigureAwait(false);

        // An UPDATE reads exactly the files it rewrites, so — like DELETE — the removed paths are both its
        // read-set (concurrentDeleteRead) and its planned removes (delete/delete). The analyzable overload
        // additionally records its read predicate so a concurrent add that matches it conflicts. Route it
        // through the OCC loop so a single-shot UPDATE rebases past a non-conflicting concurrent commit
        // instead of throwing — its copy-on-write post-image add's row-tracking baseRowId is re-derived on
        // rebase (a conflict on any file it rewrote aborts first, so the survivors' ids stay valid).
        long committed = await CommitOccAsync(
            snapshot, plan.Actions,
            new Concurrency.ReadSet { Files = plan.RemovedPaths, Predicates = readPredicates },
            plan.RemovedPaths, IsolationLevel.WriteSerializable, "UPDATE",
            rebaseSafe: true, cancellationToken).ConfigureAwait(false);

        return (plan.TotalUpdated, committed);
    }

    /// <summary>The remove/add (and CDC) actions an UPDATE produces, the paths it rewrote, and the row
    /// count — everything a commit needs, without committing. Shared by the auto-committing
    /// <see cref="UpdateAsync"/> and the transactional <see cref="DeltaTransaction"/> path.</summary>
    internal sealed record UpdateActions(
        IReadOnlyList<DeltaAction> Actions, ISet<string> RemovedPaths, long TotalUpdated);

    /// <summary>
    /// Computes the actions for an UPDATE against <paramref name="snapshot"/> WITHOUT committing. Like a
    /// DELETE, the removed-file paths double as the read-set: an UPDATE reads exactly the files it rewrites,
    /// so a concurrent commit that removed one of them is the conflict that must abort it.
    /// <para><paramref name="prunePredicate"/> skips files whose statistics prove no row can match, exactly
    /// as in <see cref="ComputeDeleteActionsAsync"/> — a pruned file has no matching row to update, so the
    /// removed-file set is unchanged; only the read is avoided.</para>
    /// </summary>
    internal async ValueTask<UpdateActions> ComputeUpdateActionsAsync(
        Snapshot.Snapshot snapshot,
        Func<RecordBatch, BooleanArray> predicate,
        Func<RecordBatch, RecordBatch> updater,
        CancellationToken cancellationToken,
        Expressions.Predicate? prunePredicate = null)
    {
        var actions = new List<DeltaAction>();
        var removedPaths = new HashSet<string>(StringComparer.Ordinal);
        long totalUpdated = 0;
        bool cdfEnabled = DeltaLake.ChangeDataFeed.CdfConfig.IsEnabled(
            snapshot.Metadata.Configuration);
        var pruner = prunePredicate is null ? null : new DeltaFilePruner(
            snapshot.Schema, snapshot.Metadata.PartitionColumns);

        // ColumnMappingRecursive reads the physical names / field ids off the schema itself — no flat maps needed.
        var mappingMode = ColumnMapping.GetMode(snapshot.Metadata.Configuration);

        // Row tracking through the copy-on-write rewrite: an UPDATE moves every row of a modified file to a new
        // file, so a row's id can no longer be derived from position. Materialize each row's ORIGINAL id +
        // commit version into the declared hidden columns (a matched row's version advances to this commit; an
        // untouched-but-rewritten row keeps its original). A fresh baseRowId/defaultRowCommitVersion still goes
        // on the new add (spec: the null-materialized fallback).
        bool rowTrackingEnabled = DeltaLake.RowTracking.RowTrackingConfig.IsEnabled(
            snapshot.Metadata.Configuration);
        var (matRowIdName, matRowVerName) = DeltaLake.RowTracking.RowTrackingConfig
            .TryGetMaterializedColumnNames(snapshot.Metadata.Configuration);
        bool materializeIds = rowTrackingEnabled && matRowIdName is not null && matRowVerName is not null;
        long newVersion = snapshot.Version + 1;
        long nextRowId = rowTrackingEnabled ? snapshot.RowIdHighWaterMark : 0;

        foreach (var addFile in snapshot.ActiveFiles.Values)
        {
            if (pruner is not null && !pruner.ShouldInclude(addFile, prunePredicate!))
                continue; // stats prove no row here matches — nothing to update, skip the read

            // Read file data with DV filtering. When materializing row ids, ask ReadFileAsync to surface each
            // surviving row's resolved id + commit version, row-aligned per emitted batch.
            var batches = new List<RecordBatch>();
            var srcIds = materializeIds ? new List<Int64Array?>() : null;
            var srcVers = materializeIds ? new List<Int64Array?>() : null;
            await foreach (var batch in ReadFileAsync(
                addFile, null, snapshot, cancellationToken, srcIds, srcVers).ConfigureAwait(false))
            {
                batches.Add(batch);
            }

            if (batches.Count == 0)
                continue;

            // Evaluate predicate and apply updates
            bool fileModified = false;
            var outputBatches = new List<RecordBatch>();
            // Per output batch, the ORIGINAL id + commit version to materialize (null entry = no tracking). An
            // UPDATE keeps EVERY row (matched rows updated in place, the rest copied), so each output row carries
            // its source id; a matched row's version becomes newVersion, an untouched row keeps its original.
            var outTracking = materializeIds ? new List<(Int64Array Ids, Int64Array Vers)?>() : null;
            var preimages = new List<RecordBatch>();
            var postimages = new List<RecordBatch>();

            for (int bi = 0; bi < batches.Count; bi++)
            {
                var batch = batches[bi];
                var batchIds = srcIds is not null && bi < srcIds.Count ? srcIds[bi] : null;
                var batchVers = srcVers is not null && bi < srcVers.Count ? srcVers[bi] : null;
                var mask = predicate(batch);
                int matchCount = CountTrue(mask);

                if (matchCount == 0)
                {
                    outputBatches.Add(batch);
                    // Untouched but (once the file is modified) rewritten: keep every row's original id + version.
                    outTracking?.Add(batchIds is not null && batchVers is not null
                        ? (batchIds, batchVers) : ((Int64Array, Int64Array)?)null);
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
                    // Matched rows keep their id; their commit version advances to this commit (they changed).
                    outTracking?.Add(batchIds is not null
                        ? (TakeIds(batchIds, matchRows), ConstInt64(newVersion, matchRows.Count))
                        : ((Int64Array, Int64Array)?)null);

                    // Collect preimage and postimage for CDC
                    if (cdfEnabled)
                    {
                        preimages.Add(matchBatch);
                        postimages.Add(updatedBatch);
                    }
                }

                if (keepRows.Count > 0)
                {
                    outputBatches.Add(TakeRowsFromBatch(batch, keepRows));
                    // Untouched rows in a modified file: original id + original commit version.
                    outTracking?.Add(batchIds is not null && batchVers is not null
                        ? (TakeIds(batchIds, keepRows), TakeIds(batchVers, keepRows))
                        : ((Int64Array, Int64Array)?)null);
                }
            }

            if (!fileModified)
                continue;

            // Write new file with all output batches. The rewritten file joins its source's partition
            // directory (a partitioned table's data must live under its Hive dir, matching the append path);
            // reuse the source path's ENCODED prefix verbatim for the add — never re-encode, which would
            // double-encode a non-ASCII partition value — and its DECODED form for the physical write. An
            // empty prefix means an unpartitioned table (files at the root). Mirrors the compaction rewrite.
            string encodedDir = "";
            int dirSlash = addFile.Path.LastIndexOf('/');
            if (dirSlash >= 0)
                encodedDir = addFile.Path.Substring(0, dirSlash + 1);
            string baseName = $"{Guid.NewGuid():N}.parquet";
            string newFileName = EngineeredWood.DeltaLake.DeltaPath.Decode(encodedDir) + baseName;
            long fileSize;

            // Physical names + parquet field ids at EVERY level (nested struct children included — the
            // top-level-only rename/stamp pair left them logical-named and id-less). When row tracking is on,
            // append the materialized id + commit-version columns (declared physical names) carrying each moved
            // row's ORIGINAL values. Prepared up front so both the built-in and pluggable writers see the same
            // batches.
            var writeBatches = new List<RecordBatch>(outputBatches.Count);
            for (int k = 0; k < outputBatches.Count; k++)
            {
                var physicalBatch = ColumnMappingRecursive.ToPhysical(
                    outputBatches[k], snapshot.Schema, mappingMode);
                // Drop the VARIANT annotation for a Spark 4.0.x-compatible table (bytes unchanged; the
                // read path recovers the type from the schema). Stats use outputBatches, not these.
                if (!_options.EmitVariantLogicalType)
                    physicalBatch = VariantColumnCoercion.StripAnnotation(physicalBatch);
                if (materializeIds && outTracking![k] is { } trk)
                {
                    physicalBatch = RowTracking.RowTrackingWriter.AddRowIdAndCommitVersionColumns(
                        physicalBatch, trk.Ids, trk.Vers, matRowIdName!, matRowVerName!, nullable: true);
                }
                writeBatches.Add(physicalBatch);
            }

            if (_options.DataFileWriter is { } rewriteWriter)
            {
                fileSize = await rewriteWriter.WriteAsync(
                    writeBatches.ToAsyncEnumerable(), newFileName, cancellationToken).ConfigureAwait(false);
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
            removedPaths.Add(addFile.Path);

            long addedRows = 0;
            foreach (var ob in outputBatches)
                addedRows += ob.Length;

            actions.Add(new AddFile
            {
                Path = encodedDir + baseName, // encoded prefix reused verbatim (see newFileName above)
                PartitionValues = addFile.PartitionValues,
                Size = fileSize,
                ModificationTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                DataChange = true,
                Stats = stats,
                // Fresh baseRowId reserves an id range for any null-materialized fallback row; every row here
                // actually carries its original id in the materialized column, which overrides it.
                BaseRowId = rowTrackingEnabled ? nextRowId : null,
                DefaultRowCommitVersion = rowTrackingEnabled ? newVersion : null,
            });
            if (rowTrackingEnabled)
                nextRowId += addedRows;

            // Write CDC files for update preimage/postimage
            if (cdfEnabled)
            {
                foreach (var pre in preimages)
                {
                    var cdcAction = await ChangeDataFeed.CdfWriter.WriteAsync(
                        _fs, snapshot, pre, DeltaLake.ChangeDataFeed.CdfConfig.UpdatePreimage,
                        addFile.PartitionValues, _options.ParquetWriteOptions,
                        cancellationToken).ConfigureAwait(false);
                    actions.Add(cdcAction);
                }
                foreach (var post in postimages)
                {
                    var cdcAction = await ChangeDataFeed.CdfWriter.WriteAsync(
                        _fs, snapshot, post, DeltaLake.ChangeDataFeed.CdfConfig.UpdatePostimage,
                        addFile.PartitionValues, _options.ParquetWriteOptions,
                        cancellationToken).ConfigureAwait(false);
                    actions.Add(cdcAction);
                }
            }
        }

        // Persist the advanced row-id high-water mark (same source of truth the append path maintains).
        if (rowTrackingEnabled && nextRowId > snapshot.RowIdHighWaterMark)
            actions.Add(DeltaLake.RowTracking.RowTrackingConfig.BuildHighWaterMarkAction(nextRowId));

        return new UpdateActions(actions, removedPaths, totalUpdated);
    }

    // Builds an Int64 array holding src[idx[0]], src[idx[1]], … preserving nulls — used to reorder/subset a
    // resolved row-id (or commit-version) array to match a rewritten batch's row order.
    private static Int64Array TakeIds(Int64Array src, List<int> idx)
    {
        var b = new Int64Array.Builder();
        foreach (int i in idx)
        {
            if (src.IsNull(i)) b.AppendNull();
            else b.Append(src.GetValue(i)!.Value);
        }
        return b.Build();
    }

    // Builds a constant Int64 array of length n (the commit version assigned to every matched/updated row).
    private static Int64Array ConstInt64(long value, int n)
    {
        var b = new Int64Array.Builder();
        for (int i = 0; i < n; i++) b.Append(value);
        return b.Build();
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
        var snapshot = CurrentSnapshot;
        return ChangeDataFeed.CdfReader.ReadChangesAsync(
            _fs, _log, startVersion, endVersion, _dataFileReadOptions,
            snapshot.ArrowSchema, snapshot.Schema,
            ColumnMapping.GetMode(snapshot.Metadata.Configuration),
            snapshot.Metadata.PartitionColumns,
            cancellationToken);
    }

    /// <summary>
    /// Writes a Change Data Feed <c>_change_data</c> parquet file for <paramref name="rows"/> WITHOUT committing —
    /// the write counterpart of <see cref="ReadChangesAsync"/>, and the CDC half of the buffered-transaction seam.
    /// The returned <see cref="CdcFile"/> action is the caller's to fuse into a later commit via
    /// <see cref="CommitDataFilesAsync"/>' <c>extraActions</c>, so a multi-statement transaction that captures its
    /// change rows eagerly (they are in hand at statement time) lands them in the SAME atomic version as its data
    /// files. <paramref name="changeType"/> must be one of <c>insert</c> / <c>delete</c> / <c>update_preimage</c> /
    /// <c>update_postimage</c> (see <see cref="ChangeDataFeed.CdfConfig"/>); the <c>_change_type</c> column is
    /// added for you. <paramref name="rows"/> carry the feed's user columns (a partitioned table's partition
    /// values ride on <paramref name="partitionValues"/>, physical-keyed like a data file). Requires the table to
    /// have Change Data Feed enabled — a CDC file on a non-CDF table would be dead weight no reader consults.
    /// </summary>
    /// <remarks>
    /// Follows engineered-wood's spec-conformant CDF on-disk layout (the same one the auto DELETE/UPDATE paths
    /// write and <see cref="ReadChangesAsync"/> reads back): on a column-mapping table the row bytes are stored
    /// under PHYSICAL names + parquet field ids, exactly like data files, so Spark's <c>table_changes</c> and
    /// delta-kernel resolve the feed correctly; <see cref="ReadChangesAsync"/> maps them back to logical and
    /// re-materializes the partition columns.
    /// </remarks>
    public async ValueTask<CdcFile> WriteChangeDataFileAsync(
        RecordBatch rows, string changeType,
        IReadOnlyDictionary<string, string>? partitionValues = null,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ProtocolVersions.ValidateWriteSupport(CurrentSnapshot.Protocol);
        if (rows is null)
            throw new ArgumentNullException(nameof(rows));
        if (changeType is not (DeltaLake.ChangeDataFeed.CdfConfig.Insert
            or DeltaLake.ChangeDataFeed.CdfConfig.Delete
            or DeltaLake.ChangeDataFeed.CdfConfig.UpdatePreimage
            or DeltaLake.ChangeDataFeed.CdfConfig.UpdatePostimage))
        {
            throw new ArgumentException(
                $"changeType must be one of 'insert', 'delete', 'update_preimage', 'update_postimage' "
                + $"(got '{changeType}').", nameof(changeType));
        }
        if (!DeltaLake.ChangeDataFeed.CdfConfig.IsEnabled(CurrentSnapshot.Metadata.Configuration))
        {
            throw new InvalidOperationException(
                "Change Data Feed is not enabled on this table — a _change_data file would never be read. "
                + "Create the table with the 'delta.enableChangeDataFeed' property set to 'true'.");
        }

        return await ChangeDataFeed.CdfWriter.WriteAsync(
            _fs, CurrentSnapshot, rows, changeType,
            partitionValues ?? EmptyPartitionValues,
            _options.ParquetWriteOptions, cancellationToken).ConfigureAwait(false);
    }

    private static readonly IReadOnlyDictionary<string, string> EmptyPartitionValues =
        new Dictionary<string, string>();

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

    // ── Read-side transient row ids ────────────────────────────────────────────────────────────────────
    //
    // A read that appends a trailing non-null Int64 _metadata.row_id = (fileOrdinal << RowIdPositionBits) |
    // ABSOLUTE in-file position (path-sorted active set; the DV-inclusive parquet row index). NOT a stable
    // Delta row id — valid only within one snapshot. It round-trips to the row-id DML surface
    // (ComputeDeletionVectorActionsAsync / ReadRowsByRowIdsAsync consume the same (ordinal, absPos)), so a host
    // (e.g. DuckDB) can read rows, keep the ids, then delete/update exactly those rows — even on a plain table
    // with no deletion vectors or row-tracking feature, the maximally reader-compatible path.

    /// <summary>
    /// The active files' <c>baseRowId</c>s in TRANSIENT-ROWID ORDINAL order (the path-sorted active set — the
    /// same ordering the rowid encoding uses), for the snapshot pinned by <paramref name="atVersion"/>. A host's
    /// eager UPDATE resolves each matched row's ORIGINAL stable id as <c>baseRowId[ordinal] + position</c>.
    /// </summary>
    public async ValueTask<IReadOnlyList<long?>> OrderedActiveBaseRowIdsAsync(
        long? atVersion = null, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        var snapshot = atVersion is { } v && v != CurrentSnapshot.Version
            ? await GetSnapshotAtVersionAsync(v, cancellationToken).ConfigureAwait(false)
            : CurrentSnapshot;
        var ordered = OrderedActiveFiles(snapshot);
        var ids = new List<long?>(ordered.Count);
        foreach (var f in ordered)
            ids.Add(f.BaseRowId);
        return ids;
    }

    /// <summary>
    /// Like <see cref="ReadAllAsync(IReadOnlyList{string}, EngineeredWood.Expressions.Predicate, CancellationToken)"/>
    /// but appends a trailing non-null Int64 <c>_metadata.row_id</c> = a TRANSIENT rowid
    /// <c>(fileOrdinal &lt;&lt; RowIdPositionBits) | absolutePosition</c>. NOT a stable Delta row id — it
    /// round-trips to the row-id DML surface within the SAME snapshot so a host can locate the rows it read
    /// (a plain copy-on-write DELETE needs no deletion vectors or row tracking).
    /// </summary>
    public IAsyncEnumerable<RecordBatch> ReadAllWithRowIdsAsync(
        IReadOnlyList<string>? columns,
        EngineeredWood.Expressions.Predicate? filter,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        return ReadWithTransientRowIdsAsync(CurrentSnapshot, columns, filter, cancellationToken);
    }

    /// <summary>
    /// Time travel WITH the transient rowid column — the version analog of <see cref="ReadAllWithRowIdsAsync"/>.
    /// Each batch carries the trailing <c>_metadata.row_id</c> over the version's path-sorted active files.
    /// </summary>
    public async IAsyncEnumerable<RecordBatch> ReadAtVersionWithRowIdsAsync(
        long version,
        IReadOnlyList<string>? columns,
        EngineeredWood.Expressions.Predicate? filter,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        var snapshot = await GetSnapshotAtVersionAsync(version, cancellationToken).ConfigureAwait(false);
        await foreach (var batch in ReadWithTransientRowIdsAsync(snapshot, columns, filter, cancellationToken)
                           .ConfigureAwait(false))
        {
            yield return batch;
        }
    }

    // Shared iterator: path-sorted active files, each emitted batch carrying the trailing _metadata.row_id built
    // from ReadFileAsync's absolute-position out-param (master surfaces positions as an out-param rather than an
    // appended column, so the wrapper appends the transient id itself — keeping ReadFileAsync's read path intact).
    private async IAsyncEnumerable<RecordBatch> ReadWithTransientRowIdsAsync(
        Snapshot.Snapshot snapshot,
        IReadOnlyList<string>? columns,
        EngineeredWood.Expressions.Predicate? filter,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var pruner = filter is null ? null : new DeltaFilePruner(
            snapshot.Schema, snapshot.Metadata.PartitionColumns);
        var ordered = OrderedActiveFiles(snapshot);
        for (int ordinal = 0; ordinal < ordered.Count; ordinal++)
        {
            var addFile = ordered[ordinal];
            if (pruner is not null && !pruner.ShouldInclude(addFile, filter!))
                continue;

            var absOut = new List<Int64Array?>();
            int bi = -1;
            await foreach (var batch in ReadFileAsync(addFile, columns, snapshot, cancellationToken,
                                                      strippedAbsPositionsOut: absOut).ConfigureAwait(false))
            {
                bi++;
                var absPos = bi < absOut.Count ? absOut[bi] : null;
                var idb = new Int64Array.Builder();
                for (int i = 0; i < batch.Length; i++)
                {
                    long absolute = absPos is not null && i < absPos.Length && !absPos.IsNull(i)
                        ? absPos.GetValue(i)!.Value : i;
                    idb.Append(((long)ordinal << RowIdPositionBits) | absolute);
                }
                yield return RowTracking.RowTrackingWriter.AddRowIdColumn(
                    batch, idb.Build(), DeltaLake.RowTracking.RowTrackingConfig.VirtualRowIdColumn);
            }
        }
    }

    /// <summary>
    /// Writes RecordBatch data as a new commit.
    /// Returns the committed version number.
    /// </summary>
    /// <summary>
    /// The write preconditions every data-changing operation shares, validated against the snapshot the
    /// operation reads from (the transaction's pinned base, or the table's current snapshot for the
    /// auto-committers): the protocol must be writable by this library, and the table's actively-declared
    /// writer features must be honored. Kept together so a transactional append/update/delete runs the same
    /// gate as its single-shot equivalent instead of skipping it.
    /// </summary>
    internal void ValidateWritable(Snapshot.Snapshot snapshot, bool isAppend)
    {
        ProtocolVersions.ValidateWriteSupport(snapshot.Protocol);
        // Appends to a row-tracking table are spec-conformant (baseRowId + position). A copy-on-write rewrite
        // (UPDATE / OVERWRITE / DELETE) now materializes each surviving row's ORIGINAL id + commit version into
        // the declared hidden columns — but only when those column names are present in the metadata. A
        // row-tracking table missing them (spec-invalid) cannot materialize, so a rewrite is still refused.
        if (!isAppend)
            RejectRowTrackingWrite(snapshot);
        HonorWriterFeatures(snapshot, isAppend);
    }

    /// <summary>
    /// Refuses a copy-on-write REWRITE of a row-tracking table ONLY when the two spec-required materialized
    /// column names (<c>delta.rowTracking.materializedRowIdColumnName</c> /
    /// <c>…materializedRowCommitVersionColumnName</c>) are absent from the metadata. With the names present —
    /// as every table <see cref="CreateAsync"/> enables row tracking on has them — an UPDATE / OVERWRITE /
    /// compaction preserves stable row ids by materializing each moved row's original id + commit version, so
    /// the rewrite is allowed. Without them EngineeredWood cannot know which physical column to write, so a
    /// rewrite would corrupt the row-id invariants a conformant engine (Spark, Databricks) relies on.
    /// </summary>
    private static void RejectRowTrackingWrite(Snapshot.Snapshot snapshot)
    {
        if (!DeltaLake.RowTracking.RowTrackingConfig.IsEnabled(snapshot.Metadata.Configuration))
            return;
        var (rowIdName, rowVerName) = DeltaLake.RowTracking.RowTrackingConfig
            .TryGetMaterializedColumnNames(snapshot.Metadata.Configuration);
        if (rowIdName is null || rowVerName is null)
        {
            throw new NotSupportedException(
                "Rewriting a row-tracking table (delta.enableRowTracking=true) that does not declare its "
                + "materialized row-id / row-commit-version column names is not supported: EngineeredWood "
                + "cannot preserve stable row IDs through a copy-on-write rewrite without them. Appending to "
                + "and reading such a table is supported.");
        }
    }

    /// <summary>
    /// Enforces the writer features a table ACTIVELY declares (Delta constraints are write-time only, so a
    /// violating commit would poison the table for every reader). <c>delta.appendOnly=true</c> blocks non-append
    /// data changes; <c>delta.constraints.*</c> / <c>delta.invariants</c> / <c>delta.generationExpression</c>
    /// carry arbitrary SQL this writer cannot evaluate, so an ACTIVE one rejects the write. A table that merely
    /// LISTS these features in its writer-v7 protocol (the common case) is unaffected.
    /// </summary>
    private static void HonorWriterFeatures(Snapshot.Snapshot snapshot, bool isAppend)
    {
        var cfg = snapshot.Metadata.Configuration;
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
        foreach (var field in snapshot.ArrowSchema.FieldsList)
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
        var snapshot = CurrentSnapshot;
        // A dynamic partition overwrite removes files, so it is NOT an append for appendOnly enforcement.
        ValidateWritable(snapshot, isAppend: mode == DeltaWriteMode.Append && !dynamicPartitionOverwrite);

        var actions = await ComputeWriteActionsAsync(
            snapshot, batches, mode, overwritePartitions, dynamicPartitionOverwrite, repartitionTo,
            cancellationToken).ConfigureAwait(false);

        bool rowTrackingEnabled = DeltaLake.RowTracking.RowTrackingConfig.IsEnabled(
            snapshot.Metadata.Configuration);
        long newVersion = snapshot.Version + 1;
        return await CommitWriteAsync(
            snapshot, actions, mode, dynamicPartitionOverwrite, newVersion,
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Computes the full action list for a write against <paramref name="snapshot"/> WITHOUT committing:
    /// pre-commit removes (overwrite family), the per-batch adds (identity, row tracking, column mapping,
    /// partition split, stats), dynamic-overwrite removes, and any identity/repartition metaData +
    /// row-tracking high-water-mark action. Shared by the auto-committing <see cref="WriteCoreAsync"/> and
    /// the append path of <see cref="DeltaTransaction"/> — the transaction only calls it with
    /// <see cref="DeltaWriteMode.Append"/>, so the overwrite branches stay inert there.
    /// </summary>
    internal async ValueTask<IReadOnlyList<DeltaAction>> ComputeWriteActionsAsync(
        Snapshot.Snapshot snapshot,
        IReadOnlyList<RecordBatch> batches,
        DeltaWriteMode mode,
        IReadOnlyDictionary<string, string>? overwritePartitions,
        bool dynamicPartitionOverwrite,
        IReadOnlyList<string>? repartitionTo,
        CancellationToken cancellationToken)
    {
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

                // Assign row IDs if row tracking is enabled. A freshly-appended file needs ONLY
                // add.baseRowId + add.defaultRowCommitVersion (set on the AddFile below): a row's stable id is
                // baseRowId + its physical position, and its commit version is the file's default. NO
                // materialized column is written — that is reserved for rows RELOCATED by a copy-on-write
                // rewrite (deferred). Materializing one here produced a non-spec physical column a foreign
                // reader would surface as a stray column.
                long fileBaseRowId = nextRowId;
                if (rowTrackingEnabled)
                {
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

                // For a Spark 4.0.x-compatible table, drop the VARIANT logical-type annotation by writing
                // the bare storage struct. Bytes are identical; only the parquet schema differs, and the
                // read path recovers the variant type from the Delta schema. Stats above use dataBatch, so
                // this does not touch them.
                var writeBatch = _options.EmitVariantLogicalType
                    ? physicalBatch
                    : VariantColumnCoercion.StripAnnotation(physicalBatch);

                if (_options.DataFileWriter is { } dataFileWriter)
                {
                    // Delegate the parquet bytes to the host writer; it places the file at the location the
                    // table filesystem maps `fileName` to and returns its byte size.
                    fileSize = await dataFileWriter.WriteAsync(
                        new[] { writeBatch }.ToAsyncEnumerable(), fileName, cancellationToken)
                        .ConfigureAwait(false);
                }
                else
                {
                    await using var file = await _fs.CreateAsync(
                        fileName, cancellationToken: cancellationToken).ConfigureAwait(false);
                    await using var writer = new ParquetFileWriter(
                        file, ownsFile: false, _options.ParquetWriteOptions);
                    await writer.WriteRowGroupAsync(writeBatch, cancellationToken)
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

        return actions;
    }

    /// <summary>
    /// Commits the actions a write produced. A pure (blind) append has no read dependency, so it goes
    /// through the optimistic-concurrency loop — rebasing past a non-conflicting concurrent commit,
    /// aborting only on a concurrent metadata/protocol change — instead of failing on a version collision.
    /// The overwrite family (full / partition-scoped / dynamic) reads the active-file set to decide what to
    /// remove, so its removes are NOT rebase-safe without partition-predicate plumbing; it keeps the
    /// single-attempt commit (a collision still throws, as before). A row-tracking append IS rebase-safe now:
    /// its fresh file's <c>baseRowId</c> is re-derived against the advanced high-water mark inside the OCC loop.
    /// </summary>
    private async ValueTask<long> CommitWriteAsync(
        Snapshot.Snapshot snapshot,
        IReadOnlyList<DeltaAction> actions,
        DeltaWriteMode mode,
        bool dynamicPartitionOverwrite,
        long newVersion,
        CancellationToken cancellationToken)
    {
        long committedVersion;
        bool blindAppend = mode == DeltaWriteMode.Append && !dynamicPartitionOverwrite;
        if (blindAppend)
        {
            committedVersion = await CommitOccAsync(
                snapshot, actions, Concurrency.ReadSet.Blind, NoRemovedPaths,
                IsolationLevel.WriteSerializable, "WRITE", rebaseSafe: true,
                cancellationToken).ConfigureAwait(false);
        }
        else
        {
            // Overwrite family: a single atomic attempt at the read version + 1 (unchanged behavior).
            var finalActions = Log.InCommitTimestamp.EnsureCommitInfo(
                actions, snapshot.Metadata.Configuration, "WRITE");
            await _log.WriteCommitAsync(newVersion, finalActions, cancellationToken)
                .ConfigureAwait(false);
            _currentSnapshot = await SnapshotBuilder.UpdateAsync(
                snapshot, _log, cancellationToken).ConfigureAwait(false);
            committedVersion = newVersion;
        }

        // Auto-checkpoint on the version that actually committed (a rebased append may differ from the
        // read version + 1). Skipped when nothing was staged (an all-empty append returns the read
        // version without committing).
        if (committedVersion > snapshot.Version &&
            _options.CheckpointInterval > 0 &&
            committedVersion % _options.CheckpointInterval == 0)
        {
            await _checkpointWriter.WriteCheckpointAsync(
                CurrentSnapshot, cancellationToken).ConfigureAwait(false);
        }

        return committedVersion;
    }

    // ── Buffered-transaction seam ──────────────────────────────────────────────────────────────────────
    //
    // WriteDataFilesAsync writes append-shaped data files WITHOUT committing (invisible orphans until
    // referenced); CommitDataFilesAsync commits those files — optionally FUSED with a caller's extraActions
    // (DML deletion-vector remove/add pairs, a schema metaData change) — into ONE atomic Delta version. The
    // pair lets a host (or a multi-statement transaction) build a commit incrementally, then flush it atomically.

    /// <summary>True when the table declares IcebergCompat (requires engineered-wood's committing write path).</summary>
    public bool IsIcebergCompat =>
        Schema.IcebergCompat.GetVersion(CurrentSnapshot.Metadata.Configuration)
        != Schema.IcebergCompatVersion.None;

    /// <summary>True when any column carries identity metadata (its values need write-time generation).</summary>
    public bool HasIdentityColumns
    {
        get
        {
            foreach (var f in CurrentSnapshot.Schema.Fields)
            {
                if (IdentityColumn.GetConfig(f) is not null)
                    return true;
            }
            return false;
        }
    }

    /// <summary>
    /// True when <see cref="CommitDataFilesAsync"/> is usable for this table — i.e. an external writer can
    /// produce the data files without engineered-wood's per-row processing. Column-mapping tables (both modes)
    /// are supported, with a caller contract: the external writer must write the files under the PHYSICAL column
    /// names and stamp each column's parquet <c>field_id</c>, and any per-file stats it supplies must be keyed by
    /// the physical names. Identity columns and IcebergCompat are NOT supported (they need write-time per-row
    /// processing). A caller checks this BEFORE writing files externally so it can fall back to the batch path
    /// without leaving an orphan. (Partitioning is a separate check — inspect
    /// <c>CurrentSnapshot.Metadata.PartitionColumns</c>.)
    /// </summary>
    public bool SupportsExternalDataFileCommit
    {
        get
        {
            if (IsIcebergCompat)
                return false;
            foreach (var f in CurrentSnapshot.Schema.Fields)
            {
                if (IdentityColumn.GetConfig(f) is not null)
                    return false;
            }
            return true;
        }
    }

    /// <summary>
    /// Generates identity-column values for a buffered transaction's eagerly-written appends: the configs seed
    /// from the CURRENT snapshot's schema, overridden by <paramref name="chainedHighWaterMarks"/> (the
    /// transaction's pending marks from earlier statements, so values CHAIN across statements without a commit
    /// in between). Returns the processed batches + the new per-column high-water marks; the caller fuses them
    /// into its commit via <see cref="BuildIdentityMetadataAction"/>. Concurrency: a concurrent identity-consuming
    /// commit necessarily carries a metaData action (the HWM lives in schema metadata), so the caller's
    /// expectedVersion abort fires — values baked here never land on a moved HWM.
    /// </summary>
    public (IReadOnlyList<RecordBatch> Batches, IReadOnlyDictionary<string, long> HighWaterMarks)
        GenerateIdentityValues(IReadOnlyList<RecordBatch> batches,
                               IReadOnlyDictionary<string, long>? chainedHighWaterMarks = null)
    {
        ThrowIfDisposed();
        return GenerateIdentityValuesForSchema(CurrentSnapshot.Schema, batches, chainedHighWaterMarks);
    }

    /// <summary>
    /// The schema-seeded form of <see cref="GenerateIdentityValues"/> — for a table that does NOT exist yet (a
    /// buffered CREATE: the identity configs come from the parked schema's <c>delta.identity.*</c> field
    /// metadata, values chain across the transaction's statements, and the flush bakes the final marks into
    /// commit-0's schema). No concurrency concern: nobody can consume ids from a table never committed.
    /// </summary>
    public static (IReadOnlyList<RecordBatch> Batches, IReadOnlyDictionary<string, long> HighWaterMarks)
        GenerateIdentityValuesForSchema(StructType schema, IReadOnlyList<RecordBatch> batches,
                                        IReadOnlyDictionary<string, long>? chainedHighWaterMarks = null)
    {
        var configs = new Dictionary<string, IdentityColumnConfig>();
        foreach (var f in schema.Fields)
        {
            if (IdentityColumn.GetConfig(f) is { } cfg)
            {
                configs[f.Name] = chainedHighWaterMarks is not null
                                  && chainedHighWaterMarks.TryGetValue(f.Name, out var h)
                    ? cfg with { HighWaterMark = h }
                    : cfg;
            }
        }
        if (configs.Count == 0)
        {
            return (batches, new Dictionary<string, long>());
        }
        var outBatches = new List<RecordBatch>(batches.Count);
        foreach (var b in batches)
        {
            var (processed, _) = IdentityColumns.IdentityColumnWriter.ProcessBatch(b, schema, ref configs);
            outBatches.Add(processed);
        }
        var marks = new Dictionary<string, long>();
        foreach (var kv in configs)
        {
            if (kv.Value.HighWaterMark is { } hwm)
                marks[kv.Key] = hwm;
        }
        return (outBatches, marks);
    }

    /// <summary>
    /// Builds the metaData action carrying updated identity high-water marks, based on
    /// <paramref name="baseMetadata"/> (default: the current snapshot's — a buffered ALTER's pending metadata
    /// composes so one commit never carries two metaData actions).
    /// </summary>
    public MetadataAction BuildIdentityMetadataAction(
        IReadOnlyDictionary<string, long> highWaterMarks, MetadataAction? baseMetadata = null)
    {
        ThrowIfDisposed();
        var meta = baseMetadata ?? CurrentSnapshot.Metadata;
        var schema = baseMetadata is null
            ? CurrentSnapshot.Schema
            : DeltaSchemaSerializer.Parse(baseMetadata.SchemaString);
        var fields = new List<StructField>(schema.Fields.Count);
        foreach (var f in schema.Fields)
        {
            fields.Add(highWaterMarks.TryGetValue(f.Name, out var hwm)
                ? IdentityColumn.UpdateHighWaterMark(f, hwm)
                : f);
        }
        var updated = new StructType { Fields = fields };
        return meta with { SchemaString = DeltaSchemaSerializer.Serialize(updated) };
    }

    /// <summary>
    /// Writes <paramref name="batches"/> to append-shaped parquet data files WITHOUT committing, returning the
    /// descriptors to hand to <see cref="CommitDataFilesAsync"/>. Partition split, recursive column-mapping
    /// physical rename + field-id stamping, the variant logical-type policy, the <see cref="IDataFileWriter"/>
    /// seam and per-file stats all apply; row-tracking <c>baseRowId</c> is NOT materialized into the files (the
    /// commit assigns it, exactly like the streaming writer). Identity columns and IcebergCompat need write-time
    /// per-row processing tied to the commit — callers must check <see cref="SupportsExternalDataFileCommit"/>
    /// first (or pass <paramref name="identityValuesPreGenerated"/> for a table whose identity values were
    /// generated up front via <c>GenerateIdentityValues</c>). The written files are invisible orphans until
    /// committed (rollback = never reference them; vacuum cleans).
    /// </summary>
    /// <param name="schemaOverride">A buffered transaction's PENDING (ALTERed) schema — the batches carry columns
    /// the committed snapshot doesn't know yet; the pending schema (whose added columns already carry their
    /// column-mapping ids / physical names) drives the physical rename + stats keying, and the paired commit
    /// includes the matching metaData action.</param>
    public async ValueTask<IReadOnlyList<WrittenDataFile>> WriteDataFilesAsync(
        IReadOnlyList<RecordBatch> batches,
        CancellationToken cancellationToken = default,
        Schema.StructType? schemaOverride = null,
        bool identityValuesPreGenerated = false)
    {
        ThrowIfDisposed();
        ProtocolVersions.ValidateWriteSupport(CurrentSnapshot.Protocol);
        if (IsIcebergCompat)
            throw new NotSupportedException(
                "WriteDataFilesAsync: IcebergCompat tables require the committing write path.");
        if (HasIdentityColumns && !identityValuesPreGenerated)
            throw new NotSupportedException(
                "WriteDataFilesAsync: table has identity columns — generate their values first "
                + "(GenerateIdentityValues) and pass identityValuesPreGenerated, or use the committing write path.");

        var snapshot = CurrentSnapshot;
        var writeSchema = schemaOverride ?? snapshot.Schema;
        var partitionColumns = snapshot.Metadata.PartitionColumns;
        var mappingMode = ColumnMapping.GetMode(snapshot.Metadata.Configuration);
        var logicalToPhysical = ColumnMapping.BuildLogicalToPhysicalMap(writeSchema, mappingMode);
        var files = new List<WrittenDataFile>();

        foreach (var batch in batches)
        {
            if (batch.Length == 0)
                continue;

            var partitions = Partitioning.PartitionUtils.SplitByPartition(batch, partitionColumns);
            foreach (var (partValues, dataBatch) in partitions)
            {
                if (dataBatch.Length == 0)
                    continue;

                // Rename logical columns to physical names + stamp field ids at every nesting level.
                var physicalBatch = ColumnMappingRecursive.ToPhysical(dataBatch, writeSchema, mappingMode);

                // partitionValues keyed by the PHYSICAL column name under mapping (the spec convention).
                var trackedPartValues = partValues;
                if (mappingMode != ColumnMappingMode.None && partValues.Count > 0)
                {
                    trackedPartValues = new Dictionary<string, string>(partValues.Count);
                    foreach (var kv in partValues)
                    {
                        trackedPartValues[logicalToPhysical.TryGetValue(kv.Key, out var p) ? p : kv.Key] = kv.Value;
                    }
                }

                string partDir = Partitioning.PartitionUtils.BuildPartitionPath(trackedPartValues);
                string fileName = string.IsNullOrEmpty(partDir)
                    ? $"{Guid.NewGuid():N}.parquet"
                    : $"{partDir}/{Guid.NewGuid():N}.parquet";

                // Same variant logical-type policy as the committing write path (Spark 4.0.x tables drop the
                // annotation; the read path recovers the type from the Delta schema).
                var writeBatch = _options.EmitVariantLogicalType
                    ? physicalBatch
                    : VariantColumnCoercion.StripAnnotation(physicalBatch);

                long fileSize;
                if (_options.DataFileWriter is { } dataFileWriter)
                {
                    fileSize = await dataFileWriter.WriteAsync(
                        new[] { writeBatch }.ToAsyncEnumerable(), fileName, cancellationToken)
                        .ConfigureAwait(false);
                }
                else
                {
                    await using var file = await _fs.CreateAsync(
                        fileName, cancellationToken: cancellationToken).ConfigureAwait(false);
                    await using var writer = new ParquetFileWriter(
                        file, ownsFile: false, _options.ParquetWriteOptions);
                    await writer.WriteRowGroupAsync(writeBatch, cancellationToken).ConfigureAwait(false);
                    await writer.DisposeAsync().ConfigureAwait(false);
                    fileSize = file.Position;
                }

                // Stats keyed PHYSICAL at every level, matching the streaming writer + spec readers.
                string? stats = _options.CollectStats
                    ? CollectStats(ColumnMappingRecursive.ToPhysical(dataBatch, writeSchema, mappingMode))
                    : null;

                files.Add(new WrittenDataFile(
                    fileName, fileSize, dataBatch.Length,
                    trackedPartValues.Count > 0 ? trackedPartValues : null, stats));
            }
        }

        return files;
    }

    /// <summary>
    /// Commits externally-written <paramref name="files"/> to the Delta log — optionally FUSED with
    /// <paramref name="extraActions"/> (a buffered transaction's deletion-vector remove/add pairs, or a schema
    /// metaData change) — as ONE atomic version. Append-shaped by default (<paramref name="mode"/>
    /// <see cref="DeltaWriteMode.Append"/>); a full <see cref="DeltaWriteMode.Overwrite"/> removes every active
    /// file, and <paramref name="dynamicPartitionOverwrite"/> removes only the active files in partitions the
    /// written files touch. Row-tracking <c>baseRowId</c> / <c>defaultRowCommitVersion</c> + the high-water-mark
    /// domain are assigned here.
    /// </summary>
    /// <param name="expectedVersion">When set, the commit ABORTS (first-committer-wins) if the table has moved off
    /// this version — the caller's snapshot-coupled <paramref name="extraActions"/> (deletion-vector ordinals /
    /// positions computed against it) would be invalidated by a concurrent commit. When null, an append rebases
    /// past a non-conflicting concurrent commit (bounded retry), reusing the already-written files as-is.</param>
    /// <param name="dataChange">False for a REWRITE commit (compaction / clustering OPTIMIZE): removes and adds
    /// carry <c>dataChange=false</c> — CDF readers exclude the commit, concurrent readers' dataChange checks
    /// ignore it, and (per the spec) it is legal on an <c>appendOnly</c> table.</param>
    /// <param name="clusteringProvider">Stamped as <c>add.clusteringProvider</c> on every add — a clustering
    /// OPTIMIZE tags its clustered output files.</param>
    /// <param name="deletedPositionsByFileIndex">Rows of a not-yet-committed file (by index into
    /// <paramref name="files"/>) that a buffered transaction deleted AFTER inserting them (same-transaction DML):
    /// the add is born with an inline deletion vector, so the rows never appear in any committed version.</param>
    public async ValueTask<long> CommitDataFilesAsync(
        IReadOnlyList<WrittenDataFile> files,
        DeltaWriteMode mode = DeltaWriteMode.Append,
        bool dynamicPartitionOverwrite = false,
        CancellationToken cancellationToken = default,
        IReadOnlyList<DeltaAction>? extraActions = null,
        long? expectedVersion = null,
        string operation = "WRITE",
        bool identityValuesPreGenerated = false,
        IReadOnlyDictionary<int, IReadOnlyCollection<long>>? deletedPositionsByFileIndex = null,
        bool dataChange = true,
        string? clusteringProvider = null)
    {
        ThrowIfDisposed();
        ProtocolVersions.ValidateWriteSupport(CurrentSnapshot.Protocol);
        // A dynamic partition overwrite removes files, so it is NOT an append for appendOnly enforcement.
        // extraActions (a buffered transaction's deletion-vector remove/add pairs) likewise make this a
        // non-append. A dataChange=false rewrite (compaction) is append-LEGAL: appendOnly forbids removing
        // ROWS, not reorganizing files.
        bool appendShaped = (mode == DeltaWriteMode.Append && !dynamicPartitionOverwrite &&
                             extraActions is not { Count: > 0 }) || !dataChange;
        HonorWriterFeatures(CurrentSnapshot, appendShaped);

        if (dynamicPartitionOverwrite)
        {
            if (mode != DeltaWriteMode.Append)
                throw new DeltaFormatException(
                    "Dynamic partition overwrite is append-shaped (a full Overwrite already removes everything).");
            if (CurrentSnapshot.Metadata.PartitionColumns.Count == 0)
                throw new DeltaFormatException(
                    "Dynamic partition overwrite requires a partitioned table (the table has no partition columns).");
        }

        // Reject configurations that require write-time per-row processing the external writer did not do (the
        // caller should have checked SupportsExternalDataFileCommit first). Only relevant when data FILES are
        // being committed — a deletion-vector-only or metadata-only fused flush (extraActions, no files) involves
        // no write-time processing.
        var cfg = CurrentSnapshot.Metadata.Configuration;
        if (files.Count > 0 && !SupportsExternalDataFileCommit
            && !(identityValuesPreGenerated && !IsIcebergCompat))
            throw new NotSupportedException(
                "CommitDataFilesAsync: table has identity columns or IcebergCompat — "
                + "these require engineered-wood's own writer.");

        bool rowTrackingEnabled = DeltaLake.RowTracking.RowTrackingConfig.IsEnabled(cfg);

        for (int attempt = 1; ; attempt++)
        {
            var snapshot = CurrentSnapshot;
            // Buffered-transaction commit: the caller's extraActions are snapshot-coupled (deletion-vector
            // ordinals/positions computed against expectedVersion), so a concurrent commit invalidates them —
            // conflict-ABORT instead of the append retry (first-committer-wins snapshot isolation).
            if (expectedVersion is { } expected && snapshot.Version != expected)
            {
                throw new DeltaConflictException(
                    $"Transaction conflict: the table moved from version {expected} to {snapshot.Version} "
                    + "while the transaction was open — the buffered changes were rolled back; retry the "
                    + "transaction.");
            }
            var actions = new List<DeltaAction>();
            long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

            // Overwrite: remove every currently-active file (full replace; STATIC partition-scoped overwrite is
            // not handled here — the caller keeps replace_where on the batch path). DYNAMIC partition overwrite:
            // remove only the active files whose partition matches one of the written files' partitions.
            if (mode == DeltaWriteMode.Overwrite)
            {
                foreach (var existingFile in snapshot.ActiveFiles.Values)
                {
                    actions.Add(new RemoveFile
                    {
                        Path = existingFile.Path,
                        DeletionTimestamp = now,
                        DataChange = dataChange,
                        ExtendedFileMetadata = true,
                        PartitionValues = existingFile.PartitionValues,
                        Size = existingFile.Size,
                        DeletionVector = existingFile.DeletionVector,
                    });
                }
            }
            else if (dynamicPartitionOverwrite)
            {
                var logicalToPhysical = ColumnMapping.BuildLogicalToPhysicalMap(
                    snapshot.Schema, ColumnMapping.GetMode(snapshot.Metadata.Configuration));
                var touched = new HashSet<string>(StringComparer.Ordinal);
                foreach (var f in files)
                {
                    if (f.PartitionValues is { Count: > 0 } pv)
                        touched.Add(CanonicalPartitionKey(pv, logicalToPhysical));
                }
                foreach (var existingFile in snapshot.ActiveFiles.Values)
                {
                    if (!touched.Contains(CanonicalPartitionKey(existingFile.PartitionValues, logicalToPhysical)))
                        continue;
                    actions.Add(new RemoveFile
                    {
                        Path = existingFile.Path,
                        DeletionTimestamp = now,
                        DataChange = true,
                        ExtendedFileMetadata = true,
                        PartitionValues = existingFile.PartitionValues,
                        Size = existingFile.Size,
                        DeletionVector = existingFile.DeletionVector,
                    });
                }
            }

            long nextRowId = rowTrackingEnabled ? snapshot.RowIdHighWaterMark : 0;
            long newVersion = snapshot.Version + 1;
            for (int fi = 0; fi < files.Count; fi++)
            {
                var f = files[fi];
                // deletedPositionsByFileIndex: rows of THIS not-yet-committed file that a buffered transaction
                // deleted after inserting them (same-transaction DML) — the add is born with an inline deletion
                // vector, so the rows never appear in any committed version. Stats stay physical-row stats,
                // marked tightBounds=false per the spec (loose supersets).
                DeletionVector? dv = null;
                string? stats = f.StatsJson ?? $"{{\"numRecords\":{f.NumRecords}}}";
                if (deletedPositionsByFileIndex is not null
                    && deletedPositionsByFileIndex.TryGetValue(fi, out var deletedPositions)
                    && deletedPositions.Count > 0)
                {
                    var dvWriter = new DeletionVectors.DeletionVectorWriter(_fs);
                    dv = await dvWriter.CreateAsync(deletedPositions, deletedPositions.Count,
                        cancellationToken).ConfigureAwait(false);
                    stats = StatsWithLooseBounds(stats);
                }
                long fileBaseRowId = nextRowId;
                actions.Add(new AddFile
                {
                    Path = DeltaPath.Encode(f.RelativePath),
                    PartitionValues = f.PartitionValues ?? new Dictionary<string, string>(),
                    Size = f.SizeBytes,
                    ModificationTime = now,
                    DataChange = dataChange,
                    // numRecords is REQUIRED (row-tracking high-water mark = baseRowId + numRecords); a caller
                    // with full stats passes StatsJson, else we emit the minimal numRecords-only stats.
                    Stats = stats,
                    BaseRowId = rowTrackingEnabled ? fileBaseRowId : null,
                    DefaultRowCommitVersion = rowTrackingEnabled ? newVersion : null,
                    DeletionVector = dv,
                    ClusteringProvider = clusteringProvider,
                    Tags = f.Tags,
                });
                if (rowTrackingEnabled)
                    nextRowId += f.NumRecords;
            }

            if (rowTrackingEnabled && nextRowId > snapshot.RowIdHighWaterMark)
            {
                actions.Add(DeltaLake.RowTracking.RowTrackingConfig.BuildHighWaterMarkAction(nextRowId));
            }

            // A buffered transaction's deletion-vector remove/add pairs (or a schema metaData change) join the
            // SAME commit (atomic DML + append flush).
            if (extraActions is { Count: > 0 })
                actions.AddRange(extraActions);

            var finalActions = Log.InCommitTimestamp.EnsureCommitInfo(actions, cfg, operation);
            try
            {
                await _log.WriteCommitAsync(newVersion, finalActions, cancellationToken).ConfigureAwait(false);
            }
            catch (DeltaConflictException) when (attempt < 16 && expectedVersion is null)
            {
                // A concurrent writer took our version — refresh the snapshot (recomputes the Overwrite removes +
                // the row-tracking high-water mark) and retry. The already-written data files are reused as-is.
                _currentSnapshot = await SnapshotBuilder.UpdateAsync(snapshot, _log, cancellationToken).ConfigureAwait(false);
                continue;
            }

            _currentSnapshot = await SnapshotBuilder.UpdateAsync(snapshot, _log, cancellationToken).ConfigureAwait(false);
            if (_options.CheckpointInterval > 0 && newVersion % _options.CheckpointInterval == 0)
                await _checkpointWriter.WriteCheckpointAsync(_currentSnapshot, cancellationToken).ConfigureAwait(false);
            return newVersion;
        }
    }

    /// <summary>Rewrites a stats JSON object with <c>tightBounds=false</c> — an add that carries an inline
    /// deletion vector has fewer live rows than its physical min/max bounds cover, so the bounds are loose
    /// supersets per the spec.</summary>
    private static string? StatsWithLooseBounds(string? stats)
    {
        if (string.IsNullOrEmpty(stats))
            return stats;
        try
        {
            using var doc = System.Text.Json.JsonDocument.Parse(stats!);
            using var stream = new MemoryStream();
            using (var writer = new System.Text.Json.Utf8JsonWriter(stream))
            {
                writer.WriteStartObject();
                writer.WriteBoolean("tightBounds", false);
                foreach (var prop in doc.RootElement.EnumerateObject())
                {
                    if (!prop.NameEquals("tightBounds"))
                        prop.WriteTo(writer);
                }
                writer.WriteEndObject();
            }
            return System.Text.Encoding.UTF8.GetString(stream.ToArray());
        }
        catch (System.Text.Json.JsonException)
        {
            return stats;
        }
    }

    // ── Buffered-transaction DML seam ──────────────────────────────────────────────────────────────────
    //
    // The deferred half of a deletion-vector DELETE + the exact-row read-back an UPDATE post-image is built
    // from. Positions and transient rowids are addressed by a file's PATH-SORTED ordinal in the snapshot's
    // active set (OrderedActiveFiles) — stable within one snapshot, which is why a buffered transaction pins the
    // version its ordinals were captured against (atVersion / resolveAgainst) and re-validates before committing.

    // The transient rowid packs (path-sorted file ordinal &lt;&lt; RowIdPositionBits) | absolute-in-file position.
    private const int RowIdPositionBits = 40;

    private static List<Actions.AddFile> OrderedActiveFiles(Snapshot.Snapshot snapshot)
    {
        var files = new List<Actions.AddFile>(snapshot.ActiveFiles.Values);
        files.Sort((a, b) => string.CompareOrdinal(a.Path, b.Path));
        return files;
    }

    /// <summary>
    /// Computes the deletion-vector actions for the given deleted positions WITHOUT committing — the deferred
    /// half of a DV DELETE, for a buffered (multi-statement) transaction that fuses its DML + appends into one
    /// commit via <see cref="CommitDataFilesAsync"/>' <c>extraActions</c>. Positions are keyed by the
    /// path-sorted file ordinal and are ABSOLUTE in-file row positions; each touched file's existing DV is
    /// unioned with the new positions and the result is a <c>remove</c>(old path+DV) + <c>add</c>(same path, new
    /// DV) pair. Change Data Feed is NOT captured here (the caller must gate CDF tables to the committing path).
    /// Returns the actions + the count of NEWLY deleted rows.
    /// </summary>
    /// <param name="resolveAgainst">Rebase support: the ordinals + old DVs were captured against the
    /// transaction's PINNED snapshot — resolve there, not against a possibly-advanced current snapshot (whose
    /// path-sorted ordering may differ after concurrent appends). The caller runs
    /// <see cref="CheckLogicalRebaseAsync"/> before committing the result on a newer snapshot.</param>
    public async ValueTask<(IReadOnlyList<DeltaAction> Actions, long RowsDeleted)> ComputeDeletionVectorActionsAsync(
        IReadOnlyDictionary<int, IReadOnlyCollection<long>> positionsByOrdinal,
        CancellationToken cancellationToken = default,
        Snapshot.Snapshot? resolveAgainst = null)
    {
        ThrowIfDisposed();
        var snapshot = resolveAgainst ?? CurrentSnapshot;
        var ordered = OrderedActiveFiles(snapshot);
        var dvWriter = new DeletionVectors.DeletionVectorWriter(_fs);
        var actions = new List<DeltaAction>();
        long totalDeleted = 0;

        foreach (var kvp in positionsByOrdinal)
        {
            int ordinal = kvp.Key;
            if (ordinal < 0 || ordinal >= ordered.Count)
                continue;
            var addFile = ordered[ordinal];

            var allDeleted = addFile.DeletionVector is not null
                ? new HashSet<long>(await _dvReader.ReadAsync(addFile.DeletionVector, cancellationToken)
                    .ConfigureAwait(false))
                : new HashSet<long>();

            long newlyDeleted = 0;
            foreach (long p in kvp.Value)
                if (allDeleted.Add(p))
                    newlyDeleted++;
            if (newlyDeleted == 0)
                continue;
            totalDeleted += newlyDeleted;

            var newDv = await dvWriter.CreateAsync(allDeleted, allDeleted.Count, cancellationToken)
                .ConfigureAwait(false);

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
                Stats = StatsWithLooseBounds(addFile.Stats),
            });
        }

        return (actions, totalDeleted);
    }

    /// <summary>
    /// DELETE by TRANSIENT rowid using DELETION VECTORS (no file rewrite): each affected file's existing DV is
    /// unioned with the new in-file positions (decoded from <c>rowid &amp; posMask</c>) and a fresh DV written;
    /// the commit is <c>remove</c>(old file+DV) + <c>add</c>(same file, new DV). The <paramref name="rowIds"/>
    /// MUST be the ABSOLUTE positions <see cref="ReadAllWithRowIdsAsync"/> emits so repeated DV deletes compose.
    /// Requires <c>delta.enableDeletionVectors</c>. With <paramref name="rowLevelRetry"/>, a concurrent DV-delete
    /// of the SAME file re-unions when the touched rows are disjoint (row-level concurrency, via
    /// <see cref="CommitOccAsync"/>'s row-level path) instead of aborting. Returns the rows newly deleted and the
    /// committed version. The committing, DV-based sibling of <see cref="ComputeDeletionVectorActionsAsync"/>.
    /// </summary>
    public async ValueTask<(long RowsDeleted, long Version)> DeleteByRowIdsViaVectorsAsync(
        IReadOnlyCollection<long> rowIds,
        CancellationToken cancellationToken = default,
        bool rowLevelRetry = false)
    {
        ThrowIfDisposed();
        ProtocolVersions.ValidateWriteSupport(CurrentSnapshot.Protocol);

        var snapshot = CurrentSnapshot;
        if (rowIds.Count == 0)
            return (0, snapshot.Version);

        HonorWriterFeatures(snapshot, isAppend: false);
        if (!DeletionVectors.DeletionVectorConfig.IsEnabled(snapshot.Metadata.Configuration))
            throw new InvalidOperationException(
                "DeleteByRowIdsViaVectorsAsync requires deletion vectors — create the table with "
                + "DeltaTable.CreateAsync(..., enableDeletionVectors: true), or use the copy-on-write "
                + "DeleteByRowIdsAsync.");

        long posMask = (1L << RowIdPositionBits) - 1;
        var positionsByFile = new Dictionary<int, HashSet<long>>();
        foreach (var rid in rowIds)
        {
            int ordinal = (int)(rid >> RowIdPositionBits);
            if (!positionsByFile.TryGetValue(ordinal, out var set))
                positionsByFile[ordinal] = set = new HashSet<long>();
            set.Add(rid & posMask);
        }

        var ordered = OrderedActiveFiles(snapshot);
        var dvWriter = new DeletionVectors.DeletionVectorWriter(_fs);
        bool cdfEnabled = DeltaLake.ChangeDataFeed.CdfConfig.IsEnabled(snapshot.Metadata.Configuration);
        var actions = new List<DeltaAction>();
        var removedPaths = new HashSet<string>(StringComparer.Ordinal);
        var dvEdits = new List<DeleteDvEdit>();
        long totalDeleted = 0;

        foreach (var kvp in positionsByFile)
        {
            int ordinal = kvp.Key;
            if (ordinal < 0 || ordinal >= ordered.Count)
                continue;
            var addFile = ordered[ordinal];

            var allDeleted = addFile.DeletionVector is not null
                ? new HashSet<long>(await _dvReader.ReadAsync(addFile.DeletionVector, cancellationToken)
                    .ConfigureAwait(false))
                : new HashSet<long>();
            var newPositions = new List<long>();
            foreach (long p in kvp.Value)
                if (allDeleted.Add(p))
                    newPositions.Add(p);
            if (newPositions.Count == 0)
                continue;
            totalDeleted += newPositions.Count;

            var newDv = await dvWriter.CreateAsync(allDeleted, allDeleted.Count, cancellationToken)
                .ConfigureAwait(false);

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
                Stats = StatsWithLooseBounds(addFile.Stats),
            });
            removedPaths.Add(addFile.Path);
            dvEdits.Add(new DeleteDvEdit(addFile.Path, newPositions));

            // Change Data Feed: a DV delete rewrites no data, so read the newly-deleted rows (matched by
            // ABSOLUTE position — the file's original DV survivors keep their absolute positions) and emit a
            // "delete" change file.
            if (cdfEnabled)
            {
                var newSet = new HashSet<long>(newPositions);
                var absOut = new List<Int64Array?>();
                int bi = -1;
                await foreach (var batch in ReadFileAsync(addFile, null, snapshot, cancellationToken,
                                                          strippedAbsPositionsOut: absOut).ConfigureAwait(false))
                {
                    bi++;
                    var absPos = bi < absOut.Count ? absOut[bi] : null;
                    if (absPos is null)
                        continue;
                    var delRows = new List<int>();
                    for (int i = 0; i < batch.Length; i++)
                        if (!absPos.IsNull(i) && newSet.Contains(absPos.GetValue(i)!.Value))
                            delRows.Add(i);
                    if (delRows.Count > 0)
                    {
                        var cdc = await ChangeDataFeed.CdfWriter.WriteAsync(
                            _fs, snapshot, TakeRowsFromBatch(batch, delRows), DeltaLake.ChangeDataFeed.CdfConfig.Delete,
                            addFile.PartitionValues, _options.ParquetWriteOptions,
                            cancellationToken).ConfigureAwait(false);
                        actions.Add(cdc);
                    }
                }
            }
        }

        if (actions.Count == 0)
            return (0, snapshot.Version);

        long version = await CommitOccAsync(
            snapshot, actions,
            new Concurrency.ReadSet { Files = removedPaths }, removedPaths,
            IsolationLevel.WriteSerializable, "DELETE", rebaseSafe: true, cancellationToken,
            rowLevelDeletes: rowLevelRetry ? dvEdits : null).ConfigureAwait(false);
        return (totalDeleted, version);
    }

    /// <summary>
    /// Deletes the rows addressed by the TRANSIENT rowids in <paramref name="rowIds"/> (each =
    /// <c>(fileOrdinal &lt;&lt; RowIdPositionBits) | absolutePosition</c>, from <see cref="ReadAllWithRowIdsAsync"/>
    /// over the SAME snapshot) using <b>copy-on-write</b>: each affected file is rewritten without the deleted
    /// rows and committed as plain <c>remove</c>/<c>add</c> — NO deletion vectors, NO row-tracking feature needed,
    /// so the result is maximally reader-compatible (Fabric OneLake, Spark, delta-kernel). Row tracking, when
    /// enabled, is preserved (survivors keep their materialized id + commit version). CDF and IcebergCompat are
    /// not yet supported on this path (use <see cref="DeleteByRowIdsViaVectorsAsync"/> for a CDF table). Returns
    /// the rows deleted and the committed version.
    /// </summary>
    public async ValueTask<(long RowsDeleted, long Version)> DeleteByRowIdsAsync(
        IReadOnlyCollection<long> rowIds,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ProtocolVersions.ValidateWriteSupport(CurrentSnapshot.Protocol);

        var snapshot = CurrentSnapshot;
        if (rowIds.Count == 0)
            return (0, snapshot.Version);

        HonorWriterFeatures(snapshot, isAppend: false);
        RejectCopyOnWriteRowIdUnsupported(snapshot, "copy-on-write DELETE");

        var positionsByFile = DecodeRowIdPositions(rowIds);
        var ordered = OrderedActiveFiles(snapshot);
        var mappingMode = ColumnMapping.GetMode(snapshot.Metadata.Configuration);
        bool rowTrackingEnabled = DeltaLake.RowTracking.RowTrackingConfig.IsEnabled(
            snapshot.Metadata.Configuration);
        var (matRowIdName, matRowVerName) = DeltaLake.RowTracking.RowTrackingConfig
            .TryGetMaterializedColumnNames(snapshot.Metadata.Configuration);
        bool materializeIds = rowTrackingEnabled && matRowIdName is not null && matRowVerName is not null;
        long newVersion = snapshot.Version + 1;
        long nextRowId = rowTrackingEnabled ? snapshot.RowIdHighWaterMark : 0;

        var actions = new List<DeltaAction>();
        var removedPaths = new HashSet<string>(StringComparer.Ordinal);
        long totalDeleted = 0;

        foreach (var kvp in positionsByFile)
        {
            int ordinal = kvp.Key;
            if (ordinal < 0 || ordinal >= ordered.Count)
                continue;
            var addFile = ordered[ordinal];
            var targets = kvp.Value;

            // Read the file (logical), keeping only rows whose ABSOLUTE position is NOT targeted; materialize
            // each survivor's original id + version so the rewrite preserves row identity.
            var srcIds = materializeIds ? new List<Int64Array?>() : null;
            var srcVers = materializeIds ? new List<Int64Array?>() : null;
            var absOut = new List<Int64Array?>();
            var outputBatches = new List<RecordBatch>();
            var outTracking = materializeIds ? new List<(Int64Array Ids, Int64Array Vers)?>() : null;
            long deletedHere = 0;
            int bi = -1;
            await foreach (var batch in ReadFileAsync(addFile, null, snapshot, cancellationToken,
                                                      srcIds, srcVers, absOut).ConfigureAwait(false))
            {
                bi++;
                var absPos = bi < absOut.Count ? absOut[bi] : null;
                var keepRows = new List<int>();
                for (int i = 0; i < batch.Length; i++)
                {
                    long abs = absPos is not null && i < absPos.Length && !absPos.IsNull(i)
                        ? absPos.GetValue(i)!.Value : i;
                    if (targets.Contains(abs))
                        deletedHere++;
                    else
                        keepRows.Add(i);
                }
                if (keepRows.Count == 0)
                    continue;

                var batchIds = srcIds is not null && bi < srcIds.Count ? srcIds[bi] : null;
                var batchVers = srcVers is not null && bi < srcVers.Count ? srcVers[bi] : null;
                if (keepRows.Count == batch.Length)
                {
                    outputBatches.Add(batch);
                    outTracking?.Add(batchIds is not null && batchVers is not null
                        ? (batchIds, batchVers) : ((Int64Array, Int64Array)?)null);
                }
                else
                {
                    outputBatches.Add(TakeRowsFromBatch(batch, keepRows));
                    outTracking?.Add(batchIds is not null && batchVers is not null
                        ? (TakeIds(batchIds, keepRows), TakeIds(batchVers, keepRows))
                        : ((Int64Array, Int64Array)?)null);
                }
            }

            if (deletedHere == 0)
                continue;
            totalDeleted += deletedHere;

            var (remove, add, addedRows) = await RewriteRowsToNewFileAsync(
                snapshot, addFile, mappingMode, outputBatches, outTracking, materializeIds,
                matRowIdName, matRowVerName, rowTrackingEnabled, nextRowId, newVersion,
                cancellationToken).ConfigureAwait(false);
            actions.Add(remove);
            removedPaths.Add(addFile.Path);
            if (add is not null)
            {
                actions.Add(add);
                if (rowTrackingEnabled)
                    nextRowId += addedRows;
            }
        }

        if (actions.Count == 0)
            return (0, snapshot.Version);

        if (rowTrackingEnabled && nextRowId > snapshot.RowIdHighWaterMark)
            actions.Add(DeltaLake.RowTracking.RowTrackingConfig.BuildHighWaterMarkAction(nextRowId));

        // A copy-on-write DELETE reads exactly the files it rewrites (removedPaths = its read-set), so it
        // rebases past a non-conflicting concurrent commit and aborts only on a real conflict — but a rewrite's
        // fresh add is NOT verbatim-rebase-safe (its baseRowId embeds the attempted version's HWM), so
        // single-attempt (rebaseSafe:false) as the overwrite family does.
        long version = await CommitOccAsync(
            snapshot, actions,
            new Concurrency.ReadSet { Files = removedPaths }, removedPaths,
            IsolationLevel.WriteSerializable, "DELETE", rebaseSafe: false, cancellationToken)
            .ConfigureAwait(false);
        return (totalDeleted, version);
    }

    // Decodes transient rowids into absolute in-file positions per path-sorted file ordinal.
    private static Dictionary<int, HashSet<long>> DecodeRowIdPositions(IReadOnlyCollection<long> rowIds)
    {
        long posMask = (1L << RowIdPositionBits) - 1;
        var positionsByFile = new Dictionary<int, HashSet<long>>();
        foreach (var rid in rowIds)
        {
            int ordinal = (int)(rid >> RowIdPositionBits);
            if (!positionsByFile.TryGetValue(ordinal, out var set))
                positionsByFile[ordinal] = set = new HashSet<long>();
            set.Add(rid & posMask);
        }
        return positionsByFile;
    }

    // The corners the copy-on-write row-id DML path does not yet cover (each needs write-time work tied to the
    // commit that the position-addressed rewrite skips). CDF callers use the DV path; IcebergCompat needs the
    // committing writer.
    private void RejectCopyOnWriteRowIdUnsupported(Snapshot.Snapshot snapshot, string op)
    {
        if (DeltaLake.ChangeDataFeed.CdfConfig.IsEnabled(snapshot.Metadata.Configuration))
            throw new NotSupportedException(
                $"{op} by row id does not yet support Change Data Feed — use DeleteByRowIdsViaVectorsAsync "
                + "(deletion-vector DELETE emits the change feed) or the predicate-based UpdateAsync.");
        if (IsIcebergCompat)
            throw new NotSupportedException($"{op} by row id is not supported on IcebergCompat tables.");
    }

    // Physical-writes the rewritten output batches for one file and returns the remove(old)+add(new) pair (Add is
    // null when every row was deleted → whole-file remove). Mirrors ComputeUpdateActionsAsync's write block;
    // shared by DeleteByRowIdsAsync + UpdateByRowIdsAsync.
    private async ValueTask<(RemoveFile Remove, AddFile? Add, long AddedRows)> RewriteRowsToNewFileAsync(
        Snapshot.Snapshot snapshot, Actions.AddFile source, ColumnMappingMode mappingMode,
        IReadOnlyList<RecordBatch> outputBatches,
        IReadOnlyList<(Int64Array Ids, Int64Array Vers)?>? outTracking,
        bool materializeIds, string? matRowIdName, string? matRowVerName,
        bool rowTrackingEnabled, long baseRowId, long newVersion,
        CancellationToken cancellationToken)
    {
        long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var remove = new RemoveFile
        {
            Path = source.Path,
            DeletionTimestamp = now,
            DataChange = true,
            ExtendedFileMetadata = true,
            PartitionValues = source.PartitionValues,
            Size = source.Size,
            DeletionVector = source.DeletionVector, // rewritten file has the DV's deletions applied
        };

        long addedRows = 0;
        foreach (var b in outputBatches)
            addedRows += b.Length;
        if (addedRows == 0)
            return (remove, null, 0); // every row deleted — drop the file outright

        // Reuse the source path's ENCODED prefix verbatim (partition dir) for the add; DECODED for the write.
        string encodedDir = "";
        int dirSlash = source.Path.LastIndexOf('/');
        if (dirSlash >= 0)
            encodedDir = source.Path.Substring(0, dirSlash + 1);
        string baseName = $"{Guid.NewGuid():N}.parquet";
        string newFileName = EngineeredWood.DeltaLake.DeltaPath.Decode(encodedDir) + baseName;

        var writeBatches = new List<RecordBatch>(outputBatches.Count);
        for (int k = 0; k < outputBatches.Count; k++)
        {
            var physicalBatch = ColumnMappingRecursive.ToPhysical(outputBatches[k], snapshot.Schema, mappingMode);
            if (!_options.EmitVariantLogicalType)
                physicalBatch = VariantColumnCoercion.StripAnnotation(physicalBatch);
            if (materializeIds && outTracking is not null && outTracking[k] is { } trk)
            {
                physicalBatch = RowTracking.RowTrackingWriter.AddRowIdAndCommitVersionColumns(
                    physicalBatch, trk.Ids, trk.Vers, matRowIdName!, matRowVerName!, nullable: true);
            }
            writeBatches.Add(physicalBatch);
        }

        long fileSize;
        if (_options.DataFileWriter is { } rewriteWriter)
        {
            fileSize = await rewriteWriter.WriteAsync(
                writeBatches.ToAsyncEnumerable(), newFileName, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            await using var file = await _fs.CreateAsync(
                newFileName, cancellationToken: cancellationToken).ConfigureAwait(false);
            await using var writer = new Parquet.ParquetFileWriter(
                file, ownsFile: false, _options.ParquetWriteOptions);
            foreach (var batch in writeBatches)
                await writer.WriteRowGroupAsync(batch, cancellationToken).ConfigureAwait(false);
            await writer.DisposeAsync().ConfigureAwait(false);
            fileSize = file.Position;
        }

        var add = new AddFile
        {
            Path = encodedDir + baseName,
            PartitionValues = source.PartitionValues,
            Size = fileSize,
            ModificationTime = now,
            DataChange = true,
            Stats = Stats.StatsCollector.Collect(outputBatches),
            BaseRowId = rowTrackingEnabled ? baseRowId : null,
            DefaultRowCommitVersion = rowTrackingEnabled ? newVersion : null,
        };
        return (remove, add, addedRows);
    }

    /// <summary>
    /// Per-file copy-on-write UPDATE by TRANSIENT rowid (the companion to <see cref="DeleteByRowIdsAsync"/>).
    /// <paramref name="rowIds"/> = <c>(fileOrdinal &lt;&lt; RowIdPositionBits) | absolutePosition</c> (same
    /// encoding as <see cref="ReadAllWithRowIdsAsync"/>). Only files containing a target row are rewritten: each
    /// such file's user batches are read (DV-filtered, in position order) and handed to
    /// <paramref name="rewriteFile"/> — which returns the SAME rows with the SET columns modified on the matched
    /// positions (the caller owns that typed logic; it MUST return one batch per source batch with identical row
    /// counts) — then re-written as plain <c>remove</c>+<c>add</c>. Row tracking is preserved (an UPDATED row's
    /// commit version advances to this commit; untouched rows keep theirs). CDF / IcebergCompat not yet supported
    /// on this path. Returns the committed version (or the current version if nothing matched).
    /// </summary>
    public ValueTask<long> UpdateByRowIdsAsync(
        IReadOnlyCollection<long> rowIds,
        Func<long, IReadOnlyList<RecordBatch>, IReadOnlyList<RecordBatch>> rewriteFile,
        CancellationToken cancellationToken = default)
        => UpdateByRowIdsCoreAsync(rowIds, (ordinal, batches, _) => rewriteFile(ordinal, batches),
                                   cancellationToken);

    /// <summary>
    /// Copy-on-write UPDATE by TRANSIENT rowid, with each source row's rowid ALSO handed to the rewriter — so a
    /// host that computed the new values keyed by rowid (e.g. a DuckDB join against the Delta table, producing
    /// <c>rowid → new SET values</c>) substitutes them by an O(1) lookup instead of re-matching on row content.
    /// <paramref name="rewriteFile"/> receives <c>(fileOrdinal, sourceBatches, rowIdsPerBatch)</c> where
    /// <c>rowIdsPerBatch[b][i]</c> is the transient rowid of row <c>i</c> of source batch <c>b</c> (same encoding
    /// as <paramref name="rowIds"/>); it returns the modified batches (one per source batch, identical row
    /// counts). Everything else matches the delegate overload above.
    /// </summary>
    public ValueTask<long> UpdateByRowIdsAsync(
        IReadOnlyCollection<long> rowIds,
        Func<long, IReadOnlyList<RecordBatch>, IReadOnlyList<Int64Array>, IReadOnlyList<RecordBatch>> rewriteFile,
        CancellationToken cancellationToken = default)
        => UpdateByRowIdsCoreAsync(rowIds, rewriteFile, cancellationToken);

    /// <summary>
    /// Copy-on-write UPDATE by TRANSIENT rowid from a batch of new values — the convenience form for the
    /// "update from a host-side join" scenario, so the caller supplies no substitution code at all.
    /// <paramref name="updates"/> carries one row per rowid to change: a rowid column (named
    /// <paramref name="rowIdColumn"/>, default <see cref="DeltaLake.RowTracking.RowTrackingConfig.VirtualRowIdColumn"/>
    /// — what <see cref="ReadAllWithRowIdsAsync"/> emits) plus one column per SET column, named by its LOGICAL
    /// table-column name and typed to match. For every source row whose rowid appears in <paramref name="updates"/>,
    /// each SET column's value is replaced with the corresponding value from <paramref name="updates"/> (type-
    /// agnostic, via concat + take — no per-type code); all other columns and rows pass through. Duplicate rowids
    /// in <paramref name="updates"/> are a caller error (last one wins). Returns the committed version (or the
    /// current version if nothing matched).
    /// </summary>
    public ValueTask<long> UpdateByRowIdsAsync(
        RecordBatch updates,
        string? rowIdColumn = null,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        if (updates is null)
            throw new ArgumentNullException(nameof(updates));
        rowIdColumn ??= DeltaLake.RowTracking.RowTrackingConfig.VirtualRowIdColumn;

        int ridIdx = updates.Schema.GetFieldIndex(rowIdColumn);
        if (ridIdx < 0)
            throw new ArgumentException(
                $"updates has no rowid column '{rowIdColumn}'.", nameof(updates));
        if (updates.Column(ridIdx) is not Int64Array ridArray)
            throw new ArgumentException(
                $"updates rowid column '{rowIdColumn}' must be Int64.", nameof(updates));

        // rowid → its row index in `updates`, and the SET columns (everything except the rowid column).
        var updIndexByRowId = new Dictionary<long, int>(updates.Length);
        for (int i = 0; i < updates.Length; i++)
            if (!ridArray.IsNull(i))
                updIndexByRowId[ridArray.GetValue(i)!.Value] = i;

        var setColumns = new List<(string Name, IArrowArray Values)>();
        for (int c = 0; c < updates.ColumnCount; c++)
            if (c != ridIdx)
                setColumns.Add((updates.Schema.FieldsList[c].Name, updates.Column(c)));

        var rowIds = updIndexByRowId.Keys.ToArray();
        return UpdateByRowIdsCoreAsync(
            rowIds,
            (ordinal, sourceBatches, rowIdsPerBatch) =>
                ApplyRowIdKeyedUpdates(sourceBatches, rowIdsPerBatch, updIndexByRowId, setColumns),
            cancellationToken);
    }

    // Substitutes the SET columns' values at every source row whose rowid is in `updIndexByRowId`, pulling the
    // new value from `setColumns` at the mapped index. Type-agnostic: per SET column, concatenate
    // [source column, updates column] and TAKE — source row i takes index i, an updated row takes
    // (sourceLen + updIndex). Untouched columns/rows pass through by reference.
    private static IReadOnlyList<RecordBatch> ApplyRowIdKeyedUpdates(
        IReadOnlyList<RecordBatch> sourceBatches,
        IReadOnlyList<Int64Array> rowIdsPerBatch,
        IReadOnlyDictionary<long, int> updIndexByRowId,
        IReadOnlyList<(string Name, IArrowArray Values)> setColumns)
    {
        var result = new List<RecordBatch>(sourceBatches.Count);
        for (int b = 0; b < sourceBatches.Count; b++)
        {
            var src = sourceBatches[b];
            var rids = rowIdsPerBatch[b];

            // take indices: normally i (from the source half); an updated row → src.Length + updIndex.
            List<int>? take = null;
            for (int i = 0; i < src.Length; i++)
            {
                if (!rids.IsNull(i) && updIndexByRowId.TryGetValue(rids.GetValue(i)!.Value, out int updIdx))
                {
                    take ??= BuildIdentity(src.Length);
                    take[i] = src.Length + updIdx;
                }
            }
            if (take is null) { result.Add(src); continue; } // no target row in this batch — untouched

            var columns = new IArrowArray[src.ColumnCount];
            for (int c = 0; c < src.ColumnCount; c++)
            {
                string name = src.Schema.FieldsList[c].Name;
                var setCol = setColumns.FirstOrDefault(s => string.Equals(s.Name, name, StringComparison.Ordinal));
                if (setCol.Values is null)
                {
                    columns[c] = src.Column(c); // not a SET column — unchanged
                    continue;
                }
                var combined = ArrowArrayConcatenator.Concatenate(new[] { src.Column(c), setCol.Values });
                columns[c] = DeletionVectors.DeletionVectorFilter.TakeRowsPublic(combined, take);
            }
            result.Add(new RecordBatch(src.Schema, columns, src.Length));
        }
        return result;
    }

    private static List<int> BuildIdentity(int n)
    {
        var list = new List<int>(n);
        for (int i = 0; i < n; i++) list.Add(i);
        return list;
    }

    private async ValueTask<long> UpdateByRowIdsCoreAsync(
        IReadOnlyCollection<long> rowIds,
        Func<long, IReadOnlyList<RecordBatch>, IReadOnlyList<Int64Array>, IReadOnlyList<RecordBatch>> rewriteFile,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ProtocolVersions.ValidateWriteSupport(CurrentSnapshot.Protocol);

        var snapshot = CurrentSnapshot;
        if (rowIds.Count == 0)
            return snapshot.Version;

        HonorWriterFeatures(snapshot, isAppend: false);
        RejectCopyOnWriteRowIdUnsupported(snapshot, "copy-on-write UPDATE");

        var positionsByFile = DecodeRowIdPositions(rowIds);
        var ordered = OrderedActiveFiles(snapshot);
        var mappingMode = ColumnMapping.GetMode(snapshot.Metadata.Configuration);
        bool rowTrackingEnabled = DeltaLake.RowTracking.RowTrackingConfig.IsEnabled(
            snapshot.Metadata.Configuration);
        var (matRowIdName, matRowVerName) = DeltaLake.RowTracking.RowTrackingConfig
            .TryGetMaterializedColumnNames(snapshot.Metadata.Configuration);
        bool materializeIds = rowTrackingEnabled && matRowIdName is not null && matRowVerName is not null;
        long newVersion = snapshot.Version + 1;
        long nextRowId = rowTrackingEnabled ? snapshot.RowIdHighWaterMark : 0;

        var actions = new List<DeltaAction>();
        var removedPaths = new HashSet<string>(StringComparer.Ordinal);

        foreach (var kvp in positionsByFile)
        {
            int ordinal = kvp.Key;
            if (ordinal < 0 || ordinal >= ordered.Count)
                continue;
            var addFile = ordered[ordinal];
            var targets = kvp.Value;

            var srcIds = materializeIds ? new List<Int64Array?>() : null;
            var srcVers = materializeIds ? new List<Int64Array?>() : null;
            var absOut = new List<Int64Array?>();
            var userBatches = new List<RecordBatch>();
            await foreach (var batch in ReadFileAsync(addFile, null, snapshot, cancellationToken,
                                                      srcIds, srcVers, absOut).ConfigureAwait(false))
            {
                userBatches.Add(batch);
            }
            if (userBatches.Count == 0)
                continue;

            // Per-batch transient rowids (row-aligned), so the rewriter can key its new values by rowid.
            var rowIdsPerBatch = new List<Int64Array>(userBatches.Count);
            for (int bi = 0; bi < userBatches.Count; bi++)
            {
                var absPos = bi < absOut.Count ? absOut[bi] : null;
                var ridb = new Int64Array.Builder();
                for (int i = 0; i < userBatches[bi].Length; i++)
                {
                    long abs = absPos is not null && i < absPos.Length && !absPos.IsNull(i)
                        ? absPos.GetValue(i)!.Value : i;
                    ridb.Append(((long)ordinal << RowIdPositionBits) | abs);
                }
                rowIdsPerBatch.Add(ridb.Build());
            }

            // The caller rebuilds each batch's rows with the SET columns modified on the matched positions.
            var rewritten = rewriteFile(ordinal, userBatches, rowIdsPerBatch);
            if (rewritten.Count != userBatches.Count)
                throw new InvalidOperationException(
                    "UpdateByRowIdsAsync: rewriteFile must return one batch per source batch.");

            // Build the materialized id/version arrays (an UPDATED row's version advances to this commit) and
            // count the rows actually matched in this file.
            var outTracking = materializeIds ? new List<(Int64Array Ids, Int64Array Vers)?>() : null;
            long updatedHere = 0;
            for (int bi = 0; bi < userBatches.Count; bi++)
            {
                var src = userBatches[bi];
                if (rewritten[bi].Length != src.Length)
                    throw new InvalidOperationException(
                        "UpdateByRowIdsAsync: rewriteFile must preserve each batch's row count.");
                var absPos = bi < absOut.Count ? absOut[bi] : null;
                var batchIds = srcIds is not null && bi < srcIds.Count ? srcIds[bi] : null;
                var batchVers = srcVers is not null && bi < srcVers.Count ? srcVers[bi] : null;
                Int64Array.Builder? idb = materializeIds ? new Int64Array.Builder() : null;
                Int64Array.Builder? vdb = materializeIds ? new Int64Array.Builder() : null;
                for (int i = 0; i < src.Length; i++)
                {
                    long abs = absPos is not null && i < absPos.Length && !absPos.IsNull(i)
                        ? absPos.GetValue(i)!.Value : i;
                    bool updated = targets.Contains(abs);
                    if (updated)
                        updatedHere++;
                    if (materializeIds)
                    {
                        long? id = batchIds is not null && !batchIds.IsNull(i) ? batchIds.GetValue(i) : null;
                        if (id is { } iv) idb!.Append(iv); else idb!.AppendNull();
                        long? ver = updated
                            ? newVersion
                            : (batchVers is not null && !batchVers.IsNull(i) ? batchVers.GetValue(i) : (long?)null);
                        if (ver is { } vv) vdb!.Append(vv); else vdb!.AppendNull();
                    }
                }
                outTracking?.Add(materializeIds ? (idb!.Build(), vdb!.Build()) : ((Int64Array, Int64Array)?)null);
            }

            if (updatedHere == 0)
                continue; // no target row actually present in this file — leave it untouched

            var (remove, add, addedRows) = await RewriteRowsToNewFileAsync(
                snapshot, addFile, mappingMode, rewritten, outTracking, materializeIds,
                matRowIdName, matRowVerName, rowTrackingEnabled, nextRowId, newVersion,
                cancellationToken).ConfigureAwait(false);
            actions.Add(remove);
            removedPaths.Add(addFile.Path);
            if (add is not null)
            {
                actions.Add(add);
                if (rowTrackingEnabled)
                    nextRowId += addedRows;
            }
        }

        if (actions.Count == 0)
            return snapshot.Version;

        if (rowTrackingEnabled && nextRowId > snapshot.RowIdHighWaterMark)
            actions.Add(DeltaLake.RowTracking.RowTrackingConfig.BuildHighWaterMarkAction(nextRowId));

        return await CommitOccAsync(
            snapshot, actions,
            new Concurrency.ReadSet { Files = removedPaths }, removedPaths,
            IsolationLevel.WriteSerializable, "UPDATE", rebaseSafe: false, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Reads exactly the rows identified by the given transient rowids (<c>(fileOrdinal &lt;&lt; RowIdPositionBits)
    /// | absolutePosition</c> against the snapshot pinned by <paramref name="atVersion"/>) — the read-back step a
    /// buffered UPDATE's post-image is built from. Deletion-vector-excluded rows never match (the read filters
    /// them), and files without a requested position are not read.
    /// </summary>
    /// <param name="atVersion">The snapshot the rowids were SCANNED against (a buffered transaction's pinned
    /// version). Ordinals are path-sort positions in THAT snapshot's active set — resolving them against a moved
    /// CurrentSnapshot would read the wrong files after a concurrent commuting append shifts the ordering.</param>
    /// <param name="sourceRowTrackingOut">When non-null, one entry per YIELDED batch (row-aligned): each matched
    /// row's ORIGINAL stable id (the source file's materialized value where present — a rewritten file — else
    /// <c>baseRowId + absolute position</c>) and commit version. Plain value arrays — no Arrow buffer lifetime to
    /// manage.</param>
    public async IAsyncEnumerable<RecordBatch> ReadRowsByRowIdsAsync(
        IReadOnlyCollection<long> rowIds,
        long? atVersion = null,
        List<(long?[] Ids, long?[] Versions)>? sourceRowTrackingOut = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        var snapshot = atVersion is { } v && v != CurrentSnapshot.Version
            ? await GetSnapshotAtVersionAsync(v, cancellationToken).ConfigureAwait(false)
            : CurrentSnapshot;
        long posMask = (1L << RowIdPositionBits) - 1;
        var positionsByFile = new Dictionary<int, HashSet<long>>();
        foreach (var rid in rowIds)
        {
            int ordinal = (int)(rid >> RowIdPositionBits);
            if (!positionsByFile.TryGetValue(ordinal, out var set))
            {
                set = new HashSet<long>();
                positionsByFile[ordinal] = set;
            }
            set.Add(rid & posMask);
        }

        var ordered = OrderedActiveFiles(snapshot);
        foreach (var kvp in positionsByFile.OrderBy(k => k.Key))
        {
            if (kvp.Key < 0 || kvp.Key >= ordered.Count)
                continue;
            var addFile = ordered[kvp.Key];

            // Master's read path surfaces each surviving row's ABSOLUTE in-file position (DV-inclusive) and its
            // RESOLVED stable id/version via out-params (materialized ids stripped from the emitted user batch),
            // instead of appending a trailing row-id column — so match on the absolute position out-param.
            var absOut = new List<Int64Array?>();
            var idsOut = sourceRowTrackingOut is not null ? new List<Int64Array?>() : null;
            var versOut = sourceRowTrackingOut is not null ? new List<Int64Array?>() : null;
            int bi = -1;
            await foreach (var batch in ReadFileAsync(addFile, null, snapshot, cancellationToken,
                                                      strippedRowIdsOut: idsOut,
                                                      strippedVersionsOut: versOut,
                                                      strippedAbsPositionsOut: absOut).ConfigureAwait(false))
            {
                bi++;
                var absPos = bi < absOut.Count ? absOut[bi] : null;
                if (absPos is null)
                    continue;
                var rows = new List<int>();
                for (int i = 0; i < batch.Length; i++)
                    if (!absPos.IsNull(i) && kvp.Value.Contains(absPos.GetValue(i)!.Value))
                        rows.Add(i);
                if (rows.Count == 0)
                    continue;

                if (sourceRowTrackingOut is not null)
                {
                    var matI = idsOut is not null && bi < idsOut.Count ? idsOut[bi] : null;
                    var matV = versOut is not null && bi < versOut.Count ? versOut[bi] : null;
                    var ids = new long?[rows.Count];
                    var vers = new long?[rows.Count];
                    for (int k = 0; k < rows.Count; k++)
                    {
                        int i = rows[k];
                        ids[k] = matI is not null && !matI.IsNull(i) ? matI.GetValue(i) : null;
                        vers[k] = matV is not null && !matV.IsNull(i) ? matV.GetValue(i) : null;
                    }
                    sourceRowTrackingOut.Add((ids, vers));
                }
                yield return TakeRowsFromBatch(batch, rows);
            }
        }
    }

    /// <summary>
    /// ROW-LEVEL rebase for the buffered surface: re-targets a DV DML action set computed against
    /// <paramref name="from"/> onto <paramref name="to"/> when a concurrent writer swapped a touched file's
    /// deletion vector. Per <c>remove</c>+<c>add</c> DV pair (matched by path): the path must still be ACTIVE
    /// in <paramref name="to"/> (a concurrent rewrite/compaction of the file is a conflict here — the auto
    /// commit path remaps by stable id, the explicit buffered remap is a follow-up); THIS transaction's
    /// newly-deleted positions (<paramref name="newPositionsByOrdinal"/>, keyed by <paramref name="from"/>'s
    /// path-sorted ordinals) must be DISJOINT from the concurrent deletions (an intersection = the same row
    /// deleted/updated by both ⇒ row-level conflict); disjoint ⇒ the pair re-issues against the CURRENT state
    /// (<c>remove</c>(path, current DV) + <c>add</c>(path, current DV ∪ ours)). Post-image adds (paths not in
    /// <paramref name="from"/>) get row-tracking <c>baseRowId</c>/<c>defaultRowCommitVersion</c> re-derived from
    /// <paramref name="to"/>, and the high-water-mark domain rebuilt. Metadata/protocol changes between the
    /// snapshots throw. The caller re-runs commitInfo assembly after the rebase.
    /// </summary>
    public async ValueTask<IReadOnlyList<DeltaAction>> RebaseDvDmlActionsAsync(
        IReadOnlyList<DeltaAction> actions,
        IReadOnlyDictionary<int, IReadOnlyCollection<long>> newPositionsByOrdinal,
        Snapshot.Snapshot from,
        Snapshot.Snapshot to,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        if (to.Version == from.Version)
        {
            return actions;
        }
        if (!MetadataEquals(from.Metadata, to.Metadata))
        {
            throw new DeltaConflictException(
                "concurrent metadata change (schema/partitioning/configuration) — cannot rebase the transaction");
        }
        if (!ProtocolEquals(from.Protocol, to.Protocol))
        {
            throw new DeltaConflictException(
                "concurrent protocol change — cannot rebase the transaction");
        }

        // Our newly-deleted positions per path (ordinals resolve against `from` — what they were captured on).
        var fromOrdered = OrderedActiveFiles(from);
        var oursByPath = new Dictionary<string, IReadOnlyCollection<long>>(StringComparer.Ordinal);
        foreach (var kvp in newPositionsByOrdinal)
        {
            if (kvp.Key >= 0 && kvp.Key < fromOrdered.Count)
                oursByPath[fromOrdered[kvp.Key].Path] = kvp.Value;
        }
        var fromByPath = new HashSet<string>(StringComparer.Ordinal);
        foreach (var f in from.ActiveFiles.Values)
            fromByPath.Add(f.Path);
        var toByPath = new Dictionary<string, AddFile>(to.ActiveFiles.Count, StringComparer.Ordinal);
        foreach (var f in to.ActiveFiles.Values)
            toByPath[f.Path] = f;

        bool rowTrackingEnabled = DeltaLake.RowTracking.RowTrackingConfig.IsEnabled(to.Metadata.Configuration);
        long nextRowId = rowTrackingEnabled ? to.RowIdHighWaterMark : 0;
        bool anyPostImage = false;
        var dvWriter = new DeletionVectors.DeletionVectorWriter(_fs);
        var rebased = new List<DeltaAction>(actions.Count);

        foreach (var action in actions)
        {
            switch (action)
            {
                case RemoveFile remove when oursByPath.ContainsKey(remove.Path):
                {
                    if (!toByPath.TryGetValue(remove.Path, out var current))
                    {
                        // The file was REWRITTEN (compaction / copy-on-write) concurrently. The auto commit
                        // path remaps the rows by stable id; the explicit buffered remap-across-rewrite is a
                        // follow-up, so conflict here.
                        throw new DeltaConflictException(
                            $"concurrent rewrite/compaction of file '{remove.Path}' this transaction modifies — "
                            + "cannot rebase the buffered transaction; retry it");
                    }
                    rebased.Add(remove with { DeletionVector = current.DeletionVector });
                    break;
                }
                case AddFile add when oursByPath.TryGetValue(add.Path, out var ours):
                {
                    // The DV-pair re-add: union OUR positions with the CURRENT deletion vector, after the
                    // row-level disjointness check against the concurrent deletions.
                    var current = toByPath[add.Path]; // present — the paired remove above would have thrown
                    var currentDeleted = current.DeletionVector is not null
                        ? new HashSet<long>(await _dvReader.ReadAsync(current.DeletionVector, cancellationToken)
                            .ConfigureAwait(false))
                        : new HashSet<long>();
                    int overlap = 0;
                    foreach (long p in ours)
                        if (currentDeleted.Contains(p))
                            overlap++;
                    if (overlap > 0)
                    {
                        throw new DeltaConflictException(
                            $"row-level conflict on file '{add.Path}': {overlap} row(s) this transaction "
                            + "deletes/updates were concurrently deleted or updated — retry the transaction");
                    }
                    foreach (long p in ours)
                        currentDeleted.Add(p);
                    var newDv = await dvWriter.CreateAsync(currentDeleted, currentDeleted.Count, cancellationToken)
                        .ConfigureAwait(false);
                    rebased.Add(current with
                    {
                        DeletionVector = newDv,
                        DataChange = true,
                        Stats = StatsWithLooseBounds(current.Stats),
                    });
                    break;
                }
                case AddFile add when !fromByPath.Contains(add.Path) && add.DataChange:
                {
                    // Post-image add (a brand-new file): re-derive its row-id range from the snapshot we are
                    // committing onto — concurrent commits may have consumed row-id space.
                    if (rowTrackingEnabled && add.BaseRowId is not null)
                    {
                        long rows = Actions.ColumnStats.Parse(add.Stats)?.NumRecords ?? 0;
                        rebased.Add(add with
                        {
                            BaseRowId = nextRowId,
                            DefaultRowCommitVersion = to.Version + 1,
                        });
                        nextRowId += rows;
                        anyPostImage = true;
                    }
                    else
                    {
                        rebased.Add(add);
                    }
                    break;
                }
                case Actions.DomainMetadata dm
                    when string.Equals(dm.Domain, DeltaLake.RowTracking.RowTrackingConfig.DomainName,
                                       StringComparison.Ordinal):
                    // Re-emitted after the loop with the re-derived mark.
                    anyPostImage = true;
                    break;
                default:
                    rebased.Add(action);
                    break;
            }
        }
        if (rowTrackingEnabled && anyPostImage)
        {
            rebased.Add(DeltaLake.RowTracking.RowTrackingConfig.BuildHighWaterMarkAction(nextRowId));
        }
        return rebased;
    }

    /// <summary>
    /// Validates that <paramref name="plannedActions"/> may still commit onto the CURRENT snapshot given the
    /// transaction's <paramref name="baseSnapshot"/> — the OptimisticTransaction conflict check for the buffered
    /// surface. Metadata/protocol changes abort; every planned <c>RemoveFile</c> must still be active UNCHANGED
    /// (same path + same deletion vector — a concurrent delete/rewrite of a file this transaction also modifies
    /// conflicts). Read-set checks (concurrentDeleteRead / concurrentAppend, per <paramref name="readPredicates"/>
    /// or <paramref name="readWholeTable"/>, isolation-scoped by <paramref name="serializable"/>) run unless
    /// <paramref name="rowLevelDml"/> — row-level mode replaces them with the row-granular validation the rebase
    /// already performed (same-row overlap conflicts there; under WriteSerializable reads are not serialized).
    /// </summary>
    public async ValueTask CheckLogicalRebaseAsync(
        Snapshot.Snapshot baseSnapshot,
        IReadOnlyList<DeltaAction> plannedActions,
        IReadOnlyList<Expressions.Predicate>? readPredicates = null,
        bool readWholeTable = false,
        bool serializable = false,
        bool rowLevelDml = false,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        var latest = CurrentSnapshot;
        if (latest.Version == baseSnapshot.Version)
        {
            return;
        }
        if (!MetadataEquals(baseSnapshot.Metadata, latest.Metadata))
        {
            throw new DeltaConflictException(
                "concurrent metadata change (schema/partitioning/configuration) — cannot rebase the transaction");
        }
        if (!ProtocolEquals(baseSnapshot.Protocol, latest.Protocol))
        {
            throw new DeltaConflictException(
                "concurrent protocol change — cannot rebase the transaction");
        }
        // delete/delete: every file the transaction removes (DV remove+add pairs, rewrites) must still be active
        // UNCHANGED — same path with the same deletion vector (DeletionVector is a record: value equality).
        Dictionary<string, AddFile>? latestByPath = null;
        foreach (var action in plannedActions)
        {
            if (action is not RemoveFile remove)
            {
                continue;
            }
            if (latestByPath is null)
            {
                latestByPath = new Dictionary<string, AddFile>(latest.ActiveFiles.Count, StringComparer.Ordinal);
                foreach (var f in latest.ActiveFiles.Values)
                {
                    latestByPath[f.Path] = f;
                }
            }
            if (!latestByPath.TryGetValue(remove.Path, out var current)
                || !Equals(current.DeletionVector, remove.DeletionVector))
            {
                throw new DeltaConflictException(
                    $"concurrent delete/rewrite of file '{remove.Path}' this transaction also modifies — "
                    + "cannot rebase the transaction");
            }
        }

        // Read-set checks (skipped when the caller recorded no reads — pure delete/delete mode). ROW-LEVEL mode
        // (rowLevelDml, WriteSerializable only): the read checks are REPLACED by the row-level write validation
        // the rebase performed.
        bool hasReads = readWholeTable || readPredicates is { Count: > 0 };
        if (!hasReads || rowLevelDml)
        {
            return;
        }
        var pruner = new DeltaFilePruner(baseSnapshot.Schema, baseSnapshot.Metadata.PartitionColumns);
        bool ReadsMatch(AddFile file)
        {
            if (readWholeTable)
            {
                return true;
            }
            foreach (var predicate in readPredicates!)
            {
                if (pruner.ShouldInclude(file, predicate))
                {
                    return true;
                }
            }
            return false;
        }
        var baseByPath = new Dictionary<string, AddFile>(baseSnapshot.ActiveFiles.Count, StringComparer.Ordinal);
        foreach (var f in baseSnapshot.ActiveFiles.Values)
        {
            baseByPath[f.Path] = f;
        }
        for (long v = baseSnapshot.Version + 1; v <= latest.Version; v++)
        {
            var commitActions = await _log.ReadCommitAsync(v, cancellationToken).ConfigureAwait(false);
            bool blindAppend = true;
            foreach (var a in commitActions)
            {
                if (a is RemoveFile or MetadataAction or ProtocolAction)
                {
                    blindAppend = false;
                    break;
                }
            }
            foreach (var a in commitActions)
            {
                switch (a)
                {
                    case RemoveFile removed when removed.DataChange:
                        // concurrentDeleteReadCheck: the file existed in our base snapshot and our reads could
                        // have consumed rows from it. dataChange=false (compaction) is exempt.
                        if (baseByPath.TryGetValue(removed.Path, out var readFile) && ReadsMatch(readFile))
                        {
                            throw new DeltaConflictException(
                                $"concurrent delete/rewrite of file '{removed.Path}' this transaction read "
                                + $"(commit v{v}) — cannot rebase the transaction");
                        }
                        break;
                    case AddFile added when added.DataChange && (!blindAppend || serializable):
                        // concurrentAppendCheck: rows appeared that the transaction's reads would have consumed.
                        // Blind appends are exempt under WriteSerializable; under Serializable they conflict.
                        if (ReadsMatch(added))
                        {
                            throw new DeltaConflictException(
                                $"concurrent append of file '{added.Path}' matching this transaction's reads "
                                + $"(commit v{v}) — cannot rebase the transaction");
                        }
                        break;
                }
            }
        }
    }

    private static bool MetadataEquals(MetadataAction a, MetadataAction b)
    {
        if (!string.Equals(a.Id, b.Id, StringComparison.Ordinal)
            || !string.Equals(a.SchemaString, b.SchemaString, StringComparison.Ordinal)
            || !a.PartitionColumns.SequenceEqual(b.PartitionColumns, StringComparer.Ordinal))
        {
            return false;
        }
        var ca = a.Configuration;
        var cb = b.Configuration;
        if ((ca?.Count ?? 0) != (cb?.Count ?? 0))
        {
            return false;
        }
        if (ca is not null && cb is not null)
        {
            foreach (var kv in ca)
            {
                if (!cb.TryGetValue(kv.Key, out var v) || !string.Equals(kv.Value, v, StringComparison.Ordinal))
                {
                    return false;
                }
            }
        }
        return true;
    }

    private static bool ProtocolEquals(ProtocolAction a, ProtocolAction b)
    {
        if (a.MinReaderVersion != b.MinReaderVersion || a.MinWriterVersion != b.MinWriterVersion)
        {
            return false;
        }
        static bool FeaturesEqual(IReadOnlyList<string>? x, IReadOnlyList<string>? y)
        {
            var sx = new HashSet<string>(x ?? System.Array.Empty<string>(), StringComparer.Ordinal);
            var sy = new HashSet<string>(y ?? System.Array.Empty<string>(), StringComparer.Ordinal);
            return sx.SetEquals(sy);
        }
        return FeaturesEqual(a.ReaderFeatures, b.ReaderFeatures) && FeaturesEqual(a.WriterFeatures, b.WriterFeatures);
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
        RejectRowTrackingWrite(CurrentSnapshot); // refused only if a row-tracking table lacks materialized names

        options ??= CompactionOptions.Default;
        var result = await Compaction.CompactionExecutor.ExecuteAsync(
            _fs, _log, CurrentSnapshot, options,
            _options.ParquetWriteOptions, _dataFileReadOptions,
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

        // Precedence: explicit argument (Spark's RETAIN N HOURS), else the table's own
        // delta.deletedFileRetentionDuration, else the library default. Measured against
        // delta-spark 4.0.0: a RETAIN-less VACUUM on a table with the property set to
        // "interval 0 seconds" collects a just-orphaned file immediately, so the property really is
        // the default retention rather than an independent protection window.
        var retention = retentionPeriod
            ?? DeletedFileRetention(CurrentSnapshot.Metadata.Configuration)
            ?? _options.VacuumRetention;

        return await Vacuum.VacuumExecutor.ExecuteAsync(
            _fs, _log, CurrentSnapshot, retention, dryRun, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// The table's <c>delta.deletedFileRetentionDuration</c> as a <see cref="TimeSpan"/>, or null when
    /// unset or unparseable. Unparseable falls through to the caller's default rather than throwing —
    /// an odd property value must not make a table impossible to vacuum.
    /// </summary>
    private static TimeSpan? DeletedFileRetention(IReadOnlyDictionary<string, string>? configuration)
    {
        if (configuration is null
            || !configuration.TryGetValue("delta.deletedFileRetentionDuration", out string? raw))
        {
            return null;
        }

        return IntervalParser.TryParse(raw, out var parsed) ? parsed : null;
    }

    private async IAsyncEnumerable<RecordBatch> ReadFileAsync(
        AddFile addFile,
        IReadOnlyList<string>? columns,
        Snapshot.Snapshot snapshot,
        [EnumeratorCancellation] CancellationToken cancellationToken,
        List<Int64Array?>? strippedRowIdsOut = null,
        List<Int64Array?>? strippedVersionsOut = null,
        List<Int64Array?>? strippedAbsPositionsOut = null)
    {
        // strippedRowIdsOut/strippedVersionsOut: when non-null, each EMITTED batch appends its per-row RESOLVED
        // row id / commit version (materialized value where present, else add.baseRowId + absolute position /
        // add.defaultRowCommitVersion; null when underivable). A copy-on-write rewrite (UPDATE) uses these so a
        // moved row keeps its ORIGINAL id. strippedAbsPositionsOut adds the surviving row's ABSOLUTE in-file
        // position (parquet row index, DV-inclusive) — the row-level remap (Layer 3 B) uses it to correlate a
        // delete's target positions with stable row ids and to place the rebased deletion vector. The hidden
        // materialized columns are always stripped from the emitted user batches regardless (a foreign reader
        // must never see them).
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
                hasPartitions, cancellationToken, strippedRowIdsOut, strippedVersionsOut,
                strippedAbsPositionsOut).ConfigureAwait(false))
            {
                yield return processed;
            }
            yield break;
        }

        // Open the file and read its Parquet schema for field_id resolution
        await using var file = await _fs.OpenReadAsync(EngineeredWood.DeltaLake.DeltaPath.Decode(addFile.Path), cancellationToken)
            .ConfigureAwait(false);
        using var reader = new ParquetFileReader(
            file, ownsFile: false, _dataFileReadOptions);

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
            hasPartitions, cancellationToken, strippedRowIdsOut, strippedVersionsOut,
            strippedAbsPositionsOut).ConfigureAwait(false))
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
        [EnumeratorCancellation] CancellationToken cancellationToken,
        List<Int64Array?>? strippedRowIdsOut = null,
        List<Int64Array?>? strippedVersionsOut = null,
        List<Int64Array?>? strippedAbsPositionsOut = null)
    {
        long batchStartRow = 0;

        // Hidden materialized row-tracking columns (a copy-on-write rewrite wrote each moved row's original id +
        // commit version under these declared physical names). Stripped from every emitted batch so a reader
        // never sees them; their values feed the rowid out-params when a caller (UPDATE) requests them.
        var (matRowIdName, matRowVerName) = DeltaLake.RowTracking.RowTrackingConfig
            .TryGetMaterializedColumnNames(snapshot.Metadata.Configuration);
        bool hasMaterialized = matRowIdName is not null || matRowVerName is not null;
        bool wantRowIds = strippedRowIdsOut is not null || strippedVersionsOut is not null
            || strippedAbsPositionsOut is not null;

        await foreach (var batch in source.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            long thisBatchStart = batchStartRow; // absolute file position of this raw batch's first row

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

            // Strip the hidden materialized row-tracking columns UP FRONT (before DV filter / widening /
            // partition re-add / backfill), so the rest of the pipeline operates on exactly the user columns
            // — its behavior is unchanged for a file that carries no such column (the common case).
            Int64Array? rawMatIds = null, rawMatVers = null;
            if (hasMaterialized)
            {
                (result, rawMatIds, rawMatVers) = RowTracking.RowTrackingWriter
                    .StripMaterializedColumns(result, matRowIdName, matRowVerName);
            }

            // Source indices (into this raw batch) of rows that survive DV filtering, in emit order — captured
            // before the filter drops them, so the rowid out-params stay row-aligned with the emitted batch.
            List<int>? survivorSrc = wantRowIds ? new List<int>(batch.Length) : null;
            if (survivorSrc is not null)
            {
                for (int i = 0; i < batch.Length; i++)
                    if (deletedRows is null || !deletedRows.Contains(thisBatchStart + i))
                        survivorSrc.Add(i);
            }

            // Apply deletion vector filtering
            if (deletedRows is not null)
            {
                result = DeletionVectorFilter.Filter(result, deletedRows, batchStartRow);
                batchStartRow += batch.Length;

                if (result.Length == 0)
                    continue; // All rows in this batch were deleted (no surviving ids to emit either)
            }
            else
            {
                batchStartRow += batch.Length; // track absolute position for the rowid out-params
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

            // The materialized row-tracking columns were already stripped up front (above).
            var cleanResult = result;

            // Schema evolution: ADD/DROP COLUMN are metadata-only commits, so a file written before an ADD
            // lacks the column and one written before a DROP still carries it — reconcile every emitted batch
            // to the current schema's expected output columns (absent ones backfilled as typed all-NULL).
            var expectedSchema = columns is not null
                ? BuildProjectedSchema(snapshot.ArrowSchema, columns)
                : snapshot.ArrowSchema;
            cleanResult = SchemaEvolution.BackfillMissingColumns(cleanResult, expectedSchema.FieldsList);

            // Present variant columns per the Delta SCHEMA, not the parquet annotation: an unannotated
            // file (Spark 4.0.x, a spec-minimal writer, or our own output under
            // EmitVariantLogicalType=false) yields a bare struct-of-binary that the parquet reader did
            // not wrap. Without this the column would silently read as a struct rather than a variant.
            cleanResult = VariantColumnCoercion.Coerce(cleanResult, expectedSchema);

            // Surface each surviving row's RESOLVED id + commit version (row-aligned with cleanResult): the
            // materialized value where present, else add.baseRowId + absolute position / defaultRowCommitVersion
            // (null when the file carries neither — a pre-row-tracking source). The rewrite path preserves ids.
            if (wantRowIds)
            {
                var idb = new Int64Array.Builder();
                var vrb = new Int64Array.Builder();
                var pb = new Int64Array.Builder();
                foreach (int i in survivorSrc!)
                {
                    long? mid = rawMatIds is not null && !rawMatIds.IsNull(i) ? rawMatIds.GetValue(i) : null;
                    long? id = mid ?? (addFile.BaseRowId is { } ab ? ab + thisBatchStart + i : (long?)null);
                    if (id is { } iv) idb.Append(iv); else idb.AppendNull();

                    long? mv = rawMatVers is not null && !rawMatVers.IsNull(i) ? rawMatVers.GetValue(i) : null;
                    long? ver = mv ?? addFile.DefaultRowCommitVersion;
                    if (ver is { } vv) vrb.Append(vv); else vrb.AppendNull();

                    pb.Append(thisBatchStart + i); // absolute in-file position (DV-inclusive), for the remap
                }
                strippedRowIdsOut?.Add(idb.Build());
                strippedVersionsOut?.Add(vrb.Build());
                strippedAbsPositionsOut?.Add(pb.Build());
            }

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
