// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.CompilerServices;
using Apache.Arrow;
using EngineeredWood.Arrow;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Checkpoint;
using EngineeredWood.DeltaLake.Concurrency;
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

    /// <summary>
    /// The optimistic-concurrency commit loop, in the log layer. Every commit this table makes goes through
    /// it; what stays up here is the part that needs a data plane — re-deriving version-coupled actions on
    /// a rebase, and collecting the files a losing attempt left behind.
    /// </summary>
    private readonly LogCommitter _committer;

    /// <summary>
    /// The interval THIS table checkpoints at — its own <c>delta.checkpointInterval</c> where it declares
    /// one, else the caller's option. Resolved once and held, because there are TWO independent checkpoint
    /// triggers — the commit loop's <see cref="LogCommitOptions"/> and
    /// <see cref="CheckpointIfDueAsync"/>, which every path that commits outside the committer calls — and
    /// a value read separately in each is a value that can drift: fixing only one leaves the property
    /// honoured on some write paths and ignored on others, which is harder to notice than ignoring it
    /// everywhere.
    /// </summary>
    private readonly int _checkpointInterval;

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
        _checkpointWriter = new CheckpointWriter(fileSystem, options.ParquetWriteOptions)
        {
            Format = options.CheckpointFormat,
        };
        _checkpointInterval = ResolveCheckpointInterval(options, snapshot);
        _committer = new LogCommitter(_log, new LogCommitOptions
        {
            CheckpointInterval = _checkpointInterval,
            // Shared with CheckpointAsync and with CheckpointIfDueAsync, so every checkpoint this table
            // writes — on the interval, from either commit trigger, or because a caller asked — uses the
            // caller's parquet options and checkpoint format rather than the committer's defaults.
            CheckpointWriter = _checkpointWriter,
            PreferTypedCheckpointStats = options.PreferTypedCheckpointStats,
        });
        _currentSnapshot = snapshot;
    }

    /// <summary>
    /// The checkpoint interval this table actually commits at: its own <c>delta.checkpointInterval</c>
    /// where it declares one, otherwise the caller's <see cref="DeltaTableOptions.CheckpointInterval"/>.
    ///
    /// <para>The property is part of the Delta spec and a table's own statement about how often it wants
    /// checkpointing — a cost it pays per commit and a count another engine may be tuning deliberately.
    /// Reading only the code-level option meant a table declaring 100 was still checkpointed every 10,
    /// i.e. ten times the objects its owner asked for. The value is STORED by writers that accept the
    /// property, so ignoring it is not neutral: it is honouring someone else's declaration incorrectly.</para>
    ///
    /// <para><b>⚠ A caller that DISABLED checkpointing keeps it disabled.</b> <c>CheckpointInterval = 0</c>
    /// means "never checkpoint" and is an absolute caller override — a table property must not switch it
    /// back on, or a host that deliberately owns checkpointing on its own schedule would start racing one
    /// it did not ask for.</para>
    ///
    /// <para>Resolved once per open, from the snapshot the table was constructed with. A value changed by
    /// a later <c>set_tblproperties</c> therefore takes effect on the next open, which is the same
    /// granularity every other configuration read here has.</para>
    /// </summary>
    private static int ResolveCheckpointInterval(DeltaTableOptions options, Snapshot.Snapshot? snapshot)
    {
        // Checked BEFORE the property is read, not after: zero is the caller taking the decision, so there
        // is nothing for the table to declare over.
        if (options.CheckpointInterval <= 0)
            return options.CheckpointInterval;

        return Checkpoint.CheckpointIntervalProperty.TryGet(snapshot?.Metadata.Configuration)
            ?? options.CheckpointInterval;
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
            throw new DeltaTableNotFoundException("No Delta table found (no commits in _delta_log/).");

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
    /// <param name="configuration">
    /// Table properties (<c>delta.*</c>) to record in the table's metadata, e.g.
    /// <c>delta.checkpoint.writeStatsAsJson</c> or <c>delta.deletedFileRetentionDuration</c>.
    /// <para>The <c>delta.enable*</c> / mode properties in it ENABLE their feature exactly like the
    /// dedicated arguments do, and each is declared in the commit-0 protocol: column mapping
    /// (<c>delta.columnMapping.mode</c>), deletion vectors, row tracking, in-commit timestamps
    /// (<c>delta.enableInCommitTimestamps</c>), change data feed (<c>delta.enableChangeDataFeed</c>) and
    /// Iceberg compatibility (<c>delta.enableIcebergCompatV1</c> / <c>…V2</c>). Enablement is
    /// one-directional — a property can turn a feature on, never off — so an argument and a property that
    /// disagree resolve to the argument. Internally-derived keys (the column-mapping mode and max id)
    /// overwrite caller-supplied ones; caller-supplied row-tracking materialized column names win.</para>
    /// </param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <param name="preAssignedSchema">
    /// A Delta schema whose column-mapping field ids and physical names were assigned BEFORE this call, used
    /// verbatim in place of converting <paramref name="schema"/>. For a host that streams a CTAS's data files
    /// eagerly, before the table exists: physical names are random GUIDs, so letting the create mint fresh
    /// ones would orphan every file already written under the old names. Assign the mapping first, write the
    /// files against it, then create with the same schema here.
    /// <para><paramref name="schema"/> is ignored when this is supplied. Under column mapping the schema must
    /// already carry ids; the metadata's max-column-id is derived from it rather than reassigned.</para>
    /// </param>
    public static ValueTask<DeltaTable> CreateAsync(
        ITableFileSystem fileSystem,
        Apache.Arrow.Schema schema,
        DeltaTableOptions? options = null,
        IReadOnlyList<string>? partitionColumns = null,
        ColumnMappingMode columnMappingMode = ColumnMappingMode.None,
        IReadOnlyList<string>? clusteringColumns = null,
        bool enableDeletionVectors = false,
        bool enableRowTracking = false,
        IReadOnlyDictionary<string, string>? configuration = null,
        CancellationToken cancellationToken = default,
        Schema.StructType? preAssignedSchema = null)
        => CreateCoreAsync(
            fileSystem,
            schema,
            initialBatches: null,
            replaceExisting: false,
            options,
            partitionColumns,
            columnMappingMode,
            clusteringColumns,
            enableDeletionVectors,
            enableRowTracking,
            configuration,
            cancellationToken,
            preAssignedSchema);

    /// <summary>
    /// Creates a Delta table with initial data, or atomically replaces an existing table and its active
    /// data. Protocol, metadata, removes, and initial adds are published in one commit: version 0 for a new
    /// table, or the next version for a replacement. Existing history is preserved.
    /// </summary>
    /// <param name="fileSystem">The filesystem rooted at the table directory.</param>
    /// <param name="schema">The Arrow schema for the replacement table.</param>
    /// <param name="initialBatches">The data made visible by the create-or-replace commit.</param>
    /// <param name="options">Table options.</param>
    /// <param name="partitionColumns">Ordered list of partition column names.</param>
    /// <param name="columnMappingMode">Column mapping mode for the replacement metadata.</param>
    /// <param name="clusteringColumns">Liquid-clustering columns for the replacement metadata.</param>
    /// <param name="enableDeletionVectors">Whether deletion vectors are enabled.</param>
    /// <param name="enableRowTracking">Whether row tracking is enabled.</param>
    /// <param name="configuration">Replacement table properties.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <param name="preAssignedSchema">
    /// A Delta schema with preassigned column-mapping ids and physical names. See
    /// <see cref="CreateAsync"/>.
    /// </param>
    public static ValueTask<DeltaTable> CreateOrReplaceAsync(
        ITableFileSystem fileSystem,
        Apache.Arrow.Schema schema,
        IReadOnlyList<RecordBatch> initialBatches,
        DeltaTableOptions? options = null,
        IReadOnlyList<string>? partitionColumns = null,
        ColumnMappingMode columnMappingMode = ColumnMappingMode.None,
        IReadOnlyList<string>? clusteringColumns = null,
        bool enableDeletionVectors = false,
        bool enableRowTracking = false,
        IReadOnlyDictionary<string, string>? configuration = null,
        CancellationToken cancellationToken = default,
        Schema.StructType? preAssignedSchema = null)
    {
        if (initialBatches is null)
            throw new ArgumentNullException(nameof(initialBatches));

        return CreateCoreAsync(
            fileSystem,
            schema,
            initialBatches,
            replaceExisting: true,
            options,
            partitionColumns,
            columnMappingMode,
            clusteringColumns,
            enableDeletionVectors,
            enableRowTracking,
            configuration,
            cancellationToken,
            preAssignedSchema);
    }

    private static async ValueTask<DeltaTable> CreateCoreAsync(
        ITableFileSystem fileSystem,
        Apache.Arrow.Schema schema,
        IReadOnlyList<RecordBatch>? initialBatches,
        bool replaceExisting,
        DeltaTableOptions? options,
        IReadOnlyList<string>? partitionColumns,
        ColumnMappingMode columnMappingMode,
        IReadOnlyList<string>? clusteringColumns,
        bool enableDeletionVectors,
        bool enableRowTracking,
        IReadOnlyDictionary<string, string>? configuration,
        CancellationToken cancellationToken,
        Schema.StructType? preAssignedSchema)
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
                DeltaTableErrorCodes.ClusteringWithPartitioning,
                "Liquid clustering and partitioning are mutually exclusive — a partitioned table cannot "
                + "declare clustering columns.");
        }

        // Check that the table doesn't already exist
        long latestVersion = await log.GetLatestVersionAsync(cancellationToken)
            .ConfigureAwait(false);

        if (latestVersion >= 0 && !replaceExisting)
            throw new InvalidOperationException("Delta table already exists.");

        Snapshot.Snapshot? previousSnapshot = null;
        if (latestVersion >= 0)
        {
            previousSnapshot = await SnapshotBuilder.BuildAsync(
                log, new CheckpointReader(fileSystem), latestVersion, cancellationToken)
                .ConfigureAwait(false);
            ProtocolVersions.ValidateWriteSupport(previousSnapshot.Protocol);
        }

        // Convert Arrow schema to Delta schema — unless the caller assigned one ALREADY (see the parameter
        // doc: a CTAS whose data files were written before commit 0 exists).
        var deltaSchema = preAssignedSchema ?? SchemaConverter.FromArrowSchema(schema);

        // Set protocol versions based on column mapping mode
        int minReaderVersion = 1;
        int minWriterVersion = 2;
        // Caller-supplied properties seed the configuration; the feature flags below overwrite their
        // own keys, so an argument and a property that disagree resolve to the argument.
        Dictionary<string, string>? configurationBuilder = configuration is { Count: > 0 }
            ? new Dictionary<string, string>(configuration.Count)
            : null;
        if (configurationBuilder is not null)
        {
            foreach (var kvp in configuration!)
                configurationBuilder[kvp.Key] = kvp.Value;
        }

        // A feature is enabled by EITHER its dedicated argument or its table property, symmetrically — a
        // caller translating CREATE TABLE ... TBLPROPERTIES gets the same table as one passing the flags.
        // Enablement is one-directional (a property turns a feature on, never off), so the argument stays
        // the source of truth where the two disagree. Everything enabled here MUST also be declared in the
        // protocol below: a delta.enable* property with no matching table feature is exactly what a strict
        // reader rejects as DELTA_FEATURES_PROTOCOL_METADATA_MISMATCH.
        var mappingMode = columnMappingMode != ColumnMappingMode.None
            ? columnMappingMode
            : ColumnMapping.GetMode(configurationBuilder);
        bool dvEnabled = enableDeletionVectors
            || DeletionVectors.DeletionVectorConfig.IsEnabled(configurationBuilder);
        bool rowTrackingEnabled = enableRowTracking
            || DeltaLake.RowTracking.RowTrackingConfig.IsEnabled(configurationBuilder);
        bool inCommitTimestampsEnabled = Log.InCommitTimestamp.IsEnabled(configurationBuilder);
        bool changeDataFeedEnabled = DeltaLake.ChangeDataFeed.CdfConfig.IsEnabled(configurationBuilder);
        bool v2CheckpointsEnabled = Checkpoint.CheckpointPolicy.WantsV2(configurationBuilder);
        var icebergCompatVersion = Schema.IcebergCompat.GetVersion(configurationBuilder);

        if (mappingMode != ColumnMappingMode.None)
        {
            minReaderVersion = 2;
            minWriterVersion = 5;

            int maxId;
            if (preAssignedSchema is not null)
            {
                // Re-assigning would mint FRESH physical names — random GUIDs — and every data file the
                // caller already wrote under the old ones would become unreadable. Keep what was assigned and
                // only derive the max id the metadata must record.
                maxId = ColumnMapping.GetMaxColumnId(deltaSchema);
                if (maxId == 0)
                {
                    throw new DeltaFormatException(
                        DeltaTableErrorCodes.InvalidPreAssignedSchema,
                        "preAssignedSchema declares no column-mapping field ids, but the table is being "
                        + $"created with column mapping '{mappingMode}'. Assign ids and physical names before "
                        + "writing the data files, or create without column mapping.");
                }
                if (previousSnapshot is not null)
                {
                    int previousMaxId = GetColumnMappingHighWaterMark(previousSnapshot);
                    ValidateReplacementColumnMappingIds(deltaSchema, previousMaxId);
                }
            }
            else
            {
                // Assign column mapping IDs and physical names
                int startId = previousSnapshot is null
                    ? 0
                    : GetColumnMappingHighWaterMark(previousSnapshot);
                var (mappedSchema, assignedMaxId) =
                    ColumnMapping.AssignColumnMapping(deltaSchema, startId);
                deltaSchema = mappedSchema;
                maxId = assignedMaxId;
            }

            string modeStr = mappingMode switch
            {
                ColumnMappingMode.Id => "id",
                ColumnMappingMode.Name => "name",
                _ => "none",
            };

            configurationBuilder ??= new Dictionary<string, string>();
            configurationBuilder[ColumnMapping.ModeKey] = modeStr;
            configurationBuilder[ColumnMapping.MaxColumnIdKey] = maxId.ToString();
        }

        // Deletion vectors are opt-in: set the table property so the DELETE path knows it may soft-delete
        // rows with a DV, and declare the reader+writer feature below so foreign readers apply them.
        if (dvEnabled)
        {
            configurationBuilder ??= new Dictionary<string, string>();
            configurationBuilder[DeletionVectors.DeletionVectorConfig.EnableKey] = "true";
        }

        // Row tracking is opt-in: set the property and store the two spec-required hidden column names now
        // (they are fixed at enablement — a reader consults them to find the materialized id/version columns
        // an eventual rewrite writes). Fresh appends need neither column: a row's id is baseRowId + position.
        // Caller-supplied names (in `configuration`) WIN — a table being recreated, or one whose files were
        // already written against known names, must keep them; only an absent name is generated.
        // The rowTracking + domainMetadata writer features are declared below.
        if (rowTrackingEnabled)
        {
            configurationBuilder ??= new Dictionary<string, string>();
            configurationBuilder[DeltaLake.RowTracking.RowTrackingConfig.EnableKey] = "true";
            string rowIdKey = DeltaLake.RowTracking.RowTrackingConfig.MaterializedRowIdColumnNameKey;
            string rowVersionKey =
                DeltaLake.RowTracking.RowTrackingConfig.MaterializedRowCommitVersionColumnNameKey;
            if (!configurationBuilder.ContainsKey(rowIdKey)
                || !configurationBuilder.ContainsKey(rowVersionKey))
            {
                var (rowIdCol, rowCommitVersionCol) =
                    DeltaLake.RowTracking.RowTrackingConfig.GenerateMaterializedColumnNames();
                if (!configurationBuilder.ContainsKey(rowIdKey))
                    configurationBuilder[rowIdKey] = rowIdCol;
                if (!configurationBuilder.ContainsKey(rowVersionKey))
                    configurationBuilder[rowVersionKey] = rowCommitVersionCol;
            }
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
        if (dvEnabled)
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
        if (rowTrackingEnabled)
        {
            minWriterVersion = 7;
            writerFeatures.Add("rowTracking");
            if (!writerFeatures.Contains("domainMetadata"))
                writerFeatures.Add("domainMetadata");
        }

        // In-commit timestamps — a WRITER-only feature ('inCommitTimestamp'); readers read the table
        // normally and only consult commitInfo.inCommitTimestamp instead of the file modification time.
        // Because it is enabled AT CREATION (version 0), the spec requires NO
        // delta.inCommitTimestampEnablementVersion / …Timestamp pair: those exist to mark where a
        // mid-life enablement began, and here every commit in the table's history carries the field
        // (EnsureCommitInfo writes it, including for this creation commit).
        if (inCommitTimestampsEnabled)
        {
            minWriterVersion = 7;
            writerFeatures.Add("inCommitTimestamp");
        }

        // Change data feed — a WRITER-only feature ('changeDataFeed'); readers read data normally and the
        // change feed is a separate, opt-in read. Declaring it is what obliges every writer of this table
        // (including foreign ones) to emit _change_data for row-level changes, which is what makes the feed
        // COMPLETE — a feed with a silent gap is worse than no feed. The DML paths honor it from the table
        // property; ReadChangesAsync / table_changes then return the recorded changes.
        if (changeDataFeedEnabled)
        {
            minWriterVersion = 7;
            writerFeatures.Add("changeDataFeed");
        }

        // V2 checkpoints — a READER+WRITER feature ('v2Checkpoint', reader 3 / writer 7), enabled by the
        // delta.checkpointPolicy=v2 table property exactly as delta-spark does it. Both lists, because a
        // UUID-named checkpoint is a form readers must understand as well as writers. Declaring it here is
        // what makes the property mean anything: CheckpointWriter writes V2 only when the policy asks AND
        // the feature permits, so a table created with the property but without the feature would quietly
        // keep getting classic checkpoints. Nothing else about the table changes — a V2-checkpointed table
        // is read and written normally, and the spec's one further obligation (no multi-part checkpoints)
        // is already met, since this writer never produces them.
        if (v2CheckpointsEnabled)
        {
            minReaderVersion = 3;
            minWriterVersion = 7;
            readerFeatures.Add(Checkpoint.CheckpointPolicy.FeatureName);
            writerFeatures.Add(Checkpoint.CheckpointPolicy.FeatureName);
        }

        // Iceberg compatibility — WRITER-only features ('icebergCompatV1' / 'icebergCompatV2'); readers see
        // an ordinary Delta table. The constraints bind the WRITER (column mapping required, partition
        // values materialized into the data files, numRecords in every stats blob; V1 additionally forbids
        // deletion vectors and array/map columns) so that an external converter — UniForm — can generate
        // Iceberg metadata over the very same parquet files. Declaring it is what tells a foreign writer it
        // must honor them too; the full constraint set is validated against the finished commit-0 actions
        // below. V2 wins if a caller somehow enables both (matching IcebergCompat.GetVersion).
        if (icebergCompatVersion != IcebergCompatVersion.None)
        {
            minWriterVersion = 7;
            writerFeatures.Add(icebergCompatVersion == IcebergCompatVersion.V2
                ? "icebergCompatV2"
                : "icebergCompatV1");
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
        // closed on those. See doc/known-issues.md, "Column-mapping protocol shape differs from Spark's".
        if (mappingMode != ColumnMappingMode.None &&
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

        ProtocolAction protocolAction = new ProtocolAction
        {
            MinReaderVersion = minReaderVersion,
            MinWriterVersion = minWriterVersion,
            ReaderFeatures = readerFeatures.Count > 0 ? readerFeatures : null,
            WriterFeatures = writerFeatures.Count > 0 ? writerFeatures : null,
        };

        if (previousSnapshot is not null)
            protocolAction = MergeReplacementProtocol(previousSnapshot.Protocol, protocolAction);

        var metadataAction = new MetadataAction
        {
            Id = Guid.NewGuid().ToString(),
            Format = Format.Parquet,
            SchemaString = schemaString,
            PartitionColumns = partitionColumns ?? [],
            Configuration = configurationBuilder,
            CreatedTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
        };

        // IcebergCompat's constraints are cross-cutting (configuration + schema + protocol), so they are
        // checked once against the FINISHED commit-0 actions rather than piecemeal. A violating table must
        // not be created at all: the alternative is a table that looks fine until UniForm tries to convert
        // it, long after data has been written into it.
        Schema.IcebergCompat.Validate(icebergCompatVersion, metadataAction, protocolAction);

        var actions = new List<DeltaAction> { protocolAction, metadataAction };

        if (clusteringColumns is { Count: > 0 })
        {
            actions.Add(BuildClusteringDomain(deltaSchema, clusteringColumns, mappingMode));
        }

        if (previousSnapshot is not null)
        {
            var replacementDomains = new HashSet<string>(
                actions.OfType<DomainMetadata>().Select(static action => action.Domain),
                StringComparer.Ordinal);
            if (rowTrackingEnabled)
                replacementDomains.Add(DeltaLake.RowTracking.RowTrackingConfig.DomainName);

            foreach (string domain in previousSnapshot.DomainMetadata.Keys)
            {
                if (!replacementDomains.Contains(domain))
                {
                    actions.Add(new DomainMetadata
                    {
                        Domain = domain,
                        Configuration = "{}",
                        Removed = true,
                    });
                }
            }
        }

        if (initialBatches is null)
        {
            // The creation commit gets a commitInfo like every other commit, so version 0 is dated and named
            // in history. Build the snapshot from the actions already in memory instead of listing and reading
            // back the commit we just wrote.
            var createActions = Log.InCommitTimestamp.EnsureCommitInfo(
                actions, configurationBuilder, "CREATE TABLE");
            await log.WriteCommitAsync(0, createActions, cancellationToken).ConfigureAwait(false);

            var builder = new SnapshotBuilder();
            builder.ApplyCommit(0, createActions);
            return new DeltaTable(fileSystem, options, builder.Build());
        }

        var provisionalBuilder = previousSnapshot is null
            ? new SnapshotBuilder()
            : SnapshotBuilder.FromSnapshot(previousSnapshot);
        provisionalBuilder.ApplyCommit(latestVersion, actions);
        Snapshot.Snapshot provisionalSnapshot = provisionalBuilder.Build();
        var provisionalTable = new DeltaTable(fileSystem, options, provisionalSnapshot);

        // Same single-attempt cleanup contract as the overwrite family (see WriteCoreAsync): there is no
        // transaction for a host to abort, so an operation that does not reach a committed version takes
        // back the parquet it wrote. Routed through the shared helper rather than hand-rolled, because the
        // ledger's invariant — cleared the instant the commit is durable, so anything left is uncommitted
        // BY CONSTRUCTION — is what makes catching everything safe, and a second copy of that reasoning is
        // a second place for it to drift.
        return await provisionalTable.CollectOnFailureAsync(async written =>
        {
            // Both gates guard a call to ComputeWriteActionsAsync below, which evaluates the
            // constraints against initialBatches.
            if (previousSnapshot is not null)
            {
                provisionalTable.ValidateWritable(
                    previousSnapshot, isAppend: false,
                    handling: WriteTimeExpressionHandling.ValidatedHere);
            }

            provisionalTable.ValidateWritable(
                provisionalSnapshot, isAppend: true,
                handling: WriteTimeExpressionHandling.ValidatedHere);
            var (writeActions, _) = await provisionalTable.ComputeWriteActionsAsync(
                provisionalSnapshot,
                initialBatches,
                DeltaWriteMode.Overwrite,
                overwritePartitions: null,
                dynamicPartitionOverwrite: false,
                repartitionTo: null,
                cancellationToken,
                written: written).ConfigureAwait(false);

            foreach (DeltaAction action in writeActions)
                AddOrReplaceCreateAction(actions, action);

            string operation = previousSnapshot is null
                ? "CREATE TABLE AS SELECT"
                : "CREATE OR REPLACE TABLE AS SELECT";
            var commitActions = Log.InCommitTimestamp.EnsureCommitInfo(
                actions, configurationBuilder, operation);
            long version = latestVersion + 1;

            await log.WriteCommitAsync(version, commitActions, cancellationToken).ConfigureAwait(false);
            written.Clear();

            var committedBuilder = previousSnapshot is null
                ? new SnapshotBuilder()
                : SnapshotBuilder.FromSnapshot(previousSnapshot);
            committedBuilder.ApplyCommit(version, commitActions);
            return new DeltaTable(fileSystem, options, committedBuilder.Build());
        }, cancellationToken).ConfigureAwait(false);
    }

    private static void AddOrReplaceCreateAction(List<DeltaAction> actions, DeltaAction action)
    {
        int existingIndex = action switch
        {
            MetadataAction => actions.FindIndex(static candidate => candidate is MetadataAction),
            ProtocolAction => actions.FindIndex(static candidate => candidate is ProtocolAction),
            DomainMetadata domain => actions.FindIndex(
                candidate => candidate is DomainMetadata existing &&
                    string.Equals(existing.Domain, domain.Domain, StringComparison.Ordinal)),
            _ => -1,
        };

        if (existingIndex >= 0)
            actions[existingIndex] = action;
        else
            actions.Add(action);
    }

    private static ProtocolAction MergeReplacementProtocol(
        ProtocolAction existing, ProtocolAction replacement)
    {
        int minReaderVersion = Math.Max(existing.MinReaderVersion, replacement.MinReaderVersion);
        int minWriterVersion = Math.Max(existing.MinWriterVersion, replacement.MinWriterVersion);

        List<string>? readerFeatures = MergeFeatures(
            existing.ReaderFeatures, replacement.ReaderFeatures);
        List<string>? writerFeatures = MergeFeatures(
            existing.WriterFeatures, replacement.WriterFeatures);

        // Table-features mode is all-or-nothing, and BOTH inputs can arrive below the threshold carrying
        // capabilities only their legacy version implied. Expanding just one side loses the other's: a
        // replacement that asks for column mapping alone is the legacy (2, 5) pair with no feature lists,
        // so replacing a table already at reader 3 / writer 7 used to produce a v7 protocol with NO
        // columnMapping entry while the metadata still said delta.columnMapping.mode=name. That reads as
        // "column mapping not supported" — Spark rejects it with DELTA_FEATURES_PROTOCOL_METADATA_MISMATCH
        // — and this library read it anyway, because it drives mapping off the metadata rather than the
        // protocol. Each side is expanded from its OWN version; a side already at the threshold has
        // nothing implied left to expand and enumerated its features when it was written.
        if (minReaderVersion >= 3)
        {
            foreach (int legacyReaderVersion in
                new[] { existing.MinReaderVersion, replacement.MinReaderVersion })
            {
                if (legacyReaderVersion >= 3)
                    continue;

                readerFeatures ??= [];
                foreach (string feature in LegacyReaderFeatures(legacyReaderVersion))
                {
                    if (!readerFeatures.Contains(feature))
                        readerFeatures.Add(feature);
                }
            }
        }

        if (minWriterVersion >= 7)
        {
            foreach (int legacyWriterVersion in
                new[] { existing.MinWriterVersion, replacement.MinWriterVersion })
            {
                if (legacyWriterVersion >= 7)
                    continue;

                writerFeatures ??= [];
                foreach (string feature in LegacyWriterFeatures(legacyWriterVersion))
                {
                    if (!writerFeatures.Contains(feature))
                        writerFeatures.Add(feature);
                }
            }
        }

        return new ProtocolAction
        {
            MinReaderVersion = minReaderVersion,
            MinWriterVersion = minWriterVersion,
            ReaderFeatures = readerFeatures is { Count: > 0 } ? readerFeatures : null,
            WriterFeatures = writerFeatures is { Count: > 0 } ? writerFeatures : null,
        };
    }

    private static List<string>? MergeFeatures(
        IReadOnlyList<string>? existing, IReadOnlyList<string>? replacement)
    {
        if (existing is null && replacement is null)
            return null;

        var merged = new List<string>();
        if (existing is not null)
        {
            foreach (string feature in existing)
            {
                if (!merged.Contains(feature))
                    merged.Add(feature);
            }
        }
        if (replacement is not null)
        {
            foreach (string feature in replacement)
            {
                if (!merged.Contains(feature))
                    merged.Add(feature);
            }
        }
        return merged;
    }

    private static int GetColumnMappingHighWaterMark(Snapshot.Snapshot snapshot)
    {
        int maxId = ColumnMapping.GetMaxColumnId(snapshot.Schema);
        if (snapshot.Metadata.Configuration is not null &&
            snapshot.Metadata.Configuration.TryGetValue(
                ColumnMapping.MaxColumnIdKey, out string? configured) &&
            int.TryParse(configured, out int configuredMax))
        {
            maxId = Math.Max(maxId, configuredMax);
        }
        return maxId;
    }

    private static void ValidateReplacementColumnMappingIds(StructType schema, int previousMaxId)
    {
        foreach (StructField field in schema.Fields)
            ValidateReplacementColumnMappingIds(field, previousMaxId);
    }

    private static void ValidateReplacementColumnMappingIds(
        StructField field, int previousMaxId)
    {
        if (ColumnMapping.GetFieldId(field) is int id && id <= previousMaxId)
        {
            throw new DeltaFormatException(
                DeltaTableErrorCodes.InvalidPreAssignedSchema,
                $"preAssignedSchema reuses column-mapping id {id}; replacement ids must be greater "
                + $"than the existing table's maxColumnId ({previousMaxId}).");
        }

        switch (field.Type)
        {
            case StructType structure:
                foreach (StructField child in structure.Fields)
                    ValidateReplacementColumnMappingIds(child, previousMaxId);
                break;
            case ArrayType array when array.ElementType is StructType element:
                foreach (StructField child in element.Fields)
                    ValidateReplacementColumnMappingIds(child, previousMaxId);
                break;
            case MapType map:
                if (map.KeyType is StructType key)
                {
                    foreach (StructField child in key.Fields)
                        ValidateReplacementColumnMappingIds(child, previousMaxId);
                }
                if (map.ValueType is StructType value)
                {
                    foreach (StructField child in value.Fields)
                        ValidateReplacementColumnMappingIds(child, previousMaxId);
                }
                break;
        }
    }

    /// <summary>
    /// Opens an existing Delta table, or creates a new one if it doesn't exist.
    /// </summary>
    /// <remarks>
    /// <paramref name="columnMappingMode"/>, <paramref name="configuration"/> and
    /// <paramref name="preAssignedSchema"/> apply only on the CREATE path — an existing table keeps the mode
    /// and properties it was created with, because changing either is a metadata commit (see
    /// <see cref="SetSchemaAsync"/>), not something an open should do silently.
    /// </remarks>
    /// <param name="preAssignedSchema">See <see cref="CreateAsync"/>. Ignored when the table already exists —
    /// which is the case this overload exists for, so a host retrying a CTAS after a crash reopens the table
    /// its earlier attempt created rather than failing.</param>
    public static async ValueTask<DeltaTable> OpenOrCreateAsync(
        ITableFileSystem fileSystem,
        Apache.Arrow.Schema schema,
        DeltaTableOptions? options = null,
        IReadOnlyList<string>? partitionColumns = null,
        IReadOnlyList<string>? clusteringColumns = null,
        ColumnMappingMode columnMappingMode = ColumnMappingMode.None,
        IReadOnlyDictionary<string, string>? configuration = null,
        CancellationToken cancellationToken = default,
        Schema.StructType? preAssignedSchema = null)
    {
        options ??= DeltaTableOptions.Default;
        var log = new TransactionLog(fileSystem);

        long latestVersion = await log.GetLatestVersionAsync(cancellationToken)
            .ConfigureAwait(false);

        if (latestVersion >= 0)
            return await OpenAsync(fileSystem, options, cancellationToken)
                .ConfigureAwait(false);

        return await CreateAsync(fileSystem, schema, options, partitionColumns,
            columnMappingMode: columnMappingMode,
            clusteringColumns: clusteringColumns,
            configuration: configuration,
            cancellationToken: cancellationToken,
            preAssignedSchema: preAssignedSchema).ConfigureAwait(false);
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

    /// <summary>Converts one incoming Arrow field to its Delta field, through a one-field schema so the
    /// conversion is the same type mapping a whole schema gets.</summary>
    private static StructField ToDeltaField(Field arrowField) =>
        SchemaConverter.FromArrowSchema(new Apache.Arrow.Schema([arrowField], null)).Fields[0];

    /// <summary>
    /// Schema evolution — appends a nullable column. Writes a metadata-only commit (a new
    /// <see cref="MetadataAction"/> whose schema = the current schema ++ <paramref name="newColumn"/>); NO data
    /// files are rewritten. Old files lack the column, so the read path backfills it as all-NULL. The column
    /// must be nullable (existing rows have no value for it). On a column-mapping table the new field is
    /// assigned a fresh column id (maxColumnId + 1) and physical name — recursively, so a struct/array/map
    /// column arrives with ids on every descendant — and <c>delta.columnMapping.maxColumnId</c> is bumped.
    /// Returns the new version.
    /// </summary>
    public ValueTask<long> AddColumnAsync(
        Field newColumn, CancellationToken cancellationToken = default) =>
        AddColumnAsync(ToDeltaField(newColumn), cancellationToken);

    /// <summary>
    /// <see cref="AddColumnAsync(Field, CancellationToken)"/> taking the DELTA field directly, for a column
    /// whose Delta type the Arrow conversion cannot express or would reshape. The motivating case is
    /// <c>variant</c>: a host whose Arrow boundary carries variants in some transport form declares a binary
    /// column, which would be added to the table as Delta <c>binary</c> — this overload lets it say
    /// <c>variant</c> and mean it. The Delta-typed counterpart of <see cref="CreateAsync"/>'
    /// <c>preAssignedSchema</c>.
    /// <para>The caller owns the field's correctness. Do NOT pre-assign column-mapping metadata on it: ids and
    /// physical names are assigned here, recursively, continuing past the table's <c>maxColumnId</c>.</para>
    /// </summary>
    public async ValueTask<long> AddColumnAsync(
        StructField newColumn, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ProtocolVersions.ValidateWriteSupport(CurrentSnapshot.Protocol);

        if (!newColumn.Nullable)
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

        var newDeltaField = newColumn;

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
        Field newColumn, MetadataAction? baseMetadata = null, ProtocolAction? baseProtocol = null) =>
        ComputeAddColumn(ToDeltaField(newColumn), baseMetadata, baseProtocol);

    /// <summary>
    /// <see cref="ComputeAddColumn(Field, MetadataAction, ProtocolAction)"/> taking the DELTA field directly —
    /// see <see cref="AddColumnAsync(StructField, CancellationToken)"/> for when that matters. Stage the
    /// result on a <see cref="DeltaTransaction"/> with <see cref="DeltaTransaction.StageSchemaChange"/>.
    /// </summary>
    public DeferredSchemaChange ComputeAddColumn(
        StructField newColumn, MetadataAction? baseMetadata = null, ProtocolAction? baseProtocol = null)
    {
        ThrowIfDisposed();
        ProtocolVersions.ValidateWriteSupport(CurrentSnapshot.Protocol);

        if (!newColumn.Nullable)
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

        var newDeltaField = newColumn;

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

        await CheckpointIfDueAsync(newVersion, cancellationToken).ConfigureAwait(false);

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
                    DeltaTableErrorCodes.ClusteringWithPartitioning,
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

        await CheckpointIfDueAsync(newVersion, cancellationToken).ConfigureAwait(false);
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
                    DeltaTableErrorCodes.ColumnNotFound,
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

        await CheckpointIfDueAsync(newVersion, cancellationToken).ConfigureAwait(false);

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

        await CheckpointIfDueAsync(newVersion, cancellationToken).ConfigureAwait(false);

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
                DeltaTableErrorCodes.NoCommitAtTimestamp,
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
    /// Begins a transaction based on a version the host pinned EARLIER — what its row addresses, deletion-vector
    /// positions and scan decisions were captured against — rather than on whatever is current now.
    ///
    /// <para><b>Why this exists.</b> For a transaction spanning several of the host's own statements,
    /// <see cref="StartTransaction(IsolationLevel)"/> makes the commit loop's validation VACUOUS: it asks "what
    /// landed since the latest version?", and the answer is nothing. Basing on the version the work was actually
    /// planned against is what makes the check mean something — a concurrent commit between the host's first
    /// statement and its commit is then seen and adjudicated instead of silently ignored.</para>
    ///
    /// <para>A version number is what a host that cannot keep the table open between statements can carry
    /// across its own statement boundary; <see cref="StartTransaction(Snapshot.Snapshot, IsolationLevel)"/> is
    /// for a caller already holding the snapshot itself.</para>
    /// </summary>
    /// <param name="baseVersion">The pinned version. Must exist, and must not be ahead of the current one.</param>
    public async ValueTask<DeltaTransaction> StartTransactionAsync(
        long baseVersion,
        IsolationLevel isolationLevel = IsolationLevel.WriteSerializable,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        if (baseVersion > CurrentSnapshot.Version)
        {
            throw new ArgumentOutOfRangeException(
                nameof(baseVersion),
                $"Cannot base a transaction on version {baseVersion}: the table is at "
                + $"{CurrentSnapshot.Version}, so that version does not exist yet.");
        }

        var baseSnapshot = baseVersion == CurrentSnapshot.Version
            ? CurrentSnapshot
            : await GetSnapshotAtVersionAsync(baseVersion, cancellationToken).ConfigureAwait(false);
        return new DeltaTransaction(this, baseSnapshot, isolationLevel);
    }

    /// <summary>
    /// Begins a transaction on a snapshot the caller already holds — another transaction's
    /// <see cref="DeltaTransaction.Snapshot"/>, or one from <see cref="GetSnapshotAtVersionAsync"/>. The
    /// snapshot form of <see cref="StartTransactionAsync"/>, with the same purpose and no I/O.
    /// </summary>
    /// <param name="baseSnapshot">The version to base on. Must be a snapshot OF THIS TABLE — one from another
    /// table would silently key every ordinal, path and row-id range to the wrong file set — and must not be
    /// ahead of the current version.</param>
    public DeltaTransaction StartTransaction(
        Snapshot.Snapshot baseSnapshot,
        IsolationLevel isolationLevel = IsolationLevel.WriteSerializable)
    {
        ThrowIfDisposed();
        if (baseSnapshot is null)
            throw new ArgumentNullException(nameof(baseSnapshot));
        RequireSnapshotOfThisTable(baseSnapshot, nameof(baseSnapshot));
        if (baseSnapshot.Version > CurrentSnapshot.Version)
        {
            throw new ArgumentOutOfRangeException(
                nameof(baseSnapshot),
                $"Cannot base a transaction on version {baseSnapshot.Version}: the table is at "
                + $"{CurrentSnapshot.Version}, so that version does not exist yet.");
        }
        return new DeltaTransaction(this, baseSnapshot, isolationLevel);
    }

    /// <summary>
    /// Rejects a snapshot that belongs to a different table. The Delta table id is in every version's
    /// <c>metaData</c> and never changes, so this is exact — and the failure it prevents is silent: another
    /// table's snapshot has its own active set, so every file ordinal, path and row-id range computed from it
    /// would address the wrong thing without anything looking wrong.
    /// </summary>
    private void RequireSnapshotOfThisTable(Snapshot.Snapshot snapshot, string paramName)
    {
        string mine = CurrentSnapshot.Metadata.Id;
        string theirs = snapshot.Metadata.Id;
        if (!string.Equals(mine, theirs, StringComparison.Ordinal))
        {
            throw new ArgumentException(
                $"The snapshot belongs to Delta table '{theirs}', but this is table '{mine}'. Its active "
                + "file set is a different one, so every path, ordinal and row-id range derived from it "
                + "would address the wrong file.",
                paramName);
        }
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
        var reads = new ReadSet
        {
            // The files this transaction's own DML rewrites, plus the ones the HOST declared its scan read
            // (DeclareFilesRead). A NEW set when there are declared paths — never transaction.RemovedPaths
            // itself, which is ALSO passed below as plannedRemovePaths and drives the delete/delete check.
            // Adding a merely-read file to that object would make a concurrent delete of it report as
            // ConcurrentDeleteDelete ("this transaction also removes it") for a file this transaction never
            // removes, instead of the ConcurrentDeleteRead it is.
            Files = UnionReadFiles(transaction),
            Predicates = transaction.ReadPredicates,
            // What the HOST declared it read (DeclareWholeTableRead), which the loop cannot infer — it never
            // saw the scan. Honoured at both isolation levels; see the method's own remarks for the proposal
            // that would narrow it and why it is not implemented.
            WholeTable = transaction.DeclaredWholeTableRead,
        };

        // The row-tracking high-water mark is emitted ONCE for the whole transaction, from the counter each
        // staged operation advanced. Per-operation marks are held back at staging time: several of them in one
        // version is malformed, and the last one written would win regardless of which reserved the most.
        var actions = transaction.DataActions;
        var required = transaction.AppTransactions;
        if ((transaction.NextRowId is { } nextRowId && nextRowId > baseSnapshot.RowIdHighWaterMark)
            || required.Count > 0)
        {
            var extended = new List<DeltaAction>(actions.Count + 1 + required.Count);
            extended.AddRange(actions);
            if (transaction.NextRowId is { } mark && mark > baseSnapshot.RowIdHighWaterMark)
                extended.Add(DeltaLake.RowTracking.RowTrackingConfig.BuildHighWaterMarkAction(mark));
            // The txn actions are emitted HERE rather than when required, so the precondition and the action
            // it guards come from one source and cannot drift apart.
            long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            foreach (var r in required)
                extended.Add(new TransactionId { AppId = r.AppId, Version = r.Version, LastUpdated = now });
            actions = extended;
        }

        return CommitOccAsync(
            baseSnapshot, actions, reads, transaction.RemovedPaths,
            transaction.IsolationLevel, transaction.EffectiveOperation, rebaseSafe: true,
            cancellationToken,
            rowLevelDeletes: transaction.DvEdits,
            appTransactions: required,
            // Keep recording through the commit: a rebase attempt writes deletion vectors of its own, and an
            // attempt that then loses the conflict check leaves them behind exactly as a staging call would.
            written: transaction.Written,
            // The host's claim, passed through verbatim. NOT derived from `reads` above: a transaction
            // records the reads made THROUGH it, and a host that scanned the table itself and staged the
            // result made one this library never saw.
            isBlindAppend: transaction.EffectiveIsBlindAppend);
    }

    /// <summary>
    /// Deletes the files a transaction wrote and did not commit, then empties the ledger. The physical half of
    /// <see cref="DeltaTransaction.AbortAsync"/>.
    ///
    /// <para><b>Best-effort by construction.</b> A failed delete is swallowed: this runs on the failure and
    /// disposal paths, where an exception would replace the caller's REAL error with a cleanup error, and where
    /// leaving an orphan behind is merely the status quo — vacuum still collects it. The ledger is emptied
    /// either way, so a second abort (or a dispose after one) does not re-attempt.</para>
    ///
    /// <para>Deliberately does NOT <see cref="ThrowIfDisposed"/>: a host may dispose the table before the
    /// transaction, and refusing to clean up then would strand exactly the files this exists to collect. The
    /// filesystem handle outlives the disposed flag.</para>
    ///
    /// <para>An ALREADY-cancelled <paramref name="cancellationToken"/> does not skip the cleanup. Every caller
    /// arrives here from a failure path and can naturally be holding the token that just failed — and since a
    /// cancelled token makes each delete throw, which is then swallowed, honouring it would collect nothing
    /// while reporting success. A token cancelled while the loop RUNS does still stop the rest: that is a live
    /// instruction rather than a stale one.</para>
    /// </summary>
    /// <summary>
    /// Runs an AUTO-COMMITTING operation — one with no <see cref="DeltaTransaction"/> for a host to abort —
    /// and deletes the files it wrote if it does not reach a committed version. The auto-committing
    /// counterpart of <see cref="DeltaTransaction.AbortAsync"/>, and the reason a conflicting
    /// <c>UpdateAsync</c> or <c>CompactAsync</c> no longer leaves its output behind for VACUUM.
    ///
    /// <para>The ledger is emptied the instant a commit becomes durable (see
    /// <see cref="CommitOccAsync"/>), so whatever is still in it when the operation throws is uncommitted BY
    /// CONSTRUCTION — including when the throw came from post-commit work rather than from the commit. That
    /// is what makes catching everything safe here: this can never delete a file a committed <c>add</c>
    /// references.</para>
    ///
    /// <para>Covers the whole operation, not just its commit: a rewrite that fails half way through its files
    /// has written some of them, and those are as orphaned as a full set the commit refused.</para>
    /// </summary>
    private async ValueTask<T> CollectOnFailureAsync<T>(
        Func<WrittenFileLedger, ValueTask<T>> operation, CancellationToken cancellationToken)
    {
        var written = new WrittenFileLedger();
        try
        {
            return await operation(written).ConfigureAwait(false);
        }
        catch
        {
            await DeleteWrittenFilesAsync(written, cancellationToken).ConfigureAwait(false);
            throw;
        }
    }

    internal async ValueTask DeleteWrittenFilesAsync(
        WrittenFileLedger written, CancellationToken cancellationToken)
    {
        var token = cancellationToken.IsCancellationRequested ? CancellationToken.None : cancellationToken;

        foreach (string path in written.Paths)
        {
            try
            {
                await _fs.DeleteAsync(path, token).ConfigureAwait(false);
            }
            catch (Exception)
            {
                // Orphans are the status quo; losing the exception that got us here would not be.
            }
        }

        written.Clear();
    }

    /// <summary>
    /// Re-validates the transaction's app-transaction preconditions against the base version plus everything
    /// that landed since. Run once before the first attempt and again before every retry — a precondition is
    /// a fact about the table, not staged output, so it has to be re-asked each time the table moves.
    ///
    /// <para>Throws <see cref="AppTransactionPreconditionException"/> — an
    /// <see cref="InvalidOperationException"/>, deliberately NOT a
    /// <see cref="DeltaConflictException"/>: the commit loop retries the latter, and no amount of retrying
    /// makes an already-committed batch un-commit. A producer told "conflict" would keep trying to write a
    /// batch the table already holds.</para>
    ///
    /// <para>Each requirement is judged on its own and the first failure aborts the whole commit. That is the
    /// only available answer when a transaction names several appIds whose preconditions disagree: a commit is
    /// atomic, so it cannot apply the ones that hold — and committing anyway would REGRESS the recorded
    /// version of an appId that had already advanced, since a snapshot reconciles <c>txn</c> actions
    /// last-wins rather than by maximum.</para>
    /// </summary>
    private static void ValidateAppTransactions(
        IReadOnlyList<DeltaTransaction.AppTransactionRequirement> required,
        Snapshot.Snapshot baseSnapshot,
        IReadOnlyList<(long Version, IReadOnlyList<DeltaAction> Actions)>? concurrent)
    {
        foreach (var r in required)
        {
            if (r.Precondition.Kind == AppTransactionPreconditionKind.None)
                continue; // no precondition — write unconditionally

            // The base version's record, overridden by any concurrent commit that moved it. Reading the
            // concurrent commits directly avoids materializing a whole snapshot just to answer this.
            long? current = baseSnapshot.AppTransactions.TryGetValue(r.AppId, out var recorded)
                ? recorded.Version
                : null;
            if (concurrent is not null)
            {
                foreach (var (_, actions) in concurrent)
                {
                    foreach (var action in actions)
                    {
                        if (action is TransactionId txn
                            && string.Equals(txn.AppId, r.AppId, StringComparison.Ordinal))
                        {
                            current = txn.Version;
                        }
                    }
                }
            }

            // One rule per kind, defined on the precondition itself so this check and the pre-commit
            // DeltaTransaction.IsAppTransactionApplied cannot answer differently.
            if (!r.Precondition.Holds(current, r.Version))
            {
                throw new AppTransactionPreconditionException(
                    $"App transaction precondition failed for '{r.AppId}': expected the table to record "
                    + r.Precondition.Describe()
                    + ", but it records "
                    + (current is { } c ? c.ToString() : "no transaction at all")
                    + $". Version {r.Version} was NOT committed. This is not a conflict to retry — retrying "
                    + "cannot make an already-committed batch un-commit; re-read the recorded version and "
                    + "decide whether this batch still needs writing.",
                    r.AppId, r.Version, r.Precondition, current);
            }
        }
    }

    /// <summary>
    /// The read-set's file half: what the transaction rewrites, unioned with what its host declared it read.
    /// Returns the transaction's own removed-path set unchanged when nothing was declared — the common case,
    /// and one copy fewer — and a fresh set otherwise, because that object is also the commit loop's
    /// <c>plannedRemovePaths</c> and must keep meaning ONLY what this transaction removes.
    /// </summary>
    private static ISet<string> UnionReadFiles(DeltaTransaction transaction)
    {
        if (transaction.DeclaredReadPaths.Count == 0)
            return transaction.RemovedPaths;

        var union = new HashSet<string>(transaction.RemovedPaths, StringComparer.Ordinal);
        union.UnionWith(transaction.DeclaredReadPaths);
        return union;
    }

    /// <summary>Shared by blind-append commits, which plan no removes.</summary>
    private static readonly HashSet<string> NoRemovedPaths = new(StringComparer.Ordinal);

    /// <summary>
    /// The optimistic-concurrency commit loop shared by the transactional path, the auto-committing
    /// <see cref="DeleteAsync"/>, and single-shot appends. Attempts the commit at the version after
    /// <paramref name="baseSnapshot"/>; on a collision it reads the intervening commits, runs the
    /// <see cref="ConflictChecker"/> against <paramref name="reads"/> /
    /// <paramref name="plannedRemovePaths"/>, and either aborts (a real conflict) or — when
    /// <paramref name="rebaseSafe"/> — rebases onto the latest version and retries. A no-conflict rebase
    /// re-commits the staged actions verbatim, valid precisely because nothing the commit read or removed
    /// was touched.
    ///
    /// <para><paramref name="rebaseSafe"/> is <c>false</c> when the staged actions embed the attempted
    /// version — row tracking's <c>baseRowId</c> / <c>defaultRowCommitVersion</c> would be wrong after a
    /// rebase — so such a commit succeeds only uncontended and otherwise aborts rather than corrupt.</para>
    ///
    /// <para>The loop itself is <see cref="LogCommitter"/>, in the log layer. What stays here is what only
    /// the table can supply: the two rebases that re-derive version-coupled actions
    /// (<see cref="OccRebaseHandler"/>), the app-transaction preconditions, and the ledger of files a losing
    /// attempt leaves behind.</para>
    /// </summary>
    internal async ValueTask<long> CommitOccAsync(
        Snapshot.Snapshot baseSnapshot,
        IReadOnlyList<DeltaAction> dataActions,
        ReadSet reads,
        ISet<string> plannedRemovePaths,
        IsolationLevel isolationLevel,
        string operation,
        bool rebaseSafe,
        CancellationToken cancellationToken,
        IReadOnlyList<DeleteDvEdit>? rowLevelDeletes = null,
        IReadOnlyList<DeltaTransaction.AppTransactionRequirement>? appTransactions = null,
        WrittenFileLedger? written = null,
        bool? isBlindAppend = null)
    {
        ThrowIfDisposed();

        bool rowLevel = rowLevelDeletes is { Count: > 0 };
        bool rowTrackingEnabled = DeltaLake.RowTracking.RowTrackingConfig.IsEnabled(
            baseSnapshot.Metadata.Configuration);

        var result = await _committer.CommitAsync(
            new LogCommitRequest
            {
                BaseSnapshot = baseSnapshot,
                Actions = dataActions,
                Reads = reads,
                PlannedRemovePaths = plannedRemovePaths,
                Isolation = isolationLevel,
                Operation = operation,
                RebaseSafe = rebaseSafe,
                // Only the version-coupled commits need one. A plain append or a metadata change means the
                // same thing at whatever version it lands on, so it is re-committed verbatim.
                Rebase = rowLevel || rowTrackingEnabled
                    ? new OccRebaseHandler(this, rowLevelDeletes, rowTrackingEnabled, written)
                    : null,
                Precondition = appTransactions is { Count: > 0 }
                    ? (snapshot, concurrent) =>
                        ValidateAppTransactions(appTransactions, snapshot, concurrent)
                    : null,
                OnCommitDurable = written is null ? null : written.Clear,
                IsBlindAppend = isBlindAppend,
                // Checkpoint here too. This line used to be `false`, with a comment calling it "an
                // accident of where the call happened to sit rather than a decision" — see #86.
                //
                // This loop carries the transaction commit, both DELETE paths (deletion-vector and
                // copy-on-write), UpdateAsync, UpdateRowsAsync, and the blind append. Opting out here meant
                // a table written through anything but a plain batch append never got a checkpoint, and so
                // never published a `_last_checkpoint` either. Three consequences, compounding: every open
                // replays the log from v0; foreign readers get no resume hint; and commits accumulate
                // without bound, because log cleanup is defined in terms of what a checkpoint subsumes, so
                // with no checkpoint nothing can ever be reclaimed.
                //
                // OPTIMIZE and the metadata-only changes do NOT come through here — they commit through
                // TransactionLog directly and get the same interval check from CheckpointIfDueAsync.
                //
                // No new mechanism — the condition (interval reached, writer present) and the ordering
                // (after the post-commit snapshot refresh, from the refreshed snapshot) are LogCommitter's
                // own, identical to the batch write path that already sets this. LogCommitRequest's default
                // is already true; this stops opting out.
                WriteCheckpointOnInterval = true,
                // Incremental: this handle's snapshot is usually newer than the transaction's base, so
                // refreshing from it replays fewer versions for the same result.
                RefreshFrom = CurrentSnapshot,
            },
            cancellationToken).ConfigureAwait(false);

        // Only on a real commit. An empty action list is a no-op that returns the BASE version, and
        // assigning its snapshot would walk this handle backwards to a version it has already moved past.
        if (result.Committed)
            _currentSnapshot = result.Snapshot;
        return result.Version;
    }

    /// <summary>
    /// The table's half of the commit loop: re-derives the actions whose CONTENT is coupled to the version
    /// they land at, on each collision, before the conflict checker delivers its verdict.
    ///
    /// <para>Two mechanisms, and a commit may need either or both:</para>
    /// <list type="number">
    /// <item><b>Row-level delete resolution</b> — rebase each staged delete's deletion vector onto the
    /// file's current one (union the rows), or remap its rows by stable id onto the file a concurrent
    /// rewrite produced. The paths it reconciles come back as
    /// <see cref="CommitRebase.RowLevelResolvedPaths"/>, which is what stops the checker from judging them
    /// again at file granularity.</item>
    /// <item><b>Row-tracking id rebase</b> — re-derive post-image <c>baseRowId</c>s against the advanced
    /// high-water mark, because a concurrent commit may have consumed the id range this one reserved.</item>
    /// </list>
    ///
    /// <para>Stateful across retries by necessity: <see cref="_priorResolvedPaths"/> carries the files the
    /// PREVIOUS resolution re-touched, which is the only way the next pass recognises a remap's vectors —
    /// written onto a concurrent rewrite's output — as this operation's own.</para>
    /// </summary>
    private sealed class OccRebaseHandler : ICommitRebaseHandler
    {
        private readonly DeltaTable _table;
        private readonly IReadOnlyList<DeleteDvEdit>? _rowLevelDeletes;
        private readonly bool _rowTrackingEnabled;
        private readonly WrittenFileLedger? _written;
        private ISet<string>? _priorResolvedPaths;

        public OccRebaseHandler(
            DeltaTable table,
            IReadOnlyList<DeleteDvEdit>? rowLevelDeletes,
            bool rowTrackingEnabled,
            WrittenFileLedger? written)
        {
            _table = table;
            _rowLevelDeletes = rowLevelDeletes;
            _rowTrackingEnabled = rowTrackingEnabled;
            _written = written;
        }

        private bool RowLevel => _rowLevelDeletes is { Count: > 0 };

        /// <summary>
        /// Both mechanisms need the newest table STATE, not just its version: the union reads the
        /// concurrent files' current deletion vectors, and the id rebase reads the advanced high-water mark.
        /// </summary>
        public bool NeedsLatestSnapshot => true;

        public async ValueTask<CommitRebase> RebaseAsync(
            CommitRebaseContext context, CancellationToken cancellationToken)
        {
            // Always from the ORIGINAL staged actions, never from a prior rebase, so each retry rebases the
            // stable staged work onto whatever the newest snapshot holds.
            var actions = context.StagedActions;
            ISet<string>? resolvedPaths = null;

            if (RowLevel)
            {
                // The vectors the LAST attempt named for the files this delete touches are about to be
                // replaced by the resolution below, so this is the moment they become garbage — and the only
                // moment anything knows it. Collect them now; a commit that eventually SUCCEEDS clears the
                // ledger wholesale and would otherwise forget every losing attempt's.
                await _table.CollectSupersededVectorsAsync(
                    context.AttemptedActions, _rowLevelDeletes!, _priorResolvedPaths, _written,
                    cancellationToken).ConfigureAwait(false);

                // A null result is a genuine conflict — the same row was deleted concurrently, or the file
                // was rewritten away such that a row cannot be remapped.
                var resolution = await _table.ResolveRowLevelDeletesAsync(
                    context.BaseSnapshot, context.LatestSnapshot!, context.StagedActions,
                    _rowLevelDeletes!, cancellationToken, _written).ConfigureAwait(false);
                if (resolution is null)
                    throw new DeltaConflictException(
                        DeltaTableErrorCodes.RowLevelConflict,
                        "A concurrent commit deleted a row this delete also removed, or rewrote a file "
                        + "it targeted such that a row cannot be remapped; the delete conflicts at row "
                        + "level and must be retried.");

                actions = resolution.Value.Actions;
                resolvedPaths = resolution.Value.ResolvedPaths;
                // Carried to the NEXT retry: a remap writes vectors on files that are not in this delete's
                // own edit list (the concurrent rewrite's output), so without this the next supersede pass
                // would not recognise them as ours. Recorded BEFORE the isolation-level narrowing below —
                // this is about provenance, not about what the checker is allowed to forgive.
                _priorResolvedPaths = resolvedPaths;

                // Under Serializable, commit order IS the logical order: a concurrent commit that CHANGED
                // DATA in a file this delete read may not be reconciled away, however disjoint the rows —
                // that is the interleaving the stricter level exists to forbid, and the level's own rule
                // (IsolationLevel) is that a dataChange=true remove of a file we read conflicts at BOTH
                // levels. So the reconciliation survives here only where the concurrent commit did not
                // change data: a compaction's dataChange=false rewrite rearranges bytes without changing
                // which rows the table contains, so remapping our rows onto the new file admits no
                // interleaving the level forbids. Dropping a path from the resolved set does not force a
                // conflict — it just restores the normal checks for it, so a delete whose files nobody
                // touched still rebases and lands.
                if (context.Isolation == IsolationLevel.Serializable)
                    resolvedPaths = KeepOnlyDataPreservingResolutions(resolvedPaths, context.Concurrent);
            }

            // Re-derive row-tracking post-image ids against the snapshot we now land on (a concurrent commit
            // may have consumed row-id space). No-op for the row-level delete's own re-adds — they keep
            // their existing baseRowId (excluded by resolvedPaths / base-active membership).
            if (_rowTrackingEnabled)
            {
                actions = RebaseRowTrackingAddIds(
                    actions, context.BaseSnapshot, context.LatestSnapshot!,
                    context.NextAttemptVersion, resolvedPaths);
            }

            return new CommitRebase(actions, resolvedPaths);
        }
    }

    /// <summary>
    /// Deletes the deletion vectors a rebase is about to make garbage: the ones
    /// <paramref name="supersededActions"/> names for the files this delete touched, which the resolution
    /// that follows replaces with vectors computed against the newer snapshot.
    ///
    /// <para>Without this they leak on SUCCESS specifically. A losing attempt's vectors stay in the ledger,
    /// and the commit that eventually lands empties it wholesale — correctly, to protect the winning
    /// attempt's files, and in doing so it forgets every earlier attempt's. (A run that ultimately FAILS
    /// needs none of this: the ledger still holds them and the abort collects them.)</para>
    ///
    /// <para><b>Why this is the delicate one.</b> Every other cleanup in the library deletes by provenance —
    /// what its own writers just created. This one reads paths out of ACTIONS, and it does so standing next to
    /// vectors that are emphatically live: the union arm builds its replacement from
    /// <c>latestAdd.DeletionVector</c>, the CONCURRENT writer's committed vector, and a remap's targets are
    /// files someone else just wrote. Two filters keep it honest, and both are necessary:</para>
    /// <list type="number">
    /// <item>only adds whose PATH this operation reconciled — its own <see cref="DeleteDvEdit"/> paths, plus
    /// the previous resolution's <paramref name="priorResolvedPaths"/> for the remap arm. This is what keeps a
    /// staged append's born-deleted vector out: that file is not in the base snapshot and not a remap target,
    /// its add survives the resolution untouched, and the commit still needs it.</item>
    /// <item>only paths the LEDGER still holds. The ledger names what this operation's own writers created and
    /// is emptied the instant a commit becomes durable, so surviving that filter proves a vector is both ours
    /// and uncommitted — including in the case where the commit landed and threw afterwards, which reaches
    /// this code with an empty ledger and therefore deletes nothing.</item>
    /// </list>
    /// </summary>
    private async ValueTask CollectSupersededVectorsAsync(
        IReadOnlyList<DeltaAction> supersededActions,
        IReadOnlyList<DeleteDvEdit> dvEdits,
        ISet<string>? priorResolvedPaths,
        WrittenFileLedger? written,
        CancellationToken cancellationToken)
    {
        if (written is null)
            return;

        var reconciled = new HashSet<string>(StringComparer.Ordinal);
        foreach (var edit in dvEdits)
            reconciled.Add(edit.Path);
        if (priorResolvedPaths is not null)
            reconciled.UnionWith(priorResolvedPaths);

        var candidates = new List<string>();
        foreach (var action in supersededActions)
        {
            if (action is AddFile add
                && reconciled.Contains(add.Path)
                && add.DeletionVector is { } dv
                && DeletionVectors.DeletionVectorPath.GetRelativePath(dv) is { } path)
            {
                candidates.Add(path);
            }
        }

        if (candidates.Count == 0)
            return;

        // TakeRecorded applies filter (2) and hands back only what was ours-and-uncommitted; routing the
        // result through the shared deleter keeps the best-effort and cancelled-token semantics identical to
        // every other cleanup path.
        var superseded = new WrittenFileLedger();
        foreach (string path in written.TakeRecorded(candidates))
            superseded.Record(path);
        await DeleteWrittenFilesAsync(superseded, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Narrows a row-level resolution to what <see cref="IsolationLevel.Serializable"/> admits: the paths no
    /// concurrent commit removed with <c>dataChange=true</c>.
    ///
    /// <para>Row-level reconciliation has two mechanisms, and the isolation level bounds them differently.
    /// The DV union reconciles a concurrent DELETE — a <c>dataChange=true</c> remove/re-add of a file this
    /// transaction read — which is a conflict at both levels, so under Serializable it must not be silenced.
    /// The remap across a rewrite reconciles a COMPACTION, whose removes and adds carry
    /// <c>dataChange=false</c>: contents unchanged, only rearranged, which the conflict checker already
    /// exempts from read conflicts at both levels. Relocating rows across it admits no non-serializable
    /// interleaving, so it survives here. A copy-on-write UPDATE's rewrite is a <c>dataChange=true</c> remove
    /// and is dropped with the rest.</para>
    ///
    /// <para>Gating the resolution as a whole (one bool for both mechanisms) would abort the compaction case
    /// too, which no reading of the level requires.</para>
    /// </summary>
    private static ISet<string> KeepOnlyDataPreservingResolutions(
        ISet<string> resolvedPaths,
        IReadOnlyList<(long Version, IReadOnlyList<DeltaAction> Actions)> concurrent)
    {
        var kept = new HashSet<string>(resolvedPaths, StringComparer.Ordinal);
        foreach (var (_, actions) in concurrent)
        {
            foreach (var action in actions)
            {
                if (action is RemoveFile { DataChange: true } remove)
                    kept.Remove(remove.Path);
            }
        }

        return kept;
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
    /// <param name="written">Records the rebased vectors this resolution writes. A rebase happens INSIDE the
    /// commit loop, so an attempt that then loses the conflict check has left its own <c>.bin</c> files behind —
    /// as much this transaction's orphans as anything it wrote at staging time.</param>
    private async ValueTask<(List<DeltaAction> Actions, ISet<string> ResolvedPaths)?>
        ResolveRowLevelDeletesAsync(
            Snapshot.Snapshot baseSnapshot,
            Snapshot.Snapshot latestSnapshot,
            IReadOnlyList<DeltaAction> originalActions,
            IReadOnlyList<DeleteDvEdit> dvEdits,
            CancellationToken cancellationToken,
            WrittenFileLedger? written)
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
            written?.RecordDeletionVector(unionDv);

            result.Add(new RemoveFile
            {
                Path = latestAdd.Path,
                DeletionTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                DataChange = true,
                DeletionVector = latestAdd.DeletionVector,
                // A remove carries the row-tracking fields of the add it removes (spec; measured on Spark
                // 4.0.1, which populates both on every remove of a row-tracking table). They are what lets a
                // reader resolve the identity of rows that are only visible through the removed file — the
                // Change Data Feed's inferred "delete" rows, whose source carries no materialized column.
                BaseRowId = latestAdd.BaseRowId,
                DefaultRowCommitVersion = latestAdd.DefaultRowCommitVersion,
            });

            result.Add(latestAdd with
            {
                DeletionVector = unionDv,
                DataChange = true,
                Stats = StatsWithLooseBounds(latestAdd.GetStatsJson()),
            });
        }

        if (remapEdits.Count > 0)
        {
            var remapped = await RemapRowLevelDeletesAsync(
                baseSnapshot, latestSnapshot, remapEdits, resolvedPaths, cancellationToken, written)
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
        CancellationToken cancellationToken,
        WrittenFileLedger? written)
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
            written?.RecordDeletionVector(newDv);

            result.Add(new RemoveFile
            {
                Path = cand.Path,
                DeletionTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                DataChange = true,
                DeletionVector = cand.DeletionVector,
                BaseRowId = cand.BaseRowId,
                DefaultRowCommitVersion = cand.DefaultRowCommitVersion,
            });
            result.Add(cand with
            {
                DeletionVector = newDv,
                DataChange = true,
                Stats = StatsWithLooseBounds(cand.GetStatsJson()),
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
    /// conflict resolution.
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
                    nextRowId += add.GetNumRecords() ?? 0;
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
        //
        // `await using`: a delete that aborts — a real conflict, an exhausted retry loop — has already written
        // its deletion vectors and change files, and disposal takes them back. A successful commit disposes to
        // a no-op.
        await using var transaction = StartTransaction();
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
        await using var transaction = StartTransaction(); // aborts (and cleans up) on any failure — see above
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
    /// <param name="written">Records the deletion-vector and change-data files this delete creates — never the
    /// data file its re-add names, which is LIVE table data this delete only masks rows in. See
    /// <see cref="WrittenFileLedger"/>.</param>
    internal async ValueTask<DeleteActions> ComputeDeleteActionsAsync(
        Snapshot.Snapshot snapshot,
        Func<RecordBatch, BooleanArray> predicate,
        CancellationToken cancellationToken,
        Expressions.Predicate? prunePredicate = null,
        WrittenFileLedger? written = null)
    {
        var dvWriter = new DeletionVectors.DeletionVectorWriter(_fs);
        var actions = new List<DeltaAction>();
        var removedPaths = new HashSet<string>(StringComparer.Ordinal);
        var dvEdits = new List<DeleteDvEdit>();
        long totalDeleted = 0;
        bool cdfEnabled = DeltaLake.ChangeDataFeed.CdfConfig.IsEnabled(
            snapshot.Metadata.Configuration);
        bool rowTrackingEnabled = DeltaLake.RowTracking.RowTrackingConfig.IsEnabled(
            snapshot.Metadata.Configuration);
        var (matRowIdName, matRowVerName) = DeltaLake.RowTracking.RowTrackingConfig
            .TryGetMaterializedColumnNames(snapshot.Metadata.Configuration);
        // Deletion vectors are opt-in. When disabled, a DELETE may only remove WHOLE files (a clean
        // file/partition boundary); a partial match that would need a soft-delete throws below.
        bool deletionVectorsEnabled = DeletionVectors.DeletionVectorConfig.IsEnabled(
            snapshot.Metadata.Configuration);
        var pruner = prunePredicate is null ? null : new DeltaFilePruner(
            snapshot.Schema, snapshot.Metadata.PartitionColumns,
            _options.PreferTypedCheckpointStats);

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
            // Paired 1:1 with deletedRowBatches: each deleted row's STABLE id + commit version, for the change
            // file. A deletion vector moves no row, so a row's identity here is exactly what it already was.
            var deletedRowTracking = new List<(Int64Array Ids, Int64Array Vers)>();
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

                // This path reads the file's raw columns rather than going through ReadFileAsync, so the hidden
                // materialized row-tracking columns (present on any file a rewrite produced) are still attached.
                // Take them off before the predicate runs — a user predicate should never be handed a column
                // that is not in the table's schema — and keep their values to identify the deleted rows.
                Int64Array? rawMatIds = null, rawMatVers = null;
                if (matRowIdName is not null || matRowVerName is not null)
                {
                    (logicalBatch, rawMatIds, rawMatVers) = RowTracking.RowTrackingWriter
                        .StripMaterializedColumns(logicalBatch, matRowIdName, matRowVerName);
                }

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
                {
                    deletedRowBatches.Add(TakeRowsFromBatch(logicalBatch, matchRows));
                    if (rowTrackingEnabled)
                    {
                        deletedRowTracking.Add(ResolveRowTrackingAt(
                            matchRows, rawMatIds, rawMatVers, addFile.BaseRowId,
                            addFile.DefaultRowCommitVersion, rowOffset));
                    }
                }

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
                BaseRowId = addFile.BaseRowId,
                DefaultRowCommitVersion = addFile.DefaultRowCommitVersion,
            });
            removedPaths.Add(addFile.Path);

            if (!wholeFile)
            {
                var newDv = await dvWriter.CreateAsync(
                    allDeleted, allDeleted.Count, cancellationToken).ConfigureAwait(false);
                // The vector is ours; the file it masks is not — the re-add below names the SOURCE data file,
                // which is live table data whatever becomes of this transaction.
                written?.RecordDeletionVector(newDv);

                actions.Add(addFile with
                {
                    DeletionVector = newDv,
                    DataChange = true,
                    Stats = StatsWithLooseBounds(addFile.GetStatsJson()),
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
                for (int b = 0; b < deletedRowBatches.Count; b++)
                {
                    var trk = b < deletedRowTracking.Count ? deletedRowTracking[b] : ((Int64Array, Int64Array)?)null;
                    var cdcAction = await ChangeDataFeed.CdfWriter.WriteAsync(
                        _fs, snapshot, deletedRowBatches[b], DeltaLake.ChangeDataFeed.CdfConfig.Delete,
                        addFile.PartitionValues, _options.ParquetWriteOptions,
                        cancellationToken, trk?.Item1, trk?.Item2, written).ConfigureAwait(false);
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
        // UPDATE is a data change, and its post-image is re-validated below, so a constrained
        // table is writable here rather than refused.
        ValidateWritable(
            snapshot, isAppend: false,
            handling: WriteTimeExpressionHandling.ValidatedHere);

        // The rewrite's post-image files are written before the commit is attempted, and there is no
        // transaction here for a host to abort — so a conflict takes them back rather than orphaning them.
        return await CollectOnFailureAsync(async written =>
        {
            var plan = await ComputeUpdateActionsAsync(
                snapshot, predicate, updater, cancellationToken, prunePredicate, written: written)
                .ConfigureAwait(false);

            // An UPDATE reads exactly the files it rewrites, so — like DELETE — the removed paths are both its
            // read-set (concurrentDeleteRead) and its planned removes (delete/delete). The analyzable overload
            // additionally records its read predicate so a concurrent add that matches it conflicts. Route it
            // through the OCC loop so a single-shot UPDATE rebases past a non-conflicting concurrent commit
            // instead of throwing — its copy-on-write post-image add's row-tracking baseRowId is re-derived on
            // rebase (a conflict on any file it rewrote aborts first, so the survivors' ids stay valid).
            long committed = await CommitOccAsync(
                snapshot, plan.Actions,
                new ReadSet { Files = plan.RemovedPaths, Predicates = readPredicates },
                plan.RemovedPaths, IsolationLevel.WriteSerializable, "UPDATE",
                rebaseSafe: true, cancellationToken, written: written,
                isBlindAppend: false).ConfigureAwait(false);

            return (plan.TotalUpdated, committed);
        }, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>The remove/add (and CDC) actions an UPDATE produces, the paths it rewrote, and the row
    /// count — everything a commit needs, without committing. Shared by the auto-committing
    /// <see cref="UpdateAsync"/> and the transactional <see cref="DeltaTransaction"/> path.</summary>
    internal sealed record UpdateActions(
        IReadOnlyList<DeltaAction> Actions, ISet<string> RemovedPaths, long TotalUpdated, long NextRowId);

    /// <summary>
    /// Computes the actions for an UPDATE against <paramref name="snapshot"/> WITHOUT committing. Like a
    /// DELETE, the removed-file paths double as the read-set: an UPDATE reads exactly the files it rewrites,
    /// so a concurrent commit that removed one of them is the conflict that must abort it.
    /// <para><paramref name="prunePredicate"/> skips files whose statistics prove no row can match, exactly
    /// as in <see cref="ComputeDeleteActionsAsync"/> — a pruned file has no matching row to update, so the
    /// removed-file set is unchanged; only the read is avoided.</para>
    /// </summary>
    /// <param name="rowIdStart">Where the rewrite's post-image adds begin reserving stable row ids — see
    /// <see cref="ComputeWriteActionsAsync"/>' parameter of the same name. Null starts at the snapshot's mark.</param>
    /// <param name="written">Records the post-image and change-data files this rewrite creates. The files it
    /// REMOVES are not recorded: they are the table's current data until this commit lands, so an abort must
    /// leave them exactly where they are.</param>
    internal async ValueTask<UpdateActions> ComputeUpdateActionsAsync(
        Snapshot.Snapshot snapshot,
        Func<RecordBatch, BooleanArray> predicate,
        Func<RecordBatch, RecordBatch> updater,
        CancellationToken cancellationToken,
        Expressions.Predicate? prunePredicate = null,
        long? rowIdStart = null,
        WrittenFileLedger? written = null)
    {
        var actions = new List<DeltaAction>();
        var removedPaths = new HashSet<string>(StringComparer.Ordinal);
        long totalUpdated = 0;

        // Parsed once for the whole rewrite rather than per batch. Create refuses an expression
        // this writer cannot read, which is what keeps the UPDATE fail-closed on a table whose
        // constraint is outside the grammar.
        var constraints = DeltaConstraintEnforcer.Create(snapshot);
        var generatedColumns = DeltaGeneratedColumns.Create(snapshot);

        bool cdfEnabled = DeltaLake.ChangeDataFeed.CdfConfig.IsEnabled(
            snapshot.Metadata.Configuration);
        var pruner = prunePredicate is null ? null : new DeltaFilePruner(
            snapshot.Schema, snapshot.Metadata.PartitionColumns,
            _options.PreferTypedCheckpointStats);

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
        long nextRowId = rowTrackingEnabled ? rowIdStart ?? snapshot.RowIdHighWaterMark : 0;

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
            // Paired 1:1 with preimages/postimages: the identity to materialize in each change file.
            var changeTracking = new List<(Int64Array PreIds, Int64Array PreVers,
                                           Int64Array PostIds, Int64Array PostVers)>();

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

                    // Only the post-image. Rows the predicate did not match are copied through
                    // untouched: they were already in the table, so re-checking them would refuse
                    // an unrelated UPDATE over data another engine wrote under semantics we do not
                    // share, and would be work with nothing to find.
                    updatedBatch = generatedColumns?.Recompute(updatedBatch) ?? updatedBatch;
                    constraints?.Validate(updatedBatch);

                    outputBatches.Add(updatedBatch);
                    // Matched rows keep their id; their commit version advances to this commit (they changed).
                    outTracking?.Add(batchIds is not null
                        ? (TakeIds(batchIds, matchRows), ConstInt64(newVersion, matchRows.Count))
                        : ((Int64Array, Int64Array)?)null);

                    // Collect preimage and postimage for CDC, each with the touched rows' identity. Both images
                    // are the SAME rows, so both carry the same ids; only the commit version differs — the
                    // pre-image belongs to the version that last wrote it, the post-image to this one.
                    if (cdfEnabled)
                    {
                        preimages.Add(matchBatch);
                        postimages.Add(updatedBatch);
                        if (rowTrackingEnabled)
                        {
                            var ids = batchIds is not null
                                ? TakeIds(batchIds, matchRows) : AllNullInt64(matchRows.Count);
                            changeTracking.Add((
                                ids,
                                batchVers is not null ? TakeIds(batchVers, matchRows) : AllNullInt64(matchRows.Count),
                                ids,
                                ConstInt64(newVersion, matchRows.Count)));
                        }
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
            // Recorded before the write, like the append path's: a part-written rewrite is still an orphan.
            written?.Record(newFileName);
            long fileSize;

            // The rows were READ, so they carry the partition columns the read path materializes — a data file
            // never stores those (their values live in add.partitionValues). Drop them first, so the rewrite's
            // layout AND its statistics match what the append path writes for the same rows.
            var dataBatches = new List<RecordBatch>(outputBatches.Count);
            foreach (var ob in outputBatches)
            {
                dataBatches.Add(Partitioning.PartitionUtils.RemovePartitionColumns(
                    ob, snapshot.Metadata.PartitionColumns));
            }

            // Physical names + parquet field ids at EVERY level (nested struct children included — the
            // top-level-only rename/stamp pair left them logical-named and id-less). When row tracking is on,
            // append the materialized id + commit-version columns (declared physical names) carrying each moved
            // row's ORIGINAL values. Prepared up front so both the built-in and pluggable writers see the same
            // batches.
            var writeBatches = new List<RecordBatch>(dataBatches.Count);
            for (int k = 0; k < dataBatches.Count; k++)
            {
                var physicalBatch = ColumnMappingRecursive.ToPhysical(
                    dataBatches[k], snapshot.Schema, mappingMode);
                // Drop the VARIANT annotation for a Spark 4.0.x-compatible table (bytes unchanged; the
                // read path recovers the type from the schema). Stats use dataBatches, not these.
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

            string? stats = Stats.StatsCollector.Collect(dataBatches);

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
                BaseRowId = addFile.BaseRowId,
                DefaultRowCommitVersion = addFile.DefaultRowCommitVersion,
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
                for (int b = 0; b < preimages.Count; b++)
                {
                    var trk = b < changeTracking.Count ? changeTracking[b] : default;
                    var cdcAction = await ChangeDataFeed.CdfWriter.WriteAsync(
                        _fs, snapshot, preimages[b], DeltaLake.ChangeDataFeed.CdfConfig.UpdatePreimage,
                        addFile.PartitionValues, _options.ParquetWriteOptions,
                        cancellationToken, trk.PreIds, trk.PreVers, written).ConfigureAwait(false);
                    actions.Add(cdcAction);
                }
                for (int b = 0; b < postimages.Count; b++)
                {
                    var trk = b < changeTracking.Count ? changeTracking[b] : default;
                    var cdcAction = await ChangeDataFeed.CdfWriter.WriteAsync(
                        _fs, snapshot, postimages[b], DeltaLake.ChangeDataFeed.CdfConfig.UpdatePostimage,
                        addFile.PartitionValues, _options.ParquetWriteOptions,
                        cancellationToken, trk.PostIds, trk.PostVers, written).ConfigureAwait(false);
                    actions.Add(cdcAction);
                }
            }
        }

        // Persist the advanced row-id high-water mark (same source of truth the append path maintains).
        if (rowTrackingEnabled && nextRowId > snapshot.RowIdHighWaterMark)
            actions.Add(DeltaLake.RowTracking.RowTrackingConfig.BuildHighWaterMarkAction(nextRowId));

        return new UpdateActions(actions, removedPaths, totalUpdated, nextRowId);
    }

    // Reorders/subsets a resolved row-id (or commit-version) array to match a rewritten batch's row order.
    // This is the take kernel with the type fixed, so it gathers value slots rather than round-tripping each
    // row through Int64Array.Builder.
    private static Int64Array TakeIds(Int64Array src, List<int> idx) =>
        (Int64Array)ArrowCompute.Take(src, idx);

    // The row-tracking identity of the rows at `rows` (indices into a raw, unfiltered batch whose first row
    // sits at absolute file position `batchStartRow`), resolved the way a conformant reader does: the
    // MATERIALIZED value where the file carries one, else add.baseRowId + absolute position and the file's
    // defaultRowCommitVersion. Used by the paths that read a file's raw columns themselves rather than going
    // through ReadFileAsync, which surfaces the same resolution via its out-params.
    private static (Int64Array Ids, Int64Array Vers) ResolveRowTrackingAt(
        List<int> rows, Int64Array? materializedIds, Int64Array? materializedVersions,
        long? baseRowId, long? defaultRowCommitVersion, long batchStartRow)
    {
        var idb = new Int64Array.Builder().Reserve(rows.Count);
        var vrb = new Int64Array.Builder().Reserve(rows.Count);
        foreach (int i in rows)
        {
            long? mid = materializedIds is not null && i < materializedIds.Length && !materializedIds.IsNull(i)
                ? materializedIds.GetValue(i) : null;
            long? id = mid ?? (baseRowId is { } br ? br + batchStartRow + i : (long?)null);
            if (id is { } iv) idb.Append(iv); else idb.AppendNull();

            long? mv = materializedVersions is not null && i < materializedVersions.Length
                       && !materializedVersions.IsNull(i)
                ? materializedVersions.GetValue(i) : null;
            long? ver = mv ?? defaultRowCommitVersion;
            if (ver is { } vv) vrb.Append(vv); else vrb.AppendNull();
        }
        return (idb.Build(), vrb.Build());
    }

    // A constant Int64 column (the commit version assigned to every matched/updated row). Tiled by Repeat
    // rather than appended per row, which also drops the validity buffer a builder allocates for a column
    // that has no nulls to record.
    private static Int64Array ConstInt64(long value, int n) =>
        (Int64Array)ArrowCompute.Repeat(
            Apache.Arrow.Types.Int64Type.Default, BitConverter.GetBytes(value), n);

    private static int CountTrue(BooleanArray mask)
    {
        int count = 0;
        for (int i = 0; i < mask.Length; i++)
            if (!mask.IsNull(i) && mask.GetValue(i) == true)
                count++;
        return count;
    }

    // The Hive-style directory a file with these partition values belongs under. Spelled per this table's
    // configured PartitionPathSpelling, with the escape decision that Spark makes from Shell.WINDOWS taken
    // from what THIS table's storage declares it cannot hold — see PartitionPathSpelling for why the
    // storage rather than the writing process. Funnelled through one helper so the write paths cannot
    // drift apart: two sites spelling one partition differently would scatter a partition across two
    // directories, which readers tolerate but nobody wants to debug.
    private string BuildPartitionPath(IReadOnlyDictionary<string, string> partitionValues) =>
        DeltaPath.BuildPartitionPath(
            partitionValues, _options.PartitionPathSpelling, _fs.PathConstraints);

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

    private static RecordBatch TakeRowsFromBatch(RecordBatch batch, List<int> rows) =>
        ArrowCompute.Take(batch, batch.Schema, rows);

    #endregion

    /// <summary>
    /// Reads row-level changes over a version range using the Change Data Feed. Each batch carries
    /// <c>_change_type</c> ("insert", "delete", "update_preimage", "update_postimage"),
    /// <c>_commit_version</c> and <c>_commit_timestamp</c>. For versions with CDC files those are used
    /// directly; for versions without, changes are inferred from add/remove actions.
    ///
    /// <para>With <see cref="DeltaRowMetadata.RowTracking"/> each change row also carries its STABLE
    /// identity, which is what lets a consumer JOIN a change to the row it happened to: an
    /// <c>update_preimage</c> and its <c>update_postimage</c> report the SAME id, and that id is the one
    /// <see cref="ReadAsync"/> reports for the row in the table. Resolution matches the main read path where
    /// the feed comes from a data file (materialized value, else <c>baseRowId + position</c>). Where it comes
    /// from a <c>_change_data</c> file the id can only be the materialized one — a <c>cdc</c> action has no
    /// <c>baseRowId</c> — so a change file written without row-tracking columns (by an older
    /// engineered-wood, or any writer that omits them) reports NULL ids rather than wrong ones. The commit
    /// version there defaults to the version being read.</para>
    /// </summary>
    public IAsyncEnumerable<RecordBatch> ReadChangesAsync(
        DeltaChangeReadOptions options,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        if (options is null)
            throw new ArgumentNullException(nameof(options));

        var snapshot = CurrentSnapshot;
        bool emitRowTracking = (options.Metadata & DeltaRowMetadata.RowTracking) != 0;

        // Locator and RowAddress both name a row's place in the ACTIVE set, and a _change_data file is not in
        // it — so neither address would mean anything a caller could key by. Refused by declaration rather
        // than left to be discovered.
        var unsupported = options.Metadata & ~DeltaRowMetadata.RowTracking;
        if (unsupported != DeltaRowMetadata.None)
        {
            throw new ArgumentException(
                $"DeltaRowMetadata.{unsupported} is not available on the change feed: a _change_data file is "
                + "not in the snapshot's active set, so a file ordinal or add.path there does not address a "
                + "row anything else can resolve. Only DeltaRowMetadata.RowTracking is valid here.",
                nameof(options));
        }
        if (emitRowTracking)
            ValidateReadMetadata(snapshot, DeltaRowMetadata.RowTracking, options.MetadataPrefix);

        return ReadChangesCoreAsync(snapshot, options, emitRowTracking, cancellationToken);
    }

    // The materialized column names are fixed at enablement and never change, so the CURRENT snapshot's
    // metadata names them correctly for every version in the range — the same simplification the feed already
    // makes for the schema itself.
    private async IAsyncEnumerable<RecordBatch> ReadChangesCoreAsync(
        Snapshot.Snapshot snapshot, DeltaChangeReadOptions options, bool emitRowTracking,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var (matRowIdName, matRowVerName) = DeltaLake.RowTracking.RowTrackingConfig
            .TryGetMaterializedColumnNames(snapshot.Metadata.Configuration);

        var emitted = ChangeDataFeed.CdfReader.ReadChangesAsync(
            _fs, _log, options.StartVersion, options.EndVersion, _dataFileReadOptions,
            snapshot.ArrowSchema, snapshot.Schema,
            ColumnMapping.GetMode(snapshot.Metadata.Configuration),
            snapshot.Metadata.PartitionColumns,
            matRowIdName, matRowVerName, emitRowTracking,
            options.MetadataPrefix + DeltaMetadataColumns.RowIdSuffix,
            options.MetadataPrefix + DeltaMetadataColumns.RowCommitVersionSuffix,
            cancellationToken);

        // Keep set for the projection, if one was asked for. The feed's own three columns are never dropped
        // — they are what makes a change row a change — and neither are the metadata columns just added.
        HashSet<string>? keep = null;
        if (options.Columns is not null)
        {
            keep = new HashSet<string>(options.Columns, StringComparer.Ordinal)
            {
                DeltaLake.ChangeDataFeed.CdfConfig.ChangeTypeColumn,
                DeltaLake.ChangeDataFeed.CdfConfig.CommitVersionColumn,
                DeltaLake.ChangeDataFeed.CdfConfig.CommitTimestampColumn,
            };
            if (emitRowTracking)
            {
                keep.Add(options.MetadataPrefix + DeltaMetadataColumns.RowIdSuffix);
                keep.Add(options.MetadataPrefix + DeltaMetadataColumns.RowCommitVersionSuffix);
            }
        }

        await foreach (var batch in emitted.ConfigureAwait(false))
            yield return keep is null ? batch : ProjectBatch(batch, keep);
    }

    /// <summary>Drops the columns not in <paramref name="keep"/>, preserving order. A post-read projection:
    /// the feed's files were read in full, so this shrinks what crosses the API boundary, not what is read
    /// from storage.</summary>
    private static RecordBatch ProjectBatch(RecordBatch batch, HashSet<string> keep)
    {
        var builder = new Apache.Arrow.Schema.Builder();
        var columns = new List<IArrowArray>(batch.ColumnCount);
        for (int c = 0; c < batch.ColumnCount; c++)
        {
            var field = batch.Schema.FieldsList[c];
            if (!keep.Contains(field.Name))
                continue;
            builder.Field(field);
            columns.Add(batch.Column(c));
        }
        return new RecordBatch(builder.Build(), columns, batch.Length);
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
    /// <param name="rowIds">On a ROW TRACKING table, each row's stable row id — one value per row of
    /// <paramref name="rows"/>, ordinarily the <c>_metadata.row_id</c> that
    /// a read with <see cref="DeltaRowMetadata.RowTracking"/> reported for it. A <c>cdc</c> action has no
    /// <c>baseRowId</c> for a reader to derive an id from, so a change file left without these carries no
    /// identity at all: the feed reports NULL for its rows. Ignored on
    /// a table that does not track row identity.</param>
    /// <param name="rowCommitVersions">The commit-version companion of <paramref name="rowIds"/>. Pass the
    /// version each row was last changed in; leaving it null defaults every row to the version the change
    /// file is committed in, which is right for a post-image and wrong for a pre-image.</param>
    public async ValueTask<CdcFile> WriteChangeDataFileAsync(
        RecordBatch rows, string changeType,
        IReadOnlyDictionary<string, string>? partitionValues = null,
        CancellationToken cancellationToken = default,
        Int64Array? rowIds = null,
        Int64Array? rowCommitVersions = null)
    {
        ThrowIfDisposed();
        if (rows is null)
            throw new ArgumentNullException(nameof(rows));
        ValidateChangeDataStageable(CurrentSnapshot, changeType);

        return await ChangeDataFeed.CdfWriter.WriteAsync(
            _fs, CurrentSnapshot, rows, changeType,
            partitionValues ?? EmptyPartitionValues,
            _options.ParquetWriteOptions, cancellationToken,
            rowIds, rowCommitVersions).ConfigureAwait(false);
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
    /// Writes a checkpoint for the table's current version, now, whatever the interval would have said —
    /// and publishes the <c>_last_checkpoint</c> hint that points readers at it.
    ///
    /// <para>Every commit path already checkpoints on
    /// <see cref="DeltaTableOptions.CheckpointInterval"/>, so this is not needed to keep a table
    /// checkpointed. It is for a host with a cadence of its own: one that sets
    /// <c>CheckpointInterval = 0</c> to take the decision entirely, or that wants a checkpoint at a moment
    /// it knows to be a good one — after a bulk load, before handing the table to another engine — rather
    /// than at whichever commit happens to land on a multiple.</para>
    ///
    /// <para>The checkpoint is written under the table's own
    /// <see cref="DeltaTableOptions.ParquetWriteOptions"/> and
    /// <see cref="DeltaTableOptions.CheckpointFormat"/>, which is the reason for having this rather than
    /// constructing a <see cref="Checkpoint.CheckpointWriter"/> at the call site: that duplicates the
    /// table's configuration, and a copy of a policy is a copy that drifts.</para>
    ///
    /// <para>Writing a checkpoint is not free — it materialises the whole active-file set — and it is
    /// idempotent in effect but not in cost, so calling it per commit is a way to pay for checkpointing
    /// twice. Concurrent writers are safe: a classic checkpoint overwrites one fixed path with identical
    /// content, and a V2 one is UUID-named, so neither can corrupt the other.</para>
    ///
    /// <para><b>⚠ This also runs log cleanup</b>, deleting commits the new checkpoint covers that are older
    /// than the table's <c>delta.logRetentionDuration</c> — the same thing an automatic checkpoint does,
    /// because it is the checkpoint that makes those files redundant and not the reason it was written.
    /// A table that wants the checkpoint without the reclaim sets
    /// <c>delta.enableExpiredLogCleanup = false</c>.</para>
    /// </summary>
    /// <returns>The version checkpointed.</returns>
    public async ValueTask<long> CheckpointAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();

        var snapshot = CurrentSnapshot;
        await WriteCheckpointAndCleanUpLogAsync(snapshot, cancellationToken).ConfigureAwait(false);
        return snapshot.Version;
    }

    /// <summary>
    /// Reads the table — one entry point for every combination of projection, pruning, version and per-row
    /// metadata. See <see cref="DeltaReadOptions"/>; passing null reads every column of the current snapshot
    /// with no metadata, i.e. the same as <see cref="ReadAllAsync(IReadOnlyList{string}, EngineeredWood.Expressions.Predicate, CancellationToken)"/>.
    ///
    /// <para>Batches are emitted in the snapshot's PATH-SORTED active-file order, which is the order
    /// <see cref="DeltaRowMetadata.RowAddress"/>' file ordinals are assigned in.</para>
    ///
    /// <para>Which version is read: <see cref="DeltaReadOptions.Snapshot"/> if set (no I/O), else
    /// <see cref="DeltaReadOptions.AtVersion"/>, else the current snapshot. Inside a transaction, pass its
    /// <see cref="DeltaTransaction.Snapshot"/> — otherwise the read follows
    /// <see cref="CurrentSnapshot"/> and can return rows from a version the transaction is not validating
    /// against.</para>
    /// </summary>
    public async IAsyncEnumerable<RecordBatch> ReadAsync(
        DeltaReadOptions? options = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        options ??= new DeltaReadOptions();
        var snapshot = ResolveReadSnapshot(options);
        if (snapshot is null)
        {
            snapshot = await GetSnapshotAtVersionAsync(options.AtVersion!.Value, cancellationToken)
                .ConfigureAwait(false);
        }

        await foreach (var batch in ReadCoreAsync(snapshot, options, cancellationToken)
                           .ConfigureAwait(false))
        {
            yield return batch;
        }
    }

    /// <summary>
    /// The schema <see cref="ReadAsync"/> will emit for these options, WITHOUT reading anything — the
    /// table's own fields (projected by <see cref="DeltaReadOptions.Columns"/>) followed by the requested
    /// metadata columns in <see cref="DeltaRowMetadata"/> declaration order. A host that advertises a scan's
    /// schema at bind time, before any data is read, gets it here instead of paying for a metadata open.
    ///
    /// <para>Validated on the same terms as the read: asking for
    /// <see cref="DeltaRowMetadata.RowTracking"/> on a table without row tracking, or for a metadata column
    /// whose name a table column already occupies, throws here too rather than at first batch.</para>
    ///
    /// <para>Resolved against <see cref="DeltaReadOptions.Snapshot"/> when one is set — a pinned snapshot
    /// costs no I/O, so the schema is the PINNED version's. <see cref="DeltaReadOptions.AtVersion"/> is
    /// deliberately NOT honoured here: resolving it would need a log read, and this method promises none.
    /// Pass the snapshot instead when the version's own schema matters.</para>
    /// </summary>
    public Apache.Arrow.Schema GetReadSchema(DeltaReadOptions options)
    {
        ThrowIfDisposed();
        if (options is null)
            throw new ArgumentNullException(nameof(options));
        var snapshot = ResolveReadSnapshot(options) ?? CurrentSnapshot;
        ValidateReadMetadata(snapshot, options.Metadata, options.MetadataPrefix);
        return BuildReadSchema(snapshot, options);
    }

    /// <summary>
    /// The snapshot a read resolves to WITHOUT I/O: the caller's pinned one, or the current one when neither
    /// pin nor time-travel version was asked for. Null means only <see cref="DeltaReadOptions.AtVersion"/>
    /// can answer, which needs a log read.
    /// </summary>
    private Snapshot.Snapshot? ResolveReadSnapshot(DeltaReadOptions options)
    {
        if (options.Snapshot is { } pinned)
        {
            // Two ways to name a version, silently disagreeing, is exactly the hazard this option exists to
            // remove — so it is refused rather than resolved by precedence.
            if (options.AtVersion is not null)
            {
                throw new ArgumentException(
                    "DeltaReadOptions.Snapshot and .AtVersion both name the version to read, and they are "
                    + "mutually exclusive. Set one.",
                    nameof(options));
            }
            RequireSnapshotOfThisTable(pinned, nameof(options));
            return pinned;
        }
        return options.AtVersion is { } v && v != CurrentSnapshot.Version ? null : CurrentSnapshot;
    }

    /// <summary>
    /// Reads all data from the current snapshot as a stream of RecordBatches — the ordinary-caller form, so
    /// that reading a table does not require constructing a record. Exposes no per-row metadata; that is
    /// <see cref="ReadAsync"/>'s job.
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
    public IAsyncEnumerable<RecordBatch> ReadAllAsync(
        IReadOnlyList<string>? columns,
        EngineeredWood.Expressions.Predicate? filter,
        CancellationToken cancellationToken = default) =>
        ReadAsync(new DeltaReadOptions { Columns = columns, Filter = filter }, cancellationToken);

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
    public IAsyncEnumerable<RecordBatch> ReadAtVersionAsync(
        long version,
        IReadOnlyList<string>? columns,
        EngineeredWood.Expressions.Predicate? filter,
        CancellationToken cancellationToken = default) =>
        ReadAsync(
            new DeltaReadOptions { AtVersion = version, Columns = columns, Filter = filter },
            cancellationToken);

    // ── Read-side transient row ids ────────────────────────────────────────────────────────────────────
    //
    // A read that appends a trailing non-null Int64 TransientRowAddress.ColumnName = (fileOrdinal <<
    // PositionBits) | ABSOLUTE in-file position (path-sorted active set; the DV-inclusive parquet row index).
    // NOT a stable Delta row id — valid only within one snapshot. It round-trips to the row-id DML surface
    // (RowSelection.FromRowAddresses unpacks it into the path-keyed key the DML consumes), so a host
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

    // ── The one read path ──────────────────────────────────────────────────────────────────────────────
    //
    // All three metadata kinds resolve from the SAME per-file read: ReadFileAsync already surfaces each
    // surviving row's absolute in-file position and its resolved stable id/commit version as out-params, so
    // asking for two kinds costs one pass rather than two reads of the table. (They used to be two private
    // iterators calling ReadFileAsync with different out-params, which is why a host wanting a mutation key
    // AND a stable identity had to read twice.) Files are walked in PATH-SORTED order in every case, which
    // is what makes RowAddress' file ordinals mean anything, and makes the plain read's batch order
    // deterministic rather than dictionary order.

    private async IAsyncEnumerable<RecordBatch> ReadCoreAsync(
        Snapshot.Snapshot snapshot,
        DeltaReadOptions options,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var metadata = options.Metadata;
        ValidateReadMetadata(snapshot, metadata, options.MetadataPrefix);

        bool wantAddress = (metadata & DeltaRowMetadata.RowAddress) != 0;
        bool wantLocator = (metadata & DeltaRowMetadata.Locator) != 0;
        bool wantTracking = (metadata & DeltaRowMetadata.RowTracking) != 0;
        bool wantPositions = wantAddress || wantLocator;

        var metadataFields = MetadataFields(metadata, options.MetadataPrefix);
        var pruner = options.Filter is null ? null : new DeltaFilePruner(
            snapshot.Schema, snapshot.Metadata.PartitionColumns,
            _options.PreferTypedCheckpointStats);

        var ordered = OrderedActiveFiles(snapshot);
        for (int ordinal = 0; ordinal < ordered.Count; ordinal++)
        {
            var addFile = ordered[ordinal];
            if (pruner is not null && !pruner.ShouldInclude(addFile, options.Filter!))
                continue;

            // Only ask ReadFileAsync for the out-params this read actually needs: each one costs a
            // per-row builder pass inside the pipeline.
            var absOut = wantPositions ? new List<Int64Array?>() : null;
            var idsOut = wantTracking ? new List<Int64Array?>() : null;
            var versOut = wantTracking ? new List<Int64Array?>() : null;
            byte[]? pathBytes = wantLocator
                ? System.Text.Encoding.UTF8.GetBytes(addFile.Path) : null;

            int bi = -1;
            await foreach (var batch in ReadFileAsync(
                               addFile, options.Columns, snapshot, cancellationToken,
                               strippedRowIdsOut: idsOut, strippedVersionsOut: versOut,
                               strippedAbsPositionsOut: absOut).ConfigureAwait(false))
            {
                bi++;
                if (metadataFields.Count == 0)
                {
                    yield return batch;
                    continue;
                }

                var absPos = absOut is not null && bi < absOut.Count ? absOut[bi] : null;
                var columns = new List<IArrowArray>(metadataFields.Count);

                if (wantAddress)
                {
                    var b = new Int64Array.Builder().Reserve(batch.Length);
                    for (int i = 0; i < batch.Length; i++)
                        b.Append(TransientRowAddress.Pack(ordinal, AbsoluteAt(absPos, i)));
                    columns.Add(b.Build());
                }
                if (wantLocator)
                {
                    columns.Add(ArrowCompute.Repeat(Apache.Arrow.Types.StringType.Default, pathBytes!, batch.Length));
                    var b = new Int64Array.Builder().Reserve(batch.Length);
                    for (int i = 0; i < batch.Length; i++)
                        b.Append(AbsoluteAt(absPos, i));
                    columns.Add(b.Build());
                }
                if (wantTracking)
                {
                    var ids = idsOut is not null && bi < idsOut.Count ? idsOut[bi] : null;
                    var vers = versOut is not null && bi < versOut.Count ? versOut[bi] : null;
                    columns.Add(ids ?? AllNullInt64(batch.Length));
                    columns.Add(vers ?? AllNullInt64(batch.Length));
                }

                yield return AppendColumns(batch, metadataFields, columns);
            }
        }
    }

    /// <summary>A row's ABSOLUTE in-file position: the out-param value where the read surfaced one, else the
    /// batch offset — which is the same number for a file with no deletion vector, the only case where the
    /// out-param is absent.</summary>
    private static long AbsoluteAt(Int64Array? absPositions, int i) =>
        absPositions is not null && i < absPositions.Length && !absPositions.IsNull(i)
            ? absPositions.GetValue(i)!.Value : i;

    /// <summary>Appends the metadata columns to a user batch. They are added AFTER the pipeline rather than
    /// inside it, so the schema reconciliation in <see cref="ProcessFileBatchesAsync"/> — which is defined
    /// against the table's Delta schema — never sees a column that is not in it.</summary>
    private static RecordBatch AppendColumns(
        RecordBatch batch, IReadOnlyList<Field> fields, IReadOnlyList<IArrowArray> values)
    {
        var builder = new Apache.Arrow.Schema.Builder();
        foreach (var field in batch.Schema.FieldsList)
            builder.Field(field);
        foreach (var field in fields)
            builder.Field(field);

        var columns = new List<IArrowArray>(batch.ColumnCount + values.Count);
        for (int c = 0; c < batch.ColumnCount; c++)
            columns.Add(batch.Column(c));
        columns.AddRange(values);

        return new RecordBatch(builder.Build(), columns, batch.Length);
    }

    /// <summary>
    /// The metadata columns a read appends, in <see cref="DeltaRowMetadata"/> declaration order. The single
    /// definition of that list: both <see cref="GetReadSchema"/> and <see cref="ReadCoreAsync"/> build from
    /// it, so the promised schema and the emitted one cannot drift.
    /// </summary>
    private static List<Field> MetadataFields(DeltaRowMetadata metadata, string prefix)
    {
        var fields = new List<Field>(5);
        if ((metadata & DeltaRowMetadata.RowAddress) != 0)
        {
            fields.Add(new Field(TransientRowAddress.ColumnName, Apache.Arrow.Types.Int64Type.Default, nullable: false));
        }
        if ((metadata & DeltaRowMetadata.Locator) != 0)
        {
            fields.Add(new Field(
                prefix + DeltaMetadataColumns.FilePathSuffix, Apache.Arrow.Types.StringType.Default, nullable: false));
            fields.Add(new Field(
                prefix + DeltaMetadataColumns.RowIndexSuffix, Apache.Arrow.Types.Int64Type.Default, nullable: false));
        }
        if ((metadata & DeltaRowMetadata.RowTracking) != 0)
        {
            // Nullable: a file predating row tracking on the table carries no baseRowId, so its rows have no
            // derivable id — reported as null rather than fabricated.
            fields.Add(new Field(
                prefix + DeltaMetadataColumns.RowIdSuffix, Apache.Arrow.Types.Int64Type.Default, nullable: true));
            fields.Add(new Field(
                prefix + DeltaMetadataColumns.RowCommitVersionSuffix, Apache.Arrow.Types.Int64Type.Default, nullable: true));
        }
        return fields;
    }

    private static Apache.Arrow.Schema BuildReadSchema(
        Snapshot.Snapshot snapshot, DeltaReadOptions options)
    {
        var baseSchema = options.Columns is not null
            ? BuildProjectedSchema(snapshot.ArrowSchema, options.Columns)
            : snapshot.ArrowSchema;

        var metadataFields = MetadataFields(options.Metadata, options.MetadataPrefix);
        if (metadataFields.Count == 0)
            return baseSchema;

        var builder = new Apache.Arrow.Schema.Builder();
        foreach (var field in baseSchema.FieldsList)
            builder.Field(field);
        foreach (var field in metadataFields)
            builder.Field(field);
        return builder.Build();
    }

    /// <summary>
    /// The preconditions for the requested metadata: row tracking must be enabled on the table if its
    /// columns were asked for, and no emitted name may collide with a column the table already has. Both
    /// failures are refused rather than papered over — an all-null id column on a table with no row
    /// tracking, or a user column shadowed by a generated one, would each be read as data rather than as
    /// the mistake it is. The prefix exists so a collision has somewhere to move to.
    /// </summary>
    private static void ValidateReadMetadata(
        Snapshot.Snapshot snapshot, DeltaRowMetadata metadata, string prefix)
    {
        if (prefix is null)
            throw new ArgumentNullException(nameof(prefix));
        if (metadata == DeltaRowMetadata.None)
            return;

        if ((metadata & DeltaRowMetadata.RowTracking) != 0
            && !DeltaLake.RowTracking.RowTrackingConfig.IsEnabled(snapshot.Metadata.Configuration))
        {
            throw new InvalidOperationException(
                "DeltaRowMetadata.RowTracking requires 'delta.enableRowTracking=true' on the table. This "
                + "table does not track row identity, so its rows have no stable id to report — use "
                + "DeltaRowMetadata.Locator or .RowAddress for a snapshot-scoped row ADDRESS instead.");
        }

        var emitted = MetadataFields(metadata, prefix);
        foreach (var field in snapshot.ArrowSchema.FieldsList)
        {
            foreach (var m in emitted)
            {
                if (string.Equals(field.Name, m.Name, StringComparison.Ordinal))
                {
                    throw new InvalidOperationException(
                        $"The table has a column named '{field.Name}', which collides with the metadata "
                        + "column of that name. The read cannot report both — set "
                        + "DeltaReadOptions.MetadataPrefix to move the metadata columns out of the way.");
                }
            }
        }
    }

    /// <summary>An all-null Int64 array of <paramref name="length"/> rows — the row-tracking value for a batch
    /// whose file carries no derivable identity at all (no materialized column and no
    /// <c>add.baseRowId</c>).</summary>
    private static Int64Array AllNullInt64(int length)
    {
        var b = new Int64Array.Builder().Reserve(length);
        for (int i = 0; i < length; i++)
            b.AppendNull();
        return b.Build();
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
    /// <param name="handling">
    /// How this path accounts for the table's write-time expressions. Defaults to
    /// <see cref="WriteTimeExpressionHandling.Refuse"/>, so a path that cannot see the rows keeps
    /// refusing a table that declares any rather than committing it unchecked.
    /// </param>
    internal void ValidateWritable(
        Snapshot.Snapshot snapshot,
        bool isAppend,
        WriteTimeExpressionHandling handling = WriteTimeExpressionHandling.Refuse)
    {
        ProtocolVersions.ValidateWriteSupport(snapshot.Protocol);
        // Appends to a row-tracking table are spec-conformant (baseRowId + position). A copy-on-write rewrite
        // (UPDATE / OVERWRITE / DELETE) now materializes each surviving row's ORIGINAL id + commit version into
        // the declared hidden columns — but only when those column names are present in the metadata. A
        // row-tracking table missing them (spec-invalid) cannot materialize, so a rewrite is still refused.
        if (!isAppend)
            RejectRowTrackingWrite(snapshot);
        HonorWriterFeatures(snapshot, isAppend, handling);
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
    private static void HonorWriterFeatures(
        Snapshot.Snapshot snapshot,
        bool isAppend,
        WriteTimeExpressionHandling handling = WriteTimeExpressionHandling.Refuse)
    {
        var cfg = snapshot.Metadata.Configuration;
        if (cfg is not null)
        {
            if (!isAppend && cfg.TryGetValue("delta.appendOnly", out var ao)
                && string.Equals(ao, "true", StringComparison.OrdinalIgnoreCase))
            {
                throw new DeltaFormatException(
                    DeltaTableErrorCodes.CannotModifyAppendOnly,
                    "Table is append-only (delta.appendOnly=true): overwrite/delete/update are not permitted.");
            }
            foreach (var key in cfg.Keys)
            {
                // Only refused when the caller is not going to check the rows. A path that hands
                // us batches evaluates the constraint instead (DeltaConstraintEnforcer); a path
                // that does not — the host-engine commit seam most of all — has nothing to check
                // against, and refusing is the only honest answer. Enforcement is write-time
                // only in Delta, so one unvalidated commit poisons the table for every reader
                // after it.
                if (handling != WriteTimeExpressionHandling.Refuse)
                    break;

                if (key.StartsWith("delta.constraints.", StringComparison.Ordinal))
                {
                    throw new DeltaFormatException(
                        DeltaTableErrorCodes.UnevaluableTableExpression,
                        $"Table declares CHECK constraint '{key}' and this write path cannot evaluate it "
                        + "against the rows; write rejected.");
                }
            }
        }
        foreach (var field in snapshot.ArrowSchema.FieldsList)
        {
            if (handling == WriteTimeExpressionHandling.Refuse
                && field.Metadata is not null && field.Metadata.ContainsKey("delta.invariants"))
            {
                throw new DeltaFormatException(
                    DeltaTableErrorCodes.UnevaluableTableExpression,
                    $"Column '{field.Name}' declares an invariant expression and this write path cannot "
                    + "evaluate it against the rows; write rejected.");
            }
            if (handling == WriteTimeExpressionHandling.Refuse
                && field.Metadata is not null
                && field.Metadata.ContainsKey("delta.generationExpression"))
            {
                throw new DeltaFormatException(
                    DeltaTableErrorCodes.UnevaluableTableExpression,
                    $"Column '{field.Name}' declares a generation expression and this write path cannot "
                    + "compute it for the rows; write rejected.");
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
    /// <param name="isBlindAppend">
    /// The caller's claim about its own transaction, recorded verbatim in
    /// <c>commitInfo.isBlindAppend</c> on a plain append; null (the default) writes no field. A host that
    /// scanned this table and staged the result must pass <c>false</c>; only a host that genuinely read
    /// nothing may pass <c>true</c>. See <see cref="DeltaTransaction.IsBlindAppend"/>, whose contract this
    /// mirrors on the auto-committing surface, and the note in <c>CommitWriteAsync</c> for why the library
    /// must not answer this itself.
    /// </param>
    public ValueTask<long> WriteAsync(
        IReadOnlyList<RecordBatch> batches,
        DeltaWriteMode mode = DeltaWriteMode.Append,
        CancellationToken cancellationToken = default,
        IReadOnlyList<string>? repartitionTo = null,
        bool? isBlindAppend = null)
        => WriteCoreAsync(batches, mode, null, cancellationToken, repartitionTo: repartitionTo,
            isBlindAppend: isBlindAppend);

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
        IReadOnlyList<string>? repartitionTo = null,
        bool? isBlindAppend = null)
    {
        ThrowIfDisposed();
        var snapshot = CurrentSnapshot;
        // A dynamic partition overwrite removes files, so it is NOT an append for appendOnly enforcement.
        // Declares rather than Create: the gate only needs to know a rule exists, and parsing it
        // belongs where the rows are, in ComputeWriteActionsAsync below.
        ValidateWritable(
            snapshot,
            isAppend: mode == DeltaWriteMode.Append && !dynamicPartitionOverwrite,
            handling: WriteTimeExpressionHandling.ValidatedHere);

        // No transaction here for a host to abort, so the cleanup is the operation's own: a commit that
        // conflicts — which for the overwrite family is any collision at all, it makes ONE attempt — takes
        // back the parquet it just wrote instead of leaving it for VACUUM.
        return await CollectOnFailureAsync(async written =>
        {
            var (actions, _) = await ComputeWriteActionsAsync(
                snapshot, batches, mode, overwritePartitions, dynamicPartitionOverwrite, repartitionTo,
                cancellationToken, written: written).ConfigureAwait(false);

            long newVersion = snapshot.Version + 1;
            return await CommitWriteAsync(
                snapshot, actions, mode, dynamicPartitionOverwrite, newVersion,
                cancellationToken, written, isBlindAppend).ConfigureAwait(false);
        }, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Computes the full action list for a write against <paramref name="snapshot"/> WITHOUT committing:
    /// pre-commit removes (overwrite family), the per-batch adds (identity, row tracking, column mapping,
    /// partition split, stats), dynamic-overwrite removes, and any identity/repartition metaData +
    /// row-tracking high-water-mark action. Shared by the auto-committing <see cref="WriteCoreAsync"/> and
    /// the append path of <see cref="DeltaTransaction"/> — the transaction only calls it with
    /// <see cref="DeltaWriteMode.Append"/>, so the overwrite branches stay inert there.
    /// </summary>
    /// <param name="rowIdStart">Where this batch of adds begins reserving stable row ids, for a caller staging
    /// SEVERAL appends against one snapshot (a transaction): each call must continue from the previous call's
    /// <c>NextRowId</c>, not restart at the snapshot's high-water mark, or the two batches reserve the SAME ids.
    /// Null starts at the snapshot's mark, which is right for a single-shot commit.</param>
    /// <param name="written">Records the data files this write creates, so a transaction that never commits
    /// can take them back (<see cref="DeltaTransaction.AbortAsync"/>). Null on the auto-committing path.</param>
    internal async ValueTask<(IReadOnlyList<DeltaAction> Actions, long NextRowId)> ComputeWriteActionsAsync(
        Snapshot.Snapshot snapshot,
        IReadOnlyList<RecordBatch> batches,
        DeltaWriteMode mode,
        IReadOnlyDictionary<string, string>? overwritePartitions,
        bool dynamicPartitionOverwrite,
        IReadOnlyList<string>? repartitionTo,
        CancellationToken cancellationToken,
        long? rowIdStart = null,
        WrittenFileLedger? written = null)
    {
        // Nanosecond and second Arrow timestamps have no faithful Delta/Parquet encoding. Creation and
        // schema evolution reject them via SchemaConverter, but a write into an EXISTING table converts no
        // schema, so check the incoming batches here — the shared chokepoint for both the auto-committing
        // path and a transaction's append.
        foreach (var b in batches)
            SchemaConverter.ThrowIfUnsupportedTimestampUnit(b.Schema);

        // Same chokepoint, same reason: this path converts no schema either, so a column the table does not
        // declare would ride into the data file unnoticed. (No write here evolves the schema — the write
        // schema is always the snapshot's — so an unknown column is a mistake, never an addition.)
        ThrowIfUndeclaredColumns(batches, snapshot.Schema, "Write");

        // Generated columns first, because a CHECK constraint may reference one: validating before
        // the column exists would read a null the table never stores.
        var generated = DeltaGeneratedColumns.Create(snapshot);
        if (generated is not null)
        {
            var materialized = new List<RecordBatch>(batches.Count);
            foreach (var batch in batches)
                materialized.Add(generated.Apply(batch));

            batches = materialized;
        }

        // Beside the other per-batch guard, and before anything is written: a constraint
        // violation must leave the table untouched rather than half-written. Create parses, so an
        // expression this writer cannot read refuses here exactly as it did before evaluation
        // existed.
        var enforcer = DeltaConstraintEnforcer.Create(snapshot);
        if (enforcer is not null)
        {
            foreach (var batch in batches)
                enforcer.Validate(batch);
        }

        // Repartition-on-overwrite: changing partitionColumns is protocol-legal ONLY when every active file
        // is removed in the same commit — i.e. a FULL overwrite (a partition-scoped or dynamic overwrite
        // keeps files that would no longer conform to the new partition schema).
        bool repartitioned = false;
        if (repartitionTo is not null)
        {
            if (mode != DeltaWriteMode.Overwrite || overwritePartitions is { Count: > 0 } || dynamicPartitionOverwrite)
            {
                throw new DeltaFormatException(
                    DeltaTableErrorCodes.InvalidWriteMode,
                    "Repartitioning requires a FULL overwrite (the new partition schema is only valid when "
                    + "every active file is replaced in the same commit).");
            }
            foreach (var col in repartitionTo)
            {
                if (!snapshot.Schema.Fields.Any(f => f.Name == col))
                {
                    throw new DeltaFormatException(
                        DeltaTableErrorCodes.ColumnNotFound,
                        $"Repartition: '{col}' is not a column of the table.");
                }
            }
            repartitioned = !repartitionTo.SequenceEqual(snapshot.Metadata.PartitionColumns);
        }

        if (dynamicPartitionOverwrite && snapshot.Metadata.PartitionColumns.Count == 0)
        {
            throw new DeltaFormatException(
                DeltaTableErrorCodes.InvalidWriteMode,
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
                        DeltaTableErrorCodes.InvalidPartitionColumn,
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
                    BaseRowId = existingFile.BaseRowId,
                    DefaultRowCommitVersion = existingFile.DefaultRowCommitVersion,
                });
            }
        }

        // Row tracking: prepare high water mark
        bool rowTrackingEnabled = DeltaLake.RowTracking.RowTrackingConfig.IsEnabled(
            snapshot.Metadata.Configuration);
        long nextRowId = rowTrackingEnabled ? rowIdStart ?? snapshot.RowIdHighWaterMark : 0;
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
                        DeltaTableErrorCodes.DataOutsideTargetPartitions,
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
                string partDir = BuildPartitionPath(trackedPartValues);
                string fileName = string.IsNullOrEmpty(partDir)
                    ? $"{Guid.NewGuid():N}.parquet"
                    : $"{partDir}/{Guid.NewGuid():N}.parquet";

                // Recorded BEFORE the write: a write that fails part-way leaves bytes at this path, and they
                // are as orphaned as a complete file's.
                written?.Record(fileName);

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
                    BaseRowId = existingFile.BaseRowId,
                    DefaultRowCommitVersion = existingFile.DefaultRowCommitVersion,
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

        return (actions, nextRowId);
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
        CancellationToken cancellationToken,
        WrittenFileLedger? written = null,
        bool? isBlindAppend = null)
    {
        long committedVersion;
        bool blindAppend = mode == DeltaWriteMode.Append && !dynamicPartitionOverwrite;
        if (blindAppend)
        {
            // The claim is the CALLER's, not ours. This branch used to
            // hardcode `isBlindAppend: true`, reasoning that "a plain append takes its rows from the caller
            // and reads no file of this table to decide what to write". That is true of what THIS library
            // does and false of what the field means: Delta's `isBlindAppend` describes the TRANSACTION
            // (`readPredicates.isEmpty && readFiles.isEmpty`), and a host with its own data plane that
            // scanned the table and handed us the resulting rows has made a read we never saw.
            // DeltaTransaction.IsBlindAppend says exactly this one file away — "This library cannot derive
            // it for a host with its own data plane" — and WriteAsync IS that host-facing surface.
            //
            // MEASURED against fabricator: an autocommit `INSERT INTO t SELECT max(id)+1 FROM t` — the
            // anti-join incremental shape, which reads the target and emits nothing but adds — arrives here
            // as a plain Append and recorded `isBlindAppend: true`. That is the UNSAFE direction and the
            // exact commit shape the interop tier singled out as the one the flag alone can tell apart:
            // another engine then SKIPS the concurrentAppend check it owed. Null (absent) costs only
            // spurious conflicts, which is what this recorded before the flag existed.
            //
            // And the claim governs the RETRY, not only the record. CommitOccAsync rebases a collision
            // onto the newer version and re-commits the staged actions verbatim, "valid precisely
            // because nothing the commit read or removed was touched" — a precondition a caller
            // declaring `false` has just told us does not hold. Rebasing anyway would re-commit rows
            // computed from a snapshot that moved: for `INSERT INTO t SELECT max(id) + 1 FROM t`, the
            // old max, with no error raised. So a declared-false append is not rebase-safe, and a
            // collision surfaces as RebaseUnsafe/Replan — recompute and try again — which is the same
            // treatment the overwrite family below gets for the same reason.
            //
            // Null keeps the rebase. It means "the caller said nothing", not "the caller read
            // something", and #125 chose to read absence permissively rather than make every silent
            // caller pay conflicts.
            //
            // ReadSet.Blind stays as it is: with the rebase disabled, any collision aborts either way,
            // so the read set no longer gates safety — and we know the caller read SOMETHING, not what,
            // so claiming WholeTable would be inventing detail we do not have.
            committedVersion = await CommitOccAsync(
                snapshot, actions, ReadSet.Blind, NoRemovedPaths,
                IsolationLevel.WriteSerializable, "WRITE", rebaseSafe: isBlindAppend != false,
                cancellationToken, written: written, isBlindAppend: isBlindAppend).ConfigureAwait(false);
        }
        else
        {
            // Overwrite family: a single atomic attempt at the read version + 1 (unchanged behavior).
            // Declared FALSE: it reads the active-file set to decide what to remove, so it plainly depends
            // on files. Recording that is what lets another engine's checker examine it rather than fall
            // back to a default that happens to agree.
            var finalActions = Log.InCommitTimestamp.EnsureCommitInfo(
                actions, snapshot.Metadata.Configuration, "WRITE", isBlindAppend: false);
            await _log.WriteCommitAsync(newVersion, finalActions, cancellationToken)
                .ConfigureAwait(false);
            // Durable: these files are the table's now, whatever the refresh below does. Same reasoning as
            // the OCC loop's own clear — see CommitOccAsync.
            written?.Clear();
            _currentSnapshot = await SnapshotBuilder.UpdateAsync(
                snapshot, _log, cancellationToken).ConfigureAwait(false);
            committedVersion = newVersion;
        }

        // Auto-checkpoint on the version that actually committed (a rebased append may differ from the
        // read version + 1). Skipped when nothing was staged (an all-empty append returns the read
        // version without committing).
        //
        // The OVERWRITE family only: a blind append goes through the OCC loop above, which checkpoints on
        // the interval itself. Running both would write the interval's checkpoint TWICE — harmless
        // duplicated work under the classic form, which overwrites one fixed path, but a V2 checkpoint is
        // UUID-named, so the second write leaves the first behind as a file nothing references and nothing
        // collects (VacuumExecutor excludes _delta_log).
        if (!blindAppend && committedVersion > snapshot.Version)
            await CheckpointIfDueAsync(committedVersion, cancellationToken).ConfigureAwait(false);

        return committedVersion;
    }

    /// <summary>
    /// Writes the interval checkpoint for a version that has just committed, if that version is one the
    /// interval falls on. The commit paths that do NOT go through <see cref="LogCommitter"/> — the
    /// overwrite family, OPTIMIZE, and the metadata-only changes — each call this; the ones that do get
    /// the same check from the committer instead, and must not call it as well.
    /// </summary>
    /// <remarks>
    /// <para>Call only AFTER the post-commit snapshot refresh: the checkpoint is written from
    /// <see cref="CurrentSnapshot"/>, and is named for the version that snapshot is at. Under a concurrent
    /// writer that can be a LATER version than <paramref name="committedVersion"/> — which is fine, since
    /// what gets written is a real checkpoint of a real version either way, and it is the behaviour the
    /// batch write path has always had.</para>
    ///
    /// <para><c>CheckpointInterval = 0</c> is "never checkpoint", an absolute caller override — a host may
    /// be driving checkpoints on a cadence of its own and must not have one appear underneath it.</para>
    /// </remarks>
    private ValueTask CheckpointIfDueAsync(long committedVersion, CancellationToken cancellationToken)
    {
        if (_checkpointInterval <= 0
            || committedVersion % _checkpointInterval != 0)
        {
            return default;
        }

        return WriteCheckpointAndCleanUpLogAsync(CurrentSnapshot, cancellationToken);
    }

    /// <summary>
    /// Writes a checkpoint and then reclaims what it made redundant. The single place a checkpoint is
    /// written from this layer, so the two cannot come apart.
    /// </summary>
    /// <remarks>
    /// <para>The pairing is the point. Log cleanup deletes only what a checkpoint covers, so a checkpoint
    /// becoming durable is the one moment the work is both legitimate and worth doing — which means every
    /// checkpoint trigger owes a cleanup, and a trigger that writes one without the other quietly stops
    /// reclaiming on whichever commit paths reach it. There are two triggers left in this class
    /// (<see cref="CheckpointIfDueAsync"/> and <see cref="CheckpointAsync"/>) and one in
    /// <see cref="LogCommitter"/>; that is three chances to wire a checkpoint and forget the cleanup, and
    /// it has already happened once — the interval itself drifted the same way twice, which is why the
    /// tests assert cleanup per trigger rather than once.</para>
    ///
    /// <para>Ordering: the checkpoint must be DURABLE before anything is deleted, or a failure between the
    /// two leaves commits removed with nothing covering them.</para>
    /// </remarks>
    private async ValueTask WriteCheckpointAndCleanUpLogAsync(
        Snapshot.Snapshot snapshot, CancellationToken cancellationToken)
    {
        await _checkpointWriter.WriteCheckpointAsync(snapshot, cancellationToken).ConfigureAwait(false);

        await Log.LogCleanup.RunAsync(
            _log, snapshot.Metadata.Configuration, snapshot.Version,
            DateTimeOffset.UtcNow, cancellationToken).ConfigureAwait(false);
    }

    // ── Buffered-transaction seam ──────────────────────────────────────────────────────────────────────
    //
    // WriteDataFilesAsync writes append-shaped data files WITHOUT committing (invisible until
    // referenced); CommitDataFilesAsync commits those files — optionally FUSED with a caller's extraActions
    // (DML deletion-vector remove/add pairs, a schema metaData change) — into ONE atomic Delta version. The
    // pair lets a host (or a multi-statement transaction) build a commit incrementally, then flush it atomically.
    // DiscardDataFilesAsync is the third verb: reclaim the bytes of a write the host has decided not to commit.

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
    ///
    /// <para>A table declaring CHECK constraints, invariants or generated columns also reports false, because
    /// this seam is handed finished files and has no rows to check them against. Such a table is still
    /// committable, but only by a caller willing to say it enforced them itself —
    /// <c>constraintsEnforcedByCaller</c> on <see cref="CommitDataFilesAsync"/>. The property answers the
    /// unqualified question, since a caller that acts on it before writing files is exactly the caller a
    /// half-true answer would leave holding orphans.</para>
    /// </summary>
    public bool SupportsExternalDataFileCommit =>
        !RequiresOwnWriterForPerRowProcessing && !WriteTimeExpressions.Declares(CurrentSnapshot);

    /// <summary>
    /// Whether the table needs engineered-wood's own writer for per-row work an outside writer cannot do.
    /// </summary>
    /// <remarks>
    /// Narrower than <see cref="SupportsExternalDataFileCommit"/> on purpose: identity values and IcebergCompat
    /// cannot be supplied by a caller's assertion, while constraint enforcement can.
    /// </remarks>
    private bool RequiresOwnWriterForPerRowProcessing
    {
        get
        {
            if (IsIcebergCompat)
                return true;
            foreach (var f in CurrentSnapshot.Schema.Fields)
            {
                if (IdentityColumn.GetConfig(f) is not null)
                    return true;
            }
            return false;
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
    /// The name the caller-supplied materialized row ids travel under while they ride the partition split.
    /// Never written: the column is stripped before the batch is renamed, and the ids go back on under the
    /// table's DECLARED materialized column name. A name no Delta schema can collide with (a table column
    /// would have to be called this literally).
    /// </summary>
    private const string RowIdRideAlongColumn = "__engineered_wood_materialized_row_id_ridealong";

    /// <summary>
    /// Refuses a batch carrying a top-level column the write schema does not declare. Every write path drops
    /// through <see cref="ColumnMappingRecursive.ToPhysical"/>, which passes an unmatched column through
    /// untouched, and the parquet writer then writes whatever columns the batch has — so an undeclared column
    /// becomes a real column of the data file. A Delta reader projects by the table schema and never surfaces
    /// it, which is what makes this worth refusing: it costs bytes in every file written, forever, with
    /// nothing anywhere reporting that it is there.
    ///
    /// <para>The motivating case is a host's own copy-on-write rewrite. The identity to preserve arrives as a
    /// metadata COLUMN of the read (<see cref="DeltaRowMetadata.RowTracking"/>), so forwarding the read's
    /// batch as a post-image — the obvious thing to write — buries <c>_metadata.row_id</c> in the data file.
    /// That name gets its own sentence in the message, because "you have an extra column" is a much worse
    /// hint than "that is the read's, not yours".</para>
    ///
    /// <para>Accepts a PHYSICAL name wherever the logical one would do, matching
    /// <c>ColumnMappingRecursive</c>'s own tolerance — a batch read out of a data file and handed straight
    /// back is legal input, and the guard must never refuse what the rename would have accepted. Top level
    /// only: a stray nested field is a narrower mistake and not the one measured here.</para>
    /// </summary>
    private static void ThrowIfUndeclaredColumns(
        IReadOnlyList<RecordBatch> batches, Schema.StructType writeSchema, string entryPoint)
    {
        foreach (var batch in batches)
        {
            foreach (var field in batch.Schema.FieldsList)
            {
                if (DeclaresColumn(writeSchema, field.Name))
                    continue;

                bool looksLikeReadMetadata =
                    field.Name.StartsWith(DeltaMetadataColumns.DefaultPrefix, StringComparison.Ordinal)
                    || string.Equals(field.Name, TransientRowAddress.ColumnName, StringComparison.Ordinal);

                throw new ArgumentException(
                    $"{entryPoint}: the batch has a column '{field.Name}' that the table does not declare. "
                    + "It would be written into the data file as a column of its own, where a Delta reader — "
                    + "projecting by the table schema — would never show it: silent bytes in every file. "
                    + (looksLikeReadMetadata
                        ? "This is a READ's metadata column, not one of yours; build the batch to the table's "
                          + "schema rather than forwarding a read's batch. "
                        : "Drop it from the batch, or ALTER the table to declare it. ")
                    + "The table declares: "
                    + string.Join(", ", writeSchema.Fields.Select(f => "'" + f.Name + "'")) + ".",
                    nameof(batches));
            }
        }
    }

    /// <summary>True when <paramref name="arrowName"/> names a top-level field of <paramref name="schema"/>,
    /// by its logical name or by its column-mapping physical name — the same either-name rule
    /// <c>ColumnMappingRecursive.FindField</c> applies when renaming.</summary>
    private static bool DeclaresColumn(Schema.StructType schema, string arrowName)
    {
        foreach (var f in schema.Fields)
        {
            if (string.Equals(f.Name, arrowName, StringComparison.Ordinal))
                return true;
            if (f.Metadata is { } md
                && md.TryGetValue(ColumnMapping.PhysicalNameKey, out var physical)
                && string.Equals(physical, arrowName, StringComparison.Ordinal))
            {
                return true;
            }
        }
        return false;
    }

    /// <summary>Returns the batch without <paramref name="name"/>, or unchanged if it has no such column.</summary>
    private static RecordBatch DropColumn(RecordBatch batch, string name)
    {
        var columns = new List<IArrowArray>(batch.ColumnCount);
        var schema = new Apache.Arrow.Schema.Builder();
        for (int i = 0; i < batch.ColumnCount; i++)
        {
            if (string.Equals(batch.Schema.FieldsList[i].Name, name, StringComparison.Ordinal))
                continue;
            columns.Add(batch.Column(i));
            schema.Field(batch.Schema.FieldsList[i]);
        }
        return columns.Count == batch.ColumnCount
            ? batch
            : new RecordBatch(schema.Build(), columns, batch.Length);
    }

    /// <summary>
    /// Writes <paramref name="batches"/> to append-shaped parquet data files WITHOUT committing, returning the
    /// descriptors to hand to <see cref="CommitDataFilesAsync"/>. Partition split, recursive column-mapping
    /// physical rename + field-id stamping, the variant logical-type policy, the <see cref="IDataFileWriter"/>
    /// seam and per-file stats all apply; row-tracking <c>baseRowId</c> is NOT materialized into the files (the
    /// commit assigns it, exactly like the streaming writer). Identity columns and IcebergCompat need write-time
    /// per-row processing tied to the commit — callers must check <see cref="SupportsExternalDataFileCommit"/>
    /// first (or pass <paramref name="identityValuesPreGenerated"/> for a table whose identity values were
    /// generated up front via <c>GenerateIdentityValues</c>). The written files are invisible until committed:
    /// not referencing them IS the rollback, and it is atomic and free. To reclaim the BYTES of a write that
    /// will never be committed, call <see cref="DiscardDataFilesAsync"/> — otherwise they wait for VACUUM.
    ///
    /// <para>A batch carrying a column the write schema does not declare is REFUSED, naming it: the file would
    /// carry the column while every Delta read projected it away. Fewer columns than the table is still legal
    /// (an absent column reads as null). See <see cref="ThrowIfUndeclaredColumns"/>.</para>
    /// </summary>
    /// <param name="schemaOverride">A buffered transaction's PENDING (ALTERed) schema — the batches carry columns
    /// the committed snapshot doesn't know yet; the pending schema (whose added columns already carry their
    /// column-mapping ids / physical names) drives the physical rename + stats keying, and the paired commit
    /// includes the matching metaData action.</param>
    /// <param name="materializedRowIds">The rows' ORIGINAL stable row ids, flat and aligned with
    /// <paramref name="batches"/> (one entry per row, batches concatenated in order) — for a host's own
    /// copy-on-write rewrite, an UPDATE's post-images most of all, where the rows are MOVED to a new file and
    /// so can no longer be identified by <c>baseRowId + position</c>. Each id is written into the table's
    /// declared materialized row-id column, which a spec reader honors over the add's <c>baseRowId</c>, so a
    /// row keeps its identity across the rewrite. A null entry leaves that row to the default (a genuinely new
    /// row in a mixed batch). The commit VERSION is deliberately not materialized: it should advance to the
    /// rewriting commit, which is exactly what the add's <c>defaultRowCommitVersion</c> already says.
    /// <para>Requires the table to declare <c>delta.rowTracking.materializedRowIdColumnName</c>. The ids ride
    /// the partition split with their rows, and are kept out of the physical rename and the statistics.</para>
    /// </param>
    public async ValueTask<IReadOnlyList<WrittenDataFile>> WriteDataFilesAsync(
        IReadOnlyList<RecordBatch> batches,
        CancellationToken cancellationToken = default,
        Schema.StructType? schemaOverride = null,
        bool identityValuesPreGenerated = false,
        IReadOnlyList<long?>? materializedRowIds = null)
    {
        ThrowIfDisposed();
        ProtocolVersions.ValidateWriteSupport(CurrentSnapshot.Protocol);
        // Same timestamp-unit rule as the committing write path; this entry point bypasses it.
        foreach (var b in batches)
            SchemaConverter.ThrowIfUnsupportedTimestampUnit(b.Schema);
        if (IsIcebergCompat)
            throw new NotSupportedException(
                "WriteDataFilesAsync: IcebergCompat tables require the committing write path.");
        if (HasIdentityColumns && !identityValuesPreGenerated)
            throw new NotSupportedException(
                "WriteDataFilesAsync: table has identity columns — generate their values first "
                + "(GenerateIdentityValues) and pass identityValuesPreGenerated, or use the committing write path.");

        var snapshot = CurrentSnapshot;
        var writeSchema = schemaOverride ?? snapshot.Schema;
        ThrowIfUndeclaredColumns(batches, writeSchema, nameof(WriteDataFilesAsync));
        var partitionColumns = snapshot.Metadata.PartitionColumns;
        var mappingMode = ColumnMapping.GetMode(snapshot.Metadata.Configuration);
        var logicalToPhysical = ColumnMapping.BuildLogicalToPhysicalMap(writeSchema, mappingMode);
        var files = new List<WrittenDataFile>();

        string? matRowIdName = null;
        if (materializedRowIds is not null)
        {
            matRowIdName = DeltaLake.RowTracking.RowTrackingConfig
                .TryGetMaterializedColumnNames(snapshot.Metadata.Configuration).RowIdColumnName;
            if (matRowIdName is null)
            {
                throw new InvalidOperationException(
                    "materializedRowIds: the table does not declare "
                    + "'delta.rowTracking.materializedRowIdColumnName', so there is no column to write the ids "
                    + "into and a spec reader would derive them from baseRowId + position instead. Enable row "
                    + "tracking at create time.");
            }
            long totalRows = 0;
            foreach (var b in batches)
                totalRows += b.Length;
            if (materializedRowIds.Count != totalRows)
            {
                throw new ArgumentException(
                    "materializedRowIds must carry one entry per row across all batches: got "
                    + $"{materializedRowIds.Count} for {totalRows} rows.", nameof(materializedRowIds));
            }
        }

        int rowIdOffset = 0;
        foreach (var batch in batches)
        {
            if (batch.Length == 0)
                continue;

            // Carry the ids as an ordinary column THROUGH the partition split, so each row keeps its own id
            // across the regrouping — the split gathers rows by partition, and a flat side-list would no
            // longer line up. Stripped again below, before anything that reads the batch as table data.
            var splitInput = batch;
            if (materializedRowIds is not null)
            {
                var rideAlong = new Int64Array.Builder();
                for (int i = 0; i < batch.Length; i++)
                {
                    long? id = materializedRowIds[rowIdOffset + i];
                    if (id is null)
                        rideAlong.AppendNull();
                    else
                        rideAlong.Append(id.Value);
                }
                splitInput = RowTracking.RowTrackingWriter.AddRowIdColumn(
                    batch, rideAlong.Build(), RowIdRideAlongColumn, nullable: true);
            }
            rowIdOffset += batch.Length;

            var partitions = Partitioning.PartitionUtils.SplitByPartition(splitInput, partitionColumns);
            foreach (var (partValues, splitBatch) in partitions)
            {
                if (splitBatch.Length == 0)
                    continue;

                var dataBatch = splitBatch;
                Int64Array? rideAlongIds = null;
                if (materializedRowIds is not null)
                {
                    rideAlongIds = (Int64Array)splitBatch.Column(RowIdRideAlongColumn);
                    dataBatch = DropColumn(splitBatch, RowIdRideAlongColumn);
                }

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

                string partDir = BuildPartitionPath(trackedPartValues);
                string fileName = string.IsNullOrEmpty(partDir)
                    ? $"{Guid.NewGuid():N}.parquet"
                    : $"{partDir}/{Guid.NewGuid():N}.parquet";

                // Same variant logical-type policy as the committing write path (Spark 4.0.x tables drop the
                // annotation; the read path recovers the type from the Delta schema).
                var writeBatch = _options.EmitVariantLogicalType
                    ? physicalBatch
                    : VariantColumnCoercion.StripAnnotation(physicalBatch);

                // The materialized ids go back on AFTER the physical rename — the column's name comes from
                // table metadata and is already physical, so passing it through the mapping would rename it.
                if (rideAlongIds is not null)
                {
                    writeBatch = RowTracking.RowTrackingWriter.AddRowIdColumn(
                        writeBatch, rideAlongIds, matRowIdName!, nullable: true);
                }

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
    /// Deletes files written for a commit that will NEVER be made — the explicit counterpart of
    /// <see cref="CommitDataFilesAsync"/>, for a host abandoning a buffered transaction.
    ///
    /// <para>Not committing IS the rollback, and always was: a file no version references changes nothing a
    /// reader can see. This is only about reclaiming the bytes. Without it the sole reclamation path is
    /// VACUUM, which waits out <c>delta.deletedFileRetentionDuration</c> — so a host that KNOWS immediately
    /// (a validation failure downstream, a user cancelling a multi-statement transaction, a crash-loop that
    /// re-writes the batch on every restart) had no way to say so, though it holds the paths.</para>
    ///
    /// <para>Best-effort and quiet, like <see cref="DeltaTransaction.AbortAsync"/>: a delete that fails is
    /// swallowed rather than allowed to mask whatever prompted the discard, and a file already gone is not an
    /// error. Ignoring this method entirely stays valid — VACUUM still collects what it always did.</para>
    ///
    /// <para><b>The library cannot infer abandonment here</b>, which is why this is a verb rather than
    /// automatic. <see cref="WriteDataFilesAsync"/> hands back a plain list and keeps no handle, deliberately:
    /// the files are meant to outlive the call and may be committed by a later, unrelated one. Only the host
    /// knows the commit is not coming.</para>
    /// </summary>
    /// <param name="files">The files to delete, as <see cref="WriteDataFilesAsync"/> returned them (or as the
    /// host's own writer describes what it wrote). Only <see cref="WrittenDataFile.RelativePath"/> is read.</param>
    /// <exception cref="ArgumentException">One of the files is REFERENCED by the table's current version — it
    /// is live data, and deleting it would leave an <c>add</c> pointing at nothing. Checked against a freshly
    /// read version, not this handle's cached one, because the commit that referenced them may have come from
    /// another handle. Nothing is deleted when this throws: validate-then-apply, so a list containing one
    /// committed file does not half-delete the rest.</exception>
    public async ValueTask DiscardDataFilesAsync(
        IReadOnlyList<WrittenDataFile> files, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        if (files is null)
            throw new ArgumentNullException(nameof(files));
        if (files.Count == 0)
            return;

        // The guard this method exists to be safe without. Every other cleanup path in the library deletes by
        // PROVENANCE — it deletes what its own writers just created — but here the caller supplies the list,
        // and a host that passes a committed file would destroy live data with one call. So the paths are
        // checked against the log instead. Read fresh rather than trusting CurrentSnapshot: these files are
        // uncommitted precisely until someone commits them, and that someone may be another handle.
        var latest = await SnapshotBuilder.UpdateAsync(CurrentSnapshot, _log, cancellationToken)
            .ConfigureAwait(false);
        // Deliberately NOT assigned to _currentSnapshot: discarding files is not a reason to move a snapshot
        // the caller may be planning against.
        var active = new HashSet<string>(StringComparer.Ordinal);
        foreach (var add in latest.ActiveFiles.Values)
            active.Add(EngineeredWood.DeltaLake.DeltaPath.Decode(add.Path)); // add.path is URL-encoded

        var ledger = new WrittenFileLedger();
        foreach (var f in files)
        {
            if (active.Contains(f.RelativePath))
            {
                throw new ArgumentException(
                    $"'{f.RelativePath}' is an active file at version {latest.Version} — it has been "
                    + "committed, so it is the table's data and not a discardable buffered write. Deleting it "
                    + "would leave an add action naming a file that does not exist. Nothing was deleted.",
                    nameof(files));
            }
            // WrittenDataFile.RelativePath is the DECODED on-disk path (add.path is the encoded form of it),
            // which is exactly what the filesystem takes — so it is recorded as-is, not via RecordEncoded.
            ledger.Record(f.RelativePath);
        }

        await DeleteWrittenFilesAsync(ledger, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Commits externally-written <paramref name="files"/> to the Delta log — optionally FUSED with
    /// <paramref name="extraActions"/> (a buffered transaction's deletion-vector remove/add pairs, or a schema
    /// metaData change) — as ONE atomic version. Append-shaped by default (<paramref name="mode"/>
    /// <see cref="DeltaWriteMode.Append"/>); a full <see cref="DeltaWriteMode.Overwrite"/> removes every active
    /// file, and <paramref name="dynamicPartitionOverwrite"/> removes only the active files in partitions the
    /// written files touch. Row-tracking <c>baseRowId</c> / <c>defaultRowCommitVersion</c> + the high-water-mark
    /// domain are assigned here.
    ///
    /// <para>A call that would commit NOTHING — no files, no <paramref name="extraActions"/>, and no removes
    /// to derive — commits nothing and returns the current version rather than minting an empty one. Same
    /// rule as every other commit path here.</para>
    /// </summary>
    /// <returns>The version the commit landed at, which after rebasing past a concurrent commit is not
    /// necessarily the read version + 1.</returns>
    /// <param name="expectedVersion">When set, the commit ABORTS (first-committer-wins) if the table has moved off
    /// this version — the caller's snapshot-coupled <paramref name="extraActions"/> (deletion-vector ordinals /
    /// positions computed against it) would be invalidated by a concurrent commit. When null, an append rebases
    /// past a non-conflicting concurrent commit (bounded retry), reusing the already-written files as-is.</param>
    /// <param name="constraintsEnforcedByCaller">
    /// Declares that the caller has already enforced the table's CHECK constraints, invariants and generated
    /// columns over the rows in <paramref name="files"/>. This seam is handed finished files, so nothing here
    /// can check them — the flag is an assertion, and a false one commits rows every later reader will trust,
    /// which is why it is spelled as a claim rather than as a way to skip validation. Left false, a table
    /// declaring any of those is refused, and <see cref="SupportsExternalDataFileCommit"/> reports false for it.
    /// </param>
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
        string? clusteringProvider = null,
        bool? isBlindAppend = null,
        bool constraintsEnforcedByCaller = false)
    {
        ThrowIfDisposed();
        ProtocolVersions.ValidateWriteSupport(CurrentSnapshot.Protocol);
        // A dynamic partition overwrite removes files, so it is NOT an append for appendOnly enforcement.
        // extraActions (a buffered transaction's deletion-vector remove/add pairs) likewise make this a
        // non-append. A dataChange=false rewrite (compaction) is append-LEGAL: appendOnly forbids removing
        // ROWS, not reorganizing files.
        bool appendShaped = (mode == DeltaWriteMode.Append && !dynamicPartitionOverwrite &&
                             extraActions is not { Count: > 0 }) || !dataChange;
        // This seam receives finished files, so there are no rows to check. The caller can still
        // commit a constrained table by declaring it enforced the rules itself — an assertion,
        // deliberately spelled as one, because nothing here can verify it and a wrong claim
        // poisons the table for every later reader.
        HonorWriterFeatures(
            CurrentSnapshot,
            appendShaped,
            constraintsEnforcedByCaller
                ? WriteTimeExpressionHandling.AssertedByCaller
                : WriteTimeExpressionHandling.Refuse);

        if (dynamicPartitionOverwrite)
        {
            if (mode != DeltaWriteMode.Append)
                throw new DeltaFormatException(
                    DeltaTableErrorCodes.InvalidWriteMode,
                    "Dynamic partition overwrite is append-shaped (a full Overwrite already removes everything).");
            if (CurrentSnapshot.Metadata.PartitionColumns.Count == 0)
                throw new DeltaFormatException(
                    DeltaTableErrorCodes.InvalidWriteMode,
                    "Dynamic partition overwrite requires a partitioned table (the table has no partition columns).");
        }

        // Reject configurations that require write-time per-row processing the external writer did not do (the
        // caller should have checked SupportsExternalDataFileCommit first). Only relevant when data FILES are
        // being committed — a deletion-vector-only or metadata-only fused flush (extraActions, no files) involves
        // no write-time processing.
        var cfg = CurrentSnapshot.Metadata.Configuration;
        if (files.Count > 0 && RequiresOwnWriterForPerRowProcessing
            && !(identityValuesPreGenerated && !IsIcebergCompat))
            throw new NotSupportedException(
                "CommitDataFilesAsync: table has identity columns or IcebergCompat — "
                + "these require engineered-wood's own writer.");

        bool rowTrackingEnabled = DeltaLake.RowTracking.RowTrackingConfig.IsEnabled(cfg);

        // Buffered-transaction commit: the caller's extraActions are snapshot-coupled (deletion-vector
        // ordinals/positions computed against expectedVersion), so a concurrent commit invalidates them —
        // conflict-ABORT instead of the append retry (first-committer-wins snapshot isolation).
        if (expectedVersion is { } expected && CurrentSnapshot.Version != expected)
        {
            throw new DeltaConflictException(
                DeltaTableErrorCodes.StaleTransactionSnapshot,
                $"Transaction conflict: the table moved from version {expected} to {CurrentSnapshot.Version} "
                + "while the transaction was open — the buffered changes were rolled back; retry the "
                + "transaction.");
        }

        var baseSnapshot = CurrentSnapshot;
        var result = await _committer.CommitAsync(
            new LogCommitRequest
            {
                BaseSnapshot = baseSnapshot,
                Actions = await BuildActionsAsync(baseSnapshot, cancellationToken).ConfigureAwait(false),
                Operation = operation,
                // The actions are a FUNCTION of the snapshot — an Overwrite's removes name its active set,
                // and a row-tracking baseRowId is drawn from its high-water mark — so a collision re-derives
                // them against the version that landed instead of re-committing a stale set. The data files
                // themselves are reused as-is: they were written before any of this and are still fine.
                Rebase = new RecomputeRebaseHandler(BuildActionsAsync),
                // A snapshot-coupled commit gets ONE attempt: its extraActions were computed against
                // expectedVersion, so there is no version but that one they are correct at. The collision
                // then propagates unexamined, which is the answer the caller is waiting for.
                MaxAttempts = expectedVersion is null ? 16 : 1,
                // Reads nothing and (as far as the checker is concerned) removes nothing: the recompute above
                // re-derives the Overwrite removes from the newest active set, so there is no stale remove
                // for the delete/delete rule to catch. What the checker DOES contribute here is the
                // metadata/protocol rule — this path used to retry straight through a concurrent schema
                // change and commit files against a schema that had moved, while the Arrow append path
                // (CommitWriteAsync, same blind read-set) aborted. They now agree.
                //
                // NOTE: if a host turns out to depend on the old permissiveness — a producer appending
                // through this while another process edits table properties will now see conflicts it did
                // not before — the fix is a public opt-out on the request rather than a quiet revert here.
                // That reopens a real hole, so it should be asked for rather than offered.
                Reads = ReadSet.Blind,
                // The caller's own claim about what it read, passed through verbatim. ⚠ NOT derived from
                // Reads above: that is hardcoded Blind here because this method has no way to know, which
                // is precisely why the claim has to come from the caller. See LogCommitRequest.IsBlindAppend.
                IsBlindAppend = isBlindAppend,
            },
            cancellationToken).ConfigureAwait(false);

        _currentSnapshot = result.Snapshot;
        return result.Version;

        // The commit's actions, derived from whichever version they are about to land on. Called once up
        // front and again per collision — see RecomputeRebaseHandler for what that demands of it.
        async ValueTask<IReadOnlyList<DeltaAction>> BuildActionsAsync(
            Snapshot.Snapshot snapshot, CancellationToken ct)
        {
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
                        BaseRowId = existingFile.BaseRowId,
                        DefaultRowCommitVersion = existingFile.DefaultRowCommitVersion,
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
                        BaseRowId = existingFile.BaseRowId,
                        DefaultRowCommitVersion = existingFile.DefaultRowCommitVersion,
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
                    dv = await dvWriter.CreateAsync(deletedPositions, deletedPositions.Count, ct)
                        .ConfigureAwait(false);
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

            return actions;
        }
    }

    /// <summary>
    /// Rewrites a stats JSON object with <c>tightBounds=false</c>. Applied wherever a deletion vector is
    /// ATTACHED: the file's min/max then describe rows the vector removed, so they are loose supersets
    /// rather than values still present. Absent means <c>true</c> per the spec, so leaving it off asserts
    /// bounds that the vector has just invalidated — harmless for skipping, which only needs a superset,
    /// but wrong for a reader answering MIN/MAX/COUNT from statistics alone.
    /// </summary>
    /// <remarks>
    /// Only the flag needs writing. Delta additionally rewrites <c>nullCount</c> into its tri-state wide
    /// form, because its tight-state counts are LOGICAL; EW collects statistics over the physical rows
    /// when the file is written and never recomputes them, so an all-null column's count already equals
    /// the physical <c>numRecords</c> that the wide reading tests against.
    /// </remarks>
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

    // The transient rowid packs (path-sorted file ordinal, absolute in-file position) — see
    // <see cref="TransientRowAddress"/>, which owns the encoding and the public pack/unpack helpers.

    internal static List<Actions.AddFile> OrderedActiveFiles(Snapshot.Snapshot snapshot)
    {
        var files = new List<Actions.AddFile>(snapshot.ActiveFiles.Values);
        files.Sort((a, b) => string.CompareOrdinal(a.Path, b.Path));
        return files;
    }

    /// <summary>
    /// The active set keyed by <c>add.path</c> — what a <see cref="RowSelection"/> speaks.
    /// <see cref="Snapshot.Snapshot.ActiveFiles"/> is NOT this map: it is keyed by the spec's reconciliation
    /// key, <c>(path, deletionVector.uniqueId)</c>, so a file carrying a deletion vector is not found under
    /// its bare path. One path has at most one active entry (a DV swap removes the old key), so this is
    /// well-defined.
    /// </summary>
    internal static Dictionary<string, Actions.AddFile> ActiveFilesByPath(Snapshot.Snapshot snapshot)
    {
        var byPath = new Dictionary<string, Actions.AddFile>(
            snapshot.ActiveFiles.Count, StringComparer.Ordinal);
        foreach (var add in snapshot.ActiveFiles.Values)
            byPath[add.Path] = add;
        return byPath;
    }

    /// <summary>
    /// The preconditions for writing a <c>_change_data</c> file: the table must be writable at its protocol,
    /// have Change Data Feed on (a CDC file on a non-CDF table is dead weight no reader consults), and the
    /// change type must be one the spec defines. Shared by <see cref="WriteChangeDataFileAsync"/> and
    /// <see cref="DeltaTransaction.StageChangeDataAsync"/>.
    /// </summary>
    internal void ValidateChangeDataStageable(Snapshot.Snapshot snapshot, string changeType)
    {
        ProtocolVersions.ValidateWriteSupport(snapshot.Protocol);
        if (changeType is not (DeltaLake.ChangeDataFeed.CdfConfig.Insert
            or DeltaLake.ChangeDataFeed.CdfConfig.Delete
            or DeltaLake.ChangeDataFeed.CdfConfig.UpdatePreimage
            or DeltaLake.ChangeDataFeed.CdfConfig.UpdatePostimage))
        {
            throw new ArgumentException(
                $"changeType must be one of 'insert', 'delete', 'update_preimage', 'update_postimage' "
                + $"(got '{changeType}').", nameof(changeType));
        }
        if (!DeltaLake.ChangeDataFeed.CdfConfig.IsEnabled(snapshot.Metadata.Configuration))
        {
            throw new InvalidOperationException(
                "Change Data Feed is not enabled on this table — a _change_data file would never be read. "
                + "Create the table with the 'delta.enableChangeDataFeed' property set to 'true'.");
        }
    }

    /// <summary>
    /// Builds the <c>add</c> actions for files a host already wrote, against <paramref name="snapshot"/> and
    /// WITHOUT committing — the staging counterpart of <see cref="CommitDataFilesAsync"/>' per-file loop, for
    /// <see cref="DeltaTransaction.StageDataFiles"/>. Append-shaped only: the overwrite family removes the
    /// active set, which is exactly what a rebase cannot re-derive, so a transaction does not stage it.
    /// </summary>
    internal (IReadOnlyList<DeltaAction> Actions, long NextRowId) BuildStagedAppendActions(
        Snapshot.Snapshot snapshot, IReadOnlyList<WrittenDataFile> files, long? rowIdStart)
    {
        // No born-deleted rows means no deletion vector to write, so this path performs no I/O and the
        // ValueTask below is already completed — which is what lets the synchronous StageDataFiles keep its
        // signature while sharing one definition of what a staged add looks like.
        var pending = BuildStagedAppendActionsAsync(
            snapshot, files, bornDeleted: null, identityValuesPreGenerated: false, rowIdStart,
            CancellationToken.None);
        System.Diagnostics.Debug.Assert(pending.IsCompleted, "the no-DV staging path must not perform I/O");
        var (actions, nextRowId, _) = pending.GetAwaiter().GetResult();
        return (actions, nextRowId);
    }

    /// <summary>
    /// The staged-append action builder, with the two things <see cref="CommitDataFilesAsync"/> could express
    /// and the staged surface could not: rows deleted inside the same transaction that inserted them, and an
    /// identity table whose values the caller generated itself.
    /// </summary>
    /// <param name="bornDeleted">Rows of the not-yet-committed <paramref name="files"/> that this transaction
    /// deleted AFTER inserting them, keyed by <see cref="WrittenDataFile.RelativePath"/>. Each such add is born
    /// with an INLINE deletion vector, so the rows never appear in any committed version — and its stats are
    /// marked <c>tightBounds=false</c>, which the spec requires once a vector hides rows the bounds were
    /// computed over.</param>
    /// <param name="written">Records only the born-deleted vector's <c>.bin</c>, when one is written to a file
    /// at all. The DATA files are the HOST's — it wrote them and it decides their lifetime — so an abort of the
    /// transaction they were staged on must not delete them.</param>
    /// <returns>The actions, the next free stable row id, and the rows the commit will make VISIBLE
    /// (every file's <c>numRecords</c> less anything <paramref name="bornDeleted"/> hides).</returns>
    internal async ValueTask<(IReadOnlyList<DeltaAction> Actions, long NextRowId, long LiveRows)>
        BuildStagedAppendActionsAsync(
            Snapshot.Snapshot snapshot,
            IReadOnlyList<WrittenDataFile> files,
            RowSelection? bornDeleted,
            bool identityValuesPreGenerated,
            long? rowIdStart,
            CancellationToken cancellationToken,
            WrittenFileLedger? written = null)
    {
        // Mirrors CommitDataFilesAsync' gate exactly: identity columns need write-time per-row processing an
        // outside writer did not do, UNLESS the caller generated the values itself (GenerateIdentityValues) —
        // which is what makes an identity table's appends stageable at all. IcebergCompat has no such escape.
        if (files.Count > 0 && RequiresOwnWriterForPerRowProcessing
            && !(identityValuesPreGenerated && !IsIcebergCompat))
        {
            throw new NotSupportedException(
                "StageDataFiles: table has identity columns or IcebergCompat — these require engineered-wood's "
                + "own writer (check SupportsExternalDataFileCommit, generate the identity values yourself and "
                + "pass identityValuesPreGenerated, or stage via WriteAsync).");
        }

        var deletedByPath = ValidateBornDeleted(files, bornDeleted);

        bool rowTrackingEnabled = DeltaLake.RowTracking.RowTrackingConfig.IsEnabled(snapshot.Metadata.Configuration);
        long nextRowId = rowTrackingEnabled ? rowIdStart ?? snapshot.RowIdHighWaterMark : 0;
        long newVersion = snapshot.Version + 1;
        long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var actions = new List<DeltaAction>(files.Count + 1);
        long liveRows = 0;

        foreach (var f in files)
        {
            DeletionVector? dv = null;
            string? stats = f.StatsJson ?? $"{{\"numRecords\":{f.NumRecords}}}";
            long hidden = 0;
            if (deletedByPath is not null
                && deletedByPath.TryGetValue(f.RelativePath, out var positions) && positions.Count > 0)
            {
                var dvWriter = new DeletionVectors.DeletionVectorWriter(_fs);
                dv = await dvWriter.CreateAsync(positions, positions.Count, cancellationToken)
                    .ConfigureAwait(false);
                written?.RecordDeletionVector(dv);
                stats = StatsWithLooseBounds(stats);
                hidden = positions.Count;
            }
            liveRows += f.NumRecords - hidden;

            actions.Add(new AddFile
            {
                Path = DeltaPath.Encode(f.RelativePath),
                PartitionValues = f.PartitionValues ?? new Dictionary<string, string>(),
                Size = f.SizeBytes,
                ModificationTime = now,
                DataChange = true,
                Stats = stats,
                BaseRowId = rowTrackingEnabled ? nextRowId : null,
                DefaultRowCommitVersion = rowTrackingEnabled ? newVersion : null,
                DeletionVector = dv,
                Tags = f.Tags,
            });
            // The row-id range covers every PHYSICAL row, hidden ones included: a born-deleted row still
            // occupies its position in the file, so skipping it would misalign every id after it.
            if (rowTrackingEnabled)
                nextRowId += f.NumRecords;
        }

        if (rowTrackingEnabled && nextRowId > snapshot.RowIdHighWaterMark)
            actions.Add(DeltaLake.RowTracking.RowTrackingConfig.BuildHighWaterMarkAction(nextRowId));

        return (actions, nextRowId, liveRows);
    }

    /// <summary>
    /// Checks a <c>bornDeleted</c> selection against the files it is supposed to describe. The key only ever
    /// has to identify a file within the SAME call's <paramref name="files"/> list — these files are in no
    /// snapshot yet — so a path naming none of them, or a position past the file's own row count, is a caller
    /// error rather than something to resolve later and silently drop.
    /// </summary>
    private static Dictionary<string, IReadOnlyCollection<long>>? ValidateBornDeleted(
        IReadOnlyList<WrittenDataFile> files, RowSelection? bornDeleted)
    {
        if (bornDeleted is null || bornDeleted.IsEmpty)
            return null;

        var rowsByPath = new Dictionary<string, long>(StringComparer.Ordinal);
        foreach (var f in files)
            rowsByPath[f.RelativePath] = f.NumRecords;

        var result = new Dictionary<string, IReadOnlyCollection<long>>(StringComparer.Ordinal);
        foreach (var kvp in bornDeleted.Entries)
        {
            if (!rowsByPath.TryGetValue(kvp.Key, out long numRecords))
            {
                throw new ArgumentException(
                    $"bornDeleted names '{kvp.Key}', which is not among the files being staged. It keys by "
                    + "WrittenDataFile.RelativePath, and can only name a file in this same call — the rows are "
                    + "ones this transaction inserted and then deleted.",
                    nameof(bornDeleted));
            }
            foreach (long position in kvp.Value)
            {
                if (position >= numRecords)
                {
                    throw new ArgumentException(
                        $"bornDeleted names position {position} in '{kvp.Key}', which has only {numRecords} "
                        + "row(s).",
                        nameof(bornDeleted));
                }
            }
            result[kvp.Key] = kvp.Value;
        }
        return result;
    }

    /// <summary>
    /// Writes <paramref name="rows"/> as <c>_change_data</c> file(s) against <paramref name="snapshot"/>,
    /// splitting by partition per the data-file convention — each partition's rows in their own file, the
    /// partition columns OUT of the bytes, and the file's <c>partitionValues</c> physical-keyed. The
    /// partition-aware counterpart of <see cref="WriteChangeDataFileAsync"/>, which takes the values ready-made:
    /// producing them means encoding the Delta partition-value convention (null as JSON null rather than the
    /// <c>__HIVE_DEFAULT_PARTITION__</c> directory sentinel, dates and timestamps in the spec's formats), which
    /// no caller outside this assembly can do — <see cref="Partitioning.PartitionUtils"/> is internal.
    /// On an unpartitioned table this is one file, so a caller need not special-case it.
    /// </summary>
    internal async ValueTask<IReadOnlyList<CdcFile>> WriteChangeDataFilesForAsync(
        Snapshot.Snapshot snapshot, RecordBatch rows, string changeType, CancellationToken cancellationToken,
        Int64Array? rowIds = null, Int64Array? rowCommitVersions = null,
        WrittenFileLedger? written = null)
    {
        var partitionColumns = snapshot.Metadata.PartitionColumns;
        if (partitionColumns is not { Count: > 0 })
        {
            return
            [
                await ChangeDataFeed.CdfWriter.WriteAsync(
                    _fs, snapshot, rows, changeType, EmptyPartitionValues, _options.ParquetWriteOptions,
                    cancellationToken, rowIds, rowCommitVersions, written).ConfigureAwait(false),
            ];
        }

        var mappingMode = ColumnMapping.GetMode(snapshot.Metadata.Configuration);
        var logicalToPhysical = ColumnMapping.BuildLogicalToPhysicalMap(snapshot.Schema, mappingMode);
        var files = new List<CdcFile>();
        // The split REORDERS rows into per-partition groups, so any per-row array travelling with them has to
        // be gathered by the same source indices or it would describe the wrong rows.
        foreach (var (partValues, dataBatch, sourceRows) in
                 Partitioning.PartitionUtils.SplitByPartitionWithSourceRows(rows, partitionColumns))
        {
            if (dataBatch.Length == 0)
                continue;
            IReadOnlyDictionary<string, string> keyed = partValues;
            if (mappingMode != ColumnMappingMode.None && partValues.Count > 0)
            {
                var byPhysical = new Dictionary<string, string>(partValues.Count, StringComparer.Ordinal);
                foreach (var kv in partValues)
                    byPhysical[logicalToPhysical.TryGetValue(kv.Key, out var p) ? p : kv.Key] = kv.Value;
                keyed = byPhysical;
            }
            // CdfWriter strips the partition columns itself, but SplitByPartition already removed them; the
            // second removal is a no-op, so the batch arrives shaped exactly like a data file's.
            files.Add(await ChangeDataFeed.CdfWriter.WriteAsync(
                _fs, snapshot, dataBatch, changeType, keyed, _options.ParquetWriteOptions,
                cancellationToken,
                rowIds is not null ? TakeIds(rowIds, sourceRows) : null,
                rowCommitVersions is not null ? TakeIds(rowCommitVersions, sourceRows) : null,
                written)
                .ConfigureAwait(false));
        }
        return files;
    }

    /// <summary>
    /// Plans the scan for a predicate WITHOUT reading any data: returns the snapshot's active files that
    /// might contain matching rows, each with its ordinal in the path-sorted active set. This is the same
    /// superset-safe verdict the library's own read paths apply — a file is dropped only when its partition
    /// values or column statistics PROVE no row can match, and the surviving files are not row-filtered — so
    /// a host assembling its own scan (its own parquet reader behind
    /// <see cref="IDataFileReader"/>, an engine that pushes the predicate down itself) prunes identically to
    /// <see cref="ReadAllAsync(IReadOnlyList{string}, EngineeredWood.Expressions.Predicate, CancellationToken)"/>
    /// and must still apply the predicate per row.
    /// <para>
    /// The ordinal is what makes the result composable with the row-level seam: it addresses
    /// <see cref="ComputeDeletionVectorActionsAsync"/>, <see cref="RebaseDvDmlActionsAsync"/>,
    /// <see cref="CommitDataFilesAsync"/>, and the transient rowid encoding. See
    /// <see cref="PlannedFile.FileOrdinal"/> for its exact domain — notably that ordinals are assigned before
    /// pruning (the result is ascending but GAPPED) and are valid only against the snapshot planned from.
    /// </para>
    /// Deletion vectors are NOT resolved: a returned file's <see cref="Actions.AddFile.DeletionVector"/> is
    /// reported as-is and the caller decides how to exclude those positions (read them with
    /// <see cref="DeletionVectorReader"/>, or let its own engine do it).
    /// No I/O is performed — the snapshot is already materialized and statistics are read from it.
    /// </summary>
    /// <param name="filter">The predicate to prune by. Null (or a true predicate) keeps every active file,
    /// which is how a caller enumerates the addressing domain itself.</param>
    /// <param name="snapshot">The snapshot to plan against; defaults to <see cref="CurrentSnapshot"/>. Pass
    /// one explicitly when the ordinals must agree with a pinned version — a rewrite that lists against the
    /// same snapshot its commit pins as <c>expectedVersion</c> cannot be made to conflict by a writer landing
    /// between the two calls.</param>
    /// <param name="schemaOverride">The schema supplying the prune key map (column types and, under column
    /// mapping, logical→physical names); defaults to the snapshot's. Pass one to plan against a schema the
    /// snapshot does not have yet — an uncommitted transaction's pending ADD/RENAME COLUMN, where a predicate
    /// on the new name would otherwise resolve to nothing and prune nothing.
    /// <para>
    /// The CALLER owns this schema's correctness. An unknown column is safe (it evaluates Unknown, keeping
    /// the file), but a name mapped to the WRONG physical name reads another column's statistics and can
    /// prove <c>AlwaysFalse</c> for a file that does contain matching rows — silently dropping data. Supply
    /// only a schema derived from this table's own.
    /// </para></param>
    public IReadOnlyList<PlannedFile> PlanFiles(
        EngineeredWood.Expressions.Predicate? filter = null,
        Snapshot.Snapshot? snapshot = null,
        StructType? schemaOverride = null)
    {
        ThrowIfDisposed();
        var planSnapshot = snapshot ?? CurrentSnapshot;
        var ordered = OrderedActiveFiles(planSnapshot);
        var pruner = filter is null ? null : new DeltaFilePruner(
            schemaOverride ?? planSnapshot.Schema, planSnapshot.Metadata.PartitionColumns,
            _options.PreferTypedCheckpointStats);

        var planned = new List<PlannedFile>(ordered.Count);
        for (int i = 0; i < ordered.Count; i++)
        {
            // The ordinal is the loop index, NOT the survivor count: a pruned file consumes its position
            // because the addressing domain is the full active set, not the surviving subset. Renumbering
            // here would point every fed-back position at the wrong file — silently, since the DV/rowid
            // APIs cannot tell a stale ordinal from a fresh one.
            if (pruner is not null && !pruner.ShouldInclude(ordered[i], filter!))
                continue;
            planned.Add(new PlannedFile(i, ordered[i]));
        }
        return planned;
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
        // This is the LOWER-LAYER primitive — the surface for a host driving its own retry loop — and it keeps
        // its historical leniency: an ordinal outside the active set is dropped rather than reported. The DML
        // boundary (RowSelection) defaults to Throw instead.
        var selection = RowSelection.FromOrdinals(
            positionsByOrdinal, snapshot, StaleAddressPolicy.Skip, nameof(positionsByOrdinal));
        var result = await ComputeDvActionsWithEditsAsync(selection, snapshot, cancellationToken)
            .ConfigureAwait(false);
        return (result.Actions, result.RowsDeleted);
    }

    /// <summary>
    /// The body of <see cref="ComputeDeletionVectorActionsAsync"/>, keyed by <see cref="RowSelection"/> and
    /// additionally reporting the per-file <see cref="DeleteDvEdit"/>s and the touched paths. A
    /// <see cref="DeltaTransaction"/> needs those: the edits are what let the commit loop reconcile this delete
    /// row-by-row against a concurrent one rather than abort, and the paths are its read-set. The public wrapper
    /// drops them because a caller driving the rebase by hand passes its positions back to
    /// <see cref="RebaseDvDmlActionsAsync"/> instead.
    /// </summary>
    /// <param name="written">Records the vectors written here — and nothing else. Every <c>add</c> this
    /// produces re-adds an EXISTING data file under a new vector, so the parquet it names is live data.</param>
    internal async ValueTask<(IReadOnlyList<DeltaAction> Actions, IReadOnlyList<DeleteDvEdit> Edits,
        IReadOnlyList<string> TouchedPaths, long RowsDeleted)> ComputeDvActionsWithEditsAsync(
        RowSelection selection,
        Snapshot.Snapshot snapshot,
        CancellationToken cancellationToken,
        WrittenFileLedger? written = null)
    {
        var dvWriter = new DeletionVectors.DeletionVectorWriter(_fs);
        var byPath = ActiveFilesByPath(snapshot);
        var actions = new List<DeltaAction>();
        var edits = new List<DeleteDvEdit>();
        var touched = new List<string>();
        long totalDeleted = 0;

        foreach (var kvp in selection.Entries)
        {
            // A path is resolved against the snapshot's active set directly — there is no ordinal to be stale,
            // and a path the snapshot does not hold is reported rather than skipped.
            if (!byPath.TryGetValue(kvp.Key, out var addFile))
                throw StaleSelectionPath(kvp.Key, snapshot);

            var allDeleted = addFile.DeletionVector is not null
                ? new HashSet<long>(await _dvReader.ReadAsync(addFile.DeletionVector, cancellationToken)
                    .ConfigureAwait(false))
                : new HashSet<long>();

            long newlyDeleted = 0;
            var newRows = new List<long>();
            foreach (long p in kvp.Value)
            {
                if (allDeleted.Add(p))
                {
                    newlyDeleted++;
                    // Only the rows NEWLY hidden are this delete's intent — a position the file's existing
                    // vector already covered was deleted by an earlier commit, and replaying it as ours would
                    // make a concurrent writer's overlapping delete look like a row-level conflict.
                    newRows.Add(p);
                }
            }
            if (newlyDeleted == 0)
                continue;
            totalDeleted += newlyDeleted;
            edits.Add(new DeleteDvEdit(addFile.Path, newRows));
            touched.Add(addFile.Path);

            var newDv = await dvWriter.CreateAsync(allDeleted, allDeleted.Count, cancellationToken)
                .ConfigureAwait(false);
            written?.RecordDeletionVector(newDv);

            actions.Add(new RemoveFile
            {
                Path = addFile.Path,
                DeletionTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                DataChange = true,
                DeletionVector = addFile.DeletionVector,
                BaseRowId = addFile.BaseRowId,
                DefaultRowCommitVersion = addFile.DefaultRowCommitVersion,
            });
            actions.Add(addFile with
            {
                DeletionVector = newDv,
                DataChange = true,
                Stats = StatsWithLooseBounds(addFile.GetStatsJson()),
            });
        }

        return (actions, edits, touched, totalDeleted);
    }

    /// <summary>
    /// Deletes exactly the rows named by <paramref name="selection"/> — the row-level DELETE, in either of the
    /// two shapes Delta allows (see <see cref="RowDeleteMode"/>). Returns the rows actually removed and the
    /// committed version; a position a deletion vector already hid is not counted or replayed.
    ///
    /// <para>Build the selection against the snapshot its addresses came from —
    /// <see cref="RowSelection.FromRowAddresses"/> for a host whose rowid is one packed <c>BIGINT</c>,
    /// <see cref="RowSelection.FromLocatorColumns"/> straight from batches read with
    /// <see cref="DeltaRowMetadata.Locator"/>. A path the current snapshot no longer holds is reported
    /// rather than silently skipped.</para>
    ///
    /// <para>With <c>delta.enableChangeDataFeed</c> the removed rows are written as <c>delete</c> change files
    /// in both modes, so the feed reports exactly them. Row tracking is preserved.</para>
    /// </summary>
    /// <param name="rowLevelRetry"><see cref="RowDeleteMode.DeletionVector"/> only: on a conflict with a
    /// concurrent DV delete of the SAME file, re-union instead of aborting when the two touched DISJOINT rows
    /// (row-level concurrency, via <see cref="CommitOccAsync"/>'s row-level path). Meaningless — and rejected —
    /// for <see cref="RowDeleteMode.CopyOnWrite"/>, whose rewrite is not verbatim-rebase-safe.</param>
    public ValueTask<(long RowsDeleted, long Version)> DeleteRowsAsync(
        RowSelection selection,
        RowDeleteMode mode = RowDeleteMode.DeletionVector,
        bool rowLevelRetry = false,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        if (selection is null)
            throw new ArgumentNullException(nameof(selection));
        if (rowLevelRetry && mode != RowDeleteMode.DeletionVector)
            throw new ArgumentException(
                "rowLevelRetry applies only to RowDeleteMode.DeletionVector: a copy-on-write delete rewrites "
                + "its files, and a rewrite's fresh add cannot be replayed verbatim onto a newer version.",
                nameof(rowLevelRetry));

        // Both modes write files (vectors, change files, or a whole rewritten parquet) before attempting the
        // commit, and neither has a transaction the caller could abort — so the operation collects its own
        // output when the commit does not land. CopyOnWrite is rebaseSafe:false, which makes that ANY
        // concurrent commit rather than only a conflicting one.
        return mode switch
        {
            RowDeleteMode.DeletionVector => CollectOnFailureAsync(
                written => DeleteRowsViaVectorsAsync(
                    selection, rowLevelRetry, written, cancellationToken),
                cancellationToken),
            RowDeleteMode.CopyOnWrite => CollectOnFailureAsync(
                written => DeleteRowsCopyOnWriteAsync(selection, written, cancellationToken),
                cancellationToken),
            _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, "Unknown row delete mode."),
        };
    }

    /// <summary>
    /// <see cref="RowDeleteMode.DeletionVector"/>: each affected file's existing DV is unioned with the
    /// selected absolute positions and a fresh DV written; the commit is <c>remove</c>(old file+DV) +
    /// <c>add</c>(same file, new DV). The committing sibling of
    /// <see cref="ComputeDeletionVectorActionsAsync"/>.
    /// </summary>
    private async ValueTask<(long RowsDeleted, long Version)> DeleteRowsViaVectorsAsync(
        RowSelection selection,
        bool rowLevelRetry,
        WrittenFileLedger written,
        CancellationToken cancellationToken)
    {
        ProtocolVersions.ValidateWriteSupport(CurrentSnapshot.Protocol);

        var snapshot = CurrentSnapshot;
        if (selection.IsEmpty)
            return (0, snapshot.Version);

        HonorWriterFeatures(snapshot, isAppend: false);
        if (!DeletionVectors.DeletionVectorConfig.IsEnabled(snapshot.Metadata.Configuration))
            throw new InvalidOperationException(
                "RowDeleteMode.DeletionVector requires deletion vectors — create the table with "
                + "DeltaTable.CreateAsync(..., enableDeletionVectors: true), or pass "
                + "RowDeleteMode.CopyOnWrite.");

        var dvWriter = new DeletionVectors.DeletionVectorWriter(_fs);
        var byPath = ActiveFilesByPath(snapshot);
        bool cdfEnabled = DeltaLake.ChangeDataFeed.CdfConfig.IsEnabled(snapshot.Metadata.Configuration);
        var actions = new List<DeltaAction>();
        var removedPaths = new HashSet<string>(StringComparer.Ordinal);
        var dvEdits = new List<DeleteDvEdit>();
        long totalDeleted = 0;

        foreach (var kvp in selection.Entries)
        {
            if (!byPath.TryGetValue(kvp.Key, out var addFile))
                throw StaleSelectionPath(kvp.Key, snapshot);

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
            // The vector is ours; the file it masks is not — the re-add below names live table data.
            written.RecordDeletionVector(newDv);

            actions.Add(new RemoveFile
            {
                Path = addFile.Path,
                DeletionTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                DataChange = true,
                DeletionVector = addFile.DeletionVector,
                BaseRowId = addFile.BaseRowId,
                DefaultRowCommitVersion = addFile.DefaultRowCommitVersion,
            });
            actions.Add(addFile with
            {
                DeletionVector = newDv,
                DataChange = true,
                Stats = StatsWithLooseBounds(addFile.GetStatsJson()),
            });
            removedPaths.Add(addFile.Path);
            dvEdits.Add(new DeleteDvEdit(addFile.Path, newPositions));

            // Change Data Feed: a DV delete rewrites no data, so read the newly-deleted rows (matched by
            // ABSOLUTE position — the file's original DV survivors keep their absolute positions) and emit a
            // "delete" change file.
            if (cdfEnabled)
            {
                bool rowTracking = DeltaLake.RowTracking.RowTrackingConfig.IsEnabled(
                    snapshot.Metadata.Configuration);
                var newSet = new HashSet<long>(newPositions);
                var absOut = new List<Int64Array?>();
                // The deleted rows' stable identity, for the change file. ReadFileAsync resolves it (materialized
                // else baseRowId + position) on the same batches the positions come from, so they stay aligned.
                var idsOut = rowTracking ? new List<Int64Array?>() : null;
                var versOut = rowTracking ? new List<Int64Array?>() : null;
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
                    var delRows = new List<int>();
                    for (int i = 0; i < batch.Length; i++)
                        if (!absPos.IsNull(i) && newSet.Contains(absPos.GetValue(i)!.Value))
                            delRows.Add(i);
                    if (delRows.Count > 0)
                    {
                        var batchIds = idsOut is not null && bi < idsOut.Count ? idsOut[bi] : null;
                        var batchVers = versOut is not null && bi < versOut.Count ? versOut[bi] : null;
                        var cdc = await ChangeDataFeed.CdfWriter.WriteAsync(
                            _fs, snapshot, TakeRowsFromBatch(batch, delRows), DeltaLake.ChangeDataFeed.CdfConfig.Delete,
                            addFile.PartitionValues, _options.ParquetWriteOptions,
                            cancellationToken,
                            batchIds is not null ? TakeIds(batchIds, delRows) : null,
                            batchVers is not null ? TakeIds(batchVers, delRows) : null,
                            written).ConfigureAwait(false);
                        actions.Add(cdc);
                    }
                }
            }
        }

        if (actions.Count == 0)
            return (0, snapshot.Version);

        long version = await CommitOccAsync(
            snapshot, actions,
            new ReadSet { Files = removedPaths }, removedPaths,
            IsolationLevel.WriteSerializable, "DELETE", rebaseSafe: true, cancellationToken,
            rowLevelDeletes: rowLevelRetry ? dvEdits : null, written: written,
            isBlindAppend: false).ConfigureAwait(false);
        return (totalDeleted, version);
    }

    /// <summary>
    /// <see cref="RowDeleteMode.CopyOnWrite"/>: each affected file is rewritten without the selected rows and
    /// committed as plain <c>remove</c>/<c>add</c> — NO deletion vectors, NO row-tracking feature needed, so
    /// the result is maximally reader-compatible (Fabric OneLake, Spark, delta-kernel). Row tracking, when
    /// enabled, is preserved (survivors keep their materialized id + commit version). IcebergCompat is not yet
    /// supported on this path.
    /// </summary>
    private async ValueTask<(long RowsDeleted, long Version)> DeleteRowsCopyOnWriteAsync(
        RowSelection selection,
        WrittenFileLedger written,
        CancellationToken cancellationToken)
    {
        ProtocolVersions.ValidateWriteSupport(CurrentSnapshot.Protocol);

        var snapshot = CurrentSnapshot;
        if (selection.IsEmpty)
            return (0, snapshot.Version);

        HonorWriterFeatures(snapshot, isAppend: false);
        RejectCopyOnWriteRowIdUnsupported("copy-on-write DELETE");

        var mappingMode = ColumnMapping.GetMode(snapshot.Metadata.Configuration);
        bool rowTrackingEnabled = DeltaLake.RowTracking.RowTrackingConfig.IsEnabled(
            snapshot.Metadata.Configuration);
        var (matRowIdName, matRowVerName) = DeltaLake.RowTracking.RowTrackingConfig
            .TryGetMaterializedColumnNames(snapshot.Metadata.Configuration);
        bool materializeIds = rowTrackingEnabled && matRowIdName is not null && matRowVerName is not null;
        bool cdfEnabled = DeltaLake.ChangeDataFeed.CdfConfig.IsEnabled(snapshot.Metadata.Configuration);
        long newVersion = snapshot.Version + 1;
        long nextRowId = rowTrackingEnabled ? snapshot.RowIdHighWaterMark : 0;

        var actions = new List<DeltaAction>();
        var removedPaths = new HashSet<string>(StringComparer.Ordinal);
        long totalDeleted = 0;

        var byPath = ActiveFilesByPath(snapshot);
        foreach (var kvp in selection.Entries)
        {
            if (!byPath.TryGetValue(kvp.Key, out var addFile))
                throw StaleSelectionPath(kvp.Key, snapshot);
            var targets = RowSelection.AsSet(kvp.Value);

            // Read the file (logical), keeping only rows whose ABSOLUTE position is NOT targeted; materialize
            // each survivor's original id + version so the rewrite preserves row identity.
            var srcIds = materializeIds ? new List<Int64Array?>() : null;
            var srcVers = materializeIds ? new List<Int64Array?>() : null;
            var absOut = new List<Int64Array?>();
            var outputBatches = new List<RecordBatch>();
            var outTracking = materializeIds ? new List<(Int64Array Ids, Int64Array Vers)?>() : null;
            // Change Data Feed: the rows this delete removes, captured as they stream past (a copy-on-write
            // delete rewrites the file, so nothing on disk afterwards holds them).
            var deletedBatches = cdfEnabled ? new List<RecordBatch>() : null;
            // Paired 1:1 with deletedBatches: each deleted row's identity as it was before the rewrite.
            var deletedTracking = cdfEnabled ? new List<(Int64Array? Ids, Int64Array? Vers)>() : null;
            long deletedHere = 0;
            int bi = -1;
            await foreach (var batch in ReadFileAsync(addFile, null, snapshot, cancellationToken,
                                                      srcIds, srcVers, absOut).ConfigureAwait(false))
            {
                bi++;
                var absPos = bi < absOut.Count ? absOut[bi] : null;
                var keepRows = new List<int>();
                var delRows = cdfEnabled ? new List<int>() : null;
                for (int i = 0; i < batch.Length; i++)
                {
                    long abs = absPos is not null && i < absPos.Length && !absPos.IsNull(i)
                        ? absPos.GetValue(i)!.Value : i;
                    if (targets.Contains(abs))
                    {
                        deletedHere++;
                        delRows?.Add(i);
                    }
                    else
                    {
                        keepRows.Add(i);
                    }
                }
                var batchIds = srcIds is not null && bi < srcIds.Count ? srcIds[bi] : null;
                var batchVers = srcVers is not null && bi < srcVers.Count ? srcVers[bi] : null;

                if (delRows is { Count: > 0 })
                {
                    deletedBatches!.Add(TakeRowsFromBatch(batch, delRows));
                    deletedTracking!.Add((
                        batchIds is not null ? TakeIds(batchIds, delRows) : null,
                        batchVers is not null ? TakeIds(batchVers, delRows) : null));
                }
                if (keepRows.Count == 0)
                    continue;

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
                cancellationToken, written).ConfigureAwait(false);
            actions.Add(remove);
            removedPaths.Add(addFile.Path);
            if (add is not null)
            {
                actions.Add(add);
                if (rowTrackingEnabled)
                    nextRowId += addedRows;
            }

            // A "delete" change file per rewritten source file. Without it the reader would INFER the feed from
            // remove(old)+add(new) — reporting every surviving row as deleted and re-inserted; a version that
            // carries cdc actions is read cdc-only, so these rows are the whole truth of the change.
            if (deletedBatches is not null)
            {
                for (int b = 0; b < deletedBatches.Count; b++)
                {
                    var trk = deletedTracking![b];
                    actions.Add(await ChangeDataFeed.CdfWriter.WriteAsync(
                        _fs, snapshot, deletedBatches[b], DeltaLake.ChangeDataFeed.CdfConfig.Delete,
                        addFile.PartitionValues, _options.ParquetWriteOptions,
                        cancellationToken, trk.Ids, trk.Vers, written).ConfigureAwait(false));
                }
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
            new ReadSet { Files = removedPaths }, removedPaths,
            IsolationLevel.WriteSerializable, "DELETE", rebaseSafe: false, cancellationToken,
            written: written, isBlindAppend: false)
            .ConfigureAwait(false);
        return (totalDeleted, version);
    }

    /// <summary>
    /// A <see cref="RowSelection"/> naming a file the snapshot's active set does not hold. Unlike a stale
    /// ORDINAL, which cannot be told from a fresh one, a stale PATH is detectable — so it is reported instead
    /// of skipped, which is the whole point of keying the DML boundary by path.
    /// </summary>
    private static InvalidOperationException StaleSelectionPath(string path, Snapshot.Snapshot snapshot) =>
        new($"The row selection names '{path}', which is not an active file at version {snapshot.Version} — "
            + "a concurrent commit removed or rewrote it since the selection was built. Re-read the rows and "
            + "rebuild the selection, or stage the delete on a DeltaTransaction, whose commit loop reconciles "
            + "a concurrently-rewritten file for you.");

    // The corner the copy-on-write row-id DML path does not yet cover: IcebergCompat needs the committing
    // writer. (Change Data Feed IS captured — each rewritten file emits its own change files.)
    private void RejectCopyOnWriteRowIdUnsupported(string op)
    {
        if (IsIcebergCompat)
            throw new NotSupportedException($"{op} by row id is not supported on IcebergCompat tables.");
    }

    // Physical-writes the rewritten output batches for one file and returns the remove(old)+add(new) pair (Add is
    // null when every row was deleted → whole-file remove). Mirrors ComputeUpdateActionsAsync's write block;
    // shared by the copy-on-write DeleteRowsAsync + UpdateRowsAsync.
    private async ValueTask<(RemoveFile Remove, AddFile? Add, long AddedRows)> RewriteRowsToNewFileAsync(
        Snapshot.Snapshot snapshot, Actions.AddFile source, ColumnMappingMode mappingMode,
        IReadOnlyList<RecordBatch> outputBatches,
        IReadOnlyList<(Int64Array Ids, Int64Array Vers)?>? outTracking,
        bool materializeIds, string? matRowIdName, string? matRowVerName,
        bool rowTrackingEnabled, long baseRowId, long newVersion,
        CancellationToken cancellationToken,
        WrittenFileLedger? written = null)
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
            BaseRowId = source.BaseRowId,
            DefaultRowCommitVersion = source.DefaultRowCommitVersion,
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
        // Recorded before the write. The SOURCE file is deliberately not recorded: it stays the table's data
        // until the commit that replaces it lands.
        written?.Record(newFileName);

        // Read rows carry the partition columns the read path materializes; a data file never stores them (the
        // values live in add.partitionValues). Dropping them here keeps the rewrite's layout and statistics
        // identical to what the append path produces for the same rows.
        var dataBatches = new List<RecordBatch>(outputBatches.Count);
        foreach (var ob in outputBatches)
        {
            dataBatches.Add(Partitioning.PartitionUtils.RemovePartitionColumns(
                ob, snapshot.Metadata.PartitionColumns));
        }

        var writeBatches = new List<RecordBatch>(dataBatches.Count);
        for (int k = 0; k < dataBatches.Count; k++)
        {
            var physicalBatch = ColumnMappingRecursive.ToPhysical(dataBatches[k], snapshot.Schema, mappingMode);
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
            Stats = Stats.StatsCollector.Collect(dataBatches),
            BaseRowId = rowTrackingEnabled ? baseRowId : null,
            DefaultRowCommitVersion = rowTrackingEnabled ? newVersion : null,
        };
        return (remove, add, addedRows);
    }

    /// <summary>
    /// Per-file copy-on-write UPDATE of exactly the rows named by <paramref name="selection"/> — the companion
    /// to <see cref="DeleteRowsAsync"/> at <see cref="RowDeleteMode.CopyOnWrite"/>. Only files the selection
    /// names are rewritten: each such file's user batches are read (DV-filtered, in position order) and handed
    /// to <paramref name="rewriteFile"/> — which returns the SAME rows with the SET columns modified on the
    /// selected positions (the caller owns that typed logic; it MUST return one batch per source batch with
    /// identical row counts) — then re-written as plain <c>remove</c>+<c>add</c>. Row tracking is preserved (an
    /// UPDATED row's commit version advances to this commit; untouched rows keep theirs). With
    /// <c>delta.enableChangeDataFeed</c> each touched row's before/after values are written as
    /// <c>update_preimage</c>/<c>update_postimage</c> change files. IcebergCompat is not yet supported on this
    /// path. Returns the committed version (or the current version if nothing matched).
    /// </summary>
    /// <param name="rewriteFile">Receives <c>(path, sourceBatches, absolutePositionsPerBatch)</c>. The PATH is
    /// the file's <c>add.path</c> — the same key the selection is built on, so a host that computed its new
    /// values per file looks them up directly rather than resolving an ordinal that may have moved.
    /// <c>absolutePositionsPerBatch[b][i]</c> is the ABSOLUTE in-file position of row <c>i</c> of source batch
    /// <c>b</c>, so <c>(path, position)</c> is the same locator pair the selection and the read side speak, and
    /// a host keying its new values by it substitutes them by an O(1) lookup instead of re-matching on row
    /// content.</param>
    public ValueTask<long> UpdateRowsAsync(
        RowSelection selection,
        Func<string, IReadOnlyList<RecordBatch>, IReadOnlyList<Int64Array>, IReadOnlyList<RecordBatch>> rewriteFile,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        if (selection is null)
            throw new ArgumentNullException(nameof(selection));
        if (rewriteFile is null)
            throw new ArgumentNullException(nameof(rewriteFile));
        // The rewrite's output is written before the commit is attempted, and rebaseSafe:false means ANY
        // concurrent commit aborts it — so it collects its own files rather than orphaning a full rewrite.
        return CollectOnFailureAsync(
            written => UpdateRowsCoreAsync(selection, rewriteFile, written, cancellationToken),
            cancellationToken);
    }

    /// <summary>
    /// Copy-on-write UPDATE from a batch of new values — the convenience form for the "update from a host-side
    /// join" scenario, so the caller supplies no substitution code at all and no selection either.
    /// <paramref name="updates"/> carries one row per row to change: the LOCATOR PAIR
    /// (<c>{prefix}file_path</c>, <c>{prefix}row_index</c>) that a read with <c>DeltaRowMetadata.Locator</c>
    /// emits, plus one column per SET column, named by its LOGICAL table-column name and typed to match. For
    /// every source row the pair names, each SET column's value is replaced with the corresponding value from
    /// <paramref name="updates"/> (type-agnostic, via concat + take — no per-type code); all other columns and
    /// rows pass through. A duplicated locator is a caller error (last one wins). Returns the committed version
    /// (or the current version if nothing matched).
    /// </summary>
    public ValueTask<long> UpdateRowsAsync(
        RecordBatch updates,
        string metadataPrefix = RowSelection.DefaultMetadataPrefix,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        if (updates is null)
            throw new ArgumentNullException(nameof(updates));
        if (metadataPrefix is null)
            throw new ArgumentNullException(nameof(metadataPrefix));

        string pathName = metadataPrefix + RowSelection.FilePathColumnSuffix;
        string indexName = metadataPrefix + RowSelection.RowIndexColumnSuffix;
        int pathIdx = updates.Schema.GetFieldIndex(pathName);
        int indexIdx = updates.Schema.GetFieldIndex(indexName);
        if (pathIdx < 0 || indexIdx < 0)
            throw new ArgumentException(
                $"updates must carry the locator pair '{pathName}' (Utf8) and '{indexName}' (Int64) — read the "
                + "rows with DeltaReadOptions { Metadata = DeltaRowMetadata.Locator } and the same prefix.",
                nameof(updates));
        if (updates.Column(pathIdx) is not StringArray pathArray)
            throw new ArgumentException($"updates column '{pathName}' must be Utf8.", nameof(updates));
        if (updates.Column(indexIdx) is not Int64Array indexArray)
            throw new ArgumentException($"updates column '{indexName}' must be Int64.", nameof(updates));

        // (path, absolute position) → its row index in `updates`, and the SET columns (everything but the pair).
        var updIndexByLocator = new Dictionary<(string Path, long Position), int>(updates.Length);
        var positionsByPath = new Dictionary<string, IReadOnlyCollection<long>>(StringComparer.Ordinal);
        for (int i = 0; i < updates.Length; i++)
        {
            if (pathArray.IsNull(i) || indexArray.IsNull(i))
                continue;
            string path = pathArray.GetString(i);
            long position = indexArray.GetValue(i)!.Value;
            updIndexByLocator[(path, position)] = i;
            if (!positionsByPath.TryGetValue(path, out var set))
                positionsByPath[path] = set = new HashSet<long>();
            ((HashSet<long>)set).Add(position);
        }

        var setColumns = new List<(string Name, IArrowArray Values)>();
        for (int c = 0; c < updates.ColumnCount; c++)
            if (c != pathIdx && c != indexIdx)
                setColumns.Add((updates.Schema.FieldsList[c].Name, updates.Column(c)));

        return CollectOnFailureAsync(
            written => UpdateRowsCoreAsync(
                RowSelection.ByPath(positionsByPath),
                (path, sourceBatches, positionsPerBatch) =>
                    ApplyLocatorKeyedUpdates(path, sourceBatches, positionsPerBatch, updIndexByLocator, setColumns),
                written,
                cancellationToken),
            cancellationToken);
    }

    // Substitutes the SET columns' values at every source row whose (path, absolute position) is in
    // `updIndexByLocator`, pulling the new value from `setColumns` at the mapped index. Type-agnostic: per SET
    // column, concatenate [source column, updates column] and TAKE — source row i takes index i, an updated row
    // takes (sourceLen + updIndex). Untouched columns/rows pass through by reference.
    private static IReadOnlyList<RecordBatch> ApplyLocatorKeyedUpdates(
        string path,
        IReadOnlyList<RecordBatch> sourceBatches,
        IReadOnlyList<Int64Array> positionsPerBatch,
        IReadOnlyDictionary<(string Path, long Position), int> updIndexByLocator,
        IReadOnlyList<(string Name, IArrowArray Values)> setColumns)
    {
        var result = new List<RecordBatch>(sourceBatches.Count);
        for (int b = 0; b < sourceBatches.Count; b++)
        {
            var src = sourceBatches[b];
            var positions = positionsPerBatch[b];

            // take indices: normally i (from the source half); an updated row → src.Length + updIndex.
            List<int>? take = null;
            for (int i = 0; i < src.Length; i++)
            {
                if (!positions.IsNull(i)
                    && updIndexByLocator.TryGetValue((path, positions.GetValue(i)!.Value), out int updIdx))
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
                columns[c] = ArrowCompute.Take(combined, take);
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

    private async ValueTask<long> UpdateRowsCoreAsync(
        RowSelection selection,
        Func<string, IReadOnlyList<RecordBatch>, IReadOnlyList<Int64Array>, IReadOnlyList<RecordBatch>> rewriteFile,
        WrittenFileLedger written,
        CancellationToken cancellationToken = default)
    {
        ProtocolVersions.ValidateWriteSupport(CurrentSnapshot.Protocol);

        var snapshot = CurrentSnapshot;
        if (selection.IsEmpty)
            return snapshot.Version;

        HonorWriterFeatures(snapshot, isAppend: false);
        RejectCopyOnWriteRowIdUnsupported("copy-on-write UPDATE");

        var mappingMode = ColumnMapping.GetMode(snapshot.Metadata.Configuration);
        bool rowTrackingEnabled = DeltaLake.RowTracking.RowTrackingConfig.IsEnabled(
            snapshot.Metadata.Configuration);
        var (matRowIdName, matRowVerName) = DeltaLake.RowTracking.RowTrackingConfig
            .TryGetMaterializedColumnNames(snapshot.Metadata.Configuration);
        bool materializeIds = rowTrackingEnabled && matRowIdName is not null && matRowVerName is not null;
        bool cdfEnabled = DeltaLake.ChangeDataFeed.CdfConfig.IsEnabled(snapshot.Metadata.Configuration);
        long newVersion = snapshot.Version + 1;
        long nextRowId = rowTrackingEnabled ? snapshot.RowIdHighWaterMark : 0;

        var actions = new List<DeltaAction>();
        var removedPaths = new HashSet<string>(StringComparer.Ordinal);

        var byPath = ActiveFilesByPath(snapshot);
        foreach (var kvp in selection.Entries)
        {
            if (!byPath.TryGetValue(kvp.Key, out var addFile))
                throw StaleSelectionPath(kvp.Key, snapshot);
            var targets = RowSelection.AsSet(kvp.Value);

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

            // Per-batch ABSOLUTE positions (row-aligned), so the rewriter can key its new values by the same
            // (path, position) locator the selection speaks.
            var positionsPerBatch = new List<Int64Array>(userBatches.Count);
            for (int bi = 0; bi < userBatches.Count; bi++)
            {
                var absPos = bi < absOut.Count ? absOut[bi] : null;
                var pb = new Int64Array.Builder().Reserve(userBatches[bi].Length);
                for (int i = 0; i < userBatches[bi].Length; i++)
                {
                    long abs = absPos is not null && i < absPos.Length && !absPos.IsNull(i)
                        ? absPos.GetValue(i)!.Value : i;
                    pb.Append(abs);
                }
                positionsPerBatch.Add(pb.Build());
            }

            // The caller rebuilds each batch's rows with the SET columns modified on the matched positions.
            var rewritten = rewriteFile(addFile.Path, userBatches, positionsPerBatch);
            if (rewritten.Count != userBatches.Count)
                throw new InvalidOperationException(
                    "UpdateRowsAsync: rewriteFile must return one batch per source batch.");

            // Build the materialized id/version arrays (an UPDATED row's version advances to this commit) and
            // count the rows actually matched in this file.
            var outTracking = materializeIds ? new List<(Int64Array Ids, Int64Array Vers)?>() : null;
            // Change Data Feed: the pre/post image of exactly the rows this update touched, paired per source
            // batch (the rewriter preserves batch structure and row order, so index i means the same row in both).
            var changePairs = cdfEnabled
                ? new List<(RecordBatch Pre, RecordBatch Post, Int64Array? Ids, Int64Array? PreVers)>()
                : null;
            long updatedHere = 0;
            for (int bi = 0; bi < userBatches.Count; bi++)
            {
                var src = userBatches[bi];
                if (rewritten[bi].Length != src.Length)
                    throw new InvalidOperationException(
                        "UpdateRowsAsync: rewriteFile must preserve each batch's row count.");
                var absPos = bi < absOut.Count ? absOut[bi] : null;
                var batchIds = srcIds is not null && bi < srcIds.Count ? srcIds[bi] : null;
                var batchVers = srcVers is not null && bi < srcVers.Count ? srcVers[bi] : null;
                Int64Array.Builder? idb = materializeIds ? new Int64Array.Builder().Reserve(src.Length) : null;
                Int64Array.Builder? vdb = materializeIds ? new Int64Array.Builder().Reserve(src.Length) : null;
                var matchedRows = cdfEnabled ? new List<int>() : null;
                for (int i = 0; i < src.Length; i++)
                {
                    long abs = absPos is not null && i < absPos.Length && !absPos.IsNull(i)
                        ? absPos.GetValue(i)!.Value : i;
                    bool updated = targets.Contains(abs);
                    if (updated)
                    {
                        updatedHere++;
                        matchedRows?.Add(i);
                    }
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
                if (matchedRows is { Count: > 0 })
                {
                    // Both images are the same rows, so one id array serves both; the pre-image keeps the
                    // version that last wrote the row, the post-image takes this commit's.
                    changePairs!.Add((
                        TakeRowsFromBatch(src, matchedRows),
                        TakeRowsFromBatch(rewritten[bi], matchedRows),
                        batchIds is not null ? TakeIds(batchIds, matchedRows) : null,
                        batchVers is not null ? TakeIds(batchVers, matchedRows) : null));
                }
            }

            if (updatedHere == 0)
                continue; // no target row actually present in this file — leave it untouched

            var (remove, add, addedRows) = await RewriteRowsToNewFileAsync(
                snapshot, addFile, mappingMode, rewritten, outTracking, materializeIds,
                matRowIdName, matRowVerName, rowTrackingEnabled, nextRowId, newVersion,
                cancellationToken, written).ConfigureAwait(false);
            actions.Add(remove);
            removedPaths.Add(addFile.Path);
            if (add is not null)
            {
                actions.Add(add);
                if (rowTrackingEnabled)
                    nextRowId += addedRows;
            }

            // update_preimage + update_postimage change files for the touched rows. As in the copy-on-write
            // DELETE, these replace what the reader would otherwise infer from remove(old)+add(new) — which for
            // a rewrite is every row of the file deleted and re-inserted.
            if (changePairs is not null)
            {
                foreach (var (pre, post, ids, preVers) in changePairs)
                {
                    actions.Add(await ChangeDataFeed.CdfWriter.WriteAsync(
                        _fs, snapshot, pre, DeltaLake.ChangeDataFeed.CdfConfig.UpdatePreimage,
                        addFile.PartitionValues, _options.ParquetWriteOptions,
                        cancellationToken, ids, preVers, written).ConfigureAwait(false));
                    actions.Add(await ChangeDataFeed.CdfWriter.WriteAsync(
                        _fs, snapshot, post, DeltaLake.ChangeDataFeed.CdfConfig.UpdatePostimage,
                        addFile.PartitionValues, _options.ParquetWriteOptions,
                        cancellationToken,
                        ids, ids is not null ? ConstInt64(newVersion, ids.Length) : null,
                        written).ConfigureAwait(false));
                }
            }
        }

        if (actions.Count == 0)
            return snapshot.Version;

        if (rowTrackingEnabled && nextRowId > snapshot.RowIdHighWaterMark)
            actions.Add(DeltaLake.RowTracking.RowTrackingConfig.BuildHighWaterMarkAction(nextRowId));

        return await CommitOccAsync(
            snapshot, actions,
            new ReadSet { Files = removedPaths }, removedPaths,
            IsolationLevel.WriteSerializable, "UPDATE", rebaseSafe: false, cancellationToken,
            written: written, isBlindAppend: false)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Reads exactly the rows named by <paramref name="selection"/> — the read-back step a host-driven UPDATE's
    /// post-image is built from. Deletion-vector-excluded rows never match (the read filters them), and files
    /// the selection does not name are not read. Batches come back in the selection's path order; a file
    /// contributing no matching row yields nothing.
    ///
    /// <para>To pair the returned rows with what was asked for, ask for
    /// <see cref="DeltaRowReadOptions.Metadata"/>: batching and deletion-vector filtering both break any
    /// positional correspondence, so a caller must match on a KEY rather than on order.
    /// <see cref="DeltaRowMetadata.Locator"/> gives the same <c>(add.path, absolute position)</c> pair the
    /// selection is built on; <see cref="DeltaRowMetadata.RowAddress"/> gives it packed, for a host whose own
    /// rowid is one integer.</para>
    ///
    /// <para>The STABLE identity to carry through a rewrite is
    /// <see cref="DeltaRowMetadata.RowTracking"/>, resolved per row as the source file's materialized value
    /// where it has one — a rewritten file — else <c>baseRowId + absolute position</c>. That is a different
    /// number from the snapshot-scoped address the selection is built from, and it is the one to hand back to
    /// <see cref="WriteDataFilesAsync"/>' <c>materializedRowIds</c>.</para>
    /// </summary>
    /// <param name="selection">The rows to read, by <c>add.path</c> and absolute in-file position.</param>
    /// <param name="options">Metadata columns, their prefix, and the snapshot to resolve against. Null takes
    /// every default: no metadata columns, and the selection resolved against <see cref="CurrentSnapshot"/>.
    ///
    /// <para><see cref="DeltaRowReadOptions.Metadata"/> is taken exactly as
    /// <see cref="ReadAsync(DeltaReadOptions, CancellationToken)"/> and
    /// <see cref="ReadChangesAsync(DeltaChangeReadOptions, CancellationToken)"/> take it — same flags, same
    /// column names, same combinability.</para></param>
    /// <param name="cancellationToken">Cancels the read.</param>
    public async IAsyncEnumerable<RecordBatch> ReadRowsAsync(
        RowSelection selection,
        DeltaRowReadOptions? options = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        if (selection is null)
            throw new ArgumentNullException(nameof(selection));
        options ??= new DeltaRowReadOptions();
        if (options.MetadataPrefix is null)
        {
            throw new ArgumentException(
                "DeltaRowReadOptions.MetadataPrefix cannot be null; leave it unset for the default.",
                nameof(options));
        }
        var metadata = options.Metadata;
        var metadataPrefix = options.MetadataPrefix;
        if (options.ResolveAgainst is not null)
            RequireSnapshotOfThisTable(options.ResolveAgainst, nameof(options));
        var snapshot = options.ResolveAgainst ?? CurrentSnapshot;
        ValidateReadMetadata(snapshot, metadata, metadataPrefix);
        var byPath = ActiveFilesByPath(snapshot);

        bool wantAddress = (metadata & DeltaRowMetadata.RowAddress) != 0;
        bool wantLocator = (metadata & DeltaRowMetadata.Locator) != 0;
        bool wantTracking = (metadata & DeltaRowMetadata.RowTracking) != 0;
        var metadataFields = MetadataFields(metadata, metadataPrefix);

        // RowAddress' ordinal is a position in the FULL active set, not in the selection — computed only when
        // asked for, since it costs a sort of every active file to answer for the few the selection names.
        Dictionary<string, int>? ordinalByPath = null;
        if (wantAddress)
        {
            var ordered = OrderedActiveFiles(snapshot);
            ordinalByPath = new Dictionary<string, int>(ordered.Count, StringComparer.Ordinal);
            for (int i = 0; i < ordered.Count; i++)
                ordinalByPath[ordered[i].Path] = i;
        }

        foreach (var kvp in selection.Entries.OrderBy(k => k.Key, StringComparer.Ordinal))
        {
            if (!byPath.TryGetValue(kvp.Key, out var addFile))
                throw StaleSelectionPath(kvp.Key, snapshot);
            var targets = RowSelection.AsSet(kvp.Value);
            int fileOrdinal = ordinalByPath is not null ? ordinalByPath[kvp.Key] : -1;
            byte[]? pathBytes = wantLocator
                ? System.Text.Encoding.UTF8.GetBytes(addFile.Path) : null;

            // The per-file read reports each surviving row's ABSOLUTE in-file position (DV-inclusive) and its
            // RESOLVED stable id/version out-of-band, having stripped the materialized id columns from the
            // emitted batch — so the position to match on, and the identity to report, both come from here
            // rather than from a column of `batch`.
            var absOut = new List<Int64Array?>();
            var idsOut = wantTracking ? new List<Int64Array?>() : null;
            var versOut = wantTracking ? new List<Int64Array?>() : null;
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
                    if (!absPos.IsNull(i) && targets.Contains(absPos.GetValue(i)!.Value))
                        rows.Add(i);
                if (rows.Count == 0)
                    continue;

                var taken = TakeRowsFromBatch(batch, rows);
                if (metadataFields.Count == 0)
                {
                    yield return taken;
                    continue;
                }

                // Built over the TAKEN rows, in the same flag order ReadCoreAsync uses — the columns describe
                // what is yielded, not what was scanned, so `rows` indexes every builder below.
                var columns = new List<IArrowArray>(metadataFields.Count);
                if (wantAddress)
                {
                    var b = new Int64Array.Builder().Reserve(rows.Count);
                    foreach (int i in rows)
                        b.Append(TransientRowAddress.Pack(fileOrdinal, AbsoluteAt(absPos, i)));
                    columns.Add(b.Build());
                }
                if (wantLocator)
                {
                    columns.Add(ArrowCompute.Repeat(
                        Apache.Arrow.Types.StringType.Default, pathBytes!, rows.Count));
                    var b = new Int64Array.Builder().Reserve(rows.Count);
                    foreach (int i in rows)
                        b.Append(AbsoluteAt(absPos, i));
                    columns.Add(b.Build());
                }
                if (wantTracking)
                {
                    var matI = idsOut is not null && bi < idsOut.Count ? idsOut[bi] : null;
                    var matV = versOut is not null && bi < versOut.Count ? versOut[bi] : null;
                    var ib = new Int64Array.Builder().Reserve(rows.Count);
                    var vb = new Int64Array.Builder().Reserve(rows.Count);
                    foreach (int i in rows)
                    {
                        if (matI is not null && i < matI.Length && !matI.IsNull(i))
                            ib.Append(matI.GetValue(i)!.Value);
                        else
                            ib.AppendNull();
                        if (matV is not null && i < matV.Length && !matV.IsNull(i))
                            vb.Append(matV.GetValue(i)!.Value);
                        else
                            vb.AppendNull();
                    }
                    columns.Add(ib.Build());
                    columns.Add(vb.Build());
                }

                yield return AppendColumns(taken, metadataFields, columns);
            }
        }
    }

    /// <summary>
    /// ROW-LEVEL rebase for the buffered surface: re-targets a DV DML action set computed against
    /// <paramref name="from"/> onto <paramref name="to"/> when a concurrent writer swapped a touched file's
    /// deletion vector. Per <c>remove</c>+<c>add</c> DV pair (matched by path), by whether the path survived:
    /// <list type="bullet">
    /// <item><b>Still ACTIVE in <paramref name="to"/></b> — the pair re-unions. THIS transaction's
    /// newly-deleted positions (<paramref name="newPositionsByOrdinal"/>, keyed by <paramref name="from"/>'s
    /// path-sorted ordinals) must be DISJOINT from the concurrent deletions (an intersection = the same row
    /// deleted/updated by both ⇒ row-level conflict); disjoint ⇒ the pair re-issues against the CURRENT state
    /// (<c>remove</c>(path, current DV) + <c>add</c>(path, current DV ∪ ours)).</item>
    /// <item><b>REWRITTEN AWAY</b> by a concurrent compaction / copy-on-write UPDATE — the rows are relocated
    /// by STABLE ROW ID onto the new files instead of aborting, through the same Layer 3 (B) remap the
    /// autocommit path uses (<see cref="RemapRowLevelDeletesAsync"/>, reached from
    /// <see cref="ResolveRowLevelDeletesAsync"/> there): the staged pair is dropped and replaced by DV pairs on
    /// the new files. The row's commit version discriminates relocated-untouched from concurrently-modified, so
    /// a row the rewriter also changed is still a row-level conflict. Requires row tracking — without stable
    /// ids to follow, a rewritten-away touched file remains a conflict.</item>
    /// </list>
    /// Post-image adds (paths not in <paramref name="from"/>) get row-tracking
    /// <c>baseRowId</c>/<c>defaultRowCommitVersion</c> re-derived from <paramref name="to"/>, and the
    /// high-water-mark domain rebuilt; the remap's re-adds are NOT post-images — they keep the new files' own
    /// <c>baseRowId</c> and leave the high-water mark alone. Metadata/protocol changes between the snapshots
    /// throw. The caller re-runs commitInfo assembly after the rebase.
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
                DeltaErrorCodes.MetadataChanged,
                "concurrent metadata change (schema/partitioning/configuration) — cannot rebase the transaction");
        }
        if (!ProtocolEquals(from.Protocol, to.Protocol))
        {
            throw new DeltaConflictException(
                DeltaErrorCodes.ProtocolChanged,
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

        // Touched files REWRITTEN AWAY concurrently (compaction / copy-on-write UPDATE): their staged DV pairs
        // have nothing left to re-union against, so the rows are relocated by STABLE ROW ID onto the new files
        // instead — the same Layer 3 (B) remap the autocommit path reaches through ResolveRowLevelDeletesAsync.
        // Collected up front so the no-row-tracking conflict aborts before any deletion vector is written.
        List<DeleteDvEdit>? remapEdits = null;
        foreach (var kvp in oursByPath)
        {
            if (toByPath.ContainsKey(kvp.Key))
                continue;
            if (!rowTrackingEnabled)
            {
                throw new DeltaConflictException(
                    DeltaTableErrorCodes.RowLevelConflict,
                    $"concurrent rewrite/compaction of file '{kvp.Key}' this transaction modifies — cannot "
                    + "rebase the buffered transaction (row tracking is disabled, so its rows cannot be "
                    + "remapped by stable id); retry it");
            }
            (remapEdits ??= []).Add(new DeleteDvEdit(
                kvp.Key, kvp.Value as IReadOnlyList<long> ?? [.. kvp.Value]));
        }

        foreach (var action in actions)
        {
            switch (action)
            {
                case RemoveFile remove when oursByPath.ContainsKey(remove.Path):
                {
                    if (!toByPath.TryGetValue(remove.Path, out var current))
                    {
                        // Rewritten away — this pair is replaced wholesale by the remap after the loop.
                        break;
                    }
                    rebased.Add(remove with { DeletionVector = current.DeletionVector });
                    break;
                }
                case AddFile add when oursByPath.TryGetValue(add.Path, out var ours):
                {
                    if (!toByPath.TryGetValue(add.Path, out var current))
                    {
                        // Rewritten away — handled by the remap, paired with the skipped remove above.
                        break;
                    }
                    // The DV-pair re-add: union OUR positions with the CURRENT deletion vector, after the
                    // row-level disjointness check against the concurrent deletions.
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
                            DeltaTableErrorCodes.RowLevelConflict,
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
                        Stats = StatsWithLooseBounds(current.GetStatsJson()),
                    });
                    break;
                }
                case AddFile add when !fromByPath.Contains(add.Path) && add.DataChange:
                {
                    // Post-image add (a brand-new file): re-derive its row-id range from the snapshot we are
                    // committing onto — concurrent commits may have consumed row-id space.
                    if (rowTrackingEnabled && add.BaseRowId is not null)
                    {
                        long rows = add.GetNumRecords() ?? 0;
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
        if (remapEdits is not null)
        {
            // Layer 3 (B): relocate the rewritten-away files' rows by stable id onto the new files. The re-adds
            // are DV pairs on files that already exist in `to`, NOT post-images, so they keep their own
            // baseRowId and consume no row-id space — which is why they are appended outside the loop rather
            // than routed through the post-image case above.
            var resolvedPaths = new HashSet<string>(StringComparer.Ordinal); // the checker bookkeeping the
            // autocommit caller needs; the buffered caller re-validates via CheckLogicalRebaseAsync instead,
            // where the remap's remove(newPath, current DV) matches the still-active file and passes.
            // No ledger: this is the buffered/host-driven rebase seam, whose files the HOST owns end to end.
            var remapped = await RemapRowLevelDeletesAsync(
                from, to, remapEdits, resolvedPaths, cancellationToken, written: null)
                .ConfigureAwait(false);
            if (remapped is null)
            {
                throw new DeltaConflictException(
                    DeltaTableErrorCodes.RowLevelConflict,
                    "row-level conflict remapping across a concurrent rewrite/compaction: a row this "
                    + "transaction deletes/updates was concurrently deleted or updated, or its stable id could "
                    + "not be resolved — retry the transaction");
            }
            rebased.AddRange(remapped);
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
                DeltaErrorCodes.MetadataChanged,
                "concurrent metadata change (schema/partitioning/configuration) — cannot rebase the transaction");
        }
        if (!ProtocolEquals(baseSnapshot.Protocol, latest.Protocol))
        {
            throw new DeltaConflictException(
                DeltaErrorCodes.ProtocolChanged,
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
                    DeltaErrorCodes.ConcurrentDeleteDelete,
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
        var pruner = new DeltaFilePruner(baseSnapshot.Schema, baseSnapshot.Metadata.PartitionColumns,
            _options.PreferTypedCheckpointStats);
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

            // ONE rule, shared with ConflictChecker. This used to be a second copy — starting `true` and
            // clearing on remove/metaData/protocol — which differed from the checker's in requiring no
            // add, so an add-less commit was blind here and not there. That disagreement was inert while
            // both only gated an AddFile branch an add-less commit never reaches; it stopped being inert
            // the moment the checker learned to believe a declared commitInfo.isBlindAppend and this did
            // not, because then a Spark commit declaring FALSE on an adds-only commit was correctly
            // examined by one path and wrongly exempted by the other — which is the whole defect, alive
            // on the buffered-transaction rebase instead of on the OCC loop.
            bool blindAppend = Concurrency.ConflictChecker.IsBlindAppend(commitActions);
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
                                DeltaErrorCodes.ConcurrentDeleteRead,
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
                                DeltaErrorCodes.ConcurrentAppend,
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
        CancellationToken cancellationToken = default,
        bool? isBlindAppend = null)
    {
        var batchList = new List<RecordBatch>();
        await foreach (var batch in batches.WithCancellation(cancellationToken)
            .ConfigureAwait(false))
        {
            batchList.Add(batch);
        }
        return await WriteAsync(batchList, mode, cancellationToken, isBlindAppend: isBlindAppend)
            .ConfigureAwait(false);
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

        // The path with the most to lose: OPTIMIZE rewrites its whole candidate set and then makes ONE commit
        // attempt at the read version + 1, so a single concurrent commit used to orphan every file it wrote.
        var result = await CollectOnFailureAsync(
            written => Compaction.CompactionExecutor.ExecuteAsync(
                _fs, _log, CurrentSnapshot, options,
                _options.ParquetWriteOptions, _dataFileReadOptions,
                cancellationToken, _options.DataFileWriter, _options.DataFileReader, written),
            cancellationToken).ConfigureAwait(false);

        if (result.HasValue)
        {
            _currentSnapshot = await SnapshotBuilder.UpdateAsync(
                CurrentSnapshot, _log, cancellationToken).ConfigureAwait(false);

            // OPTIMIZE is the operation with the most reason to checkpoint: it removes every file it
            // rewrote, so the commit it writes is the largest the table produces, and a log replay that
            // cannot start from a checkpoint reads all of it.
            await CheckpointIfDueAsync(result.Value, cancellationToken).ConfigureAwait(false);
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
            _fs, _log, CurrentSnapshot, retention, dryRun,
            _options.HideIcebergMetadataDirectory, cancellationToken)
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

    /// <summary>
    /// Every physical column name a data file may carry the row-tracking materialization under: the two the
    /// table DECLARES (<c>delta.rowTracking.materializedRowIdColumnName</c> /
    /// <c>…materializedRowCommitVersionColumnName</c>) plus the legacy internal names a pre-1.0 EngineeredWood
    /// wrote — deliberately the same set
    /// <see cref="RowTracking.RowTrackingWriter.StripMaterializedColumns"/> is prepared to strip, because a
    /// read that does not ASK for what the strip would take resolves ids off the wrong values entirely.
    /// <para>Empty when the table declares neither name (no row tracking, or a spec-invalid table that cannot
    /// materialize anyway) — a read must then name no extra column at all.</para>
    /// </summary>
    private static IReadOnlyList<string> MaterializedRowTrackingColumnNames(Snapshot.Snapshot snapshot)
    {
        var (rowIdName, rowVerName) = DeltaLake.RowTracking.RowTrackingConfig
            .TryGetMaterializedColumnNames(snapshot.Metadata.Configuration);
        if (rowIdName is null && rowVerName is null)
            return [];

        var names = new List<string>(4);
        if (rowIdName is not null)
            names.Add(rowIdName);
        if (rowVerName is not null)
            names.Add(rowVerName);
        names.Add(RowTracking.RowTrackingWriter.RowIdColumn);
        names.Add(RowTracking.RowTrackingWriter.RowCommitVersionColumn);
        return names;
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

            // Row tracking: the hidden materialized columns are NOT schema fields, so neither projection above
            // ever names them — and reading a rewrite output without them silently re-derives every row's id
            // from baseRowId + position, which on that file is a FRESH id rather than the row's own. The seam
            // hides the file's schema by design, so there is no way to name them only for the files that carry
            // them, and naming a column a file lacks is a hard error for a host that binds columns by name.
            // Read every column instead: on the narrow intersection of codec seam AND row tracking, correct
            // ids are worth more than the projection.
            if (seamColumns is not null && MaterializedRowTrackingColumnNames(snapshot).Count > 0)
                seamColumns = null;

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

        // Row tracking: same omission as the seam branch above — the hidden materialized columns are not schema
        // fields, so a PROJECTED read (either mapping mode) and an UNPROJECTED read of a PARTITIONED table both
        // build a file-level column list without them, and every id then falls back to baseRowId + position.
        // Here the file's own schema IS available, so name them and let the file-present intersect below drop
        // them again for a file that carries none — which is every fresh append, the common case.
        if (fileColumns is not null)
        {
            var trackingColumns = MaterializedRowTrackingColumnNames(snapshot);
            if (trackingColumns.Count > 0)
            {
                var withTracking = new List<string>(fileColumns.Count + trackingColumns.Count);
                withTracking.AddRange(fileColumns);
                withTracking.AddRange(trackingColumns);
                fileColumns = withTracking;
            }
        }

        // Schema evolution: ADD COLUMN is a metadata-only commit, so a file written BEFORE it does not
        // contain the new column — asking the parquet reader for it throws ("Column 'x' was not found in
        // the schema"). Intersect the projection with the file's ACTUAL top-level columns and let the
        // BackfillMissingColumns step downstream reconstitute the absent ones as typed NULL, exactly as it
        // already does for an UNPROJECTED read of the same file. Without this a projected read is strictly
        // less capable than an unprojected one over identical data, which is the wrong way round.
        //
        // An empty result — a projection naming ONLY later-added columns — is deliberately left empty
        // rather than padded with some column the file does happen to have: the reader takes its row count
        // from the row group, not from the columns it returns, so the batches still carry the lengths the
        // backfill needs, and padding would read bytes only to discard them. Pinned by
        // SchemaEvolutionTests.ProjectedRead_OfAColumnAddedAfterTheFile_BackfillsNull.
        //
        // Id mode never threw here (an unresolvable field id already drops out of ResolveFieldIds); it runs
        // through the same reconciliation so one rule covers every mapping mode.
        if (fileColumns is not null)
        {
            parquetSchema ??= await reader.GetSchemaAsync(cancellationToken).ConfigureAwait(false);
            var filePresent = new HashSet<string>(StringComparer.Ordinal);
            foreach (var child in parquetSchema.Root.Children)
                filePresent.Add(child.Name);

            if (fileColumns.Any(c => !filePresent.Contains(c)))
                fileColumns = fileColumns.Where(filePresent.Contains).ToList();
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
                var idb = new Int64Array.Builder().Reserve(survivorSrc!.Count);
                var vrb = new Int64Array.Builder().Reserve(survivorSrc.Count);
                var pb = new Int64Array.Builder().Reserve(survivorSrc.Count);
                foreach (int i in survivorSrc)
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
