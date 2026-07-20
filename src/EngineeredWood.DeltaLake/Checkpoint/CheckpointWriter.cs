// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.IO;
using EngineeredWood.Parquet;
using ArrowMapType = Apache.Arrow.Types.MapType;
using ArrowStructType = Apache.Arrow.Types.StructType;

namespace EngineeredWood.DeltaLake.Checkpoint;

/// <summary>
/// Writes Delta Lake checkpoint files (Parquet format) from a snapshot.
/// Uses the standard struct-based checkpoint schema expected by all
/// Delta Lake implementations.
/// </summary>
public sealed class CheckpointWriter
{
    private readonly ITableFileSystem _fs;
    private readonly ParquetWriteOptions? _parquetOptions;

    public CheckpointWriter(
        ITableFileSystem fileSystem,
        ParquetWriteOptions? parquetOptions = null)
    {
        _fs = fileSystem;
        _parquetOptions = parquetOptions;
    }

    /// <summary>
    /// Writes a checkpoint Parquet file for the given snapshot,
    /// then updates <c>_last_checkpoint</c>.
    /// </summary>
    public async ValueTask WriteCheckpointAsync(
        Snapshot.Snapshot snapshot,
        CancellationToken cancellationToken = default)
    {
        string path = DeltaVersion.CheckpointPath(snapshot.Version);

        var batch = BuildCheckpointBatch(snapshot, out long actionCount);

        await using (var file = await _fs.CreateAsync(path, overwrite: true, cancellationToken)
            .ConfigureAwait(false))
        {
            await using var writer = new ParquetFileWriter(file, ownsFile: false, _parquetOptions);
            await writer.WriteRowGroupAsync(batch, cancellationToken).ConfigureAwait(false);
        }

        // Write _last_checkpoint
        using var lastCheckpointStream = new MemoryStream();
        using (var w = new Utf8JsonWriter(lastCheckpointStream))
        {
            w.WriteStartObject();
            w.WriteNumber("version", snapshot.Version);
            w.WriteNumber("size", actionCount);
            w.WriteEndObject();
        }
        byte[] json = lastCheckpointStream.ToArray();

        await _fs.WriteAllBytesAsync(
            DeltaVersion.LastCheckpointPath, json, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Builds a checkpoint RecordBatch. Used by V2CheckpointWriter for sidecar files.
    /// </summary>
    internal static RecordBatch BuildCheckpointBatchPublic(
        Snapshot.Snapshot snapshot, out long actionCount) =>
        BuildCheckpointBatch(snapshot, out actionCount);

    private static RecordBatch BuildCheckpointBatch(
        Snapshot.Snapshot snapshot, out long actionCount)
    {
        // Collect all actions: 1 protocol + 1 metadata + N adds + N unexpired removes + N txns + N domainMetadata
        var allActions = new List<DeltaAction>();
        allActions.Add(snapshot.Protocol);
        allActions.Add(snapshot.Metadata);

        foreach (var add in snapshot.ActiveFiles.Values)
            allActions.Add(add);

        // The spec requires remove tombstones within the retention window to be preserved in
        // checkpoints (a reader replaying only from the checkpoint would otherwise lose them and
        // VACUUM safety / streaming readers break). Expired tombstones are reconciled away here.
        long expiryCutoffMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
            - (long)TombstoneRetention(snapshot.Metadata.Configuration).TotalMilliseconds;
        foreach (var remove in snapshot.Tombstones.Values)
        {
            if (remove.DeletionTimestamp is null || remove.DeletionTimestamp.Value >= expiryCutoffMs)
                allActions.Add(remove);
        }

        foreach (var txn in snapshot.AppTransactions.Values)
            allActions.Add(txn);

        foreach (var dm in snapshot.DomainMetadata.Values)
            allActions.Add(dm);

        actionCount = allActions.Count;
        int count = allActions.Count;

        // Build the struct-based checkpoint schema
        var schema = BuildCheckpointSchema(snapshot.Schema);

        // Build struct arrays for each action type
        var protocolArray = BuildProtocolColumn(allActions, count);
        var metadataArray = BuildMetadataColumn(allActions, count);
        var addArray = BuildAddColumn(allActions, count);
        var removeArray = BuildRemoveColumn(allActions, count);
        var txnArray = BuildTxnColumn(allActions, count);
        var domainMetadataArray = BuildDomainMetadataColumn(allActions, count);

        // Build stats_parsed column from JSON stats in add actions
        var statsParsedArray = StatsParsedBuilder.BuildStatsColumn(
            allActions, count, snapshot.Schema);

        return new RecordBatch(schema,
            [protocolArray, metadataArray, addArray, removeArray, txnArray,
             domainMetadataArray, statsParsedArray],
            count);
    }

    #region Schema Definition

    private static Apache.Arrow.Schema BuildCheckpointSchema(
        Schema.StructType deltaSchema)
    {
        // Protocol struct
        var protocolType = new ArrowStructType(new List<Field>
        {
            new Field("minReaderVersion", Int32Type.Default, true),
            new Field("minWriterVersion", Int32Type.Default, true),
            // Required by the spec (and strict readers) when minReaderVersion==3 / minWriterVersion==7:
            // a checkpoint that drops the feature lists corrupts the table protocol on replay.
            new Field("readerFeatures", new ListType(new Field("element", StringType.Default, false)), true),
            new Field("writerFeatures", new ListType(new Field("element", StringType.Default, false)), true),
        });

        // Format struct for metaData
        var formatType = new ArrowStructType(new List<Field>
        {
            new Field("provider", StringType.Default, true),
            // REQUIRED by strict readers (delta-kernel): format.options must exist (an empty map).
            new Field("options", new ArrowMapType(
                new Field("key", StringType.Default, false),
                new Field("value", StringType.Default, true)), true),
        });

        // MetaData struct
        var metadataType = new ArrowStructType(new List<Field>
        {
            new Field("id", StringType.Default, true),
            new Field("name", StringType.Default, true),
            new Field("description", StringType.Default, true),
            new Field("format", formatType, true),
            new Field("schemaString", StringType.Default, true),
            new Field("partitionColumns", new ListType(new Field("element", StringType.Default, false)), true),
            new Field("createdTime", Int64Type.Default, true),
            new Field("configuration", new ArrowMapType(
                new Field("key", StringType.Default, false),
                new Field("value", StringType.Default, true)), true),
        });

        // Add struct. deletionVector + baseRowId/defaultRowCommitVersion MUST be preserved: a checkpoint
        // that drops the DV resurrects the deleted rows for every reader replaying from it, and dropping the
        // row-tracking fields breaks stable row ids past the checkpoint.
        var dvType = new ArrowStructType(new List<Field>
        {
            new Field("storageType", StringType.Default, true),
            new Field("pathOrInlineDv", StringType.Default, true),
            new Field("offset", Int32Type.Default, true),
            new Field("sizeInBytes", Int32Type.Default, true),
            new Field("cardinality", Int64Type.Default, true),
        });
        var addType = new ArrowStructType(new List<Field>
        {
            new Field("path", StringType.Default, true),
            new Field("partitionValues", new ArrowMapType(
                new Field("key", StringType.Default, false),
                new Field("value", StringType.Default, true)), true),
            new Field("size", Int64Type.Default, true),
            new Field("modificationTime", Int64Type.Default, true),
            new Field("dataChange", BooleanType.Default, true),
            new Field("stats", StringType.Default, true),
            new Field("tags", new ArrowMapType(
                new Field("key", StringType.Default, false),
                new Field("value", StringType.Default, true)), true),
            new Field("deletionVector", dvType, true),
            new Field("baseRowId", Int64Type.Default, true),
            new Field("defaultRowCommitVersion", Int64Type.Default, true),
        });

        // Remove struct. deletionVector is preserved so a spec VACUUM (which protects a removed file's DV
        // during the retention window and sweeps it after) treats the tombstone correctly — dropping it can
        // cause premature DV deletion, breaking time-travel/CDF reads of versions still within retention.
        var removeType = new ArrowStructType(new List<Field>
        {
            new Field("path", StringType.Default, true),
            new Field("deletionTimestamp", Int64Type.Default, true),
            new Field("dataChange", BooleanType.Default, true),
            new Field("deletionVector", dvType, true),
        });

        // Txn struct
        var txnType = new ArrowStructType(new List<Field>
        {
            new Field("appId", StringType.Default, true),
            new Field("version", Int64Type.Default, true),
            new Field("lastUpdated", Int64Type.Default, true),
        });

        // DomainMetadata struct
        var domainMetadataType = new ArrowStructType(new List<Field>
        {
            new Field("domain", StringType.Default, true),
            new Field("configuration", StringType.Default, true),
            new Field("removed", BooleanType.Default, true),
        });

        var statsParsedType = StatsParsedBuilder.BuildStatsType(deltaSchema);

        return new Apache.Arrow.Schema.Builder()
            .Field(new Field("protocol", protocolType, true))
            .Field(new Field("metaData", metadataType, true))
            .Field(new Field("add", addType, true))
            .Field(new Field("remove", removeType, true))
            .Field(new Field("txn", txnType, true))
            .Field(new Field("domainMetadata", domainMetadataType, true))
            .Field(new Field("stats_parsed", statsParsedType, true))
            .Build();
    }

    #endregion

    #region Array Builders

    // Validity bitmap for a top-level action struct: TRUE exactly on the rows of that action type. The spec
    // checkpoint schema makes each action struct NULLABLE (null on rows of other action types) with required
    // fields inside — strict readers (delta-kernel) reject an always-present struct with null required
    // children ("unmasked nulls for non-nullable field"). Relies on the parquet writer handling nullable
    // structs correctly (the NestedLevelWriter null-struct fix).
    private static (ArrowBuffer Bitmap, int NullCount) BuildActionValidity<T>(
        List<DeltaAction> actions, int count) where T : DeltaAction
    {
        var validity = new ArrowBuffer.BitmapBuilder(count);
        int nullCount = 0;
        for (int i = 0; i < count; i++)
        {
            bool isType = actions[i] is T;
            validity.Append(isType);
            if (!isType)
                nullCount++;
        }
        return (validity.Build(), nullCount);
    }

    private static StructArray BuildProtocolColumn(List<DeltaAction> actions, int count)
    {
        var minReaderBuilder = new Int32Array.Builder();
        var minWriterBuilder = new Int32Array.Builder();
        var rfOffsets = new ArrowBuffer.Builder<int>();
        var rfValues = new StringArray.Builder();
        var rfValidity = new ArrowBuffer.BitmapBuilder(count);
        int rfNulls = 0, rfOffset = 0;
        var wfOffsets = new ArrowBuffer.Builder<int>();
        var wfValues = new StringArray.Builder();
        var wfValidity = new ArrowBuffer.BitmapBuilder(count);
        int wfNulls = 0, wfOffset = 0;
        rfOffsets.Append(0);
        wfOffsets.Append(0);

        for (int i = 0; i < count; i++)
        {
            if (actions[i] is ProtocolAction p)
            {
                minReaderBuilder.Append(p.MinReaderVersion);
                minWriterBuilder.Append(p.MinWriterVersion);
                if (p.ReaderFeatures is { } rf)
                {
                    foreach (var f in rf)
                    {
                        rfValues.Append(f);
                        rfOffset++;
                    }
                    rfValidity.Append(true);
                }
                else
                {
                    rfValidity.Append(false);
                    rfNulls++;
                }
                rfOffsets.Append(rfOffset);
                if (p.WriterFeatures is { } wf)
                {
                    foreach (var f in wf)
                    {
                        wfValues.Append(f);
                        wfOffset++;
                    }
                    wfValidity.Append(true);
                }
                else
                {
                    wfValidity.Append(false);
                    wfNulls++;
                }
                wfOffsets.Append(wfOffset);
            }
            else
            {
                minReaderBuilder.AppendNull();
                minWriterBuilder.AppendNull();
                rfValidity.Append(false);
                rfNulls++;
                rfOffsets.Append(rfOffset);
                wfValidity.Append(false);
                wfNulls++;
                wfOffsets.Append(wfOffset);
            }
        }

        var featureListType = new ListType(new Field("element", StringType.Default, false));
        var rfList = new ListArray(featureListType, count, rfOffsets.Build(), rfValues.Build(),
            rfValidity.Build(), rfNulls);
        var wfList = new ListArray(featureListType, count, wfOffsets.Build(), wfValues.Build(),
            wfValidity.Build(), wfNulls);

        var fields = new List<Field>
        {
            new Field("minReaderVersion", Int32Type.Default, true),
            new Field("minWriterVersion", Int32Type.Default, true),
            new Field("readerFeatures", featureListType, true),
            new Field("writerFeatures", featureListType, true),
        };

        var (validity, nullCount) = BuildActionValidity<ProtocolAction>(actions, count);
        return new StructArray(
            new ArrowStructType(fields),
            count,
            [minReaderBuilder.Build(), minWriterBuilder.Build(), rfList, wfList],
            validity, nullCount);
    }

    private static StructArray BuildMetadataColumn(List<DeltaAction> actions, int count)
    {
        var idBuilder = new StringArray.Builder();
        var nameBuilder = new StringArray.Builder();
        var descBuilder = new StringArray.Builder();
        var schemaStringBuilder = new StringArray.Builder();
        var createdTimeBuilder = new Int64Array.Builder();

        // Format struct arrays
        var formatProviderBuilder = new StringArray.Builder();

        // partitionColumns list
        var partColOffsetsBuilder = new ArrowBuffer.Builder<int>();
        var partColValues = new StringArray.Builder();
        partColOffsetsBuilder.Append(0);
        int partColOffset = 0;

        // configuration map
        var configOffsetsBuilder = new ArrowBuffer.Builder<int>();
        var configKeys = new StringArray.Builder();
        var configValues = new StringArray.Builder();
        configOffsetsBuilder.Append(0);
        int configOffset = 0;

        for (int i = 0; i < count; i++)
        {
            if (actions[i] is MetadataAction m)
            {
                idBuilder.Append(m.Id);
                nameBuilder.Append(m.Name ?? "");
                descBuilder.Append(m.Description ?? "");
                schemaStringBuilder.Append(m.SchemaString);
                createdTimeBuilder.Append(m.CreatedTime ?? 0);
                formatProviderBuilder.Append(m.Format.Provider);

                foreach (string col in m.PartitionColumns)
                {
                    partColValues.Append(col);
                    partColOffset++;
                }
                partColOffsetsBuilder.Append(partColOffset);

                if (m.Configuration is not null)
                {
                    foreach (var kvp in m.Configuration)
                    {
                        configKeys.Append(kvp.Key);
                        configValues.Append(kvp.Value);
                        configOffset++;
                    }
                }
                configOffsetsBuilder.Append(configOffset);
            }
            else
            {
                idBuilder.AppendNull();
                nameBuilder.AppendNull();
                descBuilder.AppendNull();
                schemaStringBuilder.AppendNull();
                createdTimeBuilder.AppendNull();
                formatProviderBuilder.AppendNull();
                partColOffsetsBuilder.Append(partColOffset);
                configOffsetsBuilder.Append(configOffset);
            }
        }

        // format.options: always-empty map (REQUIRED field for strict readers — delta-kernel rejects a
        // checkpoint whose format struct lacks it).
        var optMapType = new ArrowMapType(
            new Field("key", StringType.Default, false),
            new Field("value", StringType.Default, true));
        var optOffsets = new ArrowBuffer.Builder<int>();
        for (int i = 0; i <= count; i++)
        {
            optOffsets.Append(0);
        }
        var optEntries = new StructArray(
            new ArrowStructType(new List<Field> { optMapType.KeyField, optMapType.ValueField }),
            0,
            new IArrowArray[] { new StringArray.Builder().Build(), new StringArray.Builder().Build() },
            ArrowBuffer.Empty);
        var optMap = new MapArray(optMapType, count, optOffsets.Build(), optEntries, ArrowBuffer.Empty, 0);

        var formatFields = new List<Field>
        {
            new Field("provider", StringType.Default, true),
            new Field("options", optMapType, true),
        };
        var formatStruct = new StructArray(
            new ArrowStructType(formatFields),
            count,
            [formatProviderBuilder.Build(), optMap],
            ArrowBuffer.Empty, 0);

        var partColList = new ListArray(
            new ListType(new Field("element", StringType.Default, false)),
            count,
            partColOffsetsBuilder.Build(),
            partColValues.Build(),
            ArrowBuffer.Empty);

        var configMapType = new ArrowMapType(
            new Field("key", StringType.Default, false),
            new Field("value", StringType.Default, true));
        var configKeysArray = configKeys.Build();
        var configValuesArray = configValues.Build();
        var configEntries = new StructArray(
            new ArrowStructType(new List<Field> { configMapType.KeyField, configMapType.ValueField }),
            configKeysArray.Length,
            new IArrowArray[] { configKeysArray, configValuesArray },
            ArrowBuffer.Empty);
        var configMap = new MapArray(configMapType, count,
            configOffsetsBuilder.Build(), configEntries, ArrowBuffer.Empty, 0);

        var fields = new List<Field>
        {
            new Field("id", StringType.Default, true),
            new Field("name", StringType.Default, true),
            new Field("description", StringType.Default, true),
            new Field("format", new ArrowStructType(formatFields), true),
            new Field("schemaString", StringType.Default, true),
            new Field("partitionColumns", new ListType(new Field("element", StringType.Default, false)), true),
            new Field("createdTime", Int64Type.Default, true),
            new Field("configuration", new ArrowMapType(
                new Field("key", StringType.Default, false),
                new Field("value", StringType.Default, true)), true),
        };

        var (validity, nullCount) = BuildActionValidity<MetadataAction>(actions, count);
        return new StructArray(
            new ArrowStructType(fields),
            count,
            [idBuilder.Build(), nameBuilder.Build(), descBuilder.Build(),
             formatStruct, schemaStringBuilder.Build(), partColList,
             createdTimeBuilder.Build(), configMap],
            validity, nullCount);
    }

    private static StructArray BuildAddColumn(List<DeltaAction> actions, int count)
    {
        var pathBuilder = new StringArray.Builder();
        var sizeBuilder = new Int64Array.Builder();
        var modTimeBuilder = new Int64Array.Builder();
        var dataChangeBuilder = new BooleanArray.Builder();
        var statsBuilder = new StringArray.Builder();
        var dvStorageBuilder = new StringArray.Builder();
        var dvPathBuilder = new StringArray.Builder();
        var dvOffsetBuilder = new Int32Array.Builder();
        var dvSizeBuilder = new Int32Array.Builder();
        var dvCardBuilder = new Int64Array.Builder();
        var baseRowIdBuilder = new Int64Array.Builder();
        var defaultRcvBuilder = new Int64Array.Builder();

        // partitionValues map
        var pvOffsetsBuilder = new ArrowBuffer.Builder<int>();
        var pvKeys = new StringArray.Builder();
        var pvValues = new StringArray.Builder();
        pvOffsetsBuilder.Append(0);
        int pvOffset = 0;

        // tags map (null per row when the add carries no tags)
        var tagOffsetsBuilder = new ArrowBuffer.Builder<int>();
        var tagKeys = new StringArray.Builder();
        var tagValues = new StringArray.Builder();
        var tagValidity = new ArrowBuffer.BitmapBuilder(count);
        int tagNulls = 0;
        tagOffsetsBuilder.Append(0);
        int tagOffset = 0;

        for (int i = 0; i < count; i++)
        {
            if (actions[i] is AddFile a)
            {
                pathBuilder.Append(a.Path);
                sizeBuilder.Append(a.Size);
                modTimeBuilder.Append(a.ModificationTime);
                dataChangeBuilder.Append(a.DataChange);
                if (a.Stats is not null)
                    statsBuilder.Append(a.Stats);
                else
                    statsBuilder.AppendNull();

                foreach (var kvp in a.PartitionValues)
                {
                    pvKeys.Append(kvp.Key);
                    if (kvp.Value is null)
                        pvValues.AppendNull();
                    else
                        pvValues.Append(kvp.Value);
                    pvOffset++;
                }
                pvOffsetsBuilder.Append(pvOffset);

                if (a.Tags is { } tags)
                {
                    foreach (var kvp in tags)
                    {
                        tagKeys.Append(kvp.Key);
                        if (kvp.Value is null)
                            tagValues.AppendNull();
                        else
                            tagValues.Append(kvp.Value);
                        tagOffset++;
                    }
                    tagValidity.Append(true);
                }
                else
                {
                    tagValidity.Append(false);
                    tagNulls++;
                }
                tagOffsetsBuilder.Append(tagOffset);

                if (a.DeletionVector is { } dv)
                {
                    dvStorageBuilder.Append(dv.StorageType);
                    dvPathBuilder.Append(dv.PathOrInlineDv);
                    if (dv.Offset is { } off) dvOffsetBuilder.Append(off); else dvOffsetBuilder.AppendNull();
                    dvSizeBuilder.Append(dv.SizeInBytes);
                    dvCardBuilder.Append(dv.Cardinality);
                }
                else
                {
                    dvStorageBuilder.AppendNull();
                    dvPathBuilder.AppendNull();
                    dvOffsetBuilder.AppendNull();
                    dvSizeBuilder.AppendNull();
                    dvCardBuilder.AppendNull();
                }
                if (a.BaseRowId is { } bri) baseRowIdBuilder.Append(bri); else baseRowIdBuilder.AppendNull();
                if (a.DefaultRowCommitVersion is { } rcv) defaultRcvBuilder.Append(rcv); else defaultRcvBuilder.AppendNull();
            }
            else
            {
                pathBuilder.AppendNull();
                sizeBuilder.AppendNull();
                modTimeBuilder.AppendNull();
                dataChangeBuilder.AppendNull();
                statsBuilder.AppendNull();
                pvOffsetsBuilder.Append(pvOffset);
                tagValidity.Append(false);
                tagNulls++;
                tagOffsetsBuilder.Append(tagOffset);
                dvStorageBuilder.AppendNull();
                dvPathBuilder.AppendNull();
                dvOffsetBuilder.AppendNull();
                dvSizeBuilder.AppendNull();
                dvCardBuilder.AppendNull();
                baseRowIdBuilder.AppendNull();
                defaultRcvBuilder.AppendNull();
            }
        }

        var pvMapType = new ArrowMapType(
            new Field("key", StringType.Default, false),
            new Field("value", StringType.Default, true));
        var pvKeysArray = pvKeys.Build();
        var pvValuesArray = pvValues.Build();
        var pvEntries = new StructArray(
            new ArrowStructType(new List<Field> { pvMapType.KeyField, pvMapType.ValueField }),
            pvKeysArray.Length,
            new IArrowArray[] { pvKeysArray, pvValuesArray },
            ArrowBuffer.Empty);
        var pvMap = new MapArray(pvMapType, count,
            pvOffsetsBuilder.Build(), pvEntries, ArrowBuffer.Empty, 0);

        var tagMapType = new ArrowMapType(
            new Field("key", StringType.Default, false),
            new Field("value", StringType.Default, true));
        var tagKeysArray = tagKeys.Build();
        var tagValuesArray = tagValues.Build();
        var tagEntries = new StructArray(
            new ArrowStructType(new List<Field> { tagMapType.KeyField, tagMapType.ValueField }),
            tagKeysArray.Length,
            new IArrowArray[] { tagKeysArray, tagValuesArray },
            ArrowBuffer.Empty);
        var tagMap = new MapArray(tagMapType, count,
            tagOffsetsBuilder.Build(), tagEntries, tagValidity.Build(), tagNulls);

        var dvFields = new List<Field>
        {
            new Field("storageType", StringType.Default, true),
            new Field("pathOrInlineDv", StringType.Default, true),
            new Field("offset", Int32Type.Default, true),
            new Field("sizeInBytes", Int32Type.Default, true),
            new Field("cardinality", Int64Type.Default, true),
        };
        // The dv struct is NULLABLE (present only where the add carries a deletion vector) — its fields are
        // required in strict readers, so an always-present struct with null children is rejected.
        var dvValidity = new ArrowBuffer.BitmapBuilder(count);
        int dvNulls = 0;
        for (int i = 0; i < count; i++)
        {
            bool hasDv = actions[i] is AddFile af && af.DeletionVector is not null;
            dvValidity.Append(hasDv);
            if (!hasDv)
                dvNulls++;
        }
        var dvStruct = new StructArray(
            new ArrowStructType(dvFields),
            count,
            [dvStorageBuilder.Build(), dvPathBuilder.Build(), dvOffsetBuilder.Build(),
             dvSizeBuilder.Build(), dvCardBuilder.Build()],
            dvValidity.Build(), dvNulls);

        var fields = new List<Field>
        {
            new Field("path", StringType.Default, true),
            new Field("partitionValues", new ArrowMapType(
                new Field("key", StringType.Default, false),
                new Field("value", StringType.Default, true)), true),
            new Field("size", Int64Type.Default, true),
            new Field("modificationTime", Int64Type.Default, true),
            new Field("dataChange", BooleanType.Default, true),
            new Field("stats", StringType.Default, true),
            new Field("tags", tagMapType, true),
            new Field("deletionVector", new ArrowStructType(dvFields), true),
            new Field("baseRowId", Int64Type.Default, true),
            new Field("defaultRowCommitVersion", Int64Type.Default, true),
        };

        var (validity, nullCount) = BuildActionValidity<AddFile>(actions, count);
        return new StructArray(
            new ArrowStructType(fields),
            count,
            [pathBuilder.Build(), pvMap, sizeBuilder.Build(),
             modTimeBuilder.Build(), dataChangeBuilder.Build(), statsBuilder.Build(),
             tagMap, dvStruct, baseRowIdBuilder.Build(), defaultRcvBuilder.Build()],
            validity, nullCount);
    }

    /// <summary>
    /// Default tombstone retention (mirrors <c>delta.deletedFileRetentionDuration</c>'s default of
    /// one week); the table property is honored when parseable ("interval N days|hours|minutes|weeks").
    /// </summary>
    private static TimeSpan TombstoneRetention(IReadOnlyDictionary<string, string>? configuration)
    {
        if (configuration is not null
            && configuration.TryGetValue("delta.deletedFileRetentionDuration", out var raw)
            && raw is not null)
        {
            var parts = raw.Trim().Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
            if (parts.Length == 3
                && string.Equals(parts[0], "interval", StringComparison.OrdinalIgnoreCase)
                && long.TryParse(parts[1], out long n) && n >= 0)
            {
                switch (parts[2].ToLowerInvariant())
                {
                    case "week": case "weeks": return TimeSpan.FromDays(n * 7);
                    case "day": case "days": return TimeSpan.FromDays(n);
                    case "hour": case "hours": return TimeSpan.FromHours(n);
                    case "minute": case "minutes": return TimeSpan.FromMinutes(n);
                    case "second": case "seconds": return TimeSpan.FromSeconds(n);
                }
            }
        }
        return TimeSpan.FromDays(7);
    }

    private static StructArray BuildRemoveColumn(List<DeltaAction> actions, int count)
    {
        var pathBuilder = new StringArray.Builder();
        var tsBuilder = new Int64Array.Builder();
        var dcBuilder = new BooleanArray.Builder();
        var dvStorageBuilder = new StringArray.Builder();
        var dvPathBuilder = new StringArray.Builder();
        var dvOffsetBuilder = new Int32Array.Builder();
        var dvSizeBuilder = new Int32Array.Builder();
        var dvCardBuilder = new Int64Array.Builder();

        for (int i = 0; i < count; i++)
        {
            if (actions[i] is RemoveFile r)
            {
                pathBuilder.Append(r.Path);
                if (r.DeletionTimestamp is { } ts) tsBuilder.Append(ts); else tsBuilder.AppendNull();
                dcBuilder.Append(r.DataChange);
                if (r.DeletionVector is { } dv)
                {
                    dvStorageBuilder.Append(dv.StorageType);
                    dvPathBuilder.Append(dv.PathOrInlineDv);
                    if (dv.Offset is { } off) dvOffsetBuilder.Append(off); else dvOffsetBuilder.AppendNull();
                    dvSizeBuilder.Append(dv.SizeInBytes);
                    dvCardBuilder.Append(dv.Cardinality);
                }
                else
                {
                    dvStorageBuilder.AppendNull();
                    dvPathBuilder.AppendNull();
                    dvOffsetBuilder.AppendNull();
                    dvSizeBuilder.AppendNull();
                    dvCardBuilder.AppendNull();
                }
            }
            else
            {
                pathBuilder.AppendNull();
                tsBuilder.AppendNull();
                dcBuilder.AppendNull();
                dvStorageBuilder.AppendNull();
                dvPathBuilder.AppendNull();
                dvOffsetBuilder.AppendNull();
                dvSizeBuilder.AppendNull();
                dvCardBuilder.AppendNull();
            }
        }

        var dvFields = new List<Field>
        {
            new Field("storageType", StringType.Default, true),
            new Field("pathOrInlineDv", StringType.Default, true),
            new Field("offset", Int32Type.Default, true),
            new Field("sizeInBytes", Int32Type.Default, true),
            new Field("cardinality", Int64Type.Default, true),
        };
        // The dv struct is NULLABLE (present only where the remove carries a deletion vector) — its fields
        // are required in strict readers, so an always-present struct with null children is rejected.
        var dvValidity = new ArrowBuffer.BitmapBuilder(count);
        int dvNulls = 0;
        for (int i = 0; i < count; i++)
        {
            bool hasDv = actions[i] is RemoveFile rf && rf.DeletionVector is not null;
            dvValidity.Append(hasDv);
            if (!hasDv)
                dvNulls++;
        }
        var dvStruct = new StructArray(
            new ArrowStructType(dvFields),
            count,
            [dvStorageBuilder.Build(), dvPathBuilder.Build(), dvOffsetBuilder.Build(),
             dvSizeBuilder.Build(), dvCardBuilder.Build()],
            dvValidity.Build(), dvNulls);

        var fields = new List<Field>
        {
            new Field("path", StringType.Default, true),
            new Field("deletionTimestamp", Int64Type.Default, true),
            new Field("dataChange", BooleanType.Default, true),
            new Field("deletionVector", new ArrowStructType(dvFields), true),
        };

        var (validity, nullCount) = BuildActionValidity<RemoveFile>(actions, count);
        return new StructArray(
            new ArrowStructType(fields), count,
            [pathBuilder.Build(), tsBuilder.Build(), dcBuilder.Build(), dvStruct],
            validity, nullCount);
    }

    private static StructArray BuildTxnColumn(List<DeltaAction> actions, int count)
    {
        var appIdBuilder = new StringArray.Builder();
        var versionBuilder = new Int64Array.Builder();
        var lastUpdatedBuilder = new Int64Array.Builder();

        for (int i = 0; i < count; i++)
        {
            if (actions[i] is TransactionId t)
            {
                appIdBuilder.Append(t.AppId);
                versionBuilder.Append(t.Version);
                lastUpdatedBuilder.Append(t.LastUpdated ?? 0);
            }
            else
            {
                appIdBuilder.AppendNull();
                versionBuilder.AppendNull();
                lastUpdatedBuilder.AppendNull();
            }
        }

        var fields = new List<Field>
        {
            new Field("appId", StringType.Default, true),
            new Field("version", Int64Type.Default, true),
            new Field("lastUpdated", Int64Type.Default, true),
        };

        var (validity, nullCount) = BuildActionValidity<TransactionId>(actions, count);
        return new StructArray(
            new ArrowStructType(fields), count,
            [appIdBuilder.Build(), versionBuilder.Build(), lastUpdatedBuilder.Build()],
            validity, nullCount);
    }

    private static StructArray BuildDomainMetadataColumn(List<DeltaAction> actions, int count)
    {
        var domainBuilder = new StringArray.Builder();
        var configBuilder = new StringArray.Builder();
        var removedBuilder = new BooleanArray.Builder();

        for (int i = 0; i < count; i++)
        {
            if (actions[i] is Actions.DomainMetadata dm)
            {
                domainBuilder.Append(dm.Domain);
                configBuilder.Append(dm.Configuration);
                removedBuilder.Append(dm.Removed);
            }
            else
            {
                domainBuilder.AppendNull();
                configBuilder.AppendNull();
                removedBuilder.AppendNull();
            }
        }

        var fields = new List<Field>
        {
            new Field("domain", StringType.Default, true),
            new Field("configuration", StringType.Default, true),
            new Field("removed", BooleanType.Default, true),
        };

        var (validity, nullCount) = BuildActionValidity<Actions.DomainMetadata>(actions, count);
        return new StructArray(
            new ArrowStructType(fields), count,
            [domainBuilder.Build(), configBuilder.Build(), removedBuilder.Build()],
            validity, nullCount);
    }

    #endregion
}
