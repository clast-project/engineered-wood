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
        _parquetOptions = CheckpointParquetOptions.For(parquetOptions);
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

        // Disposed once written: the batch's buffers are native memory, so releasing them here rather
        // than at finalization keeps a checkpoint's peak footprint bounded.
        using var batch = BuildCheckpointBatch(snapshot, out long actionCount);

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

        // Which copies of the statistics this table asks for. stats_parsed is dropped when the schema
        // yields no bounds at all, so the mode alone does not decide the shape.
        var statsMode = CheckpointStatsMode.FromConfiguration(snapshot.Metadata.Configuration);
        var statsParsedType = statsMode.WriteStruct
            ? StatsParsedBuilder.BuildStatsType(snapshot.Schema)
            : null;

        // Build the struct-based checkpoint schema
        var schema = BuildCheckpointSchema(statsMode, statsParsedType);

        // Build struct arrays for each action type
        var protocolArray = BuildProtocolColumn(allActions, count);
        var metadataArray = BuildMetadataColumn(allActions, count);
        var addArray = BuildAddColumn(allActions, count, statsMode, snapshot.Schema, statsParsedType);
        var removeArray = BuildRemoveColumn(allActions, count);
        var txnArray = BuildTxnColumn(allActions, count);
        var domainMetadataArray = BuildDomainMetadataColumn(allActions, count);

        return new RecordBatch(schema,
            [protocolArray, metadataArray, addArray, removeArray, txnArray, domainMetadataArray],
            count);
    }

    #region Schema Definition

    /// <summary>
    /// The deletion-vector struct, shared by <c>add</c> and <c>remove</c>.
    /// </summary>
    private static List<Field> DeletionVectorFields() =>
    [
        new Field("storageType", StringType.Default, true),
        new Field("pathOrInlineDv", StringType.Default, true),
        new Field("offset", Int32Type.Default, true),
        new Field("sizeInBytes", Int32Type.Default, true),
        new Field("cardinality", Int64Type.Default, true),
    ];

    /// <summary>
    /// The <c>add</c> struct's fields. Both statistics columns are optional and controlled by the
    /// table's properties: <c>stats</c> (JSON) and <c>stats_parsed</c> (typed). Delta places the typed
    /// struct INSIDE this struct — a sibling column beside <c>add</c> is invisible to its readers —
    /// and omits <c>stats</c> entirely, rather than nulling it, when JSON stats are turned off.
    /// </summary>
    private static List<Field> BuildAddFields(
        CheckpointStatsMode statsMode, ArrowStructType? statsParsedType)
    {
        var fields = new List<Field>
        {
            new Field("path", StringType.Default, true),
            new Field("partitionValues", new ArrowMapType(
                new Field("key", StringType.Default, false),
                new Field("value", StringType.Default, true)), true),
            new Field("size", Int64Type.Default, true),
            new Field("modificationTime", Int64Type.Default, true),
            new Field("dataChange", BooleanType.Default, true),
        };

        if (statsMode.WriteJson)
            fields.Add(new Field("stats", StringType.Default, true));

        fields.Add(new Field("tags", new ArrowMapType(
            new Field("key", StringType.Default, false),
            new Field("value", StringType.Default, true)), true));
        fields.Add(new Field("deletionVector", new ArrowStructType(DeletionVectorFields()), true));
        fields.Add(new Field("baseRowId", Int64Type.Default, true));
        fields.Add(new Field("defaultRowCommitVersion", Int64Type.Default, true));

        if (statsParsedType is not null)
            fields.Add(new Field("stats_parsed", statsParsedType, true));

        return fields;
    }

    private static Apache.Arrow.Schema BuildCheckpointSchema(
        CheckpointStatsMode statsMode, ArrowStructType? statsParsedType)
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
        var dvType = new ArrowStructType(DeletionVectorFields());
        var addType = new ArrowStructType(BuildAddFields(statsMode, statsParsedType));

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

        return new Apache.Arrow.Schema.Builder()
            .Field(new Field("protocol", protocolType, true))
            .Field(new Field("metaData", metadataType, true))
            .Field(new Field("add", addType, true))
            .Field(new Field("remove", removeType, true))
            .Field(new Field("txn", txnType, true))
            .Field(new Field("domainMetadata", domainMetadataType, true))
            .Build();
    }

    #endregion

    #region Array Builders

    /// <summary>
    /// Initial capacity for a nested child column (list elements, map keys/values) fed by the one
    /// protocol / metaData row in a checkpoint. The columns grow on demand.
    /// </summary>
    private const int NestedChildCapacity = 16;

    /// <summary>A zero-length string array (used for the always-empty <c>format.options</c> map entries).</summary>
    private static StringArray EmptyStringArray()
    {
        using var empty = new StringColumn(0);
        return empty.Build();
    }

    // Validity bitmap for a top-level action struct: TRUE exactly on the rows of that action type. The spec
    // checkpoint schema makes each action struct NULLABLE (null on rows of other action types) with required
    // fields inside — strict readers (delta-kernel) reject an always-present struct with null required
    // children ("unmasked nulls for non-nullable field"). Relies on the parquet writer handling nullable
    // structs correctly (the NestedLevelWriter null-struct fix).
    private static (ArrowBuffer Bitmap, int NullCount) BuildActionValidity<T>(
        List<DeltaAction> actions, int count) where T : DeltaAction
    {
        using var validity = new ValidityBuilder(count);
        for (int i = 0; i < count; i++)
            validity.Append(actions[i] is T);
        int nullCount = validity.NullCount;
        return (validity.Build(), nullCount);
    }

    private static StructArray BuildProtocolColumn(List<DeltaAction> actions, int count)
    {
        using var minReaderBuilder = new FixedWidthColumn<int>(count);
        using var minWriterBuilder = new FixedWidthColumn<int>(count);
        using var rfOffsets = new OffsetsBuilder(count);
        using var rfValues = new StringColumn(NestedChildCapacity);
        using var rfValidity = new ValidityBuilder(count);
        using var wfOffsets = new OffsetsBuilder(count);
        using var wfValues = new StringColumn(NestedChildCapacity);
        using var wfValidity = new ValidityBuilder(count);

        for (int i = 0; i < count; i++)
        {
            if (actions[i] is ProtocolAction p)
            {
                minReaderBuilder.Append(p.MinReaderVersion);
                minWriterBuilder.Append(p.MinWriterVersion);
                if (p.ReaderFeatures is { } rf)
                {
                    int n = 0;
                    foreach (var f in rf)
                    {
                        rfValues.Append(f);
                        n++;
                    }
                    rfOffsets.Append(n);
                    rfValidity.Append(true);
                }
                else
                {
                    rfOffsets.AppendEmpty();
                    rfValidity.Append(false);
                }
                if (p.WriterFeatures is { } wf)
                {
                    int n = 0;
                    foreach (var f in wf)
                    {
                        wfValues.Append(f);
                        n++;
                    }
                    wfOffsets.Append(n);
                    wfValidity.Append(true);
                }
                else
                {
                    wfOffsets.AppendEmpty();
                    wfValidity.Append(false);
                }
            }
            else
            {
                minReaderBuilder.AppendNull();
                minWriterBuilder.AppendNull();
                rfOffsets.AppendEmpty();
                rfValidity.Append(false);
                wfOffsets.AppendEmpty();
                wfValidity.Append(false);
            }
        }

        var featureListType = new ListType(new Field("element", StringType.Default, false));
        int rfNulls = rfValidity.NullCount;
        int wfNulls = wfValidity.NullCount;
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
            [minReaderBuilder.Build(Int32Type.Default), minWriterBuilder.Build(Int32Type.Default),
             rfList, wfList],
            validity, nullCount);
    }

    private static StructArray BuildMetadataColumn(List<DeltaAction> actions, int count)
    {
        // Exactly one row of a checkpoint is the metaData action, so these value buffers start at their
        // minimum and grow for that row rather than reserving bytes per action.
        using var idBuilder = new StringColumn(count, bytesPerValueHint: 0);
        using var nameBuilder = new StringColumn(count, bytesPerValueHint: 0);
        using var descBuilder = new StringColumn(count, bytesPerValueHint: 0);
        using var schemaStringBuilder = new StringColumn(count, bytesPerValueHint: 0);
        using var createdTimeBuilder = new FixedWidthColumn<long>(count);

        // Format struct arrays
        using var formatProviderBuilder = new StringColumn(count, bytesPerValueHint: 0);

        // partitionColumns list
        using var partColOffsetsBuilder = new OffsetsBuilder(count);
        using var partColValues = new StringColumn(NestedChildCapacity);

        // configuration map
        using var configOffsetsBuilder = new OffsetsBuilder(count);
        using var configKeys = new StringColumn(NestedChildCapacity);
        using var configValues = new StringColumn(NestedChildCapacity);

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

                int partColCount = 0;
                foreach (string col in m.PartitionColumns)
                {
                    partColValues.Append(col);
                    partColCount++;
                }
                partColOffsetsBuilder.Append(partColCount);

                int configCount = 0;
                if (m.Configuration is not null)
                {
                    foreach (var kvp in m.Configuration)
                    {
                        configKeys.Append(kvp.Key);
                        configValues.Append(kvp.Value);
                        configCount++;
                    }
                }
                configOffsetsBuilder.Append(configCount);
            }
            else
            {
                idBuilder.AppendNull();
                nameBuilder.AppendNull();
                descBuilder.AppendNull();
                schemaStringBuilder.AppendNull();
                createdTimeBuilder.AppendNull();
                formatProviderBuilder.AppendNull();
                partColOffsetsBuilder.AppendEmpty();
                configOffsetsBuilder.AppendEmpty();
            }
        }

        // format.options: always-empty map (REQUIRED field for strict readers — delta-kernel rejects a
        // checkpoint whose format struct lacks it).
        var optMapType = new ArrowMapType(
            new Field("key", StringType.Default, false),
            new Field("value", StringType.Default, true));
        using var optOffsets = new OffsetsBuilder(count);
        for (int i = 0; i < count; i++)
            optOffsets.AppendEmpty();
        var optEntries = new StructArray(
            new ArrowStructType(new List<Field> { optMapType.KeyField, optMapType.ValueField }),
            0,
            new IArrowArray[] { EmptyStringArray(), EmptyStringArray() },
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
        StringArray configKeysArray = configKeys.Build();
        StringArray configValuesArray = configValues.Build();
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
             createdTimeBuilder.Build(Int64Type.Default), configMap],
            validity, nullCount);
    }

    private static StructArray BuildAddColumn(
        List<DeltaAction> actions,
        int count,
        CheckpointStatsMode statsMode,
        Schema.StructType deltaSchema,
        ArrowStructType? statsParsedType)
    {
        using var pathBuilder = new StringColumn(count, bytesPerValueHint: 64);
        using var sizeBuilder = new FixedWidthColumn<long>(count);
        using var modTimeBuilder = new FixedWidthColumn<long>(count);
        using var dataChangeBuilder = new BooleanColumn(count);
        using var statsBuilder = new StringColumn(count, bytesPerValueHint: 256);
        using var dvStorageBuilder = new StringColumn(count, bytesPerValueHint: 0);
        using var dvPathBuilder = new StringColumn(count, bytesPerValueHint: 0);
        using var dvOffsetBuilder = new FixedWidthColumn<int>(count);
        using var dvSizeBuilder = new FixedWidthColumn<int>(count);
        using var dvCardBuilder = new FixedWidthColumn<long>(count);
        using var baseRowIdBuilder = new FixedWidthColumn<long>(count);
        using var defaultRcvBuilder = new FixedWidthColumn<long>(count);

        // partitionValues map
        using var pvOffsetsBuilder = new OffsetsBuilder(count);
        using var pvKeys = new StringColumn(count);
        using var pvValues = new StringColumn(count);

        // tags map (null per row when the add carries no tags)
        using var tagOffsetsBuilder = new OffsetsBuilder(count);
        using var tagKeys = new StringColumn(count);
        using var tagValues = new StringColumn(count);
        using var tagValidity = new ValidityBuilder(count);

        for (int i = 0; i < count; i++)
        {
            if (actions[i] is AddFile a)
            {
                pathBuilder.Append(a.Path);
                sizeBuilder.Append(a.Size);
                modTimeBuilder.Append(a.ModificationTime);
                dataChangeBuilder.Append(a.DataChange);
                statsBuilder.AppendOrNull(a.Stats);

                int pvCount = 0;
                foreach (var kvp in a.PartitionValues)
                {
                    pvKeys.Append(kvp.Key);
                    pvValues.AppendOrNull(kvp.Value);
                    pvCount++;
                }
                pvOffsetsBuilder.Append(pvCount);

                if (a.Tags is { } tags)
                {
                    int tagCount = 0;
                    foreach (var kvp in tags)
                    {
                        tagKeys.Append(kvp.Key);
                        tagValues.AppendOrNull(kvp.Value);
                        tagCount++;
                    }
                    tagOffsetsBuilder.Append(tagCount);
                    tagValidity.Append(true);
                }
                else
                {
                    tagOffsetsBuilder.AppendEmpty();
                    tagValidity.Append(false);
                }

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
                pvOffsetsBuilder.AppendEmpty();
                tagOffsetsBuilder.AppendEmpty();
                tagValidity.Append(false);
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
        StringArray pvKeysArray = pvKeys.Build();
        StringArray pvValuesArray = pvValues.Build();
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
        StringArray tagKeysArray = tagKeys.Build();
        StringArray tagValuesArray = tagValues.Build();
        var tagEntries = new StructArray(
            new ArrowStructType(new List<Field> { tagMapType.KeyField, tagMapType.ValueField }),
            tagKeysArray.Length,
            new IArrowArray[] { tagKeysArray, tagValuesArray },
            ArrowBuffer.Empty);
        int tagNulls = tagValidity.NullCount;
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
        using var dvValidity = new ValidityBuilder(count);
        for (int i = 0; i < count; i++)
            dvValidity.Append(actions[i] is AddFile af && af.DeletionVector is not null);
        int dvNulls = dvValidity.NullCount;
        var dvStruct = new StructArray(
            new ArrowStructType(dvFields),
            count,
            [dvStorageBuilder.Build(), dvPathBuilder.Build(),
             dvOffsetBuilder.Build(Int32Type.Default), dvSizeBuilder.Build(Int32Type.Default),
             dvCardBuilder.Build(Int64Type.Default)],
            dvValidity.Build(), dvNulls);

        // Children in the same order as BuildAddFields — both statistics columns are optional.
        var children = new List<IArrowArray>
        {
            pathBuilder.Build(), pvMap, sizeBuilder.Build(Int64Type.Default),
            modTimeBuilder.Build(Int64Type.Default), dataChangeBuilder.Build(),
        };
        if (statsMode.WriteJson)
            children.Add(statsBuilder.Build());
        children.Add(tagMap);
        children.Add(dvStruct);
        children.Add(baseRowIdBuilder.Build(Int64Type.Default));
        children.Add(defaultRcvBuilder.Build(Int64Type.Default));
        if (statsParsedType is not null)
        {
            var statsParsed = StatsParsedBuilder.BuildStatsColumn(actions, count, deltaSchema)
                ?? throw new InvalidOperationException(
                    "stats_parsed type was resolved but the column could not be built.");
            children.Add(statsParsed);
        }

        var (validity, nullCount) = BuildActionValidity<AddFile>(actions, count);
        return new StructArray(
            new ArrowStructType(BuildAddFields(statsMode, statsParsedType)),
            count,
            children,
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
        using var pathBuilder = new StringColumn(count, bytesPerValueHint: 64);
        using var tsBuilder = new FixedWidthColumn<long>(count);
        using var dcBuilder = new BooleanColumn(count);
        using var dvStorageBuilder = new StringColumn(count, bytesPerValueHint: 0);
        using var dvPathBuilder = new StringColumn(count, bytesPerValueHint: 0);
        using var dvOffsetBuilder = new FixedWidthColumn<int>(count);
        using var dvSizeBuilder = new FixedWidthColumn<int>(count);
        using var dvCardBuilder = new FixedWidthColumn<long>(count);

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
        using var dvValidity = new ValidityBuilder(count);
        for (int i = 0; i < count; i++)
            dvValidity.Append(actions[i] is RemoveFile rf && rf.DeletionVector is not null);
        int dvNulls = dvValidity.NullCount;
        var dvStruct = new StructArray(
            new ArrowStructType(dvFields),
            count,
            [dvStorageBuilder.Build(), dvPathBuilder.Build(),
             dvOffsetBuilder.Build(Int32Type.Default), dvSizeBuilder.Build(Int32Type.Default),
             dvCardBuilder.Build(Int64Type.Default)],
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
            [pathBuilder.Build(), tsBuilder.Build(Int64Type.Default), dcBuilder.Build(), dvStruct],
            validity, nullCount);
    }

    private static StructArray BuildTxnColumn(List<DeltaAction> actions, int count)
    {
        using var appIdBuilder = new StringColumn(count, bytesPerValueHint: 0);
        using var versionBuilder = new FixedWidthColumn<long>(count);
        using var lastUpdatedBuilder = new FixedWidthColumn<long>(count);

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
            [appIdBuilder.Build(), versionBuilder.Build(Int64Type.Default),
             lastUpdatedBuilder.Build(Int64Type.Default)],
            validity, nullCount);
    }

    private static StructArray BuildDomainMetadataColumn(List<DeltaAction> actions, int count)
    {
        using var domainBuilder = new StringColumn(count, bytesPerValueHint: 0);
        using var configBuilder = new StringColumn(count, bytesPerValueHint: 0);
        using var removedBuilder = new BooleanColumn(count);

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
