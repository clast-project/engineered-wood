// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using Apache.Arrow;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.IO;
using EngineeredWood.Parquet;

namespace EngineeredWood.DeltaLake.Checkpoint;

/// <summary>
/// Reads Delta Lake checkpoint files (Parquet format) and the
/// <c>_last_checkpoint</c> file.
/// </summary>
public sealed class CheckpointReader
{
    private readonly ITableFileSystem _fs;

    public CheckpointReader(ITableFileSystem fileSystem)
    {
        _fs = fileSystem;
    }

    /// <summary>
    /// Reads the <c>_last_checkpoint</c> file. Returns null if it is absent, unreadable or unusable.
    /// </summary>
    /// <remarks>
    /// <para><c>_last_checkpoint</c> is an advisory HINT: it only saves the reader from finding the newest
    /// checkpoint itself, so every way of failing to read it means what absence means — no hint, replay
    /// from the log. Failing the caller over it would turn a hint file into a failed commit.</para>
    /// <para>That is reachable whenever an <see cref="ITableFileSystem"/> updates the file non-atomically
    /// (the local filesystem truncates before writing; an ADLS create/append/flush is three calls), leaving
    /// a window in which a concurrent reader sees it empty, truncated, or fails the read outright. Measured
    /// on Fabric OneLake, 2026-07-31: 8 concurrent writers × 12 commits killed 2 of them on the empty-file
    /// window. The three cloud backends in this repo upload in one request and so cannot expose it.</para>
    /// </remarks>
    public async ValueTask<LastCheckpointInfo?> ReadLastCheckpointAsync(
        CancellationToken cancellationToken = default)
    {
        try
        {
            // No Exists probe first: absence throws here like any other unusable hint, and skipping it
            // saves a round-trip per snapshot build.
            byte[] data = await _fs.ReadAllBytesAsync(
                DeltaVersion.LastCheckpointPath, cancellationToken).ConfigureAwait(false);

            using var doc = JsonDocument.Parse(data);
            var root = doc.RootElement;

            string? v2Path = null;
            if (root.TryGetProperty("v2Checkpoint", out var v2))
            {
                if (v2.TryGetProperty("path", out var pathProp))
                    v2Path = pathProp.GetString();
            }

            return new LastCheckpointInfo
            {
                Version = root.GetProperty("version").GetInt64(),
                Size = root.GetProperty("size").GetInt64(),
                Parts = root.TryGetProperty("parts", out var parts) ? parts.GetInt32() : null,
                SizeInBytes = root.TryGetProperty("sizeInBytes", out var sib)
                    ? sib.GetInt64() : null,
                NumOfAddFiles = root.TryGetProperty("numOfAddFiles", out var naf)
                    ? naf.GetInt32() : null,
                V2CheckpointPath = v2Path,
            };
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // The caller's own intent, not a missing hint. Guarded on the token because a store's request
            // timeout also arrives as TaskCanceledException, and that IS just an unreadable hint.
            throw;
        }
        catch (Exception)
        {
            // Deliberately broad, and scoped to the whole read-and-decode: absent, a filesystem-specific
            // read failure this layer must not have to name (on ADLS a torn ranged read surfaces as 412
            // ConditionNotMet), unparseable bytes, a root that is not an object, a missing required field,
            // or one of the wrong type. Every one of them is a hint we cannot use.
            return null;
        }
    }

    /// <summary>
    /// Finds the newest checkpoint at or below <paramref name="maxVersion"/> by listing
    /// <c>_delta_log</c>, or null if there is none. This is the fallback for an absent, unusable or
    /// stale <c>_last_checkpoint</c>: the log directory is the truth that file only summarizes.
    /// </summary>
    /// <remarks>
    /// A multi-part checkpoint counts only when every one of its parts is present — a writer that died
    /// midway leaves a prefix, and bootstrapping from that would silently drop the files in the missing
    /// parts. <c>Size</c> is reported as 0 because only the file listing is available here; it is
    /// informational and nothing on the read path consumes it.
    /// </remarks>
    public async ValueTask<LastCheckpointInfo?> FindLatestCheckpointAsync(
        long maxVersion, CancellationToken cancellationToken = default)
    {
        const string Marker = ".checkpoint.";

        var classic = new HashSet<long>();
        var v2 = new Dictionary<long, string>();
        // version -> declared part count -> the part numbers actually seen
        var multiPart = new Dictionary<long, Dictionary<int, HashSet<int>>>();

        await foreach (var file in _fs.ListAsync(DeltaVersion.LogPrefix, cancellationToken)
            .ConfigureAwait(false))
        {
            string name = Path.GetFileName(file.Path);
            if (!DeltaVersion.TryParseCheckpointVersion(name, out long version) || version > maxVersion)
                continue;

            string[] suffix = name[(name.IndexOf(Marker, StringComparison.OrdinalIgnoreCase)
                + Marker.Length)..].Split('.');

            // <version>.checkpoint.parquet
            if (suffix is ["parquet"])
            {
                classic.Add(version);
            }
            // <version>.checkpoint.<part>.<total>.parquet
            else if (suffix is [var p, var t, "parquet"] &&
                     int.TryParse(p, out int part) && int.TryParse(t, out int total) && total > 0)
            {
                if (!multiPart.TryGetValue(version, out var byTotal))
                    multiPart[version] = byTotal = [];
                if (!byTotal.TryGetValue(total, out var seen))
                    byTotal[total] = seen = [];
                seen.Add(part);
            }
            // <version>.checkpoint.<uuid>.json. The parquet-bodied V2 form is deliberately not claimed:
            // ReadV2CheckpointAsync only decodes NDJSON, so claiming it would just fail the read.
            else if (suffix is [_, "json"])
            {
                v2[version] = DeltaVersion.LogPrefix + name;
            }
        }

        var versions = new SortedSet<long>(classic);
        versions.UnionWith(v2.Keys);
        versions.UnionWith(multiPart.Keys);

        foreach (long version in versions.Reverse())
        {
            // Classic first: it is the one form every reader here handles without further lookups.
            if (classic.Contains(version))
                return new LastCheckpointInfo { Version = version, Size = 0 };

            if (multiPart.TryGetValue(version, out var byTotal))
            {
                foreach (var (total, seen) in byTotal)
                {
                    if (seen.Count == total)
                        return new LastCheckpointInfo { Version = version, Size = 0, Parts = total };
                }
            }

            if (v2.TryGetValue(version, out string? path))
                return new LastCheckpointInfo { Version = version, Size = 0, V2CheckpointPath = path };
        }

        return null;
    }

    /// <summary>
    /// Reads all actions from a checkpoint file (single or multi-part).
    /// Returns the actions as <see cref="DeltaAction"/> objects.
    /// </summary>
    public async ValueTask<IReadOnlyList<DeltaAction>> ReadCheckpointAsync(
        LastCheckpointInfo info, CancellationToken cancellationToken = default)
    {
        var actions = new List<DeltaAction>();

        if (info.IsV2)
        {
            // V2 checkpoint: JSON NDJSON file, possibly with sidecars
            await ReadV2CheckpointAsync(info.V2CheckpointPath!, actions, cancellationToken)
                .ConfigureAwait(false);
        }
        else if (info.Parts.HasValue)
        {
            // Multi-part V1 checkpoint
            for (int i = 1; i <= info.Parts.Value; i++)
            {
                string path = DeltaVersion.CheckpointPartPath(
                    info.Version, i, info.Parts.Value);
                await ReadCheckpointFileAsync(path, actions, cancellationToken)
                    .ConfigureAwait(false);
            }
        }
        else
        {
            // Single-file V1 checkpoint (Parquet)
            string path = DeltaVersion.CheckpointPath(info.Version);
            await ReadCheckpointFileAsync(path, actions, cancellationToken)
                .ConfigureAwait(false);
        }

        return actions;
    }

    /// <summary>
    /// Reads a V2 JSON checkpoint file (NDJSON) and resolves any sidecar references.
    /// </summary>
    private async ValueTask ReadV2CheckpointAsync(
        string path, List<DeltaAction> actions,
        CancellationToken cancellationToken)
    {
        byte[] data = await _fs.ReadAllBytesAsync(path, cancellationToken)
            .ConfigureAwait(false);

        var parsedActions = Log.ActionSerializer.Deserialize(data);
        var sidecarActions = new List<SidecarFile>();

        foreach (var action in parsedActions)
        {
            if (action is SidecarFile sidecar)
                sidecarActions.Add(sidecar);
            else if (action is not CheckpointMetadata) // Skip checkpointMetadata
                actions.Add(action);
        }

        // Resolve sidecars: read each sidecar Parquet file for add/remove actions
        foreach (var sidecar in sidecarActions)
        {
            string sidecarPath = sidecar.Path.Contains('/')
                ? sidecar.Path
                : $"_delta_log/_sidecars/{sidecar.Path}";

            await ReadCheckpointFileAsync(sidecarPath, actions, cancellationToken)
                .ConfigureAwait(false);
        }
    }

    private async ValueTask ReadCheckpointFileAsync(
        string path, List<DeltaAction> actions,
        CancellationToken cancellationToken)
    {
        await using var file = await _fs.OpenReadAsync(path, cancellationToken)
            .ConfigureAwait(false);
        using var reader = new ParquetFileReader(file, ownsFile: false);

        await foreach (var batch in reader.ReadAllAsync(cancellationToken: cancellationToken)
            .ConfigureAwait(false))
        {
            ConvertCheckpointBatch(batch, actions);
        }
    }

    /// <summary>
    /// Converts a Parquet RecordBatch from a checkpoint file into Delta actions.
    /// The checkpoint schema has top-level columns for each action type.
    /// </summary>
    private static void ConvertCheckpointBatch(RecordBatch batch, List<DeltaAction> actions)
    {
        // Checkpoint Parquet files have a flattened schema with columns like:
        // txn.appId, txn.version, txn.lastUpdated
        // add.path, add.partitionValues, add.size, add.modificationTime, add.dataChange, add.stats, ...
        // remove.path, remove.deletionTimestamp, remove.dataChange, ...
        // metaData.id, metaData.name, metaData.description, metaData.format, ...
        // protocol.minReaderVersion, protocol.minWriterVersion, ...
        //
        // However, in practice, checkpoints use a struct-column layout:
        // "txn" (struct), "add" (struct), "remove" (struct),
        // "metaData" (struct), "protocol" (struct), "commitInfo" (struct)
        //
        // Each row has exactly one non-null struct column representing the action.

        int txnIdx = batch.Schema.GetFieldIndex("txn");
        int addIdx = batch.Schema.GetFieldIndex("add");
        int removeIdx = batch.Schema.GetFieldIndex("remove");
        int metaDataIdx = batch.Schema.GetFieldIndex("metaData");
        int protocolIdx = batch.Schema.GetFieldIndex("protocol");
        int domainMetadataIdx = batch.Schema.GetFieldIndex("domainMetadata");

        // Located once per batch, then shared by every add row in it.
        var statsView = addIdx >= 0
            ? CheckpointStatsView.TryCreate((Apache.Arrow.StructArray)batch.Column(addIdx))
            : null;

        for (int row = 0; row < batch.Length; row++)
        {
            // Detect which action type is present by checking a key inner field.
            // We check inner fields instead of the struct itself because
            // Parquet nested column round-trips may not preserve struct-level
            // null bitmaps independently from child validity.
            if (protocolIdx >= 0 && HasStructValue(batch, protocolIdx, "minReaderVersion", row))
            {
                actions.Add(ExtractProtocol(batch, protocolIdx, row));
            }
            else if (metaDataIdx >= 0 && HasStructValue(batch, metaDataIdx, "id", row))
            {
                actions.Add(ExtractMetadata(batch, metaDataIdx, row));
            }
            else if (addIdx >= 0 && HasStructValue(batch, addIdx, "path", row))
            {
                actions.Add(ExtractAdd(batch, addIdx, row, statsView));
            }
            else if (removeIdx >= 0 && HasStructValue(batch, removeIdx, "path", row))
            {
                actions.Add(ExtractRemove(batch, removeIdx, row));
            }
            else if (txnIdx >= 0 && HasStructValue(batch, txnIdx, "appId", row))
            {
                actions.Add(ExtractTxn(batch, txnIdx, row));
            }
            else if (domainMetadataIdx >= 0 && HasStructValue(batch, domainMetadataIdx, "domain", row))
            {
                actions.Add(ExtractDomainMetadata(batch, domainMetadataIdx, row));
            }
        }
    }

    private static AddFile ExtractAdd(
        RecordBatch batch, int colIdx, int row, CheckpointStatsView? statsView = null)
    {
        var structArray = (Apache.Arrow.StructArray)batch.Column(colIdx);

        string path = GetStringField(structArray, "path", row) ?? "";
        long size = GetInt64Field(structArray, "size", row) ?? 0;
        long modTime = GetInt64Field(structArray, "modificationTime", row) ?? 0;
        bool dataChange = GetBoolField(structArray, "dataChange", row) ?? false;
        string? stats = GetStringField(structArray, "stats", row);

        // partitionValues is a map<string, string>
        var partitionValues = GetStringMapField(structArray, "partitionValues", row);

        int tagsIdx = FindFieldIndex(structArray, "tags");
        Dictionary<string, string>? tags = null;
        if (tagsIdx >= 0 && !structArray.Fields[tagsIdx].IsNull(row))
        {
            var t = GetStringMapField(structArray, "tags", row);
            if (t.Count > 0)
                tags = t;
        }

        // deletionVector (nested struct; null storageType = none) + row-tracking fields. A checkpoint written
        // before these were preserved simply lacks the columns — the lookups return null and the add behaves as
        // before (but its DV/base-row-id information is already lost in that checkpoint).
        DeletionVector? dv = null;
        int dvIdx = ((Apache.Arrow.Types.StructType)structArray.Data.DataType).GetFieldIndex("deletionVector");
        if (dvIdx >= 0)
        {
            var dvArray = (Apache.Arrow.StructArray)ArrowArrayFactory.BuildArray(structArray.Data.Children[dvIdx]);
            int dvRow = row + structArray.Data.Offset;
            string? storageType = GetStringField(dvArray, "storageType", dvRow);
            if (!string.IsNullOrEmpty(storageType))
            {
                dv = new DeletionVector
                {
                    StorageType = storageType!, // guarded by IsNullOrEmpty above (no flow attribute on netstandard2.0)
                    PathOrInlineDv = GetStringField(dvArray, "pathOrInlineDv", dvRow) ?? "",
                    Offset = (int?)GetInt32Field(dvArray, "offset", dvRow),
                    SizeInBytes = (int)(GetInt32Field(dvArray, "sizeInBytes", dvRow) ?? 0),
                    Cardinality = GetInt64Field(dvArray, "cardinality", dvRow) ?? 0,
                };
            }
        }

        return new AddFile
        {
            Path = path,
            PartitionValues = partitionValues,
            Size = size,
            ModificationTime = modTime,
            DataChange = dataChange,
            Stats = stats,
            TypedStats = statsView is not null ? new ParsedStatsRef(statsView, row) : null,
            Tags = tags,
            DeletionVector = dv,
            BaseRowId = GetInt64Field(structArray, "baseRowId", row),
            DefaultRowCommitVersion = GetInt64Field(structArray, "defaultRowCommitVersion", row),
        };
    }

    private static RemoveFile ExtractRemove(RecordBatch batch, int colIdx, int row)
    {
        var structArray = (Apache.Arrow.StructArray)batch.Column(colIdx);

        string path = GetStringField(structArray, "path", row) ?? "";
        long? deletionTimestamp = GetInt64Field(structArray, "deletionTimestamp", row);
        bool dataChange = GetBoolField(structArray, "dataChange", row) ?? false;

        var partitionValues = GetStringMapField(structArray, "partitionValues", row);

        // deletionVector (nested struct; null storageType = none). Preserved so a tombstone read back from a
        // checkpoint keeps its DV reference for the next checkpoint and for VACUUM retention safety.
        DeletionVector? dv = null;
        int dvIdx = ((Apache.Arrow.Types.StructType)structArray.Data.DataType).GetFieldIndex("deletionVector");
        if (dvIdx >= 0)
        {
            var dvArray = (Apache.Arrow.StructArray)ArrowArrayFactory.BuildArray(structArray.Data.Children[dvIdx]);
            int dvRow = row + structArray.Data.Offset;
            string? storageType = GetStringField(dvArray, "storageType", dvRow);
            if (!string.IsNullOrEmpty(storageType))
            {
                dv = new DeletionVector
                {
                    StorageType = storageType!, // guarded by IsNullOrEmpty above (no flow attribute on netstandard2.0)
                    PathOrInlineDv = GetStringField(dvArray, "pathOrInlineDv", dvRow) ?? "",
                    Offset = (int?)GetInt32Field(dvArray, "offset", dvRow),
                    SizeInBytes = (int)(GetInt32Field(dvArray, "sizeInBytes", dvRow) ?? 0),
                    Cardinality = GetInt64Field(dvArray, "cardinality", dvRow) ?? 0,
                };
            }
        }

        return new RemoveFile
        {
            Path = path,
            DeletionTimestamp = deletionTimestamp,
            DataChange = dataChange,
            PartitionValues = partitionValues.Count > 0 ? partitionValues : null,
            DeletionVector = dv,
        };
    }

    private static ProtocolAction ExtractProtocol(RecordBatch batch, int colIdx, int row)
    {
        var structArray = (Apache.Arrow.StructArray)batch.Column(colIdx);

        return new ProtocolAction
        {
            MinReaderVersion = (int)(GetInt32Field(structArray, "minReaderVersion", row) ?? 1),
            MinWriterVersion = (int)(GetInt32Field(structArray, "minWriterVersion", row) ?? 2),
            // The feature lists must round-trip: a snapshot rebuilt from a checkpoint that drops
            // them would lose the table's feature declarations (nullable — absent below v3/v7).
            ReaderFeatures = GetStringListFieldOrNull(structArray, "readerFeatures", row),
            WriterFeatures = GetStringListFieldOrNull(structArray, "writerFeatures", row),
        };
    }

    private static MetadataAction ExtractMetadata(RecordBatch batch, int colIdx, int row)
    {
        var structArray = (Apache.Arrow.StructArray)batch.Column(colIdx);

        string id = GetStringField(structArray, "id", row) ?? "";
        string? name = GetStringField(structArray, "name", row);
        string? description = GetStringField(structArray, "description", row);
        string schemaString = GetStringField(structArray, "schemaString", row) ?? "{}";
        long? createdTime = GetInt64Field(structArray, "createdTime", row);

        // partitionColumns is a list<string>
        var partitionColumns = GetStringListField(structArray, "partitionColumns", row);

        // format is a struct with provider and options
        var format = Format.Parquet; // Default
        int formatIdx = FindFieldIndex(structArray, "format");
        if (formatIdx >= 0 && !structArray.Fields[formatIdx].IsNull(row))
        {
            var formatStruct = (Apache.Arrow.StructArray)structArray.Fields[formatIdx];
            string provider = GetStringField(formatStruct, "provider", row) ?? "parquet";
            format = new Format { Provider = provider };
        }

        // configuration is a map<string,string> — DROPPING it loses delta.enableDeletionVectors /
        // enableChangeDataFeed / columnMapping.mode / maxColumnId / retention settings after the first
        // checkpoint (DV-mode misdetection, mapped tables falling back to mode=none, ...).
        var configuration = GetStringMapField(structArray, "configuration", row);

        return new MetadataAction
        {
            Id = id,
            Name = name,
            Description = description,
            Format = format,
            SchemaString = schemaString,
            PartitionColumns = partitionColumns,
            CreatedTime = createdTime,
            Configuration = configuration,
        };
    }

    private static TransactionId ExtractTxn(RecordBatch batch, int colIdx, int row)
    {
        var structArray = (Apache.Arrow.StructArray)batch.Column(colIdx);

        return new TransactionId
        {
            AppId = GetStringField(structArray, "appId", row) ?? "",
            Version = GetInt64Field(structArray, "version", row) ?? 0,
            LastUpdated = GetInt64Field(structArray, "lastUpdated", row),
        };
    }

    private static Actions.DomainMetadata ExtractDomainMetadata(
        RecordBatch batch, int colIdx, int row)
    {
        var structArray = (Apache.Arrow.StructArray)batch.Column(colIdx);

        return new Actions.DomainMetadata
        {
            Domain = GetStringField(structArray, "domain", row) ?? "",
            Configuration = GetStringField(structArray, "configuration", row) ?? "",
            Removed = GetBoolField(structArray, "removed", row) ?? false,
        };
    }

    #region Field extraction helpers

    /// <summary>
    /// Checks if a struct column has a non-null value for a key inner field at the given row.
    /// Used to detect which action type is present since struct-level null bitmaps
    /// may not survive Parquet round-trips.
    /// </summary>
    private static bool HasStructValue(RecordBatch batch, int colIdx, string keyField, int row)
    {
        if (colIdx < 0) return false;
        var col = batch.Column(colIdx);
        if (col is not Apache.Arrow.StructArray sa) return false;

        int fieldIdx = FindFieldIndex(sa, keyField);
        if (fieldIdx < 0) return false;

        return !sa.Fields[fieldIdx].IsNull(row);
    }

    private static int FindFieldIndex(Apache.Arrow.StructArray structArray, string name)
    {
        for (int i = 0; i < structArray.Fields.Count; i++)
        {
            if (structArray.Data.DataType is Apache.Arrow.Types.StructType st &&
                st.Fields[i].Name == name)
                return i;
        }
        return -1;
    }

    private static string? GetStringField(Apache.Arrow.StructArray structArray, string name, int row)
    {
        int idx = FindFieldIndex(structArray, name);
        if (idx < 0) return null;
        var array = structArray.Fields[idx];
        if (array.IsNull(row)) return null;
        return array switch
        {
            Apache.Arrow.StringArray sa => sa.GetString(row),
            Apache.Arrow.LargeStringArray lsa => lsa.GetString(row),
            _ => null,
        };
    }

    private static long? GetInt64Field(Apache.Arrow.StructArray structArray, string name, int row)
    {
        int idx = FindFieldIndex(structArray, name);
        if (idx < 0) return null;
        var array = structArray.Fields[idx];
        if (array.IsNull(row)) return null;
        return array switch
        {
            Apache.Arrow.Int64Array ia => ia.GetValue(row),
            _ => null,
        };
    }

    private static int? GetInt32Field(Apache.Arrow.StructArray structArray, string name, int row)
    {
        int idx = FindFieldIndex(structArray, name);
        if (idx < 0) return null;
        var array = structArray.Fields[idx];
        if (array.IsNull(row)) return null;
        return array switch
        {
            Apache.Arrow.Int32Array ia => ia.GetValue(row),
            _ => null,
        };
    }

    private static bool? GetBoolField(Apache.Arrow.StructArray structArray, string name, int row)
    {
        int idx = FindFieldIndex(structArray, name);
        if (idx < 0) return null;
        var array = structArray.Fields[idx];
        if (array.IsNull(row)) return null;
        return array switch
        {
            Apache.Arrow.BooleanArray ba => ba.GetValue(row),
            _ => null,
        };
    }

    private static Dictionary<string, string> GetStringMapField(
        Apache.Arrow.StructArray structArray, string name, int row)
    {
        var result = new Dictionary<string, string>();
        int idx = FindFieldIndex(structArray, name);
        if (idx < 0 || structArray.Fields[idx].IsNull(row))
            return result;

        // Map arrays in Arrow are stored as a list of key-value structs
        if (structArray.Fields[idx] is Apache.Arrow.MapArray mapArray)
        {
            var offsets = mapArray.ValueOffsets;
            int start = offsets[row];
            int end = offsets[row + 1];
            var keys = mapArray.Keys;
            var values = mapArray.Values;

            for (int i = start; i < end; i++)
            {
                string? key = keys switch
                {
                    Apache.Arrow.StringArray sa => sa.GetString(i),
                    Apache.Arrow.LargeStringArray lsa => lsa.GetString(i),
                    _ => null,
                };
                string? value = values switch
                {
                    Apache.Arrow.StringArray sa => sa.GetString(i),
                    Apache.Arrow.LargeStringArray lsa => lsa.GetString(i),
                    _ => null,
                };
                if (key is not null)
                    result[key] = value!;   // a null VALUE is meaningful (null partition value)
            }
        }

        return result;
    }

    // Like GetStringListField but distinguishes a NULL/absent list (null) from an empty one.
    private static List<string>? GetStringListFieldOrNull(
        Apache.Arrow.StructArray structArray, string name, int row)
    {
        int idx = FindFieldIndex(structArray, name);
        if (idx < 0 || structArray.Fields[idx].IsNull(row))
            return null;
        return GetStringListField(structArray, name, row);
    }

    private static List<string> GetStringListField(
        Apache.Arrow.StructArray structArray, string name, int row)
    {
        var result = new List<string>();
        int idx = FindFieldIndex(structArray, name);
        if (idx < 0 || structArray.Fields[idx].IsNull(row))
            return result;

        if (structArray.Fields[idx] is Apache.Arrow.ListArray listArray)
        {
            var offsets = listArray.ValueOffsets;
            int start = offsets[row];
            int end = offsets[row + 1];
            var values = listArray.Values;

            for (int i = start; i < end; i++)
            {
                string? value = values switch
                {
                    Apache.Arrow.StringArray sa => sa.GetString(i),
                    Apache.Arrow.LargeStringArray lsa => lsa.GetString(i),
                    _ => null,
                };
                if (value is not null)
                    result.Add(value);
            }
        }

        return result;
    }

    #endregion
}
