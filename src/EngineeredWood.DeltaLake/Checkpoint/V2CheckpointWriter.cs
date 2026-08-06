// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.IO;
using EngineeredWood.Parquet;

namespace EngineeredWood.DeltaLake.Checkpoint;

/// <summary>
/// Writes V2 spec checkpoints. V2 checkpoints are UUID-named JSON files
/// containing NDJSON actions. File actions (add/remove) can be embedded
/// inline or moved to sidecar Parquet files in <c>_delta_log/_sidecars/</c>.
/// </summary>
public sealed class V2CheckpointWriter
{
    private readonly ITableFileSystem _fs;
    private readonly ParquetWriteOptions? _parquetOptions;

    /// <summary>
    /// Threshold in number of file actions above which sidecars are used.
    /// Below this threshold, file actions are embedded inline.
    /// </summary>
    public int SidecarThreshold { get; init; } = 100;

    public V2CheckpointWriter(
        ITableFileSystem fileSystem,
        ParquetWriteOptions? parquetOptions = null)
    {
        _fs = fileSystem;
        _parquetOptions = CheckpointParquetOptions.For(parquetOptions);
    }

    /// <summary>
    /// Writes a V2 checkpoint for the given snapshot.
    /// </summary>
    public async ValueTask WriteCheckpointAsync(
        Snapshot.Snapshot snapshot,
        CancellationToken cancellationToken = default)
    {
        string uuid = Guid.NewGuid().ToString();
        string checkpointPath = $"_delta_log/{DeltaVersion.Format(snapshot.Version)}.checkpoint.{uuid}.json";

        var actions = new List<DeltaAction>();

        // 1. CheckpointMetadata must be first
        actions.Add(new CheckpointMetadata
        {
            Version = snapshot.Version,
        });

        // 2. Protocol and metadata
        actions.Add(snapshot.Protocol);
        actions.Add(snapshot.Metadata);

        // 3. Transactions
        foreach (var txn in snapshot.AppTransactions.Values)
            actions.Add(txn);

        // 4. Domain metadata
        foreach (var dm in snapshot.DomainMetadata.Values)
            actions.Add(dm);

        // 5. File actions — embed inline or write to sidecar. Both arms take the SAME set: active files
        // plus the unexpired remove tombstones. Dropping the tombstones (as this writer used to) makes a
        // checkpoint a reader cannot replay safely — VACUUM can then delete a file a concurrent reader or
        // a time-travel/CDF read still needs. The spec also forbids splitting file actions between the
        // checkpoint and its sidecars, so the choice is all-inline or all-sidecar, never a mix.
        var fileActions = CheckpointWriter.CollectFileActions(snapshot);
        bool useSidecars = fileActions.Count > SidecarThreshold;

        if (useSidecars)
        {
            var sidecarInfo = await WriteSidecarAsync(
                fileActions, snapshot, cancellationToken).ConfigureAwait(false);
            actions.Add(sidecarInfo);
        }
        else
        {
            actions.AddRange(fileActions);
        }

        // Write the JSON checkpoint file
        byte[] ndjson = ActionSerializer.Serialize(actions);
        await _fs.WriteAllBytesAsync(checkpointPath, ndjson, cancellationToken)
            .ConfigureAwait(false);

        // Update _last_checkpoint
        using var lastCheckpointStream = new MemoryStream();
        using (var w = new Utf8JsonWriter(lastCheckpointStream))
        {
            w.WriteStartObject();
            w.WriteNumber("version", snapshot.Version);
            w.WriteNumber("size", (long)actions.Count);
            w.WriteStartObject("v2Checkpoint");
            w.WriteString("path", checkpointPath);
            w.WriteNumber("sidecarFiles", useSidecars ? 1 : 0);
            w.WriteEndObject();
            w.WriteEndObject();
        }
        byte[] json = lastCheckpointStream.ToArray();

        await _fs.WriteAllBytesAsync(
            DeltaVersion.LastCheckpointPath, json, cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask<SidecarFile> WriteSidecarAsync(
        List<DeltaAction> fileActions,
        Snapshot.Snapshot snapshot,
        CancellationToken cancellationToken)
    {
        string sidecarName = $"{Guid.NewGuid()}.parquet";
        string sidecarPath = $"_delta_log/_sidecars/{sidecarName}";
        long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        long sizeInBytes;

        // The sidecar body reuses the V1 Parquet checkpoint schema, but over the file actions ALONE:
        // PROTOCOL.md allows a sidecar "only add file and remove file entries", so the batch cannot be
        // built from a snapshot (which would emit a protocol and a metaData row too, duplicating the ones
        // already in the checkpoint file itself).
        await using (var file = await _fs.CreateAsync(sidecarPath, cancellationToken: cancellationToken)
            .ConfigureAwait(false))
        {
            // Declared before the writer so it is disposed last; its buffers are native memory.
            using var batch = CheckpointWriter.BuildBatchForActions(snapshot, fileActions, out _);

            // Scoped so the writer's footer lands before Position is read.
            await using (var writer = new ParquetFileWriter(file, ownsFile: false, _parquetOptions))
            {
                await writer.WriteRowGroupAsync(batch, cancellationToken).ConfigureAwait(false);
            }

            // sidecar.sizeInBytes is required by the spec, and the write already knows it: Position is
            // the total written. Reading the file back to measure it doubled the I/O of every sidecar
            // and pulled a potentially multi-hundred-megabyte Parquet file into memory to take .Length.
            sizeInBytes = file.Position;
        }

        return new SidecarFile
        {
            Path = sidecarName,
            SizeInBytes = sizeInBytes,
            ModificationTime = now,
        };
    }
}
