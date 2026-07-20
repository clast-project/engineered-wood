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
        _parquetOptions = parquetOptions;
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

        // 5. File actions — embed inline or write to sidecar
        int fileActionCount = snapshot.ActiveFiles.Count;
        bool useSidecars = fileActionCount > SidecarThreshold;

        if (useSidecars)
        {
            // Write all add actions to a sidecar Parquet file
            var sidecarActions = new List<DeltaAction>();
            foreach (var add in snapshot.ActiveFiles.Values)
                sidecarActions.Add(add);

            var sidecarInfo = await WriteSidecarAsync(
                sidecarActions, snapshot, cancellationToken).ConfigureAwait(false);
            actions.Add(sidecarInfo);
        }
        else
        {
            // Embed file actions inline
            foreach (var add in snapshot.ActiveFiles.Values)
                actions.Add(add);
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

        // Use the existing V1 checkpoint writer machinery to build the Parquet batch
        // for the sidecar (it only needs add/remove columns)
        var sidecarWriter = new CheckpointWriter(_fs, _parquetOptions);
        var sidecarSnapshot = new Snapshot.Snapshot
        {
            Version = snapshot.Version,
            Metadata = snapshot.Metadata,
            Protocol = snapshot.Protocol,
            Schema = snapshot.Schema,
            ArrowSchema = snapshot.ArrowSchema,
            ActiveFiles = snapshot.ActiveFiles,
            AppTransactions = new Dictionary<string, TransactionId>(),
            DomainMetadata = new Dictionary<string, DomainMetadata>(),
            RowIdHighWaterMark = snapshot.RowIdHighWaterMark,
        };

        // Write sidecar using the V1 Parquet checkpoint format
        // (which handles struct columns for add/remove)
        await using (var file = await _fs.CreateAsync(sidecarPath, cancellationToken: cancellationToken)
            .ConfigureAwait(false))
        {
            await using var writer = new ParquetFileWriter(file, ownsFile: false, _parquetOptions);
            var batch = CheckpointWriter.BuildCheckpointBatchPublic(sidecarSnapshot, out _);
            await writer.WriteRowGroupAsync(batch, cancellationToken).ConfigureAwait(false);
        }

        // Get file size
        byte[] sidecarBytes = await _fs.ReadAllBytesAsync(sidecarPath, cancellationToken)
            .ConfigureAwait(false);

        return new SidecarFile
        {
            Path = sidecarName,
            SizeInBytes = sidecarBytes.Length,
            ModificationTime = now,
        };
    }
}
