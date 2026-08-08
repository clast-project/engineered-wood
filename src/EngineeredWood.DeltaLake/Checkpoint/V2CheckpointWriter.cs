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
    /// <remarks>
    /// Whether to use sidecars AT ALL. How many to split them across is
    /// <see cref="MaxActionsPerSidecar"/> — the spec forbids a mix, so this cannot be a per-sidecar
    /// bound as well.
    /// </remarks>
    public int SidecarThreshold { get; init; } = 100;

    /// <summary>
    /// The most file actions any one sidecar carries. Above this, the actions are split across as many
    /// sidecars as it takes. Default: 500,000.
    /// </summary>
    /// <remarks>
    /// <para>A V2 checkpoint "could reference zero or more sidecar file actions", and splitting is what
    /// the feature exists for on a table with millions of files — a single sidecar for a table however
    /// far above <see cref="SidecarThreshold"/> gives up the whole point.</para>
    ///
    /// <para>The cap is also a memory bound, which is the reason for a default rather than "unlimited".
    /// Each sidecar's Parquet body is assembled as one in-memory Arrow batch, so an unsplit sidecar makes
    /// a table's peak checkpoint footprint proportional to its file count — with statistics attached, a
    /// few million adds is gigabytes. 500,000 keeps that bounded while leaving the sidecar count small
    /// enough that resolving them is a handful of reads, not thousands.</para>
    ///
    /// <para>delta-spark's equivalent (<c>spark.databricks.delta.checkpoint.partSize</c>, "a maximum of
    /// this many actions per checkpoint file") is unset by default, so its sidecars are unsplit until an
    /// operator says otherwise. Differing here is deliberate: an unbounded default is only safe when
    /// someone is watching.</para>
    /// </remarks>
    public int MaxActionsPerSidecar { get; init; } = 500_000;

    /// <summary>
    /// Which of the two spec-defined bodies to write. Defaults to
    /// <see cref="V2CheckpointBody.Json"/>, matching delta-spark.
    /// </summary>
    public V2CheckpointBody Body { get; init; } = V2CheckpointBody.Json;

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
    /// <exception cref="DeltaFormatException">
    /// The table has not enabled the <c>v2Checkpoint</c> table feature.
    /// </exception>
    public async ValueTask WriteCheckpointAsync(
        Snapshot.Snapshot snapshot,
        CancellationToken cancellationToken = default)
    {
        // PROTOCOL.md: the V2 spec "can be used only when [the] v2 checkpoint table feature is enabled".
        // Without the gate this call succeeded and left behind a checkpoint conforming readers are
        // entitled to reject — and the table's own protocol would give them no warning that it exists.
        // Checked here rather than in the caller because a host driving this writer directly, which is
        // the only way to reach it before now, must hit the same rule.
        if (!ProtocolVersions.SupportsV2Checkpoints(snapshot.Protocol))
        {
            throw new DeltaFormatException(
                DeltaErrorCodes.FeatureNotEnabled,
                "A V2 checkpoint may only be written to a table that has enabled the 'v2Checkpoint' " +
                "table feature, which requires reader version 3, writer version 7, and the feature " +
                "named in BOTH readerFeatures and writerFeatures. This table declares reader version " +
                $"{snapshot.Protocol.MinReaderVersion} / writer version " +
                $"{snapshot.Protocol.MinWriterVersion}. Enable the feature first, or write a classic " +
                "checkpoint.");
        }

        string uuid = Guid.NewGuid().ToString();
        string extension = Body == V2CheckpointBody.Parquet ? "parquet" : "json";
        string checkpointName = $"{DeltaVersion.Format(snapshot.Version)}.checkpoint.{uuid}.{extension}";
        string checkpointPath = DeltaVersion.LogPrefix + checkpointName;

        // File actions first, because the sidecars have to exist before the checkpointMetadata action
        // that summarises them can be built — and that action must be the checkpoint's FIRST row.
        //
        // Both arms take the SAME set: active files plus the unexpired remove tombstones. Dropping the
        // tombstones (as this writer used to) makes a checkpoint a reader cannot replay safely — VACUUM
        // can then delete a file a concurrent reader or a time-travel/CDF read still needs. The spec also
        // forbids splitting file actions between the checkpoint and its sidecars, so the choice is
        // all-inline or all-sidecar, never a mix.
        var fileActions = CheckpointWriter.CollectFileActions(snapshot);
        bool useSidecars = fileActions.Count > SidecarThreshold;

        var sidecars = useSidecars
            ? await WriteSidecarsAsync(fileActions, snapshot, cancellationToken).ConfigureAwait(false)
            : [];

        var actions = new List<DeltaAction>(
            5 + snapshot.AppTransactions.Count + snapshot.DomainMetadata.Count +
            (useSidecars ? sidecars.Count : fileActions.Count))
        {
            // Exactly one, and first.
            BuildCheckpointMetadata(snapshot, fileActions, sidecars),
            snapshot.Protocol,
            snapshot.Metadata,
        };

        foreach (var txn in snapshot.AppTransactions.Values)
            actions.Add(txn);

        foreach (var dm in snapshot.DomainMetadata.Values)
            actions.Add(dm);

        if (useSidecars)
            actions.AddRange(sidecars);
        else
            actions.AddRange(fileActions);

        long bodySize = await WriteBodyAsync(checkpointPath, actions, snapshot, cancellationToken)
            .ConfigureAwait(false);

        await WriteLastCheckpointAsync(
            snapshot, checkpointName, bodySize, actions.Count, fileActions, sidecars, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// The <c>checkpointMetadata</c> action, carrying the optional tags PROTOCOL.md names for it.
    /// </summary>
    /// <remarks>
    /// All four are optional and "readers cannot assume their presence", so this is a courtesy — but a
    /// cheap one, since every number is already known here, and <c>sidecarFileSchema</c> in particular
    /// lets a reader plan its sidecar reads without opening a single Parquet footer.
    /// </remarks>
    private static CheckpointMetadata BuildCheckpointMetadata(
        Snapshot.Snapshot snapshot, List<DeltaAction> fileActions, List<SidecarFile> sidecars)
    {
        long numOfAddFiles = 0;
        foreach (var action in fileActions)
        {
            if (action is AddFile)
                numOfAddFiles++;
        }

        var tags = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            // Counts of what is IN the sidecars, so both are zero for an inline checkpoint.
            ["sidecarNumActions"] = (sidecars.Count > 0 ? fileActions.Count : 0)
                .ToString(System.Globalization.CultureInfo.InvariantCulture),
            ["sidecarSizeInBytes"] = sidecars.Sum(s => s.SizeInBytes)
                .ToString(System.Globalization.CultureInfo.InvariantCulture),
            ["numOfAddFiles"] = numOfAddFiles
                .ToString(System.Globalization.CultureInfo.InvariantCulture),
        };

        if (sidecars.Count > 0)
            tags["sidecarFileSchema"] = SidecarFileSchemaJson(snapshot);

        return new CheckpointMetadata { Version = snapshot.Version, Tags = tags };
    }

    /// <summary>
    /// The sidecar Parquet schema, as the JSON-serialized Delta <c>StructType</c> the tag calls for.
    /// </summary>
    /// <remarks>
    /// Derived from an EMPTY batch in the same shape the real sidecars were built in, so the two cannot
    /// drift: the statistics mode and the table schema decide which <c>add</c> columns exist, and reading
    /// the schema off a throwaway batch is how that stays true without duplicating the decision.
    /// </remarks>
    private static string SidecarFileSchemaJson(Snapshot.Snapshot snapshot)
    {
        using var empty = CheckpointWriter.BuildBatchForActions(snapshot, [], out _);
        return Schema.DeltaSchemaSerializer.Serialize(
            Schema.SchemaConverter.FromArrowSchema(empty.Schema));
    }

    /// <summary>
    /// Writes <c>_last_checkpoint</c>: the spec's own fields, plus the <c>v2Checkpoint</c> object
    /// delta-spark writes under the spec's allowance to "embed some or all of the V2 spec checkpoint in
    /// the <c>_last_checkpoint</c> file, so that readers don't have to read the V2 Checkpoint".
    /// </summary>
    /// <remarks>
    /// <para><c>sidecarFiles</c> used to be written here as an integer 0-or-1. That is delta-spark's
    /// field name with delta-spark's meaning replaced: it writes an ARRAY of sidecar objects, which is
    /// the shape a reader consuming the field expects. Reusing the name for a count made EW's
    /// <c>_last_checkpoint</c> actively misleading to anything that read it, which was worse than
    /// omitting it — MEASURED against delta-spark 4.0.0, 2026-08-08.</para>
    ///
    /// <para><c>path</c> is the BARE file name, also matching delta-spark. A UUID-named checkpoint always
    /// lives in <c>_delta_log</c>, so the directory carries no information; writing the rooted path
    /// instead would be betting that every reader re-roots it the way this one does.</para>
    ///
    /// <para>The optional <c>checksum</c> is not written. It is an MD5 over a canonicalization with its
    /// own escaping and ordering rules, and a wrong one is worse than an absent one — a reader that
    /// validates would reject a perfectly good hint.</para>
    /// </remarks>
    private async ValueTask WriteLastCheckpointAsync(
        Snapshot.Snapshot snapshot,
        string checkpointName,
        long bodySize,
        int actionCount,
        List<DeltaAction> fileActions,
        List<SidecarFile> sidecars,
        CancellationToken cancellationToken)
    {
        // "The number of actions that are stored in the checkpoint" — the sidecars' contents included,
        // since they are the checkpoint's actions however they are stored. The checkpoint's own rows
        // already count the sidecar references, so those are swapped out for what they point at.
        long size = sidecars.Count > 0
            ? actionCount - sidecars.Count + fileActions.Count
            : actionCount;

        long numOfAddFiles = 0;
        foreach (var action in fileActions)
        {
            if (action is AddFile)
                numOfAddFiles++;
        }

        using var stream = new MemoryStream();
        using (var w = new Utf8JsonWriter(stream))
        {
            w.WriteStartObject();
            w.WriteNumber("version", snapshot.Version);
            w.WriteNumber("size", size);
            w.WriteNumber("sizeInBytes", bodySize + sidecars.Sum(s => s.SizeInBytes));
            w.WriteNumber("numOfAddFiles", numOfAddFiles);

            w.WriteStartObject("v2Checkpoint");
            w.WriteString("path", checkpointName);
            w.WriteNumber("sizeInBytes", bodySize);
            w.WriteStartArray("sidecarFiles");
            foreach (var sidecar in sidecars)
            {
                w.WriteStartObject();
                w.WriteString("path", sidecar.Path);
                w.WriteNumber("sizeInBytes", sidecar.SizeInBytes);
                w.WriteNumber("modificationTime", sidecar.ModificationTime);
                w.WriteEndObject();
            }
            w.WriteEndArray();
            w.WriteEndObject();

            w.WriteEndObject();
        }

        await _fs.WriteAllBytesAsync(
            DeltaVersion.LastCheckpointPath, stream.ToArray(), cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Writes the checkpoint's own body — every action it carries, in whichever of the two spec-defined
    /// formats <see cref="Body"/> selects. Returns the bytes written.
    /// </summary>
    private async ValueTask<long> WriteBodyAsync(
        string checkpointPath,
        List<DeltaAction> actions,
        Snapshot.Snapshot snapshot,
        CancellationToken cancellationToken)
    {
        if (Body != V2CheckpointBody.Parquet)
        {
            byte[] ndjson = ActionSerializer.Serialize(actions);
            await _fs.WriteAllBytesAsync(checkpointPath, ndjson, cancellationToken)
                .ConfigureAwait(false);
            return ndjson.Length;
        }

        await using var file = await _fs.CreateAsync(checkpointPath, cancellationToken: cancellationToken)
            .ConfigureAwait(false);

        // Declared before the writer so it is disposed last; its buffers are native memory.
        using var batch = CheckpointWriter.BuildBatchForActions(
            snapshot, actions, out _, v2Spec: true);

        // Scoped so the writer's footer lands before Position is read.
        await using (var writer = new ParquetFileWriter(file, ownsFile: false, _parquetOptions))
        {
            await writer.WriteRowGroupAsync(batch, cancellationToken).ConfigureAwait(false);
        }

        return file.Position;
    }

    /// <summary>
    /// Writes the file actions out across as many sidecars as <see cref="MaxActionsPerSidecar"/> calls
    /// for, in order.
    /// </summary>
    private async ValueTask<List<SidecarFile>> WriteSidecarsAsync(
        List<DeltaAction> fileActions,
        Snapshot.Snapshot snapshot,
        CancellationToken cancellationToken)
    {
        int perSidecar = Math.Max(1, MaxActionsPerSidecar);
        var sidecars = new List<SidecarFile>((fileActions.Count + perSidecar - 1) / perSidecar);

        for (int start = 0; start < fileActions.Count; start += perSidecar)
        {
            var chunk = fileActions.GetRange(start, Math.Min(perSidecar, fileActions.Count - start));
            sidecars.Add(await WriteSidecarAsync(chunk, snapshot, cancellationToken)
                .ConfigureAwait(false));
        }

        return sidecars;
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
