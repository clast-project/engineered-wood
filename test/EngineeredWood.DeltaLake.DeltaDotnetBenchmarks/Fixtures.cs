// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;
using DeltaLake.Table;
using EwDeltaTable = EngineeredWood.DeltaLake.Table.DeltaTable;

namespace DeltaDotnetComparison;

/// <summary>
/// Template tables, built once and cached under <see cref="Root"/> (on local disk, or in Azure Blob Storage: see
/// <see cref="BenchStorage"/>), and fresh copies of them for the benchmarks that mutate a table.
///
/// <para>Each template is built by its <see cref="TableWriter"/> with that engine's DEFAULT writer settings,
/// so a table's file layout (page version, encodings, row-group size, stats) is that engine's own. Readers
/// are then run against both writers' tables.</para>
///
/// <para>A template is marked complete by a sibling <c>.ready</c> file, never by a file inside the table:
/// a vacuum would collect anything unreferenced in the table root.</para>
/// </summary>
internal static class Fixtures
{
    /// <summary>Rows in the single-commit tables and in the write benchmarks.</summary>
    public const int Rows = 1_000_000;

    /// <summary>The many-small-files tables: <see cref="Commits"/> appends of <see cref="RowsPerCommit"/>.</summary>
    public const int Commits = 100;
    public const int RowsPerCommit = 10_000;

    /// <summary>The prunable tables: <see cref="PrunableFiles"/> appends of sorted, disjoint id ranges.</summary>
    public const int PrunableFiles = 20;

    /// <summary>
    /// Holds automatic checkpoints off for both writers (EW's default interval is 10, delta-rs's is 100), so a
    /// fixture is checkpointed only where it says it is.
    /// </summary>
    private static readonly Dictionary<string, string> NoAutoCheckpoint =
        new() { ["delta.checkpointInterval"] = "1000000" };

    private static BenchStorage Storage => BenchStorage.Current;

    public static string Root => Storage.Root;

    private static string Templates => Storage.Combine(Root, "templates");
    private static string ScratchRoot => Storage.Combine(Root, "scratch");

    /// <summary>
    /// This machine's scratch directories. A process id means something only on its own machine, and an Azure
    /// root may be shared by runs on several, so each machine keeps (and sweeps) only its own subtree.
    /// </summary>
    private static string HostScratch => Storage.Combine(ScratchRoot, Environment.MachineName.ToLowerInvariant());

    /// <summary>
    /// This process's own scratch directory. BenchmarkDotNet runs each benchmark in a child process, and a
    /// <c>--verify</c> or a second filtered run may share <see cref="Root"/> with a run in progress, so no
    /// process may sweep another live process's iteration copies.
    /// </summary>
    private static string Scratch => Storage.Combine(HostScratch, Environment.ProcessId.ToString(CultureInfo.InvariantCulture));

    /// <summary>One commit of <see cref="Rows"/> rows.</summary>
    public static string SingleCommit(TableWriter writer) =>
        Ensure($"single-{writer}-{Rows}", path => Write(writer, path, [Data.Generate(0, Rows)]));

    /// <summary>
    /// <see cref="SingleCommit"/> written by EW with <c>delta.enableDeletionVectors</c>, for EW's own DELETE
    /// path. There is no delta-dotnet counterpart: delta-rs deletes copy-on-write.
    /// </summary>
    public static string SingleCommitWithDeletionVectors() =>
        Ensure($"single-dv-{TableWriter.EngineeredWood}-{Rows}", async path =>
        {
            await using var table = await EwDeltaTable.CreateAsync(
                Ew.Fs(path), Data.Schema, enableDeletionVectors: true, configuration: NoAutoCheckpoint);
            await table.WriteAsync(Data.Generate(0, Rows));
        });

    /// <summary><see cref="PrunableFiles"/> commits of contiguous id ranges, so file statistics can prune.</summary>
    public static string Prunable(TableWriter writer) =>
        Ensure($"prunable-{writer}-{PrunableFiles}x{Rows / PrunableFiles}", path =>
        {
            int per = Rows / PrunableFiles;
            return Write(writer, path,
                Enumerable.Range(0, PrunableFiles).Select(i => Data.Generate((long)i * per, per)).ToList());
        });

    /// <summary>The large tables: <see cref="LargeFiles"/> appends of <see cref="LargeRowsPerFile"/>, sorted and disjoint.</summary>
    public const int LargeFiles = 20;
    public const int LargeRowsPerFile = 500_000;

    /// <summary>
    /// <see cref="Prunable"/>'s shape at ten times its rows. Each file's rows are generated just before they are
    /// written, so building it never holds all 10M rows.
    /// </summary>
    public static string Large(TableWriter writer) =>
        Ensure($"large-{writer}-{LargeFiles}x{LargeRowsPerFile}", path =>
            Write(writer, path, Enumerable.Range(0, LargeFiles)
                .Select(i => (IReadOnlyList<Apache.Arrow.RecordBatch>)Data.Generate((long)i * LargeRowsPerFile, LargeRowsPerFile))));

    /// <summary><see cref="Commits"/> small appends; a checkpoint at the last version when asked.</summary>
    public static string ManyCommits(TableWriter writer, bool checkpointed) =>
        Ensure($"commits-{writer}-{Commits}x{RowsPerCommit}-{(checkpointed ? "ckpt" : "nockpt")}", async path =>
        {
            await Write(writer, path,
                Enumerable.Range(0, Commits)
                    .Select(i => Data.Generate((long)i * RowsPerCommit, RowsPerCommit)).ToList());
            if (checkpointed)
                await CheckpointAsync(writer, path);
        });

    /// <summary>
    /// The <see cref="ManyCommits"/> shape, then compacted by the same writer: the 100 removed small files are
    /// what a vacuum collects.
    ///
    /// <para>Built with <c>delta.deletedFileRetentionDuration = interval 0 seconds</c>, and both engines vacuum
    /// with the TABLE's retention rather than an explicit one. delta-dotnet cannot be asked for zero retention:
    /// its bridge maps <c>VacuumOptions.RetentionHours = 0</c> to "no override", so delta-rs falls back to the
    /// 7-day default and deletes nothing. That made an earlier version of this benchmark time EW's vacuum
    /// against a delta-rs no-op.</para>
    /// </summary>
    public static string Compacted(TableWriter writer) =>
        Ensure($"compacted-retain0-{writer}-{Commits}x{RowsPerCommit}", async path =>
        {
            var configuration = new Dictionary<string, string>(NoAutoCheckpoint)
            {
                ["delta.deletedFileRetentionDuration"] = "interval 0 seconds",
            };
            await Write(writer, path,
                Enumerable.Range(0, Commits)
                    .Select(i => Data.Generate((long)i * RowsPerCommit, RowsPerCommit)).ToList(),
                configuration);
            await CompactAsync(writer, path);
        });

    /// <summary>
    /// A change-data-feed table: ten appends, then an UPDATE and a DELETE so the feed carries
    /// <c>_change_data</c> files as well as inferred inserts.
    /// </summary>
    public static string ChangeFeed(TableWriter writer) =>
        Ensure($"cdf-{writer}", async path =>
        {
            const int appends = 10, per = 100_000;
            var configuration = new Dictionary<string, string>(NoAutoCheckpoint)
            {
                ["delta.enableChangeDataFeed"] = "true",
            };
            var commits = Enumerable.Range(0, appends).Select(i => Data.Generate((long)i * per, per)).ToList();
            await Write(writer, path, commits, configuration);
            if (writer == TableWriter.EngineeredWood)
            {
                await Ew.UpdateAsync(path, Sql.OneCategory);
                await Ew.DeleteAsync(path, "category = 'cat_05'");
            }
            else
            {
                await Dd.UpdateAsync(path, Sql.OneCategory);
                await Dd.DeleteAsync(path, "category = 'cat_05'");
            }
        });

    /// <summary>A fresh, private copy of a template, for one benchmark iteration to mutate.</summary>
    public static string FreshCopy(string template)
    {
        string path = Storage.Combine(Scratch, Guid.NewGuid().ToString("N"));
        Storage.CopyTree(template, path);
        return path;
    }

    /// <summary>A fresh, empty directory, for one write iteration.</summary>
    public static string FreshEmpty()
    {
        string path = Storage.Combine(Scratch, Guid.NewGuid().ToString("N"));
        Storage.CreateEmpty(path);
        return path;
    }

    public static void Delete(string? path)
    {
        if (path is not null)
            Storage.DeleteTree(path);
    }

    /// <summary>
    /// Clears out iteration copies left by interrupted runs on THIS machine: this process's own directory, and
    /// those of processes that have exited. A directory whose process is still running is left alone, and other
    /// machines' subtrees are never looked at. (A reused process id can only make a dead run's copies survive
    /// one more sweep; it can never delete a live run's.)
    /// </summary>
    public static void SweepScratch()
    {
        Delete(Scratch);
        foreach (string dir in Storage.Children(HostScratch))
        {
            if (int.TryParse(Storage.NameOf(dir), NumberStyles.None, CultureInfo.InvariantCulture, out int pid)
                && !IsRunning(pid))
            {
                Delete(dir);
            }
        }
    }

    private static bool IsRunning(int pid)
    {
        try
        {
            using var process = System.Diagnostics.Process.GetProcessById(pid);
            return !process.HasExited;
        }
        catch (ArgumentException)
        {
            return false; // no such process
        }
        catch (InvalidOperationException)
        {
            return false;
        }
        catch (System.ComponentModel.Win32Exception)
        {
            return true; // exists but not inspectable: assume it is live
        }
    }

    private static string Ensure(string name, Func<string, Task> build)
    {
        // The marker names the template; each build writes a fresh location of its own (templates/<name>.<id>)
        // and publishes it by writing the marker last. No run ever writes into a location another run may be
        // building or using, so even a run that lost its lock mid-build (a stalled process whose lease expired)
        // can only publish a second complete build, never mix its files into someone else's.
        string marker = Storage.Combine(Templates, name);
        if (Built(marker) is { } built)
            return built;

        // The lock (a file on local disk, a blob lease in Azure) spares the second run a duplicate build: it
        // waits, re-checks and finds the first run's template. A run that dies mid-build releases it and leaves
        // no marker, and the next holder builds a fresh location.
        using (Storage.AcquireBuildLock(marker, name))
        {
            if (Built(marker) is { } builtMeanwhile)
                return builtMeanwhile; // built by whoever held the lock before us

            string path = Storage.Combine(Templates, $"{name}.{Guid.NewGuid():N}");
            Console.WriteLine($"// building fixture {name} on {Storage.Description} ...");
            try
            {
                Storage.CreateEmpty(path);
                build(path).GetAwaiter().GetResult();
            }
            catch
            {
                Delete(path);
                throw;
            }
            Storage.WriteMarker(marker, $"{GenerationPrefix}{Storage.NameOf(path)}\nbuilt {DateTime.UtcNow:O}\n");
            return path;
        }
    }

    private const string GenerationPrefix = "template ";

    /// <summary>
    /// The published template behind a marker, or null if none. A marker from before templates had their own
    /// locations holds only a timestamp, and its template is the marker's own location.
    /// </summary>
    private static string? Built(string marker)
    {
        if (Storage.ReadMarker(marker) is not { } content)
            return null;
        if (!content.StartsWith(GenerationPrefix, StringComparison.Ordinal))
            return marker;
        string generation = content[GenerationPrefix.Length..].Split('\n')[0].Trim();
        return Storage.Combine(Templates, generation);
    }

    /// <summary>Creates the table with the first batch list, then appends each further list as one commit.</summary>
    private static async Task Write(
        TableWriter writer,
        string path,
        IEnumerable<IReadOnlyList<Apache.Arrow.RecordBatch>> commits,
        IReadOnlyDictionary<string, string>? configuration = null)
    {
        configuration ??= NoAutoCheckpoint;
        if (writer == TableWriter.EngineeredWood)
        {
            await using var table = await EwDeltaTable.CreateAsync(
                Ew.Fs(path), Data.Schema, configuration: configuration);
            foreach (var commit in commits)
                await table.WriteAsync(commit);
        }
        else
        {
            using var table = await Dd.Engine.CreateTableAsync(Dd.CreateOptions(path, configuration), CancellationToken.None);
            foreach (var commit in commits)
                await Dd.InsertAsync(table, commit, Dd.DefaultMaxRowsPerGroup);
        }
    }

    public static async Task CheckpointAsync(TableWriter writer, string path)
    {
        if (writer == TableWriter.EngineeredWood)
        {
            await using var table = await EwDeltaTable.OpenAsync(Ew.Fs(path));
            await table.CheckpointAsync();
        }
        else
        {
            using var table = await Dd.OpenAsync(path);
            await table.CheckpointAsync(CancellationToken.None);
        }
    }

    public static async Task CompactAsync(TableWriter writer, string path)
    {
        if (writer == TableWriter.EngineeredWood)
        {
            await using var table = await EwDeltaTable.OpenAsync(Ew.Fs(path));
            await table.CompactAsync();
        }
        else
        {
            using var table = await Dd.OpenAsync(path);
            await table.OptimizeAsync(new OptimizeOptions(), CancellationToken.None);
        }
    }
}
