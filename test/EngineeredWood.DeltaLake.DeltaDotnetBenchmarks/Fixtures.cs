// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using DeltaLake.Table;
using EwDeltaTable = EngineeredWood.DeltaLake.Table.DeltaTable;

namespace DeltaDotnetComparison;

/// <summary>
/// Template tables, built once and cached under <see cref="Root"/> (override with <c>EWDD_BENCH_ROOT</c>), and
/// fresh copies of them for the benchmarks that mutate a table.
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

    public static string Root { get; } =
        Environment.GetEnvironmentVariable("EWDD_BENCH_ROOT")
        ?? Path.Combine(Path.GetTempPath(), "ew-vs-delta-dotnet");

    private static string Templates => Path.Combine(Root, "templates");
    private static string Scratch => Path.Combine(Root, "scratch");

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
    /// <see cref="ManyCommits"/> then compacted by the same writer: the removed small files are what a
    /// vacuum collects.
    /// </summary>
    public static string Compacted(TableWriter writer) =>
        Ensure($"compacted-{writer}-{Commits}x{RowsPerCommit}", async path =>
        {
            CopyDirectory(ManyCommits(writer, checkpointed: false), path);
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
                await Ew.DeleteCopyOnWriteAsync(path, "category = 'cat_05'");
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
        string path = Path.Combine(Scratch, Guid.NewGuid().ToString("N"));
        CopyDirectory(template, path);
        return path;
    }

    /// <summary>A fresh, empty directory, for one write iteration.</summary>
    public static string FreshEmpty()
    {
        string path = Path.Combine(Scratch, Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(path);
        return path;
    }

    public static void Delete(string? path)
    {
        if (path is null || !Directory.Exists(path))
            return;
        try
        {
            Directory.Delete(path, recursive: true);
        }
        catch (IOException)
        {
            // A native handle released a moment late; the scratch root is swept on the next run anyway.
        }
        catch (UnauthorizedAccessException)
        {
        }
    }

    /// <summary>Clears out iteration copies left by an earlier, interrupted run.</summary>
    public static void SweepScratch() => Delete(Scratch);

    private static string Ensure(string name, Func<string, Task> build)
    {
        string path = Path.Combine(Templates, name);
        string marker = path + ".ready";
        if (File.Exists(marker))
            return path;

        Delete(path);
        Directory.CreateDirectory(path);
        Console.WriteLine($"// building fixture {name} ...");
        build(path).GetAwaiter().GetResult();
        File.WriteAllText(marker, DateTime.UtcNow.ToString("O"));
        return path;
    }

    /// <summary>Creates the table with the first batch list, then appends each further list as one commit.</summary>
    private static async Task Write(
        TableWriter writer,
        string path,
        IReadOnlyList<IReadOnlyList<Apache.Arrow.RecordBatch>> commits,
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
            var create = new TableCreateOptions(path, Data.Schema)
            {
                SaveMode = global::DeltaLake.Table.SaveMode.ErrorIfExists,
                Configuration = configuration.ToDictionary(p => p.Key, p => p.Value),
            };
            using var table = await Dd.Engine.CreateTableAsync(create, CancellationToken.None);
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

    private static void CopyDirectory(string source, string destination)
    {
        Directory.CreateDirectory(destination);
        foreach (string dir in Directory.EnumerateDirectories(source, "*", SearchOption.AllDirectories))
            Directory.CreateDirectory(Path.Combine(destination, Path.GetRelativePath(source, dir)));
        foreach (string file in Directory.EnumerateFiles(source, "*", SearchOption.AllDirectories))
        {
            string target = Path.Combine(destination, Path.GetRelativePath(source, file));
            File.Copy(file, target);
            // Keep the source's mtime: vacuum retention and file-age logic key off it.
            File.SetLastWriteTimeUtc(target, File.GetLastWriteTimeUtc(file));
        }
    }
}
