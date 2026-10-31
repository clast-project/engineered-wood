// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using BenchmarkDotNet.Attributes;
using DeltaLake.Table;
using EwDeltaTable = EngineeredWood.DeltaLake.Table.DeltaTable;

namespace DeltaDotnetComparison;

/// <summary>
/// A benchmark whose every invocation works on its own table directory. The verify pass reads
/// <see cref="CurrentPath"/> back after each method to check that both engines left the same table behind.
/// </summary>
public abstract class MutationBenchmark
{
    public string? CurrentPath { get; protected set; }

    [IterationCleanup]
    public void Cleanup()
    {
        Fixtures.Delete(CurrentPath);
        CurrentPath = null;
    }
}

/// <summary>
/// Create a table and write 1M rows in one commit, into an empty directory.
///
/// <para>delta-dotnet's <c>InsertOptions.MaxRowsPerGroup</c> defaults to 100. It is passed to delta-rs as
/// the writer's <c>write_batch_size</c> (the slice the Arrow-to-Parquet writer is fed in), so it is measured
/// both at that default and at 64K.</para>
/// </summary>
[Config(typeof(SingleShotConfig))]
public class WriteBenchmarks : MutationBenchmark
{
    private IReadOnlyList<RecordBatch> _batches = [];

    [GlobalSetup]
    public void Setup()
    {
        Fixtures.SweepScratch();
        _batches = Data.Generate(0, Fixtures.Rows);
    }

    [IterationSetup]
    public void Fresh() => CurrentPath = Fixtures.FreshEmpty();

    [BenchmarkCategory("CreateAndWrite"), Benchmark(Baseline = true, Description = "EW CreateAsync + WriteAsync")]
    public Task<long> EW() => Ew.CreateAndWriteAsync(CurrentPath!, _batches);

    [BenchmarkCategory("CreateAndWrite"), Benchmark(Description = "DD CreateTable + InsertAsync (MaxRowsPerGroup=100, default)")]
    public async Task<long> DD_Default()
    {
        await Dd.CreateAndInsertAsync(CurrentPath!, _batches);
        return 1;
    }

    [BenchmarkCategory("CreateAndWrite"), Benchmark(Description = "DD CreateTable + InsertAsync (MaxRowsPerGroup=65536)")]
    public async Task<long> DD_Tuned()
    {
        await Dd.CreateAndInsertAsync(CurrentPath!, _batches, maxRowsPerGroup: 65_536);
        return 1;
    }
}

/// <summary>
/// Open a 100-commit table and append 10K rows: snapshot load + small write + commit, the shape of a
/// streaming ingest.
/// </summary>
[Config(typeof(SingleShotConfig))]
public class AppendBenchmarks : MutationBenchmark
{
    [ParamsAllValues]
    public TableWriter Writer { get; set; }

    [ParamsAllValues]
    public bool Checkpointed { get; set; }

    private string _template = "";
    private IReadOnlyList<RecordBatch> _batches = [];

    [GlobalSetup]
    public void Setup()
    {
        Fixtures.SweepScratch();
        _template = Fixtures.ManyCommits(Writer, Checkpointed);
        _batches = Data.Generate((long)Fixtures.Commits * Fixtures.RowsPerCommit, Fixtures.RowsPerCommit);
    }

    [IterationSetup]
    public void Fresh() => CurrentPath = Fixtures.FreshCopy(_template);

    [BenchmarkCategory("Append10K"), Benchmark(Baseline = true, Description = "EW OpenAsync + WriteAsync")]
    public Task<long> EW() => Ew.AppendAsync(CurrentPath!, _batches);

    [BenchmarkCategory("Append10K"), Benchmark(Description = "DD LoadTable + InsertAsync")]
    public async Task<long> DD()
    {
        await Dd.AppendAsync(CurrentPath!, _batches, maxRowsPerGroup: 65_536);
        return 1;
    }
}

/// <summary>
/// DELETE and UPDATE of one category (1 row in 16) on the one-commit 1M-row table (1 file when
/// delta-dotnet wrote it, 16 when EW did: EW writes a file per input batch). The tables do not enable
/// deletion vectors, so both engines copy-on-write: read the file, drop/alter the matching rows, rewrite them.
///
/// <para>EW's predicate <c>DeleteAsync</c> refuses a partial-file delete on such a table (it only writes
/// deletion vectors), so the like-for-like EW DELETE is the host-composed copy-on-write in
/// <see cref="Ew.DeleteCopyOnWriteAsync"/>. EW's own deletion-vector DELETE is measured separately, in
/// <see cref="DeletionVectorDeleteBenchmarks"/>.</para>
/// </summary>
[Config(typeof(SingleShotConfig))]
public class DmlBenchmarks : MutationBenchmark
{
    [ParamsAllValues]
    public TableWriter Writer { get; set; }

    private string _template = "";

    [GlobalSetup]
    public void Setup()
    {
        Fixtures.SweepScratch();
        _template = Fixtures.SingleCommit(Writer);
    }

    [IterationSetup]
    public void Fresh() => CurrentPath = Fixtures.FreshCopy(_template);

    [BenchmarkCategory("Delete"), Benchmark(Baseline = true, Description = "EW scan + DeleteRowsAsync(CopyOnWrite)")]
    public Task<long> EW_DeleteCopyOnWrite() => Ew.DeleteCopyOnWriteAsync(CurrentPath!, Sql.OneCategory);

    [BenchmarkCategory("Delete"), Benchmark(Description = "DD DeleteAsync")]
    public Task<long> DD_Delete() => Dd.DeleteAsync(CurrentPath!, Sql.OneCategory);

    [BenchmarkCategory("Update"), Benchmark(Baseline = true, Description = "EW UpdateAsync")]
    public Task<long> EW_Update() => Ew.UpdateAsync(CurrentPath!, Sql.OneCategory);

    [BenchmarkCategory("Update"), Benchmark(Description = "DD UpdateAsync (SQL)")]
    public Task<long> DD_Update() => Dd.UpdateAsync(CurrentPath!, Sql.OneCategory);
}

/// <summary>
/// EW's own predicate DELETE, which writes a deletion vector, on the same 1M rows written by EW with
/// <c>delta.enableDeletionVectors</c>. A reference for what EW's native path costs next to the copy-on-write
/// DELETE in <see cref="DmlBenchmarks"/>. It has no delta-dotnet counterpart (delta-rs never writes deletion
/// vectors) and only one writer (delta-rs cannot create the table), so this class has no <c>Writer</c>
/// parameter.
/// </summary>
[Config(typeof(SingleShotConfig))]
public class DeletionVectorDeleteBenchmarks : MutationBenchmark
{
    private string _template = "";

    [GlobalSetup]
    public void Setup()
    {
        Fixtures.SweepScratch();
        _template = Fixtures.SingleCommitWithDeletionVectors();
    }

    [IterationSetup]
    public void Fresh() => CurrentPath = Fixtures.FreshCopy(_template);

    [BenchmarkCategory("DeleteDeletionVector"), Benchmark(Baseline = true, Description = "EW DeleteAsync (deletion vector)")]
    public Task<long> EW_DeleteDeletionVector() => Ew.DeleteAsync(CurrentPath!, Sql.OneCategory);
}

/// <summary>
/// Table maintenance on the 100-small-file table: checkpoint, compaction (bin-packing OPTIMIZE), and
/// vacuuming the 100 files a compaction removed.
/// </summary>
[Config(typeof(SingleShotConfig))]
public class MaintenanceBenchmarks : MutationBenchmark
{
    [ParamsAllValues]
    public TableWriter Writer { get; set; }

    private string _commits = "";
    private string _compacted = "";

    [GlobalSetup]
    public void Setup()
    {
        Fixtures.SweepScratch();
        _commits = Fixtures.ManyCommits(Writer, checkpointed: false);
        _compacted = Fixtures.Compacted(Writer);
    }

    [IterationSetup(Targets = [nameof(EW_Checkpoint), nameof(DD_Checkpoint), nameof(EW_Compact), nameof(DD_Optimize)])]
    public void FreshCommits() => CurrentPath = Fixtures.FreshCopy(_commits);

    [IterationSetup(Targets = [nameof(EW_Vacuum), nameof(DD_VacuumFull), nameof(DD_VacuumLite)])]
    public void FreshCompacted() => CurrentPath = Fixtures.FreshCopy(_compacted);

    [BenchmarkCategory("Checkpoint"), Benchmark(Baseline = true, Description = "EW CheckpointAsync")]
    public async Task<long> EW_Checkpoint()
    {
        await using var table = await EwDeltaTable.OpenAsync(Ew.Fs(CurrentPath!));
        return await table.CheckpointAsync();
    }

    [BenchmarkCategory("Checkpoint"), Benchmark(Description = "DD CheckpointAsync")]
    public async Task<long> DD_Checkpoint()
    {
        using var table = await Dd.OpenAsync(CurrentPath!);
        await table.CheckpointAsync(CancellationToken.None);
        return (long)table.Version()!.Value;
    }

    [BenchmarkCategory("Compact"), Benchmark(Baseline = true, Description = "EW CompactAsync")]
    public async Task<long> EW_Compact()
    {
        await using var table = await EwDeltaTable.OpenAsync(Ew.Fs(CurrentPath!));
        return await table.CompactAsync() ?? -1;
    }

    [BenchmarkCategory("Compact"), Benchmark(Description = "DD OptimizeAsync (BinPack)")]
    public async Task<long> DD_Optimize()
    {
        using var table = await Dd.OpenAsync(CurrentPath!);
        await table.OptimizeAsync(new OptimizeOptions(), CancellationToken.None);
        return (long)table.Version()!.Value;
    }

    [BenchmarkCategory("Vacuum"), Benchmark(Baseline = true, Description = "EW VacuumAsync (table retention: 0s)")]
    public async Task<long> EW_Vacuum()
    {
        await using var table = await EwDeltaTable.OpenAsync(Ew.Fs(CurrentPath!));
        var result = await table.VacuumAsync(); // the table's delta.deletedFileRetentionDuration (0s)
        return result.FilesDeleted;
    }

    [BenchmarkCategory("Vacuum"), Benchmark(Description = "DD VacuumAsync(Full, table retention: 0s)")]
    public async Task<long> DD_VacuumFull()
    {
        using var table = await Dd.OpenAsync(CurrentPath!);
        await table.VacuumAsync(new VacuumOptions { VacuumMode = VacuumMode.Full }, CancellationToken.None);
        return (long)table.Version()!.Value;
    }

    [BenchmarkCategory("Vacuum"), Benchmark(Description = "DD VacuumAsync(Lite, table retention: 0s)")]
    public async Task<long> DD_VacuumLite()
    {
        using var table = await Dd.OpenAsync(CurrentPath!);
        await table.VacuumAsync(new VacuumOptions { VacuumMode = VacuumMode.Lite }, CancellationToken.None);
        return (long)table.Version()!.Value;
    }
}
