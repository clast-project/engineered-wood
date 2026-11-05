// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.CompilerServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.Expressions;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// A data file re-added under a deletion vector carries its physical row count even when its statistics lacked one
/// (#705): the protocol requires "correct <c>numRecords</c>" on any logical file with a deletion vector. Each
/// deletion-vector writer is driven over a file whose statistics were taken away. (MERGE's own case is
/// <see cref="MergeDeletionVectorTests"/>.)
/// </summary>
public class DeletionVectorRowCountTests : IDisposable
{
    private readonly string _tempDir;

    public DeletionVectorRowCountTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_dv_count_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    private static readonly Apache.Arrow.Schema TableSchema = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, true))
        .Build();

    private static RecordBatch Ids(params long[] ids) =>
        new(TableSchema, [new Int64Array.Builder().AppendRange(ids).Build()], ids.Length);

    /// <summary>A deletion-vector table of one file holding ids 1..5, whose statistics are then
    /// <paramref name="stats"/>; with <paramref name="existingVector"/>, id 5 is already deleted first.</summary>
    private async Task<DeltaTable> CreateAsync(
        string? stats, bool existingVector = false, DeltaTableOptions? options = null)
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var table = options is null
            ? await DeltaTable.CreateAsync(fs, TableSchema, enableDeletionVectors: true)
            : await DeltaTable.CreateAsync(fs, TableSchema, options, enableDeletionVectors: true);
        await table.WriteAsync([Ids(1, 2, 3, 4, 5)]);
        if (existingVector)
            await table.DeleteAsync(Ex.Equal("id", LiteralValue.Of(5L)));

        var file = Only(table);
        await using var transaction = table.StartTransaction();
        transaction.StageActions(
        [
            new RemoveFile
            {
                Path = file.Path,
                DeletionTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                DataChange = false,
            },
            file with { Stats = stats, TypedStats = null, DataChange = false },
        ]);
        await transaction.CommitAsync();
        Assert.Null(Only(table).GetNumRecords());
        return table;
    }

    private static AddFile Only(DeltaTable table) => table.CurrentSnapshot.ActiveFiles.Values.Single();

    private static RowSelection Rows(DeltaTable table, params long[] positions) => RowSelection.ByPath(
        new Dictionary<string, IReadOnlyCollection<long>> { [Only(table).Path] = positions });

    private static void AssertCounted(AddFile file, string? originalStats)
    {
        Assert.NotNull(file.DeletionVector);
        Assert.Equal(5, file.GetNumRecords());
        Assert.Contains("\"tightBounds\":false", file.Stats);
        if (originalStats is not null)
            Assert.Contains("\"minValues\":{\"id\":1}", file.Stats);
    }

    public static TheoryData<string?, bool> Cases => new()
    {
        { null, false },
        { "{\"minValues\":{\"id\":1}}", false },
        { null, true },
    };

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task APredicateDelete(string? stats, bool existingVector)
    {
        await using var table = await CreateAsync(stats, existingVector);

        await table.DeleteAsync(Ex.Equal("id", LiteralValue.Of(2L)));

        AssertCounted(Only(table), stats);
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task ARowLevelDelete(string? stats, bool existingVector)
    {
        await using var table = await CreateAsync(stats, existingVector);

        await table.DeleteRowsAsync(Rows(table, 1), RowDeleteMode.DeletionVector);

        AssertCounted(Only(table), stats);
    }

    [Fact]
    public async Task AStagedRowLevelDelete()
    {
        await using var table = await CreateAsync(stats: null);

        await using (var transaction = table.StartTransaction())
        {
            await transaction.StageRowDeletesAsync(Rows(table, 1), RowDeleteMode.DeletionVector);
            await transaction.CommitAsync();
        }

        AssertCounted(Only(table), originalStats: null);
    }

    [Fact]
    public async Task TheActionsOfAnUncommittedVectorDelete()
    {
        await using var table = await CreateAsync(stats: null);

        var (actions, _) = await table.ComputeDeletionVectorActionsAsync(
            new Dictionary<int, IReadOnlyCollection<long>> { [0] = [1L] });

        AssertCounted(actions.OfType<AddFile>().Single(), originalStats: null);
    }

    [Fact]
    public async Task AFileWithACount_KeepsIt()
    {
        // A count already there is kept, and through the codec seam the file is not read to find one.
        var reader = new CountingReader(new LocalTableFileSystem(_tempDir));
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), TableSchema,
            new DeltaTableOptions { CheckpointInterval = 0, DataFileReader = reader }, enableDeletionVectors: true);
        await table.WriteAsync([Ids(1, 2, 3, 4, 5)]);
        reader.Reads = 0;

        await table.DeleteRowsAsync(Rows(table, 1), RowDeleteMode.DeletionVector);

        Assert.Equal(5, Only(table).GetNumRecords());
        Assert.Equal(0, reader.Reads);
    }

    [Fact]
    public async Task ThroughTheCodecSeam_TheFileIsCountedByReadingIt()
    {
        // The seam hides the footer, so the count comes from reading the file through it.
        var fs = new LocalTableFileSystem(_tempDir);
        var reader = new CountingReader(fs);
        await using var table = await CreateAsync(
            stats: null, options: new DeltaTableOptions { CheckpointInterval = 0, DataFileReader = reader });
        reader.Reads = 0;

        await table.DeleteRowsAsync(Rows(table, 1), RowDeleteMode.DeletionVector);

        AssertCounted(Only(table), originalStats: null);
        Assert.Equal(1, reader.Reads);
    }

    /// <summary>A codec-seam reader that counts the files it decodes, delegating to the built-in codec.</summary>
    private sealed class CountingReader(ITableFileSystem fs) : IDataFileReader
    {
        public int Reads { get; set; }

        public async IAsyncEnumerable<RecordBatch> ReadAsync(
            string relativePath, IReadOnlyList<string>? physicalColumns,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            Reads++;
            await using var file = await fs.OpenReadAsync(relativePath, cancellationToken);
            using var reader = new ParquetFileReader(file, ownsFile: false);
            await foreach (var b in reader.ReadAllAsync(columnNames: physicalColumns, cancellationToken: cancellationToken))
                yield return b;
        }
    }
}
