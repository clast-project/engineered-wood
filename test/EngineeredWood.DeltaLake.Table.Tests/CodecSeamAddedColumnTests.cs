// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.CompilerServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.Expressions;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// #790: through the codec seam, a read never names a column to a file that lacks it. A column added after a file
/// was written is absent from it, and a host that binds columns by name (as <see cref="ParquetFileReader"/> does)
/// refuses the name before the pipeline can backfill the NULLs. An unprojected read asks for every column; a
/// projection is kept for a file whose statistics show it holds every column named, and otherwise that file is read
/// whole.
/// </summary>
public class CodecSeamAddedColumnTests : IDisposable
{
    private readonly string _tempDir;

    public CodecSeamAddedColumnTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_seam_added_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    /// <summary>A host that binds columns by name: <see cref="ParquetFileReader"/> refuses a name the file lacks.</summary>
    private sealed class ByNameReader(ITableFileSystem fs) : IDataFileReader
    {
        public List<(string Path, IReadOnlyList<string>? Columns)> Reads { get; } = [];

        public async IAsyncEnumerable<RecordBatch> ReadAsync(
            string relativePath, IReadOnlyList<string>? physicalColumns,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            lock (Reads)
                Reads.Add((relativePath, physicalColumns));
            await using var file = await fs.OpenReadAsync(relativePath, cancellationToken);
            using var reader = new ParquetFileReader(file, ownsFile: false);
            await foreach (var b in reader.ReadAllAsync(columnNames: physicalColumns, cancellationToken: cancellationToken))
                yield return b;
        }
    }

    private static RecordBatch Rows(Apache.Arrow.Schema schema, long first, int count, bool withScore)
    {
        var ids = new Int64Array.Builder();
        var parts = new StringArray.Builder();
        var scores = new DoubleArray.Builder();
        for (long i = first; i < first + count; i++)
        {
            ids.Append(i);
            parts.Append(i % 2 == 0 ? "even" : "odd");
            scores.Append(i * 1.5);
        }

        IArrowArray[] columns = withScore
            ? [ids.Build(), parts.Build(), scores.Build()]
            : [ids.Build(), parts.Build()];
        return new RecordBatch(schema, columns, count);
    }

    /// <summary>
    /// Ids 0-9 written before <c>score</c> was added, 10-14 after; id 3 deleted (a deletion vector). Returns the
    /// table, its seam, and the paths of the files written with <c>score</c> (one per partition).
    /// </summary>
    private async Task<(DeltaTable Table, ByNameReader Seam, HashSet<string> NewFiles)> CreateAsync(
        bool partitioned, ColumnMappingMode mapping = ColumnMappingMode.None)
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var seam = new ByNameReader(fs);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("part", StringType.Default, true))
            .Build();
        var table = await DeltaTable.CreateAsync(fs, schema,
            new DeltaTableOptions { CheckpointInterval = 0, DataFileReader = seam },
            partitionColumns: partitioned ? ["part"] : null,
            columnMappingMode: mapping,
            enableDeletionVectors: true);

        await table.WriteAsync([Rows(table.ArrowSchema, 0, 10, withScore: false)]);
        await table.AddColumnAsync(new Field("score", DoubleType.Default, true));
        var before = table.CurrentSnapshot.ActiveFiles.Keys.ToHashSet();
        await table.WriteAsync([Rows(table.ArrowSchema, 10, 5, withScore: true)]);
        var newFiles = table.CurrentSnapshot.ActiveFiles.Keys.Where(p => !before.Contains(p)).ToHashSet();
        await table.DeleteAsync(batch =>
        {
            var ids = (Int64Array)batch.Column(batch.Schema.GetFieldIndex("id"));
            var mask = new BooleanArray.Builder();
            for (int i = 0; i < batch.Length; i++)
                mask.Append(ids.GetValue(i) == 3);
            return mask.Build();
        });

        seam.Reads.Clear();
        return (table, seam, newFiles);
    }

    private static async Task<List<string>> ReadAsync(DeltaTable table, DeltaReadOptions options)
    {
        var rows = new List<string>();
        await foreach (var b in table.ReadAsync(options))
        {
            using (b)
            {
                int id = b.Schema.GetFieldIndex("id");
                int score = b.Schema.GetFieldIndex("score");
                for (int i = 0; i < b.Length; i++)
                {
                    long value = ((Int64Array)b.Column(id)).GetValue(i)!.Value;
                    string s = score < 0 ? "-" : ((DoubleArray)b.Column(score)).GetValue(i) is { } d ? d.ToString("0.0", System.Globalization.CultureInfo.InvariantCulture) : "null";
                    rows.Add($"{value}:{s}");
                }
            }
        }

        rows.Sort(StringComparer.Ordinal);
        return rows;
    }

    private static List<string> Expected(IEnumerable<long> ids, bool withScore) =>
        ids.Where(i => i != 3)
            .Select(i => $"{i}:{(!withScore ? "-" : i >= 10 ? (i * 1.5).ToString("0.0", System.Globalization.CultureInfo.InvariantCulture) : "null")}")
            .OrderBy(s => s, StringComparer.Ordinal)
            .ToList();

    /// <summary>The issue's repro: an unprojected read of a partitioned table asks for every column.</summary>
    [Fact]
    public async Task AnUnprojectedReadOfAPartitionedTableReadsTheOlderFiles()
    {
        var (table, seam, _) = await CreateAsync(partitioned: true);
        await using (table)
        {
            Assert.Equal(Expected(Enumerable.Range(0, 15).Select(i => (long)i), withScore: true),
                await ReadAsync(table, new DeltaReadOptions()));
            Assert.All(seam.Reads, r => Assert.Null(r.Columns));
        }
    }

    /// <summary>
    /// A projection naming the added column: the older files lack it, so they are read whole; the file written with
    /// it keeps the projection.
    /// </summary>
    [Theory]
    [InlineData(false, ColumnMappingMode.None)]
    [InlineData(true, ColumnMappingMode.None)]
    [InlineData(false, ColumnMappingMode.Name)]
    [InlineData(true, ColumnMappingMode.Id)]
    public async Task AProjectionOfTheAddedColumnReadsTheOlderFilesWhole(bool partitioned, ColumnMappingMode mapping)
    {
        var (table, seam, newFiles) = await CreateAsync(partitioned, mapping);
        await using (table)
        {
            Assert.Equal(Expected(Enumerable.Range(0, 15).Select(i => (long)i), withScore: true),
                await ReadAsync(table, new DeltaReadOptions { Columns = ["id", "score"] }));
            Assert.Contains(seam.Reads, r => !newFiles.Contains(r.Path));
            Assert.All(seam.Reads, r =>
            {
                if (newFiles.Contains(r.Path))
                    Assert.Equal(2, r.Columns?.Count);
                else
                    Assert.Null(r.Columns);
            });
        }
    }

    /// <summary>A projection of columns every file holds keeps the projection on every file.</summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AProjectionOfOriginalColumnsIsKeptOnEveryFile(bool partitioned)
    {
        var (table, seam, _) = await CreateAsync(partitioned);
        await using (table)
        {
            Assert.Equal(Expected(Enumerable.Range(0, 15).Select(i => (long)i), withScore: false),
                await ReadAsync(table, new DeltaReadOptions { Columns = ["id"] }));
            Assert.NotEmpty(seam.Reads);
            Assert.All(seam.Reads, r => Assert.Equal(["id"], r.Columns));
        }
    }

    /// <summary>
    /// A struct column's statistics are its leaves' (<c>s.a</c>, <c>s.b</c>), never <c>s</c> itself: a projection of
    /// the struct is still kept.
    /// </summary>
    [Fact]
    public async Task AProjectionOfAStructColumnIsKept()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var seam = new ByNameReader(fs);
        var sType = new Apache.Arrow.Types.StructType([new Field("a", Int64Type.Default, true), new Field("b", StringType.Default, true)]);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("s", sType, true))
            .Build();
        await using var table = await DeltaTable.CreateAsync(fs, schema,
            new DeltaTableOptions { CheckpointInterval = 0, DataFileReader = seam });
        var a = new Int64Array.Builder().Append(1).Append(2).Build();
        var b = new StringArray.Builder().Append("x").Append("y").Build();
        await table.WriteAsync([new RecordBatch(table.ArrowSchema,
            [new Int64Array.Builder().Append(10).Append(20).Build(), new StructArray(sType, 2, [a, b], ArrowBuffer.Empty, 0)], 2)]);

        int rows = 0;
        await foreach (var batch in table.ReadAsync(new DeltaReadOptions { Columns = ["s"] }))
        {
            using (batch)
                rows += batch.Length;
        }

        Assert.Equal(2, rows);
        var read = Assert.Single(seam.Reads);
        Assert.Equal(["s"], read.Columns);
    }

    /// <summary>
    /// An exact filter on the added column, not projected: the older files are read whole (the filter's column is
    /// NULL there), and the file holding it keeps the projection plus the filter's column.
    /// </summary>
    [Fact]
    public async Task AnExactFilterOnTheAddedColumnReadsOnlyTheOlderFilesWhole()
    {
        var (table, seam, newFiles) = await CreateAsync(partitioned: false);
        await using (table)
        {
            var options = new DeltaReadOptions
            {
                Columns = ["id"],
                Filter = Ex.GreaterThan("score", LiteralValue.Of(18.0)),
                FilterRows = true,
            };
            Assert.Equal(["13:-", "14:-"], await ReadAsync(table, options));
            Assert.All(seam.Reads, r =>
            {
                if (newFiles.Contains(r.Path))
                    Assert.Equal(["id", "score"], r.Columns!.OrderBy(c => c, StringComparer.Ordinal));
                else
                    Assert.Null(r.Columns);
            });
        }
    }
}
