// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// Opening a data file asks the store nothing before its first read (#722): every path that reads one takes the
/// file's size from its <c>add</c> action, where it used to ask the file (a HEAD per file on an object store). With
/// the default <see cref="ParquetReadOptions.FooterReadSize"/>, the footer then costs one request, so a small
/// file's first request is a read of its tail.
/// </summary>
public class DataFileOpenRequestTests : IDisposable
{
    private const int Files = 3;
    private const int RowsPerFile = 100;

    private readonly string _tempDir;

    public DataFileOpenRequestTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_open_{Guid.NewGuid():N}");
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
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("tag", StringType.Default, false))
        .Build();

    private async Task<(DeltaTable Table, DataFileCountingFileSystem Fs)> CreateAsync(string name, bool dv)
    {
        var fs = new DataFileCountingFileSystem(new LocalTableFileSystem(Path.Combine(_tempDir, name)));
        var table = await DeltaTable.CreateAsync(fs, TableSchema, enableDeletionVectors: dv);
        for (int f = 0; f < Files; f++)
        {
            var ids = new Int64Array.Builder();
            var tags = new StringArray.Builder();
            for (int i = 0; i < RowsPerFile; i++)
            {
                ids.Append((f * RowsPerFile) + i);
                tags.Append(i % 2 == 0 ? "a" : "c");
            }
            await table.WriteAsync([new RecordBatch(TableSchema, [ids.Build(), tags.Build()], RowsPerFile)]);
        }
        Assert.Equal(Files, table.CurrentSnapshot.ActiveFiles.Count);
        fs.Reset();
        return (table, fs);
    }

    [Fact]
    public async Task Scan_AsksNoLengthAndReadsEachFooterInOneRequest()
    {
        var (table, fs) = await CreateAsync("scan", dv: false);
        var sizes = table.CurrentSnapshot.ActiveFiles.Values.ToDictionary(a => DeltaPath.Decode(a.Path), a => a.Size);

        long rows = 0;
        await foreach (var batch in table.ReadAllAsync())
            rows += batch.Length;

        Assert.Equal(Files * RowsPerFile, rows);
        Assert.Equal(0, fs.DataLengthRequests);
        // The first read of each file is its footer: one request that ends at the file's end and holds the whole
        // (small) file, since the file is shorter than the default speculative tail.
        foreach (var path in sizes.Keys)
        {
            var first = fs.DataReads.First(r => r.Path == path).Range;
            Assert.Equal(sizes[path], first.Offset + first.Length);
            Assert.Equal(0, first.Offset);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Delete_AsksNoLength(bool dv)
    {
        var (table, fs) = await CreateAsync($"delete_{dv}", dv);
        var (deleted, _) = await table.DeleteAsync(Ex.Equal("tag", "a"));
        Assert.Equal(Files * RowsPerFile / 2, deleted);
        Assert.Equal(0, fs.DataLengthRequests);
    }

    [Fact]
    public async Task FunctionalDelete_AsksNoLength()
    {
        var (table, fs) = await CreateAsync("fdelete", dv: true);
        var (deleted, _) = await table.DeleteAsync(batch =>
        {
            var ids = (Int64Array)batch.Column("id");
            var b = new BooleanArray.Builder();
            for (int i = 0; i < ids.Length; i++)
                b.Append(ids.GetValue(i)!.Value % 10 == 0);
            return b.Build();
        });
        Assert.Equal(Files * RowsPerFile / 10, deleted);
        Assert.Equal(0, fs.DataLengthRequests);
    }

    [Fact]
    public async Task Compact_AsksNoLength()
    {
        var (table, fs) = await CreateAsync("compact", dv: false);
        Assert.NotNull(await table.CompactAsync());
        Assert.Single(table.CurrentSnapshot.ActiveFiles);
        Assert.Equal(0, fs.DataLengthRequests);
    }
}
