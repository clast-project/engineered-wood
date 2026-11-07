// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.DeltaLake.Table.ChangeDataFeed;
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

    private async Task<(DeltaTable Table, DataFileCountingFileSystem Fs)> CreateAsync(
        string name, bool dv, bool cdf = false)
    {
        var fs = new DataFileCountingFileSystem(new LocalTableFileSystem(Path.Combine(_tempDir, name)));
        var table = await DeltaTable.CreateAsync(
            fs, TableSchema, enableDeletionVectors: dv,
            configuration: cdf ? new Dictionary<string, string> { ["delta.enableChangeDataFeed"] = "true" } : null);
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

    public enum Operation { Scan, Delete, Compact, ChangeFeed }

    /// <summary>Runs <paramref name="operation"/>, which reads every data file of a table made by
    /// <see cref="CreateAsync"/> with <c>cdf: true</c>.</summary>
    private static async Task RunAsync(DeltaTable table, Operation operation)
    {
        switch (operation)
        {
            case Operation.Scan:
                await foreach (var b in table.ReadAllAsync())
                    b.Dispose();
                break;
            case Operation.Delete:
                await table.DeleteAsync(Ex.Equal("tag", "a"));
                break;
            case Operation.Compact:
                await table.CompactAsync();
                break;
            case Operation.ChangeFeed:
                await foreach (var b in table.ReadChangesAsync(new DeltaChangeReadOptions
                {
                    StartVersion = 1, EndVersion = table.CurrentSnapshot.Version,
                }))
                    b.Dispose();
                break;
        }
    }

    /// <summary>The path on disk of the table's first data file (by path, so the same one every run).</summary>
    private string FirstDataFile(DeltaTable table, string name) =>
        Path.Combine(_tempDir, name, DeltaPath.Decode(
            table.CurrentSnapshot.ActiveFiles.Values.Select(a => a.Path).OrderBy(p => p, StringComparer.Ordinal).First()));

    /// <summary>
    /// A data file whose size no longer matches its action fails with an error naming both sizes, whichever path
    /// reads it, rather than with the error the misplaced footer read raises: a Parquet one for a larger file (5
    /// bytes inserted at its START, so the recorded tail is no longer the footer), a read past the end for a
    /// smaller one. Bytes appended at the end are not a case: the file's first <c>size</c> bytes are still the
    /// file the action describes, and it reads correctly.
    /// </summary>
    [Theory]
    [InlineData(Operation.Scan, 5)]
    [InlineData(Operation.Scan, -5)]
    [InlineData(Operation.Delete, 5)]
    [InlineData(Operation.Compact, 5)]
    [InlineData(Operation.ChangeFeed, 5)]
    [InlineData(Operation.ChangeFeed, -5)]
    public async Task AFileWhoseSizeChanged_FailsNamingBothSizes(Operation operation, int change)
    {
        string name = $"size_{operation}_{change}";
        var (table, fs) = await CreateAsync(name, dv: false, cdf: true);
        string path = FirstDataFile(table, name);
        long recorded = new FileInfo(path).Length;
        byte[] bytes = File.ReadAllBytes(path);
        File.WriteAllBytes(path, change > 0
            ? new byte[change].Concat(bytes).ToArray()
            : bytes.Take(bytes.Length + change).ToArray());

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(() => RunAsync(table, operation));

        Assert.Equal(DeltaTableErrorCodes.DataFileSizeMismatch, ex.ErrorCode);
        Assert.Contains($"is {recorded + change} bytes", ex.Message);
        Assert.Contains($"records its size as {recorded}", ex.Message);
        Assert.NotNull(ex.InnerException);
        // The store is asked for the size only to explain the failure.
        Assert.Equal(1, fs.DataLengthRequests);
    }

    /// <summary>A corrupt file of the recorded size keeps its own Parquet error: the size is not the problem.</summary>
    [Theory]
    [InlineData(Operation.Scan)]
    [InlineData(Operation.ChangeFeed)]
    public async Task ACorruptFileOfTheRecordedSize_KeepsItsParquetError(Operation operation)
    {
        string name = $"corrupt_{operation}";
        var (table, _) = await CreateAsync(name, dv: false, cdf: true);
        string path = FirstDataFile(table, name);
        using (var stream = new FileStream(path, FileMode.Open, FileAccess.ReadWrite))
        {
            stream.Seek(-4, SeekOrigin.End);
            stream.Write("XXXX"u8.ToArray(), 0, 4);
        }

        var ex = await Assert.ThrowsAsync<ParquetFormatException>(() => RunAsync(table, operation));
        Assert.Contains("PAR1", ex.Message);
    }
}
