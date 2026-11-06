// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.Tests.Parquet;

/// <summary>
/// A projected column name may hold a dot. A top-level column literally named <c>s.a</c> beside a struct <c>s</c>
/// with a field <c>a</c> is that column, whichever comes first in the file, and not the struct's leaf, whose
/// dotted path is the same text (#718). The struct's leaf is still reachable by its path when no top-level
/// column holds that name.
/// </summary>
public class DottedNameProjectionTests : IDisposable
{
    private readonly string _tempDir;

    public DottedNameProjectionTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"ew_dotted_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    private static readonly Field StructField =
        new("s", new StructType([new Field("a", Int64Type.Default, false)]), false);

    private async Task<string> WriteAsync(bool structFirst, bool withDotted = true)
    {
        var leaves = new Int64Array.Builder().AppendRange([1, 2, 3]).Build();
        var s = new StructArray(StructField.DataType, 3, [leaves], ArrowBuffer.Empty, 0);
        var dotted = new Int64Array.Builder().AppendRange([101, 102, 103]).Build();
        var fields = new List<Field>();
        var columns = new List<IArrowArray>();
        foreach (bool structNow in structFirst ? new[] { true, false } : [false, true])
        {
            if (structNow)
            {
                fields.Add(StructField);
                columns.Add(s);
            }
            else if (withDotted)
            {
                fields.Add(new Field("s.a", Int64Type.Default, false));
                columns.Add(dotted);
            }
        }

        string path = Path.Combine(_tempDir, $"{structFirst}-{withDotted}.parquet");
        await using var file = new LocalSequentialFile(path);
        await using var writer = new ParquetFileWriter(file, ownsFile: false);
        await writer.WriteRowGroupAsync(new RecordBatch(new Apache.Arrow.Schema(fields, null), columns, 3));
        await writer.CloseAsync();
        return path;
    }

    private static async Task<RecordBatch> ReadAsync(string path, string column, ParquetReadOptions? options = null)
    {
        var batch = await TryReadAsync(path, column, options);
        return Assert.IsType<RecordBatch>(batch);
    }

    /// <summary>The one batch a read of <paramref name="column"/> returns, or null when pruning skipped it.</summary>
    private static async Task<RecordBatch?> TryReadAsync(string path, string column, ParquetReadOptions? options)
    {
        await using var file = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(file, ownsFile: false, options);
        RecordBatch? result = null;
        await foreach (var batch in reader.ReadAllAsync([column]))
        {
            Assert.Null(result);
            result = batch;
        }
        return result;
    }

    /// <summary>
    /// A filter on <c>s.a</c> prunes by the top-level column too, the column the read returns, in either order:
    /// the statistics, page-index, dictionary and Bloom filter lookups all take the top-level name first. Only
    /// the top-level column holds 102; the struct's leaf holds 1..3, so pruning by its statistics or dictionary
    /// skips the one row group. The statistics lookup let the later of the two win and the dictionary lookup the
    /// earlier, so each order failed one of them.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task AFilterOnATopLevelDottedName_PrunesByTheTopLevelColumn(bool structFirst)
    {
        string path = await WriteAsync(structFirst);
        var options = ParquetReadOptions.Default with { Filter = Ex.Equal("s.a", 102L) };

        var batch = await TryReadAsync(path, "s.a", options);

        Assert.True(batch is not null, "the filter pruned the row group holding s.a = 102");
        using (batch)
        {
            var values = (Int64Array)batch!.Column(0);
            Assert.Contains(102L, Enumerable.Range(0, values.Length).Select(i => values.GetValue(i)!.Value));
        }

        // And a value only the struct's leaf holds is pruned: the filter is not ignored.
        var pruned = await TryReadAsync(path, "s.a", options with { Filter = Ex.Equal("s.a", 2L) });
        Assert.Null(pruned);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ATopLevelDottedName_ReadsTheTopLevelColumn(bool structFirst)
    {
        using var batch = await ReadAsync(await WriteAsync(structFirst), "s.a");

        var field = Assert.Single(batch.Schema.FieldsList);
        Assert.Equal("s.a", field.Name);
        var values = (Int64Array)batch.Column(0);
        Assert.Equal([101L, 102L, 103L], Enumerable.Range(0, values.Length).Select(i => values.GetValue(i)!.Value));
    }

    [Fact]
    public async Task ADottedPath_StillReadsTheNestedLeaf_WhenNoTopLevelColumnHasThatName()
    {
        using var batch = await ReadAsync(await WriteAsync(structFirst: true, withDotted: false), "s.a");

        var field = Assert.Single(batch.Schema.FieldsList);
        Assert.Equal("s", field.Name);
        var leaf = (Int64Array)((StructArray)batch.Column(0)).Fields[0];
        Assert.Equal([1L, 2L, 3L], Enumerable.Range(0, leaf.Length).Select(i => leaf.GetValue(i)!.Value));
    }
}
