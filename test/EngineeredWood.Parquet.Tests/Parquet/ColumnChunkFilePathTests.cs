// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using System.Reflection;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Expressions;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Metadata;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.Tests.Parquet;

/// <summary>
/// <see cref="ParquetReadOptions.ColumnChunkFilePath"/> (#405): a column chunk whose <c>file_path</c>
/// says it is stored in another file is refused by default, everywhere its offsets would be read,
/// and read from this file when the caller chooses to ignore the field.
/// </summary>
/// <remarks>
/// Column <c>b</c>'s chunk is given a <c>file_path</c> by rewriting the footer; its data really is in
/// this file, so reading it anyway (<see cref="ColumnChunkFilePathKind.Ignore"/>) returns the right
/// values. Column <c>a</c> has none.
/// </remarks>
public class ColumnChunkFilePathTests : IDisposable
{
    private const int Rows = 1000;
    private const long BOffset = 100_000;

    private readonly string _tempDir;

    public ColumnChunkFilePathTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-file-path-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    public static TheoryData<string> FilePaths() => new() { "part-0.parquet", "" };

    // ───── Refuse (default) ─────

    [Theory]
    [MemberData(nameof(FilePaths))]
    public async Task Refuse_EveryReadOfTheChunk(string filePath)
    {
        string path = await WriteAsync(filePath);
        var options = new ParquetReadOptions();
        Assert.Equal(ColumnChunkFilePathKind.Refuse, options.ColumnChunkFilePath);

        var whole = await Assert.ThrowsAsync<NotSupportedException>(() => ReadRowGroupAsync(path, options, null));
        Assert.Contains("Column 'b' is stored in another file", whole.Message);
        Assert.Contains("ColumnChunkFilePath", whole.Message);

        await Assert.ThrowsAsync<NotSupportedException>(() => ReadAllAsync(path, options));
        await Assert.ThrowsAsync<NotSupportedException>(() => ReadAllAsync(path, options with { BatchSize = 100 }));
        await Assert.ThrowsAsync<NotSupportedException>(() => ReadPageIndexAsync(path, options, ["b"]));
    }

    [Fact]
    public async Task Refuse_LeavesTheFooterAndTheOtherColumnsReadable()
    {
        string path = await WriteAsync("part-0.parquet");
        var options = new ParquetReadOptions();

        await using var input = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(input, ownsFile: false, options);
        var metadata = await reader.ReadMetadataAsync();
        Assert.Equal("part-0.parquet", metadata.RowGroups[0].Columns[1].FilePath);

        var a = await reader.ReadRowGroupAsync(0, ["a"]);
        Assert.Equal(Rows, a.Length);
        Assert.Single(await reader.ReadPageIndexAsync(0, ["a"]));
    }

    /// <summary>
    /// A chunk stored elsewhere has its Bloom filter there too. Reading this file at that offset
    /// reads some other filter, which here says the value is absent, and the row group would be
    /// skipped: the caller would get no rows and no error, instead of the refusal.
    /// </summary>
    [Fact]
    public async Task Refuse_DoesNotPruneOnTheChunksBloomFilter()
    {
        string path = await WriteAsync("part-0.parquet", bloomFilterOfA: true);
        var options = new ParquetReadOptions
        {
            Filter = Ex.Equal("b", LiteralValue.Of(BOffset + 500)),
            FilterUseBloomFilters = true,
        };

        await Assert.ThrowsAsync<NotSupportedException>(() => ReadAllAsync(path, options));

        await using var input = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(input, ownsFile: false, options);
        var candidates = await reader.GetCandidateRowGroupsAsync("b", BOffset + 500);
        Assert.True(candidates[0]);
    }

    // ───── Ignore ─────

    [Theory]
    [MemberData(nameof(FilePaths))]
    public async Task Ignore_ReadsTheChunkFromThisFile(string filePath)
    {
        string path = await WriteAsync(filePath);
        var options = new ParquetReadOptions { ColumnChunkFilePath = ColumnChunkFilePathKind.Ignore };

        var batch = await ReadRowGroupAsync(path, options, null);
        var b = (Int64Array)batch.Column("b");
        Assert.Equal(Enumerable.Range(0, Rows).Select(i => (long?)(BOffset + i)), Enumerable.Range(0, Rows).Select(i => b.GetValue(i)));

        // The batched read builds its page map from the OffsetIndex, whose offsets are this file's too.
        var batched = await ReadAllAsync(path, options with { BatchSize = 100 });
        Assert.Equal(Rows, batched.Sum(x => x.Length));
        Assert.Equal(BOffset + Rows - 1, ((Int64Array)batched[^1].Column("b")).GetValue(batched[^1].Length - 1));

        var index = Assert.Single(await ReadPageIndexAsync(path, options, ["b"]));
        Assert.True(index.HasOffsetIndex);
    }

    /// <summary>
    /// Page pruning follows the policy the rest of the reader does: under Ignore, b's page index is this
    /// file's and narrows the rows; under Refuse, b's chunk is someone else's and the whole row group
    /// comes back rather than an error.
    /// </summary>
    [Theory]
    [MemberData(nameof(FilePaths))]
    public async Task CandidateRowRanges_UseAStoredElsewhereIndexOnlyUnderIgnore(string filePath)
    {
        string path = await WriteAsync(filePath);
        var filter = Ex.Equal("b", LiteralValue.Of(BOffset + 500));

        async Task<IReadOnlyList<RowRange>> RangesAsync(ColumnChunkFilePathKind kind)
        {
            await using var input = new LocalRandomAccessFile(path);
            using var reader = new ParquetFileReader(input, ownsFile: false, new ParquetReadOptions { ColumnChunkFilePath = kind });
            return await reader.GetCandidateRowRangesAsync(0, filter);
        }

        var ignored = Assert.Single(await RangesAsync(ColumnChunkFilePathKind.Ignore));
        Assert.True(ignored.Start <= 500 && 500 < ignored.End && ignored.Length < Rows, $"{ignored}");

        Assert.Equal([new RowRange(0, Rows)], await RangesAsync(ColumnChunkFilePathKind.Refuse));
    }

    /// <summary>
    /// A filtered read with <see cref="ParquetReadOptions.FilterUsePageIndex"/> reads the OffsetIndexes of
    /// the columns it projects ahead of their data. Under Refuse, b's is another file's, so it is not
    /// read: the read refuses b as any read does, without first fetching bytes b's offsets point at.
    /// </summary>
    [Theory]
    [MemberData(nameof(FilePaths))]
    public async Task FilterUsePageIndex_UnderRefuse_DoesNotReadAStoredElsewhereIndex(string filePath)
    {
        string path = await WriteAsync(filePath);
        long bIndex;
        int bIndexLength;
        await using (var probe = new LocalRandomAccessFile(path))
        using (var reader = new ParquetFileReader(probe, ownsFile: false))
        {
            var b = (await reader.ReadMetadataAsync()).RowGroups[0].Columns[1];
            bIndex = b.OffsetIndexOffset!.Value;
            bIndexLength = b.OffsetIndexLength!.Value;
        }

        var counting = new RequestCountingFile(new LocalRandomAccessFile(path));
        using (var reader = new ParquetFileReader(counting, ownsFile: true, new ParquetReadOptions
        {
            Filter = Ex.Equal("a", LiteralValue.Of(500L)),
            FilterUsePageIndex = true,
        }))
        {
            await Assert.ThrowsAsync<NotSupportedException>(async () =>
            {
                await foreach (var batch in reader.ReadAllAsync())
                    batch.Dispose();
            });
        }

        Assert.DoesNotContain(counting.RequestRanges.SelectMany(r => r),
            r => r.Offset < bIndex + bIndexLength && bIndex < r.Offset + r.Length);
    }

    // ───── Helpers ─────

    /// <summary>
    /// Writes columns <c>a</c> (0..999) and <c>b</c> (100000..100999), both with Bloom filters, then
    /// gives <c>b</c>'s chunk <paramref name="filePath"/>. With <paramref name="bloomFilterOfA"/>, b's
    /// Bloom filter location is pointed at a's filter, standing in for bytes of another file.
    /// </summary>
    private async Task<string> WriteAsync(string filePath, bool bloomFilterOfA = false)
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("a", Int64Type.Default, false))
            .Field(new Field("b", Int64Type.Default, false))
            .Build();
        var batch = new RecordBatch(schema,
        [
            new Int64Array.Builder().AppendRange(Enumerable.Range(0, Rows).Select(i => (long)i)).Build(),
            new Int64Array.Builder().AppendRange(Enumerable.Range(0, Rows).Select(i => BOffset + i)).Build(),
        ], Rows);

        string path = Path.Combine(_tempDir, Guid.NewGuid().ToString("N")[..8] + ".parquet");
        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false, new ParquetWriteOptions
        {
            BloomFilterColumns = ["a", "b"],
            DictionaryEnabled = false,
            DataPageSize = 1024,
        }))
        {
            await writer.WriteRowGroupAsync(batch);
            await writer.CloseAsync();
        }

        byte[] bytes = File.ReadAllBytes(path);
        int footerLength = BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(bytes.Length - 8, 4));
        int bodyLength = bytes.Length - 8 - footerLength;
        var metadata = MetadataDecoder.DecodeFileMetaData(bytes.AsSpan(bodyLength, footerLength));

        var rowGroup = metadata.RowGroups[0];
        var a = rowGroup.Columns[0];
        var b = With(rowGroup.Columns[1], nameof(ColumnChunk.FilePath), filePath);
        if (bloomFilterOfA)
        {
            var meta = With(b.MetaData!, nameof(ColumnMetaData.BloomFilterOffset), a.MetaData!.BloomFilterOffset);
            meta = With(meta, nameof(ColumnMetaData.BloomFilterLength), a.MetaData.BloomFilterLength);
            b = With(b, nameof(ColumnChunk.MetaData), meta);
        }

        var rewritten = With(metadata, nameof(FileMetaData.RowGroups),
            new[] { With(rowGroup, nameof(RowGroup.Columns), new[] { a, b }) });
        byte[] footer = MetadataEncoder.EncodeFileMetaData(rewritten);

        using var output = new MemoryStream();
        output.Write(bytes, 0, bodyLength);
        output.Write(footer, 0, footer.Length);
        var suffix = new byte[8];
        BinaryPrimitives.WriteInt32LittleEndian(suffix, footer.Length);
        "PAR1"u8.CopyTo(suffix.AsSpan(4));
        output.Write(suffix, 0, suffix.Length);
        File.WriteAllBytes(path, output.ToArray());
        return path;
    }

    private static async Task<RecordBatch> ReadRowGroupAsync(string path, ParquetReadOptions options, IReadOnlyList<string>? columns)
    {
        await using var input = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(input, ownsFile: false, options);
        return await reader.ReadRowGroupAsync(0, columns);
    }

    private static async Task<List<RecordBatch>> ReadAllAsync(string path, ParquetReadOptions options)
    {
        await using var input = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(input, ownsFile: false, options);
        var batches = new List<RecordBatch>();
        await foreach (var batch in reader.ReadAllAsync())
            batches.Add(batch);
        return batches;
    }

    private static async Task<IReadOnlyList<ColumnChunkPageIndex>> ReadPageIndexAsync(
        string path, ParquetReadOptions options, IReadOnlyList<string> columns)
    {
        await using var input = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(input, ownsFile: false, options);
        return await reader.ReadPageIndexAsync(0, columns);
    }

    /// <summary>A copy of <paramref name="source"/> with one init-only property replaced.</summary>
    private static T With<T>(T source, string property, object? value)
        where T : class
    {
        var copy = (T)typeof(object).GetMethod("MemberwiseClone", BindingFlags.Instance | BindingFlags.NonPublic)!
            .Invoke(source, null)!;
        typeof(T).GetProperty(property)!.SetValue(copy, value);
        return copy;
    }
}
