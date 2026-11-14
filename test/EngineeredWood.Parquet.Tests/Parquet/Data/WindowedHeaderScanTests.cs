// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Data;
using EngineeredWood.Parquet.Metadata;
using EngineeredWood.Parquet.Schema;

namespace EngineeredWood.Tests.Parquet.Data;

/// <summary>
/// A chunk larger than the header-scan budget has its page headers scanned in windows rather than read whole
/// (#878): a foreign ~1 GB row group held its whole chunk while scanning, and one over 2 GiB on disk could not be
/// read at all, since one buffer cannot hold it.
/// </summary>
public class WindowedHeaderScanTests : IDisposable
{
    private readonly string _tempDir =
        Path.Combine(Path.GetTempPath(), "ew-windowed-scan-" + Guid.NewGuid().ToString("N")[..8]);

    public WindowedHeaderScanTests() => Directory.CreateDirectory(_tempDir);

    public void Dispose()
    {
        try
        {
            Directory.Delete(_tempDir, recursive: true);
        }
        catch (IOException)
        {
        }
    }

    /// <summary>
    /// Windows from one byte (every header cut, every window grown) to larger than any chunk: dictionary pages
    /// cut by a window's end, data pages longer than a window, V1 and V2 pages and every codec the fixtures use.
    /// </summary>
    public static TheoryData<long> Windows => new() { 1, 37, 4096, 1L << 30 };

    /// <summary>For every flat chunk of every fixture the whole-chunk scan reads, the windowed scan finds the same
    /// pages, rows and side pages.</summary>
    [Theory]
    [MemberData(nameof(Windows))]
    public async Task EveryFixture_WindowedScanMatchesTheWholeScan(long window)
    {
        int compared = 0;
        foreach (string path in TestData.GetAllParquetFiles())
            compared += await CompareAsync(path, window);

        Assert.True(compared > 200, $"only {compared} chunks compared");
    }

    /// <summary>A header the window cuts is read from the next window, which starts with it.</summary>
    [Fact]
    public async Task HeadersCutByEveryWindow_AreReadWhole()
    {
        string path = WriteFixture(pages: 400, dictionary: true);
        Assert.True(await CompareAsync(path, 100) > 0);
    }

    /// <summary>A data page the reader skips past need not be read: only one window's bytes are held at a time.
    /// No request holds the whole chunk.</summary>
    [Fact]
    public async Task ABatchedRead_ScansAChunkOverTheBudgetInWindows()
    {
        string path = WriteFixture(pages: 200, dictionary: false);
        await using var local = new LocalRandomAccessFile(path);
        var file = new RequestCountingFile(local);
        using var reader = new ParquetFileReader(
            file, ownsFile: false, new ParquetReadOptions { BatchSize = 1000 })
        {
            HeaderScanBudgetBytes = 1,
            BatchPageWindowBudgetBytes = 16 * 1024,
        };
        var metadata = await reader.ReadMetadataAsync();
        long chunkLength = metadata.RowGroups[0].Columns[0].MetaData!.TotalCompressedSize;
        int before = file.RequestRanges.Count;

        long rows = 0, mismatched = 0;
        await foreach (var batch in reader.ReadAllAsync())
        {
            using (batch)
            {
                var ids = (Int64Array)batch.Column(0);
                for (int i = 0; i < ids.Length; i++, rows++)
                {
                    if (ids.GetValue(i)!.Value != (long)((ulong)rows * 0x9E3779B97F4A7C15UL >> 1))
                        mismatched++;
                }
            }
        }

        Assert.Equal(200 * 1000, rows);
        Assert.Equal(0, mismatched);
        long largest = file.RequestRanges.Skip(before).SelectMany(r => r).Max(r => r.Length);
        Assert.True(largest < chunkLength / 4, $"a request of {largest} bytes for a chunk of {chunkLength}");
    }

    /// <summary>A corrupt header in the largest window is reported, not retried for ever.</summary>
    [Fact]
    public async Task ACorruptHeader_IsReported()
    {
        string path = WriteFixture(pages: 10, dictionary: false);
        var (bytes, metadata) = ReadFooter(path);
        var meta = metadata.RowGroups[0].Columns[0].MetaData!;
        long start = meta.DataPageOffset;
        bytes.AsSpan(checked((int)start), 64).Fill(0xFF);
        File.WriteAllBytes(path, bytes);

        var column = new SchemaDescriptor(metadata.Schema).Columns[0];
        await using var file = new LocalRandomAccessFile(path);
        await Assert.ThrowsAsync<ParquetFormatException>(async () => await PageMapBuilder.BuildWindowedAsync(
            file, new FileRange(start, meta.TotalCompressedSize), column, meta, false, null, 1, CancellationToken.None));
    }

    private static async Task<int> CompareAsync(string path, long window)
    {
        FileMetaData metadata;
        byte[] bytes;
        try
        {
            (bytes, metadata) = ReadFooter(path);
        }
        catch (Exception ex) when (ex is ParquetFormatException or ArgumentOutOfRangeException or NotSupportedException)
        {
            return 0; // a fixture whose footer EW refuses is another test's business
        }

        SchemaDescriptor schema;
        try
        {
            schema = new SchemaDescriptor(metadata.Schema);
        }
        catch (Exception ex) when (ex is ParquetFormatException or NotSupportedException)
        {
            return 0;
        }

        await using var file = new LocalRandomAccessFile(path);
        int compared = 0;
        foreach (var rowGroup in metadata.RowGroups)
        {
            for (int c = 0; c < rowGroup.Columns.Count && c < schema.Columns.Count; c++)
            {
                var chunk = rowGroup.Columns[c];
                var column = schema.Columns[c];
                if (chunk.MetaData is not { } meta || column.MaxRepetitionLevel > 0 || chunk.FilePath is not null)
                    continue;
                long start = meta.DictionaryPageOffset is > 0 and long d ? Math.Min(d, meta.DataPageOffset) : meta.DataPageOffset;
                if (start < 0 || meta.TotalCompressedSize <= 0 || start + meta.TotalCompressedSize > bytes.Length)
                    continue;

                ColumnPageMap whole;
                try
                {
                    whole = PageMapBuilder.Build(
                        bytes.AsSpan(checked((int)start), checked((int)meta.TotalCompressedSize)), column, meta);
                }
                catch (Exception ex) when (ex is ParquetFormatException or NotSupportedException)
                {
                    continue; // the whole scan is the oracle
                }

                ColumnPageMap windowed;
                try
                {
                    windowed = await PageMapBuilder.BuildWindowedAsync(
                        file, new FileRange(start, meta.TotalCompressedSize), column, meta, false, null, window,
                        CancellationToken.None);
                }
                finally
                {
                    whole.ReleaseDictionary();
                }

                string where = $"{Path.GetFileName(path)} rg column {column.DottedPath}, window {window}";
                Assert.True(whole.Pages.SequenceEqual(windowed.Pages), where);
                Assert.Equal(whole.CumulativeRows, windowed.CumulativeRows);
                Assert.Equal(whole.CumulativeValues, windowed.CumulativeValues);
                Assert.Equal(whole.Dictionary is null, windowed.Dictionary is null);
                Assert.Equal(whole.SymbolTable is null, windowed.SymbolTable is null);
                windowed.ReleaseDictionary();
                compared++;
            }
        }

        return compared;
    }

    /// <summary>One row group of one int64 column (<c>id</c>), 1,000 rows a page, with no page index, so a batched
    /// read scans its headers; dictionary-encoded, so a dictionary page comes first, or plain.</summary>
    private string WriteFixture(int pages, bool dictionary)
    {
        string path = Path.Combine(_tempDir, $"windowed-{pages}-{dictionary}.parquet");
        int rows = pages * 1000;
        var schema = new Apache.Arrow.Schema.Builder().Field(new Field("id", Int64Type.Default, false)).Build();
        var ids = new Int64Array.Builder();
        for (int i = 0; i < rows; i++)
            ids.Append(dictionary ? i % 500 : (long)((ulong)i * 0x9E3779B97F4A7C15UL >> 1));
        using var file = new LocalSequentialFile(path);
        using var writer = new ParquetFileWriter(file, ownsFile: false, ParquetWriteOptions.Default with
        {
            DataPageRowCountLimit = 1000,
            WritePageIndex = false,
            DictionaryEnabled = dictionary,
        });
        using var batch = new RecordBatch(schema, [ids.Build()], rows);
        writer.WriteRowGroupAsync(batch).AsTask().GetAwaiter().GetResult();
        writer.CloseAsync().AsTask().GetAwaiter().GetResult();
        return path;
    }

    private static (byte[] Bytes, FileMetaData Metadata) ReadFooter(string path)
    {
        byte[] bytes = File.ReadAllBytes(path);
        int length = BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(bytes.Length - 8, 4));
        return (bytes, MetadataDecoder.DecodeFileMetaData(bytes.AsSpan(bytes.Length - 8 - length, length)));
    }
}
