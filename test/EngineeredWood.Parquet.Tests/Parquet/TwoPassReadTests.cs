// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.Tests.Parquet;

/// <summary>
/// The two-pass selected read (#502 phase 1b): the output columns no predicate reads are decoded only over the
/// batches where a row was selected. <see cref="SelectedReadTests"/> checks the rows it returns; this checks
/// that the skipped batches' pages really are never decoded.
/// </summary>
/// <remarks>
/// The probe is a page checksum. One page of the output column <c>note</c>, far from any selected row, has a
/// byte flipped, and the read validates checksums when it decodes a page. A one-pass read decodes that page
/// and fails; a two-pass read never decodes it and succeeds. A two-pass read whose selection DOES reach that
/// page fails too, which shows the corruption is caught when the page is decoded at all.
/// </remarks>
public class TwoPassReadTests : IDisposable
{
    private const int Rows = 40_000;
    private readonly string _tempDir;

    public TwoPassReadTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-twopass-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        try { Directory.Delete(_tempDir, recursive: true); } catch (IOException) { }
    }

    [Fact]
    public async Task ABatchWithNoSelectedRowIsNeverDecoded()
    {
        var (path, corruptFrom) = await WriteCorruptedAsync();
        Assert.True(corruptFrom >= 3 * ParquetFileReader.TwoPassBlockRows, "the corrupted page must lie past the selected rows' batches");

        // Selects rows in the first batch only; the corrupted page is batches away.
        var scan = Scan(maxId: 1_000);
        var rows = await ReadIdsAsync(path, scan, twoPass: true);
        Assert.Equal(Enumerable.Range(0, 1_000).Select(i => (long)i), rows);
    }

    /// <summary>
    /// The same with pruning by a predicate the page index cannot narrow: pruning then hands back the whole row
    /// group as one range rather than none, and the read must still go in two passes.
    /// </summary>
    [Fact]
    public async Task ABatchWithNoSelectedRowIsNeverDecodedWhenPruningNarrowsNothing()
    {
        var (path, _) = await WriteCorruptedAsync();

        // Every row satisfies id >= 0, so the page index keeps every page.
        var scan = Scan(maxId: 1_000) with
        {
            Prune = EngineeredWood.Expressions.Expressions.GreaterThanOrEqual("id", EngineeredWood.Expressions.LiteralValue.Of(0L)),
        };
        var rows = await ReadIdsAsync(path, scan, twoPass: true);
        Assert.Equal(Enumerable.Range(0, 1_000).Select(i => (long)i), rows);
    }

    [Fact]
    public async Task AOnePassReadDecodesTheSamePageAndFails()
    {
        var (path, _) = await WriteCorruptedAsync();
        var error = await Record.ExceptionAsync(() => ReadIdsAsync(path, Scan(maxId: 1_000), twoPass: false));
        Assert.NotNull(error);
    }

    [Fact]
    public async Task ATwoPassReadWhoseSelectionReachesThePageFails()
    {
        var (path, corruptFrom) = await WriteCorruptedAsync();
        var error = await Record.ExceptionAsync(() => ReadIdsAsync(path, Scan(maxId: corruptFrom + 1), twoPass: true));
        Assert.NotNull(error);
    }

    // Rows whose id is below maxId, read through an opaque predicate (no pruning narrows the row group, so the
    // read is eligible for two passes), returning id and note.
    private static ParquetScan Scan(long maxId) => new()
    {
        Columns = ["id", "note"],
        RowFilter = new RowFilter(new RowPredicate(["id"], batch =>
        {
            var ids = (Int64Array)batch.Column(0);
            var builder = new BooleanArray.Builder().Reserve(ids.Length);
            for (int i = 0; i < ids.Length; i++)
                builder.Append(ids.GetValue(i) < maxId);
            return builder.Build();
        })),
    };

    private static async Task<List<long>> ReadIdsAsync(string path, ParquetScan scan, bool twoPass)
    {
        await using var file = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(file, ownsFile: false, new ParquetReadOptions { PageChecksumValidation = true })
        {
            TwoPassReads = twoPass,
        };
        var ids = new List<long>();
        await foreach (var selected in reader.ReadSelectedAsync(scan))
        {
            using (selected)
            {
                var column = (Int64Array)selected.Batch.Column(0);
                var notes = (StringArray)selected.Batch.Column(1);
                for (int i = 0; i < column.Length; i++)
                {
                    ids.Add(column.GetValue(i)!.Value);
                    Assert.Equal(NoteFor(column.GetValue(i)!.Value), notes.GetString(i));
                }
            }
        }

        return ids;
    }

    private static string NoteFor(long id) => "note-" + id.ToString("D12", System.Globalization.CultureInfo.InvariantCulture);

    /// <summary>
    /// One row group of 40,000 rows in 2,000-row pages, with page checksums, and one byte flipped at the end of the
    /// <c>note</c> page holding row 30,000. Returns the first row of that page.
    /// </summary>
    private async Task<(string Path, long CorruptFrom)> WriteCorruptedAsync()
    {
        string path = Path.Combine(_tempDir, "data.parquet");
        var ids = new Int64Array.Builder();
        var notes = new StringArray.Builder();
        for (long i = 0; i < Rows; i++)
        {
            ids.Append(i);
            notes.Append(NoteFor(i));
        }

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("note", StringType.Default, false))
            .Build();
        using (var batch = new RecordBatch(schema, [ids.Build(), notes.Build()], Rows))
        {
            await using var output = new LocalSequentialFile(path);
            await using var writer = new ParquetFileWriter(output, ownsFile: false, ParquetWriteOptions.Default with
            {
                RowGroupMaxRows = Rows,
                DataPageRowCountLimit = 2_000,
                PageChecksumEnabled = true,
            });
            await writer.WriteRowGroupAsync(batch);
            await writer.CloseAsync();
        }

        long offset, firstRow;
        await using (var file = new LocalRandomAccessFile(path))
        using (var reader = new ParquetFileReader(file, ownsFile: false))
        {
            var index = (await reader.ReadPageIndexAsync(0, ["note"])).Single().OffsetIndex!;
            var page = index.PageLocations.Last(p => p.FirstRowIndex <= 30_000);
            offset = page.Offset + page.CompressedPageSize - 1;
            firstRow = page.FirstRowIndex;
        }

        var bytes = File.ReadAllBytes(path);
        bytes[offset] ^= 0xFF;
        File.WriteAllBytes(path, bytes);
        return (path, firstRow);
    }
}
