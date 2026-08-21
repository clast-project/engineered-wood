// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Compression;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.Tests.Parquet.Data;

/// <summary>
/// <see cref="ParquetWriteOptions.WriteStatistics"/> and its per-column override (issue #158).
/// </summary>
/// <remarks>
/// "Off" means the column chunk carries NO <c>Statistics</c> at all, not merely no bounds — MEASURED,
/// that is what pyarrow's <c>write_statistics=False</c> produces (<c>statistics</c> is null on every
/// column chunk, null count included) and what parquet-cpp's <c>disable_statistics</c> does underneath.
/// The tests assert the whole struct is absent rather than just the bounds, because a weaker assertion
/// would pass against a half-suppressed implementation.
/// </remarks>
public class WriteStatisticsOptionTests : IDisposable
{
    private readonly string _tempDir;

    public WriteStatisticsOptionTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-writestats-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private string TempPath(string name) => Path.Combine(_tempDir, name);

    [Fact]
    public async Task Default_WritesStatistics()
    {
        var stats = await WriteAndReadStatistics(
            TempPath("stats_default.parquet"),
            new ParquetWriteOptions { Compression = CompressionCodec.Uncompressed });

        Assert.All(stats, s =>
        {
            Assert.NotNull(s);
            Assert.NotNull(s!.MinValue);
            Assert.NotNull(s.MaxValue);
        });
    }

    [Fact]
    public async Task WriteStatisticsFalse_LeavesNoStatisticsAtAll()
    {
        var stats = await WriteAndReadStatistics(
            TempPath("stats_off.parquet"),
            new ParquetWriteOptions
            {
                Compression = CompressionCodec.Uncompressed,
                WriteStatistics = false,
            });

        // Not merely MinValue/MaxValue null — the whole struct, null count included.
        Assert.All(stats, Assert.Null);
    }

    // The per-column map overrides in BOTH directions, which is the shape that lets a caller keep bounds
    // on the one column it prunes by and drop them everywhere else.
    [Fact]
    public async Task PerColumn_OverridesTheGlobalInBothDirections()
    {
        var offGlobally = await WriteAndReadStatistics(
            TempPath("stats_percol_on.parquet"),
            new ParquetWriteOptions
            {
                Compression = CompressionCodec.Uncompressed,
                WriteStatistics = false,
                ColumnWriteStatistics = new Dictionary<string, bool> { ["n"] = true },
            });

        Assert.NotNull(offGlobally[0]);   // "n" — re-enabled
        Assert.Null(offGlobally[1]);      // "s" — follows the global

        var onGlobally = await WriteAndReadStatistics(
            TempPath("stats_percol_off.parquet"),
            new ParquetWriteOptions
            {
                Compression = CompressionCodec.Uncompressed,
                ColumnWriteStatistics = new Dictionary<string, bool> { ["n"] = false },
            });

        Assert.Null(onGlobally[0]);       // "n" — suppressed
        Assert.NotNull(onGlobally[1]);    // "s" — follows the global default
    }

    // A dictionary-encoded column takes ComputeFromDictEntries rather than the full scan, so it is a
    // separate arm of the same decision and has to honour the switch too.
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task DictionaryEncodedColumn_HonoursTheSwitch(bool writeStatistics)
    {
        string path = TempPath($"stats_dict_{writeStatistics}.parquet");
        const int rows = 200;

        var builder = new Int32Array.Builder();
        for (int i = 0; i < rows; i++) builder.Append(i % 3);

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("n", Int32Type.Default, nullable: false)).Build();

        await WriteAsync(path, new RecordBatch(schema, [builder.Build()], rows),
            new ParquetWriteOptions
            {
                Compression = CompressionCodec.Uncompressed,
                WriteStatistics = writeStatistics,
            });

        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        var metadata = await reader.ReadMetadataAsync();
        var column = metadata.RowGroups[0].Columns[0];

        Assert.Contains(EngineeredWood.Parquet.Encoding.RleDictionary, column.MetaData!.Encodings);

        if (writeStatistics)
            Assert.NotNull(column.MetaData.Statistics);
        else
            Assert.Null(column.MetaData.Statistics);
    }

    // Turning statistics off must not disturb the values. A reader without bounds simply cannot prune.
    [Fact]
    public async Task WriteStatisticsFalse_RoundTripsTheValues()
    {
        string path = TempPath("stats_off_roundtrip.parquet");
        var batch = MakeBatch();

        await WriteAsync(path, batch, new ParquetWriteOptions
        {
            Compression = CompressionCodec.Uncompressed,
            WriteStatistics = false,
        });

        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        var read = await reader.ReadRowGroupAsync(0);

        var n = Assert.IsType<Int32Array>(read.Column(0));
        var s = Assert.IsType<StringArray>(read.Column(1));
        Assert.Equal([1, 2, 3], Enumerable.Range(0, 3).Select(i => n.GetValue(i)!.Value));
        Assert.Equal(["a", "b", "c"], Enumerable.Range(0, 3).Select(i => s.GetString(i)));
    }

    // BufferedParquetWriter is the other writer, and it reaches ColumnChunkWriter by different entry
    // points — so the switch has to hold there too. Since issue #163 the low-cardinality column carries
    // statistics when the option is ON, so its absence here is now this option's doing and nothing else;
    // BufferedWriterStatisticsTests asserts the ON direction.
    [Fact]
    public async Task BufferedWriter_HonoursTheSwitch()
    {
        string path = TempPath("stats_off_buffered.parquet");
        const int rows = 200;

        var lowCard = new Int32Array.Builder();
        var highCard = new Int32Array.Builder();
        for (int i = 0; i < rows; i++) { lowCard.Append(i % 3); highCard.Append(i); }

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("lowCard", Int32Type.Default, nullable: false))
            .Field(new Field("highCard", Int32Type.Default, nullable: false))
            .Build();
        var batch = new RecordBatch(schema, [lowCard.Build(), highCard.Build()], rows);

        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new BufferedParquetWriter(file, ownsFile: false,
            new ParquetWriteOptions
            {
                Compression = CompressionCodec.Uncompressed,
                WriteStatistics = false,
            }))
        {
            await writer.AppendAsync(batch);
            await writer.CloseAsync();
        }

        await using var readFile = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(readFile, ownsFile: false);
        var metadata = await reader.ReadMetadataAsync();

        Assert.All(metadata.RowGroups[0].Columns, c => Assert.Null(c.MetaData!.Statistics));
    }

    // ParquetSharp is the independent check that the field is genuinely absent from the footer rather
    // than merely unread by us.
    [Fact]
    public async Task WriteStatisticsFalse_PSSeesNoStatistics()
    {
        string path = TempPath("stats_off_ps.parquet");

        await WriteAsync(path, MakeBatch(), new ParquetWriteOptions
        {
            Compression = CompressionCodec.Uncompressed,
            WriteStatistics = false,
        });

        using var reader = new ParquetSharp.ParquetFileReader(path);
        using var rg = reader.RowGroup(0);

        for (int c = 0; c < 2; c++)
        {
            var meta = rg.MetaData.GetColumnChunkMetaData(c);
            Assert.False(meta.IsStatsSet);
        }
    }

    private static RecordBatch MakeBatch()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("n", Int32Type.Default, nullable: false))
            .Field(new Field("s", StringType.Default, nullable: false))
            .Build();

        return new RecordBatch(
            schema,
            [
                new Int32Array.Builder().AppendRange([1, 2, 3]).Build(),
                new StringArray.Builder().Append("a").Append("b").Append("c").Build(),
            ],
            3);
    }

    /// <summary>The per-column statistics of a two-column file written with these options.</summary>
    private static async Task<EngineeredWood.Parquet.Metadata.Statistics?[]> WriteAndReadStatistics(
        string path, ParquetWriteOptions options)
    {
        await WriteAsync(path, MakeBatch(), options);

        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        var metadata = await reader.ReadMetadataAsync();

        return metadata.RowGroups[0].Columns.Select(c => c.MetaData!.Statistics).ToArray();
    }

    private static async Task WriteAsync(string path, RecordBatch batch, ParquetWriteOptions options)
    {
        await using var file = new LocalSequentialFile(path);
        await using var writer = new ParquetFileWriter(file, ownsFile: false, options);
        await writer.WriteRowGroupAsync(batch);
        await writer.CloseAsync();
    }
}
