// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Metadata;

namespace EngineeredWood.Tests.Parquet.Data;

/// <summary>
/// <see cref="ParquetWriteOptions.ColumnDictionaryEnabled"/> lets a producer that already knows a column's
/// cardinality skip the analysis pass for it. Deciding a column is unsuitable otherwise costs a full pass
/// that ends in nothing — the encoder hashes to a fifth of the row count in distinct values before giving
/// up, and the column is then read again and written PLAIN.
///
/// <para>The switch is per column, in BOTH directions, because the file-wide flag is the only alternative
/// today: a table with one high-cardinality id column beside twenty low-cardinality ones had to choose
/// between paying the futile attempt on the id or losing dictionary encoding everywhere.</para>
/// </summary>
public class PerColumnDictionaryTests : IDisposable
{
    private readonly string _tempDir;
    private int _fileCounter;

    public PerColumnDictionaryTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-coldict-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    // ── Resolution, independent of any writing ──

    private static readonly string[] Path1 = ["c"];
    private static readonly string[] Nested = ["s", "f"];

    [Fact]
    public void GetDictionaryEnabled_UnlistedColumn_FollowsTheFileWideFlag()
    {
        Assert.True(new ParquetWriteOptions().GetDictionaryEnabled(Path1));
        Assert.False(new ParquetWriteOptions { DictionaryEnabled = false }.GetDictionaryEnabled(Path1));
    }

    [Fact]
    public void GetDictionaryEnabled_ListedColumn_OverridesInBothDirections()
    {
        var off = new ParquetWriteOptions
        {
            ColumnDictionaryEnabled = new Dictionary<string, bool> { ["c"] = false },
        };
        Assert.False(off.GetDictionaryEnabled(Path1));

        // The direction a list of exclusions could not express: on, where the file-wide flag is off.
        var on = new ParquetWriteOptions
        {
            DictionaryEnabled = false,
            ColumnDictionaryEnabled = new Dictionary<string, bool> { ["c"] = true },
        };
        Assert.True(on.GetDictionaryEnabled(Path1));
    }

    [Fact]
    public void GetDictionaryEnabled_KeysOnTheDottedPath_LikeTheOtherPerColumnOverrides()
    {
        var options = new ParquetWriteOptions
        {
            ColumnDictionaryEnabled = new Dictionary<string, bool> { ["s.f"] = false },
        };

        Assert.False(options.GetDictionaryEnabled(Nested));
        Assert.True(options.GetDictionaryEnabled(["f"]));
    }

    // ── What it does to a file ──

    private static StringArray LowCardinality(int rows)
    {
        var values = new[] { "alpha", "beta", "gamma", "delta" };
        var b = new StringArray.Builder();
        for (int i = 0; i < rows; i++) b.Append(values[i % values.Length]);
        return b.Build();
    }

    private static StringArray Distinct(int rows)
    {
        var b = new StringArray.Builder();
        for (int i = 0; i < rows; i++) b.Append($"id-{i:D7}");
        return b.Build();
    }

    private async Task<string> WriteAsync(RecordBatch batch, ParquetWriteOptions options)
    {
        string path = Path.Combine(_tempDir, $"f{_fileCounter++}.parquet");
        await using var file = new LocalSequentialFile(path);
        await using var writer = new ParquetFileWriter(file, ownsFile: false, options);
        await writer.WriteRowGroupAsync(batch);
        await writer.CloseAsync();
        return path;
    }

    private static async Task<ColumnMetaData> ColumnMetaAsync(string path, int column)
    {
        await using var file = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(file, ownsFile: false);
        var metadata = await reader.ReadMetadataAsync();
        return metadata.RowGroups[0].Columns[column].MetaData;
    }

    private static RecordBatch TwoColumns(int rows)
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("kind", StringType.Default, false))
            .Field(new Field("id", StringType.Default, false))
            .Build();

        return new RecordBatch(schema, [LowCardinality(rows), Distinct(rows)], rows);
    }

    /// <summary>A dictionary-encoded chunk is the one with a dictionary page.</summary>
    private static bool IsDictionaryEncoded(ColumnMetaData meta) =>
        meta.DictionaryPageOffset is not null;

    [Fact]
    public async Task DisablingOneColumn_LeavesTheOthersDictionaryEncoded()
    {
        var batch = TwoColumns(2_000);
        var options = new ParquetWriteOptions
        {
            ColumnDictionaryEnabled = new Dictionary<string, bool> { ["id"] = false },
        };

        string path = await WriteAsync(batch, options);

        Assert.True(IsDictionaryEncoded(await ColumnMetaAsync(path, 0)));
        Assert.False(IsDictionaryEncoded(await ColumnMetaAsync(path, 1)));
    }

    [Fact]
    public async Task EnablingOneColumn_SurvivesTheFileWideFlagBeingOff()
    {
        var batch = TwoColumns(2_000);
        var options = new ParquetWriteOptions
        {
            DictionaryEnabled = false,
            ColumnDictionaryEnabled = new Dictionary<string, bool> { ["kind"] = true },
        };

        string path = await WriteAsync(batch, options);

        Assert.True(IsDictionaryEncoded(await ColumnMetaAsync(path, 0)));
        Assert.False(IsDictionaryEncoded(await ColumnMetaAsync(path, 1)));
    }

    [Fact]
    public async Task ADisabledColumn_WritesTheSameBytesAsAFileWithNoDictionaryAtAll()
    {
        // The switch decides whether the column is ANALYZED, not how it is written once the dictionary
        // has been declined. A column turned off must land byte for byte where the same column lands
        // when the whole file has dictionaries off.
        var batch = TwoColumns(2_000);

        string perColumn = await WriteAsync(batch, new ParquetWriteOptions
        {
            ColumnDictionaryEnabled = new Dictionary<string, bool> { ["kind"] = false, ["id"] = false },
        });
        string fileWide = await WriteAsync(batch, new ParquetWriteOptions { DictionaryEnabled = false });

        Assert.Equal(File.ReadAllBytes(fileWide), File.ReadAllBytes(perColumn));
    }

    [Fact]
    public async Task ADisabledColumn_StillReadsBackItsValues()
    {
        var batch = TwoColumns(2_000);
        string path = await WriteAsync(batch, new ParquetWriteOptions
        {
            ColumnDictionaryEnabled = new Dictionary<string, bool> { ["id"] = false },
        });

        await using var file = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(file, ownsFile: false);
        var read = await reader.ReadRowGroupAsync(0);

        var kind = (StringArray)read.Column(0);
        var id = (StringArray)read.Column(1);

        Assert.Equal(2_000, id.Length);
        Assert.Equal("alpha", kind.GetString(0));
        Assert.Equal("id-0000000", id.GetString(0));
        Assert.Equal("id-0001999", id.GetString(1_999));
    }

    [Fact]
    public async Task ADisabledColumn_StillGetsStatistics()
    {
        // Statistics come from the dictionary entries when there is a dictionary and from a full scan
        // otherwise. Turning the dictionary off moves that decision; it must not lose the statistics.
        var batch = TwoColumns(2_000);
        string path = await WriteAsync(batch, new ParquetWriteOptions
        {
            ColumnDictionaryEnabled = new Dictionary<string, bool> { ["id"] = false },
        });

        var stats = (await ColumnMetaAsync(path, 1)).Statistics;

        Assert.NotNull(stats);
        Assert.Equal(0, stats.NullCount);
        Assert.Equal("id-0000000", System.Text.Encoding.UTF8.GetString(stats.MinValue!));
        Assert.Equal("id-0001999", System.Text.Encoding.UTF8.GetString(stats.MaxValue!));
    }

    [Fact]
    public async Task TheBufferedWriter_HonoursTheSameSwitch()
    {
        // The buffered writer decides dictionary encoding for itself at flush time. An option that held
        // for one writer and not the other would be worse than not having it.
        var batch = TwoColumns(2_000);
        string path = Path.Combine(_tempDir, $"buffered{_fileCounter++}.parquet");

        await using (var file = new LocalSequentialFile(path))
        {
            await using var writer = new BufferedParquetWriter(
                file, ownsFile: false,
                options: new ParquetWriteOptions
                {
                    ColumnDictionaryEnabled = new Dictionary<string, bool> { ["kind"] = false },
                });

            await writer.AppendAsync(batch);
            await writer.CloseAsync();
        }

        // "kind" is low-cardinality and would otherwise be dictionary-encoded; the switch is the only
        // reason it is not.
        Assert.False(IsDictionaryEncoded(await ColumnMetaAsync(path, 0)));
    }
}
