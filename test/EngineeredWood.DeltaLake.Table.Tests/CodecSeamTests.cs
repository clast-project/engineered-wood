// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.CompilerServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// The pluggable codec seam: a host supplies the parquet bytes in both directions while the Delta layer keeps
/// the _delta_log protocol. Both hooks are opt-in — default null leaves the built-in codec untouched.
/// </summary>
public class CodecSeamTests : IDisposable
{
    private readonly string _tempDir;

    public CodecSeamTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_seam_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    /// <summary>A writer that records what it was asked to write, then delegates to the built-in codec so the
    /// bytes remain readable — standing in for a host's native parquet writer.</summary>
    private sealed class RecordingWriter(ITableFileSystem fs) : IDataFileWriter
    {
        public List<string> Written { get; } = [];
        public List<int> BatchCounts { get; } = [];

        public async ValueTask<long> WriteAsync(
            IReadOnlyList<RecordBatch> batches, string relativePath, CancellationToken cancellationToken)
        {
            Written.Add(relativePath);
            BatchCounts.Add(batches.Count);

            await using var file = await fs.CreateAsync(relativePath, cancellationToken: cancellationToken);
            await using var writer = new ParquetFileWriter(file, ownsFile: false);
            foreach (var b in batches)
                await writer.WriteRowGroupAsync(b, cancellationToken);
            await writer.DisposeAsync();
            return file.Position;
        }
    }

    /// <summary>A reader that records the files it decoded, delegating to the built-in codec.</summary>
    private sealed class RecordingReader(ITableFileSystem fs) : IDataFileReader
    {
        public List<string> Read { get; } = [];
        public List<IReadOnlyList<string>?> Projections { get; } = [];

        public async IAsyncEnumerable<RecordBatch> ReadAsync(
            string relativePath, IReadOnlyList<string>? physicalColumns,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            Read.Add(relativePath);
            Projections.Add(physicalColumns);

            await using var file = await fs.OpenReadAsync(relativePath, cancellationToken);
            using var reader = new ParquetFileReader(file, ownsFile: false);
            await foreach (var b in reader.ReadAllAsync(
                columnNames: physicalColumns, cancellationToken: cancellationToken))
            {
                yield return b;
            }
        }
    }

    private static Apache.Arrow.Schema Schema() =>
        new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("name", StringType.Default, true))
            .Build();

    private static RecordBatch Rows(Apache.Arrow.Schema schema, params (long Id, string Name)[] rows)
    {
        var ids = new Int64Array.Builder();
        var names = new StringArray.Builder();
        foreach (var (i, n) in rows)
        {
            ids.Append(i);
            names.Append(n);
        }
        return new RecordBatch(schema, [ids.Build(), names.Build()], rows.Length);
    }

    private static async Task<List<(long, string)>> ReadAsync(DeltaTable table)
    {
        var outRows = new List<(long, string)>();
        await foreach (var b in table.ReadAllAsync())
        {
            var ids = (Int64Array)b.Column(b.Schema.GetFieldIndex("id"));
            var names = (StringArray)b.Column(b.Schema.GetFieldIndex("name"));
            for (int i = 0; i < b.Length; i++)
                outRows.Add((ids.GetValue(i)!.Value, names.GetString(i)));
        }
        outRows.Sort();
        return outRows;
    }

    [Fact]
    public async Task Defaults_AreNull_SoTheBuiltInCodecIsUnchanged()
    {
        Assert.Null(DeltaTableOptions.Default.DataFileWriter);
        Assert.Null(DeltaTableOptions.Default.DataFileReader);

        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, Schema());
        await table.WriteAsync([Rows(table.ArrowSchema, (1, "a"))]);
        Assert.Equal([(1L, "a")], await ReadAsync(table));
    }

    [Fact]
    public async Task DataFileWriter_ProducesEveryDataFile()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var writer = new RecordingWriter(fs);
        var options = new DeltaTableOptions { CheckpointInterval = 0, DataFileWriter = writer };

        await using var table = await DeltaTable.CreateAsync(fs, Schema(), options);
        await table.WriteAsync([Rows(table.ArrowSchema, (1, "a"), (2, "b"))]);

        var written = Assert.Single(writer.Written);
        Assert.EndsWith(".parquet", written);
        // The add action points at the file the host wrote.
        var addFile = table.CurrentSnapshot.ActiveFiles.Values.Single();
        Assert.Equal(written, DeltaPath.Decode(addFile.Path));
        Assert.True(addFile.Size > 0);

        Assert.Equal([(1L, "a"), (2L, "b")], await ReadAsync(table));
    }

    [Fact]
    public async Task DataFileReader_DecodesEveryDataFile()
    {
        var fs = new LocalTableFileSystem(_tempDir);

        // Write with the built-in codec, then read back through the seam.
        await using (var seed = await DeltaTable.CreateAsync(
            fs, Schema(), new DeltaTableOptions { CheckpointInterval = 0 }))
        {
            await seed.WriteAsync([Rows(seed.ArrowSchema, (1, "a"), (2, "b"))]);
        }

        var reader = new RecordingReader(fs);
        await using var table = await DeltaTable.OpenAsync(
            fs, new DeltaTableOptions { CheckpointInterval = 0, DataFileReader = reader });

        Assert.Equal([(1L, "a"), (2L, "b")], await ReadAsync(table));
        Assert.Single(reader.Read);
    }

    [Fact]
    public async Task Seam_RoundTripsWriteThenRead()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var writer = new RecordingWriter(fs);
        var reader = new RecordingReader(fs);
        var options = new DeltaTableOptions
        {
            CheckpointInterval = 0,
            DataFileWriter = writer,
            DataFileReader = reader,
        };

        await using var table = await DeltaTable.CreateAsync(fs, Schema(), options);
        await table.WriteAsync([Rows(table.ArrowSchema, (1, "a"))]);
        await table.WriteAsync([Rows(table.ArrowSchema, (2, "b"))]);

        Assert.Equal(2, writer.Written.Count);
        Assert.Equal([(1L, "a"), (2L, "b")], await ReadAsync(table));
        Assert.Equal(2, reader.Read.Count);
    }

    // Deletion vectors are applied ABOVE the decode, so a seam reader that returns raw rows (DV rows
    // included, in file order) still produces correctly filtered results.
    [Fact]
    public async Task Seam_AppliesDeletionVectorsAboveTheDecode()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var reader = new RecordingReader(fs);
        var options = new DeltaTableOptions
        {
            CheckpointInterval = 0,
            DataFileWriter = new RecordingWriter(fs),
            DataFileReader = reader,
        };

        await using var table = await DeltaTable.CreateAsync(
            fs, Schema(), options, enableDeletionVectors: true);
        await table.WriteAsync([Rows(table.ArrowSchema, (1, "a"), (2, "b"), (3, "c"))]);

        await table.DeleteAsync(batch =>
        {
            var ids = (Int64Array)batch.Column(0);
            var mask = new BooleanArray.Builder();
            for (int i = 0; i < batch.Length; i++)
                mask.Append(ids.GetValue(i) == 2);
            return mask.Build();
        });

        Assert.Equal([(1L, "a"), (3L, "c")], await ReadAsync(table));
    }

    // Column mapping is applied above the decode too: the seam reader sees PHYSICAL names and the Delta layer
    // renames back to logical.
    [Theory]
    [InlineData(ColumnMappingMode.Name)]
    [InlineData(ColumnMappingMode.Id)]
    public async Task Seam_HandlesColumnMappingAboveTheDecode(ColumnMappingMode mode)
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var writer = new RecordingWriter(fs);
        var reader = new RecordingReader(fs);
        var options = new DeltaTableOptions
        {
            CheckpointInterval = 0,
            DataFileWriter = writer,
            DataFileReader = reader,
        };

        await using var table = await DeltaTable.CreateAsync(fs, Schema(), options, columnMappingMode: mode);
        await table.WriteAsync([Rows(table.ArrowSchema, (1, "a"))]);

        // Read back under the LOGICAL names even though the file stores physical ones.
        Assert.Equal([(1L, "a")], await ReadAsync(table));

        // A projection reaches the seam as PHYSICAL names (id-mode field-id resolution needs the footer,
        // which the seam hides — spec files carry physicalName in both modes, so this is exact).
        reader.Projections.Clear();
        await foreach (var _ in table.ReadAllAsync(columns: ["id"])) { }
        var projection = Assert.Single(reader.Projections);
        Assert.NotNull(projection);
        Assert.All(projection!, c => Assert.StartsWith("col-", c));
    }

    [Fact]
    public async Task Compaction_UsesBothHooks()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var writer = new RecordingWriter(fs);
        var reader = new RecordingReader(fs);
        var options = new DeltaTableOptions
        {
            CheckpointInterval = 0,
            DataFileWriter = writer,
            DataFileReader = reader,
        };

        await using var table = await DeltaTable.CreateAsync(fs, Schema(), options);
        for (int i = 0; i < 4; i++)
            await table.WriteAsync([Rows(table.ArrowSchema, (i, $"v{i}"))]);

        writer.Written.Clear();
        reader.Read.Clear();

        var compacted = await table.CompactAsync(new CompactionOptions
        {
            MinFileSize = long.MaxValue,
            TargetFileSize = long.MaxValue,
        });
        Assert.NotNull(compacted);

        // OPTIMIZE read the four sources and wrote the compacted file through the host codec.
        Assert.Equal(4, reader.Read.Count);
        Assert.Single(writer.Written);
        Assert.Equal(1, table.CurrentSnapshot.FileCount);

        Assert.Equal([(0L, "v0"), (1L, "v1"), (2L, "v2"), (3L, "v3")], await ReadAsync(table));
    }

    // A host codec discriminates column representations with ARROW:extension:* field metadata; a rewrite that
    // stripped it would silently change the column's type for the host.
    [Fact]
    public void CleanField_PreservesArrowExtensionMarkers_AndDropsParquetInternals()
    {
        var field = new Field("v", BinaryType.Default, true, new Dictionary<string, string>
        {
            ["ARROW:extension:name"] = "host.variant",
            ["ARROW:extension:metadata"] = "{}",
            ["PARQUET:field_id"] = "7",
        });

        var cleaned = Compaction.CompactionExecutor.CleanField(field);

        Assert.Equal("host.variant", cleaned.Metadata!["ARROW:extension:name"]);
        Assert.Equal("{}", cleaned.Metadata!["ARROW:extension:metadata"]);
        Assert.False(cleaned.Metadata!.ContainsKey("PARQUET:field_id"));
    }
}
