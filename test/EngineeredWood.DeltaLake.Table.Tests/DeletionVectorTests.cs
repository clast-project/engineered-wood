// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.DeletionVectors;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.DeltaLake.Table.Tests;

public class DeletionVectorTests : IDisposable
{
    private readonly string _tempDir;

    public DeletionVectorTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_dv_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    /// <summary>
    /// Builds a RoaringBitmapArray DV blob for the given deleted row indices.
    /// All values must be < 65536 (single container).
    /// </summary>
    private static byte[] BuildDvBlob(params ushort[] deletedRows)
    {
        // Serialize through the production writer so the blob is the spec RoaringBitmapArray form.
        return RoaringBitmapWriter.Serialize(deletedRows.Select(v => (long)v));
    }

    [Fact]
    public async Task ReadWithDeletionVector_FiltersDeletedRows()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("value", StringType.Default, true))
            .Build();

        // Step 1: Create the table
        await using var table = await DeltaTable.CreateAsync(fs, schema);

        // Step 2: Write initial data (5 rows)
        var ids = new Int64Array.Builder()
            .Append(1).Append(2).Append(3).Append(4).Append(5).Build();
        var values = new StringArray.Builder()
            .Append("a").Append("b").Append("c").Append("d").Append("e").Build();
        var batch = new RecordBatch(schema, [ids, values], 5);
        await table.WriteAsync([batch]);

        // Get the file path from the add action
        var addFile = table.CurrentSnapshot.ActiveFiles.Values.First();
        string dataFilePath = addFile.Path;

        // Step 3: Build an inline deletion vector marking rows 1 and 3 as deleted
        byte[] dvBlob = BuildDvBlob(1, 3); // Delete rows at index 1 ("b") and 3 ("d")
        string dvEncoded = Base85.Encode(PadToMultipleOf4(dvBlob));

        // Step 4: Write a new commit that replaces the add action with one that has a DV
        var log = new TransactionLog(fs);
        var newActions = new List<DeltaAction>
        {
            // Remove the old add (without DV)
            new RemoveFile
            {
                Path = dataFilePath,
                DataChange = false,
                DeletionTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            },
            // Add the same file with a DV
            new AddFile
            {
                Path = dataFilePath,
                PartitionValues = addFile.PartitionValues,
                Size = addFile.Size,
                ModificationTime = addFile.ModificationTime,
                DataChange = false,
                Stats = addFile.Stats,
                DeletionVector = new DeletionVector
                {
                    StorageType = "i",
                    PathOrInlineDv = dvEncoded,
                    SizeInBytes = dvBlob.Length,
                    Cardinality = 2,
                },
            },
        };
        await log.WriteCommitAsync(2, newActions);

        // Step 5: Refresh and read
        await table.RefreshAsync();

        var readBatches = new List<RecordBatch>();
        await foreach (var b in table.ReadAllAsync())
            readBatches.Add(b);

        // Should have 3 rows (5 - 2 deleted)
        int totalRows = readBatches.Sum(b => b.Length);
        Assert.Equal(3, totalRows);

        // Collect all read values
        var readIds = new List<long>();
        var readValues = new List<string>();
        foreach (var b in readBatches)
        {
            var idCol = (Int64Array)b.Column(0);
            var valCol = (StringArray)b.Column(1);
            for (int i = 0; i < b.Length; i++)
            {
                readIds.Add(idCol.GetValue(i)!.Value);
                readValues.Add(valCol.GetString(i));
            }
        }

        // Rows 1 and 3 (values "b" and "d") should be gone
        Assert.Equal(new long[] { 1, 3, 5 }, readIds.ToArray());
        Assert.Equal(new[] { "a", "c", "e" }, readValues.ToArray());
    }

    [Fact]
    public async Task ReadWithFileDeletionVector_FiltersDeletedRows()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        await using var table = await DeltaTable.CreateAsync(fs, schema);

        // Write 4 rows
        var ids = new Int64Array.Builder()
            .Append(10).Append(20).Append(30).Append(40).Build();
        var batch = new RecordBatch(schema, [ids], 4);
        await table.WriteAsync([batch]);

        var addFile = table.CurrentSnapshot.ActiveFiles.Values.First();

        // Write a file-based DV (absolute path)
        byte[] dvBlob = BuildDvBlob(0, 2); // Delete rows 0 ("10") and 2 ("30")
        string dvFilePath = "_delta_log/dv_test.bin";
        await fs.WriteAllBytesAsync(dvFilePath, dvBlob);

        // Commit with DV
        var log = new TransactionLog(fs);
        await log.WriteCommitAsync(2, new List<DeltaAction>
        {
            new RemoveFile
            {
                Path = addFile.Path,
                DataChange = false,
                DeletionTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            },
            new AddFile
            {
                Path = addFile.Path,
                PartitionValues = addFile.PartitionValues,
                Size = addFile.Size,
                ModificationTime = addFile.ModificationTime,
                DataChange = false,
                DeletionVector = new DeletionVector
                {
                    StorageType = "p",
                    PathOrInlineDv = dvFilePath,
                    Offset = 0,
                    SizeInBytes = dvBlob.Length,
                    Cardinality = 2,
                },
            },
        });

        await table.RefreshAsync();

        var readIds = new List<long>();
        await foreach (var b in table.ReadAllAsync())
        {
            var idCol = (Int64Array)b.Column(0);
            for (int i = 0; i < b.Length; i++)
                readIds.Add(idCol.GetValue(i)!.Value);
        }

        Assert.Equal(new long[] { 20, 40 }, readIds.ToArray());
    }

    [Fact]
    public async Task ReadWithoutDeletionVector_ReturnsAllRows()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        await using var table = await DeltaTable.CreateAsync(fs, schema);

        var ids = new Int64Array.Builder().Append(1).Append(2).Append(3).Build();
        var batch = new RecordBatch(schema, [ids], 3);
        await table.WriteAsync([batch]);

        int totalRows = 0;
        await foreach (var b in table.ReadAllAsync())
            totalRows += b.Length;

        Assert.Equal(3, totalRows);
    }

    [Fact]
    public async Task ProtocolV3_WithDeletionVectorsFeature_Succeeds()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);

        // Manually create a v3/v7 table with deletionVectors feature
        await log.WriteCommitAsync(0, new List<DeltaAction>
        {
            new ProtocolAction
            {
                MinReaderVersion = 3,
                MinWriterVersion = 7,
                ReaderFeatures = ["deletionVectors"],
                WriterFeatures = ["deletionVectors"],
            },
            new MetadataAction
            {
                Id = "dv-table",
                Format = Format.Parquet,
                SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}}]}""",
                PartitionColumns = [],
            },
        });

        // Should be able to open without throwing
        await using var table = await DeltaTable.OpenAsync(fs);
        Assert.Equal(3, table.CurrentSnapshot.Protocol.MinReaderVersion);
    }

    [Fact]
    public void ProtocolV3_WithUnsupportedFeature_Throws()
    {
        var protocol = new ProtocolAction
        {
            MinReaderVersion = 3,
            MinWriterVersion = 7,
            ReaderFeatures = ["deletionVectors", "liquidClustering"],
        };

        var ex = Assert.Throws<DeltaFormatException>(
            () => ProtocolVersions.ValidateReadSupport(protocol));
        Assert.Contains("liquidClustering", ex.Message);
    }

    /// <summary>
    /// Z85 requires input length to be a multiple of 4.
    /// Pad with zero bytes if needed.
    /// </summary>
    private static byte[] PadToMultipleOf4(byte[] data)
    {
        int remainder = data.Length % 4;
        if (remainder == 0) return data;

        int padded = data.Length + (4 - remainder);
        var result = new byte[padded];
        System.Array.Copy(data, result, data.Length);
        return result;
    }
}
