// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

public class TimestampNtzTests : IDisposable
{
    private readonly string _tempDir;

    public TimestampNtzTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_tsntz_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    [Fact]
    public async Task WriteAndRead_TimestampNtz_RoundTrips()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var tsNtzType = new TimestampType(TimeUnit.Microsecond, (string?)null);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("created_at", tsNtzType, true))
            .Build();

        await using var table = await DeltaTable.CreateAsync(fs, schema);

        // Write timestamps without timezone
        var ids = new Int64Array.Builder().Append(1).Append(2).Append(3).Build();
        var timestamps = new TimestampArray.Builder(tsNtzType)
            .Append(new DateTimeOffset(2024, 1, 15, 10, 30, 0, TimeSpan.Zero))
            .Append(new DateTimeOffset(2024, 6, 20, 14, 45, 30, TimeSpan.Zero))
            .AppendNull()
            .Build();
        var batch = new RecordBatch(schema, [ids, timestamps], 3);

        await table.WriteAsync([batch]);

        // Read back
        var readBatches = new List<RecordBatch>();
        await foreach (var b in table.ReadAllAsync())
            readBatches.Add(b);

        Assert.Single(readBatches);
        Assert.Equal(3, readBatches[0].Length);
        Assert.Equal(2, readBatches[0].ColumnCount);

        var readTs = (TimestampArray)readBatches[0].Column(1);
        Assert.False(readTs.IsNull(0));
        Assert.False(readTs.IsNull(1));
        Assert.True(readTs.IsNull(2));
    }

    [Fact]
    public async Task WriteAndRead_TimestampWithTz_RoundTrips()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var tsType = new TimestampType(TimeUnit.Microsecond, (string?)"UTC");
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("event_time", tsType, false))
            .Build();

        await using var table = await DeltaTable.CreateAsync(fs, schema);

        var ids = new Int64Array.Builder().Append(1).Append(2).Build();
        var timestamps = new TimestampArray.Builder(tsType)
            .Append(new DateTimeOffset(2024, 3, 15, 12, 0, 0, TimeSpan.Zero))
            .Append(new DateTimeOffset(2024, 9, 1, 18, 30, 0, TimeSpan.Zero))
            .Build();
        var batch = new RecordBatch(schema, [ids, timestamps], 2);

        await table.WriteAsync([batch]);

        var readBatches = new List<RecordBatch>();
        await foreach (var b in table.ReadAllAsync())
            readBatches.Add(b);

        Assert.Single(readBatches);
        Assert.Equal(2, readBatches[0].Length);
    }

    [Fact]
    public async Task Stats_TimestampNtz_CollectsMinMax()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var tsNtzType = new TimestampType(TimeUnit.Microsecond, (string?)null);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("ts", tsNtzType, false))
            .Build();

        await using var table = await DeltaTable.CreateAsync(fs, schema);

        var timestamps = new TimestampArray.Builder(tsNtzType)
            .Append(new DateTimeOffset(2024, 6, 15, 0, 0, 0, TimeSpan.Zero))
            .Append(new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero))
            .Append(new DateTimeOffset(2024, 12, 31, 23, 59, 59, TimeSpan.Zero))
            .Build();
        var batch = new RecordBatch(schema, [timestamps], 3);

        await table.WriteAsync([batch]);

        // Check that stats were collected
        var addFile = table.CurrentSnapshot.ActiveFiles.Values.First();
        Assert.NotNull(addFile.Stats);

        var stats = JsonDocument.Parse(addFile.Stats!);
        Assert.Equal(3, stats.RootElement.GetProperty("numRecords").GetInt64());

        // Min/max should be present as strings
        Assert.True(stats.RootElement.GetProperty("minValues").TryGetProperty("ts", out _));
        Assert.True(stats.RootElement.GetProperty("maxValues").TryGetProperty("ts", out _));

        string minStr = stats.RootElement.GetProperty("minValues").GetProperty("ts").GetString()!;
        string maxStr = stats.RootElement.GetProperty("maxValues").GetProperty("ts").GetString()!;

        Assert.Contains("2024-01-01", minStr);
        Assert.Contains("2024-12-31", maxStr);
    }

    [Fact]
    public async Task Schema_TimestampNtz_CorrectDeltaType()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var tsNtzType = new TimestampType(TimeUnit.Microsecond, (string?)null);
        var tsType = new TimestampType(TimeUnit.Microsecond, (string?)"UTC");
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("ts_ntz", tsNtzType, true))
            .Field(new Field("ts_utc", tsType, true))
            .Build();

        await using var table = await DeltaTable.CreateAsync(fs, schema);

        // Verify Delta schema has the right type names
        var deltaSchema = table.CurrentSnapshot.Schema;
        Assert.Equal("timestamp_ntz",
            ((EngineeredWood.DeltaLake.Schema.PrimitiveType)deltaSchema.Fields[0].Type).TypeName);
        Assert.Equal("timestamp",
            ((EngineeredWood.DeltaLake.Schema.PrimitiveType)deltaSchema.Fields[1].Type).TypeName);
    }

    [Fact]
    public async Task Protocol_TimestampNtzFeature_Accepted()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);

        // Create table with timestampNtz feature
        await log.WriteCommitAsync(0, new List<DeltaAction>
        {
            new ProtocolAction
            {
                MinReaderVersion = 3,
                MinWriterVersion = 7,
                ReaderFeatures = ["timestampNtz"],
                WriterFeatures = ["timestampNtz"],
            },
            new MetadataAction
            {
                Id = "ts-ntz-table",
                Format = Format.Parquet,
                SchemaString = """{"type":"struct","fields":[{"name":"ts","type":"timestamp_ntz","nullable":true,"metadata":{}}]}""",
                PartitionColumns = [],
            },
        });

        // Should open without error
        await using var table = await DeltaTable.OpenAsync(fs);
        Assert.Equal(3, table.CurrentSnapshot.Protocol.MinReaderVersion);
    }
}
