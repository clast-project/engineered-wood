// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Log;

namespace EngineeredWood.DeltaLake.Tests;

public class ActionSerializerTests
{
    [Fact]
    public void RoundTrip_AddFile()
    {
        var original = new AddFile
        {
            Path = "part-00000-abc.parquet",
            PartitionValues = new Dictionary<string, string> { { "date", "2024-01-01" } },
            Size = 12345,
            ModificationTime = 1700000000000,
            DataChange = true,
            Stats = """{"numRecords":100}""",
        };

        var actions = new List<DeltaAction> { original };
        byte[] serialized = ActionSerializer.Serialize(actions);
        var deserialized = ActionSerializer.Deserialize(serialized);

        var result = Assert.Single(deserialized);
        var add = Assert.IsType<AddFile>(result);
        Assert.Equal(original.Path, add.Path);
        Assert.Equal("2024-01-01", add.PartitionValues["date"]);
        Assert.Equal(12345L, add.Size);
        Assert.Equal(1700000000000L, add.ModificationTime);
        Assert.True(add.DataChange);
        Assert.Equal("""{"numRecords":100}""", add.Stats);
    }

    [Fact]
    public void RoundTrip_RemoveFile()
    {
        var original = new RemoveFile
        {
            Path = "part-00000-abc.parquet",
            DeletionTimestamp = 1700000000000,
            DataChange = true,
            ExtendedFileMetadata = true,
            PartitionValues = new Dictionary<string, string> { { "date", "2024-01-01" } },
            Size = 12345,
        };

        var actions = new List<DeltaAction> { original };
        byte[] serialized = ActionSerializer.Serialize(actions);
        var deserialized = ActionSerializer.Deserialize(serialized);

        var result = Assert.Single(deserialized);
        var remove = Assert.IsType<RemoveFile>(result);
        Assert.Equal(original.Path, remove.Path);
        Assert.Equal(1700000000000L, remove.DeletionTimestamp);
        Assert.True(remove.DataChange);
        Assert.True(remove.ExtendedFileMetadata);
        Assert.Equal(12345L, remove.Size);
    }

    [Fact]
    public void RoundTrip_MetadataAction()
    {
        var original = new MetadataAction
        {
            Id = "test-id-123",
            Name = "test_table",
            Description = "A test table",
            Format = Format.Parquet,
            SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}}]}""",
            PartitionColumns = ["date"],
            Configuration = new Dictionary<string, string>
            {
                { "delta.appendOnly", "true" },
            },
            CreatedTime = 1700000000000,
        };

        var actions = new List<DeltaAction> { original };
        byte[] serialized = ActionSerializer.Serialize(actions);
        var deserialized = ActionSerializer.Deserialize(serialized);

        var result = Assert.Single(deserialized);
        var metadata = Assert.IsType<MetadataAction>(result);
        Assert.Equal("test-id-123", metadata.Id);
        Assert.Equal("test_table", metadata.Name);
        Assert.Equal("A test table", metadata.Description);
        Assert.Equal("parquet", metadata.Format.Provider);
        Assert.Contains("id", metadata.SchemaString);
        Assert.Single(metadata.PartitionColumns);
        Assert.Equal("date", metadata.PartitionColumns[0]);
        Assert.Equal("true", metadata.Configuration!["delta.appendOnly"]);
        Assert.Equal(1700000000000L, metadata.CreatedTime);
    }

    [Fact]
    public void RoundTrip_ProtocolAction()
    {
        var original = new ProtocolAction
        {
            MinReaderVersion = 1,
            MinWriterVersion = 2,
        };

        var actions = new List<DeltaAction> { original };
        byte[] serialized = ActionSerializer.Serialize(actions);
        var deserialized = ActionSerializer.Deserialize(serialized);

        var result = Assert.Single(deserialized);
        var protocol = Assert.IsType<ProtocolAction>(result);
        Assert.Equal(1, protocol.MinReaderVersion);
        Assert.Equal(2, protocol.MinWriterVersion);
        Assert.Null(protocol.ReaderFeatures);
        Assert.Null(protocol.WriterFeatures);
    }

    [Fact]
    public void RoundTrip_ProtocolAction_WithFeatures()
    {
        var original = new ProtocolAction
        {
            MinReaderVersion = 3,
            MinWriterVersion = 7,
            ReaderFeatures = ["columnMapping", "deletionVectors"],
            WriterFeatures = ["columnMapping", "deletionVectors", "changeDataFeed"],
        };

        var actions = new List<DeltaAction> { original };
        byte[] serialized = ActionSerializer.Serialize(actions);
        var deserialized = ActionSerializer.Deserialize(serialized);

        var result = Assert.Single(deserialized);
        var protocol = Assert.IsType<ProtocolAction>(result);
        Assert.Equal(3, protocol.MinReaderVersion);
        Assert.Equal(7, protocol.MinWriterVersion);
        Assert.Equal(2, protocol.ReaderFeatures!.Count);
        Assert.Equal(3, protocol.WriterFeatures!.Count);
    }

    [Fact]
    public void RoundTrip_TransactionId()
    {
        var original = new TransactionId
        {
            AppId = "my-streaming-app",
            Version = 42,
            LastUpdated = 1700000000000,
        };

        var actions = new List<DeltaAction> { original };
        byte[] serialized = ActionSerializer.Serialize(actions);
        var deserialized = ActionSerializer.Deserialize(serialized);

        var result = Assert.Single(deserialized);
        var txn = Assert.IsType<TransactionId>(result);
        Assert.Equal("my-streaming-app", txn.AppId);
        Assert.Equal(42L, txn.Version);
        Assert.Equal(1700000000000L, txn.LastUpdated);
    }

    [Fact]
    public void RoundTrip_CommitInfo()
    {
        var original = new CommitInfo
        {
            Values = new Dictionary<string, System.Text.Json.JsonElement>
            {
                { "operation", System.Text.Json.JsonDocument.Parse("\"WRITE\"").RootElement.Clone() },
                { "timestamp", System.Text.Json.JsonDocument.Parse("1700000000000").RootElement.Clone() },
            },
        };

        var actions = new List<DeltaAction> { original };
        byte[] serialized = ActionSerializer.Serialize(actions);
        var deserialized = ActionSerializer.Deserialize(serialized);

        var result = Assert.Single(deserialized);
        var commitInfo = Assert.IsType<CommitInfo>(result);
        Assert.Equal("WRITE", commitInfo.GetValue("operation")!.Value.GetString());
        Assert.Equal(1700000000000L, commitInfo.GetValue("timestamp")!.Value.GetInt64());
    }

    [Fact]
    public void RoundTrip_DomainMetadata()
    {
        var original = new DomainMetadata
        {
            Domain = "delta.myDomain",
            Configuration = """{"key":"value"}""",
            Removed = false,
        };

        var actions = new List<DeltaAction> { original };
        byte[] serialized = ActionSerializer.Serialize(actions);
        var deserialized = ActionSerializer.Deserialize(serialized);

        var result = Assert.Single(deserialized);
        var dm = Assert.IsType<DomainMetadata>(result);
        Assert.Equal("delta.myDomain", dm.Domain);
        Assert.Equal("""{"key":"value"}""", dm.Configuration);
        Assert.False(dm.Removed);
    }

    [Fact]
    public void RoundTrip_MultipleActions()
    {
        var actions = new List<DeltaAction>
        {
            new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 },
            new MetadataAction
            {
                Id = "abc",
                Format = Format.Parquet,
                SchemaString = """{"type":"struct","fields":[]}""",
                PartitionColumns = [],
            },
            new AddFile
            {
                Path = "file1.parquet",
                PartitionValues = new Dictionary<string, string>(),
                Size = 100,
                ModificationTime = 1000,
                DataChange = true,
            },
            new AddFile
            {
                Path = "file2.parquet",
                PartitionValues = new Dictionary<string, string>(),
                Size = 200,
                ModificationTime = 2000,
                DataChange = true,
            },
        };

        byte[] serialized = ActionSerializer.Serialize(actions);
        var deserialized = ActionSerializer.Deserialize(serialized);

        Assert.Equal(4, deserialized.Count);
        Assert.IsType<ProtocolAction>(deserialized[0]);
        Assert.IsType<MetadataAction>(deserialized[1]);
        Assert.IsType<AddFile>(deserialized[2]);
        Assert.IsType<AddFile>(deserialized[3]);
    }

    [Fact]
    public void Deserialize_UnknownActionType_Skipped()
    {
        string ndjson = """
            {"unknownAction":{"key":"value"}}
            {"add":{"path":"file.parquet","partitionValues":{},"size":100,"modificationTime":1000,"dataChange":true}}
            """;

        byte[] data = System.Text.Encoding.UTF8.GetBytes(ndjson);
        var actions = ActionSerializer.Deserialize(data);

        // Unknown action is skipped, only the add action is returned
        var result = Assert.Single(actions);
        Assert.IsType<AddFile>(result);
    }

    [Fact]
    public void Deserialize_RealWorldCommit()
    {
        // Simulated PySpark-style commit file content
        string ndjson = """
            {"commitInfo":{"timestamp":1700000000000,"operation":"WRITE","operationParameters":{"mode":"Append"},"engineInfo":"Apache-Spark/3.5.0"}}
            {"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
            {"metaData":{"id":"d5e8d2a2-dc5e-4b53-9f3c-123456789abc","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":false,\"metadata\":{}},{\"name\":\"value\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1700000000000}}
            {"add":{"path":"part-00000-abc.snappy.parquet","partitionValues":{},"size":1234,"modificationTime":1700000000000,"dataChange":true,"stats":"{\"numRecords\":100,\"minValues\":{\"id\":1},\"maxValues\":{\"id\":100},\"nullCount\":{\"id\":0,\"value\":5}}"}}
            """;

        byte[] data = System.Text.Encoding.UTF8.GetBytes(ndjson);
        var actions = ActionSerializer.Deserialize(data);

        Assert.Equal(4, actions.Count);
        Assert.IsType<CommitInfo>(actions[0]);
        Assert.IsType<ProtocolAction>(actions[1]);
        Assert.IsType<MetadataAction>(actions[2]);
        Assert.IsType<AddFile>(actions[3]);

        var add = (AddFile)actions[3];
        Assert.Equal("part-00000-abc.snappy.parquet", add.Path);
        Assert.Equal(1234L, add.Size);
        Assert.NotNull(add.Stats);
    }
}
