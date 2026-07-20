// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Snapshot;

namespace EngineeredWood.DeltaLake.Tests;

public class SnapshotBuilderTests
{
    [Fact]
    public void Build_InitialCommit()
    {
        var builder = new SnapshotBuilder();
        builder.ApplyCommit(0, new List<DeltaAction>
        {
            new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 },
            new MetadataAction
            {
                Id = "test-id",
                Format = Format.Parquet,
                SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}}]}""",
                PartitionColumns = [],
            },
        });

        var snapshot = builder.Build();
        Assert.Equal(0L, snapshot.Version);
        Assert.Equal(1, snapshot.Protocol.MinReaderVersion);
        Assert.Equal(2, snapshot.Protocol.MinWriterVersion);
        Assert.Equal("test-id", snapshot.Metadata.Id);
        Assert.Empty(snapshot.ActiveFiles);
    }

    [Fact]
    public void Build_AddAndRemoveFiles()
    {
        var builder = new SnapshotBuilder();
        builder.ApplyCommit(0, new List<DeltaAction>
        {
            new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 },
            new MetadataAction
            {
                Id = "test-id",
                Format = Format.Parquet,
                SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}}]}""",
                PartitionColumns = [],
            },
        });

        // Add two files
        builder.ApplyCommit(1, new List<DeltaAction>
        {
            new AddFile
            {
                Path = "file1.parquet",
                PartitionValues = new Dictionary<string, string>(),
                Size = 100, ModificationTime = 1000, DataChange = true,
            },
            new AddFile
            {
                Path = "file2.parquet",
                PartitionValues = new Dictionary<string, string>(),
                Size = 200, ModificationTime = 2000, DataChange = true,
            },
        });

        var snapshot1 = builder.Build();
        Assert.Equal(2, snapshot1.FileCount);

        // Remove one file
        builder.ApplyCommit(2, new List<DeltaAction>
        {
            new RemoveFile
            {
                Path = "file1.parquet",
                DeletionTimestamp = 3000,
                DataChange = true,
            },
        });

        var snapshot2 = builder.Build();
        Assert.Equal(1, snapshot2.FileCount);
        Assert.Contains("file2.parquet", snapshot2.ActiveFiles.Keys);
        Assert.DoesNotContain("file1.parquet", snapshot2.ActiveFiles.Keys);
    }

    [Fact]
    public void Build_MetadataOverwrite()
    {
        var builder = new SnapshotBuilder();
        builder.ApplyCommit(0, new List<DeltaAction>
        {
            new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 },
            new MetadataAction
            {
                Id = "original-id",
                Format = Format.Parquet,
                SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}}]}""",
                PartitionColumns = [],
            },
        });

        builder.ApplyCommit(1, new List<DeltaAction>
        {
            new MetadataAction
            {
                Id = "new-id",
                Format = Format.Parquet,
                SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}},{"name":"name","type":"string","nullable":true,"metadata":{}}]}""",
                PartitionColumns = [],
            },
        });

        var snapshot = builder.Build();
        Assert.Equal("new-id", snapshot.Metadata.Id);
    }

    [Fact]
    public void Build_TransactionReconciliation()
    {
        var builder = new SnapshotBuilder();
        builder.ApplyCommit(0, new List<DeltaAction>
        {
            new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 },
            new MetadataAction
            {
                Id = "test-id",
                Format = Format.Parquet,
                SchemaString = """{"type":"struct","fields":[]}""",
                PartitionColumns = [],
            },
            new TransactionId { AppId = "app1", Version = 1 },
        });

        builder.ApplyCommit(1, new List<DeltaAction>
        {
            new TransactionId { AppId = "app1", Version = 5 },
            new TransactionId { AppId = "app2", Version = 1 },
        });

        var snapshot = builder.Build();
        Assert.Equal(2, snapshot.AppTransactions.Count);
        Assert.Equal(5L, snapshot.AppTransactions["app1"].Version);
        Assert.Equal(1L, snapshot.AppTransactions["app2"].Version);
    }

    [Fact]
    public void Build_DomainMetadata_Tombstone()
    {
        var builder = new SnapshotBuilder();
        builder.ApplyCommit(0, new List<DeltaAction>
        {
            new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 },
            new MetadataAction
            {
                Id = "test-id",
                Format = Format.Parquet,
                SchemaString = """{"type":"struct","fields":[]}""",
                PartitionColumns = [],
            },
            new DomainMetadata
            {
                Domain = "myDomain",
                Configuration = """{"key":"value"}""",
                Removed = false,
            },
        });

        var snapshot1 = builder.Build();
        Assert.Single(snapshot1.DomainMetadata);

        builder.ApplyCommit(1, new List<DeltaAction>
        {
            new DomainMetadata
            {
                Domain = "myDomain",
                Configuration = "",
                Removed = true,
            },
        });

        var snapshot2 = builder.Build();
        Assert.Empty(snapshot2.DomainMetadata);
    }

    [Fact]
    public void Build_ThrowsWithoutMetadata()
    {
        var builder = new SnapshotBuilder();
        builder.ApplyCommit(0, new List<DeltaAction>
        {
            new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 },
        });

        Assert.Throws<DeltaFormatException>(() => builder.Build());
    }

    [Fact]
    public void Build_ThrowsWithoutProtocol()
    {
        var builder = new SnapshotBuilder();
        builder.ApplyCommit(0, new List<DeltaAction>
        {
            new MetadataAction
            {
                Id = "test-id",
                Format = Format.Parquet,
                SchemaString = """{"type":"struct","fields":[]}""",
                PartitionColumns = [],
            },
        });

        Assert.Throws<DeltaFormatException>(() => builder.Build());
    }
}
