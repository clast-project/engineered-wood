// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Azure;
using Azure.Core;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;
using EngineeredWood.IO.Azure;

namespace EngineeredWood.Tests.IO;

public class AzureDataLakeFileSystemTests
{
    [Fact]
    public async Task Rename_UsesConditionalDfsRename()
    {
        var fileClient = new RecordingFileClient();
        var fileSystemClient = new RecordingFileSystemClient(fileClient);
        var fileSystem = new AzureDataLakeTableFileSystem(fileSystemClient);

        bool renamed = await fileSystem.RenameAsync("source.tmp", "target.json");

        Assert.True(renamed);
        Assert.Equal("source.tmp", fileSystemClient.RequestedFilePath);
        Assert.Equal("target.json", fileClient.RenameDestination);
        Assert.Equal(ETag.All, fileClient.DestinationConditions!.IfNoneMatch);
    }

    [Fact]
    public async Task Rename_TargetConflict_ReturnsFalse()
    {
        var fileClient = new RecordingFileClient { RenameStatus = 412 };
        var fileSystem = new AzureDataLakeTableFileSystem(
            new RecordingFileSystemClient(fileClient));

        bool renamed = await fileSystem.RenameAsync("source.tmp", "target.json");

        Assert.False(renamed);
    }

    [Fact]
    public async Task TryWriteAllBytes_CreatesFlushesAndRenamesTemporaryFile()
    {
        var fileClient = new RecordingFileClient();
        var fileSystemClient = new RecordingFileSystemClient(fileClient);
        var fileSystem = new AzureDataLakeTableFileSystem(fileSystemClient);

        bool written = await fileSystem.TryWriteAllBytesAsync(
            "target.json", new byte[] { 1, 2, 3 });

        Assert.True(written);
        Assert.StartsWith(".tmp.", fileSystemClient.RequestedFilePath);
        Assert.Equal(1, fileClient.CreateCalls);
        Assert.Equal(new byte[] { 1, 2, 3 }, fileClient.AppendedBytes);
        Assert.Equal(new bool?[] { true }, fileClient.AppendFlushValues);
        Assert.Equal("target.json", fileClient.RenameDestination);
        Assert.Equal(ETag.All, fileClient.DestinationConditions!.IfNoneMatch);
        Assert.Equal(0, fileClient.DeleteCalls);
    }

    [Fact]
    public async Task TryWriteAllBytes_TargetConflict_ReturnsFalseAndDeletesTemporaryFile()
    {
        var fileClient = new RecordingFileClient { RenameStatus = 412 };
        var fileSystem = new AzureDataLakeTableFileSystem(
            new RecordingFileSystemClient(fileClient));

        bool written = await fileSystem.TryWriteAllBytesAsync(
            "target.json", new byte[] { 1, 2, 3 });

        Assert.False(written);
        Assert.Equal(1, fileClient.DeleteCalls);
    }

    [Fact]
    public async Task List_UsesRecursiveDirectoryListing_AndFiltersPrefix()
    {
        DateTimeOffset modified = DateTimeOffset.UtcNow;
        PathItem[] paths =
        [
            CreatePathItem("table/_delta_log/00000000000000000001.json", false, 23, modified),
            CreatePathItem("table/_delta_log/00000000000000000000.json", false, 12, modified),
            CreatePathItem("table/_delta_log/checkpoints", true, null, modified),
            CreatePathItem("table/data/part.parquet", false, 34, modified),
        ];
        var fileSystemClient = new RecordingFileSystemClient(new RecordingFileClient(), paths);
        var fileSystem = new AzureDataLakeTableFileSystem(fileSystemClient, "table");

        var listed = new List<EngineeredWood.IO.TableFileInfo>();
        await foreach (EngineeredWood.IO.TableFileInfo item in fileSystem.ListAsync("_delta_log/"))
        {
            listed.Add(item);
        }

        Assert.Equal(
            [
                "_delta_log/00000000000000000000.json",
                "_delta_log/00000000000000000001.json",
            ],
            listed.Select(static item => item.Path));
        Assert.Equal(12, listed[0].Size);
        Assert.Equal("table/_delta_log", fileSystemClient.GetPathsOptions!.Path);
        Assert.True(fileSystemClient.GetPathsOptions.Recursive);
    }

    [Fact]
    public async Task List_MissingDirectory_ReturnsEmpty()
    {
        var fileSystem = new AzureDataLakeTableFileSystem(
            new RecordingFileSystemClient(new RecordingFileClient(), getPathsStatus: 404));

        var listed = new List<EngineeredWood.IO.TableFileInfo>();
        await foreach (EngineeredWood.IO.TableFileInfo item in fileSystem.ListAsync("missing/"))
        {
            listed.Add(item);
        }

        Assert.Empty(listed);
    }

    [Fact]
    public async Task SequentialFile_AppendsInChunks_AndFlushesOnce()
    {
        var client = new RecordingFileClient();
        await using var file = new AzureDataLakeSequentialFile(client, appendSize: 4);

        await file.WriteAsync(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 });
        await file.FlushAsync();
        await file.FlushAsync();

        Assert.Equal(new long[] { 0, 4, 8 }, client.AppendOffsets);
        Assert.Equal(
            new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 },
            client.AppendedBytes);
        Assert.Equal(new long[] { 10 }, client.FlushPositions);

        await file.WriteAsync(new byte[] { 10 });
        await file.FlushAsync();

        Assert.Equal(
            new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 },
            client.AppendedBytes);
        Assert.Equal(new long[] { 10, 11 }, client.FlushPositions);
    }

    [Fact]
    public async Task RandomAccessFile_ReadsRequestedRange()
    {
        var client = new RecordingFileClient
        {
            ReadBytes = new byte[] { 0, 1, 2, 3, 4, 5 },
        };
        using var file = new AzureDataLakeRandomAccessFile(client, knownLength: 6);

        using System.Buffers.IMemoryOwner<byte> owner = await file.ReadAsync(
            new EngineeredWood.IO.FileRange(2, 3));

        Assert.Equal(new byte[] { 2, 3, 4 }, owner.Memory.ToArray());
        Assert.Equal(2, client.ReadRange!.Value.Offset);
        Assert.Equal(3, client.ReadRange.Value.Length);
    }

    [Fact]
    public async Task SequentialFile_SynchronousDispose_FinalizesBufferedData()
    {
        var client = new RecordingFileClient();
        var file = new AzureDataLakeSequentialFile(client, appendSize: 4);

        await file.WriteAsync(new byte[] { 1, 2, 3 });
        file.Dispose();

        Assert.Equal(new byte[] { 1, 2, 3 }, client.AppendedBytes);
        Assert.Equal(new long[] { 3 }, client.FlushPositions);
    }

    private static PathItem CreatePathItem(
        string name, bool isDirectory, long? contentLength, DateTimeOffset lastModified) =>
        DataLakeModelFactory.PathItem(
            name,
            isDirectory,
            lastModified,
            default,
            contentLength,
            owner: string.Empty,
            group: string.Empty,
            permissions: string.Empty,
            createdOn: null,
            expiresOn: null,
            encryptionContext: string.Empty);

    private sealed class RecordingFileSystemClient(
        RecordingFileClient fileClient,
        IReadOnlyList<PathItem>? paths = null,
        int? getPathsStatus = null)
        : DataLakeFileSystemClient
    {
        public string? RequestedFilePath { get; private set; }

        public DataLakeGetPathsOptions? GetPathsOptions { get; private set; }

        public override DataLakeFileClient GetFileClient(string filePath)
        {
            RequestedFilePath = filePath;
            return fileClient;
        }

        public override AsyncPageable<PathItem> GetPathsAsync(
            DataLakeGetPathsOptions? options = null,
            CancellationToken cancellationToken = default)
        {
            GetPathsOptions = options;
            if (getPathsStatus is int status)
            {
                throw new RequestFailedException(status, "Path not found");
            }

            Page<PathItem> page = Page<PathItem>.FromValues(
                paths ?? [], continuationToken: null, new StubResponse(200));
            return AsyncPageable<PathItem>.FromPages([page]);
        }
    }

    private sealed class RecordingFileClient : DataLakeFileClient
    {
        private readonly List<byte> _appendedBytes = new();

        public int? RenameStatus { get; init; }

        public byte[]? ReadBytes { get; init; }

        public HttpRange? ReadRange { get; private set; }

        public string? RenameDestination { get; private set; }

        public DataLakeRequestConditions? DestinationConditions { get; private set; }

        public int CreateCalls { get; private set; }

        public int DeleteCalls { get; private set; }

        public List<long> AppendOffsets { get; } = new();

        public List<bool?> AppendFlushValues { get; } = new();

        public byte[] AppendedBytes => _appendedBytes.ToArray();

        public List<long> FlushPositions { get; } = new();

        public override Task<Response<PathInfo>> CreateAsync(
            DataLakePathCreateOptions? options = null,
            CancellationToken cancellationToken = default)
        {
            CreateCalls++;
            PathInfo info = DataLakeModelFactory.PathInfo(default, default);
            return Task.FromResult(Response.FromValue(info, new StubResponse(201)));
        }

        public override Task<Response<DataLakeFileReadStreamingResult>> ReadStreamingAsync(
            DataLakeFileReadOptions? options = null,
            CancellationToken cancellationToken = default)
        {
            ReadRange = options!.Range;
            int offset = checked((int)options.Range.Offset);
            int length = checked((int)options.Range.Length!.Value);
            var content = new MemoryStream(ReadBytes!, offset, length, writable: false);
            DataLakeFileReadStreamingResult result =
                DataLakeModelFactory.DataLakeFileReadStreamingResult(content, null!);
            return Task.FromResult(Response.FromValue(result, new StubResponse(206)));
        }

        public override Task<Response<DataLakeFileClient>> RenameAsync(
            string destinationPath,
            string? destinationFileSystem = null,
            DataLakeRequestConditions? sourceConditions = null,
            DataLakeRequestConditions? destinationConditions = null,
            CancellationToken cancellationToken = default)
        {
            RenameDestination = destinationPath;
            DestinationConditions = destinationConditions;
            if (RenameStatus is int status)
            {
                throw new RequestFailedException(status, "Conflict");
            }

            return Task.FromResult(Response.FromValue<DataLakeFileClient>(
                this, new StubResponse(200)));
        }

        public override async Task<Response> AppendAsync(
            Stream content,
            long offset,
            DataLakeFileAppendOptions? options = null,
            CancellationToken cancellationToken = default)
        {
            AppendOffsets.Add(offset);
            AppendFlushValues.Add(options?.Flush);
            using var copy = new MemoryStream();
            await content.CopyToAsync(copy, 81920, cancellationToken);
            _appendedBytes.AddRange(copy.ToArray());
            return new StubResponse(202);
        }

        public override Task<Response<PathInfo>> FlushAsync(
            long position,
            DataLakeFileFlushOptions? options = null,
            CancellationToken cancellationToken = default)
        {
            FlushPositions.Add(position);
            PathInfo info = DataLakeModelFactory.PathInfo(default, default);
            return Task.FromResult(Response.FromValue(info, new StubResponse(200)));
        }

        public override Task<Response<bool>> DeleteIfExistsAsync(
            DataLakeRequestConditions? conditions = null,
            CancellationToken cancellationToken = default)
        {
            DeleteCalls++;
            return Task.FromResult(Response.FromValue(true, new StubResponse(200)));
        }
    }

    private sealed class StubResponse(int status) : Response
    {
        public override int Status => status;

        public override string ReasonPhrase => string.Empty;

        public override Stream? ContentStream { get; set; }

        public override string ClientRequestId { get; set; } = string.Empty;

        protected override bool ContainsHeader(string name) => false;

        public override void Dispose()
        {
        }

        protected override bool TryGetHeader(string name, out string value)
        {
            value = string.Empty;
            return false;
        }

        protected override bool TryGetHeaderValues(string name, out IEnumerable<string> values)
        {
            values = [];
            return false;
        }

        protected override IEnumerable<HttpHeader> EnumerateHeaders() => [];
    }
}
