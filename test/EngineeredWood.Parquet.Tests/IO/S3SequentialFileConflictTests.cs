// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Net;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using EngineeredWood.IO.Aws;

namespace EngineeredWood.Tests.IO;

/// <summary>
/// S3 answers a conditional write with 409 ConditionalRequestConflict when a concurrent write to the same key
/// was in flight and it could not evaluate the precondition. That provably did not create the object, so the
/// completing request is retried; a conflict that persists is reported as an <see cref="IOException"/>, like a
/// lost race, so a caller that checks for the object afterwards (the classic checkpoint writer) is not
/// bypassed by an <see cref="AmazonS3Exception"/>. No emulator produces a 409 on demand, hence the stub.
/// </summary>
public class S3SequentialFileConflictTests
{
    /// <summary>The first try plus <c>S3TableFileSystem.MaxConflictRetries</c> (internal, and the Aws assembly
    /// cannot expose its internals here without duplicating PolySharp's polyfills on .NET Framework).</summary>
    private const int AttemptsBeforeGivingUp = 4;

    [Fact]
    public async Task PutObject_ConflictThatClears_IsRetriedAndPublishes()
    {
        var client = new ConflictingS3Client { PutConflicts = 2 };
        await using (var file = new S3SequentialFile(client, "bucket", "key", overwrite: false))
        {
            await file.WriteAsync(new byte[] { 1, 2, 3 });
            await file.CompleteAsync();
        }

        Assert.Equal(3, client.PutCalls);
        Assert.Equal([1, 2, 3], client.PutBodies[^1]);
    }

    [Fact]
    public async Task PutObject_ConflictThatPersists_IsReportedAsIOException()
    {
        var client = new ConflictingS3Client { PutConflicts = int.MaxValue };
        await using var file = new S3SequentialFile(client, "bucket", "key", overwrite: false);
        await file.WriteAsync(new byte[] { 1, 2, 3 });

        var ex = await Assert.ThrowsAsync<IOException>(async () => await file.CompleteAsync());

        Assert.IsType<AmazonS3Exception>(ex.InnerException);
        Assert.Equal(AttemptsBeforeGivingUp, client.PutCalls);
    }

    [Fact]
    public async Task CompleteMultipartUpload_ConflictThatClears_IsRetriedWithTheSameParts()
    {
        var client = new ConflictingS3Client { CompleteConflicts = 1 };
        await using (var file = new S3SequentialFile(
            client, "bucket", "key", S3SequentialFile.MinPartSize, overwrite: false))
        {
            // One full part now, and a last one that completion uploads: a retried completion must not
            // upload it again.
            await file.WriteAsync(new byte[S3SequentialFile.MinPartSize]);
            await file.WriteAsync(new byte[1]);
            await file.CompleteAsync();
        }

        Assert.Equal(2, client.CompleteCalls);
        Assert.Equal(2, client.PartsUploaded);
        Assert.Equal(0, client.Aborts);
    }

    [Fact]
    public async Task CompleteMultipartUpload_ConflictThatPersists_IsReportedAndTheUploadAborted()
    {
        var client = new ConflictingS3Client { CompleteConflicts = int.MaxValue };
        await using var file = new S3SequentialFile(
            client, "bucket", "key", S3SequentialFile.MinPartSize, overwrite: false);
        await file.WriteAsync(new byte[S3SequentialFile.MinPartSize + 1]);

        await Assert.ThrowsAsync<IOException>(async () => await file.CompleteAsync());

        Assert.Equal(AttemptsBeforeGivingUp, client.CompleteCalls);
        Assert.Equal(1, client.Aborts);
    }

    /// <summary>With overwrite there is no precondition, so a persistent 409 is not a lost race.</summary>
    [Fact]
    public async Task Overwrite_ConflictThatPersists_KeepsTheS3Exception()
    {
        var client = new ConflictingS3Client { PutConflicts = int.MaxValue };
        await using var file = new S3SequentialFile(client, "bucket", "key", overwrite: true);

        await Assert.ThrowsAsync<AmazonS3Exception>(async () => await file.CompleteAsync());
    }

    private sealed class ConflictingS3Client() : AmazonS3Client(
        new BasicAWSCredentials("unused", "unused"),
        new AmazonS3Config { ServiceURL = "http://127.0.0.1:1", ForcePathStyle = true })
    {
        public int PutConflicts { get; init; }
        public int CompleteConflicts { get; init; }
        public int PutCalls { get; private set; }
        public int CompleteCalls { get; private set; }
        public int PartsUploaded { get; private set; }
        public int Aborts { get; private set; }
        public List<byte[]> PutBodies { get; } = [];

        private static AmazonS3Exception Conflict() => new(
            "conflict (injected)", ErrorType.Sender, "ConditionalRequestConflict", "request", HttpStatusCode.Conflict);

        public override Task<PutObjectResponse> PutObjectAsync(
            PutObjectRequest request, CancellationToken cancellationToken = default)
        {
            PutCalls++;
            var body = new MemoryStream();
            request.InputStream.CopyTo(body);
            PutBodies.Add(body.ToArray());
            if (PutCalls <= PutConflicts)
                throw Conflict();
            return Task.FromResult(new PutObjectResponse());
        }

        public override Task<InitiateMultipartUploadResponse> InitiateMultipartUploadAsync(
            InitiateMultipartUploadRequest request, CancellationToken cancellationToken = default) =>
            Task.FromResult(new InitiateMultipartUploadResponse { UploadId = "upload" });

        public override Task<UploadPartResponse> UploadPartAsync(
            UploadPartRequest request, CancellationToken cancellationToken = default)
        {
            PartsUploaded++;
            return Task.FromResult(new UploadPartResponse { ETag = $"etag-{request.PartNumber}" });
        }

        public override Task<CompleteMultipartUploadResponse> CompleteMultipartUploadAsync(
            CompleteMultipartUploadRequest request, CancellationToken cancellationToken = default)
        {
            CompleteCalls++;
            Assert.Equal("*", request.IfNoneMatch);
            Assert.Equal(PartsUploaded, request.PartETags.Count);
            if (CompleteCalls <= CompleteConflicts)
                throw Conflict();
            return Task.FromResult(new CompleteMultipartUploadResponse());
        }

        public override Task<AbortMultipartUploadResponse> AbortMultipartUploadAsync(
            AbortMultipartUploadRequest request, CancellationToken cancellationToken = default)
        {
            Aborts++;
            return Task.FromResult(new AbortMultipartUploadResponse());
        }
    }
}
