// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

#if NET9_0_OR_GREATER // .NET 8's DispatchProxy cannot emit a proxy for IAmazonS3 (TypeLoadException)
using System.Collections.Concurrent;
using System.Net;
using System.Reflection;
using Amazon.S3;
using Amazon.S3.Model;
using EngineeredWood.IO;
using EngineeredWood.IO.Aws;

namespace EngineeredWood.Tests.IO;

/// <summary>
/// <see cref="S3SequentialFile"/>'s parts uploading in the background (#713), against an in-memory S3 that
/// implements only the requests the writer sends. The emulators cannot check this: gofakes3 stores the SDK's
/// <c>aws-chunked</c> framing in a part's body, and MinIO, which handles it, runs only in CI. Each part upload
/// is delayed, so that uploads overlap when the writer lets them.
/// </summary>
public class S3SequentialFileBackgroundTests
{
    private const int Part = S3SequentialFile.DefaultPartSize;

    /// <summary>
    /// The S3 requests <see cref="S3SequentialFile"/> sends, kept in memory: multipart uploads, their parts,
    /// whole objects. It records how many part uploads were in flight at once, and whether an abort arrived
    /// while one was.
    /// </summary>
    public class FakeS3 : DispatchProxy
    {
        private readonly object _gate = new();
        private readonly ConcurrentDictionary<string, ConcurrentDictionary<int, (byte[] Bytes, string ETag)>> _uploads = new();
        private int _inFlight;

        public ConcurrentDictionary<string, byte[]> Objects { get; } = new();
        public int MaxPartsInFlight { get; private set; }
        public int AbortsWhilePartsInFlight { get; private set; }
        public int Aborts { get; private set; }
        public int PartDelayMs { get; set; } = 30;
        public int? FailPart { get; set; }

        public static (IAmazonS3 Client, FakeS3 Fake) Create()
        {
            var client = Create<IAmazonS3, FakeS3>();
            return (client, (FakeS3)(object)client);
        }

        protected override object? Invoke(MethodInfo? method, object?[]? args)
        {
            var ct = args?.OfType<CancellationToken>().FirstOrDefault() ?? default;
            return (method!.Name, args?.FirstOrDefault()) switch
            {
                ("InitiateMultipartUploadAsync", InitiateMultipartUploadRequest)
                    => Task.FromResult(Initiate()),
                ("UploadPartAsync", UploadPartRequest request) => UploadPartAsync(request, ct),
                ("CompleteMultipartUploadAsync", CompleteMultipartUploadRequest request)
                    => Task.FromResult(Complete(request)),
                ("AbortMultipartUploadAsync", AbortMultipartUploadRequest request)
                    => Task.FromResult(Abort(request)),
                ("PutObjectAsync", PutObjectRequest request) => Task.FromResult(Put(request)),
                ("Dispose", _) => null,
                _ => throw new NotSupportedException($"FakeS3 does not implement {method.Name}."),
            };
        }

        private InitiateMultipartUploadResponse Initiate()
        {
            string id = Guid.NewGuid().ToString("N");
            _uploads[id] = new();
            return new InitiateMultipartUploadResponse { UploadId = id };
        }

        private async Task<UploadPartResponse> UploadPartAsync(UploadPartRequest request, CancellationToken ct)
        {
            lock (_gate)
                MaxPartsInFlight = Math.Max(MaxPartsInFlight, ++_inFlight);
            try
            {
                using var copy = new MemoryStream();
                await request.InputStream.CopyToAsync(copy, ct);
                await Task.Delay(PartDelayMs, ct);
                int number = (int)request.PartNumber!;
                if (number == FailPart)
                    throw new AmazonS3Exception($"part {number} failed") { StatusCode = HttpStatusCode.InternalServerError };
                if (!_uploads.TryGetValue(request.UploadId, out var parts))
                    throw new AmazonS3Exception("NoSuchUpload") { StatusCode = HttpStatusCode.NotFound };
                string etag = Guid.NewGuid().ToString("N");
                parts[number] = (copy.ToArray(), etag);
                return new UploadPartResponse { ETag = etag };
            }
            finally
            {
                lock (_gate)
                    _inFlight--;
            }
        }

        private CompleteMultipartUploadResponse Complete(CompleteMultipartUploadRequest request)
        {
            if (!_uploads.TryRemove(request.UploadId, out var parts))
                throw new AmazonS3Exception("NoSuchUpload") { StatusCode = HttpStatusCode.NotFound };
            var listed = request.PartETags.Select(p => (int)p.PartNumber!).ToList();
            // S3 refuses a list out of order (InvalidPartOrder) or naming a part it does not have.
            Assert.Equal(listed.OrderBy(n => n), listed);
            Assert.Equal(parts.Keys.OrderBy(n => n), listed);
            foreach (var p in request.PartETags)
                Assert.Equal(parts[(int)p.PartNumber!].ETag, p.ETag);
            Objects[request.Key] = listed.SelectMany(n => parts[n].Bytes).ToArray();
            return new CompleteMultipartUploadResponse();
        }

        private AbortMultipartUploadResponse Abort(AbortMultipartUploadRequest request)
        {
            lock (_gate)
            {
                Aborts++;
                if (_inFlight > 0)
                    AbortsWhilePartsInFlight++;
            }
            _uploads.TryRemove(request.UploadId, out _);
            return new AbortMultipartUploadResponse();
        }

        private PutObjectResponse Put(PutObjectRequest request)
        {
            using var copy = new MemoryStream();
            request.InputStream.CopyTo(copy);
            Objects[request.Key] = copy.ToArray();
            return new PutObjectResponse();
        }

        public int OpenUploads => _uploads.Count;
    }

    private static byte[] Payload(int length, int seed)
    {
        var payload = new byte[length];
        new Random(seed).NextBytes(payload);
        return payload;
    }

    private static async Task WriteInPiecesAsync(ISequentialFile file, byte[] payload)
    {
        int[] pieces = [1, 4093, 1 << 20, 5 * (1 << 20) + 7, 65536];
        int offset = 0;
        for (int i = 0; offset < payload.Length; i++)
        {
            int length = Math.Min(pieces[i % pieces.Length], payload.Length - offset);
            await file.WriteAsync(payload.AsMemory(offset, length));
            offset += length;
        }
    }

    /// <summary>
    /// Six parts and a partial one: with two spare parts, up to three upload at once (two in the background, one
    /// in turn), and the object is the payload, its parts listed in order whichever finished first. With none,
    /// one at a time.
    /// </summary>
    [Theory]
    [InlineData(0, 1, 1)]
    [InlineData(2, 2, 3)]
    public async Task Parts_UploadAtOnce_UpToTheSpares_AndCompleteInOrder(int uploadBuffers, int minInFlight, int maxInFlight)
    {
        var (client, fake) = FakeS3.Create();
        var fs = new S3TableFileSystem(client, "bucket", uploadBuffers: uploadBuffers);
        var payload = Payload(6 * Part + 777, seed: uploadBuffers);

        await using (var file = await fs.CreateAsync("data.bin", overwrite: true))
        {
            await WriteInPiecesAsync(file, payload);
            await file.CompleteAsync();
        }

        Assert.Equal(payload, fake.Objects["data.bin"]);
        Assert.InRange(fake.MaxPartsInFlight, minInFlight, maxInFlight);
        Assert.Equal(0, fake.OpenUploads);
    }

    /// <summary>An object under one part is still a single PutObject, its buffer never a whole part.</summary>
    [Fact]
    public async Task ASmallObject_IsOnePut()
    {
        var (client, fake) = FakeS3.Create();
        var fs = new S3TableFileSystem(client, "bucket");
        var payload = Payload(100_000, seed: 7);

        await using (var file = await fs.CreateAsync("small.bin", overwrite: true))
        {
            await WriteInPiecesAsync(file, payload);
            await file.CompleteAsync();
        }

        Assert.Equal(payload, fake.Objects["small.bin"]);
        Assert.Equal(0, fake.MaxPartsInFlight);
    }

    /// <summary>
    /// Disposed with parts uploading, the writer publishes nothing and aborts the upload only after the parts in
    /// flight have ended: S3 may keep a part that finishes after its upload was aborted.
    /// </summary>
    [Fact]
    public async Task Abandoned_AbortsAfterThePartsInFlight()
    {
        var (client, fake) = FakeS3.Create();
        fake.PartDelayMs = 200;
        var fs = new S3TableFileSystem(client, "bucket", uploadBuffers: 4);

        await using (var file = await fs.CreateAsync("abandoned.bin", overwrite: true))
            await WriteInPiecesAsync(file, Payload(4 * Part + 1, seed: 8));

        Assert.Empty(fake.Objects);
        Assert.Equal(1, fake.Aborts);
        Assert.Equal(0, fake.AbortsWhilePartsInFlight);
        Assert.Equal(0, fake.OpenUploads);
    }

    /// <summary>
    /// A part that fails in the background fails the write: completing reports it, the upload is aborted, and
    /// nothing is published.
    /// </summary>
    [Fact]
    public async Task AFailedBackgroundPart_FailsTheCompletion_AndAbortsTheUpload()
    {
        var (client, fake) = FakeS3.Create();
        fake.FailPart = 2;
        var fs = new S3TableFileSystem(client, "bucket", uploadBuffers: 4);

        await using (var file = await fs.CreateAsync("failed.bin", overwrite: true))
        {
            var failure = await Assert.ThrowsAsync<AmazonS3Exception>(async () =>
            {
                await WriteInPiecesAsync(file, Payload(5 * Part, seed: 9));
                await file.CompleteAsync();
            });
            Assert.Equal("part 2 failed", failure.Message);
        }

        Assert.Empty(fake.Objects);
        Assert.Equal(1, fake.Aborts);
        Assert.Equal(0, fake.OpenUploads);
    }
}
#endif
