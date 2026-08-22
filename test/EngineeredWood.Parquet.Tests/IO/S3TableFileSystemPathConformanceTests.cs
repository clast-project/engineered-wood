// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using EngineeredWood.IO;
using EngineeredWood.IO.Aws;

namespace EngineeredWood.Tests.IO;

/// <summary>
/// <see cref="TableFileSystemPathConformanceTests"/> against <see cref="S3TableFileSystem"/>, which needs
/// an S3-compatible endpoint on 127.0.0.1:9000 (gofakes3, MinIO or LocalStack).
/// </summary>
/// <remarks>
/// S3 is the backend where a name can fail in the quietest way. SigV4 signs a canonical URI built by
/// encoding the key, and the request sends its own encoding of the same key; when the two disagree the
/// result is <c>SignatureDoesNotMatch</c> or a 404 -- but when the ENCODING is consistent and merely wrong
/// (a <c>%</c> passed through unescaped, say), the request succeeds against a different object and no layer
/// reports anything.
/// </remarks>
public sealed class S3TableFileSystemPathConformanceTests : TableFileSystemPathConformanceTests
{
    private const string ServiceUrl = "http://localhost:9000";

    // See the note in the Azure suite: one probe pays the connect timeout, the rest read the answer.
    private static bool s_knownUnreachable;
    private static string? s_knownUnreachableReason;

    private IAmazonS3? _client;
    private string _bucket = "";
    private S3TableFileSystem? _fileSystem;
    private string? _unavailableReason;

    /// <inheritdoc/>
    protected override string Emulator => "an S3 emulator on 127.0.0.1:9000";

    /// <inheritdoc/>
    protected override bool Available => _fileSystem is not null;

    /// <inheritdoc/>
    protected override string? UnavailableReason => _unavailableReason;

    /// <inheritdoc/>
    protected override ITableFileSystem FileSystem => _fileSystem!;

    /// <inheritdoc/>
    public override async Task InitializeAsync()
    {
        if (s_knownUnreachable)
        {
            _unavailableReason = s_knownUnreachableReason;
            return;
        }

        try
        {
            var config = new AmazonS3Config
            {
                ServiceURL = ServiceUrl,
                ForcePathStyle = true,
                Timeout = TimeSpan.FromSeconds(2),
                MaxErrorRetry = 0,

                // AWSSDK v4 defaults to wrapping request bodies in `aws-chunked` framing with a trailing
                // checksum. Real S3 strips it; gofakes3 stores the framing AS the object body, so a
                // 600-byte payload reads back longer than it went in and every byte assertion in this
                // suite would fail for a reason that has nothing to do with the object's NAME. Turning the
                // framing off keeps the emulator an oracle for what is actually under test here.
                RequestChecksumCalculation = RequestChecksumCalculation.WHEN_REQUIRED,
                ResponseChecksumValidation = ResponseChecksumValidation.WHEN_REQUIRED,
            };

            _client = new AmazonS3Client(new BasicAWSCredentials("minioadmin", "minioadmin"), config);

            _bucket = "ew-conf-" + Guid.NewGuid().ToString("N")[..8];
            await _client.PutBucketAsync(new PutBucketRequest { BucketName = _bucket });
            _fileSystem = new S3TableFileSystem(_client, _bucket, "table-root");
        }
        catch (Exception ex)
        {
            _unavailableReason = ex.Message;
            s_knownUnreachable = true;
            s_knownUnreachableReason = ex.Message;
        }
    }

    /// <inheritdoc/>
    public override async Task DisposeAsync()
    {
        if (_client is not null && _fileSystem is not null)
        {
            try
            {
                var listed = await _client.ListObjectsV2Async(
                    new ListObjectsV2Request { BucketName = _bucket });
                if (listed.S3Objects.Count > 0)
                {
                    await _client.DeleteObjectsAsync(new DeleteObjectsRequest
                    {
                        BucketName = _bucket,
                        Objects = listed.S3Objects.Select(o => new KeyVersion { Key = o.Key }).ToList(),
                    });
                }

                await _client.DeleteBucketAsync(_bucket);
            }
            catch (Exception)
            {
                // Best-effort cleanup.
            }
        }

        // Dispose on every path: the probe builds the client before the call that can fail.
        _client?.Dispose();
    }
}
