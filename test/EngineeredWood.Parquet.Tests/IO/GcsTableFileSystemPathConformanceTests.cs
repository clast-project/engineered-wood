// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.IO;
using EngineeredWood.IO.Gcs;
using Google.Cloud.Storage.V1;

namespace EngineeredWood.Tests.IO;

/// <summary>
/// <see cref="TableFileSystemPathConformanceTests"/> against <see cref="GcsTableFileSystem"/>, which needs
/// a GCS emulator (fake-gcs-server) on 127.0.0.1:4443.
/// </summary>
/// <remarks>
/// <para>GCS puts the object name in the PATH for a download and in a QUERY PARAMETER for an upload, so a
/// single name has to survive two different encodings that are easy to get independently wrong -- and a
/// disagreement between them writes to one object and reads from another.</para>
/// <para>Run the emulator with <c>-backend memory</c>, never <c>-backend filesystem</c>: a
/// filesystem-backed fake stores object names as on-disk paths and so reports the HOST volume's naming
/// rules as the store's, which makes it an invalid oracle for exactly the question this suite asks.</para>
/// </remarks>
public sealed class GcsTableFileSystemPathConformanceTests : TableFileSystemPathConformanceTests
{
    private const string EmulatorBaseUri = "http://localhost:4443/storage/v1/";
    private const string ProjectId = "ew-test-project";

    // See the note in the Azure suite: one probe pays the connect timeout, the rest read the answer.
    private static bool s_knownUnreachable;
    private static string? s_knownUnreachableReason;

    private StorageClient? _client;
    private string _bucket = "";
    private GcsTableFileSystem? _fileSystem;
    private string? _unavailableReason;

    /// <summary>
    /// <para>fake-gcs-server is the least faithful of the three emulators, and both gaps are measured at
    /// the wire level rather than inferred from a red test:</para>
    /// <list type="bullet">
    /// <item><description>a <c>Range</c> request comes back <c>206 Partial Content</c> carrying
    /// <c>X-Goog-Hash: crc32c=</c> over the WHOLE object, which the Google client then checks against the
    /// 50 bytes it asked for.</description></item>
    /// <item><description><c>ifGenerationMatch=0</c> is ignored: two raw create-if-absent uploads both
    /// return 200 and the second overwrites.</description></item>
    /// </list>
    /// <para>The second is the more serious of the two for step 2. It means fake-gcs-server cannot be the
    /// oracle for <see cref="ITableFileSystem.TryWriteAllBytesAsync"/> on GCS -- the commit primitive
    /// itself -- and so a CI job standing this emulator up gets either a false green or a permanent false
    /// red on exactly the property Delta and Iceberg commits are built on. That is an argument for a
    /// stricter GCS fake, not for a weaker assertion.</para>
    /// </summary>
    protected override EmulatorGap Gaps => EmulatorGap.RangedRead | EmulatorGap.CreateIfAbsent;

    /// <inheritdoc/>
    protected override string Emulator => "fake-gcs-server on 127.0.0.1:4443";

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
            _client = new StorageClientBuilder
            {
                BaseUri = EmulatorBaseUri,
                UnauthenticatedAccess = true,
            }.Build();

            _bucket = "ew-conf-" + Guid.NewGuid().ToString("N")[..8];
            await _client.CreateBucketAsync(ProjectId, _bucket);
            _fileSystem = new GcsTableFileSystem(_client, _bucket, "table-root");
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
                await _client.DeleteBucketAsync(
                    _bucket, new DeleteBucketOptions { DeleteObjects = true });
            }
            catch (Exception)
            {
                // Best-effort cleanup.
            }
        }

        _client?.Dispose();
    }
}
