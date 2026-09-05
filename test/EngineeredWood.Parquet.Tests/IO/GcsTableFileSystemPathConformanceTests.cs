// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.IO;
using EngineeredWood.IO.Gcs;
using Google.Cloud.Storage.V1;

namespace EngineeredWood.Tests.IO;

/// <summary>
/// <see cref="TableFileSystemPathConformanceTests"/> against <see cref="GcsTableFileSystem"/>, which needs
/// <c>googleapis/storage-testbench</c> on 127.0.0.1:4443.
/// </summary>
/// <remarks>
/// <para>GCS puts the object name in the PATH for a download and in a QUERY PARAMETER for an upload, so a
/// single name has to survive two different encodings that are easy to get independently wrong -- and a
/// disagreement between them writes to one object and reads from another.</para>
/// <para><b>The emulator is <c>googleapis/storage-testbench</c>, not fake-gcs-server, and the choice is
/// load-bearing.</b> fake-gcs-server ignores <c>ifGenerationMatch=0</c> — measured at the wire level, two
/// create-if-absent uploads both return 200 and the second overwrites — so it cannot answer for
/// <see cref="ITableFileSystem.TryWriteAllBytesAsync"/>, the primitive Delta and Iceberg commits are built
/// on. storage-testbench returns <c>412</c> and preserves the winner's bytes. Google maintains it to
/// validate its own client libraries, which is the property that makes it a credible oracle: it is the
/// fake the vendor's own conformance tests run against.</para>
/// <para>Install and run it with
/// <c>pip install git+https://github.com/googleapis/storage-testbench.git</c> then
/// <c>storage-testbench --port 4443</c>. It takes <c>--port</c>; a bare <c>PORT</c> environment variable
/// is ignored and it binds a random port instead.</para>
/// <para>Whatever fake is used, it must not store object names as paths on the host filesystem — such a
/// fake reports the HOST volume's naming rules as the store's, which makes it an invalid oracle for
/// exactly the question this suite asks. storage-testbench keeps objects in memory.</para>
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


    /// <inheritdoc/>
    protected override string Emulator => "storage-testbench on 127.0.0.1:4443";

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
        if (_client is not null)
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
