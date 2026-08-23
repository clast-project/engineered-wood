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
    /// <para>fake-gcs-server <b>ignores <c>ifGenerationMatch=0</c></b> — measured at the wire level, two
    /// raw create-if-absent uploads both return 200 and the second overwrites. That makes it unable to
    /// answer for <see cref="ITableFileSystem.TryWriteAllBytesAsync"/>, the commit primitive Delta and
    /// Iceberg commits are built on, so a CI job standing this emulator up would get either a false green
    /// or a permanent false red on exactly that property. An argument for a stricter GCS fake, not for a
    /// weaker assertion — <c>googleapis/storage-testbench</c> returns <c>412</c> here and preserves the
    /// winner's bytes.</para>
    ///
    /// <para><b>A <c>RangedRead</c> gap used to be declared here and should never have
    /// been.</b> The hash mismatch on a ranged read was not this emulator being unfaithful; it was
    /// <c>GcsRandomAccessFile</c> leaving <c>DownloadValidationMode</c> unset. storage-testbench — which
    /// Google builds to validate its own client libraries — sends the same whole-object hash on a
    /// <c>206</c>, so the behaviour being blamed on the fake is how the service behaves. Worth recording
    /// as a caution: a gap declared against someone else's component is a claim that needs the same
    /// evidence as any other, and this one was wrong for a fortnight.</para>
    /// </summary>
    protected override EmulatorGap Gaps => EmulatorGap.CreateIfAbsent;

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
