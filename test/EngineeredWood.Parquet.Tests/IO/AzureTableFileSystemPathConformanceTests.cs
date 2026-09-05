// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Azure.Storage.Blobs;
using EngineeredWood.IO;
using EngineeredWood.IO.Azure;

namespace EngineeredWood.Tests.IO;

/// <summary>
/// <see cref="TableFileSystemPathConformanceTests"/> against <see cref="AzureTableFileSystem"/>, which
/// needs Azurite on 127.0.0.1:10000.
/// </summary>
/// <remarks>
/// Azure is where <c>#</c> and <c>?</c> bite hardest: a blob is addressed by a URL whose path is the blob
/// name, so a name pasted in unencoded loses everything from the first <c>#</c> onward to the fragment and
/// everything from the first <c>?</c> onward to the query string -- and the truncated name is a perfectly
/// valid request for a different blob.
/// </remarks>
public sealed class AzureTableFileSystemPathConformanceTests : TableFileSystemPathConformanceTests
{
    private const string AzuriteConnectionString =
        "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;";

    // Probing costs a connect timeout per test instance, and this suite has many instances. Once one has
    // established that nothing is listening, the rest skip on the cached answer rather than each paying
    // the wait again.
    private static bool s_knownUnreachable;
    private static string? s_knownUnreachableReason;

    private BlobContainerClient? _container;
    private AzureTableFileSystem? _fileSystem;
    private string? _unavailableReason;

    /// <inheritdoc/>
    protected override string Emulator => "Azurite on 127.0.0.1:10000";

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
            // The service version is PINNED. A default BlobClientOptions negotiates the SDK's newest REST
            // version, which Azurite answers with 400 "The API version is not supported by Azurite" -- the
            // second cause behind issue #79, and the reason the Azure suite could not have run honestly
            // even on a machine with Azurite up.
            var options = new BlobClientOptions(BlobClientOptions.ServiceVersion.V2024_11_04);
            options.Retry.MaxRetries = 0;
            options.Retry.NetworkTimeout = TimeSpan.FromSeconds(2);

            var service = new BlobServiceClient(AzuriteConnectionString, options);
            _container = service.GetBlobContainerClient("ew-conf-" + Guid.NewGuid().ToString("N")[..8]);
            await _container.CreateIfNotExistsAsync();
            _fileSystem = new AzureTableFileSystem(_container, "table-root");
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
        if (_container is not null)
        {
            try
            {
                await _container.DeleteIfExistsAsync();
            }
            catch (Exception)
            {
                // Best-effort cleanup.
            }
        }
    }
}
