// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.IO;
using EngineeredWood.IO.Local;

namespace EngineeredWood.Tests.IO;

/// <summary>
/// <see cref="TableFileSystemPathConformanceTests"/> against <see cref="LocalTableFileSystem"/>.
/// </summary>
/// <remarks>
/// This is the leg that always runs, and it earns its place twice over. It is the only backend that
/// declares real <see cref="PathNameConstraints"/>, so it is the only one exercising the skip path -- on
/// Windows it reports <see cref="PathNameConstraints.Win32"/> and therefore declines <c>?</c>, which is
/// exactly the difference a portable partition spelling exists to absorb. And on the <c>net472</c> leg its
/// relative-path helper goes through <see cref="Uri"/> and <c>Uri.UnescapeDataString</c>, which is the same
/// double-decode hazard the cloud backends have on the wire, reached without a socket.
/// </remarks>
public sealed class LocalTableFileSystemPathConformanceTests : TableFileSystemPathConformanceTests
{
    private readonly string _root =
        Path.Combine(Path.GetTempPath(), "ew-conformance-" + Guid.NewGuid().ToString("N"));

    private LocalTableFileSystem? _fileSystem;

    /// <inheritdoc/>
    protected override string Emulator => "the local filesystem (needs no emulator)";

    /// <inheritdoc/>
    protected override bool Available => true;

    /// <inheritdoc/>
    protected override string? UnavailableReason => null;

    /// <inheritdoc/>
    protected override ITableFileSystem FileSystem => _fileSystem!;

    /// <inheritdoc/>
    public override Task InitializeAsync()
    {
        Directory.CreateDirectory(_root);
        _fileSystem = new LocalTableFileSystem(_root);
        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    public override Task DisposeAsync()
    {
        try
        {
            if (Directory.Exists(_root))
                Directory.Delete(_root, recursive: true);
        }
        catch (IOException)
        {
            // Best-effort cleanup of a temp directory; a leftover here must not fail the test that
            // already made its assertions.
        }

        return Task.CompletedTask;
    }
}
