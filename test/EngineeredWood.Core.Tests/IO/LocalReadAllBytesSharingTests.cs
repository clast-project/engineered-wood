// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.InteropServices;
using EngineeredWood.IO.Local;

namespace EngineeredWood.Core.Tests.IO;

/// <summary>
/// <see cref="LocalTableFileSystem.ReadAllBytesAsync"/> rides out a brief sharing violation, which Windows can
/// raise on a file that was just renamed into place (#960), and still reports one that persists.
/// </summary>
public class LocalReadAllBytesSharingTests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), $"ew-share-{Guid.NewGuid():N}");

    public LocalReadAllBytesSharingTests() => Directory.CreateDirectory(_root);

    public void Dispose()
    {
        if (Directory.Exists(_root))
            Directory.Delete(_root, recursive: true);
    }

    [SkippableFact]
    public async Task BriefSharingViolation_IsRetried()
    {
        Skip.IfNot(RuntimeInformation.IsOSPlatform(OSPlatform.Windows), "sharing violations are a Windows behaviour");
        var path = Path.Combine(_root, "v1.metadata.json");
        File.WriteAllBytes(path, [1, 2, 3]);
        var fs = new LocalTableFileSystem(_root);

        var holder = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
        var release = Task.Run(async () =>
        {
            await Task.Delay(30);
            holder.Dispose();
        });

        Assert.Equal(new byte[] { 1, 2, 3 }, await fs.ReadAllBytesAsync("v1.metadata.json"));
        await release;
    }

    [SkippableFact]
    public async Task PersistentSharingViolation_Throws()
    {
        Skip.IfNot(RuntimeInformation.IsOSPlatform(OSPlatform.Windows), "sharing violations are a Windows behaviour");
        var path = Path.Combine(_root, "held.json");
        File.WriteAllBytes(path, [1]);
        var fs = new LocalTableFileSystem(_root);

        using var holder = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
        await Assert.ThrowsAsync<IOException>(() => fs.ReadAllBytesAsync("held.json").AsTask());
    }

    [Fact]
    public async Task MissingFile_ThrowsAtOnce()
    {
        var fs = new LocalTableFileSystem(_root);
        await Assert.ThrowsAsync<FileNotFoundException>(() => fs.ReadAllBytesAsync("absent.json").AsTask());
    }
}
