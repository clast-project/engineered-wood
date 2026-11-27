// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.IO;

namespace EngineeredWood.IO.Aws;

/// <summary>Registers <see cref="S3TableFileSystem"/> instances with a <see cref="StorageResolver"/>.</summary>
public static class S3StorageResolverExtensions
{
    /// <summary>
    /// Registers <paramref name="fileSystem"/> for every location under the one it is rooted at
    /// (<see cref="S3TableFileSystem.Location"/>).
    /// </summary>
    /// <returns>The resolver, so registrations can be chained.</returns>
    public static StorageResolver Register(this StorageResolver resolver, S3TableFileSystem fileSystem)
    {
        if (resolver is null)
            throw new ArgumentNullException(nameof(resolver));
        if (fileSystem is null)
            throw new ArgumentNullException(nameof(fileSystem));
        return resolver.Register(fileSystem.Location, fileSystem);
    }
}
