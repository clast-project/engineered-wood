// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>Reads whose native memory is counted (<c>NativeMemoryTracker</c> is process-wide): none may run beside them.</summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class NativeMemoryCollection
{
    public const string Name = "Native memory";
}
