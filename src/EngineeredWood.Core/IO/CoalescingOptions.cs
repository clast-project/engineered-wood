// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.IO;

/// <summary>
/// Configuration for <see cref="CoalescingFileReader"/> range-merging behavior.
/// </summary>
public sealed class CoalescingOptions
{
    /// <summary>
    /// Maximum gap in bytes between two ranges that will be merged into a single request.
    /// Default: 1 MB.
    /// </summary>
    public long MaxGapBytes { get; init; } = 1024 * 1024;

    /// <summary>
    /// Maximum total size in bytes of a single merged request.
    /// Default: 64 MB.
    /// </summary>
    public long MaxRequestBytes { get; init; } = 64 * 1024 * 1024;
}
