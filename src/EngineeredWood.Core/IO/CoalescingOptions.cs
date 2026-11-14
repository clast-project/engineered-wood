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
    /// Maximum total size in bytes of a single merged request. A single range larger than this is still read
    /// in one request.
    /// Default: 16 MiB.
    /// </summary>
    /// <remarks>
    /// Was 64 MiB. Measured in #876 on scans of 12M rows in 2 files, 16 reads at once: with 6M-row row groups a
    /// 64 MiB cap merged a row group into a few large requests that the scan then waited on, and 16 MiB cut the
    /// scan by about a third, both on Azure Blob from a bandwidth-limited client and in a simulated store with
    /// ample aggregate bandwidth. Tables with 100K-row row groups were unchanged at 16 MiB, and drifted slower at
    /// smaller caps.
    /// </remarks>
    public long MaxRequestBytes { get; init; } = 16 * 1024 * 1024;
}
