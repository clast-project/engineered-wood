// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.IO;

namespace EngineeredWood.Tests.IO;

/// <summary>
/// <para>The <see cref="IRandomAccessFile"/> contract for a range past the end of the file, held against every
/// backend: it throws <see cref="IOException"/>, whether the range STRADDLES the end or STARTS at or beyond
/// it.</para>
///
/// <para>The two are different on the wire. An object store answers a straddling range with a short 206, which
/// the adapters' short-read check turns into an <see cref="IOException"/>; it answers a range starting at or
/// past the end with HTTP 416, which each SDK raises as its own exception type. A caller that tells a
/// malformed offset from a storage failure by catching <see cref="IOException"/> (the Delta deletion-vector
/// reader does, to report <c>DELTA_INVALID_DELETION_VECTOR</c>) saw a 416 escape as
/// <c>AmazonS3Exception</c>, <c>RequestFailedException</c> or <c>GoogleApiException</c> (PR #628).</para>
/// </summary>
public abstract partial class TableFileSystemPathConformanceTests
{
    private const string RangeFile = "ranges/part-00000.parquet";

    public static TheoryData<string, long, long> RangesPastTheEnd =>
        new()
        {
            { "straddles the end", PayloadLength - 10, 20 },
            { "starts at the end", PayloadLength, 8 },
            { "starts beyond the end", PayloadLength + 100, 8 },
        };

    [SkippableTheory]
    [MemberData(nameof(RangesPastTheEnd))]
    public async Task RangedRead_PastTheEnd_ThrowsIOException(string label, long offset, long length)
    {
        Require();
        var fs = FileSystem;
        await fs.WriteAllBytesAsync(RangeFile, PayloadFor(RangeFile));

        await using var handle = await fs.OpenReadAsync(RangeFile);
        Assert.Equal(PayloadLength, await handle.GetLengthAsync());

        var ex = await Record.ExceptionAsync(async () => (await handle.ReadAsync(new FileRange(offset, length))).Dispose());

        Assert.True(ex is IOException, $"[{label}] expected an IOException, got {ex?.GetType().FullName ?? "no exception"}: {ex?.Message}");
    }
}
