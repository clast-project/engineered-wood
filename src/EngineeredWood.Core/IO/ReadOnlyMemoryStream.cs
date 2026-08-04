// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.InteropServices;

namespace EngineeredWood.IO;

/// <summary>
/// Wraps a <see cref="ReadOnlyMemory{T}"/> as a readable <see cref="Stream"/> for the cloud SDKs, which
/// take payloads as streams.
/// </summary>
public static class ReadOnlyMemoryStream
{
    /// <summary>
    /// Creates a non-writable, seekable stream over <paramref name="data"/>, without copying when the
    /// memory is already array-backed.
    /// </summary>
    public static Stream Create(ReadOnlyMemory<byte> data)
    {
        // TryGetArray reports success for EMPTY memory while handing back a default ArraySegment whose
        // Array is null, and MemoryStream(null, …) throws. The three cloud backends each had their own
        // copy of this and each dereferenced the segment unchecked, so writing an empty file threw a
        // NullReferenceException from inside the SDK call rather than writing nothing.
        if (MemoryMarshal.TryGetArray(data, out ArraySegment<byte> segment) &&
            segment.Array is not null)
        {
            return new MemoryStream(
                segment.Array, segment.Offset, segment.Count, writable: false, publiclyVisible: false);
        }

        return new MemoryStream(data.ToArray(), writable: false);
    }
}
