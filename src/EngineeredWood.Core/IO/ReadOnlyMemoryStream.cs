// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.InteropServices;

namespace EngineeredWood.IO;

/// <summary>
/// Wraps a <see cref="ReadOnlyMemory{T}"/> as a readable <see cref="Stream"/> for the upload APIs that
/// take one: <c>AWSSDK.S3</c> (<c>PutObjectRequest.InputStream</c>), <c>Google.Cloud.Storage</c>
/// (<c>UploadObject</c>), and <c>Azure.Storage.Files.DataLake</c> (<c>Append</c> / <c>Upload</c>).
/// </summary>
/// <remarks>
/// <c>Azure.Storage.Blobs</c> is deliberately absent. Its upload takes a <c>BinaryData</c>, which wraps a
/// <see cref="ReadOnlyMemory{T}"/> directly with no stream and no copy, so that backend is better off not
/// using this — it is not an inconsistency to tidy up.
/// </remarks>
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
