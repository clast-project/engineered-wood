// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Avro;

/// <summary>
/// Zero-copy per-row access to encoded Avro messages produced by <see cref="AvroEncoder"/>.
/// Each row includes the wire format header (fingerprint prefix) followed by the Avro binary payload.
/// </summary>
public sealed class EncodedRows
{
    private readonly byte[] _buffer;
    private readonly int[] _offsets; // _offsets[i] = start of row i, _offsets[Count] = end

    internal EncodedRows(byte[] buffer, int[] offsets)
    {
        _buffer = buffer;
        _offsets = offsets;
    }

    /// <summary>Number of encoded rows.</summary>
    public int Count => _offsets.Length - 1;

    /// <summary>Whether this collection is empty.</summary>
    public bool IsEmpty => Count == 0;

    /// <summary>
    /// Gets the encoded bytes for the specified row (zero-copy slice).
    /// Includes the wire format header and Avro binary payload.
    /// </summary>
    public ReadOnlyMemory<byte> this[int index]
    {
        get
        {
#if NET8_0_OR_GREATER
            ArgumentOutOfRangeException.ThrowIfNegative(index);
            ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(index, Count);
#else
            if (index < 0) throw new ArgumentOutOfRangeException(nameof(index));
            if (index >= Count) throw new ArgumentOutOfRangeException(nameof(index));
#endif
            int start = _offsets[index];
            int length = _offsets[index + 1] - start;
            return _buffer.AsMemory(start, length);
        }
    }

    /// <summary>Iterate all encoded rows.</summary>
    public IEnumerable<ReadOnlyMemory<byte>> AsEnumerable()
    {
        for (int i = 0; i < Count; i++)
            yield return this[i];
    }

    /// <summary>
    /// The full backing buffer containing all encoded rows contiguously.
    /// </summary>
    public ReadOnlyMemory<byte> GetBuffer() => _buffer.AsMemory(0, _offsets[^1]);
}
