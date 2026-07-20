// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

#if NETSTANDARD2_0
using System.Buffers;

namespace System.Buffers;

/// <summary>
/// Minimal polyfill for <see cref="SequenceReader{T}"/> on netstandard2.0.
/// Only implements the subset used by OcfReaderAsync: constructor, TryRead, Position.
/// </summary>
internal ref struct SequenceReader<T> where T : unmanaged, IEquatable<T>
{
    private ReadOnlySequence<T> _sequence;
    private SequencePosition _position;
    private ReadOnlyMemory<T> _currentSegment;
    private int _currentIndex;
    private long _consumed;

    public SequenceReader(ReadOnlySequence<T> sequence)
    {
        _sequence = sequence;
        _position = sequence.Start;
        _consumed = 0;
        _currentSegment = default;
        _currentIndex = 0;

        if (sequence.TryGet(ref _position, out _currentSegment, advance: false))
        {
            _currentIndex = 0;
        }
    }

    public SequencePosition Position => _sequence.GetPosition(_consumed);
    public long Consumed => _consumed;

    public bool TryRead(out T value)
    {
        while (true)
        {
            if (_currentIndex < _currentSegment.Length)
            {
                value = _currentSegment.Span[_currentIndex];
                _currentIndex++;
                _consumed++;
                return true;
            }

            // Move to next segment
            if (!_sequence.TryGet(ref _position, out _currentSegment, advance: true))
            {
                value = default;
                return false;
            }
            _currentIndex = 0;
        }
    }
}
#endif
