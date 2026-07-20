// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers;

namespace EngineeredWood.IO;

/// <summary>
/// Decorator over <see cref="IRandomAccessFile"/> that merges nearby byte ranges
/// into larger requests to reduce the number of I/O operations.
/// </summary>
public sealed class CoalescingFileReader : IRandomAccessFile
{
    private readonly IRandomAccessFile _inner;
    private readonly BufferAllocator _allocator;
    private readonly CoalescingOptions _options;

    public CoalescingFileReader(
        IRandomAccessFile inner,
        CoalescingOptions? options = null,
        BufferAllocator? allocator = null)
    {
        _inner = inner;
        _options = options ?? new CoalescingOptions();
        _allocator = allocator ?? PooledBufferAllocator.Default;
    }

    public ValueTask<long> GetLengthAsync(CancellationToken cancellationToken = default) =>
        _inner.GetLengthAsync(cancellationToken);

    public ValueTask<IMemoryOwner<byte>> ReadAsync(
        FileRange range, CancellationToken cancellationToken = default) =>
        _inner.ReadAsync(range, cancellationToken);

    public async ValueTask<IReadOnlyList<IMemoryOwner<byte>>> ReadRangesAsync(
        IReadOnlyList<FileRange> ranges, CancellationToken cancellationToken = default)
    {
        if (ranges.Count == 0)
            return [];

        if (ranges.Count == 1)
        {
            IMemoryOwner<byte> single = await _inner.ReadAsync(ranges[0], cancellationToken)
                .ConfigureAwait(false);
            return [single];
        }

        // Build index-sorted pairs so we can sort by offset and restore original order
        var indexed = new (FileRange Range, int OriginalIndex)[ranges.Count];
        for (int i = 0; i < ranges.Count; i++)
            indexed[i] = (ranges[i], i);

        Array.Sort(indexed, (a, b) => a.Range.Offset.CompareTo(b.Range.Offset));

        // Merge ranges that are within the gap threshold
        var mergedGroups = BuildMergedGroups(indexed);

        // Fetch each merged range individually (in parallel). Using ReadAsync
        // rather than ReadRangesAsync avoids recursion when this coalescer
        // is composed inside another IRandomAccessFile implementation.
        var mergedBuffers = new IMemoryOwner<byte>[mergedGroups.Count];
        if (mergedGroups.Count == 1)
        {
            mergedBuffers[0] = await _inner.ReadAsync(mergedGroups[0].MergedRange, cancellationToken)
                .ConfigureAwait(false);
        }
        else
        {
            var tasks = new Task[mergedGroups.Count];
            for (int i = 0; i < mergedGroups.Count; i++)
            {
                int idx = i;
                tasks[idx] = Task.Run(async () =>
                {
                    mergedBuffers[idx] = await _inner
                        .ReadAsync(mergedGroups[idx].MergedRange, cancellationToken)
                        .ConfigureAwait(false);
                }, cancellationToken);
            }

            try
            {
                await Task.WhenAll(tasks).ConfigureAwait(false);
            }
            catch
            {
                foreach (IMemoryOwner<byte>? buf in mergedBuffers)
                    buf?.Dispose();
                throw;
            }
        }

        // Slice results back into individual buffers in original order
        var results = new IMemoryOwner<byte>[ranges.Count];
        try
        {
            for (int groupIndex = 0; groupIndex < mergedGroups.Count; groupIndex++)
            {
                MergedGroup group = mergedGroups[groupIndex];
                ReadOnlyMemory<byte> mergedMemory = mergedBuffers[groupIndex].Memory;

                foreach ((FileRange range, int originalIndex) in group.Members)
                {
                    int sliceOffset = checked((int)(range.Offset - group.MergedRange.Offset));
                    int sliceLength = checked((int)range.Length);

                    IMemoryOwner<byte> sliceBuffer = _allocator.Allocate(sliceLength);
                    mergedMemory.Slice(sliceOffset, sliceLength).CopyTo(sliceBuffer.Memory);
                    results[originalIndex] = sliceBuffer;
                }
            }

            return results;
        }
        catch
        {
            foreach (IMemoryOwner<byte>? buf in results)
                buf?.Dispose();
            throw;
        }
        finally
        {
            // Dispose the merged buffers — individual slices have been copied out
            foreach (IMemoryOwner<byte> buf in mergedBuffers)
                buf.Dispose();
        }
    }

    private List<MergedGroup> BuildMergedGroups(
        (FileRange Range, int OriginalIndex)[] sorted)
    {
        var groups = new List<MergedGroup>();

        var currentMembers = new List<(FileRange Range, int OriginalIndex)> { sorted[0] };
        FileRange currentMerged = sorted[0].Range;

        for (int i = 1; i < sorted.Length; i++)
        {
            FileRange candidate = sorted[i].Range;
            FileRange potentialMerge = currentMerged.Merge(candidate);

            if (currentMerged.IsWithinGap(candidate, _options.MaxGapBytes) &&
                potentialMerge.Length <= _options.MaxRequestBytes)
            {
                currentMerged = potentialMerge;
                currentMembers.Add(sorted[i]);
            }
            else
            {
                groups.Add(new MergedGroup(currentMerged, currentMembers));
                currentMembers = [sorted[i]];
                currentMerged = candidate;
            }
        }

        groups.Add(new MergedGroup(currentMerged, currentMembers));
        return groups;
    }

    public void Dispose() => _inner.Dispose();

    public ValueTask DisposeAsync() => _inner.DisposeAsync();

    private readonly record struct MergedGroup(
        FileRange MergedRange,
        List<(FileRange Range, int OriginalIndex)> Members);
}
