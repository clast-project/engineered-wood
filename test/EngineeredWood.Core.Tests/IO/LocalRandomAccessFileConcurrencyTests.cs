// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;

namespace EngineeredWood.Core.Tests.IO;

/// <summary>
/// One <see cref="LocalRandomAccessFile"/> read from many threads at once, as a parallel row-group read
/// (#612) does. On net472 the file seeks a shared <see cref="FileStream"/>, so unsynchronized reads would
/// hand each other's bytes back; on net6+ the reads are positional. Every 4-byte word of the file holds its
/// own index, so a read from the wrong offset is caught by its contents.
/// </summary>
public class LocalRandomAccessFileConcurrencyTests
{
    private const int Words = 1 << 20; // 4 MiB

    [Fact]
    public async Task ConcurrentReadsReturnTheirOwnRanges()
    {
        string path = Path.Combine(Path.GetTempPath(), $"ew-concurrent-{Guid.NewGuid():N}.bin");
        try
        {
            var bytes = new byte[Words * 4];
            for (int i = 0; i < Words; i++)
                BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(i * 4), i);
            File.WriteAllBytes(path, bytes);

            using var file = new LocalRandomAccessFile(path);
            var tasks = new Task[Math.Max(4, Environment.ProcessorCount)];
            for (int t = 0; t < tasks.Length; t++)
            {
                int seed = t;
                tasks[t] = Task.Run(async () =>
                {
                    var random = new Random(seed);
                    for (int r = 0; r < 500; r++)
                    {
                        int firstWord = random.Next(Words - 1024);
                        int wordCount = 1 + random.Next(1024);
                        if (r % 2 == 0)
                        {
                            using var owner = await file.ReadAsync(new FileRange(firstWord * 4L, wordCount * 4L));
                            AssertWords(owner.Memory.Span, firstWord);
                        }
                        else
                        {
                            int second = random.Next(Words - wordCount);
                            var owners = await file.ReadRangesAsync(
                            [
                                new FileRange(firstWord * 4L, wordCount * 4L),
                                new FileRange(second * 4L, wordCount * 4L),
                            ]);
                            try
                            {
                                AssertWords(owners[0].Memory.Span, firstWord);
                                AssertWords(owners[1].Memory.Span, second);
                            }
                            finally
                            {
                                foreach (var owner in owners)
                                    owner.Dispose();
                            }
                        }
                    }
                });
            }

            await Task.WhenAll(tasks);
        }
        finally
        {
            File.Delete(path);
        }
    }

    private static void AssertWords(ReadOnlySpan<byte> span, int firstWord)
    {
        for (int i = 0; i < span.Length / 4; i++)
        {
            int word = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(i * 4));
            if (word != firstWord + i)
                Assert.Fail($"Word {i} of a read at word {firstWord} held {word}.");
        }
    }
}
