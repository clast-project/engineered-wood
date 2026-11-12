// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.Tests.Parquet;

/// <summary>
/// A decoded string column's buffers hold what its values take, not the decoder's guess. The data buffer is sized
/// ahead at 32 bytes a value, and was handed on whole: a column of one-character strings held 32 bytes a row, and
/// a scan's memory budget, which charges buffer lengths, counted every one of them (#829).
/// </summary>
public class ByteArrayDataBufferTests : IDisposable
{
    private const int Rows = 20_000;
    private static readonly string[] Flags = ["A", "N", "R"];

    private readonly string _tempDir =
        Path.Combine(Path.GetTempPath(), "ew-bytearray-data-" + Guid.NewGuid().ToString("N")[..8]);

    public ByteArrayDataBufferTests() => Directory.CreateDirectory(_tempDir);

    public void Dispose()
    {
        try
        {
            Directory.Delete(_tempDir, recursive: true);
        }
        catch (IOException)
        {
        }
    }

    private async Task<string> WriteAsync(bool dictionary)
    {
        var schema = new Apache.Arrow.Schema.Builder().Field(new Field("flag", StringType.Default, false)).Build();
        var values = new StringArray.Builder();
        for (int i = 0; i < Rows; i++)
            values.Append(Flags[i % Flags.Length]);
        string path = Path.Combine(_tempDir, $"flags-{dictionary}.parquet");
        using var batch = new RecordBatch(schema, [values.Build()], Rows);
        await using var output = new LocalSequentialFile(path);
        await using var writer = new ParquetFileWriter(output, ownsFile: false,
            ParquetWriteOptions.Default with { DictionaryEnabled = dictionary });
        await writer.WriteRowGroupAsync(batch);
        await writer.CloseAsync();
        return path;
    }

    [Theory]
    [InlineData(ByteArrayOutputKind.Default, true)]
    [InlineData(ByteArrayOutputKind.Default, false)]
    [InlineData(ByteArrayOutputKind.LargeOffsets, true)]
    [InlineData(ByteArrayOutputKind.LargeOffsets, false)]
    [InlineData(ByteArrayOutputKind.ViewType, true)]
    [InlineData(ByteArrayOutputKind.ViewType, false)]
    public async Task ShortStrings_DecodeIntoBuffersTheirOwnSize(ByteArrayOutputKind output, bool dictionary)
    {
        string path = await WriteAsync(dictionary);
        await using var file = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(file, ownsFile: false, new ParquetReadOptions { ByteArrayOutput = output });

        long rows = 0, bytes = 0;
        await foreach (var batch in reader.ReadAllAsync())
        {
            using (batch)
            {
                var column = batch.Column(0);
                for (int i = 0; i < batch.Length; i++)
                    Assert.Equal(Flags[(rows + i) % Flags.Length], GetString(column, i));
                rows += batch.Length;
                bytes += column.Data.Buffers.Sum(b => (long)b.Length);
            }
        }

        Assert.Equal(Rows, rows);
        // What the values need: offsets (or 16-byte views, which inline a 1-byte string) and one byte a value of
        // data. The old data buffer alone was 32 bytes a row.
        long needed = output switch
        {
            ByteArrayOutputKind.Default => (Rows + 1) * 4L + Rows,
            ByteArrayOutputKind.LargeOffsets => (Rows + 1) * 8L + Rows,
            _ => Rows * 16L,
        };
        Assert.True(bytes <= needed + 1024, $"{output}: buffers hold {bytes} bytes; the values need {needed}");
    }

    private static string? GetString(IArrowArray array, int index) => array switch
    {
        StringArray s => s.GetString(index),
        LargeStringArray l => l.GetString(index),
        StringViewArray v => v.GetString(index),
        _ => throw new InvalidOperationException(array.GetType().Name),
    };
}
