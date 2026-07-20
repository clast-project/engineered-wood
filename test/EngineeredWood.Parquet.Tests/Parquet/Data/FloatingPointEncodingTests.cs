// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

#pragma warning disable EWPARQUET0001 // The Alp branch intentionally references the experimental enum values.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.Tests.Parquet.Data;

/// <summary>
/// End-to-end tests covering each <see cref="FloatingPointEncoding"/> option for V2 data pages.
/// </summary>
public class FloatingPointEncodingTests : IDisposable
{
    private readonly string _tempDir;

    public FloatingPointEncodingTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-fpe-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    [Theory]
    [InlineData(FloatingPointEncoding.ByteStreamSplit, Encoding.ByteStreamSplit)]
    [InlineData(FloatingPointEncoding.Alp, Encoding.Alp)]
    [InlineData(FloatingPointEncoding.Plain, Encoding.Plain)]
    public async Task Double_RoundTripsAndSelectsExpectedEncoding(
        FloatingPointEncoding fpe, Encoding expected)
    {
        var path = Path.Combine(_tempDir, $"double-{fpe}.parquet");
        var values = new double[1500];
        var rng = new Random((int)fpe + 1);
        for (int i = 0; i < values.Length; i++)
            values[i] = (rng.Next(-9999, 9999)) / 100.0;

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", DoubleType.Default, nullable: false))
            .Build();
        var batch = new RecordBatch(schema,
            [new DoubleArray.Builder().AppendRange(values).Build()], values.Length);

        var options = ParquetWriteOptions.Default with
        {
            FloatingPointEncoding = fpe,
            DataPageVersion = DataPageVersion.V2,
            DictionaryEnabled = false,
            Compression = EngineeredWood.Compression.CompressionCodec.Uncompressed,
        };

        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false, options))
        {
            await writer.WriteRowGroupAsync(batch);
            await writer.CloseAsync();
        }

        await using var rf = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(rf, ownsFile: false);

        var meta = await reader.ReadMetadataAsync();
        Assert.Contains(expected, meta.RowGroups[0].Columns[0].MetaData!.Encodings);

        var read = await reader.ReadRowGroupAsync(0);
        var arr = (DoubleArray)read.Column(0);
        for (int i = 0; i < values.Length; i++)
            Assert.Equal(BitConverter.DoubleToInt64Bits(values[i]),
                         BitConverter.DoubleToInt64Bits(arr.GetValue(i)!.Value));
    }
}
