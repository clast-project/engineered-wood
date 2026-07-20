// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.IO;
using EngineeredWood.IO.Local;
using Xunit.Abstractions;

namespace EngineeredWood.Orc.Tests;

public class ColumnReadTests(ITestOutputHelper output)
{
    [Fact]
    public async Task CanReadSingleIntColumn_Demo11None()
    {
        var path = TestHelpers.GetTestFilePath("demo-11-none.orc");
        await using var reader = await OrcReader.OpenAsync(path);

        // Read just the first column (_col0 which is Int)
        var rowReader = reader.CreateRowReader(new OrcReaderOptions
        {
            Columns = ["_col0"],
            BatchSize = 100
        });

        int batchCount = 0;
        long totalRows = 0;
        await foreach (var batch in rowReader)
        {
            totalRows += batch.Length;
            batchCount++;
            if (batchCount == 1)
            {
                output.WriteLine($"First batch: {batch.Length} rows, {batch.ColumnCount} columns");
                var col = batch.Column(0) as Apache.Arrow.Int32Array;
                if (col != null)
                {
                    for (int i = 0; i < Math.Min(5, col.Length); i++)
                    {
                        output.WriteLine($"  [{i}] = {col.GetValue(i)}");
                    }
                }
            }
            if (batchCount >= 3) break; // Just test first few batches
        }

        output.WriteLine($"Read {totalRows} rows in {batchCount} batches");
        Assert.True(totalRows > 0);
    }

    [Fact]
    public async Task CanReadAllIntBatches_Demo12Zlib()
    {
        var path = TestHelpers.GetTestFilePath("demo-12-zlib.orc");
        await using var reader = await OrcReader.OpenAsync(path);

        var rowReader = reader.CreateRowReader(new OrcReaderOptions
        {
            Columns = ["_col0"],
            BatchSize = 1024
        });

        int batchCount = 0;
        long totalRows = 0;
        try
        {
            await foreach (var batch in rowReader)
            {
                totalRows += batch.Length;
                batchCount++;
            }
        }
        catch (Exception ex)
        {
            output.WriteLine($"Failed after {batchCount} batches ({totalRows} rows): {ex.Message}");
            output.WriteLine($"Expected rows: {reader.NumberOfRows}");
            throw;
        }
        output.WriteLine($"Read {totalRows} rows in {batchCount} batches");
        Assert.Equal(reader.NumberOfRows, totalRows);
    }

    [Theory]
    [InlineData("_col0")]
    [InlineData("_col1")]
    [InlineData("_col2")]
    [InlineData("_col3")]
    [InlineData("_col4")]
    [InlineData("_col5")]
    [InlineData("_col6")]
    [InlineData("_col7")]
    [InlineData("_col8")]
    public async Task CanReadEachColumn_Demo12Zlib(string colName)
    {
        var path = TestHelpers.GetTestFilePath("demo-12-zlib.orc");
        await using var reader = await OrcReader.OpenAsync(path);

        var rowReader = reader.CreateRowReader(new OrcReaderOptions
        {
            Columns = [colName],
            BatchSize = 1024
        });

        long totalRows = 0;
        int batchCount = 0;
        try
        {
            await foreach (var batch in rowReader)
            {
                totalRows += batch.Length;
                batchCount++;
            }
        }
        catch (Exception ex)
        {
            output.WriteLine($"Column {colName} failed after {batchCount} batches ({totalRows} rows): {ex.Message}");
            throw;
        }
        output.WriteLine($"Column {colName}: {totalRows} rows OK");
        Assert.Equal(reader.NumberOfRows, totalRows);
    }

    [Theory]
    [InlineData("_col3")]
    [InlineData("_col4")]
    public async Task DebugFailingColumn(string colName)
    {
        var path = TestHelpers.GetTestFilePath("demo-12-zlib.orc");
        await using var reader = await OrcReader.OpenAsync(path);

        using var file = new LocalRandomAccessFile(path);
        var stripe0 = reader.GetStripe(0);
        var stripeStart = (long)stripe0.Offset;
        var totalLen = (long)(stripe0.IndexLength + stripe0.DataLength + stripe0.FooterLength);
        using var stripeOwner = await file.ReadAsync(new FileRange(stripeStart, totalLen));
        var stripeBytes = stripeOwner.Memory;

        var footerStart = (int)(stripe0.IndexLength + stripe0.DataLength);
        var compressedFooter = stripeBytes.Span.Slice(footerStart, (int)stripe0.FooterLength);
        var footerBytes = OrcCompression.Decompress(reader.Compression, compressedFooter, (int)reader.CompressionBlockSize);
        var stripeFooter = EngineeredWood.Orc.Proto.StripeFooter.Parser.ParseFrom(footerBytes);

        // Find the column's schema
        var col = reader.Schema.Children.First(c => c.Name == colName);
        output.WriteLine($"Column {colName}: id={col.ColumnId}, kind={col.Kind}");
        output.WriteLine($"Encoding: {stripeFooter.Columns[col.ColumnId].Kind}");

        // Find and decompress the DATA stream
        long streamOffset = 0;
        foreach (var s in stripeFooter.Streams)
        {
            if ((int)s.Column == col.ColumnId && s.Kind == EngineeredWood.Orc.Proto.Stream.Types.Kind.Data)
            {
                var raw = stripeBytes.Span.Slice((int)streamOffset, (int)s.Length).ToArray();
                var decompressed = OrcCompression.Decompress(reader.Compression, raw, (int)reader.CompressionBlockSize);
                output.WriteLine($"DATA stream: compressed={s.Length}, decompressed={decompressed.Length}");

                // Count sub-encoding types
                var ms = new MemoryStream(decompressed);
                int shortRepeat = 0, direct = 0, patchedBase = 0, delta = 0;
                while (ms.Position < ms.Length)
                {
                    int fb = ms.ReadByte();
                    if (fb < 0) break;
                    int type = (fb >> 6) & 3;
                    switch (type)
                    {
                        case 0: shortRepeat++; break;
                        case 1: direct++; break;
                        case 2: patchedBase++; break;
                        case 3: delta++; break;
                    }
                    // Can't easily skip without full parsing, so just count first bytes
                    // Reset to start and use the decoder to count values
                    break;
                }

                // Try the actual decoder to read exactly the expected number of values
                bool isSigned = col.Kind == EngineeredWood.Orc.Proto.Type.Types.Kind.Int;
                var decoder = new EngineeredWood.Orc.Encodings.RleDecoderV2(new EngineeredWood.Orc.Encodings.OrcByteStream(decompressed, 0, decompressed.Length), signed: isSigned);
                long valuesRead = 0;
                long expected = (long)stripe0.NumberOfRows;
                try
                {
                    while (valuesRead < expected)
                    {
                        int batchSize = (int)Math.Min(1024, expected - valuesRead);
                        var batch = new long[batchSize];
                        decoder.ReadValues(batch);
                        valuesRead += batchSize;
                    }
                    output.WriteLine($"Stream pos after reading all: {ms.Position}/{ms.Length}");
                }
                catch (Exception ex)
                {
                    output.WriteLine($"Decoder failed after {valuesRead} values at stream pos {ms.Position}/{ms.Length}: {ex.Message}");
                }
                output.WriteLine($"First sub-encoding type: SR={shortRepeat}, D={direct}, PB={patchedBase}, Delta={delta}");
                output.WriteLine($"Total values decoded: {valuesRead}");
                output.WriteLine($"Expected: {expected}");
            }
            streamOffset += (long)s.Length;
        }
    }

    [Fact]
    public async Task CanReadDictStringColumn_Demo12Zlib()
    {
        var path = TestHelpers.GetTestFilePath("demo-12-zlib.orc");
        await using var reader = await OrcReader.OpenAsync(path);

        // _col1 is string with dictionary encoding
        var rowReader = reader.CreateRowReader(new OrcReaderOptions
        {
            Columns = ["_col1"],
            BatchSize = 100
        });

        int batchCount = 0;
        await foreach (var batch in rowReader)
        {
            batchCount++;
            if (batchCount == 1)
            {
                var col = batch.Column(0) as Apache.Arrow.StringArray;
                if (col != null)
                {
                    for (int i = 0; i < Math.Min(5, col.Length); i++)
                        output.WriteLine($"  [{i}] = '{col.GetString(i)}'");
                }
            }
            if (batchCount >= 3) break;
        }
        output.WriteLine($"Read {batchCount} batches");
        Assert.True(batchCount > 0);
    }

    [Fact]
    public async Task CanReadSingleStringColumn_Demo12Zlib()
    {
        var path = TestHelpers.GetTestFilePath("demo-12-zlib.orc");
        await using var reader = await OrcReader.OpenAsync(path);

        output.WriteLine($"Schema columns:");
        foreach (var col in reader.Schema.Children)
            output.WriteLine($"  [{col.ColumnId}] {col.Name}: {col.Kind}");

        // Read just the first column
        var firstName = reader.Schema.Children[0].Name;
        output.WriteLine($"\nReading column: {firstName}");

        var rowReader = reader.CreateRowReader(new OrcReaderOptions
        {
            Columns = [firstName!],
            BatchSize = 100
        });

        int batchCount = 0;
        long totalRows = 0;
        await foreach (var batch in rowReader)
        {
            totalRows += batch.Length;
            batchCount++;
            if (batchCount == 1)
            {
                output.WriteLine($"First batch: {batch.Length} rows, {batch.ColumnCount} columns");
            }
            if (batchCount >= 3) break;
        }

        output.WriteLine($"Read {totalRows} rows in {batchCount} batches");
        Assert.True(totalRows > 0);
    }
}
