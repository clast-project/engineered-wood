// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.IO;
using EngineeredWood.IO.Local;
using Xunit.Abstractions;

namespace EngineeredWood.Orc.Tests;

public class DiagnosticTests(ITestOutputHelper output)
{
    [Fact]
    public async Task DumpDemo11NoneStructure()
    {
        var path = TestHelpers.GetTestFilePath("demo-11-none.orc");
        await using var reader = await OrcReader.OpenAsync(path);

        output.WriteLine($"Rows: {reader.NumberOfRows}");
        output.WriteLine($"Stripes: {reader.NumberOfStripes}");
        output.WriteLine($"Compression: {reader.Compression}");
        output.WriteLine($"Schema root: {reader.Schema.Kind}");
        output.WriteLine($"Columns:");

        void DumpSchema(OrcSchema schema, string indent = "  ")
        {
            output.WriteLine($"{indent}[{schema.ColumnId}] {schema.Name ?? "(root)"}: {schema.Kind}");
            foreach (var child in schema.Children)
                DumpSchema(child, indent + "  ");
        }
        DumpSchema(reader.Schema);

        for (int i = 0; i < reader.NumberOfStripes; i++)
        {
            var stripe = reader.GetStripe(i);
            output.WriteLine($"\nStripe {i}: offset={stripe.Offset}, rows={stripe.NumberOfRows}");
            output.WriteLine($"  index={stripe.IndexLength}, data={stripe.DataLength}, footer={stripe.FooterLength}");
        }

        // Read stripe footer for stripe 0 to inspect streams
        using var rangeReader = new LocalRandomAccessFile(path);
        var stripe0 = reader.GetStripe(0);
        var stripeStart = (long)stripe0.Offset;
        var totalLen = (long)(stripe0.IndexLength + stripe0.DataLength + stripe0.FooterLength);
        using var stripeOwner = await rangeReader.ReadAsync(new FileRange(stripeStart, totalLen));
        var stripeBytes = stripeOwner.Memory.Span;

        var footerStart = (int)(stripe0.IndexLength + stripe0.DataLength);
        var stripeFooter = EngineeredWood.Orc.Proto.StripeFooter.Parser.ParseFrom(stripeBytes.Slice(footerStart, (int)stripe0.FooterLength));

        output.WriteLine($"\nStripe 0 streams:");
        foreach (var stream in stripeFooter.Streams)
        {
            output.WriteLine($"  col={stream.Column}, kind={stream.Kind}, length={stream.Length}");
        }

        output.WriteLine($"\nStripe 0 column encodings:");
        for (int i = 0; i < stripeFooter.Columns.Count; i++)
        {
            output.WriteLine($"  col {i}: {stripeFooter.Columns[i].Kind}, dictSize={stripeFooter.Columns[i].DictionarySize}");
        }

        rangeReader.Dispose();
    }

    [Fact]
    public async Task DumpDemo12ZlibStreams()
    {
        var path = TestHelpers.GetTestFilePath("demo-12-zlib.orc");
        await using var reader = await OrcReader.OpenAsync(path);

        using var rangeReader = new LocalRandomAccessFile(path);
        var stripe0 = reader.GetStripe(0);
        var stripeStart = (long)stripe0.Offset;
        var totalLen = (long)(stripe0.IndexLength + stripe0.DataLength + stripe0.FooterLength);
        using var stripeOwner2 = await rangeReader.ReadAsync(new FileRange(stripeStart, totalLen));
        var stripeBytes = stripeOwner2.Memory;

        var footerStart = (int)(stripe0.IndexLength + stripe0.DataLength);
        var compressedFooter = stripeBytes.Span.Slice(footerStart, (int)stripe0.FooterLength);
        var footerBytes = OrcCompression.Decompress(reader.Compression, compressedFooter, (int)reader.CompressionBlockSize);
        var stripeFooter = EngineeredWood.Orc.Proto.StripeFooter.Parser.ParseFrom(footerBytes);

        long streamOffset = 0;
        output.WriteLine("Stripe 0 streams (with offsets):");
        output.WriteLine($"  indexLength={stripe0.IndexLength}, dataLength={stripe0.DataLength}");
        foreach (var s in stripeFooter.Streams)
        {
            output.WriteLine($"  offset={streamOffset,8} col={s.Column} kind={s.Kind,-20} length={s.Length}");
            streamOffset += (long)s.Length;
        }
        output.WriteLine($"\nColumn encodings:");
        for (int i = 0; i < stripeFooter.Columns.Count; i++)
        {
            output.WriteLine($"  col {i}: {stripeFooter.Columns[i].Kind}, dictSize={stripeFooter.Columns[i].DictionarySize}");
        }

        // Try to decompress the DATA stream for col 2 (first dict-encoded string)
        streamOffset = 0;
        foreach (var s in stripeFooter.Streams)
        {
            if ((int)s.Column == 2 && s.Kind == EngineeredWood.Orc.Proto.Stream.Types.Kind.Data)
            {
                output.WriteLine($"\nCol 2 DATA stream: offset={streamOffset}, length={s.Length}");
                var raw = stripeBytes.Span.Slice((int)streamOffset, (int)s.Length).ToArray();
                output.WriteLine($"  First 16 bytes (hex): {BitConverter.ToString(raw, 0, Math.Min(16, raw.Length))}");
                try
                {
                    var decompressed = OrcCompression.Decompress(reader.Compression, raw, (int)reader.CompressionBlockSize);
                    output.WriteLine($"  Decompressed length: {decompressed.Length}");
                    output.WriteLine($"  First 16 bytes: {BitConverter.ToString(decompressed, 0, Math.Min(16, decompressed.Length))}");
                }
                catch (Exception ex)
                {
                    output.WriteLine($"  Decompression failed: {ex.Message}");
                }
            }
            streamOffset += (long)s.Length;
        }
    }

    [Fact]
    public async Task CountRleValues_Demo12Zlib()
    {
        var path = TestHelpers.GetTestFilePath("demo-12-zlib.orc");
        await using var reader = await OrcReader.OpenAsync(path);

        using var rangeReader = new LocalRandomAccessFile(path);
        var stripe0 = reader.GetStripe(0);
        var stripeStart = (long)stripe0.Offset;
        var totalLen = (long)(stripe0.IndexLength + stripe0.DataLength + stripe0.FooterLength);
        using var stripeOwner3 = await rangeReader.ReadAsync(new FileRange(stripeStart, totalLen));
        var stripeBytes = stripeOwner3.Memory;

        var footerStart = (int)(stripe0.IndexLength + stripe0.DataLength);
        var compressedFooter = stripeBytes.Span.Slice(footerStart, (int)stripe0.FooterLength);
        var footerBytes = OrcCompression.Decompress(reader.Compression, compressedFooter, (int)reader.CompressionBlockSize);
        var stripeFooter = EngineeredWood.Orc.Proto.StripeFooter.Parser.ParseFrom(footerBytes);

        // Find col 1 DATA stream
        long streamOffset = 0;
        foreach (var s in stripeFooter.Streams)
        {
            if ((int)s.Column == 1 && s.Kind == EngineeredWood.Orc.Proto.Stream.Types.Kind.Data)
            {
                output.WriteLine($"Col 1 DATA: offset={streamOffset}, length={s.Length}");
                var raw = stripeBytes.Span.Slice((int)streamOffset, (int)s.Length).ToArray();
                var decompressed = OrcCompression.Decompress(reader.Compression, raw, (int)reader.CompressionBlockSize);
                output.WriteLine($"Decompressed length: {decompressed.Length} bytes");

                // Count values by decoding
                var ms = new MemoryStream(decompressed);
                long totalValues = 0;
                try
                {
                    while (ms.Position < ms.Length)
                    {
                        int firstByte = ms.ReadByte();
                        if (firstByte < 0) break;
                        int encodingType = (firstByte >> 6) & 0x03;
                        int secondByte = ms.ReadByte();

                        int length = ((firstByte & 0x01) << 8) | secondByte;
                        length += 1;

                        if (encodingType == 0) // ShortRepeat
                        {
                            int w = ((firstByte >> 3) & 0x07) + 1;
                            int count = (firstByte & 0x07) + 3;
                            // Put back secondByte since ShortRepeat only uses 1 header byte
                            ms.Position -= 1;
                            // Read W value bytes
                            ms.Position += w;
                            totalValues += count;
                        }
                        else if (encodingType == 1) // Direct
                        {
                            int encodedWidth = (firstByte >> 1) & 0x1F;
                            int bitWidth = EngineeredWood.Orc.Encodings.WidthEncoding.Decode(encodedWidth);
                            int dataBytes = (bitWidth * length + 7) / 8;
                            ms.Position += dataBytes;
                            totalValues += length;
                        }
                        else if (encodingType == 3) // Delta
                        {
                            // Skip - too complex to parse manually here
                            output.WriteLine($"  Delta at pos {ms.Position - 2}, will stop counting");
                            break;
                        }
                        else // PatchedBase
                        {
                            output.WriteLine($"  PatchedBase at pos {ms.Position - 2}, will stop counting");
                            break;
                        }
                    }
                    output.WriteLine($"Total values counted (partial): {totalValues}");
                }
                catch (Exception ex)
                {
                    output.WriteLine($"Counting failed at value {totalValues}: {ex.Message}");
                }
            }
            streamOffset += (long)s.Length;
        }

        output.WriteLine($"Expected rows: {stripe0.NumberOfRows}");
    }

    [Fact]
    public async Task DumpDemo12ZlibStructure()
    {
        var path = TestHelpers.GetTestFilePath("demo-12-zlib.orc");
        await using var reader = await OrcReader.OpenAsync(path);

        output.WriteLine($"Rows: {reader.NumberOfRows}");
        output.WriteLine($"Stripes: {reader.NumberOfStripes}");
        output.WriteLine($"Compression: {reader.Compression}");
        output.WriteLine($"CompressionBlockSize: {reader.CompressionBlockSize}");

        void DumpSchema(OrcSchema schema, string indent = "  ")
        {
            output.WriteLine($"{indent}[{schema.ColumnId}] {schema.Name ?? "(root)"}: {schema.Kind}");
            foreach (var child in schema.Children)
                DumpSchema(child, indent + "  ");
        }
        DumpSchema(reader.Schema);

        for (int i = 0; i < reader.NumberOfStripes; i++)
        {
            var stripe = reader.GetStripe(i);
            output.WriteLine($"\nStripe {i}: offset={stripe.Offset}, rows={stripe.NumberOfRows}");
            output.WriteLine($"  index={stripe.IndexLength}, data={stripe.DataLength}, footer={stripe.FooterLength}");
        }
    }

    [Fact]
    public async Task DumpTest1Structure()
    {
        var path = TestHelpers.GetTestFilePath("TestOrcFile.test1.orc");
        await using var reader = await OrcReader.OpenAsync(path);

        output.WriteLine($"Rows: {reader.NumberOfRows}");
        output.WriteLine($"Stripes: {reader.NumberOfStripes}");
        output.WriteLine($"Compression: {reader.Compression}");

        void DumpSchema(OrcSchema schema, string indent = "  ")
        {
            output.WriteLine($"{indent}[{schema.ColumnId}] {schema.Name ?? "(root)"}: {schema.Kind}");
            foreach (var child in schema.Children)
                DumpSchema(child, indent + "  ");
        }
        DumpSchema(reader.Schema);
    }
}
