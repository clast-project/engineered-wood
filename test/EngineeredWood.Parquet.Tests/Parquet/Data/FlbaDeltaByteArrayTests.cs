// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Data;

namespace EngineeredWood.Tests.Parquet.Data;

/// <summary>
/// DELTA_BYTE_ARRAY is legal for FIXED_LEN_BYTE_ARRAY as well as BYTE_ARRAY, and the writer emits it for
/// both whenever <see cref="ByteArrayEncoding.DeltaByteArray"/> is chosen with V2 pages. The reader could
/// not read the result back: <c>DeltaByteArrayDecoder</c> finished by calling
/// <c>ColumnBuildState.AddByteArrayValues</c>, which writes through the data/offsets buffer pair that the
/// state only allocates for BYTE_ARRAY columns. A fixed-width column reached it with both buffers null and
/// the read died on a <see cref="NullReferenceException"/>.
///
/// So this library wrote files it could not itself read, for every FIXED_LEN_BYTE_ARRAY column there is —
/// DECIMAL(precision &gt; 18), UUID, FLOAT16, and plain fixed binary — with no test covering any of it.
/// </summary>
public sealed class FlbaDeltaByteArrayTests : IDisposable
{
    private readonly string _tempDir;

    public FlbaDeltaByteArrayTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-flba-dba-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            Directory.Delete(_tempDir, recursive: true);
        }

        GC.SuppressFinalize(this);
    }

    private static ParquetWriteOptions DeltaByteArrayOptions => new()
    {
        ByteArrayEncoding = ByteArrayEncoding.DeltaByteArray,
        DataPageVersion = DataPageVersion.V2,
        DictionaryEnabled = false,
    };

    private async Task<RecordBatch> RoundTripAsync(RecordBatch batch)
    {
        var path = Path.Combine(_tempDir, Guid.NewGuid().ToString("N")[..8] + ".parquet");

        await using (var outFile = new LocalSequentialFile(path))
        {
            await using var writer = new ParquetFileWriter(outFile, options: DeltaByteArrayOptions);
            await writer.WriteRowGroupAsync(batch);
        }

        await using var inFile = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(inFile, ownsFile: false);
        return await reader.ReadRowGroupAsync(0);
    }

    private static RecordBatch FixedBinaryBatch(byte[][] values, bool[]? valid = null)
    {
        int width = values[0].Length;
        var type = new FixedSizeBinaryType(width);
        var packed = new byte[values.Length * width];
        for (int i = 0; i < values.Length; i++)
        {
            values[i].CopyTo(packed, i * width);
        }

        var validity = new byte[(values.Length + 7) / 8];
        int nullCount = 0;
        for (int i = 0; i < values.Length; i++)
        {
            if (valid is null || valid[i])
            {
                validity[i / 8] |= (byte)(1 << (i % 8));
            }
            else
            {
                nullCount++;
            }
        }

        var data = new ArrayData(
            type, values.Length, nullCount, 0,
            [new ArrowBuffer(validity), new ArrowBuffer(packed)]);

        var schema = new Apache.Arrow.Schema([new Field("f", type, nullable: valid is not null)], null);
        return new RecordBatch(schema, [new FixedSizeBinaryArray(data)], values.Length);
    }

    private static byte[][] DistinctValues(int count, int width)
    {
        var values = new byte[count][];
        for (int i = 0; i < count; i++)
        {
            values[i] = new byte[width];
            values[i][0] = (byte)i;
            values[i][width - 1] = (byte)(255 - i);
        }

        return values;
    }

    [Theory]
    [InlineData(2)]   // FLOAT16's width
    [InlineData(12)]  // the extended-precision timestamp carrier
    [InlineData(16)]  // DECIMAL128 / UUID
    [InlineData(32)]  // DECIMAL256
    public async Task FixedSizeBinaryRoundTripsAtEveryWidthTheFormatUses(int width)
    {
        var values = DistinctValues(8, width);

        var read = await RoundTripAsync(FixedBinaryBatch(values));

        var array = Assert.IsType<FixedSizeBinaryArray>(read.Column(0));
        Assert.Equal(values.Length, array.Length);
        for (int i = 0; i < values.Length; i++)
        {
            Assert.Equal(values[i], array.GetBytes(i).ToArray());
        }
    }

    [Fact]
    public async Task ValuesSharingAPrefixRoundTrip()
    {
        // The whole point of the encoding is that value N stores only what it does not share with N-1.
        // Distinct-first-byte values leave every prefix length at zero and never exercise reconstruction.
        var values = new byte[6][];
        for (int i = 0; i < values.Length; i++)
        {
            values[i] = [0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x00, 0x11, 0x22, 0x33, 0x44, (byte)i];
        }

        var read = await RoundTripAsync(FixedBinaryBatch(values));

        var array = Assert.IsType<FixedSizeBinaryArray>(read.Column(0));
        for (int i = 0; i < values.Length; i++)
        {
            Assert.Equal(values[i], array.GetBytes(i).ToArray());
        }
    }

    [Fact]
    public async Task NullsRoundTrip()
    {
        var values = DistinctValues(6, 12);
        bool[] valid = [true, false, true, true, false, true];

        var read = await RoundTripAsync(FixedBinaryBatch(values, valid));

        var array = Assert.IsType<FixedSizeBinaryArray>(read.Column(0));
        Assert.Equal(values.Length, array.Length);
        for (int i = 0; i < values.Length; i++)
        {
            Assert.Equal(!valid[i], array.IsNull(i));
            if (valid[i])
            {
                Assert.Equal(values[i], array.GetBytes(i).ToArray());
            }
        }
    }

    [Fact]
    public async Task DecimalsRoundTrip()
    {
        // The blast radius in practice: DECIMAL above precision 18 is carried on FIXED_LEN_BYTE_ARRAY, so
        // this is an ordinary column that a caller could already not read back.
        var type = new Decimal128Type(30, 4);
        var values = new Decimal128Array.Builder(type)
            .Append(12.3456m).Append(-99.9999m).Append(0m).Append(1234567.8901m)
            .Build();

        var schema = new Apache.Arrow.Schema([new Field("d", type, nullable: false)], null);
        var read = await RoundTripAsync(new RecordBatch(schema, [values], values.Length));

        var array = Assert.IsType<Decimal128Array>(read.Column(0));
        Assert.Equal(values.Length, array.Length);
        for (int i = 0; i < values.Length; i++)
        {
            Assert.Equal(values.GetValue(i), array.GetValue(i));
        }
    }

    [Fact]
    public void AValueOfTheWrongWidthIsRejectedRatherThanMisaligned()
    {
        // A malformed file could carry variable-length values on a fixed-width column. The bulk copy the
        // decoder now does would silently shift every later value, so the width is checked per value.
        byte[] joined = [1, 2, 3, 4, 5, 6, 7];
        int[] offsets = [0, 3, 7]; // two values, 3 and 4 bytes wide
        var encoded = new byte[256];
        int written = DeltaByteArrayEncoder.Encode(offsets, joined, 0, 2, 2, null, encoded);

        using var state = new ColumnBuildState(PhysicalType.FixedLenByteArray, 0, 0, capacity: 8);

        var error = Assert.Throws<ParquetFormatException>(
            () => DeltaByteArrayDecoder.Decode(encoded.AsSpan(0, written), 2, state, typeLength: 4));
        Assert.Contains("FIXED_LEN_BYTE_ARRAY(4)", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AMissingTypeLengthIsRejected()
    {
        var encoded = new byte[256];
        int written = DeltaByteArrayEncoder.EncodeFixed(new byte[8], 4, 0, 2, 2, null, encoded);

        using var state = new ColumnBuildState(PhysicalType.FixedLenByteArray, 0, 0, capacity: 8);

        Assert.Throws<ParquetFormatException>(
            () => DeltaByteArrayDecoder.Decode(encoded.AsSpan(0, written), 2, state, typeLength: 0));
    }
}
