// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Compression;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Metadata;

namespace EngineeredWood.Tests.Parquet.Interop;

/// <summary>
/// ALP (encoding 10) against arrow-rs, the only other implementation so far: files written here
/// must decode bit for bit in arrow-rs, and files arrow-rs writes must decode bit for bit here.
/// See <see cref="ArrowRsAlpOracle"/> for why this is a Rust binary for now.
/// </summary>
/// <remarks>
/// Bits, not values: ALP's decode is specified exactly (two multiplications by fixed constants),
/// and exceptions must keep their IEEE 754 bit patterns — NaN payloads and sign included — so any
/// difference at all is a bug on one side or the other.
/// </remarks>
public sealed class AlpArrowRsInteropTests : IDisposable
{
    private const int Rows = 5000;

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ew-alp-rs-" + Guid.NewGuid().ToString("N")[..8]);

    public AlpArrowRsInteropTests() => Directory.CreateDirectory(_dir);

    public void Dispose()
    {
        if (Directory.Exists(_dir))
            Directory.Delete(_dir, recursive: true);
    }

    [SkippableTheory]
    [InlineData(DataPageVersion.V1)]
    [InlineData(DataPageVersion.V2)]
    public async Task ArrowRs_ReadsAlpWrittenHere_BitForBit(DataPageVersion pageVersion)
    {
        var oracle = ArrowRsAlpOracle.Require();
        var (batch, expected) = BuildBatch();
        var path = Path.Combine(_dir, $"ew-{pageVersion}.parquet");

        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false, new ParquetWriteOptions
        {
            FloatingPointEncoding = FloatingPointEncoding.Alp,
            DictionaryEnabled = false,
            DataPageVersion = pageVersion,
            DataPageSize = 8 * 1024,     // several pages per chunk, and partial vectors
            RowGroupMaxRows = 3000,      // and two row groups
            Compression = CompressionCodec.Snappy,
        }))
        {
            await writer.WriteRowGroupAsync(batch);
            await writer.CloseAsync();
        }

        var (encodings, values) = ArrowRsAlpOracle.Read(oracle, path);

        // The data is chosen to suit ALP, so every chunk must actually be ALP; a writer that fell
        // back to PLAIN everywhere would pass the value comparison while testing nothing.
        Assert.Equal(4, encodings.Count);
        Assert.All(encodings, e => Assert.Contains("ALP", e.Value));
        Assert.Equal(expected, values);
    }

    [SkippableFact]
    public async Task AlpWrittenByArrowRs_ReadsHere_BitForBit()
    {
        var oracle = ArrowRsAlpOracle.Require();
        var path = Path.Combine(_dir, "arrow-rs.parquet");
        ArrowRsAlpOracle.Write(oracle, path);
        var (_, expected) = ArrowRsAlpOracle.Read(oracle, path);

        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);

        var metadata = await reader.ReadMetadataAsync();
        Assert.All(metadata.RowGroups.SelectMany(rg => rg.Columns),
            chunk => Assert.Contains(Encoding.Alp, chunk.MetaData!.Encodings));

        var actual = new List<List<string>> { new(), new() };
        await foreach (var batch in reader.ReadAllAsync())
        {
            for (int c = 0; c < 2; c++)
                actual[c].AddRange(Bits(batch.Column(c)));
        }

        Assert.Equal(expected, actual);
    }

    /// <summary>
    /// Decimal-like FLOAT and DOUBLE columns with nulls and, every 500 rows, each kind of exception:
    /// NaN, −0.0, +∞, a value with no decimal form, and a NaN with a payload.
    /// </summary>
    private static (RecordBatch Batch, List<List<string>> Bits) BuildBatch()
    {
        var f = new FloatArray.Builder();
        var d = new DoubleArray.Builder();
        var rng = new Random(7);
        for (int i = 0; i < Rows; i++)
        {
            if (i % 97 == 3)
            {
                f.AppendNull();
                d.AppendNull();
                continue;
            }

            int r = rng.Next(-1_000_000, 1_000_000);
            (float fv, double dv) = (i % 500) switch
            {
                11 => (float.NaN, double.NaN),
                12 => (-0.0f, -0.0),
                13 => (float.PositiveInfinity, double.PositiveInfinity),
                14 => ((float)(Math.PI * i), Math.PI * i),
                15 => (BitConverter.ToSingle(BitConverter.GetBytes(0x7FC0_BEEFu), 0),
                       BitConverter.Int64BitsToDouble(0x7FF8_0000_DEAD_BEEFL)),
                // FLOAT keeps one decimal digit: single precision cannot hold two decimals at this
                // magnitude exactly, and every value would be an exception.
                _ => ((float)(r / 100 / 10.0), r / 100.0),
            };
            f.Append(fv);
            d.Append(dv);
        }

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("f", FloatType.Default, nullable: true))
            .Field(new Field("d", DoubleType.Default, nullable: true))
            .Build();
        IArrowArray floats = f.Build(), doubles = d.Build();
        return (new RecordBatch(schema, [floats, doubles], Rows), [Bits(floats).ToList(), Bits(doubles).ToList()]);
    }

    /// <summary>Each value's IEEE 754 bits in lowercase hex, in the oracle's format.</summary>
    private static IEnumerable<string> Bits(IArrowArray column)
    {
        for (int i = 0; i < column.Length; i++)
        {
            yield return column.IsNull(i) ? "null" : column switch
            {
                FloatArray a => BitConverter.ToUInt32(BitConverter.GetBytes(a.GetValue(i)!.Value), 0).ToString("x8"),
                DoubleArray a => BitConverter.DoubleToInt64Bits(a.GetValue(i)!.Value).ToString("x16"),
                _ => throw new NotSupportedException(column.GetType().Name),
            };
        }
    }
}
