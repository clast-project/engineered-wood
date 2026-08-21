// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Compression;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.Tests.Parquet.Data;

/// <summary>
/// A dictionary entry is a set of BYTES, so two values share one exactly when their encoded bytes match.
/// Keying the dictionary on the VALUE instead merged +0.0 with -0.0 — <c>IEquatable&lt;double&gt;.Equals</c>
/// calls them equal — and every index for the second zero pointed at the first one's bytes, so whichever
/// zero appeared first won and the other was silently rewritten on disk (issue #154).
///
/// Every assertion here is on the raw BITS: <c>-0.0 == 0.0</c> compares true, so an equality assertion
/// passes against the corrupt file too.
/// </summary>
public class SignedZeroDictionaryTests : IDisposable
{
    private readonly string _tempDir;

    public SignedZeroDictionaryTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-signedzero-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private string TempPath(string name) => Path.Combine(_tempDir, name);

    // 200 rows of two distinct values is 1% cardinality, comfortably inside the 20% threshold, so the
    // dictionary path is genuinely taken — and asserted, because the issue's own two-row repro now falls
    // OUT of dictionary encoding once the zeros count as two entries (maxCardinality is 1 at two rows).
    private const int Rows = 200;

    [Fact]
    public async Task Double_AlternatingSignedZeros_KeepTheirSignThroughTheDictionary()
    {
        string path = TempPath("double_signed_zeros.parquet");

        var builder = new DoubleArray.Builder();
        for (int i = 0; i < Rows; i++)
            builder.Append(i % 2 == 0 ? 0.0 : -0.0);

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("d", DoubleType.Default, nullable: false)).Build();
        await WriteAsync(path, new RecordBatch(schema, [builder.Build()], Rows));

        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        var metadata = await reader.ReadMetadataAsync();

        Assert.Contains(EngineeredWood.Parquet.Encoding.RleDictionary,
            metadata.RowGroups[0].Columns[0].MetaData!.Encodings);

        var read = (DoubleArray)(await reader.ReadRowGroupAsync(0)).Column(0);
        Assert.Equal(Rows, read.Length);
        for (int i = 0; i < Rows; i++)
        {
            long expected = BitPatterns.Of(i % 2 == 0 ? 0.0 : -0.0);
            Assert.Equal(expected, BitPatterns.Of(read.GetValue(i)!.Value));
        }
    }

    [Fact]
    public async Task Float_AlternatingSignedZeros_KeepTheirSignThroughTheDictionary()
    {
        string path = TempPath("float_signed_zeros.parquet");

        var builder = new FloatArray.Builder();
        for (int i = 0; i < Rows; i++)
            builder.Append(i % 2 == 0 ? 0.0f : -0.0f);

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("f", FloatType.Default, nullable: false)).Build();
        await WriteAsync(path, new RecordBatch(schema, [builder.Build()], Rows));

        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        var metadata = await reader.ReadMetadataAsync();

        Assert.Contains(EngineeredWood.Parquet.Encoding.RleDictionary,
            metadata.RowGroups[0].Columns[0].MetaData!.Encodings);

        var read = (FloatArray)(await reader.ReadRowGroupAsync(0)).Column(0);
        Assert.Equal(Rows, read.Length);
        for (int i = 0; i < Rows; i++)
        {
            int expected = BitPatterns.Of(i % 2 == 0 ? 0.0f : -0.0f);
            Assert.Equal(expected, BitPatterns.Of(read.GetValue(i)!.Value));
        }
    }

    // BufferedParquetWriter keeps its own Dictionary&lt;float,int&gt; / Dictionary&lt;double,int&gt; and
    // never reaches DictionaryEncoder, so it carried the same defect independently of the one the issue
    // names.
    [Fact]
    public async Task BufferedWriter_AlternatingSignedZeros_KeepTheirSign()
    {
        string path = TempPath("buffered_signed_zeros.parquet");

        var doubles = new DoubleArray.Builder();
        var floats = new FloatArray.Builder();
        for (int i = 0; i < Rows; i++)
        {
            doubles.Append(i % 2 == 0 ? 0.0 : -0.0);
            floats.Append(i % 2 == 0 ? 0.0f : -0.0f);
        }

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("d", DoubleType.Default, nullable: false))
            .Field(new Field("f", FloatType.Default, nullable: false))
            .Build();
        var batch = new RecordBatch(schema, [doubles.Build(), floats.Build()], Rows);

        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new BufferedParquetWriter(file, ownsFile: false,
            new ParquetWriteOptions { Compression = CompressionCodec.Uncompressed }))
        {
            await writer.AppendAsync(batch);
            await writer.CloseAsync();
        }

        await using var readFile = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(readFile, ownsFile: false);
        var metadata = await reader.ReadMetadataAsync();

        // Same guard as the two writer tests above: the defect is in the dictionary, so the case is only
        // exercising it while the dictionary is what got chosen. BufferedParquetWriter decides that at
        // FLUSH time from the accumulated cardinality, so this is a heuristic here too.
        for (int c = 0; c < 2; c++)
        {
            Assert.Contains(EngineeredWood.Parquet.Encoding.RleDictionary,
                metadata.RowGroups[0].Columns[c].MetaData!.Encodings);
        }

        var read = await reader.ReadRowGroupAsync(0);

        var d = (DoubleArray)read.Column(0);
        var f = (FloatArray)read.Column(1);
        for (int i = 0; i < Rows; i++)
        {
            Assert.Equal(BitPatterns.Of(i % 2 == 0 ? 0.0 : -0.0),
                BitPatterns.Of(d.GetValue(i)!.Value));
            Assert.Equal(BitPatterns.Of(i % 2 == 0 ? 0.0f : -0.0f),
                BitPatterns.Of(f.GetValue(i)!.Value));
        }
    }

    // The issue's minimised repro, kept verbatim. At two and three rows maxCardinality is 1, so once the
    // zeros are two entries these fall OUT of dictionary encoding and are written plain — the values must
    // still be right, which is the only thing this case asserts.
    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    public async Task Double_MinimisedRepro_SurvivesWhicheverEncodingIsChosen(int rows)
    {
        string path = TempPath($"double_minimised_{rows}.parquet");

        double[] values = rows == 2 ? [0.0, -0.0] : [0.0, -0.0, 0.0];
        var builder = new DoubleArray.Builder();
        foreach (var v in values) builder.Append(v);

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("d", DoubleType.Default, nullable: false)).Build();
        await WriteAsync(path, new RecordBatch(schema, [builder.Build()], rows));

        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        var read = (DoubleArray)(await reader.ReadRowGroupAsync(0)).Column(0);

        Assert.Equal(
            values.Select(v => BitPatterns.Of(v)),
            Enumerable.Range(0, rows).Select(i => BitPatterns.Of(read.GetValue(i)!.Value)));
    }

    // A column that is ALL -0.0 goes through the constant fast path, which was already byte-based and so
    // already right. Pinning it means the fix cannot regress it.
    [Fact]
    public async Task Double_AllNegativeZero_KeepsItsSign()
    {
        string path = TempPath("double_all_negative_zero.parquet");

        var builder = new DoubleArray.Builder();
        for (int i = 0; i < Rows; i++) builder.Append(-0.0);

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("d", DoubleType.Default, nullable: false)).Build();
        await WriteAsync(path, new RecordBatch(schema, [builder.Build()], Rows));

        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        var read = (DoubleArray)(await reader.ReadRowGroupAsync(0)).Column(0);

        for (int i = 0; i < Rows; i++)
        {
            Assert.Equal(BitPatterns.Of(-0.0),
                BitPatterns.Of(read.GetValue(i)!.Value));
        }
    }

    private static async Task WriteAsync(string path, RecordBatch batch)
    {
        await using var file = new LocalSequentialFile(path);
        await using var writer = new ParquetFileWriter(file, ownsFile: false,
            new ParquetWriteOptions { Compression = CompressionCodec.Uncompressed });
        await writer.WriteRowGroupAsync(batch);
        await writer.CloseAsync();
    }
}
