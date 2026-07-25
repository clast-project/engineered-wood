// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Compression;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using Xunit.Abstractions;

namespace EngineeredWood.Tests.Parquet.Data;

/// <summary>
/// End-to-end checks for <see cref="ParquetWriteOptions.BatchBitPackedRuns"/>: files written with
/// batched literal runs must be byte-for-byte decodable by EngineeredWood <em>and</em> by an
/// independent implementation (ParquetSharp / parquet-cpp), since the flag changes the framing of
/// every level and dictionary-index stream in the file.
/// </summary>
public class BatchedRunsInteropTests : IDisposable
{
    private readonly string _tempDir;
    private readonly ITestOutputHelper _output;

    public BatchedRunsInteropTests(ITestOutputHelper output)
    {
        _output = output;
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-batched-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private string TempPath(string name) => Path.Combine(_tempDir, name);

    private async Task<string> WriteAsync(string name, RecordBatch batch, ParquetWriteOptions options)
    {
        string path = TempPath(name);
        await using var file = new LocalSequentialFile(path);
        await using var writer = new ParquetFileWriter(file, ownsFile: false, options);
        await writer.WriteRowGroupAsync(batch);
        await writer.CloseAsync();
        return path;
    }

    private static async Task<RecordBatch> ReadAsync(string path)
    {
        await using var file = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(file, ownsFile: false);
        return await reader.ReadRowGroupAsync(0);
    }

    /// <summary>Fixed-length float lists — the small-n repetition pattern.</summary>
    private static RecordBatch BuildListBatch(int rows, int length)
    {
        var listType = new ListType(new Field("element", FloatType.Default, nullable: false));
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("vec", listType, nullable: false))
            .Build();

        var values = new FloatArray.Builder();
        var offsets = new int[rows + 1];
        for (int i = 0; i < rows; i++)
        {
            offsets[i] = i * length;
            for (int j = 0; j < length; j++) values.Append(i * length + j + 0.25f);
        }
        offsets[rows] = rows * length;

        var data = new ArrayData(listType, rows, 0, 0,
            [ArrowBuffer.Empty, new ArrowBuffer(MemoryMarshal.AsBytes(offsets.AsSpan()).ToArray())],
            [values.Build().Data]);
        return new RecordBatch(schema, [new ListArray(data)], rows);
    }

    /// <summary>
    /// High-cardinality strings, a nullable int, and a boolean — exercises dictionary indices, def
    /// levels, and the RLE boolean value encoding (which also flows through the batched encoder).
    /// </summary>
    private static RecordBatch BuildDictBatch(int rows, int cardinality)
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("name", StringType.Default, nullable: false))
            .Field(new Field("score", Int32Type.Default, nullable: true))
            .Field(new Field("flag", BooleanType.Default, nullable: false))
            .Build();

        var names = new StringArray.Builder();
        var scores = new Int32Array.Builder();
        var flags = new BooleanArray.Builder();
        var rng = new Random(7);
        for (int i = 0; i < rows; i++)
        {
            names.Append($"name-{rng.Next(cardinality)}");
            if (i % 5 == 2) scores.AppendNull();
            else scores.Append(i);
            // Runs of identical bits interspersed with flips, so both RLE and literal runs occur.
            flags.Append((i / 9) % 2 == 0);
        }

        return new RecordBatch(schema, [names.Build(), scores.Build(), flags.Build()], rows);
    }

    public static TheoryData<string, DataPageVersion, CompressionCodec> Configurations() => new()
    {
        { "v2-uncompressed", DataPageVersion.V2, CompressionCodec.Uncompressed },
        { "v2-snappy", DataPageVersion.V2, CompressionCodec.Snappy },
        { "v1-uncompressed", DataPageVersion.V1, CompressionCodec.Uncompressed },
        { "v1-snappy", DataPageVersion.V1, CompressionCodec.Snappy },
    };

    [Theory]
    [MemberData(nameof(Configurations))]
    public async Task BatchedRuns_RoundTripThroughEngineeredWood(
        string label, DataPageVersion pageVersion, CompressionCodec codec)
    {
        var batch = BuildListBatch(rows: 2000, length: 3);
        var baseOptions = ParquetWriteOptions.Default with
        {
            DataPageVersion = pageVersion,
            Compression = codec,
        };

        string plain = await WriteAsync($"list-plain-{label}.parquet", batch, baseOptions);
        string batched = await WriteAsync($"list-batched-{label}.parquet", batch,
            baseOptions with { BatchBitPackedRuns = true });

        var fromPlain = (ListArray)(await ReadAsync(plain)).Column(0);
        var fromBatched = (ListArray)(await ReadAsync(batched)).Column(0);

        Assert.Equal(fromPlain.Length, fromBatched.Length);
        for (int i = 0; i < fromPlain.Length; i++)
        {
            var a = (FloatArray)fromPlain.GetSlicedValues(i);
            var b = (FloatArray)fromBatched.GetSlicedValues(i);
            Assert.Equal(a.Length, b.Length);
            for (int j = 0; j < a.Length; j++)
                Assert.Equal(a.GetValue(j), b.GetValue(j));
        }

        _output.WriteLine(
            $"list n=3 {label,-16} plain={new FileInfo(plain).Length,8}  " +
            $"batched={new FileInfo(batched).Length,8}  " +
            $"({100.0 * new FileInfo(batched).Length / new FileInfo(plain).Length:F1}%)");
    }

    [Theory]
    [MemberData(nameof(Configurations))]
    public async Task BatchedRuns_RoundTripDictionaryColumns(
        string label, DataPageVersion pageVersion, CompressionCodec codec)
    {
        var batch = BuildDictBatch(rows: 20_000, cardinality: 1000);
        var baseOptions = ParquetWriteOptions.Default with
        {
            DataPageVersion = pageVersion,
            Compression = codec,
        };

        string plain = await WriteAsync($"dict-plain-{label}.parquet", batch, baseOptions);
        string batched = await WriteAsync($"dict-batched-{label}.parquet", batch,
            baseOptions with { BatchBitPackedRuns = true });

        var a = await ReadAsync(plain);
        var b = await ReadAsync(batched);

        var namesA = (StringArray)a.Column(0);
        var namesB = (StringArray)b.Column(0);
        var scoresA = (Int32Array)a.Column(1);
        var scoresB = (Int32Array)b.Column(1);
        var flagsA = (BooleanArray)a.Column(2);
        var flagsB = (BooleanArray)b.Column(2);

        Assert.Equal(namesA.Length, namesB.Length);
        for (int i = 0; i < namesA.Length; i++)
        {
            Assert.Equal(namesA.GetString(i), namesB.GetString(i));
            Assert.Equal(scoresA.IsNull(i), scoresB.IsNull(i));
            if (!scoresA.IsNull(i))
                Assert.Equal(scoresA.GetValue(i), scoresB.GetValue(i));
            Assert.Equal(flagsA.GetValue(i), flagsB.GetValue(i));
        }

        _output.WriteLine(
            $"dict     {label,-16} plain={new FileInfo(plain).Length,8}  " +
            $"batched={new FileInfo(batched).Length,8}  " +
            $"({100.0 * new FileInfo(batched).Length / new FileInfo(plain).Length:F1}%)");
    }

    [Theory]
    [MemberData(nameof(Configurations))]
    public async Task BatchedRuns_AreReadableByParquetSharp(
        string label, DataPageVersion pageVersion, CompressionCodec codec)
    {
        const int rows = 20_000;
        var batch = BuildDictBatch(rows, cardinality: 1000);
        string path = await WriteAsync($"ps-{label}.parquet", batch, ParquetWriteOptions.Default with
        {
            DataPageVersion = pageVersion,
            Compression = codec,
            BatchBitPackedRuns = true,
        });

        var expectedNames = (StringArray)batch.Column(0);
        var expectedScores = (Int32Array)batch.Column(1);
        var expectedFlags = (BooleanArray)batch.Column(2);

        using var reader = new ParquetSharp.ParquetFileReader(path);
        using var rowGroup = reader.RowGroup(0);
        Assert.Equal(rows, rowGroup.MetaData.NumRows);

        using (var nameCol = rowGroup.Column(0).LogicalReader<string>())
        {
            var actual = new string[rows];
            nameCol.ReadBatch(actual, 0, rows);
            for (int i = 0; i < rows; i++)
                Assert.Equal(expectedNames.GetString(i), actual[i]);
        }

        using (var scoreCol = rowGroup.Column(1).LogicalReader<int?>())
        {
            var actual = new int?[rows];
            scoreCol.ReadBatch(actual, 0, rows);
            for (int i = 0; i < rows; i++)
            {
                if (expectedScores.IsNull(i)) Assert.Null(actual[i]);
                else Assert.Equal(expectedScores.GetValue(i), actual[i]);
            }
        }

        using (var flagCol = rowGroup.Column(2).LogicalReader<bool>())
        {
            var actual = new bool[rows];
            flagCol.ReadBatch(actual, 0, rows);
            for (int i = 0; i < rows; i++)
                Assert.Equal(expectedFlags.GetValue(i), actual[i]);
        }
    }

    [Fact]
    public async Task BatchedFixedLists_AreReadableByParquetSharp()
    {
        const int rows = 2000, length = 3;
        string path = await WriteAsync("ps-list.parquet", BuildListBatch(rows, length),
            ParquetWriteOptions.Default with { BatchBitPackedRuns = true });

        using var reader = new ParquetSharp.ParquetFileReader(path);
        using var rowGroup = reader.RowGroup(0);
        Assert.Equal(rows, rowGroup.MetaData.NumRows);

        using var col = rowGroup.Column(0).LogicalReader<float[]>();
        var values = new float[rows][];
        col.ReadBatch(values, 0, rows);

        for (int i = 0; i < rows; i++)
        {
            Assert.Equal(length, values[i].Length);
            for (int j = 0; j < length; j++)
                Assert.Equal(i * length + j + 0.25f, values[i][j]);
        }
    }

    [Fact]
    public async Task BatchedRuns_PreserveFixedListFastPathDetection()
    {
        // The batched framing is exactly the dense-run shape the detector's tiled scan targets, so
        // detection must still fire — and now take the vectorised path.
        var batch = BuildListBatch(rows: 5000, length: 3);
        string path = await WriteAsync("batched-fastpath.parquet", batch,
            ParquetWriteOptions.Default with { BatchBitPackedRuns = true });

        await using var file = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(
            file, ownsFile: false, new ParquetReadOptions { FixedListFastPath = true });
        var result = (ListArray)(await reader.ReadRowGroupAsync(0)).Column(0);

        Assert.Equal(5000, result.Length);
        for (int i = 0; i < 5000; i += 250)
        {
            var vec = (FloatArray)result.GetSlicedValues(i);
            Assert.Equal(3, vec.Length);
            Assert.Equal(i * 3 + 0.25f, vec.GetValue(0));
        }
    }
}
