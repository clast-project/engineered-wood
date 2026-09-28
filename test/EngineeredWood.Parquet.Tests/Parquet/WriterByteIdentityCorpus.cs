// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

#pragma warning disable EWPARQUET0001 // ALP
#pragma warning disable EWPARQUET0002 // OmitPathInSchema
#pragma warning disable EWPARQUET0003 // FSST
#pragma warning disable EWPARQUET0005 // PFOR

using System.Security.Cryptography;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Compression;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using Xunit.Abstractions;

namespace EngineeredWood.Tests.Parquet;

/// <summary>
/// Writes a fixed corpus through both writers and records a SHA-256 of every file, so that a
/// refactor meant to change no output can be checked byte for byte: run it before and after, then
/// diff the two manifests.
/// </summary>
/// <remarks>
/// <para>Skipped unless <c>EW_WRITER_CORPUS_DIR</c> names an output directory. It is a tool rather than
/// a regression test: the hashes are not committed, because any intended change to the output would
/// break them.</para>
///
/// <para>The corpus aims to reach every page emitter (data pages V1 and V2, dictionary pages, FSST
/// symbol tables), every chunk-placement branch (dictionary and symbol-table offsets, bloom filter
/// blocks, several row groups) and every footer input. <see cref="ParquetWriteOptions.CreatedBy"/> is
/// pinned, because its default carries the assembly version.</para>
/// </remarks>
public class WriterByteIdentityCorpus
{
    private const string CreatedBy = "ew-corpus version 0";
    private const int Rows = 5000;

    private readonly ITestOutputHelper _output;

    public WriterByteIdentityCorpus(ITestOutputHelper output) => _output = output;

    [SkippableFact]
    public async Task WriteCorpus()
    {
        string? dir = Environment.GetEnvironmentVariable("EW_WRITER_CORPUS_DIR");
        Skip.If(string.IsNullOrEmpty(dir), "set EW_WRITER_CORPUS_DIR to write the corpus");
        Directory.CreateDirectory(dir!);

        var manifest = new List<string>();
        async Task Record(string name, Func<string, Task> write)
        {
            string path = Path.Combine(dir!, name + ".parquet");
            await write(path);
            using var sha = SHA256.Create();
            string hash = BitConverter.ToString(sha.ComputeHash(File.ReadAllBytes(path))).Replace("-", "").ToLowerInvariant();
            manifest.Add($"{hash}  {name}");
        }

        var full = BuildBatch(nested: true);
        var flat = BuildBatch(nested: false);

        foreach (var (name, options) in FileWriterVariants())
            await Record("file-" + name, path => WriteFileAsync(path, full, options));

        await Record("file-empty", path => WriteFileAsync(path, batch: null, Base));
        await Record("file-empty-declared", async path =>
        {
            await using var file = new LocalSequentialFile(path);
            await using var writer = new ParquetFileWriter(file, ownsFile: false, Base);
            writer.DeclareSchema(full.Schema);
            await writer.CloseAsync();
        });

        foreach (var (name, options) in BufferedWriterVariants())
            await Record("buffered-" + name, path => WriteBufferedAsync(path, flat, options));

        await Record("buffered-empty", async path =>
        {
            await using var file = new LocalSequentialFile(path);
            await using var writer = new BufferedParquetWriter(file, ownsFile: false, Base);
            await writer.CloseAsync();
        });

        manifest.Sort(StringComparer.Ordinal);
        File.WriteAllLines(Path.Combine(dir!, "manifest.txt"), manifest);
        _output.WriteLine($"{manifest.Count} files written to {dir}");
    }

    // Every option a default change could move is pinned, so that a manifest stays comparable across
    // one: when WritePageIndex became the default, a corpus that followed Default stopped being able
    // to show "unchanged with the index off". The page index has variants of its own below, and so
    // does the page row-count limit, whose 20,000-row default could not cut the corpus's 5,000 rows
    // anyway.
    private static readonly ParquetWriteOptions Base = ParquetWriteOptions.Default with
    {
        CreatedBy = CreatedBy,
        DataPageSize = 2048,
        WritePageIndex = false,
        DataPageRowCountLimit = null,
    };

    private static IEnumerable<(string Name, ParquetWriteOptions Options)> FileWriterVariants()
    {
        foreach (var version in new[] { DataPageVersion.V1, DataPageVersion.V2 })
        foreach (var codec in new[] { CompressionCodec.Uncompressed, CompressionCodec.Snappy, CompressionCodec.Zstd })
        foreach (bool dictionary in new[] { true, false })
        foreach (bool crc in new[] { false, true })
        {
            yield return ($"{version}-{codec}-dict{dictionary}-crc{crc}".ToLowerInvariant(), Base with
            {
                DataPageVersion = version,
                Compression = codec,
                DictionaryEnabled = dictionary,
                PageChecksumEnabled = crc,
            });
        }

        var bloom = new[] { "i32", "i64", "str_hi", "str_lo" };
        yield return ("bloom-v2", Base with { BloomFilterColumns = bloom });
        yield return ("bloom-v1", Base with { BloomFilterColumns = bloom, DataPageVersion = DataPageVersion.V1 });
        yield return ("fsst", Base with { ByteArrayEncoding = ByteArrayEncoding.Fsst, DictionaryEnabled = false });
        yield return ("fsst16-crc", Base with { ByteArrayEncoding = ByteArrayEncoding.Fsst16, DictionaryEnabled = false, PageChecksumEnabled = true });
        yield return ("fsst-bloom", Base with { ByteArrayEncoding = ByteArrayEncoding.Fsst, DictionaryEnabled = false, BloomFilterColumns = bloom });
        yield return ("alp", Base with { FloatingPointEncoding = FloatingPointEncoding.Alp, DictionaryEnabled = false });
        yield return ("bss", Base with { FloatingPointEncoding = FloatingPointEncoding.ByteStreamSplit, DictionaryEnabled = false });
        yield return ("pfor", Base with { IntegerEncoding = IntegerEncoding.Pfor, DictionaryEnabled = false });
        yield return ("plain-v2", Base with
        {
            IntegerEncoding = IntegerEncoding.Plain,
            FloatingPointEncoding = FloatingPointEncoding.Plain,
            ByteArrayEncoding = ByteArrayEncoding.Plain,
            DictionaryEnabled = false,
        });
        yield return ("dba", Base with { ByteArrayEncoding = ByteArrayEncoding.DeltaByteArray, DictionaryEnabled = false });
        yield return ("rowgroups", Base with { RowGroupMaxRows = 1500 });
        yield return ("rowgroups-v1-crc", Base with { RowGroupMaxRows = 1500, DataPageVersion = DataPageVersion.V1, PageChecksumEnabled = true });
        yield return ("footer-inputs", Base with
        {
            KeyValueMetadata = [new EngineeredWood.Parquet.Metadata.KeyValue { Key = "k", Value = "v" }],
            OmitPathInSchema = true,
            WriteArrowSchema = false,
            FloatingPointOrder = FloatingPointColumnOrder.Ieee754TotalOrder,
        });
        yield return ("no-stats", Base with { WriteStatistics = false });
        yield return ("batched-runs", Base with { BatchBitPackedRuns = true });
        yield return ("per-column", Base with
        {
            ColumnCodecs = new Dictionary<string, CompressionCodec> { ["str_lo"] = CompressionCodec.Zstd },
            ColumnDictionaryEnabled = new Dictionary<string, bool> { ["i64"] = false },
        });
        yield return ("big-pages", Base with { DataPageSize = 1024 * 1024 });

        // Page indexes: both page versions, dictionary on and off, several row groups, truncation at a
        // custom limit and turned off, and the float order that changes NaN bounds.
        yield return ("pageindex-v2", Base with { WritePageIndex = true });
        yield return ("pageindex-v1-plain-crc", Base with
        {
            WritePageIndex = true,
            DataPageVersion = DataPageVersion.V1,
            DictionaryEnabled = false,
            PageChecksumEnabled = true,
        });
        yield return ("pageindex-rowgroups-truncate8", Base with
        {
            WritePageIndex = true,
            RowGroupMaxRows = 1500,
            PageIndexTruncateLength = 8,
        });
        yield return ("pageindex-untruncated-totalorder", Base with
        {
            WritePageIndex = true,
            PageIndexTruncateLength = null,
            FloatingPointOrder = FloatingPointColumnOrder.Ieee754TotalOrder,
        });

        // The row-count limit, with pages large enough that only it cuts: both page loops (dictionary
        // on and off), both page versions, and the list, whose pages it cuts on record boundaries.
        yield return ("rowcap-v2", Base with { DataPageSize = 1024 * 1024, DataPageRowCountLimit = 700 });
        yield return ("rowcap-v1-plain-crc", Base with
        {
            DataPageSize = 1024 * 1024,
            DataPageRowCountLimit = 700,
            DataPageVersion = DataPageVersion.V1,
            DictionaryEnabled = false,
            PageChecksumEnabled = true,
        });
    }

    private static IEnumerable<(string Name, ParquetWriteOptions Options)> BufferedWriterVariants()
    {
        foreach (var version in new[] { DataPageVersion.V1, DataPageVersion.V2 })
        foreach (bool dictionary in new[] { true, false })
        foreach (bool crc in new[] { false, true })
        {
            yield return ($"{version}-dict{dictionary}-crc{crc}".ToLowerInvariant(), Base with
            {
                DataPageVersion = version,
                Compression = CompressionCodec.Snappy,
                DictionaryEnabled = dictionary,
                PageChecksumEnabled = crc,
                RowGroupMaxRows = 2500,
            });
        }

        yield return ("bloom", Base with { BloomFilterColumns = ["i32", "str_hi"], RowGroupMaxRows = 2500 });
        yield return ("pageindex", Base with { WritePageIndex = true, RowGroupMaxRows = 2500 });
        yield return ("rowcap", Base with { DataPageSize = 1024 * 1024, DataPageRowCountLimit = 700, RowGroupMaxRows = 2500 });
        yield return ("footer-inputs", Base with
        {
            KeyValueMetadata = [new EngineeredWood.Parquet.Metadata.KeyValue { Key = "k", Value = "v" }],
            OmitPathInSchema = true,
        });
    }

    private static async Task WriteFileAsync(string path, RecordBatch? batch, ParquetWriteOptions options)
    {
        await using var file = new LocalSequentialFile(path);
        await using var writer = new ParquetFileWriter(file, ownsFile: false, options);
        if (batch is not null)
            await writer.WriteRowGroupAsync(batch);
        await writer.CloseAsync();
    }

    /// <summary>Appends in uneven slices, so row groups fill across append boundaries.</summary>
    private static async Task WriteBufferedAsync(string path, RecordBatch batch, ParquetWriteOptions options)
    {
        await using var file = new LocalSequentialFile(path);
        await using var writer = new BufferedParquetWriter(file, ownsFile: false, options);
        int offset = 0;
        foreach (int length in new[] { 700, 1300, 1000, 2000 })
        {
            await writer.AppendAsync(Slice(batch, offset, length));
            offset += length;
        }

        await writer.CloseAsync();
    }

    private static RecordBatch Slice(RecordBatch batch, int offset, int length) =>
        new(batch.Schema,
            batch.Arrays.Select(a => EngineeredWood.Arrow.ArrowCompute.Take(a, Enumerable.Range(offset, length).ToArray())).ToArray(),
            length);

    private static RecordBatch BuildBatch(bool nested)
    {
        var i32 = new Int32Array.Builder();
        var i64 = new Int64Array.Builder();
        var dbl = new DoubleArray.Builder();
        var strLo = new StringArray.Builder();
        var strHi = new StringArray.Builder();
        var flag = new BooleanArray.Builder();
        var date = new Date32Array.Builder();
        var ts = new TimestampArray.Builder(new TimestampType(TimeUnit.Microsecond, "UTC"));
        var allNull = new Int32Array.Builder();
        var list = new ListArray.Builder(Int64Type.Default);
        var listValues = (Int64Array.Builder)list.ValueBuilder;
        var map = new MapArray.Builder(new MapType(StringType.Default, Int64Type.Default));
        var mapKeys = (StringArray.Builder)map.KeyBuilder;
        var mapValues = (Int64Array.Builder)map.ValueBuilder;
        var structA = new Int32Array.Builder();
        var structB = new StringArray.Builder();

        var rng = new Random(1234);
        var epoch = new DateTimeOffset(2020, 1, 1, 0, 0, 0, TimeSpan.Zero);
        for (int i = 0; i < Rows; i++)
        {
            if (i % 7 == 3) i32.AppendNull(); else i32.Append(rng.Next(-1000, 1000));
            i64.Append(i * 1_000_003L);
            if (i % 11 == 5) dbl.AppendNull();
            else if (i % 97 == 0) dbl.Append(double.NaN);
            else dbl.Append(Math.Round(rng.NextDouble() * 1000, 2));
            if (i % 13 == 1) strLo.AppendNull(); else strLo.Append("category-" + (i % 17));
            strHi.Append($"row-{i:D6}-{rng.Next():x8}-the-quick-brown-fox");
            if (i % 5 == 0) flag.AppendNull(); else flag.Append(rng.Next(2) == 0);
            date.Append(epoch.AddDays(i % 400).Date);
            ts.Append(epoch.AddSeconds(i * 37L));
            allNull.AppendNull();

            if (i % 9 == 4) list.AppendNull();
            else
            {
                list.Append();
                for (int j = 0; j < i % 6; j++) listValues.Append(rng.Next(50));
            }

            map.Append();
            for (int j = 0; j < i % 4; j++)
            {
                mapKeys.Append("k" + j);
                mapValues.Append(i + j);
            }

            structA.Append(i);
            structB.Append("s" + (i % 23));
        }

        var fields = new List<Field>
        {
            new("i32", Int32Type.Default, true),
            new("i64", Int64Type.Default, false),
            new("dbl", DoubleType.Default, true),
            new("str_lo", StringType.Default, true),
            new("str_hi", StringType.Default, false),
            new("flag", BooleanType.Default, true),
            new("date", Date32Type.Default, false),
            new("ts", new TimestampType(TimeUnit.Microsecond, "UTC"), false),
            new("all_null", Int32Type.Default, true),
        };
        var arrays = new List<IArrowArray>
        {
            i32.Build(), i64.Build(), dbl.Build(), strLo.Build(), strHi.Build(),
            flag.Build(), date.Build(), ts.Build(), allNull.Build(),
        };

        if (nested)
        {
            var listArray = list.Build();
            var mapArray = map.Build();
            var structType = new StructType([new Field("a", Int32Type.Default, false), new Field("b", StringType.Default, false)]);
            var structArray = new StructArray(structType, Rows, [structA.Build(), structB.Build()], ArrowBuffer.Empty, 0);
            fields.Add(new("list", listArray.Data.DataType, true));
            fields.Add(new("map", mapArray.Data.DataType, true));
            fields.Add(new("struct", structType, true));
            arrays.Add(listArray);
            arrays.Add(mapArray);
            arrays.Add(structArray);
        }

        var schema = new Apache.Arrow.Schema(fields, null);
        return new RecordBatch(schema, arrays, Rows);
    }
}
