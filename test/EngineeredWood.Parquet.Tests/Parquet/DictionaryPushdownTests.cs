// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Compression;
using EngineeredWood.Expressions;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Data;
using EngineeredWood.Parquet.Metadata;
using EngineeredWood.Parquet.Schema;
using Xunit.Abstractions;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.Tests.Parquet;

/// <summary>
/// Row-group pruning from dictionary pages (#57): <see cref="ParquetReadOptions.FilterUseDictionaries"/>.
/// </summary>
/// <remarks>
/// A dictionary answers exactly, but only for a chunk that never left it, and the wrong answer is
/// silent: a pruned row group is never read. So the main oracle is a soundness sweep over the
/// parquet-testing fixtures: every value actually present in a row group must leave that group a
/// candidate. The same sweep counts the groups the dictionary rules out where statistics could not,
/// to show it does something on files other writers produced.
/// </remarks>
public class DictionaryPushdownTests : IDisposable
{
    private readonly ITestOutputHelper _output;
    private readonly string _tempDir;

    public DictionaryPushdownTests(ITestOutputHelper output)
    {
        _output = output;
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-dict-pd-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    // ───── EW's own files ─────

    /// <summary>
    /// Three row groups whose min/max all span "a".."z" but hold disjoint values, so statistics cannot
    /// decide an equality and only a membership source can. Each group repeats its values
    /// <see cref="Repeats"/> times: the writer only builds a dictionary when it saves space.
    /// </summary>
    private async Task<string> WriteThreeRowGroups(string name, ParquetWriteOptions? options = null)
    {
        string path = Path.Combine(_tempDir, name + ".parquet");
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("name", StringType.Default, false))
            .Build();
        string[][] groups =
        [
            ["alpha", "zebra", "carrot"],
            ["apple", "zinnia", "cherry"],
            ["avocado", "zucchini", "celery"],
        ];

        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false,
            options ?? new ParquetWriteOptions { Compression = CompressionCodec.Snappy }))
        {
            foreach (var values in groups)
            {
                var builder = new StringArray.Builder();
                for (int r = 0; r < Repeats; r++)
                    foreach (var v in values)
                        builder.Append(v);
                await writer.WriteRowGroupAsync(new RecordBatch(schema, [builder.Build()], values.Length * Repeats));
            }
            await writer.CloseAsync();
        }
        return path;
    }

    private static async Task<bool[]> Candidates(string path, Predicate filter, ParquetReadOptions options)
    {
        await using var input = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(input, ownsFile: false, options);
        var bits = await reader.GetCandidateRowGroupsAsync(filter);
        return Enumerable.Range(0, bits.Length).Select(i => bits[i]).ToArray();
    }

    private const int Repeats = 10;

    private static readonly ParquetReadOptions WithDictionaries = new() { FilterUseDictionaries = true };

    [Fact]
    public async Task Equality_KeepsOnlyTheGroupsHoldingTheValue()
    {
        string path = await WriteThreeRowGroups("eq");

        Assert.Equal(new[] { false, true, false }, await Candidates(path, Ex.Equal("name", "cherry"), WithDictionaries));
        Assert.Equal(new[] { false, false, false }, await Candidates(path, Ex.Equal("name", "banana"), WithDictionaries));
    }

    [Fact]
    public async Task IsOffByDefault()
    {
        string path = await WriteThreeRowGroups("off");

        Assert.Equal(new[] { true, true, true }, await Candidates(path, Ex.Equal("name", "banana"), new ParquetReadOptions()));
    }

    [Fact]
    public async Task InList_KeepsAGroupHoldingAnyMember()
    {
        string path = await WriteThreeRowGroups("in");

        Assert.Equal(new[] { true, false, true },
            await Candidates(path, Ex.In("name", "banana", "carrot", "celery"), WithDictionaries));
        Assert.Equal(new[] { false, false, false },
            await Candidates(path, Ex.In("name", "banana", "blueberry"), WithDictionaries));
    }

    [Fact]
    public async Task ComposesThroughNotAndOr()
    {
        string path = await WriteThreeRowGroups("tree");

        // NOT (name = banana) is never ruled out; OR is ruled out only when every branch is.
        Assert.Equal(new[] { true, true, true },
            await Candidates(path, Ex.Not(Ex.Equal("name", "banana")), WithDictionaries));
        Assert.Equal(new[] { false, true, false },
            await Candidates(path, Ex.Or(Ex.Equal("name", "banana"), Ex.Equal("name", "zinnia")), WithDictionaries));
    }

    [Fact]
    public async Task APlainChunk_IsNeverAsked()
    {
        string path = await WriteThreeRowGroups("plain", new ParquetWriteOptions { DictionaryEnabled = false });

        Assert.Equal(new[] { true, true, true }, await Candidates(path, Ex.Equal("name", "banana"), WithDictionaries));
    }

    [Fact]
    public async Task ReadAll_SkipsTheRowGroupsRuledOut()
    {
        string path = await WriteThreeRowGroups("read");
        await using var input = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(input, ownsFile: false,
            new ParquetReadOptions { Filter = Ex.Equal("name", "zucchini"), FilterUseDictionaries = true });

        var rows = new List<string>();
        await foreach (var batch in reader.ReadAllAsync())
        {
            var column = (StringArray)batch.Column(0);
            for (int i = 0; i < batch.Length; i++)
                rows.Add(column.GetString(i));
        }

        Assert.Equal(Enumerable.Repeat(new[] { "avocado", "zucchini", "celery" }, Repeats).SelectMany(r => r), rows);
    }

    /// <summary>
    /// A dictionary page that fails its checksum is not trusted: the chunk is declined rather than
    /// answered from corrupt values, and the ordinary read is left to report the corruption.
    /// </summary>
    [Fact]
    public async Task ACorruptDictionary_IsDeclinedNotTrusted()
    {
        string path = await WriteThreeRowGroups("crc", new ParquetWriteOptions
        {
            Compression = CompressionCodec.Uncompressed,
            PageChecksumEnabled = true,
        });

        byte[] file = File.ReadAllBytes(path);
        FileMetaData metadata;
        await using (var input = new LocalRandomAccessFile(path))
        using (var reader = new ParquetFileReader(input, ownsFile: false))
            metadata = await reader.ReadMetadataAsync();

        // Flip one byte of "cherry" inside row group 1's dictionary page, leaving its CRC as written.
        var meta = metadata.RowGroups[1].Columns[0].MetaData!;
        long start = meta.DictionaryPageOffset!.Value;
        int at = IndexOf(file, "cherry"u8.ToArray(), (int)start);
        Assert.True(at > start && at < meta.DataPageOffset);
        file[at] ^= 0x01;
        File.WriteAllBytes(path, file);

        var options = new ParquetReadOptions { FilterUseDictionaries = true, PageChecksumValidation = true };
        Assert.True((await Candidates(path, Ex.Equal("name", "cherry"), options))[1]);
    }

    /// <summary>
    /// A compressed dictionary page that its codec cannot decompress is declined, whatever the codec
    /// throws: ZstdSharp signals corruption with its own exception type, not an InvalidDataException, and
    /// pruning must never be what fails a read. No checksum here, so the codec is the first to see it.
    /// </summary>
    [Theory]
    [InlineData(CompressionCodec.Zstd)]
    [InlineData(CompressionCodec.Snappy)]
    [InlineData(CompressionCodec.Gzip)]
    [InlineData(CompressionCodec.Brotli)]
    [InlineData(CompressionCodec.Lz4)]
    public async Task AnUndecompressableDictionary_IsDeclinedNotThrown(CompressionCodec codec)
    {
        string path = await WriteThreeRowGroups("codec_" + codec, new ParquetWriteOptions { Compression = codec });

        byte[] file = File.ReadAllBytes(path);
        FileMetaData metadata;
        await using (var input = new LocalRandomAccessFile(path))
        using (var reader = new ParquetFileReader(input, ownsFile: false))
            metadata = await reader.ReadMetadataAsync();

        // Overwrite the whole compressed payload of row group 1's dictionary page, after its header.
        var meta = metadata.RowGroups[1].Columns[0].MetaData!;
        int start = checked((int)meta.DictionaryPageOffset!.Value);
        var header = PageHeaderDecoder.Decode(file.AsSpan(start), out int headerLength);
        Assert.Equal(PageType.DictionaryPage, header.Type);
        file.AsSpan(start + headerLength, header.CompressedPageSize).Fill(0xFF);
        File.WriteAllBytes(path, file);

        var candidates = await Candidates(path, Ex.Equal("name", "banana"), WithDictionaries);

        Assert.True(candidates[1]);
    }

    // ───── Requests: dictionaries are read a window at a time ─────

    private const int PrefetchGroups = 8;

    /// <summary>
    /// <see cref="PrefetchGroups"/> row groups of a dictionary-encoded key. Groups 0-5 hold the extremes
    /// "a-min" and "z-max", so statistics leave an equality on a middle value undecided; groups 6-7 hold
    /// only "y" values, so statistics rule out any value below "y" and those groups never need a
    /// dictionary. "common" is in groups 0-5; "k3" only in group 3.
    /// </summary>
    private async Task<string> WritePrefetchFile(string name, int groups = PrefetchGroups)
    {
        string path = Path.Combine(_tempDir, name + ".parquet");
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("key", StringType.Default, false))
            .Build();

        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false,
            new ParquetWriteOptions { Compression = CompressionCodec.Uncompressed }))
        {
            for (int g = 0; g < groups; g++)
            {
                string[] values = g < groups - 2 ? ["a-min", "z-max", "common", $"k{g}"] : ["y1", "y2"];
                var builder = new StringArray.Builder();
                for (int r = 0; r < Repeats; r++)
                    foreach (var v in values)
                        builder.Append(v);
                await writer.WriteRowGroupAsync(new RecordBatch(schema, [builder.Build()], values.Length * Repeats));
            }
            await writer.CloseAsync();
        }
        return path;
    }

    /// <summary>The requests a candidate evaluation issues, with the number of ranges in each.</summary>
    private static async Task<(bool[] Candidates, List<int> RangesPerRequest)> CandidatesCounted(
        string path, Predicate filter, bool dictionaries, long? budget = null)
    {
        await using var input = new CountingFile(new LocalRandomAccessFile(path));
        await using var reader = new ParquetFileReader(input, ownsFile: false,
            new ParquetReadOptions { FilterUseDictionaries = dictionaries });
        if (budget is { } b)
            reader.DictionaryPrefetchBudgetBytes = b;

        await reader.ReadMetadataAsync(); // footer requests are not the subject
        input.Requests.Clear();

        var bits = await reader.GetCandidateRowGroupsAsync(filter);
        return (Enumerable.Range(0, bits.Length).Select(i => bits[i]).ToArray(), input.Requests.ToList());
    }

    [Fact]
    public async Task EveryUndecidedGroup_IsAskedInOneRequest()
    {
        string path = await WritePrefetchFile("one_request");

        var (candidates, requests) = await CandidatesCounted(path, Ex.Equal("key", "k3"), dictionaries: true);

        Assert.Equal(new[] { false, false, false, true, false, false, false, false }, candidates);
        // Six undecided groups (0-5), six dictionary pages, one request; groups 6-7 are ruled out by
        // statistics and never read.
        Assert.Equal(new[] { 6 }, requests);
    }

    [Fact]
    public async Task AValueInEveryUndecidedGroup_CostsOneRequest()
    {
        string path = await WritePrefetchFile("worst_case");

        var (candidates, requests) = await CandidatesCounted(path, Ex.Equal("key", "common"), dictionaries: true);

        Assert.Equal(new[] { true, true, true, true, true, true, false, false }, candidates);
        Assert.Equal(new[] { 6 }, requests);
    }

    [Fact]
    public async Task ASmallBudget_SplitsTheReadIntoWindows_WithTheSameAnswers()
    {
        string path = await WritePrefetchFile("windows");
        var (expected, _) = await CandidatesCounted(path, Ex.Equal("key", "k3"), dictionaries: true);

        // A budget below one page: every group is its own window, and a window always holds its first group.
        var (candidates, requests) = await CandidatesCounted(path, Ex.Equal("key", "k3"), dictionaries: true, budget: 1);

        Assert.Equal(expected, candidates);
        Assert.Equal(Enumerable.Repeat(1, 6), requests);
    }

    [Fact]
    public async Task ThePrefetch_IsOnlyForTheColumnsTheEqualitiesName()
    {
        string path = await WritePrefetchFile("no_equality");

        // A range predicate never asks a dictionary, so nothing is read ahead for it.
        var (_, requests) = await CandidatesCounted(path, Ex.GreaterThan("key", "b"), dictionaries: true);

        Assert.Empty(requests);
    }

    /// <summary>
    /// Leaves the probe declines before reading must not make the prefetch read either: an IN list
    /// holding a column reference, a NULL literal, and a literal the column cannot hold.
    /// </summary>
    [Fact]
    public async Task ThePrefetch_SkipsLeavesTheProbeWouldDecline()
    {
        string path = await WritePrefetchFile("declined_leaves");

        var nonLiteralIn = new SetPredicate(
            new UnboundReference("key"), [new LiteralExpression(LiteralValue.Of("k3")), new UnboundReference("other")],
            SetOperator.In);
        var nullLiteral = new ComparisonPredicate(
            new UnboundReference("key"), ComparisonOperator.Equal, new LiteralExpression(LiteralValue.Null));
        var wrongType = Ex.Equal("key", 5);

        foreach (var filter in new Predicate[] { nonLiteralIn, nullLiteral, wrongType })
        {
            var (_, requests) = await CandidatesCounted(path, filter, dictionaries: true);
            Assert.Empty(requests);
        }
    }

    [Fact]
    public async Task AWindow_AsksForAtMost64Pages()
    {
        // 70 undecided groups (plus 2 statistics rule out) and a budget they all fit: the page cap splits
        // them, since on object storage each page of a window is its own concurrent GET.
        string path = await WritePrefetchFile("page_cap", groups: 72);

        var (candidates, requests) = await CandidatesCounted(path, Ex.Equal("key", "k3"), dictionaries: true);

        Assert.Equal(Enumerable.Range(0, 72).Select(g => g == 3), candidates);
        Assert.Equal(new[] { DictionaryPrefetch.MaxRangesPerWindow, 70 - DictionaryPrefetch.MaxRangesPerWindow }, requests);
    }

    [Fact]
    public async Task ReadAll_ReadsTheDictionariesOnceAndOnlyTheKeptGroupsData()
    {
        string path = await WritePrefetchFile("read_all");
        await using var input = new CountingFile(new LocalRandomAccessFile(path));
        await using var reader = new ParquetFileReader(input, ownsFile: false,
            new ParquetReadOptions { Filter = Ex.Equal("key", "k3"), FilterUseDictionaries = true });
        await reader.ReadMetadataAsync();
        input.Requests.Clear();

        int rows = 0;
        await foreach (var batch in reader.ReadAllAsync())
            rows += batch.Length;

        Assert.Equal(4 * Repeats, rows);
        Assert.Equal(2, input.Requests.Count); // the dictionary window, then group 3's data
    }

    /// <summary>Records the number of ranges in each request.</summary>
    private sealed class CountingFile(EngineeredWood.IO.IRandomAccessFile inner) : EngineeredWood.IO.IRandomAccessFile
    {
        public List<int> Requests { get; } = new();

        public ValueTask<long> GetLengthAsync(CancellationToken cancellationToken = default) =>
            inner.GetLengthAsync(cancellationToken);

        public ValueTask<System.Buffers.IMemoryOwner<byte>> ReadAsync(
            EngineeredWood.IO.FileRange range, CancellationToken cancellationToken = default)
        {
            lock (Requests) Requests.Add(1);
            return inner.ReadAsync(range, cancellationToken);
        }

        public ValueTask<IReadOnlyList<System.Buffers.IMemoryOwner<byte>>> ReadRangesAsync(
            IReadOnlyList<EngineeredWood.IO.FileRange> ranges, CancellationToken cancellationToken = default)
        {
            lock (Requests) Requests.Add(ranges.Count);
            return inner.ReadRangesAsync(ranges, cancellationToken);
        }

        public ValueTask DisposeAsync() => inner.DisposeAsync();

        public void Dispose() => inner.Dispose();
    }

    // ───── Floating point and the stored representation ─────

    private static readonly double OtherNaN = BitConverter.Int64BitsToDouble(0x7FF8_0000_0000_0001);

    private async Task<string> WriteDoubleColumn(string name, double stored)
    {
        string path = Path.Combine(_tempDir, name + ".parquet");
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", DoubleType.Default, false))
            .Build();
        var builder = new DoubleArray.Builder();
        for (int r = 0; r < Repeats; r++)
            builder.Append(stored).Append(5.0).Append(-3.0);

        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false,
            new ParquetWriteOptions { Compression = CompressionCodec.Uncompressed }))
        {
            await writer.WriteRowGroupAsync(new RecordBatch(schema, [builder.Build()], 3 * Repeats));
            await writer.CloseAsync();
        }

        await using var input = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(input, ownsFile: false);
        var meta = (await reader.ReadMetadataAsync()).RowGroups[0].Columns[0].MetaData!;
        Assert.True(MembershipPredicateEvaluator.IsWhollyDictionaryEncoded(meta), "expected a dictionary chunk");
        return path;
    }

    [Theory]
    [InlineData("neg_zero", -0.0, 0.0, true)]
    [InlineData("pos_zero", 0.0, -0.0, true)]
    [InlineData("nan", double.NaN, double.NaN, true)] // stored with another payload below
    [InlineData("absent", -0.0, 1.0, false)]
    public async Task Doubles_CompareAsSqlDoes(string name, double stored, double literal, bool kept)
    {
        string path = await WriteDoubleColumn(name, double.IsNaN(stored) ? OtherNaN : stored);

        Assert.Equal(kept, (await Candidates(path, Ex.Equal("x", literal), WithDictionaries))[0]);
    }

    /// <summary>
    /// A DECIMAL held as INT32 stores 5.00 as 500, so the bytes of an integer literal 5 are not the bytes
    /// of the value it equals: the probe must decline rather than report 5.00 absent. parquet-mr writes
    /// this layout for DECIMAL(9) and below, and marks it with the legacy converted_type only.
    /// </summary>
    [Fact]
    public async Task ADecimalStoredAsAnInteger_IsNotProbedWithAnIntegerLiteral()
    {
        var column = (await FixtureSchema("int32_decimal.parquet")).Columns[0];
        Assert.Equal(PhysicalType.Int32, column.PhysicalType);

        Assert.False(MembershipPredicateEvaluator.StoresPlainValues(column.SchemaElement));
        Assert.False(MembershipPredicateEvaluator.TryEncodeForProbe(LiteralValue.Of(5), column, out _));
        Assert.False(MembershipPredicateEvaluator.TryEncodeForProbe(LiteralValue.Of(500), column, out _));
    }

    // ───── Fixtures ─────

    [Fact]
    public void Fixture_AChunkThatFellBack_IsNotAsked()
    {
        var chunks = TestDataFooter("large_string_map.brotli.parquet")
            .RowGroups.SelectMany(rg => rg.Columns)
            .Select(c => c.MetaData!)
            .Where(m => m.EncodingStats!.Any(s => s.PageType == PageType.DictionaryPage))
            .ToList();

        var fellBack = Assert.Single(chunks, m => m.EncodingStats!.Any(s =>
            s.PageType == PageType.DataPage && s.Encoding == Encoding.Plain));
        Assert.False(MembershipPredicateEvaluator.IsWhollyDictionaryEncoded(fellBack));
        Assert.Contains(chunks, MembershipPredicateEvaluator.IsWhollyDictionaryEncoded);
    }

    [Fact]
    public void WithoutEncodingStats_NoChunkIsAsked()
    {
        var meta = new ColumnMetaData
        {
            Type = PhysicalType.ByteArray,
            Encodings = [Encoding.PlainDictionary, Encoding.Rle],
            Codec = CompressionCodec.Uncompressed,
            NumValues = 1,
            TotalUncompressedSize = 1,
            TotalCompressedSize = 1,
            DataPageOffset = 100,
            DictionaryPageOffset = 4,
        };
        Assert.False(MembershipPredicateEvaluator.IsWhollyDictionaryEncoded(meta));
    }

    [Theory]
    [InlineData(PageType.DataPage)]
    [InlineData(PageType.DataPageV2)] // parquet-cpp's spelling and EW's must both count
    public void EncodingStats_BothDataPageTypesCount(PageType dataPageType)
    {
        Assert.True(MembershipPredicateEvaluator.IsWhollyDictionaryEncoded(Stats(
            new(PageType.DictionaryPage, Encoding.Plain, 1), new(dataPageType, Encoding.RleDictionary, 3))));
        Assert.False(MembershipPredicateEvaluator.IsWhollyDictionaryEncoded(Stats(
            new(PageType.DictionaryPage, Encoding.Plain, 1), new(dataPageType, Encoding.RleDictionary, 3),
            new(dataPageType, Encoding.Plain, 1))));
    }

    [Fact]
    public void EncodingStats_AnUnknownPageType_DeclinesTheChunk()
    {
        Assert.False(MembershipPredicateEvaluator.IsWhollyDictionaryEncoded(Stats(
            new(PageType.DictionaryPage, Encoding.Plain, 1), new(PageType.DataPage, Encoding.RleDictionary, 3),
            new((PageType)99, Encoding.Plain, 1))));
    }

    [Fact]
    public void EncodingStats_TwoDictionaryPages_DeclinesTheChunk()
    {
        Assert.False(MembershipPredicateEvaluator.IsWhollyDictionaryEncoded(Stats(
            new(PageType.DictionaryPage, Encoding.Plain, 2), new(PageType.DataPage, Encoding.RleDictionary, 3))));
    }

    private static ColumnMetaData Stats(params PageEncodingStats[] stats) => new()
    {
        Type = PhysicalType.Int32,
        Encodings = [Encoding.Plain, Encoding.RleDictionary],
        Codec = CompressionCodec.Uncompressed,
        NumValues = 1,
        TotalUncompressedSize = 1,
        TotalCompressedSize = 1,
        DataPageOffset = 100,
        DictionaryPageOffset = 4,
        EncodingStats = stats,
    };

    /// <summary>Every fixture with at least one chunk a dictionary can answer for.</summary>
    public static TheoryData<string> FixturesWithDictionaryChunks()
    {
        var data = new TheoryData<string>();
        foreach (string path in TestData.GetAllParquetFiles().OrderBy(p => p, StringComparer.Ordinal))
        {
            FileMetaData metadata;
            try
            {
                metadata = TestDataFooter(Path.GetFileName(path));
            }
            catch (Exception)
            {
                continue;
            }

            if (metadata.RowGroups.Any(rg => rg.Columns.Any(c =>
                    c.MetaData is { } m && MembershipPredicateEvaluator.IsWhollyDictionaryEncoded(m))))
                data.Add(Path.GetFileName(path));
        }
        return data;
    }

    [Theory]
    [MemberData(nameof(FixturesWithDictionaryChunks))]
    public async Task Fixture_NoPresentValueIsRuledOut(string fileName)
    {
        var (present, dictionaryPrunes) = await SweepAsync(fileName);
        _output.WriteLine($"{fileName}: {present} present values kept, {dictionaryPrunes} groups ruled out by dictionary alone");
    }

    /// <summary>
    /// The sweep's other half: the dictionary must rule out SOME row group statistics could not, or the
    /// soundness half would pass on an implementation that never prunes at all.
    /// </summary>
    [Fact]
    public async Task Fixtures_TheDictionaryRulesOutGroupsStatisticsCannot()
    {
        int total = 0;
        foreach (string fileName in FixturesWithDictionaryChunks())
            total += (await SweepAsync(fileName)).DictionaryPrunes;

        _output.WriteLine($"{total} row groups ruled out by dictionaries alone");
        Assert.True(total > 0);
    }

    private const int ValuesPerChunk = 8;

    /// <summary>
    /// For each flat column of a plain-valued type: every value present in a row group must leave that
    /// group a candidate (asserted here), and a nearby value absent from it is counted when the
    /// dictionary rules the group out and statistics alone did not.
    /// </summary>
    private static async Task<(int Present, int DictionaryPrunes)> SweepAsync(string fileName)
    {
        string path = TestData.GetPath(fileName);
        await using var input = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(input, ownsFile: false);
        var metadata = await reader.ReadMetadataAsync();
        var schema = await reader.GetSchemaAsync();

        int present = 0, prunes = 0;
        foreach (var column in schema.Columns)
        {
            if (column.Path.Count != 1 || column.MaxRepetitionLevel > 0
                || column.PhysicalType is not (PhysicalType.Int32 or PhysicalType.Int64 or PhysicalType.ByteArray)
                || !MembershipPredicateEvaluator.StoresPlainValues(column.SchemaElement))
                continue;

            for (int rg = 0; rg < metadata.RowGroups.Count; rg++)
            {
                List<LiteralValue> values;
                try
                {
                    values = Distinct(await reader.ReadRowGroupAsync(rg, [column.Path[0]]));
                }
                catch (Exception)
                {
                    break; // a type or feature this reader does not read; not this test's subject
                }
                if (values.Count == 0)
                    continue;

                var name = column.Path[0];
                foreach (var value in values.Take(ValuesPerChunk))
                {
                    var filter = Ex.Equal(name, value);
                    Assert.True(await CandidateAsync(path, filter, rg, dictionaries: true),
                        $"{fileName} row group {rg}: {name} = {value} is present but was ruled out");
                    present++;

                    if (Neighbour(value) is { } absent && !values.Contains(absent))
                    {
                        var probe = Ex.Equal(name, absent);
                        if (!await CandidateAsync(path, probe, rg, dictionaries: true)
                            && await CandidateAsync(path, probe, rg, dictionaries: false))
                            prunes++;
                    }
                }
            }
        }
        return (present, prunes);
    }

    private static async Task<bool> CandidateAsync(string path, Predicate filter, int rowGroup, bool dictionaries)
    {
        await using var input = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(input, ownsFile: false,
            new ParquetReadOptions { FilterUseDictionaries = dictionaries });
        return (await reader.GetCandidateRowGroupsAsync(filter))[rowGroup];
    }

    private static List<LiteralValue> Distinct(RecordBatch batch)
    {
        var values = new List<LiteralValue>();
        var column = batch.Column(0);
        for (int i = 0; i < batch.Length; i++)
        {
            if (column.IsNull(i))
                continue;
            LiteralValue? value = column switch
            {
                Int32Array a => (LiteralValue?)LiteralValue.Of(a.GetValue(i)!.Value),
                Int64Array a => LiteralValue.Of(a.GetValue(i)!.Value),
                StringArray a => LiteralValue.Of(a.GetString(i)),
                BinaryArray a => LiteralValue.Of(a.GetBytes(i).ToArray()),
                _ => null,
            };
            if (value is null)
                return [];
            if (!values.Contains(value.Value))
                values.Add(value.Value);
        }
        return values;
    }

    /// <summary>A value next to <paramref name="value"/>, so that it usually lies inside the chunk's range.</summary>
    private static LiteralValue? Neighbour(LiteralValue value) => value.Type switch
    {
        LiteralValue.Kind.Int32 when value.AsInt32 < int.MaxValue => (LiteralValue?)LiteralValue.Of(value.AsInt32 + 1),
        LiteralValue.Kind.Int64 when value.AsInt64 < long.MaxValue => LiteralValue.Of(value.AsInt64 + 1),
        LiteralValue.Kind.String => LiteralValue.Of(value.AsString + "\u0001"),
        _ => null,
    };

    private static FileMetaData TestDataFooter(string fileName) =>
        MetadataDecoder.DecodeFileMetaData(TestData.ReadFooterBytes(fileName));

    private static async Task<SchemaDescriptor> FixtureSchema(string fileName)
    {
        await using var input = new LocalRandomAccessFile(TestData.GetPath(fileName));
        using var reader = new ParquetFileReader(input, ownsFile: false);
        return await reader.GetSchemaAsync();
    }

    private static int IndexOf(byte[] haystack, byte[] needle, int from)
    {
        for (int i = from; i + needle.Length <= haystack.Length; i++)
        {
            if (haystack.AsSpan(i, needle.Length).SequenceEqual(needle))
                return i;
        }
        return -1;
    }
}
