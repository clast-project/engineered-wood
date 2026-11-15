// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using EngineeredWood.Arrow;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using Xunit.Abstractions;

namespace EngineeredWood.Tests.Parquet;

/// <summary>
/// Keeping rows as they are decoded (#502 phase 2, <c>ColumnBuildState.SetRowSelection</c>) against building every row and
/// gathering afterwards (<see cref="ArrowCompute.Take(IArrowArray, ReadOnlySpan{int})"/>), over every flat column of
/// every readable parquet-testing fixture: every encoding and value layout real writers produce, under each
/// BYTE_ARRAY output kind, with nulls where the fixtures have them.
/// </summary>
public class KeepRowsTests(ITestOutputHelper output)
{
    public static IEnumerable<object[]> OutputKinds() =>
    [
        [ByteArrayOutputKind.Default],
        [ByteArrayOutputKind.ViewType],
        [ByteArrayOutputKind.LargeOffsets],
    ];

    [Theory]
    [MemberData(nameof(OutputKinds))]
    public async Task KeepingRowsAsTheyAreDecodedEqualsGatheringThemAfterwards(ByteArrayOutputKind byteArrayOutput)
    {
        int files = 0, columns = 0;
        var failures = new List<string>();
        foreach (var path in TestData.GetAllParquetFiles())
        {
            string name = Path.GetFileName(path);
            if (name.Contains("encrypt", StringComparison.OrdinalIgnoreCase)
                || name.Contains("malformed", StringComparison.OrdinalIgnoreCase)
                || name == "large_string_map.brotli.parquet"
                || name == "fixed_length_byte_array.parquet")
            {
                continue;
            }

            await using var file = new LocalRandomAccessFile(path);
            using var reader = new ParquetFileReader(file, ownsFile: false, new ParquetReadOptions { ByteArrayOutput = byteArrayOutput });

            List<string> flat;
            RecordBatch whole;
            try
            {
                var metadata = await reader.ReadMetadataAsync();
                if (metadata.RowGroups.Count == 0 || metadata.RowGroups[0].NumRows == 0)
                    continue;
                var schema = await reader.GetSchemaAsync();
                flat = schema.Root.Children
                    .Where(c => c.IsLeaf && c.Element.RepetitionType != EngineeredWood.Parquet.FieldRepetitionType.Repeated)
                    .Select(c => c.Name)
                    .ToList();
                if (flat.Count == 0)
                    continue;
                whole = await reader.ReadRowGroupAsync(0, flat);
            }
            catch (Exception e) when (e is not Xunit.Sdk.XunitException)
            {
                continue; // out of reach for reasons that have nothing to do with keeping rows
            }

            using (whole)
            {
                files++;
                int rows = whole.Length;
                foreach (var (label, keep) in Masks(rows, seed: name.GetHashCode() & 0x7fffffff))
                {
                    var indices = Enumerable.Range(0, rows).Where(r => keep[r] != 0).ToArray();
                    RecordBatch keptBatch;
                    try
                    {
                        keptBatch = await reader.ReadRowGroupKeepingAsync(0, flat, keep);
                    }
                    catch (Exception e) when (e is not Xunit.Sdk.XunitException)
                    {
                        failures.Add($"{name} {label}: {e.GetType().Name}: {e.Message}");
                        continue;
                    }

                    using var kept = keptBatch;
                    if (kept.Length != indices.Length)
                    {
                        failures.Add($"{name} {label}: {kept.Length} rows kept, {indices.Length} expected");
                        continue;
                    }

                    for (int c = 0; c < whole.ColumnCount; c++)
                    {
                        // Not disposed: a gathered view column shares the whole batch's data buffers, which the
                        // next mask's gather still reads.
                        var expected = ArrowCompute.Take(whole.Column(c), indices);
                        var actual = kept.Column(c);
                        if (!expected.Data.DataType.Equals(actual.Data.DataType)
                            && expected.Data.DataType.TypeId != actual.Data.DataType.TypeId)
                        {
                            failures.Add($"{name}.{flat[c]} {label}: type {actual.Data.DataType} vs {expected.Data.DataType}");
                            continue;
                        }

                        for (int r = 0; r < indices.Length; r++)
                        {
                            string want, got;
                            try
                            {
                                want = ArrowValues.Render(expected, r);
                                got = ArrowValues.Render(actual, r);
                            }
                            catch (NotSupportedException)
                            {
                                break; // a type the renderer cannot show; the row count above still held
                            }

                            if (want != got)
                            {
                                failures.Add($"{name}.{flat[c]} {label} row {r}: {got} vs {want}");
                                break;
                            }
                        }

                        columns++;
                    }
                }
            }
        }

        output.WriteLine($"{files} files, {columns} column comparisons, {byteArrayOutput}");
        Assert.True(files > 20, $"only {files} fixtures were compared");
        Assert.True(failures.Count == 0, string.Join("\n", failures.Take(30)));
    }

    /// <summary>
    /// A view-typed column's null count matches its rows. DELTA_BYTE_ARRAY, DELTA_LENGTH_BYTE_ARRAY and FSST pages
    /// counted every value twice in view output (ColumnBuildState.AddByteArrayValues), and the build derives the null
    /// count from that count; found while adding phase 2a's in-place compaction, which relies on it.
    /// </summary>
    [Fact]
    public async Task AViewColumnsNullCountMatchesItsRows()
    {
        int columns = 0;
        var failures = new List<string>();
        foreach (var path in TestData.GetAllParquetFiles())
        {
            string name = Path.GetFileName(path);
            if (name.Contains("encrypt", StringComparison.OrdinalIgnoreCase)
                || name.Contains("malformed", StringComparison.OrdinalIgnoreCase)
                || name == "large_string_map.brotli.parquet"
                || name == "fixed_length_byte_array.parquet")
            {
                continue;
            }

            RecordBatch batch;
            try
            {
                await using var file = new LocalRandomAccessFile(path);
                using var reader = new ParquetFileReader(file, ownsFile: false,
                    new ParquetReadOptions { ByteArrayOutput = ByteArrayOutputKind.ViewType });
                if ((await reader.ReadMetadataAsync()).RowGroups.Count == 0)
                    continue;
                batch = await reader.ReadRowGroupAsync(0);
            }
            catch (Exception e) when (e is not Xunit.Sdk.XunitException)
            {
                continue;
            }

            using (batch)
            {
                for (int c = 0; c < batch.ColumnCount; c++)
                {
                    if (batch.Column(c) is not BinaryViewArray views)
                        continue;
                    columns++;
                    int nulls = Enumerable.Range(0, views.Length).Count(views.IsNull);
                    if (views.NullCount != nulls)
                        failures.Add($"{name}.{batch.Schema.GetFieldByIndex(c).Name}: NullCount {views.NullCount}, {nulls} null rows");
                }
            }
        }

        Assert.True(columns > 5, $"only {columns} view columns were read");
        Assert.True(failures.Count == 0, string.Join(Environment.NewLine, failures));
    }

#pragma warning disable EWPARQUET0003 // FSST is experimental; the reader must keep rows of FSST pages all the same.
    public static IEnumerable<object[]> ManyPageEncodings()
    {
        foreach (var encoding in new[] { ByteArrayEncoding.DeltaLengthByteArray, ByteArrayEncoding.DeltaByteArray, ByteArrayEncoding.Fsst })
        {
            foreach (var output in new[] { ByteArrayOutputKind.Default, ByteArrayOutputKind.ViewType, ByteArrayOutputKind.LargeOffsets })
                yield return [encoding, output];
        }
    }
#pragma warning restore EWPARQUET0003

    /// <summary>
    /// The same comparison over many small pages of each BYTE_ARRAY encoding EW writes without a dictionary, nullable,
    /// beside a nullable boolean: selection is applied page by page, so a page past the first must compact (or skip)
    /// relative to the values earlier pages kept. DELTA_BYTE_ARRAY and FSST pages are decoded whole and compacted;
    /// DELTA_LENGTH_BYTE_ARRAY pages skip. No parquet-testing fixture has many pages of these.
    /// </summary>
    [Theory]
    [MemberData(nameof(ManyPageEncodings))]
    public async Task KeepingRowsOverManyPagesEqualsGatheringThem(ByteArrayEncoding encoding, ByteArrayOutputKind output)
    {
        const int rows = 5_000;
        string path = Path.Combine(Path.GetTempPath(), "ew-keeprows-" + Guid.NewGuid().ToString("N")[..8] + ".parquet");
        try
        {
            var random = new Random(23);
            var text = new StringArray.Builder();
            var flags = new BooleanArray.Builder();
            for (int r = 0; r < rows; r++)
            {
                if (random.Next(5) == 0)
                    text.AppendNull();
                else
                    text.Append("value-" + (r % 37).ToString(System.Globalization.CultureInfo.InvariantCulture) + new string('x', random.Next(30)));
                if (random.Next(7) == 0)
                    flags.AppendNull();
                else
                    flags.Append(random.Next(2) == 0);
            }

            var schema = new Apache.Arrow.Schema.Builder()
                .Field(new Field("text", Apache.Arrow.Types.StringType.Default, true))
                .Field(new Field("flag", Apache.Arrow.Types.BooleanType.Default, true))
                .Build();
            using (var batch = new RecordBatch(schema, [text.Build(), flags.Build()], rows))
            {
                await using var outputFile = new LocalSequentialFile(path);
                await using var writer = new ParquetFileWriter(outputFile, ownsFile: false, ParquetWriteOptions.Default with
                {
                    DictionaryEnabled = false,
                    ByteArrayEncoding = encoding,
                    DataPageRowCountLimit = 300,
                });
                await writer.WriteRowGroupAsync(batch);
                await writer.CloseAsync();
            }

            await using var file = new LocalRandomAccessFile(path);
            using var reader = new ParquetFileReader(file, ownsFile: false, new ParquetReadOptions { ByteArrayOutput = output });
            using var whole = await reader.ReadRowGroupAsync(0, ["text", "flag"]);
            foreach (var (label, keep) in Masks(rows, seed: 5))
            {
                var indices = Enumerable.Range(0, rows).Where(r => keep[r] != 0).ToArray();
                using var kept = await reader.ReadRowGroupKeepingAsync(0, ["text", "flag"], keep);
                Assert.Equal(indices.Length, kept.Length);
                for (int c = 0; c < 2; c++)
                {
                    var expected = ArrowCompute.Take(whole.Column(c), indices); // not disposed: may share view buffers
                    for (int r = 0; r < indices.Length; r++)
                        Assert.True(ArrowValues.Render(expected, r) == ArrowValues.Render(kept.Column(c), r), $"{label} column {c} row {r}");
                    Assert.Equal(expected.NullCount, kept.Column(c).NullCount);
                }
            }
        }
        finally
        {
            File.Delete(path);
        }
    }

    public static IEnumerable<object[]> LargePageCases()
    {
        foreach (bool dictionary in new[] { false, true })
        {
            foreach (var output in new[] { ByteArrayOutputKind.Default, ByteArrayOutputKind.ViewType, ByteArrayOutputKind.LargeOffsets })
                yield return [dictionary, output];
        }
    }

    /// <summary>
    /// Pages larger than the room a selected decode starts with (the kept rows and
    /// <c>ColumnChunkReader.KeepRowsPageRoom</c>, #880), so every buffer grows mid-chunk, over each physical type: the
    /// rows kept before a grow must survive it, and a boolean's grown bits must start clear.
    /// </summary>
    [Theory]
    [MemberData(nameof(LargePageCases))]
    public async Task KeepingRowsAcrossPagesLargerThanTheStartingRoomEqualsGatheringThem(bool dictionary, ByteArrayOutputKind output)
    {
        const int rows = 150_000;
        string path = Path.Combine(Path.GetTempPath(), "ew-keeprows-" + Guid.NewGuid().ToString("N")[..8] + ".parquet");
        try
        {
            var random = new Random(41);
            var ints = new Int32Array.Builder();
            var longs = new Int64Array.Builder();
            var doubles = new DoubleArray.Builder();
            var flags = new BooleanArray.Builder();
            var text = new StringArray.Builder();
            var decimals = new Decimal128Array.Builder(new Apache.Arrow.Types.Decimal128Type(20, 2));
            for (int r = 0; r < rows; r++)
            {
                ints.Append(r);
                if (r % 11 == 0) longs.AppendNull(); else longs.Append(r * 3L);
                doubles.Append(r / 4.0);
                if (r % 13 == 0) flags.AppendNull(); else flags.Append(random.Next(2) == 0);
                if (r % 7 == 0) text.AppendNull(); else text.Append("v" + (r % 500).ToString(System.Globalization.CultureInfo.InvariantCulture));
                if (r % 5 == 0) decimals.AppendNull(); else decimals.Append((r % 1000) + 0.25m);
            }

            var schema = new Apache.Arrow.Schema.Builder()
                .Field(new Field("i", Apache.Arrow.Types.Int32Type.Default, false))
                .Field(new Field("l", Apache.Arrow.Types.Int64Type.Default, true))
                .Field(new Field("d", Apache.Arrow.Types.DoubleType.Default, false))
                .Field(new Field("b", Apache.Arrow.Types.BooleanType.Default, true))
                .Field(new Field("s", Apache.Arrow.Types.StringType.Default, true))
                .Field(new Field("m", new Apache.Arrow.Types.Decimal128Type(20, 2), true))
                .Build();
            string[] names = ["i", "l", "d", "b", "s", "m"];
            using (var batch = new RecordBatch(schema,
                       [ints.Build(), longs.Build(), doubles.Build(), flags.Build(), text.Build(), decimals.Build()], rows))
            {
                await using var outputFile = new LocalSequentialFile(path);
                await using var writer = new ParquetFileWriter(outputFile, ownsFile: false, ParquetWriteOptions.Default with
                {
                    DictionaryEnabled = dictionary,
                    DataPageRowCountLimit = 50_000,
                    DataPageSize = 64 * 1024 * 1024,
                });
                await writer.WriteRowGroupAsync(batch);
                await writer.CloseAsync();
            }

            await using var file = new LocalRandomAccessFile(path);
            using var reader = new ParquetFileReader(file, ownsFile: false, new ParquetReadOptions { ByteArrayOutput = output });
            using var whole = await reader.ReadRowGroupAsync(0, names);
            var sparse = Enumerable.Range(0, rows).Select(r => (byte)(r % 997 == 0 ? 1 : 0)).ToArray();
            var lastPage = Enumerable.Range(0, rows).Select(r => (byte)(r >= 120_000 && r % 2 == 0 ? 1 : 0)).ToArray();
            foreach (var (label, keep) in Masks(rows, seed: 9).Append(("sparse", sparse)).Append(("last-page", lastPage)))
            {
                var indices = Enumerable.Range(0, rows).Where(r => keep[r] != 0).ToArray();
                using var kept = await reader.ReadRowGroupKeepingAsync(0, names, keep);
                Assert.Equal(indices.Length, kept.Length);
                for (int c = 0; c < names.Length; c++)
                {
                    var expected = ArrowCompute.Take(whole.Column(c), indices); // not disposed: may share view buffers
                    for (int r = 0; r < indices.Length; r++)
                        Assert.True(ArrowValues.Render(expected, r) == ArrowValues.Render(kept.Column(c), r), $"{label} {names[c]} row {r}");
                    Assert.Equal(expected.NullCount, kept.Column(c).NullCount);
                }
            }
        }
        finally
        {
            File.Delete(path);
        }
    }

    // Masks with different shapes: random half, first row only, all but the first, every third, none.
    private static IEnumerable<(string Label, byte[] Keep)> Masks(int rows, int seed)
    {
        var random = new Random(seed);
        yield return ("random", Enumerable.Range(0, rows).Select(_ => (byte)random.Next(2)).ToArray());
        yield return ("first", Enumerable.Range(0, rows).Select(r => (byte)(r == 0 ? 1 : 0)).ToArray());
        yield return ("all-but-first", Enumerable.Range(0, rows).Select(r => (byte)(r == 0 ? 0 : 1)).ToArray());
        yield return ("every-third", Enumerable.Range(0, rows).Select(r => (byte)(r % 3 == 0 ? 1 : 0)).ToArray());
        yield return ("none", new byte[rows]);
    }
}
