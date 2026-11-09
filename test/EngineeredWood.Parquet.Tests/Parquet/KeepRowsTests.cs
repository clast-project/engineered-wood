// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using EngineeredWood.Arrow;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using Xunit.Abstractions;

namespace EngineeredWood.Tests.Parquet;

/// <summary>
/// Keeping rows as they are decoded (#502 phase 2a, <c>ColumnBuildState.KeepRows</c>) against building every row and
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
