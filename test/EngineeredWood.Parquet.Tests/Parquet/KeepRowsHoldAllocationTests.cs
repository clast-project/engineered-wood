// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Arrow;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

#pragma warning disable EWPARQUET0003 // FSST, an evaluation encoding: the fixtures use it as a decoder that does not skip

namespace EngineeredWood.Tests.Parquet;

/// <summary>
/// A split group's keep-rows second pass holds budget for what it decodes (#873, sized to the kept rows since #880).
/// These read it over varied columns and check that the native memory the read allocates stays within the budget it
/// reserved: the property the hold exists for, measured rather than computed from the hold's own formula
/// (#886 review).
/// </summary>
[Collection(NativeMemoryCollection.Name)]
public sealed class KeepRowsHoldAllocationTests : IDisposable
{
    private const int Rows = 150_000;
    private const int PageRows = 50_000;

    private readonly string _tempDir =
        Path.Combine(Path.GetTempPath(), "ew-keeprows-hold-" + Guid.NewGuid().ToString("N")[..8]);

    public KeepRowsHoldAllocationTests() => Directory.CreateDirectory(_tempDir);

    public void Dispose()
    {
        try
        {
            Directory.Delete(_tempDir, recursive: true);
        }
        catch (IOException)
        {
        }
    }

    public static IEnumerable<object[]> Cases()
    {
        string[] columns =
        [
            "int32-nullable", "boolean", "decimal-flba", "short-strings", "short-strings-view", "long-strings-large",
            "uneven-delta", "uneven-fsst", "uneven-plain", "prefix-delta",
        ];
        foreach (string column in columns)
        {
            foreach (bool prefetched in new[] { true, false })
                yield return [column, prefetched];
        }
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task AKeepRowsSecondPass_AllocatesNoMoreThanItsBudgetReserved(string column, bool prefetched)
    {
        string path = await WriteAsync(column);
        var output = column switch
        {
            "short-strings-view" => ByteArrayOutputKind.ViewType,
            "long-strings-large" => ByteArrayOutputKind.LargeOffsets,
            _ => ByteArrayOutputKind.Default,
        };
        await using var file = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(
            file, ownsFile: false, new ParquetReadOptions { BatchSize = 5_000, ByteArrayOutput = output })
        {
            TwoPassReads = true,
            // Not fetched ahead: the hold takes the column whole, without reading its page headers.
            MaxPrefetchRangeBytes = prefetched ? 0x7FFFFFC7 : 1,
        };
        var group = (await reader.ReadMetadataAsync()).RowGroups[0];
        var parquetSchema = await reader.GetSchemaAsync();
        // The chunks the read holds are managed buffers, which the native tracker does not see: the budget's other
        // bytes are for the decodes. (Pass 1 reads k, pass 2 reads k and v.)
        long chunks = ParquetFileReader.PlannedBytes(group, parquetSchema, ["k"], null).Compressed
                      + ParquetFileReader.PlannedBytes(group, parquetSchema, ["k", "v"], null).Compressed;

        // One row in 100, a row in every block, within one 5,000-row batch: the second pass keeps rows.
        var predicate = ParallelRowGroupReadTests.KeyPredicate(k => k < 1, false);
        NativeMemoryTracker.Reset();
        var (returned, _, peak, used, held) = await ParallelRowGroupReadTests.ReadTwoPassUnitHeldAsync(
            reader, predicate, returnPredicateColumn: true);
        long allocated = NativeMemoryTracker.PeakBytes;

        Assert.Equal(Rows / 100, returned);
        Assert.Equal(0, used);
        Assert.True(held > 0, $"{column}: the second pass held nothing, so it did not keep rows in one batch");
        Assert.True(allocated <= peak - chunks,
            $"{column} (prefetched: {prefetched}): the read allocated {allocated} native bytes, past the " +
            $"{peak - chunks} its budget reserved at most for decoding (peak {peak}, chunks {chunks}, held {held})");
    }

    // One row group of k (int64, i % 100) and v, in 50,000-row pages.
    private async Task<string> WriteAsync(string column)
    {
        var random = new Random(886);
        var keys = new Int64Array.Builder();
        for (int i = 0; i < Rows; i++)
            keys.Append(i % 100);

        IArrowArray values;
        IArrowType type;
        ByteArrayEncoding? encoding = null;
        bool dictionary = true;
        switch (column)
        {
            case "int32-nullable":
            {
                var b = new Int32Array.Builder();
                for (int i = 0; i < Rows; i++)
                {
                    if (i % 3 == 0)
                        b.AppendNull();
                    else
                        b.Append(random.Next());
                }
                (values, type) = (b.Build(), Int32Type.Default);
                break;
            }
            case "boolean":
            {
                var b = new BooleanArray.Builder();
                for (int i = 0; i < Rows; i++)
                    b.Append(random.Next(2) == 0);
                (values, type) = (b.Build(), BooleanType.Default);
                break;
            }
            case "decimal-flba":
            {
                var decimalType = new Decimal128Type(20, 2);
                var b = new Decimal128Array.Builder(decimalType);
                for (int i = 0; i < Rows; i++)
                    b.Append((decimal)random.Next() / 100);
                (values, type) = (b.Build(), decimalType);
                break;
            }
            case "short-strings" or "short-strings-view":
                (values, type) = (Strings(i => 1, random), StringType.Default);
                break;
            case "long-strings-large":
                dictionary = false;
                (values, type) = (Strings(i => 200, random), StringType.Default);
                break;
            case "prefix-delta":
            {
                // Random 200-character values, then a last page of 2,000-character values sharing all but their last
                // character: DELTA_BYTE_ARRAY encodes that page in a few bytes a value and decodes it to 20 MB, far
                // past the column's average ratio of decoded to encoded bytes.
                dictionary = false;
                encoding = ByteArrayEncoding.DeltaByteArray;
                string prefix = new('p', 1_999);
                var b = new StringArray.Builder();
                var chars = new char[200];
                for (int i = 0; i < Rows; i++)
                {
                    if (i >= Rows - 10_000)
                    {
                        b.Append(prefix + (char)('a' + random.Next(26)));
                        continue;
                    }
                    for (int c = 0; c < chars.Length; c++)
                        chars[c] = (char)('a' + random.Next(26));
                    b.Append(new string(chars));
                }
                (values, type) = (b.Build(), StringType.Default);
                break;
            }
            default: // uneven-*: 2,000-character values in the last page only
                dictionary = false;
                encoding = column switch
                {
                    "uneven-delta" => ByteArrayEncoding.DeltaByteArray,
                    "uneven-fsst" => ByteArrayEncoding.Fsst,
                    _ => null,
                };
                (values, type) = (Strings(i => i >= Rows - 10_000 ? 2_000 : 1, random), StringType.Default);
                break;
        }

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("k", Int64Type.Default, false))
            .Field(new Field("v", type, column == "int32-nullable"))
            .Build();
        string path = Path.Combine(_tempDir, column + ".parquet");
        using (var batch = new RecordBatch(schema, [keys.Build(), values], Rows))
        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false, new ParquetWriteOptions
                     {
                         DataPageSize = 256 << 20,
                         DataPageRowCountLimit = PageRows,
                         DictionaryEnabled = dictionary,
                         ColumnEncodings = encoding is { } e
                             ? new Dictionary<string, ByteArrayEncoding> { ["v"] = e }
                             : null,
                     }))
        {
            await writer.WriteRowGroupAsync(batch);
            await writer.CloseAsync();
        }
        return path;
    }

    private static StringArray Strings(Func<int, int> length, Random random)
    {
        var b = new StringArray.Builder();
        for (int i = 0; i < Rows; i++)
        {
            var chars = new char[length(i)];
            for (int c = 0; c < chars.Length; c++)
                chars[c] = (char)('a' + random.Next(26));
            b.Append(new string(chars));
        }
        return b.Build();
    }
}
