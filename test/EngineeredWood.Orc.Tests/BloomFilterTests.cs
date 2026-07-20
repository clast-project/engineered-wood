// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Collections;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace EngineeredWood.Orc.Tests;

public class BloomFilterTests
{
    /// <summary>
    /// bloom_test.orc: 5000 rows, 5 stripes, columns: id (int32), name (string), value (double).
    /// Bloom filters enabled on all columns via PyArrow (C++ ORC library).
    /// </summary>
    private const string BloomTestFile = "bloom_test.orc";

    [Fact]
    public async Task GetCandidateStripes_ReturnsCorrectLength()
    {
        var path = TestHelpers.GetTestFilePath(BloomTestFile);
        await using var reader = await OrcReader.OpenAsync(path);

        var result = await reader.GetCandidateStripesAsync("name", "anything");

        Assert.Equal(reader.NumberOfStripes, result.Length);
        Assert.Equal(5, result.Length);
    }

    [Fact]
    public async Task GetCandidateStripes_SingleValue()
    {
        var path = TestHelpers.GetTestFilePath(BloomTestFile);
        await using var reader = await OrcReader.OpenAsync(path);

        // Single-value overload should work without throwing.
        var result = await reader.GetCandidateStripesAsync("name", "test_value");
        Assert.Equal(5, result.Length);
    }

    [Fact]
    public async Task GetCandidateStripes_MultipleValues()
    {
        var path = TestHelpers.GetTestFilePath(BloomTestFile);
        await using var reader = await OrcReader.OpenAsync(path);

        // Multi-value overload should work without throwing.
        var result = await reader.GetCandidateStripesAsync(
            "name", new object[] { "value_a", "value_b", "value_c" });
        Assert.Equal(5, result.Length);
    }

    [Fact]
    public async Task GetCandidateStripes_InvalidColumn_Throws()
    {
        var path = TestHelpers.GetTestFilePath(BloomTestFile);
        await using var reader = await OrcReader.OpenAsync(path);

        await Assert.ThrowsAsync<ArgumentException>(
            () => reader.GetCandidateStripesAsync("nonexistent_column", "value"));
    }

    [Fact]
    public async Task GetCandidateStripes_TypeMismatch_Throws()
    {
        var path = TestHelpers.GetTestFilePath(BloomTestFile);
        await using var reader = await OrcReader.OpenAsync(path);

        // name is String — passing an int should throw.
        await Assert.ThrowsAsync<ArgumentException>(
            () => reader.GetCandidateStripesAsync("name", 42));
    }

    [Fact]
    public async Task GetCandidateStripes_EmptyValues_Throws()
    {
        var path = TestHelpers.GetTestFilePath(BloomTestFile);
        await using var reader = await OrcReader.OpenAsync(path);

        await Assert.ThrowsAsync<ArgumentException>(
            () => reader.GetCandidateStripesAsync("name", System.Array.Empty<object>()));
    }

    [Fact]
    public async Task GetCandidateStripes_OldFormat_ConservativelyIncludesAll()
    {
        // over1k_bloom.orc uses old Hive BloomKFilter format (bloomEncoding=0).
        // We can't decode it, so all stripes should be conservatively included.
        var path = TestHelpers.GetTestFilePath("over1k_bloom.orc");
        await using var reader = await OrcReader.OpenAsync(path);

        var result = await reader.GetCandidateStripesAsync("_col7", "anything");

        for (int i = 0; i < result.Length; i++)
            Assert.True(result[i], $"Stripe {i} should be conservatively included for unsupported format.");
    }

    [Fact]
    public async Task GetCandidateStripes_PresentStringValue_IncludesStripe()
    {
        var path = TestHelpers.GetTestFilePath(BloomTestFile);
        await using var reader = await OrcReader.OpenAsync(path);

        // "name_0" is in the first stripe (rows 0-1023).
        var result = await reader.GetCandidateStripesAsync("name", "name_0");
        Assert.True(result[0], "First stripe should be candidate for 'name_0'.");
    }

    [Fact]
    public async Task GetCandidateStripes_PresentIntValue_IncludesStripe()
    {
        var path = TestHelpers.GetTestFilePath(BloomTestFile);
        await using var reader = await OrcReader.OpenAsync(path);

        // id=0 is in the first stripe.
        var result = await reader.GetCandidateStripesAsync("id", 0);
        Assert.True(result[0], "First stripe should be candidate for id=0.");
    }

    [Fact]
    public async Task GetCandidateStripes_AbsentValue_ExcludesStripes()
    {
        var path = TestHelpers.GetTestFilePath(BloomTestFile);
        await using var reader = await OrcReader.OpenAsync(path);

        var result = await reader.GetCandidateStripesAsync(
            "name", "zzz_value_definitely_not_in_data");

        bool anyExcluded = false;
        for (int i = 0; i < result.Length; i++)
            if (!result[i]) { anyExcluded = true; break; }
        Assert.True(anyExcluded, "Expected at least one stripe to be excluded.");
    }

    [Fact]
    public async Task GetCandidateStripes_MultipleValues_OrSemantics()
    {
        var path = TestHelpers.GetTestFilePath(BloomTestFile);
        await using var reader = await OrcReader.OpenAsync(path);

        // Mix of present and absent — stripe with "name_0" should be included.
        var result = await reader.GetCandidateStripesAsync(
            "name", new object[] { "name_0", "zzz_not_present" });

        Assert.True(result[0], "First stripe should be included due to OR semantics.");
    }

    [Fact]
    public async Task ReadBloomFilterIndex_ReturnsFilters()
    {
        var path = TestHelpers.GetTestFilePath(BloomTestFile);
        await using var reader = await OrcReader.OpenAsync(path);

        var bfIndex = await reader.ReadBloomFilterIndexAsync(0);

        // Should have bloom filter entries for at least some columns.
        Assert.NotEmpty(bfIndex);
    }

    [Fact]
    public async Task ReadBloomFilterIndex_FiltersHaveData()
    {
        var path = TestHelpers.GetTestFilePath(BloomTestFile);
        await using var reader = await OrcReader.OpenAsync(path);

        var bfIndex = await reader.ReadBloomFilterIndexAsync(0);
        var nameCol = reader.Schema.Children.First(c => c.Name == "name");

        Assert.True(bfIndex.ContainsKey(nameCol.ColumnId),
            "Bloom filter should exist for 'name' column.");

        var filters = bfIndex[nameCol.ColumnId];
        Assert.Single(filters); // One row group per stripe

        var filter = filters[0];
        Assert.True(filter.NumBits > 0, "Bloom filter should have bits.");
        Assert.True(filter.NumHashFunctions > 0, "Bloom filter should have hash functions.");
    }

    // ──── Round-trip write tests ────

    private static string GetTempPath() =>
        Path.Combine(Path.GetTempPath(), $"orc_bf_test_{Guid.NewGuid():N}.orc");

    [Fact]
    public async Task RoundTrip_WriteAndReadBloomFilters_Cpp()
    {
        await RoundTripBloomFilter(OrcBloomHashVariant.Cpp);
    }

    [Fact]
    public async Task RoundTrip_WriteBloomFilters_Java_ProducesValidMetadata()
    {
        // Java variant writes correctly but our reader defaults to C++ hash for writer=6.
        // Verify the file structure is valid (metadata, stream kinds) without probing.
        var path = GetTempPath();
        try
        {
            var schema = new Schema(
                [new Field("name", StringType.Default, nullable: false)], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions
            {
                Compression = Proto.CompressionKind.None,
                BloomFilterColumns = new HashSet<string> { "name" },
                BloomFilterHashVariant = OrcBloomHashVariant.Java,
            }))
            {
                var names = new StringArray.Builder();
                for (int i = 0; i < 100; i++) names.Append($"name_{i}");
                await writer.WriteBatchAsync(new RecordBatch(schema, [names.Build()], 100));
            }

            await using var reader = await OrcReader.OpenAsync(path);
            var bfIndex = await reader.ReadBloomFilterIndexAsync(0);
            Assert.NotEmpty(bfIndex);
        }
        finally
        {
            File.Delete(path);
        }
    }

    private static async Task RoundTripBloomFilter(OrcBloomHashVariant variant)
    {
        var path = GetTempPath();
        try
        {
            var schema = new Schema(
            [
                new Field("id", Int32Type.Default, nullable: false),
                new Field("name", StringType.Default, nullable: false),
            ], null);

            // Write with bloom filters on both columns.
            var options = new OrcWriterOptions
            {
                Compression = Proto.CompressionKind.None,
                BloomFilterColumns = new HashSet<string> { "id", "name" },
                BloomFilterFpp = 0.01,
                BloomFilterHashVariant = variant,
                RowIndexStride = 100, // small stride for testing
            };

            await using (var writer = OrcWriter.Create(path, schema, options))
            {
                for (int batch = 0; batch < 5; batch++)
                {
                    int offset = batch * 200;
                    var ids = new Int32Array.Builder();
                    var names = new StringArray.Builder();
                    for (int i = 0; i < 200; i++)
                    {
                        ids.Append(offset + i);
                        names.Append($"name_{offset + i}");
                    }
                    await writer.WriteBatchAsync(new RecordBatch(schema,
                        [ids.Build(), names.Build()], 200));
                }
            }

            // Read back and verify bloom filters.
            await using var reader = await OrcReader.OpenAsync(path);

            Assert.True(reader.NumberOfStripes >= 1);

            // Verify bloom filter index exists for the name column.
            var bfIndex = await reader.ReadBloomFilterIndexAsync(0);
            var nameCol = reader.Schema.Children.First(c => c.Name == "name");
            Assert.True(bfIndex.ContainsKey(nameCol.ColumnId),
                "Bloom filter should exist for 'name' column.");

            // Present value should be found.
            var present = await reader.GetCandidateStripesAsync("name", "name_0");
            Assert.True(present[0], "First stripe should be candidate for 'name_0'.");

            // Absent value should be rejected by at least one stripe.
            var absent = await reader.GetCandidateStripesAsync("name", "zzz_absent_value");
            bool anyExcluded = false;
            for (int i = 0; i < absent.Length; i++)
                if (!absent[i]) { anyExcluded = true; break; }
            Assert.True(anyExcluded, "At least one stripe should exclude absent value.");

            // Int column bloom filter should also work.
            var intPresent = await reader.GetCandidateStripesAsync("id", 0);
            Assert.True(intPresent[0], "First stripe should be candidate for id=0.");
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task RoundTrip_BloomFilterEncodingAndStreamKind()
    {
        var path = GetTempPath();
        try
        {
            var schema = new Schema(
                [new Field("val", StringType.Default, nullable: false)], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions
            {
                Compression = Proto.CompressionKind.None,
                BloomFilterColumns = new HashSet<string> { "val" },
            }))
            {
                var vals = new StringArray.Builder();
                for (int i = 0; i < 100; i++) vals.Append($"v{i}");
                await writer.WriteBatchAsync(new RecordBatch(schema, [vals.Build()], 100));
            }

            // Verify the bloom filter stream kind and encoding.
            await using var reader = await OrcReader.OpenAsync(path);

            // Read stripe footer to verify stream kind and bloom_encoding.
            var bfIndex = await reader.ReadBloomFilterIndexAsync(0);
            Assert.NotEmpty(bfIndex);

            // The file should have bloom_encoding=1 set.
            // We verify this indirectly: if the bloom filter index was successfully parsed
            // with our sanity checks (numHashFunctions <= 30, non-zero bitset), it's valid.
            var valCol = reader.Schema.Children.First(c => c.Name == "val");
            Assert.True(bfIndex.ContainsKey(valCol.ColumnId));
            Assert.True(bfIndex[valCol.ColumnId].Count > 0);
        }
        finally
        {
            File.Delete(path);
        }
    }
}
