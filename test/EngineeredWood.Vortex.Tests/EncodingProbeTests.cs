// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.IO;
using EngineeredWood.IO.Local;
using EngineeredWood.Vortex.Encodings;
using EngineeredWood.Vortex.Tests.TestData;
using Xunit.Abstractions;

namespace EngineeredWood.Vortex.Tests;

/// <summary>
/// Probe tests: open a fixture, fetch its first segment, parse the
/// SerializedArray, and dump the root ArrayNode encoding id. Used during
/// chunk-8 work to figure out which Vortex encoding the writer chose.
/// </summary>
public class EncodingProbeTests
{
    private readonly ITestOutputHelper _output;

    public EncodingProbeTests(ITestOutputHelper output) { _output = output; }

    [Theory]
    [InlineData("struct_int_3rows.vortex")]
    [InlineData("primitive_int_random.vortex")]
    [InlineData("constant_int_5rows.vortex")]
    [InlineData("nullable_int_6rows.vortex")]
    [InlineData("string_col_5rows.vortex")]
    [InlineData("bitpacked_int_64rows.vortex")]
    [InlineData("bitpacked_int_2048rows.vortex")]
    [InlineData("for_int_2048rows.vortex")]
    [InlineData("alp_double_2048rows.vortex")]
    [InlineData("alprd_double_2048rows.vortex")]
    [InlineData("nullable_bitpacked_2048rows.vortex")]
    [InlineData("nullable_alp_2048rows.vortex")]
    [InlineData("bitpacked_patches_2048rows.vortex")]
    [InlineData("alp_patches_2048rows.vortex")]
    [InlineData("decimal128_2048rows.vortex")]
    [InlineData("timestamp_us_2048rows.vortex")]
    [InlineData("date_days_2048rows.vortex")]
    [InlineData("time_us_2048rows.vortex")]
    [InlineData("fsl_int_2048rows.vortex")]
    [InlineData("list_int_2048rows.vortex")]
    [InlineData("delta_int_2048rows.vortex")]
    [InlineData("dict_int_64rows.vortex")]
    [InlineData("fsst_string_64rows.vortex")]
    public async Task DumpsTopLevelEncodingForFixture(string fixture)
    {
        var path = TestDataPath.Resolve(fixture);
        await using var reader = await VortexFileReader.OpenAsync(path);

        // Dump every segment's array tree.
        _output.WriteLine($"{fixture}:");
        using var local = new LocalRandomAccessFile(path);
        for (int i = 0; i < reader.SegmentSpecs.Count; i++)
        {
            var locator = reader.SegmentSpecs[i];
            using var owner = await local.ReadAsync(
                new FileRange(checked((long)locator.Offset), checked((int)locator.Length)));
            var raw = owner.Memory.Span;
            var serialized = SerializedArray.Parse(raw);
            _output.WriteLine($"  segment {i} (offset={locator.Offset} len={locator.Length}):");
            DumpNode(serialized.Message.Root, reader.ArraySpecs, depth: 2);
        }
    }

    private void DumpNode(EngineeredWood.Vortex.Format.ArrayNode node, IReadOnlyList<string> arraySpecs, int depth)
    {
        var indent = new string(' ', depth * 2);
        var encId = arraySpecs[node.EncodingIndex];
        _output.WriteLine(
            $"{indent}{encId} buffers={node.BufferRefCount} children={node.ChildCount} meta={node.Metadata.Length}B");
        for (int i = 0; i < node.ChildCount; i++)
            DumpNode(node.Child(i), arraySpecs, depth + 1);
    }
}
