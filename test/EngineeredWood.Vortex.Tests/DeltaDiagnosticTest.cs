// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;
using EngineeredWood.Vortex.Encodings;
using EngineeredWood.Vortex.Tests.TestData;
using Xunit.Abstractions;

namespace EngineeredWood.Vortex.Tests;

/// <summary>
/// One-shot diagnostic for the fastlanes.delta layout question. Reads the
/// raw stored deltas from <c>delta_diag.vortex</c> (input was [0..1023] u32)
/// and dumps the first 32 deltas to distinguish:
/// <list type="bullet">
///   <item>[0, 1, 1, …, 1] → vortex stores deltas in lane-major (no transpose), Clast reads them as UTL → mismatch.</item>
///   <item>[0, 32, 32, …, 32] → vortex transpose-then-delta, stores in lane-major UTL — Clast needs only an untranspose at the boundary.</item>
///   <item>[0, 1, 2, …, 31] → already in UTL — should round-trip via Clast directly.</item>
/// </list>
/// </summary>
public class DeltaDiagnosticTest
{
    private readonly ITestOutputHelper _output;
    public DeltaDiagnosticTest(ITestOutputHelper output) { _output = output; }

    [Fact]
    public async Task DumpStoredDeltasFromDiagFixture()
    {
        var path = TestDataPath.Resolve("delta_diag.vortex");
        await using var reader = await VortexFileReader.OpenAsync(path);

        // The column plan should resolve to a single segment with the delta-encoded array.
        var plan = (EngineeredWood.Vortex.Layouts.FlatColumnPlan)reader.ColumnPlans[0];
        var chunk = plan.Chunks[0];
        var locator = reader.SegmentSpecs[(int)chunk.SegmentRef];

        using var local = new LocalRandomAccessFile(path);
        using var owner = await local.ReadAsync(
            new FileRange(checked((long)locator.Offset), checked((int)locator.Length)));
        var serialized = SerializedArray.Parse(owner.Memory.Span);

        var root = serialized.Message.Root;
        var rootEnc = reader.ArraySpecs[root.EncodingIndex];
        _output.WriteLine($"Root encoding: {rootEnc}");
        _output.WriteLine($"Root metadata bytes: {root.Metadata.Length}");

        // For fastlanes.delta we expect 2 children: bases (child 0), deltas (child 1).
        Assert.Equal("fastlanes.delta", rootEnc);
        Assert.Equal(2, root.ChildCount);

        var basesNode = root.Child(0);
        var deltasNode = root.Child(1);
        _output.WriteLine($"Bases encoding: {reader.ArraySpecs[basesNode.EncodingIndex]} buffers={basesNode.BufferRefCount}");
        _output.WriteLine($"Deltas encoding: {reader.ArraySpecs[deltasNode.EncodingIndex]} buffers={deltasNode.BufferRefCount}");

        // Pull the raw bytes of the deltas buffer.
        var deltaBufRef = deltasNode.BufferRef(0);
        var deltaBytes = serialized.BufferBytes(deltaBufRef);
        _output.WriteLine($"Deltas buffer size: {deltaBytes.Length} bytes (expect 4096 for 1024×u32)");

        // Pull bases too.
        var basesBufRef = basesNode.BufferRef(0);
        var basesBytes = serialized.BufferBytes(basesBufRef);
        _output.WriteLine($"Bases buffer size: {basesBytes.Length} bytes");

        var deltas = new uint[Math.Min(64, deltaBytes.Length / 4)];
        for (int i = 0; i < deltas.Length; i++)
            deltas[i] = BinaryPrimitives.ReadUInt32LittleEndian(deltaBytes.Slice(i * 4));

        var bases = new uint[Math.Min(64, basesBytes.Length / 4)];
        for (int i = 0; i < bases.Length; i++)
            bases[i] = BinaryPrimitives.ReadUInt32LittleEndian(basesBytes.Slice(i * 4));

        _output.WriteLine($"bases[0..32]: {string.Join(",", bases.Take(32))}");
        _output.WriteLine($"deltas[0..32]: {string.Join(",", deltas.Take(32))}");
        _output.WriteLine($"deltas[32..64]: {string.Join(",", deltas.Skip(32).Take(32))}");
    }

    [Fact]
    public async Task DecodesDiagFixtureToArithmeticSequence()
    {
        var path = TestDataPath.Resolve("delta_diag.vortex");
        await using var reader = await VortexFileReader.OpenAsync(path);
        var array = await reader.ReadColumnAsync(0);
        var u32 = Assert.IsType<Apache.Arrow.UInt32Array>(array);
        Assert.Equal(1024, u32.Length);
        for (int i = 0; i < 1024; i++)
            Assert.Equal((uint)i, u32.GetValue(i)!.Value);
    }
}
