// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using Apache.Arrow;
using EngineeredWood.Compression;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Data;
using EngineeredWood.Parquet.Metadata;
using Encoding = EngineeredWood.Parquet.Encoding;

namespace EngineeredWood.Tests.Parquet;

/// <summary>
/// A Parquet file whose repetition levels describe more list elements than its definition levels
/// define decodes into a <see cref="ListArray"/> whose offsets run past the end of its child —
/// an array that violates Arrow's own invariants, which the reader used to hand back regardless.
/// See issue #165.
/// </summary>
/// <remarks>
/// The files are hand-built rather than checked in, because the malformation is far easier to read
/// as level arrays than as bytes, and because no writer we can invoke here produces one on purpose.
/// The list cases reproduce what DuckDB 1.5.5 writes for an Arrow <c>fixed_size_list&lt;bool&gt;[2]</c>
/// column containing null rows: one level entry per declared slot even for a NULL row, where the
/// format allows exactly one.
/// </remarks>
public class MalformedNestedLevelsTests
{
    /// <summary>
    /// <c>[null, [null, false]]</c> as DuckDB writes it: the null row spends TWO level entries.
    /// Offsets come out [0, 1, 3] over a two-element child.
    /// </summary>
    [Fact]
    public async Task DuckDbStyleNullFixedSizeListRow_IsRefused()
    {
        var ex = await ReadAsync(ThreeLevelListFile(
            repLevels: [0, 1, 0, 1],
            defLevels: [0, 0, 2, 3],
            values: [false],
            rowCount: 2));

        Assert.NotNull(ex);
        Assert.Contains("Malformed Parquet file", ex!.Message);
        Assert.Contains("column 'v'", ex.Message);
        Assert.Contains("needs 3 element(s)", ex.Message);
        Assert.Contains("only defines 2", ex.Message);
    }

    /// <summary>All-null rows: every level entry is a phantom, so the child comes out empty.</summary>
    [Fact]
    public async Task DuckDbStyleAllNullRows_AreRefused()
    {
        var ex = await ReadAsync(ThreeLevelListFile(
            repLevels: [0, 1, 0, 1],
            defLevels: [0, 0, 0, 0],
            values: [],
            rowCount: 2));

        Assert.NotNull(ex);
        Assert.Contains("needs 2 element(s)", ex!.Message);
        Assert.Contains("only defines 0", ex.Message);
    }

    /// <summary>
    /// The same malformation one schema shape over: a bare <c>repeated boolean</c> leaf, which the
    /// assembler wraps in a list on a separate code path. <c>[[], [true, false]]</c> with a second
    /// level entry spent on the empty row.
    /// </summary>
    [Fact]
    public async Task BareRepeatedLeaf_ExtraLevelForEmptyRow_IsRefused()
    {
        var ex = await ReadAsync(BareRepeatedLeafFile(
            repLevels: [0, 1, 0, 1],
            defLevels: [0, 0, 1, 1],
            values: [true, false],
            rowCount: 2));

        Assert.NotNull(ex);
        Assert.Contains("column 'v'", ex!.Message);
        Assert.Contains("needs 3 element(s)", ex.Message);
        Assert.Contains("only defines 2", ex.Message);
    }

    /// <summary>
    /// A map whose null row spends an extra level entry, the same way the list cases do. The offsets
    /// then claim two key/value entries over a single decoded one.
    /// </summary>
    [Fact]
    public async Task MapWithExtraLevelForNullRow_IsRefused()
    {
        var ex = await ReadAsync(MapFile(
            keyRepLevels: [0, 1, 0], keyDefLevels: [0, 0, 2], keys: [7],
            valueRepLevels: [0, 1, 0], valueDefLevels: [0, 0, 3], values: [true],
            rowCount: 2));

        Assert.NotNull(ex);
        Assert.Contains("column 'm'", ex!.Message);
        Assert.Contains("needs 2 key/value entry(s)", ex.Message);
        Assert.Contains("only defines 1", ex.Message);
    }

    /// <summary>The same map written correctly still reads.</summary>
    [Fact]
    public async Task WellFormedMap_StillReads()
    {
        byte[] file = MapFile(
            keyRepLevels: [0, 0], keyDefLevels: [0, 2], keys: [7],
            valueRepLevels: [0, 0], valueDefLevels: [0, 3], values: [true],
            rowCount: 2);

        var batch = await ReadBatchAsync(file);
        var map = Assert.IsType<MapArray>(batch.Column(0));

        Assert.Equal(2, map.Length);
        Assert.True(map.IsNull(0));
        Assert.Equal(1, map.ValueOffsets[2]);
    }

    /// <summary>
    /// A struct whose two leaves disagree about how many rows they hold. A different failure from the
    /// DuckDB one — the short leaf never reaches assembly — but it used to surface as a bare
    /// <see cref="IndexOutOfRangeException"/> from inside the array builder, naming nothing.
    /// </summary>
    [Fact]
    public async Task StructWithShortChildColumn_IsRefused()
    {
        var ex = await ReadAsync(StructFile(
            aDefLevels: [1, 2, 2],
            aValues: [true, false],
            bDefLevels: [2, 2],
            bValues: [true, true],
            rowCount: 3));

        Assert.NotNull(ex);
        Assert.Contains("Malformed Parquet file", ex!.Message);
        Assert.Contains("column 's.b'", ex.Message);
        Assert.Contains("holds 2 value(s)", ex.Message);
        Assert.Contains("declares 3 row(s)", ex.Message);
    }

    /// <summary>
    /// Repetition levels that start more lists than the row group has rows. One too many overwrote
    /// the end offset of the last list, which read cut short; two ran off the end of the offsets as
    /// an <see cref="IndexOutOfRangeException"/> (#426).
    /// </summary>
    [Theory]
    [InlineData(3)]
    [InlineData(4)]
    public async Task MoreListsThanRows_IsRefused(int lists)
    {
        var ex = await ReadAsync(ThreeLevelListFile(
            repLevels: new int[lists],
            defLevels: Enumerable.Repeat(3, lists).ToArray(),
            values: new bool[lists],
            rowCount: 2));

        Assert.NotNull(ex);
        Assert.Contains("column 'v' holds 2 list(s), but its repetition levels start more", ex!.Message);
    }

    /// <summary>
    /// A level above the column's maximum, which the width levels are packed at can still hold. The
    /// assembler took a repetition level past it for deeper nesting and skipped it, and a definition
    /// level past it for a value, so both read as wrong data rather than an error (#426).
    /// </summary>
    [Fact]
    public async Task ARepetitionLevelAboveTheMaximum_IsRefused()
    {
        // list<list<boolean>>: maximum repetition level 2, packed two bits wide.
        var ex = await ReadAsync(NestedListFile(
            repLevels: [0, 3],
            defLevels: [5, 5],
            values: [true, false],
            rowCount: 1));

        Assert.NotNull(ex);
        Assert.Contains("A level of 3 exceeds the column's maximum of 2", ex!.Message);

        // The same file with the level in range: one row, [[true, false]].
        var batch = await ReadBatchAsync(NestedListFile(
            repLevels: [0, 2], defLevels: [5, 5], values: [true, false], rowCount: 1));
        var outer = Assert.IsType<ListArray>(batch.Column(0));
        Assert.Equal(1, outer.Length);
        Assert.Equal(2, ((ListArray)outer.Values).GetValueLength(0));
    }

    [Fact]
    public async Task ADefinitionLevelAboveTheMaximum_IsRefused()
    {
        var ex = await ReadAsync(StructFile(
            aDefLevels: [2, 3], aValues: [true, false],
            bDefLevels: [2, 2], bValues: [true, false],
            rowCount: 2));

        Assert.NotNull(ex);
        Assert.Contains("A level of 3 exceeds the column's maximum of 2", ex!.Message);
    }

    /// <summary>
    /// Levels whose first entry continues a list rather than starting one, as in parquet-testing's
    /// bad_data/ARROW-GH-45185.parquet. The entry was counted into the first list and every list
    /// after it shifted by one, and the file read without complaint.
    /// </summary>
    [Fact]
    public async Task FirstLevelContinuingAList_IsRefused()
    {
        var ex = await ReadAsync(ThreeLevelListFile(
            repLevels: [1, 0, 1],
            defLevels: [3, 3, 3],
            values: [true, false, true],
            rowCount: 2));

        Assert.NotNull(ex);
        Assert.Contains("column 'v' begins with repetition level 1", ex!.Message);

        await using var file = new LocalRandomAccessFile(TestData.GetBadDataPath("ARROW-GH-45185.parquet"));
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        var fixture = await Assert.ThrowsAsync<ParquetFormatException>(async () => await reader.ReadRowGroupAsync(0));
        Assert.Contains("begins with repetition level 1", fixture.Message);
    }

    /// <summary>
    /// The list data written correctly — as PyArrow writes it, with ONE level entry for the null row
    /// — still reads, and produces offsets that stay inside the child.
    /// </summary>
    [Fact]
    public async Task WellFormedNullListRow_StillReads()
    {
        byte[] file = ThreeLevelListFile(
            repLevels: [0, 0, 1],
            defLevels: [0, 2, 3],
            values: [false],
            rowCount: 2);

        var batch = await ReadBatchAsync(file);
        var list = Assert.IsType<ListArray>(batch.Column(0));

        Assert.Equal(2, list.Length);
        Assert.True(list.IsNull(0));
        Assert.Equal(0, list.ValueOffsets[0]);
        Assert.Equal(0, list.ValueOffsets[1]);
        Assert.Equal(2, list.ValueOffsets[2]);
        Assert.Equal(2, list.Values.Length);
    }

    /// <summary>The bare-repeated equivalent, written correctly, still reads.</summary>
    [Fact]
    public async Task WellFormedBareRepeatedLeaf_StillReads()
    {
        byte[] file = BareRepeatedLeafFile(
            repLevels: [0, 0, 1],
            defLevels: [0, 1, 1],
            values: [true, false],
            rowCount: 2);

        var batch = await ReadBatchAsync(file);
        var list = Assert.IsType<ListArray>(batch.Column(0));

        Assert.Equal(2, list.Length);
        Assert.Equal(0, list.ValueOffsets[1]);
        Assert.Equal(2, list.ValueOffsets[2]);
        Assert.Equal(2, list.Values.Length);
    }

    /// <summary>The struct equivalent, with both leaves the same length, still reads.</summary>
    [Fact]
    public async Task WellFormedStruct_StillReads()
    {
        byte[] file = StructFile(
            aDefLevels: [1, 2, 2],
            aValues: [true, false],
            bDefLevels: [2, 2, 1],
            bValues: [true, true],
            rowCount: 3);

        var batch = await ReadBatchAsync(file);
        var structArray = Assert.IsType<StructArray>(batch.Column(0));

        Assert.Equal(3, structArray.Length);
        Assert.Equal(3, structArray.Fields[0].Length);
        Assert.Equal(3, structArray.Fields[1].Length);
    }

    // ───── Read helpers ─────

    /// <summary>
    /// Reads the file's single row group, returning the <see cref="ParquetFormatException"/> it threw
    /// or null if the read succeeded.
    /// </summary>
    private static async Task<ParquetFormatException?> ReadAsync(byte[] file)
    {
        try
        {
            await ReadBatchAsync(file);
            return null;
        }
        catch (ParquetFormatException ex)
        {
            return ex;
        }
    }

    private static async Task<RecordBatch> ReadBatchAsync(byte[] file)
    {
        string path = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName() + ".parquet");
        File.WriteAllBytes(path, file);
        try
        {
            await using var randomAccess = new LocalRandomAccessFile(path);
            await using var reader = new ParquetFileReader(randomAccess, ownsFile: false);
            return await reader.ReadRowGroupAsync(0);
        }
        finally
        {
            File.Delete(path);
        }
    }

    // ───── File shapes ─────

    /// <summary>
    /// <c>optional group v (LIST) { repeated group list { optional boolean element; } }</c>.
    /// </summary>
    private static byte[] ThreeLevelListFile(
        int[] repLevels, int[] defLevels, bool[] values, int rowCount)
    {
        List<SchemaElement> schema =
        [
            new() { Name = "root", NumChildren = 1 },
            new()
            {
                Name = "v",
                RepetitionType = FieldRepetitionType.Optional,
                NumChildren = 1,
                ConvertedType = ConvertedType.List,
                LogicalType = new LogicalType.ListType(),
            },
            new() { Name = "list", RepetitionType = FieldRepetitionType.Repeated, NumChildren = 1 },
            new()
            {
                Name = "element",
                Type = PhysicalType.Boolean,
                RepetitionType = FieldRepetitionType.Optional,
            },
        ];

        return BuildFile(schema, rowCount,
        [
            new ColumnSpec(["v", "list", "element"], PhysicalType.Boolean,
                repLevels, 1, defLevels, 3, BoolValues(values)),
        ]);
    }

    /// <summary>
    /// <c>optional group v (LIST) { repeated group list { optional group element (LIST) {
    /// repeated group list { optional boolean element; } } } }</c>: maximum repetition level 2,
    /// maximum definition level 5.
    /// </summary>
    private static byte[] NestedListFile(
        int[] repLevels, int[] defLevels, bool[] values, int rowCount)
    {
        List<SchemaElement> schema =
        [
            new() { Name = "root", NumChildren = 1 },
            new()
            {
                Name = "v",
                RepetitionType = FieldRepetitionType.Optional,
                NumChildren = 1,
                ConvertedType = ConvertedType.List,
                LogicalType = new LogicalType.ListType(),
            },
            new() { Name = "list", RepetitionType = FieldRepetitionType.Repeated, NumChildren = 1 },
            new()
            {
                Name = "element",
                RepetitionType = FieldRepetitionType.Optional,
                NumChildren = 1,
                ConvertedType = ConvertedType.List,
                LogicalType = new LogicalType.ListType(),
            },
            new() { Name = "list", RepetitionType = FieldRepetitionType.Repeated, NumChildren = 1 },
            new()
            {
                Name = "element",
                Type = PhysicalType.Boolean,
                RepetitionType = FieldRepetitionType.Optional,
            },
        ];

        return BuildFile(schema, rowCount,
        [
            new ColumnSpec(["v", "list", "element", "list", "element"], PhysicalType.Boolean,
                repLevels, 2, defLevels, 5, BoolValues(values)),
        ]);
    }

    /// <summary>A bare <c>repeated boolean v;</c> at the top level — the 1-level list encoding.</summary>
    private static byte[] BareRepeatedLeafFile(
        int[] repLevels, int[] defLevels, bool[] values, int rowCount)
    {
        List<SchemaElement> schema =
        [
            new() { Name = "root", NumChildren = 1 },
            new()
            {
                Name = "v",
                Type = PhysicalType.Boolean,
                RepetitionType = FieldRepetitionType.Repeated,
            },
        ];

        return BuildFile(schema, rowCount,
        [
            new ColumnSpec(["v"], PhysicalType.Boolean, repLevels, 1, defLevels, 1, BoolValues(values)),
        ]);
    }

    /// <summary>
    /// <c>optional group s { optional boolean a; optional boolean b; }</c>, with each leaf's level
    /// count supplied independently so the two can be made to disagree.
    /// </summary>
    private static byte[] StructFile(
        int[] aDefLevels, bool[] aValues, int[] bDefLevels, bool[] bValues, int rowCount)
    {
        List<SchemaElement> schema =
        [
            new() { Name = "root", NumChildren = 1 },
            new() { Name = "s", RepetitionType = FieldRepetitionType.Optional, NumChildren = 2 },
            new() { Name = "a", Type = PhysicalType.Boolean, RepetitionType = FieldRepetitionType.Optional },
            new() { Name = "b", Type = PhysicalType.Boolean, RepetitionType = FieldRepetitionType.Optional },
        ];

        return BuildFile(schema, rowCount,
        [
            new ColumnSpec(["s", "a"], PhysicalType.Boolean, null, 0, aDefLevels, 2, BoolValues(aValues)),
            new ColumnSpec(["s", "b"], PhysicalType.Boolean, null, 0, bDefLevels, 2, BoolValues(bValues)),
        ]);
    }

    /// <summary>
    /// <c>optional group m (MAP) { repeated group key_value { required int32 key; optional boolean value; } }</c>.
    /// </summary>
    private static byte[] MapFile(
        int[] keyRepLevels, int[] keyDefLevels, int[] keys,
        int[] valueRepLevels, int[] valueDefLevels, bool[] values,
        int rowCount)
    {
        List<SchemaElement> schema =
        [
            new() { Name = "root", NumChildren = 1 },
            new()
            {
                Name = "m",
                RepetitionType = FieldRepetitionType.Optional,
                NumChildren = 1,
                ConvertedType = ConvertedType.Map,
                LogicalType = new LogicalType.MapType(),
            },
            new() { Name = "key_value", RepetitionType = FieldRepetitionType.Repeated, NumChildren = 2 },
            new() { Name = "key", Type = PhysicalType.Int32, RepetitionType = FieldRepetitionType.Required },
            new() { Name = "value", Type = PhysicalType.Boolean, RepetitionType = FieldRepetitionType.Optional },
        ];

        return BuildFile(schema, rowCount,
        [
            new ColumnSpec(["m", "key_value", "key"], PhysicalType.Int32,
                keyRepLevels, 1, keyDefLevels, 2, Int32Values(keys)),
            new ColumnSpec(["m", "key_value", "value"], PhysicalType.Boolean,
                valueRepLevels, 1, valueDefLevels, 3, BoolValues(values)),
        ]);
    }

    // ───── Raw file construction ─────

    /// <summary>
    /// One leaf column's page contents, written verbatim — including levels no correct writer emits.
    /// </summary>
    private sealed record ColumnSpec(
        string[] Path, PhysicalType Type,
        int[]? RepLevels, int MaxRepLevel, int[]? DefLevels, int MaxDefLevel, byte[] ValueBytes)
    {
        public int NumValues => RepLevels?.Length ?? DefLevels?.Length ?? 0;
    }

    /// <summary>PLAIN booleans: one bit each, LSB first.</summary>
    private static byte[] BoolValues(bool[] values)
    {
        var bytes = new byte[(values.Length + 7) / 8];
        for (int i = 0; i < values.Length; i++)
        {
            if (values[i])
                bytes[i >> 3] |= (byte)(1 << (i & 7));
        }
        return bytes;
    }

    /// <summary>PLAIN INT32: four little-endian bytes each.</summary>
    private static byte[] Int32Values(int[] values)
    {
        var bytes = new byte[values.Length * 4];
        for (int i = 0; i < values.Length; i++)
            BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(i * 4), values[i]);
        return bytes;
    }

    /// <summary>
    /// Emits a one-row-group Parquet file holding one uncompressed V1 data page per supplied column.
    /// </summary>
    private static byte[] BuildFile(List<SchemaElement> schema, int rowCount, ColumnSpec[] columns)
    {
        using var output = new MemoryStream();
        Append(output, Magic);

        var chunks = new List<ColumnChunk>(columns.Length);
        long totalByteSize = 0;

        foreach (var column in columns)
        {
            byte[] body = BuildPageBody(column);
            byte[] header = MetadataEncoder.EncodePageHeader(new PageHeader
            {
                Type = PageType.DataPage,
                UncompressedPageSize = body.Length,
                CompressedPageSize = body.Length,
                DataPageHeader = new DataPageHeader
                {
                    NumValues = column.NumValues,
                    Encoding = Encoding.Plain,
                    DefinitionLevelEncoding = Encoding.Rle,
                    RepetitionLevelEncoding = Encoding.Rle,
                },
            });

            long dataPageOffset = output.Position;
            Append(output, header);
            Append(output, body);
            long chunkLength = output.Position - dataPageOffset;
            totalByteSize += chunkLength;

            chunks.Add(new ColumnChunk
            {
                FileOffset = dataPageOffset,
                MetaData = new ColumnMetaData
                {
                    Type = column.Type,
                    Encodings = [Encoding.Plain, Encoding.Rle],
                    PathInSchema = column.Path,
                    Codec = CompressionCodec.Uncompressed,
                    NumValues = column.NumValues,
                    TotalUncompressedSize = chunkLength,
                    TotalCompressedSize = chunkLength,
                    DataPageOffset = dataPageOffset,
                },
            });
        }

        var metadata = new FileMetaData
        {
            Version = 1,
            Schema = schema,
            NumRows = rowCount,
            CreatedBy = "engineered-wood test (deliberately malformed)",
            RowGroups = [new RowGroup { NumRows = rowCount, TotalByteSize = totalByteSize, Columns = chunks }],
        };

        byte[] footer = MetadataEncoder.EncodeFileMetaData(metadata);
        Append(output, footer);
        var footerLength = new byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(footerLength, footer.Length);
        Append(output, footerLength);
        Append(output, Magic);
        return output.ToArray();
    }

    /// <summary>
    /// Builds an uncompressed V1 data page body: length-prefixed RLE repetition levels, the same for
    /// definition levels, then PLAIN (bit-packed) boolean values.
    /// </summary>
    private static byte[] BuildPageBody(ColumnSpec column)
    {
        using var body = new MemoryStream();

        if (column.MaxRepLevel > 0)
            WriteLengthPrefixed(body, EncodeLevels(column.RepLevels!, BitWidth(column.MaxRepLevel)));
        if (column.MaxDefLevel > 0)
            WriteLengthPrefixed(body, EncodeLevels(column.DefLevels!, BitWidth(column.MaxDefLevel)));

        Append(body, column.ValueBytes);

        return body.ToArray();

        static void WriteLengthPrefixed(MemoryStream target, byte[] payload)
        {
            var length = new byte[4];
            BinaryPrimitives.WriteInt32LittleEndian(length, payload.Length);
            Append(target, length);
            Append(target, payload);
        }
    }

    private static byte[] EncodeLevels(int[] levels, int bitWidth)
    {
        var encoder = new RleBitPackedEncoder(bitWidth);
        encoder.Encode(levels);
        return encoder.ToArray();
    }

    private static int BitWidth(int maxLevel)
    {
        int width = 0;
        while (maxLevel > 0)
        {
            width++;
            maxLevel >>= 1;
        }
        return width;
    }

    private static readonly byte[] Magic = [(byte)'P', (byte)'A', (byte)'R', (byte)'1'];

    /// <summary>net472 has no <c>Stream.Write(ReadOnlySpan&lt;byte&gt;)</c>, and this file builds for it too.</summary>
    private static void Append(MemoryStream target, byte[] payload)
        => target.Write(payload, 0, payload.Length);
}
