// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Orc.Proto;
using Google.Protobuf;
using StreamKind = EngineeredWood.Orc.Proto.Stream.Types.Kind;

namespace EngineeredWood.Orc.Tests;

/// <summary>
/// A stripe without a stream its column needs to read its values (#636). A conforming writer never leaves one out,
/// so the file is corrupt, and reading it must throw rather than return zeros or, from a pooled buffer, stale
/// memory. Each case writes a small file, then hides one stream by renaming it in the stripe footer to a row
/// index, which the reader skips: the stripe's layout is unchanged, so only that stream goes missing.
/// </summary>
public class MissingStreamTests
{
    private const int Rows = 5;

    private static ArrowBuffer Offsets(params int[] offsets) =>
        new ArrowBuffer.Builder<int>().AppendRange(offsets).Build();

    private static Int32Array Ints(int count) =>
        new Int32Array.Builder().AppendRange(Enumerable.Range(1, count)).Build();

    private static StringArray Strings(params string[] values) =>
        new StringArray.Builder().AppendRange(values).Build();

    private static readonly MapType IntToString = new(
        new Field("key", Int32Type.Default, false), new Field("value", StringType.Default, true));

    private static readonly UnionType IntOrString = new(
        [new Field("i", Int32Type.Default, true), new Field("s", StringType.Default, true)], [0, 1], UnionMode.Dense);

    private static OrcWriterOptions Options(EncodingFamily strings) =>
        new() { Compression = CompressionKind.None, DefaultStringEncoding = strings };

    /// <summary>A non-null column of <see cref="Rows"/> values, and how to write it.</summary>
    private static (IArrowArray Array, OrcWriterOptions Options) Column(string type) => type switch
    {
        "boolean" => (new BooleanArray.Builder().AppendRange([true, false, true, true, false]).Build(), Options(EncodingFamily.V2)),
        "byte" => (new Int8Array.Builder().AppendRange([1, 2, 3, 4, 5]).Build(), Options(EncodingFamily.V2)),
        "short" => (new Int16Array.Builder().AppendRange([1, 2, 3, 4, 5]).Build(), Options(EncodingFamily.V2)),
        "int" => (Ints(Rows), Options(EncodingFamily.V2)),
        "long" => (new Int64Array.Builder().AppendRange([1, 2, 3, 4, 5]).Build(), Options(EncodingFamily.V2)),
        "float" => (new FloatArray.Builder().AppendRange([1, 2, 3, 4, 5]).Build(), Options(EncodingFamily.V2)),
        "double" => (new DoubleArray.Builder().AppendRange([1, 2, 3, 4, 5]).Build(), Options(EncodingFamily.V2)),
        "date" => (new Date32Array.Builder().AppendRange(
            Enumerable.Range(0, Rows).Select(i => new DateTime(2024, 1, 1 + i))).Build(), Options(EncodingFamily.V2)),
        "timestamp" => (new TimestampArray.Builder(new TimestampType(TimeUnit.Nanosecond, (string?)null)).AppendRange(
            Enumerable.Range(0, Rows).Select(i => new DateTimeOffset(2024, 1, 1, 0, 0, i, 500, TimeSpan.Zero))).Build(),
            Options(EncodingFamily.V2)),
        "decimal" => (new Decimal128Array.Builder(new Decimal128Type(10, 2)).AppendRange([1.25m, 2.5m, 3m, 4.75m, 5m]).Build(),
            Options(EncodingFamily.V2)),
        "string-direct" => (Strings("a", "bb", "ccc", "dd", "e"), Options(EncodingFamily.V2)),
        "string-dictionary" => (Strings("a", "bb", "a", "bb", "a"), Options(EncodingFamily.DictionaryV2)),
        "empty-string-direct" => (Strings("", "", "", "", ""), Options(EncodingFamily.V2)),
        "empty-string-dictionary" => (Strings("", "", "", "", ""), Options(EncodingFamily.DictionaryV2)),
        "binary" => (new BinaryArray.Builder().AppendRange(
            Enumerable.Range(1, Rows).Select(i => new byte[i])).Build(), Options(EncodingFamily.V2)),
        "list" => (new ListArray(new ListType(new Field("item", Int32Type.Default, true)), Rows,
            Offsets(0, 1, 3, 3, 4, 6), Ints(6), ArrowBuffer.Empty, 0), Options(EncodingFamily.V2)),
        "map" => (new MapArray(IntToString, Rows, Offsets(0, 1, 3, 3, 4, 6),
            new StructArray(new StructType([IntToString.KeyField, IntToString.ValueField]), 6,
                [Ints(6), Strings("a", "b", "c", "d", "e", "f")], ArrowBuffer.Empty, 0),
            ArrowBuffer.Empty, 0), Options(EncodingFamily.V2)),
        "union" => (new DenseUnionArray(IntOrString, Rows,
            [new Int32Array.Builder().AppendRange([7, 8, 9]).Build(), Strings("x", "y")],
            new ArrowBuffer.Builder<byte>().AppendRange([0, 1, 0, 1, 0]).Build(), Offsets(0, 0, 1, 1, 2)),
            Options(EncodingFamily.V2)),
        _ => throw new ArgumentOutOfRangeException(nameof(type), type, null),
    };

    private static string TempPath() => Path.Combine(Path.GetTempPath(), $"ew_orc_636_{Guid.NewGuid():N}.orc");

    /// <summary>Writes column <paramref name="type"/> as the file's only field (column 1) and returns its bytes.</summary>
    private static async Task<byte[]> WriteAsync(string type)
    {
        var (array, options) = Column(type);
        var schema = new Schema([new Field("c", array.Data.DataType, nullable: false)], null);
        var path = TempPath();
        try
        {
            await using (var writer = OrcWriter.Create(path, schema, options))
            {
                await writer.WriteBatchAsync(new RecordBatch(schema, [array], Rows));
                await writer.CloseAsync();
            }
            return File.ReadAllBytes(path);
        }
        finally
        {
            File.Delete(path);
        }
    }

    /// <summary>Opens <paramref name="file"/> and passes the reader to <paramref name="read"/>.</summary>
    private static async Task<T> OpenAsync<T>(byte[] file, Func<OrcReader, Task<T>> read)
    {
        var path = TempPath();
        File.WriteAllBytes(path, file);
        try
        {
            await using var reader = await OrcReader.OpenAsync(path);
            return await read(reader);
        }
        finally
        {
            File.Delete(path);
        }
    }

    private static Task<List<RecordBatch>> ReadAllAsync(byte[] file) => OpenAsync(file, async reader =>
    {
        var batches = new List<RecordBatch>();
        await foreach (var batch in reader.CreateRowReader())
            batches.Add(batch);
        return batches;
    });

    /// <summary>
    /// Hides column 1's <paramref name="kind"/> stream from the reader by renaming it to a row index. Fails the
    /// test if the column has no such stream, so a case cannot pass by testing nothing.
    /// </summary>
    private static async Task<byte[]> HideStreamAsync(byte[] file, StreamKind kind)
    {
        var stripe = await OpenAsync(file, reader =>
        {
            Assert.Equal(CompressionKind.None, reader.Compression);
            Assert.Equal(1, reader.NumberOfStripes);
            return Task.FromResult(reader.GetStripe(0));
        });

        int footerStart = checked((int)(stripe.Offset + stripe.IndexLength + stripe.DataLength));
        int footerLength = checked((int)stripe.FooterLength);
        var footer = StripeFooter.Parser.ParseFrom(file, footerStart, footerLength);

        var target = footer.Streams.SingleOrDefault(s => s.Column == 1 && s.Kind == kind);
        Assert.True(target != null, $"column 1 has no {kind} stream to hide");
        Assert.True(target!.Length > 0, $"column 1's {kind} stream is already empty");
        target.Kind = StreamKind.RowIndex;

        // Every stream kind here is a one-byte enum, so the footer keeps its length and nothing else moves.
        var rewritten = footer.ToByteArray();
        Assert.Equal(footerLength, rewritten.Length);
        var result = (byte[])file.Clone();
        rewritten.CopyTo(result, footerStart);
        return result;
    }

    [Theory]
    [InlineData("boolean", StreamKind.Data)]
    [InlineData("byte", StreamKind.Data)]
    [InlineData("short", StreamKind.Data)]
    [InlineData("int", StreamKind.Data)]
    [InlineData("long", StreamKind.Data)]
    [InlineData("float", StreamKind.Data)]
    [InlineData("double", StreamKind.Data)]
    [InlineData("date", StreamKind.Data)]
    [InlineData("timestamp", StreamKind.Data)]
    [InlineData("timestamp", StreamKind.Secondary)]
    [InlineData("decimal", StreamKind.Data)]
    [InlineData("string-direct", StreamKind.Length)]
    [InlineData("string-direct", StreamKind.Data)]
    [InlineData("string-dictionary", StreamKind.Data)]
    [InlineData("string-dictionary", StreamKind.Length)]
    [InlineData("string-dictionary", StreamKind.DictionaryData)]
    [InlineData("binary", StreamKind.Length)]
    [InlineData("binary", StreamKind.Data)]
    [InlineData("list", StreamKind.Length)]
    [InlineData("map", StreamKind.Length)]
    [InlineData("union", StreamKind.Data)]
    public async Task MissingRequiredStream_Throws(string type, StreamKind kind)
    {
        var file = await WriteAsync(type);
        // The intact file reads: what throws below is the missing stream, not the file.
        Assert.Equal(Rows, (await ReadAllAsync(file)).Sum(b => b.Length));

        var corrupt = await HideStreamAsync(file, kind);
        var ex = await Assert.ThrowsAsync<InvalidDataException>(() => ReadAllAsync(corrupt));
        Assert.Contains("column 1", ex.Message);
    }

    /// <summary>Streams a column can rightly lack: its values still read, and as written.</summary>
    [Theory]
    [InlineData("decimal", StreamKind.Secondary)] // scales fall back to the column's
    public async Task MissingOptionalStream_Reads(string type, StreamKind kind)
    {
        var file = await WriteAsync(type);
        var expected = (await ReadAllAsync(file)).Single().Column(0);

        var actual = (await ReadAllAsync(await HideStreamAsync(file, kind))).Single().Column(0);
        Assert.Equal(Rows, actual.Length);
        var want = (Decimal128Array)expected;
        var got = (Decimal128Array)actual;
        for (int i = 0; i < Rows; i++)
            Assert.Equal(want.GetValue(i), got.GetValue(i));
    }

    /// <summary>Empty strings take no bytes, so a column of them reads without DATA or DICTIONARY_DATA.</summary>
    [Theory]
    [InlineData("empty-string-direct")]
    [InlineData("empty-string-dictionary")]
    public async Task EmptyStrings_ReadWithoutByteStreams(string type)
    {
        var file = await WriteAsync(type);
        var column = (StringArray)(await ReadAllAsync(file)).Single().Column(0);
        Assert.Equal(Rows, column.Length);
        for (int i = 0; i < Rows; i++)
            Assert.Equal("", column.GetString(i));
    }
}
