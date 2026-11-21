// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Data;

namespace EngineeredWood.Tests.Parquet.Data;

/// <summary>
/// The open page of a flat column that is not dictionary-encoded is held in buffers the chunk reuses from page to
/// page (#940): Detach appends the borrowed pieces to them, and the join appends the entries that complete the page.
/// The pages are the ones the chunk writes holding and joining the pieces as arrays of their own.
/// </summary>
public class OpenPageBufferTests
{
    private static readonly string[] Path = ["c"];

    public static TheoryData<string, string, bool> Cases()
    {
        var data = new TheoryData<string, string, bool>();
        foreach (var kind in Kinds)
        {
            foreach (var options in new[] { "v2", "v1", "rows", "bytes" })
            {
                data.Add(kind, options, true);
                if (options == "v2")
                    data.Add(kind, options, false);
            }
        }
        data.Add("string", "fsst", true); // FSST-compressed pieces are held and joined as before
        return data;
    }

    private static readonly string[] Kinds =
        ["int32", "int32-nullable", "decimal", "double", "boolean", "string", "fixed"];

    /// <summary>
    /// Appends of every size write what the same appends write with the open page held as arrays: data pages, page
    /// index and statistics. Pieces start at every bit offset of the appends they come from.
    /// </summary>
    [Theory]
    [MemberData(nameof(Cases))]
    public void Appends_WriteWhatTheyWriteWithoutTheBuffers(string kind, string optionSet, bool detach)
    {
        var (values, physicalType, typeLength, nullable) = Column(kind);
        var options = Options(optionSet);
        ColumnChunkWriter NewWriter(bool accumulates) =>
            new(Path, physicalType, typeLength, maxDefLevel: nullable ? 1 : 0, maxRepLevel: 0, options)
            {
                AccumulatesOpenPage = accumulates,
            };

        var (held, accumulated) = InAppends(NewWriter(accumulates: true), values, detach);
        var (unheld, _) = InAppends(NewWriter(accumulates: false), values, detach);

        if (optionSet != "fsst")
            Assert.Equal(detach, accumulated);
        Assert.True(unheld.PageIndex!.PageLocations.Count > 5);
        Assert.Equal(unheld.Data.ToArray(), held.Data.ToArray());
        Assert.Equal(
            unheld.PageIndex.PageLocations.Select(p => p.FirstRowIndex),
            held.PageIndex!.PageLocations.Select(p => p.FirstRowIndex));
        Assert.Equal(unheld.PageIndex.ColumnIndex?.MinValues, held.PageIndex.ColumnIndex?.MinValues);
        Assert.Equal(unheld.PageIndex.ColumnIndex?.MaxValues, held.PageIndex.ColumnIndex?.MaxValues);
        Assert.Equal(unheld.MetaData.Statistics?.MinValue, held.MetaData.Statistics?.MinValue);
        Assert.Equal(unheld.MetaData.Statistics?.MaxValue, held.MetaData.Statistics?.MaxValue);
        Assert.Equal(unheld.MetaData.Statistics?.NullCount, held.MetaData.Statistics?.NullCount);
        GC.KeepAlive(values);
    }

    /// <summary>
    /// Appends of a fixed-width column write what one append of all of it writes: its pages are cut at the same
    /// level entries however its appends fall.
    /// </summary>
    [Fact]
    public void FixedWidthAppends_WriteWhatOneAppendWrites()
    {
        var (values, physicalType, typeLength, nullable) = Column("int32-nullable");
        var options = Options("rows");
        ColumnChunkWriter NewWriter() =>
            new(Path, physicalType, typeLength, maxDefLevel: nullable ? 1 : 0, maxRepLevel: 0, options);

        var whole = NewWriter();
        whole.AppendFlat(values);
        var oneAppend = whole.Finish();
        var (appended, accumulated) = InAppends(NewWriter(), values, detach: true);

        Assert.True(accumulated);
        Assert.Equal(oneAppend.Data.ToArray(), appended.Data.ToArray());
    }

#if NET6_0_OR_GREATER
    /// <summary>
    /// Appends of 19,999 strings against 20,000-row pages leave nearly a page open each time. Copying the open piece
    /// at Detach and again into the joined page allocated two arrays of its values a page; the buffers are reused, so
    /// the appends allocate what their encoding does.
    /// </summary>
    /// <remarks>.NET Framework has no per-thread allocation counter, so the pin runs on the modern targets only.</remarks>
    [Fact]
    public void OpenPages_AreHeldInReusedBuffers_NotCopiedPerPage()
    {
        const int appendRows = 19_999; // nearly a page left open by each append
        const int appends = 20;
        var options = ParquetWriteOptions.Default with { DictionaryEnabled = false };
        var builder = new StringArray.Builder();
        for (int i = 0; i < appendRows; i++)
            builder.Append("value-" + i);
        var values = builder.Build();

        long Allocated(bool accumulates)
        {
            var writer = new ColumnChunkWriter(Path, PhysicalType.ByteArray, 0, maxDefLevel: 0, maxRepLevel: 0, options)
            {
                AccumulatesOpenPage = accumulates,
            };
            writer.AppendFlat(values);
            writer.Detach();
            long before = GC.GetAllocatedBytesForCurrentThread();
            for (int i = 0; i < appends; i++)
            {
                writer.AppendFlat(values);
                writer.Detach();
            }
            long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
            writer.Finish();
            return allocated;
        }

        Allocated(true);
        Allocated(false);
        long held = Allocated(true);
        long unheld = Allocated(false);

        // Measured: 4.3 MB, against 18.5 MB copying the open piece at Detach and again into each joined page.
        Assert.True(held < unheld / 2,
            $"{appends} appends of {appendRows:N0} strings allocated {held:N0} bytes, against {unheld:N0} without the buffers.");
        GC.KeepAlive(values);
    }
#endif

    /// <summary>
    /// Appends of every size from 1 row to most of the column, Detached after each when <paramref name="detach"/>.
    /// Each is a copy at offset 0, as <see cref="ParquetFileWriter"/> hands the chunk its arrays; the pieces they leave
    /// open start anywhere in them. Also whether the open page was ever held in the buffers.
    /// </summary>
    private static (ColumnChunkWriter.ColumnChunkResult Result, bool Accumulated) InAppends(
        ColumnChunkWriter writer, IArrowArray values, bool detach)
    {
        var random = new Random(940);
        bool accumulated = false;
        for (int offset = 0; offset < values.Length;)
        {
            int left = values.Length - offset;
            int length = random.Next(4) switch
            {
                0 => Math.Min(left, 1 + random.Next(3)),
                1 => Math.Min(left, 1 + random.Next(100)),
                2 => Math.Min(left, 1 + random.Next(5000)),
                _ => Math.Min(left, 1 + random.Next(Math.Max(1, left / 3))),
            };
            writer.AppendFlat(EngineeredWood.Arrow.ArrowCompute.Take(values, Enumerable.Range(offset, length).ToArray()));
            if (detach)
                writer.Detach();
            accumulated |= writer.OpenPageAccumulated;
            offset += length;
        }
        return (writer.Finish(), accumulated);
    }

    private static ParquetWriteOptions Options(string name)
    {
        // Pages of 5,000 rows unless cut sooner, so that the 60,001 rows make several.
        var plain = ParquetWriteOptions.Default with { DictionaryEnabled = false, DataPageRowCountLimit = 5_000 };
        return name switch
        {
            "v1" => plain with { DataPageVersion = DataPageVersion.V1 },
            "rows" => plain with { DataPageRowCountLimit = 1_000 },
            "bytes" => plain with { DataPageRowCountLimit = null, DataPageSize = 4 << 10 },
#pragma warning disable EWPARQUET0003 // FSST is experimental; tested here as a path the buffers leave alone.
            "fsst" => plain with { ByteArrayEncoding = ByteArrayEncoding.Fsst },
#pragma warning restore EWPARQUET0003
            _ => plain,
        };
    }

    private static (IArrowArray Values, PhysicalType PhysicalType, int TypeLength, bool Nullable) Column(string kind)
    {
        const int rows = 60_001;
        var random = new Random(kind.Length);
        bool Null() => random.Next(6) == 0;

        switch (kind)
        {
            case "int32":
            case "int32-nullable":
            {
                bool nullable = kind == "int32-nullable";
                var builder = new Int32Array.Builder();
                for (int i = 0; i < rows; i++)
                {
                    if (nullable && Null()) builder.AppendNull();
                    else builder.Append(random.Next());
                }
                return (builder.Build(), PhysicalType.Int32, 0, nullable);
            }

            case "decimal":
            {
                // Decimal128 of precision 18 is narrowed to INT64 per append, into a new Decimal64Type each time.
                var builder = new Decimal128Array.Builder(new Decimal128Type(18, 2));
                for (int i = 0; i < rows; i++)
                {
                    if (Null()) builder.AppendNull();
                    else builder.Append((decimal)random.Next() / 100m);
                }
                return (builder.Build(), PhysicalType.Int64, 0, true);
            }

            case "double":
            {
                var builder = new DoubleArray.Builder();
                for (int i = 0; i < rows; i++)
                {
                    switch (random.Next(8))
                    {
                        case 0: builder.AppendNull(); break;
                        case 1: builder.Append(double.NaN); break;
                        case 2: builder.Append(-0.0); break;
                        default: builder.Append(random.NextDouble()); break;
                    }
                }
                return (builder.Build(), PhysicalType.Double, 0, true);
            }

            case "boolean":
            {
                var builder = new BooleanArray.Builder();
                for (int i = 0; i < rows; i++)
                {
                    if (Null()) builder.AppendNull();
                    else builder.Append(random.Next(3) == 0);
                }
                return (builder.Build(), PhysicalType.Boolean, 0, true);
            }

            case "string":
            {
                var builder = new StringArray.Builder();
                for (int i = 0; i < rows; i++)
                {
                    if (Null()) builder.AppendNull();
                    else builder.Append(new string((char)('a' + random.Next(26)), random.Next(40)) + i);
                }
                return (builder.Build(), PhysicalType.ByteArray, 0, true);
            }

            case "fixed":
            {
                var bytes = new byte[rows * 5];
                random.NextBytes(bytes);
                var valid = new byte[(rows + 7) / 8];
                int nulls = 0;
                for (int i = 0; i < rows; i++)
                {
                    if (Null()) nulls++;
                    else valid[i >> 3] |= (byte)(1 << (i & 7));
                }
                var array = Apache.Arrow.ArrowArrayFactory.BuildArray(new ArrayData(
                    new FixedSizeBinaryType(5), rows, nulls, 0, [new ArrowBuffer(valid), new ArrowBuffer(bytes)]));
                return (array, PhysicalType.FixedLenByteArray, 5, true);
            }

            default:
                throw new ArgumentOutOfRangeException(nameof(kind), kind, null);
        }
    }
}
