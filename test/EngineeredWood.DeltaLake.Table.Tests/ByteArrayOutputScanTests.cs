// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.CompilerServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// #793: under a non-default <see cref="ParquetReadOptions.ByteArrayOutput"/>, every string and binary column of a
/// scan comes back in the representation it asks for, and matches <see cref="DeltaTable.GetReadSchema"/>: the data
/// columns the Parquet reader decodes, and also a partition column, a column backfilled for a file written before it
/// was added, a nested field, and a codec-seam host's columns. The metadata columns keep their documented types.
/// </summary>
public class ByteArrayOutputScanTests : IDisposable
{
    private readonly string _tempDir;

    public ByteArrayOutputScanTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_bao_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    /// <summary>A host that returns its strings and binaries in one fixed representation, whatever the table's
    /// options ask for.</summary>
    private sealed class FixedReader(ITableFileSystem fs, ByteArrayOutputKind hostKind) : IDataFileReader
    {
        public async IAsyncEnumerable<RecordBatch> ReadAsync(
            string relativePath, IReadOnlyList<string>? physicalColumns,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await using var file = await fs.OpenReadAsync(relativePath, cancellationToken);
            using var reader = new ParquetFileReader(file, ownsFile: false,
                new ParquetReadOptions { ByteArrayOutput = hostKind });
            await foreach (var b in reader.ReadAllAsync(columnNames: physicalColumns, cancellationToken: cancellationToken))
                yield return b;
        }
    }

    private static readonly StructType InfoType = new([new Field("tag", StringType.Default, true), new Field("n", Int32Type.Default, true)]);

    private async Task<string> WriteTableAsync()
    {
        string path = Path.Combine(_tempDir, "t");
        var fs = new LocalTableFileSystem(path);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("region", StringType.Default, true))
            .Field(new Field("note", StringType.Default, true))
            .Field(new Field("blob", BinaryType.Default, true))
            .Field(new Field("info", InfoType, true))
            .Build();
        await using var table = await DeltaTable.CreateAsync(fs, schema, partitionColumns: ["region"]);

        var ids = new Int64Array.Builder();
        var regions = new StringArray.Builder();
        var notes = new StringArray.Builder();
        var blobs = new BinaryArray.Builder();
        var tags = new StringArray.Builder();
        var ns = new Int32Array.Builder();
        for (int i = 0; i < 6; i++)
        {
            ids.Append(i);
            regions.Append(i % 2 == 0 ? "east" : "west");
            if (i == 3) notes.AppendNull(); else notes.Append("note-" + i);
            blobs.Append(new byte[] { (byte)i, 0xFF });
            tags.Append("tag-" + i);
            ns.Append(i);
        }

        await table.WriteAsync([new RecordBatch(schema,
            [ids.Build(), regions.Build(), notes.Build(), blobs.Build(),
             new StructArray(InfoType, 6, [tags.Build(), ns.Build()], ArrowBuffer.Empty, 0)], 6)]);

        // Added after the file was written: every row of it is backfilled.
        await table.AddColumnAsync(new Field("extra", StringType.Default, true));
        return path;
    }

    public static IEnumerable<object[]> Cases()
    {
        foreach (var kind in new[] { ByteArrayOutputKind.Default, ByteArrayOutputKind.ViewType, ByteArrayOutputKind.LargeOffsets })
        {
            yield return [kind, "none"];
            yield return [kind, "utf8 host"];
            yield return [kind, "view host"];
        }
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task EveryBatchMatchesGetReadSchema(ByteArrayOutputKind kind, string seam)
    {
        string path = await WriteTableAsync();
        var fs = new LocalTableFileSystem(path);
        var options = new DeltaTableOptions
        {
            ParquetReadOptions = new ParquetReadOptions { ByteArrayOutput = kind },
            DataFileReader = seam switch
            {
                "utf8 host" => new FixedReader(fs, ByteArrayOutputKind.Default),
                "view host" => new FixedReader(fs, ByteArrayOutputKind.ViewType),
                _ => null,
            },
        };
        await using var table = await DeltaTable.OpenAsync(fs, options);
        var read = new DeltaReadOptions { Metadata = DeltaRowMetadata.Locator };

        var expected = table.GetReadSchema(read);
        IArrowType stringType = kind switch
        {
            ByteArrayOutputKind.ViewType => StringViewType.Default,
            ByteArrayOutputKind.LargeOffsets => LargeStringType.Default,
            _ => StringType.Default,
        };
        IArrowType binaryType = kind switch
        {
            ByteArrayOutputKind.ViewType => BinaryViewType.Default,
            ByteArrayOutputKind.LargeOffsets => LargeBinaryType.Default,
            _ => BinaryType.Default,
        };
        foreach (string name in new[] { "region", "note", "extra" })
            Assert.Equal(stringType.TypeId, expected.GetFieldByName(name).DataType.TypeId);
        Assert.Equal(binaryType.TypeId, expected.GetFieldByName("blob").DataType.TypeId);
        Assert.Equal(stringType.TypeId, ((StructType)expected.GetFieldByName("info").DataType).Fields[0].DataType.TypeId);

        var rows = new List<string>();
        int batches = 0;
        await foreach (var batch in table.ReadAsync(read))
        {
            using (batch)
            {
                batches++;
                Assert.Equal(Render(expected), Render(batch.Schema));
                for (int c = 0; c < batch.ColumnCount; c++)
                    Assert.Equal(Render(batch.Schema.FieldsList[c].DataType), Render(batch.Column(c).Data.DataType));

                // The locator's path column keeps its documented type: an update by locator requires utf8.
                Assert.IsType<StringArray>(batch.Column(batch.Schema.GetFieldIndex(DeltaMetadataColumns.DefaultPrefix + DeltaMetadataColumns.FilePathSuffix)));

                for (int r = 0; r < batch.Length; r++)
                    rows.Add(Row(batch, r));
            }
        }

        Assert.True(batches > 0);
        rows.Sort(StringComparer.Ordinal);
        Assert.Equal(
            Enumerable.Range(0, 6).Select(i =>
                $"{i}|{(i % 2 == 0 ? "east" : "west")}|{(i == 3 ? "null" : "note-" + i)}|{i:x2}ff|tag-{i}|null").OrderBy(s => s, StringComparer.Ordinal),
            rows);
    }

    /// <summary>
    /// DML on a partitioned table under each representation (#822 review round 2): an UPDATE reads the files it rewrites
    /// through the same pipeline as a scan, and routes each row by its string partition value, whether or not that
    /// changes. It must not fail on the representation, and a scan afterwards sees the new values.
    /// </summary>
    [Theory]
    [InlineData(ByteArrayOutputKind.Default)]
    [InlineData(ByteArrayOutputKind.ViewType)]
    [InlineData(ByteArrayOutputKind.LargeOffsets)]
    public async Task APartitionedUpdateWorksUnderEachRepresentation(ByteArrayOutputKind kind)
    {
        string path = await WriteTableAsync();
        var fs = new LocalTableFileSystem(path);
        await using var table = await DeltaTable.OpenAsync(fs, new DeltaTableOptions
        {
            ParquetReadOptions = new ParquetReadOptions { ByteArrayOutput = kind },
        });

        static BooleanArray Where(RecordBatch batch, Func<long, bool> predicate)
        {
            var ids = (Int64Array)batch.Column(batch.Schema.GetFieldIndex("id"));
            var mask = new BooleanArray.Builder();
            for (int i = 0; i < batch.Length; i++)
                mask.Append(predicate(ids.GetValue(i)!.Value));
            return mask.Build();
        }

        // An updater may hand back a column as a view: the second UPDATE sets the partition column that way.
        static RecordBatch Set(RecordBatch batch, string column, string value, bool asView = false)
        {
            int index = batch.Schema.GetFieldIndex(column);
            IArrowArray replacement;
            if (asView)
            {
                var views = new StringViewArray.Builder();
                for (int i = 0; i < batch.Length; i++)
                    views.Append(value);
                replacement = views.Build();
            }
            else
            {
                var builder = new StringArray.Builder();
                for (int i = 0; i < batch.Length; i++)
                    builder.Append(value);
                replacement = builder.Build();
            }

            var columns = Enumerable.Range(0, batch.ColumnCount).Select(c => c == index ? replacement : batch.Column(c)).ToArray();
            var fields = batch.Schema.FieldsList.Select((f, c) => c == index ? new Field(f.Name, replacement.Data.DataType, f.IsNullable) : f);
            return new RecordBatch(new Apache.Arrow.Schema(fields, batch.Schema.Metadata), columns, batch.Length);
        }

        // The partition unchanged, then a row moved to another partition.
        await table.UpdateAsync(b => Where(b, id => id == 1), b => Set(b, "note", "changed"));
        await table.UpdateAsync(b => Where(b, id => id == 2), b => Set(b, "region", "north", asView: true));

        var rows = new Dictionary<long, string>();
        await foreach (var batch in table.ReadAsync(new DeltaReadOptions { Columns = ["id", "region", "note"] }))
        {
            using (batch)
            {
                for (int r = 0; r < batch.Length; r++)
                {
                    long id = ((Int64Array)batch.Column(0)).GetValue(r)!.Value;
                    rows[id] = Value(batch.Column(1), r) + "|" + Value(batch.Column(2), r);
                }
            }
        }

        Assert.Equal("west|changed", rows[1]);
        Assert.Equal("north|note-2", rows[2]);
        Assert.Equal("west|null", rows[3]);
        Assert.Equal(6, rows.Count);
    }

    /// <summary>
    /// A partition value in a string view is routed like any other string (review round 2): partition routing read
    /// only plain and large strings, and refused a view.
    /// </summary>
    [Fact]
    public async Task AWriteRoutesAStringViewPartitionValue()
    {
        var fs = new LocalTableFileSystem(Path.Combine(_tempDir, "views"));
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("region", StringType.Default, true))
            .Build();
        await using var table = await DeltaTable.CreateAsync(fs, schema, partitionColumns: ["region"]);

        var viewSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("region", StringViewType.Default, true))
            .Build();
        var regions = new StringViewArray.Builder().Append("east").Append("west").Append("east").Build();
        await table.WriteAsync([new RecordBatch(viewSchema,
            [new Int64Array.Builder().Append(0).Append(1).Append(2).Build(), regions], 3)]);

        Assert.Equal(["region=east", "region=west"],
            table.CurrentSnapshot.ActiveFiles.Keys.Select(k => k.Split('/')[0]).Distinct().OrderBy(k => k, StringComparer.Ordinal));
        var rows = new List<string>();
        await foreach (var batch in table.ReadAsync(new DeltaReadOptions()))
        {
            using (batch)
            {
                for (int r = 0; r < batch.Length; r++)
                    rows.Add(((Int64Array)batch.Column(0)).GetValue(r) + "|" + Value(batch.Column(1), r));
            }
        }

        rows.Sort(StringComparer.Ordinal);
        Assert.Equal(["0|east", "1|west", "2|east"], rows);
    }

    private static string Render(Apache.Arrow.Schema schema) =>
        string.Join(", ", schema.FieldsList.Select(f => f.Name + ":" + Render(f.DataType)));

    private static string Render(IArrowType type) => type switch
    {
        StructType s => "struct<" + string.Join(",", s.Fields.Select(f => f.Name + ":" + Render(f.DataType))) + ">",
        _ => type.Name,
    };

    private static string Row(RecordBatch batch, int r)
    {
        string Text(string name)
        {
            var column = batch.Column(batch.Schema.GetFieldIndex(name));
            return Value(column, r);
        }

        var info = (StructArray)batch.Column(batch.Schema.GetFieldIndex("info"));
        long id = ((Int64Array)batch.Column(batch.Schema.GetFieldIndex("id"))).GetValue(r)!.Value;
        return $"{id}|{Text("region")}|{Text("note")}|{Text("blob")}|{Value(info.Fields[0], r)}|{Text("extra")}";
    }

    private static string Hex(ReadOnlySpan<byte> bytes) =>
        string.Concat(bytes.ToArray().Select(b => b.ToString("x2")));

    private static string Value(IArrowArray column, int r)
    {
        if (column.IsNull(r))
            return "null";
        return column switch
        {
            StringArray a => a.GetString(r),
            StringViewArray a => a.GetString(r),
            LargeStringArray a => a.GetString(r),
            BinaryArray a => Hex(a.GetBytes(r)),
            BinaryViewArray a => Hex(a.GetBytes(r)),
            LargeBinaryArray a => Hex(a.GetBytes(r)),
            _ => throw new NotSupportedException(column.GetType().Name),
        };
    }
}
