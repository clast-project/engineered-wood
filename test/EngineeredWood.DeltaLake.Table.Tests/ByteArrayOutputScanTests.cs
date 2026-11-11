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

    /// <summary>A host that always returns plain utf8 and binary, whatever the table's options ask for.</summary>
    private sealed class PlainReader(ITableFileSystem fs) : IDataFileReader
    {
        public async IAsyncEnumerable<RecordBatch> ReadAsync(
            string relativePath, IReadOnlyList<string>? physicalColumns,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await using var file = await fs.OpenReadAsync(relativePath, cancellationToken);
            using var reader = new ParquetFileReader(file, ownsFile: false);
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
            foreach (bool seam in new[] { false, true })
                yield return [kind, seam];
        }
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task EveryBatchMatchesGetReadSchema(ByteArrayOutputKind kind, bool seam)
    {
        string path = await WriteTableAsync();
        var fs = new LocalTableFileSystem(path);
        var options = new DeltaTableOptions
        {
            ParquetReadOptions = new ParquetReadOptions { ByteArrayOutput = kind },
            DataFileReader = seam ? new PlainReader(fs) : null,
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
