// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Metadata;
using ArrowSchema = Apache.Arrow.Schema;

namespace EngineeredWood.Tests.Parquet.Data;

/// <summary>
/// <see cref="ParquetFileReader.ReadStoredInt32"/> reads the chosen INT(8)/INT(16)-annotated INT32 leaves as the
/// INT32 values they store (#839), in batches and in the reported schema alike, and leaves every other leaf as
/// its annotation says.
/// </summary>
public class StoredInt32ReadTests : IDisposable
{
    private readonly string _tempDir;

    public StoredInt32ReadTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-stored-int32-" + Guid.NewGuid().ToString("N").Substring(0, 8));
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private static StructType Inner => new([new Field("c", Int8Type.Default, true)]);

    private async Task<string> WriteAsync()
    {
        var b = new Int8Array.Builder().Append(sbyte.MinValue).AppendNull().Append(sbyte.MaxValue).Build();
        var s = new Int16Array.Builder().Append(short.MinValue).Append(0).Append(short.MaxValue).Build();
        var keep = new Int8Array.Builder().Append(1).Append(2).Append(3).Build();
        var u = new UInt8Array.Builder().Append(0).Append(200).Append(255).Build();
        var c = new Int8Array.Builder().Append(-7).Append(7).AppendNull().Build();
        var st = new StructArray(Inner, 3, [c], ArrowBuffer.Empty, 0);
        var schema = new ArrowSchema(
        [
            new Field("b", Int8Type.Default, true),
            new Field("s", Int16Type.Default, false),
            new Field("keep", Int8Type.Default, false),
            new Field("u", UInt8Type.Default, false),
            new Field("st", Inner, true),
        ], null);

        string path = Path.Combine(_tempDir, Guid.NewGuid().ToString("N") + ".parquet");
        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false))
        {
            await writer.WriteRowGroupAsync(new RecordBatch(schema, [b, s, keep, u, st], 3));
            await writer.CloseAsync();
        }
        return path;
    }

    [Fact]
    public async Task ChosenLeaves_ReadAsStoredInt32_OthersAsAnnotated()
    {
        string path = await WriteAsync();
        using var reader = new ParquetFileReader(new LocalRandomAccessFile(path));
        var schema = await reader.GetSchemaAsync();
        // u (UINT_8) is chosen too, but only SIGNED narrow ints are relabeled.
        var chosen = new HashSet<SchemaElement>(schema.Columns
            .Where(col => col.DottedPath is "b" or "s" or "u" or "st.c")
            .Select(col => col.SchemaElement));
        reader.ReadStoredInt32(chosen);

        var arrow = await reader.GetArrowSchemaAsync();
        Assert.Equal(Int32Type.Default.TypeId, arrow.GetFieldByName("b").DataType.TypeId);
        Assert.Equal(Int32Type.Default.TypeId, arrow.GetFieldByName("s").DataType.TypeId);
        Assert.Equal(Int8Type.Default.TypeId, arrow.GetFieldByName("keep").DataType.TypeId);
        Assert.Equal(UInt8Type.Default.TypeId, arrow.GetFieldByName("u").DataType.TypeId);
        Assert.Equal(Int32Type.Default.TypeId,
            ((StructType)arrow.GetFieldByName("st").DataType).Fields[0].DataType.TypeId);

        var batch = Assert.Single(await ReadAllAsync(reader));
        Assert.Equal([sbyte.MinValue, null, sbyte.MaxValue], Assert.IsType<Int32Array>(batch.Column("b")).ToArray());
        Assert.Equal([short.MinValue, 0, short.MaxValue], Assert.IsType<Int32Array>(batch.Column("s")).ToArray());
        Assert.Equal([1, 2, 3], Assert.IsType<Int8Array>(batch.Column("keep")).Values.ToArray().Select(v => (int)v));
        Assert.Equal([0, 200, 255], Assert.IsType<UInt8Array>(batch.Column("u")).Values.ToArray().Select(v => (int)v));
        var st = Assert.IsType<StructArray>(batch.Column("st"));
        Assert.Equal([-7, 7, null], Assert.IsType<Int32Array>(st.Fields[0]).ToArray());
    }

    [Fact]
    public async Task NothingChosen_ReadsAsAnnotated()
    {
        string path = await WriteAsync();
        using var reader = new ParquetFileReader(new LocalRandomAccessFile(path));
        reader.ReadStoredInt32([]);

        var batch = Assert.Single(await ReadAllAsync(reader));
        Assert.IsType<Int8Array>(batch.Column("b"));
        Assert.IsType<Int16Array>(batch.Column("s"));
    }

    private static async Task<List<RecordBatch>> ReadAllAsync(ParquetFileReader reader)
    {
        var batches = new List<RecordBatch>();
        await foreach (var batch in reader.ReadAllAsync())
            batches.Add(batch);
        return batches;
    }
}
