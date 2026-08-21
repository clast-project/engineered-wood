// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Compression;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.Tests.Parquet;

/// <summary>
/// A table with no rows still has columns, and a file with no row groups still has a schema.
/// Both ends of that used to be unreachable: the writer captures its schema from the first batch it
/// encodes, so writing none left the footer declaring nothing, and the reader surfaces the Arrow
/// schema only on a <see cref="RecordBatch"/>, of which such a file yields none.
/// </summary>
public class ZeroRowSchemaTests : IDisposable
{
    private readonly string _tempDir;

    public ZeroRowSchemaTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-zerorow-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
        GC.SuppressFinalize(this);
    }

    private string TempPath(string name) => Path.Combine(_tempDir, name);

    private static readonly ParquetWriteOptions Uncompressed =
        new() { Compression = CompressionCodec.Uncompressed };

    private static Apache.Arrow.Schema TwoColumns() =>
        new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, nullable: false))
            .Field(new Field("label", StringType.Default, nullable: true))
            .Build();

    [Fact]
    public async Task ATableWithNoRowsKeepsItsColumns()
    {
        string path = TempPath("declared.parquet");
        var declared = TwoColumns();

        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false, Uncompressed))
        {
            writer.DeclareSchema(declared);
            await writer.CloseAsync();
        }

        await using var readFile = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(readFile, ownsFile: false);
        var metadata = await reader.ReadMetadataAsync();
        var arrow = await reader.GetArrowSchemaAsync();

        Assert.Equal(0, metadata.NumRows);
        Assert.Empty(metadata.RowGroups);
        Assert.Equal(2, arrow.FieldsList.Count);
        Assert.Equal(["id", "label"], arrow.FieldsList.Select(field => field.Name));
        Assert.False(arrow.FieldsList[0].IsNullable);
        Assert.True(arrow.FieldsList[1].IsNullable);
    }

    [Fact]
    public async Task WithoutADeclarationAnEmptyFileStillDeclaresNothing()
    {
        // The declaration is the fix, not a change to what an undeclared empty write does. This
        // pins that so the two cases cannot quietly merge.
        string path = TempPath("undeclared.parquet");

        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false, Uncompressed))
        {
            await writer.CloseAsync();
        }

        await using var readFile = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(readFile, ownsFile: false);

        Assert.Empty((await reader.GetArrowSchemaAsync()).FieldsList);
    }

    [Fact]
    public async Task AWrittenRowGroupWinsOverTheDeclaration()
    {
        // The declaration is a footer fallback only. Were it to win, a caller could describe the
        // file as something other than what was encoded -- and variant shredding rewrites column
        // types during the write, so the encoded truth is the only safe source.
        string path = TempPath("superseded.parquet");
        var encoded = new Apache.Arrow.Schema.Builder()
            .Field(new Field("actual", Int32Type.Default, nullable: false))
            .Build();
        var batch = new RecordBatch(
            encoded, [new Int32Array.Builder().Append(1).Append(2).Build()], length: 2);

        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false, Uncompressed))
        {
            writer.DeclareSchema(TwoColumns());
            await writer.WriteRowGroupAsync(batch);
            await writer.CloseAsync();
        }

        await using var readFile = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(readFile, ownsFile: false);
        var arrow = await reader.GetArrowSchemaAsync();

        Assert.Equal(["actual"], arrow.FieldsList.Select(field => field.Name));
    }

    [Fact]
    public async Task TheArrowSchemaMatchesWhatABatchWouldCarry()
    {
        // GetArrowSchemaAsync exists so a caller can see the schema without data, so it must be
        // the same schema -- not merely a compatible one -- as the batches carry when there are any.
        string path = TempPath("matches.parquet");
        var schema = TwoColumns();
        var batch = new RecordBatch(
            schema,
            [
                new Int64Array.Builder().Append(1L).Build(),
                new StringArray.Builder().Append("x").Build(),
            ],
            length: 1);

        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false, Uncompressed))
        {
            await writer.WriteRowGroupAsync(batch);
            await writer.CloseAsync();
        }

        await using var readFile = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(readFile, ownsFile: false);
        var withoutData = await reader.GetArrowSchemaAsync();
        var fromBatch = (await reader.ReadRowGroupAsync(0)).Schema;

        Assert.Equal(
            fromBatch.FieldsList.Select(field => $"{field.Name}:{field.DataType}:{field.IsNullable}"),
            withoutData.FieldsList.Select(field => $"{field.Name}:{field.DataType}:{field.IsNullable}"));
    }

    [Fact]
    public async Task DeclaringNothingOrDeclaringLateIsRefused()
    {
        string path = TempPath("refused.parquet");
        await using var file = new LocalSequentialFile(path);
        await using var writer = new ParquetFileWriter(file, ownsFile: false, Uncompressed);

        Assert.Throws<ArgumentNullException>(() => writer.DeclareSchema(null!));

        await writer.CloseAsync();

        // After close the footer is already written, so a declaration could not reach it.
        Assert.Throws<InvalidOperationException>(() => writer.DeclareSchema(TwoColumns()));
    }
}
