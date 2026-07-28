// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

#pragma warning disable EWDELTA0001 // codec seam is experimental

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// The codec seam is VALUE-BLIND: between a caller's batch and the host's <see cref="IDataFileWriter"/>, the
/// library moves and renames columns but never inspects what is IN them, so a host that owns the bytes may
/// present its own physical representation for a column whose Delta type it has declared.
///
/// <para>That property is what lets a host handle a representation the library has no mode for — the
/// motivating case being VARIANT transport: an embedding host whose Arrow boundary cannot carry the canonical
/// struct storage (DuckDB's <c>ArrowAppender</c> crashes on the nested extension type across the C data
/// interface) exchanges each variant as one self-delimiting blob, converts on its own side, and needs nothing
/// from the library. Pinned here because it is a CONTRACT, not an accident: the physical rename, the variant
/// annotation policy and the statistics collector all sit on that path, and any of them growing a type check
/// would break an embedding host silently, at write time, with no library test otherwise noticing.</para>
///
/// <para>The read direction is deliberately NOT symmetric, and the asymmetry is asserted below: a
/// variant-declared column must reach the read path as the physical struct-of-binary (or an already-wrapped
/// <c>VariantArray</c>), because the library presents variants from the SCHEMA and fails loudly rather than
/// emit a column contradicting the declared type. A host reader splits its blob into (metadata, value) — pure
/// in-process work that never crosses the C interface.</para>
/// </summary>
public class CodecSeamValueBlindnessTests : IDisposable
{
    private readonly string _tempDir;

    public CodecSeamValueBlindnessTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_vspike_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    private static readonly byte[] EmptyMetadata = [0x01, 0x00, 0x00];

    /// <summary>Captures what the library hands the host writer, and writes nothing.</summary>
    private sealed class CapturingWriter : IDataFileWriter
    {
        public List<RecordBatch> Received { get; } = [];
        public List<string> Paths { get; } = [];

        public async ValueTask<long> WriteAsync(
            IAsyncEnumerable<RecordBatch> batches, string relativePath, CancellationToken cancellationToken)
        {
            Paths.Add(relativePath);
            await foreach (var b in batches.WithCancellation(cancellationToken))
                Received.Add(b);
            return 4096;
        }
    }

    private static Apache.Arrow.Schema VariantSchema() =>
        new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("v", VariantType.Default, true))
            .Build();

    /// <summary>
    /// The host's TRANSPORT shape: the variant column as ONE self-delimiting binary per row (metadata
    /// bytes ++ value bytes), declared as plain binary in the Arrow schema — what a boundary that cannot
    /// carry the canonical nested struct would produce.
    /// </summary>
    private static RecordBatch TransportBatch(int rows)
    {
        var ids = new Int64Array.Builder();
        var blobs = new BinaryArray.Builder();
        for (int i = 0; i < rows; i++)
        {
            ids.Append(i + 1);
            var blob = new byte[EmptyMetadata.Length + 1];
            EmptyMetadata.CopyTo(blob, 0);
            blob[^1] = 0x04; // a spec-valid variant primitive: boolean-true
            blobs.Append(blob);
        }
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("v", BinaryType.Default, true))   // ← NOT VariantType
            .Build();
        return new RecordBatch(schema, [ids.Build(), blobs.Build()], rows);
    }

    [Fact]
    public async Task WriteDataFiles_BlobColumnAgainstVariantSchema_ThroughCodecSeam()
    {
        var writer = new CapturingWriter();
        var options = DeltaTableOptions.Default with { DataFileWriter = writer };
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), VariantSchema(), options);

        // The whole question: does this throw, mangle the column, or pass it through?
        var files = await table.WriteDataFilesAsync([TransportBatch(3)]);

        Assert.Single(files);
        var handed = Assert.Single(writer.Received);

        // What reached the host writer, verbatim?
        var column = handed.Column(handed.Schema.GetFieldIndex("v"));
        Assert.IsType<BinaryArray>(column);
        Assert.Equal(3, handed.Length);

        // And the add action the library built for it.
        Assert.Equal(3, files[0].NumRecords);
    }

    [Fact]
    public async Task WriteDataFiles_BlobColumn_UnderColumnMapping()
    {
        // Column mapping is where the physical rename actually walks the schema recursively, so it is the
        // likelier place for a type mismatch to be noticed.
        var writer = new CapturingWriter();
        var options = DeltaTableOptions.Default with { DataFileWriter = writer };
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), VariantSchema(), options,
            columnMappingMode: Schema.ColumnMappingMode.Name);

        var files = await table.WriteDataFilesAsync([TransportBatch(2)]);

        Assert.Single(files);
        var handed = Assert.Single(writer.Received);
        Assert.Equal(2, handed.Length);
        // The variant column should have been renamed to its physical name and still be a blob.
        Assert.Equal(2, handed.ColumnCount);
    }

    [Fact]
    public async Task WriteDataFiles_BlobColumn_PartitionedAndStatsCollected()
    {
        // Stats collection reads the batch's columns; a partitioned table also runs the split first.
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("region", StringType.Default, true))
            .Field(new Field("v", VariantType.Default, true))
            .Build();

        var writer = new CapturingWriter();
        var options = DeltaTableOptions.Default with { DataFileWriter = writer, CollectStats = true };
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), schema, options, partitionColumns: ["region"]);

        var ids = new Int64Array.Builder();
        var regions = new StringArray.Builder();
        var blobs = new BinaryArray.Builder();
        foreach (var (id, region) in new[] { (1L, "us"), (2L, "eu") })
        {
            ids.Append(id);
            regions.Append(region);
            var blob = new byte[EmptyMetadata.Length + 1];
            EmptyMetadata.CopyTo(blob, 0);
            blob[^1] = 0x04;
            blobs.Append(blob);
        }
        var arrowSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("region", StringType.Default, true))
            .Field(new Field("v", BinaryType.Default, true))
            .Build();
        var batch = new RecordBatch(arrowSchema, [ids.Build(), regions.Build(), blobs.Build()], 2);

        var files = await table.WriteDataFilesAsync([batch]);

        Assert.Equal(2, files.Count); // one per partition
        Assert.All(files, f => Assert.Equal(1, f.NumRecords));
        Assert.All(files, f => Assert.NotNull(f.StatsJson));
    }

    /// <summary>Returns the PHYSICAL struct-of-binary layout for the variant column — what a host would
    /// build in .NET after splitting its transport blob back into (metadata, value).</summary>
    private sealed class StructEmittingReader : IDataFileReader
    {
        private readonly int _rows;
        public StructEmittingReader(int rows) => _rows = rows;

        public async IAsyncEnumerable<RecordBatch> ReadAsync(
            string relativePath, IReadOnlyList<string>? physicalColumns,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            var ids = new Int64Array.Builder();
            var metadata = new BinaryArray.Builder();
            var values = new BinaryArray.Builder();
            for (int i = 0; i < _rows; i++)
            {
                ids.Append(i + 1);
                metadata.Append(EmptyMetadata);
                values.Append(new byte[] { 0x04 });
            }
            var storage = new StructArray(
                new StructType([
                    new Field("metadata", BinaryType.Default, false),
                    new Field("value", BinaryType.Default, false),
                ]),
                _rows,
                [metadata.Build(), values.Build()],
                ArrowBuffer.Empty); // no nulls

            var schema = new Apache.Arrow.Schema.Builder()
                .Field(new Field("id", Int64Type.Default, false))
                .Field(new Field("v", storage.Data.DataType, true))
                .Build();
            yield return new RecordBatch(schema, [ids.Build(), storage], _rows);
            await Task.CompletedTask;
        }
    }

    /// <summary>
    /// The read half: a host reader that hands back the physical struct-of-binary (rather than its own
    /// transport blob) gets the canonical <see cref="VariantArray"/> from the library, which it can then
    /// convert to blobs on its own side of the boundary. If this works, the read direction needs no
    /// library support either.
    /// </summary>
    [Fact]
    public async Task Read_HostReaderEmittingPhysicalStruct_CoercesToVariant()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        // Build a real table + file through the normal path so the log is genuine.
        await using (var seed = await DeltaTable.CreateAsync(fs, VariantSchema()))
        {
            var ids = new Int64Array.Builder();
            var variants = new VariantArray.Builder();
            for (int i = 0; i < 3; i++)
            {
                ids.Append(i + 1);
                variants.Append(EmptyMetadata, [0x04]);
            }
            await seed.WriteAsync([new RecordBatch(
                VariantSchema(), [ids.Build(), variants.Build(allocator: null)], 3)]);
        }

        // Now read it back with the host owning the decode.
        var options = DeltaTableOptions.Default with { DataFileReader = new StructEmittingReader(3) };
        await using var table = await DeltaTable.OpenAsync(fs, options);

        var read = new List<RecordBatch>();
        await foreach (var b in table.ReadAllAsync())
            read.Add(b);

        var batch = Assert.Single(read);
        var v = Assert.IsType<VariantArray>(batch.Column(batch.Schema.GetFieldIndex("v")));
        Assert.Equal(3, v.Length);
    }

    /// <summary>Returns the host's TRANSPORT blob for the variant column — the shape the read path must
    /// refuse.</summary>
    private sealed class BlobEmittingReader : IDataFileReader
    {
        public async IAsyncEnumerable<RecordBatch> ReadAsync(
            string relativePath, IReadOnlyList<string>? physicalColumns,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            yield return TransportBatch(2);
            await Task.CompletedTask;
        }
    }

    /// <summary>
    /// The read direction is NOT value-blind, by design: a variant-declared column arriving as anything but
    /// the physical struct (or an already-wrapped VariantArray) fails LOUDLY rather than emitting a column
    /// that contradicts the declared type. This is what obliges a host reader to split its blob rather than
    /// pass it through, so the asymmetry is asserted rather than left to be discovered.
    /// </summary>
    [Fact]
    public async Task Read_HostReaderEmittingBlob_FailsLoudly()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using (var seed = await DeltaTable.CreateAsync(fs, VariantSchema()))
        {
            var ids = new Int64Array.Builder();
            var variants = new VariantArray.Builder();
            ids.Append(1L);
            variants.Append(EmptyMetadata, [0x04]);
            await seed.WriteAsync([new RecordBatch(
                VariantSchema(), [ids.Build(), variants.Build(allocator: null)], 1)]);
        }

        var options = DeltaTableOptions.Default with { DataFileReader = new BlobEmittingReader() };
        await using var table = await DeltaTable.OpenAsync(fs, options);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (var _ in table.ReadAllAsync())
            {
            }
        });
        Assert.Contains("declared variant", ex.Message, StringComparison.Ordinal);
    }

    // ── Delta-typed ADD COLUMN ──

    private static Schema.StructField VariantField(string name) => new()
    {
        Name = name,
        Type = new Schema.PrimitiveType { TypeName = "variant" },
        Nullable = true,
    };

    /// <summary>
    /// A host whose Arrow boundary carries variants as binary cannot express "add a VARIANT column" through
    /// the Arrow overload — it would add Delta <c>binary</c>, permanently, in a metadata commit. The
    /// Delta-typed overload is how it says what it means. Both halves asserted, since the contrast is the
    /// reason the overload exists.
    /// </summary>
    [Fact]
    public async Task AddColumn_DeltaTyped_AddsVariantWhereTheArrowOverloadWouldAddBinary()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build());

        await table.AddColumnAsync(new Field("as_binary", BinaryType.Default, true));
        await table.AddColumnAsync(VariantField("as_variant"));

        string TypeOf(string name) =>
            ((Schema.PrimitiveType)table.CurrentSnapshot.Schema.Fields
                .Single(f => f.Name == name).Type).TypeName;

        Assert.Equal("binary", TypeOf("as_binary"));
        Assert.Equal("variant", TypeOf("as_variant"));
    }

    [Fact]
    public async Task ComputeAddColumn_DeltaTyped_StagesOnATransaction()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build(), columnMappingMode: Schema.ColumnMappingMode.Name);

        var txn = table.StartTransaction();
        txn.StageSchemaChange(table.ComputeAddColumn(VariantField("v")));
        await txn.CommitAsync();

        await using var check = await DeltaTable.OpenAsync(fs);
        var field = check.CurrentSnapshot.Schema.Fields.Single(f => f.Name == "v");
        Assert.Equal("variant", ((Schema.PrimitiveType)field.Type).TypeName);
        // Column mapping assigned it an id + physical name, as for any other added column.
        Assert.NotNull(Schema.ColumnMapping.GetFieldId(field));
    }

    [Fact]
    public async Task AddColumn_DeltaTyped_RejectsANonNullableColumn()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build());

        var notNullable = new Schema.StructField
        {
            Name = "v",
            Type = new Schema.PrimitiveType { TypeName = "variant" },
            Nullable = false,
        };
        await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await table.AddColumnAsync(notNullable));
    }
}
