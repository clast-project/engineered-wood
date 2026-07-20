// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Operations.VariantJson;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Metadata;

namespace EngineeredWood.Tests.Parquet;

/// <summary>
/// Round-trip tests for <see cref="VariantArray"/> through the Parquet writer/reader.
/// Verifies that a VariantArray column emits a Parquet group annotated with the
/// VARIANT logical type, and that the reader produces VariantArray when the
/// caller registers the extension via
/// <see cref="ParquetReadOptions.ExtensionRegistry"/>.
/// </summary>
public class VariantArrayRoundTripTests : IDisposable
{
    private readonly string _tempDir;

    public VariantArrayRoundTripTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-variant-test-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
        GC.SuppressFinalize(this);
    }

    private static ExtensionTypeRegistry VariantRegistry()
    {
        var registry = new ExtensionTypeRegistry();
        registry.Register(VariantExtensionDefinition.Instance);
        return registry;
    }

    /// <summary>
    /// Builds a tiny RecordBatch with a single nullable VARIANT column.
    /// Each row uses canonical empty metadata (version=1, no dict entries) and
    /// the value bytes provided by the caller.
    /// </summary>
    private static (RecordBatch batch, byte[][] metas, byte[][] values) MakeVariantBatch(byte[][] valueBytes)
    {
        // Canonical empty metadata: version byte 0x01, dict_size=0 (varint), offset=0.
        byte[] meta = [0x01, 0x00, 0x00];
        var metas = new byte[valueBytes.Length][];

        var builder = new VariantArray.Builder();
        for (int i = 0; i < valueBytes.Length; i++)
        {
            metas[i] = meta;
            builder.Append(meta, valueBytes[i]);
        }
        var arr = builder.Build(allocator: null);

        var field = new Field("v", arr.Data.DataType, nullable: true);
        var schema = new Apache.Arrow.Schema(new[] { field }, metadata: null);
        return (new RecordBatch(schema, new IArrowArray[] { arr }, valueBytes.Length), metas, valueBytes);
    }

    [Fact]
    public async Task VariantColumn_EmitsVariantLogicalTypeAnnotation()
    {
        string path = Path.Combine(_tempDir, "variant_annot.parquet");
        var (batch, _, _) = MakeVariantBatch(new[]
        {
            new byte[] { 0x00 }, // primitive null
            new byte[] { 0x0C }, // primitive boolean true (per Variant spec basic_type=primitive, type_id=2 -> 0b00001100)
        });

        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false))
        {
            await writer.WriteRowGroupAsync(batch);
            await writer.CloseAsync();
        }

        await using var readFile = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(readFile, ownsFile: false);
        var meta = await reader.ReadMetadataAsync();

        // The top-level group element (index 1, since index 0 is the synthetic
        // root) should carry the VARIANT logical type.
        var schema = meta.Schema;
        var groupElement = schema.First(s => s.Name == "v");
        Assert.IsType<LogicalType.VariantType>(groupElement.LogicalType);
        Assert.Equal(2, groupElement.NumChildren);

        // Storage children: metadata (BYTE_ARRAY) and value (BYTE_ARRAY).
        var metaChild = schema.First(s => s.Name == "metadata");
        var valueChild = schema.First(s => s.Name == "value");
        Assert.Equal(PhysicalType.ByteArray, metaChild.Type);
        Assert.Equal(PhysicalType.ByteArray, valueChild.Type);
    }

    [Fact]
    public async Task ReadWithoutRegistry_ProducesStructArray()
    {
        string path = Path.Combine(_tempDir, "variant_no_reg.parquet");
        var (batch, _, _) = MakeVariantBatch(new[]
        {
            new byte[] { 0x00 },
            new byte[] { 0x0C },
            new byte[] { 0x00 },
        });

        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false))
        {
            await writer.WriteRowGroupAsync(batch);
            await writer.CloseAsync();
        }

        // No registry → reader returns a bare StructArray with metadata+value
        // BinaryArray children.
        await using var readFile = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(readFile, ownsFile: false);
        var read = await reader.ReadRowGroupAsync(0);

        var col = read.Column(0);
        var sa = Assert.IsType<StructArray>(col);
        Assert.Equal(3, sa.Length);
        Assert.Equal(2, sa.Fields.Count);

        var metaCol = Assert.IsAssignableFrom<BinaryArray>(sa.Fields[0]);
        var valueCol = Assert.IsAssignableFrom<BinaryArray>(sa.Fields[1]);
        Assert.Equal(3, metaCol.Length);
        Assert.Equal(3, valueCol.Length);
    }

    [Fact]
    public async Task ReadWithRegistry_ProducesVariantArray()
    {
        string path = Path.Combine(_tempDir, "variant_with_reg.parquet");
        var (batch, _, values) = MakeVariantBatch(new[]
        {
            new byte[] { 0x00 }, // primitive null
            new byte[] { 0x0C }, // primitive boolean true
            new byte[] { 0x08 }, // primitive boolean false
            new byte[] { 0x00 },
        });

        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false))
        {
            await writer.WriteRowGroupAsync(batch);
            await writer.CloseAsync();
        }

        await using var readFile = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(readFile, ownsFile: false,
            new ParquetReadOptions { ExtensionRegistry = VariantRegistry() });
        var read = await reader.ReadRowGroupAsync(0);

        var col = read.Column(0);
        var va = Assert.IsType<VariantArray>(col);
        Assert.Equal(values.Length, va.Length);
        Assert.False(va.IsShredded);

        // Round-trip the value bytes for each row.
        for (int i = 0; i < values.Length; i++)
        {
            var got = va.GetValueBytes(i);
            Assert.Equal(values[i], got.ToArray());
        }
    }

    /// <summary>
    /// Walks every <c>.parquet</c> file in
    /// <c>parquet-testing/shredded_variant/</c> with the registry registered.
    /// Each VARIANT-annotated column should materialise as a
    /// <see cref="VariantArray"/>; the test fails if any file errors out or
    /// produces a column we recognise as VARIANT but didn't wrap.
    /// <para>Structure only — for VALUE-level conformance see
    /// <see cref="VariantCorpus_MatchesReferenceVariants"/>. The corpus's declared error cases are
    /// excluded here: reassembly validates the shredding, so those files now throw by design (that
    /// they throw is asserted by the value-level test).</para>
    /// </summary>
    [Fact]
    public async Task SweepTest_ShreddedVariantCorpus_AllReadAsVariantArray()
    {
        string? shreddedRoot = FindShreddedVariantDirectory();
        if (shreddedRoot is null) return; // submodule not initialized in this checkout

        var errorCases = await ReadManifestErrorCasesAsync(shreddedRoot);
        var files = Directory.GetFiles(shreddedRoot, "*.parquet")
            .Where(f => !errorCases.Contains(Path.GetFileName(f)))
            .ToArray();
        Assert.NotEmpty(files);

        var registry = VariantRegistry();
        var failures = new List<string>();
        int variantColumns = 0;
        int shreddedFiles = 0;

        foreach (var filePath in files)
        {
            string name = Path.GetFileName(filePath);
            try
            {
                await using var file = new LocalRandomAccessFile(filePath);
                await using var reader = new ParquetFileReader(file, ownsFile: false,
                    new ParquetReadOptions { ExtensionRegistry = registry });

                var meta = await reader.ReadMetadataAsync();
                if (meta.RowGroups.Count == 0) continue;

                // Shredded-ness is a property of the FILE: the materialised array is always
                // unshredded now, because the reader reassembles before handing it back.
                if (meta.Schema.Any(s => s.Name == "typed_value")) shreddedFiles++;

                var batch = await reader.ReadRowGroupAsync(0);
                for (int i = 0; i < batch.ColumnCount; i++)
                {
                    bool annotated = batch.Schema.FieldsList[i].DataType is VariantType;
                    bool wrapped = batch.Column(i) is VariantArray;
                    if (annotated != wrapped)
                    {
                        failures.Add($"{name} col[{i}]: annotated={annotated} wrapped={wrapped}");
                    }
                    if (wrapped)
                    {
                        variantColumns++;
                        if (((VariantArray)batch.Column(i)).IsShredded)
                        {
                            failures.Add($"{name} col[{i}]: still shredded — reassembly did not run");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                failures.Add($"{name}: {ex.GetType().Name}: {ex.Message}");
            }
        }

        Assert.Empty(failures);
        Assert.True(variantColumns > 0, "Expected at least one VariantArray column across the corpus.");
        Assert.True(shreddedFiles > 0, "Expected at least one shredded file (typed_value present) in the corpus.");
    }

    // ── Nested variant: a variant inside struct / list / map. The schema converter produces a nested
    //    VariantType field at any depth; these pin that the ARRAY matches it (VariantNestedWrapper),
    //    rather than silently materialising the nested variant as a bare struct. ──

    private static readonly byte[] Meta = [0x01, 0x00, 0x00];
    private static readonly byte[] VTrue = [0x04];
    private static readonly byte[] VFalse = [0x08];

    private static VariantArray MakeVariants(params byte[][] values)
    {
        var b = new VariantArray.Builder();
        foreach (var v in values)
            b.Append(Meta, v);
        return b.Build(allocator: null);
    }

    private async Task<RecordBatch> RoundTrip(Apache.Arrow.Schema schema, IArrowArray[] columns, int rowCount)
    {
        string path = Path.Combine(_tempDir, $"nested_{Guid.NewGuid():N}.parquet");
        await using (var f = new LocalSequentialFile(path))
        await using (var w = new ParquetFileWriter(f, ownsFile: false))
        {
            await w.WriteRowGroupAsync(new RecordBatch(schema, columns, rowCount));
            await w.CloseAsync();
        }
        await using var rf = new LocalRandomAccessFile(path);
        await using var r = new ParquetFileReader(rf, ownsFile: false,
            new ParquetReadOptions { ExtensionRegistry = VariantRegistry() });
        return await r.ReadRowGroupAsync(0);
    }

    [Fact]
    public async Task VariantInsideStruct_ReadsAsVariantArray()
    {
        var v = MakeVariants(VTrue, VFalse);
        var st = new StructType([new Field("v", v.Data.DataType, true)]);
        var s = new StructArray(st, 2, [v], ArrowBuffer.Empty, 0);
        var schema = new Apache.Arrow.Schema.Builder().Field(new Field("s", st, true)).Build();

        var read = await RoundTrip(schema, [s], 2);

        // Schema field and array must agree: struct<v: variant>, array struct<v: VariantArray>.
        Assert.IsType<VariantType>(((StructType)read.Schema.GetFieldByName("s").DataType).Fields[0].DataType);
        var outStruct = Assert.IsType<StructArray>(read.Column(0));
        var outVar = Assert.IsType<VariantArray>(outStruct.Fields[0]);
        Assert.Equal(VTrue, outVar.GetValueBytes(0).ToArray());
        Assert.Equal(VFalse, outVar.GetValueBytes(1).ToArray());
    }

    [Fact]
    public async Task VariantInsideList_ReadsAsVariantArray()
    {
        var v = MakeVariants(VTrue, VFalse, VTrue);
        var offsets = new ArrowBuffer.Builder<int>();
        offsets.Append(0).Append(2).Append(3);          // row 0 -> [true, false], row 1 -> [true]
        var listType = new ListType(new Field("item", v.Data.DataType, true));
        var list = new ListArray(listType, 2, offsets.Build(), v, ArrowBuffer.Empty, 0);
        var schema = new Apache.Arrow.Schema.Builder().Field(new Field("l", listType, true)).Build();

        var read = await RoundTrip(schema, [list], 2);

        Assert.IsType<VariantType>(((ListType)read.Schema.GetFieldByName("l").DataType).ValueDataType);
        var outList = Assert.IsType<ListArray>(read.Column(0));
        var outVar = Assert.IsType<VariantArray>(outList.Values);
        Assert.Equal(VTrue, outVar.GetValueBytes(0).ToArray());
        Assert.Equal(VFalse, outVar.GetValueBytes(1).ToArray());
        Assert.Equal(VTrue, outVar.GetValueBytes(2).ToArray());
    }

    [Fact]
    public async Task VariantInsideMapValue_ReadsAsVariantArray()
    {
        // map<string, variant>: one row with entries {"a": true, "b": false}.
        var keys = new StringArray.Builder().Append("a").Append("b").Build();
        var vals = MakeVariants(VTrue, VFalse);
        var keyField = new Field("key", StringType.Default, false);
        var valueField = new Field("value", vals.Data.DataType, true);
        var entryType = new StructType([keyField, valueField]);
        var entries = new StructArray(entryType, 2, [keys, vals], ArrowBuffer.Empty, 0);
        var offsets = new ArrowBuffer.Builder<int>();
        offsets.Append(0).Append(2);
        var mapType = new MapType(keyField, valueField, keySorted: false);
        var map = new MapArray(mapType, 1, offsets.Build(), entries, ArrowBuffer.Empty, 0);
        var schema = new Apache.Arrow.Schema.Builder().Field(new Field("m", mapType, true)).Build();

        var read = await RoundTrip(schema, [map], 1);

        var outMap = Assert.IsType<MapArray>(read.Column(0));
        var outVar = Assert.IsType<VariantArray>(outMap.KeyValues.Fields[1]);
        Assert.Equal(VTrue, outVar.GetValueBytes(0).ToArray());
        Assert.Equal(VFalse, outVar.GetValueBytes(1).ToArray());
    }

    [Fact]
    public async Task NoRegistry_LeavesNestedVariantAsStruct()
    {
        var v = MakeVariants(VTrue);
        var st = new StructType([new Field("v", v.Data.DataType, true)]);
        var s = new StructArray(st, 1, [v], ArrowBuffer.Empty, 0);
        var schema = new Apache.Arrow.Schema.Builder().Field(new Field("s", st, true)).Build();

        string path = Path.Combine(_tempDir, $"nested_noreg_{Guid.NewGuid():N}.parquet");
        await using (var f = new LocalSequentialFile(path))
        await using (var w = new ParquetFileWriter(f, ownsFile: false))
        {
            await w.WriteRowGroupAsync(new RecordBatch(schema, [s], 1));
            await w.CloseAsync();
        }
        await using var rf = new LocalRandomAccessFile(path);
        await using var r = new ParquetFileReader(rf, ownsFile: false); // no registry
        var read = await r.ReadRowGroupAsync(0);

        // Without a registry the nested group stays a bare struct — symmetric with the top-level
        // behaviour, and the wrapper pass is skipped entirely.
        var outStruct = Assert.IsType<StructArray>(read.Column(0));
        Assert.IsNotType<VariantArray>(outStruct.Fields[0]);
    }

    private static string? FindShreddedVariantDirectory()
    {
        var dir = AppContext.BaseDirectory;
        while (dir is not null)
        {
            var candidate = Path.Combine(dir, "parquet-testing", "shredded_variant");
            if (Directory.Exists(candidate)) return candidate;
            dir = Path.GetDirectoryName(dir);
        }
        return null;
    }

    [Fact]
    public async Task ToggleRegistry_SameFile_GivesDifferentArrayTypes()
    {
        string path = Path.Combine(_tempDir, "variant_toggle.parquet");
        var (batch, _, _) = MakeVariantBatch(new[]
        {
            new byte[] { 0x00 },
            new byte[] { 0x0C },
        });

        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false))
        {
            await writer.WriteRowGroupAsync(batch);
            await writer.CloseAsync();
        }

        // No registry: StructArray.
        await using (var f1 = new LocalRandomAccessFile(path))
        await using (var r1 = new ParquetFileReader(f1, ownsFile: false))
        {
            var b1 = await r1.ReadRowGroupAsync(0);
            Assert.IsType<StructArray>(b1.Column(0));
        }

        // With registry: VariantArray.
        await using (var f2 = new LocalRandomAccessFile(path))
        await using (var r2 = new ParquetFileReader(f2, ownsFile: false,
            new ParquetReadOptions { ExtensionRegistry = VariantRegistry() }))
        {
            var b2 = await r2.ReadRowGroupAsync(0);
            Assert.IsType<VariantArray>(b2.Column(0));
        }
    }


    /// <summary>
    /// VALUE-level conformance against the shredded-variant corpus, driven by its own
    /// <c>cases.json</c> manifest. The sibling sweep test only asserts that annotation and wrapping
    /// agree — it never reads a value, so value-level correctness was previously unverified in BOTH
    /// directions; this is the acceptance gate for <see cref="Data.VariantShredding"/>.
    ///
    /// <para><b>Before reassembly landed</b> a shredded row's <c>value</c> child was empty (the data
    /// lives in <c>typed_value</c>), so <c>GetValueBytes</c> returned ZERO bytes while <c>IsNull</c>
    /// reported false — a valid row holding an empty variant, i.e. silent data loss. 61 of 131 cases
    /// failed then.</para>
    ///
    /// <para><b>Two assertion strengths, deliberately.</b> Every case is compared SEMANTICALLY (both
    /// sides rendered to JSON), because reassembly rebuilds the metadata dictionary and may legally
    /// choose a different encoding than the reference writer did — case 41 differs only in the
    /// metadata header's offset-size bits, case 43 in dictionary pruning. Unshredded columns are
    /// additionally compared BYTE-EXACTLY, since those are passed through untouched and any
    /// difference there would be a real defect.</para>
    ///
    /// <para>Manifest error cases must THROW: rejecting malformed shredding (conflicting
    /// value/typed_value, non-object values with shredded fields, unsupported shredded types) is
    /// part of the contract, and before reassembly they silently produced garbage.</para>
    ///
    /// No-ops when the parquet-testing submodule is not checked out.
    /// </summary>
    [Fact]
    public async Task VariantCorpus_MatchesReferenceVariants()
    {
        string? root = FindShreddedVariantDirectory();
        if (root is null) return; // submodule not initialized in this checkout

        string manifest = Path.Combine(root, "cases.json");
        if (!File.Exists(manifest)) return;

        using var doc = System.Text.Json.JsonDocument.Parse(File.ReadAllBytes(manifest));
        var registry = VariantRegistry();
        var failures = new List<string>();
        int rowsCompared = 0, bytesCompared = 0, errorCases = 0, casesRun = 0, implementationDefined = 0;

        foreach (var c in doc.RootElement.EnumerateArray())
        {
            if (!c.TryGetProperty("parquet_file", out var pf)) continue;   // manifest stub (case 3)
            string caseName = pf.GetString()!;
            string path = Path.Combine(root, caseName);

            // Error cases: the corpus says these are malformed, so reading must fail loudly.
            if (c.TryGetProperty("error_message", out var em))
            {
                errorCases++;
                try
                {
                    await ReadVariantColumnAsync(path, registry);
                    failures.Add($"{caseName}: expected a throw ({em.GetString()}), but read succeeded");
                }
                catch (Exception)
                {
                    // Expected. The corpus's own message is advisory; we only require fail-closed.
                }
                continue;
            }

            // Cases the corpus itself flags as spec-invalid-but-readable: its notes say
            // "implementations can choose to error, or read the shredded value". The reference
            // `variant` records Iceberg's choice (prefer the residual); Apache.Arrow reads the
            // shredded value. Both are conformant, so require only that we do not crash or corrupt —
            // reading must either throw or produce a well-formed variant, not a specific one.
            if (c.TryGetProperty("notes", out var notes)
                && notes.GetString() is { } n
                && n.Contains("not valid according to the spec", StringComparison.Ordinal))
            {
                implementationDefined++;
                try
                {
                    var permissive = await ReadVariantColumnAsync(path, registry);
                    foreach (var row in permissive.Rows.Where(r => r is not null))
                    {
                        // Well-formed means renderable; a corrupt reassembly throws here.
                        VariantJsonWriter.ToJson(row!.Value.Metadata, row.Value.Value, indented: false);
                    }
                }
                catch (Exception)
                {
                    // Erroring is the other permitted choice.
                }
                continue;
            }

            var expectedFiles = new List<string?>();
            if (c.TryGetProperty("variant_file", out var one))
            {
                expectedFiles.Add(one.GetString());
            }
            else if (c.TryGetProperty("variant_files", out var many))
            {
                foreach (var f in many.EnumerateArray())
                {
                    expectedFiles.Add(f.ValueKind == System.Text.Json.JsonValueKind.Null ? null : f.GetString());
                }
            }
            else
            {
                continue;
            }

            casesRun++;
            try
            {
                var actual = await ReadVariantColumnAsync(path, registry);

                if (actual.Rows.Count != expectedFiles.Count)
                {
                    failures.Add($"{caseName}: row count {actual.Rows.Count}, manifest lists {expectedFiles.Count}");
                    continue;
                }

                for (int i = 0; i < expectedFiles.Count; i++)
                {
                    string? expectedFile = expectedFiles[i];
                    var got = actual.Rows[i];

                    if (expectedFile is null)
                    {
                        if (got is not null)
                        {
                            failures.Add($"{caseName} row {i}: expected a null variant, got a value");
                        }
                        continue;
                    }

                    if (got is null)
                    {
                        failures.Add($"{caseName} row {i}: got a null variant, expected a value");
                        continue;
                    }

                    byte[] expected = File.ReadAllBytes(Path.Combine(root, expectedFile));
                    var (expMeta, expValue) = SplitVariantBinary(expected);

                    // Semantic comparison — always.
                    string expectedJson = VariantJsonWriter.ToJson(expMeta, expValue, indented: false);
                    string actualJson = VariantJsonWriter.ToJson(got.Value.Metadata, got.Value.Value, indented: false);
                    if (expectedJson != actualJson)
                    {
                        failures.Add($"{caseName} row {i}: expected {Truncate(expectedJson)}, got {Truncate(actualJson)}");
                        continue;
                    }
                    rowsCompared++;

                    // Byte comparison — only where the reader passes the bytes through untouched.
                    if (!actual.Shredded)
                    {
                        byte[] gotBytes = Concat(got.Value.Metadata, got.Value.Value);
                        if (!expected.AsSpan().SequenceEqual(gotBytes))
                        {
                            failures.Add($"{caseName} row {i}: unshredded byte mismatch — expected "
                                + $"{Describe(expected)}, got {Describe(gotBytes)}");
                            continue;
                        }
                        bytesCompared++;
                    }
                }
            }
            catch (Exception ex)
            {
                failures.Add($"{caseName}: {ex.GetType().Name}: {ex.Message}");
            }
        }

        Assert.True(casesRun > 0, "cases.json yielded no readable cases.");
        Assert.True(errorCases > 0, "cases.json yielded no error cases.");
        Assert.True(implementationDefined > 0, "cases.json yielded no implementation-defined cases.");
        Assert.True(
            failures.Count == 0,
            $"{failures.Count} of {casesRun} corpus cases failed ({rowsCompared} rows matched "
            + $"semantically, {bytesCompared} byte-exact, {errorCases} error cases, "
            + $"{implementationDefined} implementation-defined):{Environment.NewLine}"
            + string.Join(Environment.NewLine, failures.Take(40)));
    }

    /// <summary>Reads the corpus file's <c>var</c> column across every row group, returning each row's
    /// metadata/value bytes (null for a null variant) plus whether the FILE was shredded (determined
    /// from the parquet schema, since reassembly clears <c>IsShredded</c> on the materialised array).</summary>
    private static async Task<(List<(byte[] Metadata, byte[] Value)?> Rows, bool Shredded)>
        ReadVariantColumnAsync(string path, ExtensionTypeRegistry registry)
    {
        var rows = new List<(byte[], byte[])?>();

        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false,
            new ParquetReadOptions { ExtensionRegistry = registry });

        var meta = await reader.ReadMetadataAsync();
        bool shredded = meta.Schema.Any(s => s.Name == "typed_value");

        for (int rg = 0; rg < meta.RowGroups.Count; rg++)
        {
            var batch = await reader.ReadRowGroupAsync(rg);
            int idx = batch.Schema.GetFieldIndex("var");
            if (idx < 0)
            {
                throw new InvalidOperationException(
                    "no 'var' column; columns: " + string.Join(", ", batch.Schema.FieldsList.Select(f => f.Name)));
            }

            if (batch.Column(idx) is not VariantArray va)
            {
                throw new InvalidOperationException(
                    $"'var' materialised as {batch.Column(idx).GetType().Name}, not VariantArray "
                    + $"(schema type {batch.Schema.FieldsList[idx].DataType.GetType().Name})");
            }

            for (int i = 0; i < va.Length; i++)
            {
                rows.Add(va.IsNull(i)
                    ? null
                    : (va.GetMetadataBytes(i).ToArray(), va.GetValueBytes(i).ToArray()));
            }
        }

        return (rows, shredded);
    }

    /// <summary>
    /// Splits a <c>*.variant.bin</c> reference file into its metadata and value halves. The file is
    /// the metadata bytes immediately followed by the value bytes (corpus README); the metadata is
    /// self-delimiting, so its length is derived from the header: bits 0-3 version, bit 4
    /// sorted_strings, bits 5-6 offset_size_minus_one, then <c>dictionary_size</c> and
    /// <c>dictionary_size + 1</c> offsets, each <c>offset_size</c> bytes, then the string bytes
    /// (whose total length is the final offset).
    /// </summary>
    private static (byte[] Metadata, byte[] Value) SplitVariantBinary(byte[] bytes)
    {
        int offsetSize = ((bytes[0] >> 6) & 0x03) + 1;
        int dictSize = ReadLittleEndian(bytes, 1, offsetSize);
        int offsetsStart = 1 + offsetSize;
        int stringsStart = offsetsStart + ((dictSize + 1) * offsetSize);
        int stringBytes = ReadLittleEndian(bytes, offsetsStart + (dictSize * offsetSize), offsetSize);
        int metadataLength = stringsStart + stringBytes;

        return (bytes.AsSpan(0, metadataLength).ToArray(), bytes.AsSpan(metadataLength).ToArray());
    }

    private static int ReadLittleEndian(byte[] bytes, int start, int count)
    {
        int value = 0;
        for (int i = count - 1; i >= 0; i--)
        {
            value = (value << 8) | bytes[start + i];
        }
        return value;
    }

    private static byte[] Concat(byte[] a, byte[] b)
    {
        var result = new byte[a.Length + b.Length];
        a.CopyTo(result, 0);
        b.CopyTo(result, a.Length);
        return result;
    }

    private static string Truncate(string s) => s.Length <= 100 ? s : s[..100] + "…";

    /// <summary>Parquet file names the corpus manifest declares as malformed.</summary>
    private static async Task<HashSet<string>> ReadManifestErrorCasesAsync(string root)
    {
        var result = new HashSet<string>(StringComparer.Ordinal);
        string manifest = Path.Combine(root, "cases.json");
        if (!File.Exists(manifest)) return result;

        using var doc = System.Text.Json.JsonDocument.Parse(File.ReadAllBytes(manifest));
        foreach (var c in doc.RootElement.EnumerateArray())
        {
            if (c.TryGetProperty("error_message", out _)
                && c.TryGetProperty("parquet_file", out var pf)
                && pf.GetString() is { } name)
            {
                result.Add(name);
            }
        }
        return result;
    }

    private static string Describe(ReadOnlySpan<byte> bytes)
    {
        int n = Math.Min(bytes.Length, 12);
        var hex = string.Join(" ", bytes[..n].ToArray().Select(b => b.ToString("x2")));
        return bytes.Length <= n ? $"{bytes.Length}B [{hex}]" : $"{bytes.Length}B [{hex} …]";
    }
}
