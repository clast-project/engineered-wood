// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// A NULL in a column the TABLE declares non-nullable is refused on every write path, whatever the batch's own
/// field says (#514). The Parquet writer refused one only when the batch also declared the field non-nullable,
/// so a batch declaring it nullable committed nulls the table's schema says cannot exist.
/// </summary>
public class NotNullDataColumnTests : IDisposable
{
    private readonly string _tempDir;

    public NotNullDataColumnTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_notnull_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    /// <summary><c>id BIGINT NOT NULL, v STRING NOT NULL</c>.</summary>
    private static Apache.Arrow.Schema TableSchema => new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("v", StringType.Default, false))
        .Build();

    /// <summary>Rows <c>(i, values[i])</c>, with <c>v</c> declared nullable in the BATCH.</summary>
    private static RecordBatch Rows(params string?[] values)
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("v", StringType.Default, true))
            .Build();
        var ids = new Int64Array.Builder();
        var vs = new StringArray.Builder();
        for (int i = 0; i < values.Length; i++)
        {
            ids.Append(i);
            if (values[i] is null) vs.AppendNull(); else vs.Append(values[i]);
        }
        return new RecordBatch(schema, [ids.Build(), vs.Build()], values.Length);
    }

    private static RecordBatch WithV(RecordBatch batch, IArrowArray v, IArrowType? type = null) => new(
        new Apache.Arrow.Schema.Builder()
            .Field(batch.Schema.GetFieldByName("id"))
            .Field(new Field("v", type ?? StringType.Default, true))
            .Build(),
        [batch.Column(0), v], batch.Length);

    private ValueTask<DeltaTable> NewTableAsync(Apache.Arrow.Schema schema) => DeltaTable.CreateAsync(
        new LocalTableFileSystem(Path.Combine(_tempDir, Guid.NewGuid().ToString("N"))), schema);

    private static async Task<List<string?>> ReadV(DeltaTable table)
    {
        var result = new List<string?>();
        await foreach (var b in table.ReadAllAsync())
        {
            var v = (StringArray)b.Column(b.Schema.GetFieldIndex("v"));
            for (int i = 0; i < v.Length; i++) result.Add(v.GetString(i));
        }
        result.Sort(StringComparer.Ordinal);
        return result;
    }

    private static void AssertNotNullViolation(DeltaFormatException ex, string column)
    {
        Assert.Equal(DeltaTableErrorCodes.NotNullConstraintViolated, ex.ErrorCode);
        Assert.Contains($"for column: {column}.", ex.Message);
    }

    [Fact]
    public async Task Write_NullInNotNullColumn_IsRefused()
    {
        await using var table = await NewTableAsync(TableSchema);

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(async () =>
            await table.WriteAsync([Rows("a", null)]));

        AssertNotNullViolation(ex, "v");
        Assert.Equal(0, table.CurrentSnapshot.Version);
    }

    /// <summary>The batch's nullable flag alone is not a violation: only a null value is.</summary>
    [Fact]
    public async Task Write_NullableBatchFieldWithoutNulls_IsAccepted()
    {
        await using var table = await NewTableAsync(TableSchema);

        await table.WriteAsync([Rows("a", "b")]);

        Assert.Equal(["a", "b"], await ReadV(table));
    }

    /// <summary>An omitted column reads as null, so omitting a non-nullable one is the same violation.</summary>
    [Fact]
    public async Task Write_OmittingNotNullColumn_IsRefused()
    {
        await using var table = await NewTableAsync(TableSchema);
        var idOnly = new RecordBatch(
            new Apache.Arrow.Schema.Builder().Field(new Field("id", Int64Type.Default, false)).Build(),
            [new Int64Array.Builder().Append(1).Build()], 1);

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(async () => await table.WriteAsync([idOnly]));

        AssertNotNullViolation(ex, "v");
    }

    [Fact]
    public async Task TransactionWrite_NullInNotNullColumn_IsRefused()
    {
        await using var table = await NewTableAsync(TableSchema);
        var txn = table.StartTransaction();

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(async () => await txn.WriteAsync([Rows("a", null)]));

        AssertNotNullViolation(ex, "v");
    }

    [Fact]
    public async Task WriteDataFiles_NullInNotNullColumn_IsRefused()
    {
        await using var table = await NewTableAsync(TableSchema);

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(async () =>
            await table.WriteDataFilesAsync([Rows("a", null)]));

        AssertNotNullViolation(ex, "v");
        Assert.Empty(Directory.GetFiles(_tempDir, "*.parquet", SearchOption.AllDirectories));
    }

    /// <summary>
    /// A pending schema cannot relax a NOT NULL (nothing here can), so an override declaring <c>v</c> nullable is
    /// checked under the committed schema's nullability rather than becoming a way around it.
    /// </summary>
    [Fact]
    public async Task WriteDataFiles_OverrideThatRelaxesNotNull_IsStillRefused()
    {
        await using var table = await NewTableAsync(TableSchema);
        var relaxed = DeltaLake.Schema.SchemaConverter.FromArrowSchema(Rows("a").Schema);

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(async () =>
            await table.WriteDataFilesAsync([Rows("a", null)], schemaOverride: relaxed));

        AssertNotNullViolation(ex, "v");
        await table.WriteDataFilesAsync([Rows("a", "b")], schemaOverride: relaxed);
    }

    [Fact]
    public async Task Update_SettingNull_IsRefused()
    {
        await using var table = await NewTableAsync(TableSchema);
        await table.WriteAsync([Rows("a", "b")]);

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(async () => await table.UpdateAsync(
            b => new BooleanArray.Builder().AppendRange(Enumerable.Repeat(true, b.Length)).Build(),
            b => WithV(b, new StringArray.Builder().AppendNull().AppendNull().Build())));

        AssertNotNullViolation(ex, "v");
        Assert.Equal(1, table.CurrentSnapshot.Version);
        Assert.Equal(["a", "b"], await ReadV(table));
    }

    [Fact]
    public async Task UpdateRows_SettingNull_IsRefused()
    {
        await using var table = await NewTableAsync(TableSchema);
        await table.WriteAsync([Rows("a", "b")]);

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(async () => await table.UpdateRowsAsync(
            RowSelection.FromRowAddresses([TransientRowAddress.Pack(0, 0)], table.CurrentSnapshot),
            (_, batches, _) => batches
                .Select(b => WithV(b, new StringArray.Builder().AppendNull().Append("b").Build()))
                .ToList()));

        AssertNotNullViolation(ex, "v");
        Assert.Equal(1, table.CurrentSnapshot.Version);
    }

    /// <summary>
    /// Only the post-image is checked, as for CHECK constraints: a file already holding a null (as an earlier
    /// writer without this check could leave one; here written by a twin table whose <c>v</c> is nullable, and
    /// committed through the seam that takes finished files) does not block an update of a different row.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Update_OfAnotherRow_DoesNotRecheckAnExistingNull(bool byRows)
    {
        string root = Path.Combine(_tempDir, "strict");
        string twinRoot = Path.Combine(_tempDir, "twin");
        await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(root), TableSchema);
        await using (var twin = await DeltaTable.CreateAsync(new LocalTableFileSystem(twinRoot), Rows("a").Schema))
        {
            var files = await twin.WriteDataFilesAsync([Rows("a", null)]);
            foreach (var f in files)
                File.Copy(Path.Combine(twinRoot, f.RelativePath), Path.Combine(root, f.RelativePath));
            await table.CommitDataFilesAsync(files);
        }

        if (byRows)
        {
            await table.UpdateRowsAsync(
                RowSelection.FromRowAddresses([TransientRowAddress.Pack(0, 0)], table.CurrentSnapshot),
                (_, batches, _) => batches
                    .Select(b => WithV(b, new StringArray.Builder().Append("z").AppendNull().Build()))
                    .ToList());
        }
        else
        {
            await table.UpdateAsync(
                b => new BooleanArray.Builder().Append(true).Append(false).Build(),
                b => WithV(b, new StringArray.Builder().Append("z").Build()));
        }

        Assert.Equal(2, table.CurrentSnapshot.Version);
        Assert.Equal([null, "z"], await ReadV(table));
    }

    // The built-in writer refuses dictionary and run-end encoded columns before this check (they have no Delta
    // type), but a host's value-blind IDataFileWriter may be handed them, so the enforcer reads them itself.
    private static DeltaLake.Schema.StructType DeltaSchema =>
        DeltaLake.Schema.SchemaConverter.FromArrowSchema(TableSchema);

    /// <summary>A dictionary column's null can be a valid index pointing at a null dictionary value.</summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Enforcer_DictionaryIndexOfNullValue_IsRefused(bool pointsAtNull)
    {
        var dictionary = new StringArray.Builder().Append("a").AppendNull().Build();
        var indices = new Int32Array.Builder().Append(0).Append(pointsAtNull ? 1 : 0).Build();
        var type = new DictionaryType(Int32Type.Default, StringType.Default, false);
        var batch = WithV(Rows("x", "y"), new DictionaryArray(type, indices, dictionary), type);

        if (pointsAtNull)
            AssertNotNullViolation(
                Assert.Throws<DeltaFormatException>(() => DeltaNotNullEnforcer.Validate(batch, DeltaSchema)), "v");
        else
            DeltaNotNullEnforcer.Validate(batch, DeltaSchema);
    }

    /// <summary>A run-end encoded column has no validity bitmap: its nulls are null runs in its values.</summary>
    [Fact]
    public void Enforcer_RunEndEncodedNullRun_IsRefused()
    {
        var runEnds = new Int32Array.Builder().Append(1).Append(2).Build();
        var values = new StringArray.Builder().Append("a").AppendNull().Build();
        var v = new RunEndEncodedArray(runEnds, values);
        var batch = WithV(Rows("x", "y"), v, v.Data.DataType);

        AssertNotNullViolation(
            Assert.Throws<DeltaFormatException>(() => DeltaNotNullEnforcer.Validate(batch, DeltaSchema)), "v");
    }

    /// <summary>
    /// A dictionary of structs: each row's value is the entry its index names, so a present entry's NULL
    /// <c>NOT NULL</c> child is a violation, and one in an entry no row names is not.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Enforcer_DictionaryOfStructs_ChecksTheNamedEntries(bool namesTheBadEntry)
    {
        var schema = new DeltaLake.Schema.StructType
        {
            Fields =
            [
                new DeltaLake.Schema.StructField
                {
                    Name = "s", Nullable = true,
                    Type = new DeltaLake.Schema.StructType
                    {
                        Fields =
                        [
                            new DeltaLake.Schema.StructField
                            {
                                Name = "c", Type = new DeltaLake.Schema.PrimitiveType { TypeName = "integer" },
                                Nullable = false,
                            },
                        ],
                    },
                },
            ],
        };
        var structType = new StructType([new Field("c", Int32Type.Default, true)]);
        var entries = new StructArray(
            structType, 2, [new Int32Array.Builder().Append(1).AppendNull().Build()],
            ArrowBuffer.Empty, nullCount: 0);
        var indices = new Int32Array.Builder().Append(0).Append(namesTheBadEntry ? 1 : 0).Build();
        var type = new DictionaryType(Int32Type.Default, structType, false);
        var batch = new RecordBatch(
            new Apache.Arrow.Schema.Builder().Field(new Field("s", type, true)).Build(),
            [new DictionaryArray(type, indices, entries)], 2);

        if (namesTheBadEntry)
            AssertNotNullViolation(
                Assert.Throws<DeltaFormatException>(() => DeltaNotNullEnforcer.Validate(batch, schema)), "s.c");
        else
            DeltaNotNullEnforcer.Validate(batch, schema);
    }

    /// <summary>
    /// After a rename, one column's physical name can be another's logical name: <c>x NOT NULL</c> stored as
    /// <c>a</c>, beside a nullable column now NAMED <c>a</c>. The write binds a batch field by logical name
    /// first, so a batch carrying only <c>a</c> omits <c>x</c> and must be refused, not credited with
    /// <c>a</c>'s values.
    /// </summary>
    [Fact]
    public void Enforcer_ReusedName_BindsLogicalFirst()
    {
        static DeltaLake.Schema.StructField Column(string name, string physical, bool nullable) => new()
        {
            Name = name, Type = new DeltaLake.Schema.PrimitiveType { TypeName = "string" }, Nullable = nullable,
            Metadata = new Dictionary<string, string> { [DeltaLake.Schema.ColumnMapping.PhysicalNameKey] = physical },
        };
        var schema = new DeltaLake.Schema.StructType
        {
            Fields = [Column("x", "a", nullable: false), Column("a", "col-2", nullable: true)],
        };
        var batch = new RecordBatch(
            new Apache.Arrow.Schema.Builder().Field(new Field("a", StringType.Default, true)).Build(),
            [new StringArray.Builder().Append("v").Build()], 1);

        AssertNotNullViolation(
            Assert.Throws<DeltaFormatException>(() => DeltaNotNullEnforcer.Validate(batch, schema)), "x");

        // And a physical-named batch still binds by physical name where no logical name claims it.
        var physical = new RecordBatch(
            new Apache.Arrow.Schema.Builder().Field(new Field("col-2", StringType.Default, true)).Build(),
            [new StringArray.Builder().AppendNull().Build()], 1);
        AssertNotNullViolation(
            Assert.Throws<DeltaFormatException>(() => DeltaNotNullEnforcer.Validate(physical, schema)), "x");
    }

    /// <summary>
    /// <c>m MAP&lt;STRING, INT NOT NULL&gt;</c>: a null value is refused, and so is a null key (a Delta map key is
    /// never null), but an entry under a null map is no row's value. Row 0 holds entry 0 and row 1 entry 1.
    /// </summary>
    [Theory]
    [InlineData("none", null)]
    [InlineData("value", "m.value")]
    [InlineData("key", "m.key")]
    [InlineData("valueUnderNullMap", null)]
    public void Enforcer_Map_ChecksKeysAndValues(string nullAt, string? violation)
    {
        var schema = new DeltaLake.Schema.StructType
        {
            Fields =
            [
                new DeltaLake.Schema.StructField
                {
                    Name = "m", Nullable = true,
                    Type = new DeltaLake.Schema.MapType
                    {
                        KeyType = new DeltaLake.Schema.PrimitiveType { TypeName = "string" },
                        ValueType = new DeltaLake.Schema.PrimitiveType { TypeName = "integer" },
                        ValueContainsNull = false,
                    },
                },
            ],
        };
        var mapType = new MapType(
            new Field("key", StringType.Default, true), new Field("value", Int32Type.Default, true));
        var keys = new StringArray.Builder().Append("k0");
        if (nullAt == "key") keys.AppendNull(); else keys.Append("k1");
        var values = new Int32Array.Builder().Append(0);
        if (nullAt is "value" or "valueUnderNullMap") values.AppendNull(); else values.Append(1);
        var entries = new StructArray(
            new StructType([mapType.KeyField, mapType.ValueField]), 2,
            [keys.Build(), values.Build()], ArrowBuffer.Empty);
        var offsets = new ArrowBuffer.Builder<int>().Append(0).Append(1).Append(2).Build();
        bool rowOneNull = nullAt == "valueUnderNullMap";
        var validity = new ArrowBuffer.BitmapBuilder().Append(true).Append(!rowOneNull).Build();
        var map = new MapArray(mapType, 2, offsets, entries, validity, rowOneNull ? 1 : 0);
        var batch = new RecordBatch(
            new Apache.Arrow.Schema.Builder().Field(new Field("m", mapType, true)).Build(), [map], 2);

        if (violation is null)
            DeltaNotNullEnforcer.Validate(batch, schema);
        else
            AssertNotNullViolation(
                Assert.Throws<DeltaFormatException>(() => DeltaNotNullEnforcer.Validate(batch, schema)), violation);
    }

    /// <summary>
    /// Only the given rows are checked, read in place (a host writer's dictionary column is not gathered, which
    /// the gather cannot do).
    /// </summary>
    [Fact]
    public void Enforcer_Rows_ChecksOnlyThoseRows()
    {
        var dictionary = new StringArray.Builder().Append("a").Build();
        var indices = new Int32Array.Builder().Append(0).AppendNull().Build();
        var type = new DictionaryType(Int32Type.Default, StringType.Default, false);
        var batch = WithV(Rows("x", "y"), new DictionaryArray(type, indices, dictionary), type);

        DeltaNotNullEnforcer.Validate(batch, DeltaSchema, rows: [0]);
        AssertNotNullViolation(
            Assert.Throws<DeltaFormatException>(() => DeltaNotNullEnforcer.Validate(batch, DeltaSchema, rows: [1])),
            "v");
    }

    /// <summary>
    /// <c>s STRUCT&lt;c INT NOT NULL&gt;</c>, nullable: <c>c</c> applies only where <c>s</c> holds a value, so a
    /// null <c>c</c> under a null <c>s</c> is fine and one under a present <c>s</c> is not.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Write_NotNullStructField_AppliesOnlyUnderPresentParent(bool parentPresent)
    {
        var tableSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("s", new StructType([new Field("c", Int32Type.Default, false)]), true))
            .Build();
        await using var table = await NewTableAsync(tableSchema);

        var structType = new StructType([new Field("c", Int32Type.Default, true)]);
        var c = new Int32Array.Builder().Append(1).AppendNull().Build();
        var validity = new ArrowBuffer.BitmapBuilder().Append(true).Append(parentPresent).Build();
        var s = new StructArray(structType, 2, [c], validity, nullCount: parentPresent ? 0 : 1);
        var batch = new RecordBatch(
            new Apache.Arrow.Schema.Builder()
                .Field(new Field("id", Int64Type.Default, false))
                .Field(new Field("s", structType, true))
                .Build(),
            [new Int64Array.Builder().Append(0).Append(1).Build(), s], 2);

        if (parentPresent)
        {
            var ex = await Assert.ThrowsAsync<DeltaFormatException>(async () => await table.WriteAsync([batch]));
            AssertNotNullViolation(ex, "s.c");
        }
        else
        {
            await table.WriteAsync([batch]);
            Assert.Equal(1, table.CurrentSnapshot.Version);
        }
    }

    /// <summary><c>a ARRAY&lt;INT NOT NULL&gt;</c>: a null element is refused, a null array is not.</summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Write_ArrayWithoutNullElements(bool nullElement)
    {
        var tableSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("a", new ListType(new Field("element", Int32Type.Default, false)), true))
            .Build();
        await using var table = await NewTableAsync(tableSchema);

        var listType = new ListType(new Field("element", Int32Type.Default, true));
        var builder = new ListArray.Builder(new Field("element", Int32Type.Default, true));
        var elements = (Int32Array.Builder)builder.ValueBuilder;
        builder.Append();
        elements.Append(1);
        if (nullElement) elements.AppendNull(); else elements.Append(2);
        builder.AppendNull();
        var a = builder.Build();
        var batch = new RecordBatch(
            new Apache.Arrow.Schema.Builder()
                .Field(new Field("id", Int64Type.Default, false))
                .Field(new Field("a", listType, true))
                .Build(),
            [new Int64Array.Builder().Append(0).Append(1).Build(), a], 2);

        if (nullElement)
        {
            var ex = await Assert.ThrowsAsync<DeltaFormatException>(async () => await table.WriteAsync([batch]));
            AssertNotNullViolation(ex, "a.element");
        }
        else
        {
            await table.WriteAsync([batch]);
            Assert.Equal(1, table.CurrentSnapshot.Version);
        }
    }

    /// <summary>Under column mapping the batch may carry a physical name, and the check still finds it.</summary>
    [Fact]
    public async Task Write_ColumnMapping_NullIsRefused()
    {
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(Path.Combine(_tempDir, "mapped")), TableSchema,
            columnMappingMode: DeltaLake.Schema.ColumnMappingMode.Name);

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(async () =>
            await table.WriteAsync([Rows("a", null)]));

        AssertNotNullViolation(ex, "v");
        await table.WriteAsync([Rows("a", "b")]);
        Assert.Equal(["a", "b"], await ReadV(table));
    }
}
