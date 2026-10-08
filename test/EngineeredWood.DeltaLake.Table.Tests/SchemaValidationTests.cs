// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.IO.Local;
using ArrowStructType = Apache.Arrow.Types.StructType;
using DeltaArrayType = EngineeredWood.DeltaLake.Schema.ArrayType;
using DeltaStructType = EngineeredWood.DeltaLake.Schema.StructType;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// Schemas EW used to accept or produce although they break the spec or Spark refuses them (#456):
/// case-insensitive duplicate names, column-mapping ids reused from inside arrays and maps, and Arrow
/// types with no Delta equivalent.
/// </summary>
public class SchemaValidationTests : IDisposable
{
    private readonly string _tempDir;

    public SchemaValidationTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_schemaval_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private LocalTableFileSystem Fs => new(_tempDir);

    private static Apache.Arrow.Schema Schema(params Field[] fields)
    {
        var b = new Apache.Arrow.Schema.Builder();
        foreach (var f in fields)
            b.Field(f);
        return b.Build();
    }

    private static Field Long(string name) => new(name, Int64Type.Default, true);

    private static Field Struct(string name, params Field[] children) =>
        new(name, new ArrowStructType(children), true);

    private static async Task<DeltaFormatException> RefusedAsync(string? errorCode, Func<Task> action)
    {
        var ex = await Assert.ThrowsAsync<DeltaFormatException>(action);
        if (errorCode is not null)
            Assert.Equal(errorCode, ex.ErrorCode);
        return ex;
    }

    private static Task<DeltaFormatException> DuplicateAsync(Func<Task> action) =>
        RefusedAsync(DeltaTableErrorCodes.DuplicateColumnsFound, action);

    // ── Case-insensitive duplicate names ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task Create_TopLevelNamesDifferingOnlyInCase_AreRefused()
    {
        await DuplicateAsync(async () =>
        {
            await using var t = await DeltaTable.CreateAsync(Fs, Schema(Long("id"), Long("ID")));
        });
    }

    [Fact]
    public async Task Create_StructChildrenDifferingOnlyInCase_AreRefused()
    {
        var ex = await DuplicateAsync(async () =>
        {
            await using var t = await DeltaTable.CreateAsync(Fs, Schema(Struct("s", Long("x"), Long("X"))));
        });
        Assert.Contains("'s.x'", ex.Message);
    }

    [Fact]
    public async Task Create_DuplicatesInsideAnArrayOrMapStruct_AreRefused()
    {
        var elem = new ArrowStructType([Long("x"), Long("X")]);
        await DuplicateAsync(async () =>
        {
            await using var t = await DeltaTable.CreateAsync(
                Fs, Schema(new Field("arr", new ListType(new Field("element", elem, true)), true)));
        });

        var map = new Apache.Arrow.Types.MapType(new Field("key", StringType.Default, false), new Field("value", elem, true));
        await DuplicateAsync(async () =>
        {
            await using var t = await DeltaTable.CreateAsync(Fs, Schema(new Field("m", map, true)));
        });
    }

    [Fact]
    public async Task AddColumn_CaseInsensitiveDuplicate_IsRefused()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, Schema(Long("id")));

        await DuplicateAsync(async () => await table.AddColumnAsync(new Field("ID", StringType.Default, true)));
        await DuplicateAsync(() =>
        {
            table.ComputeAddColumn(new Field("Id", StringType.Default, true));
            return Task.CompletedTask;
        });
    }

    [Fact]
    public async Task AddField_CaseInsensitiveDuplicate_IsRefused()
    {
        await using var table = await DeltaTable.CreateAsync(
            Fs, Schema(Struct("s", Long("a"))), columnMappingMode: ColumnMappingMode.Name);

        await DuplicateAsync(async () => await table.AddFieldAsync(["s"], Long("A")));
    }

    [Fact]
    public async Task RenameColumnAndField_ToACaseInsensitiveDuplicate_AreRefused()
    {
        await using var table = await DeltaTable.CreateAsync(
            Fs, Schema(Long("id"), Long("name"), Struct("s", Long("a"), Long("b"))),
            columnMappingMode: ColumnMappingMode.Name);

        await DuplicateAsync(async () => await table.RenameColumnAsync("id", "Name"));
        await DuplicateAsync(async () => await table.RenameFieldAsync(["s", "a"], "B"));
    }

    [Fact]
    public async Task RenameColumn_ChangingOnlyItsOwnCase_IsAllowed()
    {
        await using var table = await DeltaTable.CreateAsync(
            Fs, Schema(Long("id"), Struct("s", Long("a"))), columnMappingMode: ColumnMappingMode.Name);

        await table.RenameColumnAsync("id", "ID");
        await table.RenameFieldAsync(["s", "a"], "A");

        Assert.Equal("ID", table.CurrentSnapshot.Schema.Fields[0].Name);
    }

    [Fact]
    public async Task SetSchema_WithACaseInsensitiveDuplicate_IsRefused()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, Schema(Long("id")));

        await DuplicateAsync(async () => await table.SetSchemaAsync(Schema(Long("id"), Long("Id"))));
    }

    // ── Column-mapping ids inside arrays and maps ─────────────────────────────────────────────────────

    private static StructField Mapped(string name, DeltaDataType type, int id, string physical) => new()
    {
        Name = name,
        Type = type,
        Nullable = true,
        Metadata = new Dictionary<string, string>
        {
            [ColumnMapping.FieldIdKey] = id.ToString(),
            [ColumnMapping.PhysicalNameKey] = physical,
        },
    };

    private static PrimitiveType DeltaLong => new() { TypeName = "long" };

    private static IEnumerable<int> AllIds(DeltaDataType type)
    {
        switch (type)
        {
            case DeltaStructType st:
                foreach (var f in st.Fields)
                {
                    if (ColumnMapping.GetFieldId(f) is { } id)
                        yield return id;
                    foreach (var c in AllIds(f.Type))
                        yield return c;
                }
                break;
            case DeltaArrayType at:
                foreach (var c in AllIds(at.ElementType))
                    yield return c;
                break;
            case EngineeredWood.DeltaLake.Schema.MapType mt:
                foreach (var c in AllIds(mt.KeyType).Concat(AllIds(mt.ValueType)))
                    yield return c;
                break;
        }
    }

    /// <summary>arr (id 1): array&lt;struct&lt;x id 2, y id 3&gt;&gt; — the highest ids live inside the array.</summary>
    private Task<DeltaTable> CreateArrayOfStructAsync()
    {
        var elem = new ArrowStructType([Long("x"), Long("y")]);
        var arrow = Schema(new Field("arr", new ListType(new Field("element", elem, true)), true));
        var pre = new DeltaStructType
        {
            Fields =
            [
                Mapped("arr", new DeltaArrayType
                {
                    ElementType = new DeltaStructType
                    {
                        Fields = [Mapped("x", DeltaLong, 2, "col-x"), Mapped("y", DeltaLong, 3, "col-y")],
                    },
                    ContainsNull = true,
                }, 1, "col-arr"),
            ],
        };
        return DeltaTable.CreateAsync(Fs, arrow, columnMappingMode: ColumnMappingMode.Id, preAssignedSchema: pre)
            .AsTask();
    }

    [Fact]
    public async Task PreAssigned_ArrayOfStructIds_MaxColumnIdCoversTheNestedIds()
    {
        await using var table = await CreateArrayOfStructAsync();

        Assert.Equal("3", table.CurrentSnapshot.Metadata.Configuration![ColumnMapping.MaxColumnIdKey]);
    }

    [Fact]
    public async Task PreAssigned_ArrayOfStructIds_AddColumnDoesNotReuseANestedId()
    {
        await using var table = await CreateArrayOfStructAsync();
        await table.AddColumnAsync(Long("z"));

        var ids = AllIds(table.CurrentSnapshot.Schema).ToList();
        Assert.Equal(ids.Count, ids.Distinct().Count());
    }

    [Fact]
    public void GetMaxColumnId_LooksInsideMapKeysAndValues()
    {
        var schema = new DeltaStructType
        {
            Fields =
            [
                Mapped("m", new EngineeredWood.DeltaLake.Schema.MapType
                {
                    KeyType = new DeltaStructType { Fields = [Mapped("k", DeltaLong, 7, "col-k")] },
                    ValueType = new DeltaStructType { Fields = [Mapped("v", DeltaLong, 9, "col-v")] },
                    ValueContainsNull = true,
                }, 1, "col-m"),
            ],
        };

        Assert.Equal(9, ColumnMapping.GetMaxColumnId(schema));
    }

    [Theory]
    [InlineData(false)] // array<struct>: refused before too
    [InlineData(true)]  // array<array<struct>>: was never checked
    public async Task Replace_PreAssigned_ReusingAnIdAtAnyDepth_IsRefused(bool doubleList)
    {
        await using (var t = await DeltaTable.CreateAsync(Fs, Schema(Long("v")), columnMappingMode: ColumnMappingMode.Name))
        {
            await t.AddColumnAsync(Long("w"));
            await t.AddColumnAsync(Long("u"));
        }
        // maxColumnId is now 3, so id 2 below is a reuse.

        var elem = new ArrowStructType([Long("x")]);
        var inner = new ListType(new Field("element", elem, true));
        var arrow = Schema(new Field("ll", doubleList ? new ListType(new Field("element", inner, true)) : inner, true));
        var innerDelta = new DeltaArrayType
        {
            ElementType = new DeltaStructType { Fields = [Mapped("x", DeltaLong, 2, "col-x")] },
            ContainsNull = true,
        };
        var pre = new DeltaStructType
        {
            Fields =
            [
                Mapped("ll", doubleList
                    ? new DeltaArrayType { ElementType = innerDelta, ContainsNull = true }
                    : innerDelta, 10, "col-ll"),
            ],
        };

        await RefusedAsync(DeltaTableErrorCodes.InvalidPreAssignedSchema, async () =>
        {
            await using var r = await DeltaTable.CreateOrReplaceAsync(
                Fs, arrow, [], columnMappingMode: ColumnMappingMode.Name, preAssignedSchema: pre);
        });
    }

    // ── Decimal precision ─────────────────────────────────────────────────────────────────────────────────

    [Theory]
    [InlineData(39)]
    [InlineData(40)]
    public async Task Create_DecimalPrecisionAbove38_IsRefused(int precision)
    {
        var ex = await RefusedAsync(null, async () =>
        {
            await using var t = await DeltaTable.CreateAsync(
                Fs, Schema(new Field("d", new Decimal256Type(precision, 2), true)));
        });
        Assert.Contains("38", ex.Message);
    }

    [Fact]
    public async Task Create_Decimal38_IsAccepted()
    {
        await using var t = await DeltaTable.CreateAsync(Fs, Schema(new Field("d", new Decimal256Type(38, 2), true)));
        Assert.Equal("decimal(38,2)", ((PrimitiveType)t.CurrentSnapshot.Schema.Fields[0].Type).TypeName);
    }

    [Fact]
    public void ReadSchema_DecimalPrecisionAbove38_IsRefused()
    {
        // A spec-invalid schema written by someone else: refuse clearly rather than build a Decimal128
        // that cannot hold the values.
        Assert.Throws<DeltaFormatException>(
            () => SchemaConverter.ToArrowType(new PrimitiveType { TypeName = "decimal(40,2)" }));
    }

    // A Delta-typed schema never passes through the Arrow-to-Delta conversion that refuses these types on
    // the way in. Before the pre-commit check, the refusal came from building the NEXT snapshot — after
    // the commit had landed, leaving a table that could no longer be opened.

    private static StructField DeltaDecimal(string name, int precision) => new()
    {
        Name = name,
        Type = new PrimitiveType { TypeName = $"decimal({precision},2)" },
        Nullable = true,
    };

    [Fact]
    public async Task Create_PreAssignedDecimalAbove38_IsRefusedBeforeAnythingIsCommitted()
    {
        var arrow = Schema(new Field("d", new Decimal256Type(38, 2), true));
        var pre = new DeltaStructType { Fields = [DeltaDecimal("d", 40)] };

        await RefusedAsync(null, async () =>
        {
            await using var t = await DeltaTable.CreateAsync(Fs, arrow, preAssignedSchema: pre);
        });

        var log = Path.Combine(_tempDir, "_delta_log");
        Assert.False(Directory.Exists(log) && Directory.EnumerateFiles(log, "*.json").Any());
    }

    [Fact]
    public async Task AddColumn_DeltaTypedDecimalAbove38_IsRefusedAndTheTableStillOpens()
    {
        long version;
        await using (var table = await DeltaTable.CreateAsync(Fs, Schema(Long("id"))))
        {
            version = table.CurrentSnapshot.Version;

            await RefusedAsync(null, async () => await table.AddColumnAsync(DeltaDecimal("d", 40)));
            await RefusedAsync(null, () =>
            {
                table.ComputeAddColumn(DeltaDecimal("d", 39));
                return Task.CompletedTask;
            });
        }

        await using var reopened = await DeltaTable.OpenAsync(Fs);
        Assert.Equal(version, reopened.CurrentSnapshot.Version);
    }

    [Fact]
    public async Task AddColumn_DeltaTypedUnknownPrimitive_IsRefusedAndTheTableStillOpens()
    {
        await using (var table = await DeltaTable.CreateAsync(Fs, Schema(Long("id"))))
        {
            await RefusedAsync(null, async () => await table.AddColumnAsync(new StructField
            {
                Name = "u", Type = new PrimitiveType { TypeName = "no_such_type" }, Nullable = true,
            }));
        }

        await using var reopened = await DeltaTable.OpenAsync(Fs);
        Assert.Single(reopened.CurrentSnapshot.Schema.Fields);
    }

    // ── Arrow types that convert to the Delta type's canonical form on write ────────────────────────
    //
    // Several Arrow types map onto one Delta type. EW accepts them, and a write converts each to the form
    // the table reads back as (Binary, Date32, Decimal128), so the files of a column agree.

    private static async Task<List<RecordBatch>> ReadAllAsync(DeltaTable table)
    {
        var batches = new List<RecordBatch>();
        await foreach (var b in table.ReadAllAsync())
            batches.Add(b);
        return batches;
    }

    private static FixedSizeBinaryArray FixedSizeBinary(int width, int length, byte[] bytes, byte[]? validity = null)
    {
        int nulls = validity is null
            ? 0
            : Enumerable.Range(0, length).Count(i => (validity[i / 8] & (1 << (i % 8))) == 0);
        return new(new ArrayData(new FixedSizeBinaryType(width), length, nulls, 0,
            [validity is null ? ArrowBuffer.Empty : new ArrowBuffer(validity), new ArrowBuffer(bytes)]));
    }

    [Fact]
    public async Task Create_FixedSizeBinary_IsDeclaredBinary()
    {
        await using var t = await DeltaTable.CreateAsync(Fs, Schema(new Field("b", new FixedSizeBinaryType(4), true)));

        Assert.Equal("binary", ((PrimitiveType)t.CurrentSnapshot.Schema.Fields[0].Type).TypeName);
        Assert.IsType<BinaryType>(t.ArrowSchema.FieldsList[0].DataType);
    }

    [Fact]
    public async Task Write_FixedSizeBinary_ReadsBackAsBinaryOnEveryEntryPoint()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, Schema(new Field("b", BinaryType.Default, true)));

        // [1,2,3,4], null, [9,10,11,12]; the later writes are SLICES, so the conversion must honour the offset.
        var array = FixedSizeBinary(4, 3, [1, 2, 3, 4, 0, 0, 0, 0, 9, 10, 11, 12], validity: [0b101]);
        var batch = new RecordBatch(Schema(new Field("b", new FixedSizeBinaryType(4), true)), [array], 3);
        await table.WriteAsync([batch]);
        await table.WriteAsync([batch.Slice(1, 2)]);
        await table.CommitDataFilesAsync(await table.WriteDataFilesAsync([batch.Slice(2, 1)]));

        var rows = new List<string>();
        foreach (var b in await ReadAllAsync(table))
        {
            var col = Assert.IsType<BinaryArray>(b.Column(0));
            for (int i = 0; i < col.Length; i++)
                rows.Add(col.IsNull(i) ? "null" : string.Join(",", col.GetBytes(i).ToArray()));
        }
        rows.Sort(StringComparer.Ordinal);
        Assert.Equal(["1,2,3,4", "9,10,11,12", "9,10,11,12", "9,10,11,12", "null", "null"], rows);
    }

    [Fact]
    public async Task Write_FixedSizeBinaryNestedInAStructAndList_ReadsBackAsBinary()
    {
        var structType = new ArrowStructType([new Field("b", new FixedSizeBinaryType(2), true)]);
        var listType = new ListType(new Field("element", new FixedSizeBinaryType(2), true));
        var schema = Schema(new Field("s", structType, true), new Field("l", listType, true));
        await using var table = await DeltaTable.CreateAsync(Fs, schema);

        var list = new ListArray(listType, 1,
            new ArrowBuffer(new byte[] { 0, 0, 0, 0, 2, 0, 0, 0 }), FixedSizeBinary(2, 2, [5, 6, 7, 8]), ArrowBuffer.Empty);
        await table.WriteAsync([new RecordBatch(schema,
            [new StructArray(structType, 1, [FixedSizeBinary(2, 1, [7, 8])], ArrowBuffer.Empty), list], 1)]);

        var batch = Assert.Single(await ReadAllAsync(table));
        var child = Assert.IsType<BinaryArray>(((StructArray)batch.Column(0)).Fields[0]);
        Assert.Equal(new byte[] { 7, 8 }, child.GetBytes(0).ToArray());
        var elements = Assert.IsType<BinaryArray>(((ListArray)batch.Column(1)).Values);
        Assert.Equal(new byte[] { 5, 6 }, elements.GetBytes(0).ToArray());
        Assert.Equal(new byte[] { 7, 8 }, elements.GetBytes(1).ToArray());
    }

    [Fact]
    public async Task Write_Date64_ReadsBackAsDate32()
    {
        var schema = Schema(new Field("d", Date64Type.Default, true));
        await using var table = await DeltaTable.CreateAsync(Fs, schema);
        Assert.Equal("date", ((PrimitiveType)table.CurrentSnapshot.Schema.Fields[0].Type).TypeName);

        var array = new Date64Array.Builder()
            .Append(new DateTime(2024, 3, 1)).AppendNull().Append(new DateTime(1969, 12, 31)).Build();
        await table.WriteAsync([new RecordBatch(schema, [array], 3)]);

        var col = Assert.IsType<Date32Array>(Assert.Single(await ReadAllAsync(table)).Column(0));
        Assert.Equal(new DateTime(2024, 3, 1), col.GetDateTime(0));
        Assert.True(col.IsNull(1));
        Assert.Equal(new DateTime(1969, 12, 31), col.GetDateTime(2));
    }

    [Fact]
    public async Task Write_Date64ThatIsNotAWholeDay_IsRefusedAndNothingIsWritten()
    {
        // The Arrow format requires Date64 values to be evenly divisible by 86400000. One that is not has
        // no single day it means, so it is refused rather than truncated.
        var schema = Schema(new Field("d", Date64Type.Default, true));
        await using var table = await DeltaTable.CreateAsync(Fs, schema);
        long version = table.CurrentSnapshot.Version;

        var data = new ArrayData(Date64Type.Default, 1, 0, 0,
            [ArrowBuffer.Empty, new ArrowBuffer(BitConverter.GetBytes(86_400_000L + 1))]);
        var ex = await RefusedAsync(DeltaTableErrorCodes.UnwritableValue, async () =>
            await table.WriteAsync([new RecordBatch(schema, [new Date64Array(data)], 1)]));
        Assert.Contains("86400000", ex.Message);

        Assert.Equal(version, table.CurrentSnapshot.Version);
        Assert.Empty(Directory.EnumerateFiles(_tempDir, "*.parquet", SearchOption.AllDirectories));
    }

    [Fact]
    public async Task Create_Decimal32And64_AreDeclaredDecimal()
    {
        await using var t = await DeltaTable.CreateAsync(Fs, Schema(
            new Field("a", new Decimal32Type(5, 2), true), new Field("b", new Decimal64Type(15, 3), true)));

        Assert.Equal("decimal(5,2)", ((PrimitiveType)t.CurrentSnapshot.Schema.Fields[0].Type).TypeName);
        Assert.Equal("decimal(15,3)", ((PrimitiveType)t.CurrentSnapshot.Schema.Fields[1].Type).TypeName);
    }

    private static decimal? DecimalAt(IArrowArray array, int i) => array switch
    {
        Decimal32Array a => a.GetValue(i),
        Decimal64Array a => a.GetValue(i),
        Decimal128Array a => a.GetValue(i),
        _ => throw new InvalidOperationException(array.GetType().Name),
    };

    private static ArrowBuffer Int32s(params int[] values) =>
        new(values.SelectMany(BitConverter.GetBytes).ToArray());

    [Fact]
    public void Normalize_WidensNarrowDecimalsToDecimal128_KeepingValuesAndSlices()
    {
        // So every file EW writes for a decimal column has one physical form (FIXED_LEN_BYTE_ARRAY(16)),
        // whichever Arrow width the caller handed in.
        var d64 = new Decimal64Type(15, 3);
        var array = new Decimal64Array(new ArrayData(d64, 3, 0, 0,
            [ArrowBuffer.Empty, new ArrowBuffer(new long[] { -1, 5_000, long.MaxValue / 1_000 }.SelectMany(BitConverter.GetBytes).ToArray())]));
        var batch = new RecordBatch(Schema(new Field("d", d64, true)), [array], 3).Slice(1, 2);

        var normalized = WriteTypeNormalization.Normalize(batch);

        var wide = Assert.IsType<Decimal128Array>(normalized.Column(0));
        Assert.Equal(new Decimal128Type(15, 3), (Decimal128Type)wide.Data.DataType, new DecimalTypeComparer());
        Assert.Equal(5m, wide.GetValue(0));
        Assert.Equal((long.MaxValue / 1_000) / 1000m, wide.GetValue(1));
    }

    private sealed class DecimalTypeComparer : IEqualityComparer<Decimal128Type>
    {
        public bool Equals(Decimal128Type? x, Decimal128Type? y) => x!.Precision == y!.Precision && x.Scale == y.Scale;
        public int GetHashCode(Decimal128Type obj) => obj.Precision;
    }

    [Fact]
    public async Task Write_NarrowAndWideDecimals_KeepTheirValues()
    {
        // The read TYPE is #469's business, so only the values are asserted here.
        var d32 = new Decimal32Type(9, 2);
        var d128 = new Decimal128Type(9, 2);
        await using var table = await DeltaTable.CreateAsync(Fs, Schema(new Field("d", d128, true)));

        var narrow = new Decimal32Array(new ArrayData(d32, 3, 0, 0, [ArrowBuffer.Empty, Int32s(-125, 0, 99_999_999)]));
        var wide = new Decimal128Array.Builder(d128).Append(12.34m).Build();
        await table.WriteAsync([
            new RecordBatch(Schema(new Field("d", d32, true)), [narrow], 3),
            new RecordBatch(Schema(new Field("d", d128, true)), [wide], 1),
        ]);

        var values = new List<decimal?>();
        foreach (var b in await ReadAllAsync(table))
        {
            for (int i = 0; i < b.Length; i++)
                values.Add(DecimalAt(b.Column(0), i));
        }
        values.Sort();
        Assert.Equal([-1.25m, 0m, 12.34m, 999_999.99m], values);
    }

    [Fact]
    public async Task Write_Decimal_IsNotMistakenForFixedSizeBinary()
    {
        // Every Arrow decimal type derives from FixedSizeBinaryType, so the FixedSizeBinary handling must
        // match the type exactly.
        var type = new Decimal128Type(10, 2);
        await using var table = await DeltaTable.CreateAsync(Fs, Schema(new Field("d", type, true)));
        Assert.Equal("decimal(10,2)", ((PrimitiveType)table.CurrentSnapshot.Schema.Fields[0].Type).TypeName);

        await table.WriteAsync([new RecordBatch(table.ArrowSchema,
            [new Decimal128Array.Builder(type).Append(12.34m).Build()], 1)]);
        Assert.Equal(12.34m, DecimalAt(Assert.Single(await ReadAllAsync(table)).Column(0), 0));
    }

    // ── Rewrites keep the canonical form ──────────────────────────────────────────────────────────────

    private static async Task<(Parquet.PhysicalType? Type, int? Length)> PhysicalTypeOf(string file, string column)
    {
        await using var raf = new LocalRandomAccessFile(file);
        await using var reader = new Parquet.ParquetFileReader(raf, ownsFile: false);
        var element = (await reader.ReadMetadataAsync()).Schema.First(s => s.Name == column);
        return (element.Type, element.TypeLength);
    }

    private static BooleanArray Where(RecordBatch batch, Func<decimal?, bool> test)
    {
        var builder = new BooleanArray.Builder();
        var column = batch.Column(batch.Schema.GetFieldIndex("d"));
        for (int i = 0; i < batch.Length; i++)
            builder.Append(test(DecimalAt(column, i)));
        return builder.Build();
    }

    [Fact]
    public async Task Rewrites_AndChangeFiles_WriteDecimalsInTheCanonicalForm()
    {
        // Reads hand back Decimal32 for a decimal(9,2) column (#469), so every rewrite starts from a
        // non-canonical batch. UPDATE, a copy-on-write DELETE, OPTIMIZE and the change files they emit must
        // all still write the canonical FIXED_LEN_BYTE_ARRAY(16), not INT32.
        var type = new Decimal128Type(9, 2);
        var schema = Schema(new Field("d", type, true));
        await using var table = await DeltaTable.CreateAsync(Fs, schema,
            configuration: new Dictionary<string, string> { ["delta.enableChangeDataFeed"] = "true" });
        await table.WriteAsync([new RecordBatch(schema, [new Decimal128Array.Builder(type).Append(1m).Append(2m).Build()], 2)]);
        string first = table.CurrentSnapshot.ActiveFiles.Values.Single().Path;
        await table.WriteAsync([new RecordBatch(schema, [new Decimal128Array.Builder(type).Append(3m).Build()], 1)]);

        await table.DeleteRowsAsync(
            RowSelection.ByPath(new Dictionary<string, IReadOnlyCollection<long>> { [first] = [1L] }),
            RowDeleteMode.CopyOnWrite);
        await table.UpdateAsync(b => Where(b, d => d == 1m), b => b);
        await table.CompactAsync(new CompactionOptions { MinFileSize = long.MaxValue });

        var files = Directory.GetFiles(_tempDir, "*.parquet", SearchOption.AllDirectories);
        Assert.Contains(files, f => f.Contains("_change_data"));
        foreach (var file in files)
            Assert.Equal((Parquet.PhysicalType.FixedLenByteArray, 16), await PhysicalTypeOf(file, "d"));

        var values = new List<decimal?>();
        foreach (var b in await ReadAllAsync(table))
        {
            for (int i = 0; i < b.Length; i++)
                values.Add(DecimalAt(b.Column(0), i));
        }
        values.Sort();
        Assert.Equal([1m, 3m], values);
    }

    // ── Decimal256 ────────────────────────────────────────────────────────────────────────────────────

    [Fact]
    public void Normalize_NarrowsDecimal256ToDecimal128_KeepingValuesAndSlices()
    {
        var d256 = new Decimal256Type(20, 2);
        var array = new Decimal256Array.Builder(d256).Append(-12.34m).Append(5m).Append(99_999_999_999_999_999.99m).Build();
        var batch = new RecordBatch(Schema(new Field("d", d256, true)), [array], 3).Slice(1, 2);

        var wide = Assert.IsType<Decimal128Array>(WriteTypeNormalization.Normalize(batch).Column(0));
        Assert.Equal(20, ((Decimal128Type)wide.Data.DataType).Precision);
        Assert.Equal(5m, wide.GetValue(0));
        Assert.Equal(99_999_999_999_999_999.99m, wide.GetValue(1));

        var negative = Assert.IsType<Decimal128Array>(WriteTypeNormalization.Normalize(
            new RecordBatch(Schema(new Field("d", d256, true)), [array], 3)).Column(0));
        Assert.Equal(-12.34m, negative.GetValue(0));
    }

    [Fact]
    public async Task Write_Decimal256ValueTooLargeForItsPrecision_IsRefused()
    {
        // decimal(20,2) promises the value fits Decimal128; a high half that is not the low half's sign
        // extension breaks that promise.
        var d256 = new Decimal256Type(20, 2);
        var schema = Schema(new Field("d", d256, true));
        await using var table = await DeltaTable.CreateAsync(Fs, schema);

        var bytes = new byte[32];
        bytes[16] = 1; // 2^128: no Decimal128 holds it
        var array = new Decimal256Array(new ArrayData(d256, 1, 0, 0, [ArrowBuffer.Empty, new ArrowBuffer(bytes)]));
        await RefusedAsync(DeltaTableErrorCodes.UnwritableValue, async () =>
            await table.WriteAsync([new RecordBatch(schema, [array], 1)]));
    }

    // ── Values nobody can see are not checked ─────────────────────────────────────────────────────────

    private static Date64Array Date64s(long[] millis, byte[]? validity = null)
    {
        int nulls = validity is null
            ? 0
            : Enumerable.Range(0, millis.Length).Count(i => (validity[i / 8] & (1 << (i % 8))) == 0);
        return new(new ArrayData(Date64Type.Default, millis.Length, nulls, 0,
            [validity is null ? ArrowBuffer.Empty : new ArrowBuffer(validity),
             new ArrowBuffer(millis.SelectMany(BitConverter.GetBytes).ToArray())]));
    }

    private const long Day = 86_400_000;

    [Fact]
    public async Task Write_Date64UnderANullStruct_IsNotChecked()
    {
        // Row 1's struct is NULL, so its child slot is never written, whatever it holds.
        var structType = new ArrowStructType([new Field("d", Date64Type.Default, true)]);
        var schema = Schema(new Field("s", structType, true));
        await using var table = await DeltaTable.CreateAsync(Fs, schema);

        var structs = new StructArray(structType, 2, [Date64s([3 * Day, 3 * Day + 1])],
            new ArrowBuffer(new byte[] { 0b01 }), nullCount: 1);
        await table.WriteAsync([new RecordBatch(schema, [structs], 2)]);

        var batch = Assert.Single(await ReadAllAsync(table));
        var read = (StructArray)batch.Column(0);
        Assert.True(read.IsNull(1));
        Assert.Equal(new DateTime(1970, 1, 4), ((Date32Array)read.Fields[0]).GetDateTime(0));
    }

    [Fact]
    public void Normalize_Date64ListElementsNoVisibleRowReferences_AreNotChecked()
    {
        // Values [2 days, 5 ms, 7 ms, 9 ms]: row 0 references element 0, row 1 is NULL over elements 1..2,
        // and element 3 is referenced by nothing. Only element 0 is ever written. (Called directly: the
        // Parquet writer itself cannot yet write an unreferenced element, #470.)
        var listType = new ListType(new Field("element", Date64Type.Default, true));
        var list = new ListArray(listType, 2,
            new ArrowBuffer(new byte[] { 0, 0, 0, 0, 1, 0, 0, 0, 3, 0, 0, 0 }),
            Date64s([2 * Day, 5, 7, 9]), new ArrowBuffer(new byte[] { 0b01 }), nullCount: 1);

        var normalized = WriteTypeNormalization.Normalize(new RecordBatch(Schema(new Field("l", listType, true)), [list], 2));

        var elements = Assert.IsType<Date32Array>(((ListArray)normalized.Column(0)).Values);
        Assert.Equal(new DateTime(1970, 1, 3), elements.GetDateTime(0));
    }

    [Fact]
    public void Normalize_Date64ListElementAVisibleRowReferences_IsStillChecked()
    {
        var listType = new ListType(new Field("element", Date64Type.Default, true));
        var list = new ListArray(listType, 1,
            new ArrowBuffer(new byte[] { 0, 0, 0, 0, 2, 0, 0, 0 }), Date64s([2 * Day, 5]), ArrowBuffer.Empty);

        var ex = Assert.Throws<DeltaFormatException>(() =>
            WriteTypeNormalization.Normalize(new RecordBatch(Schema(new Field("l", listType, true)), [list], 1)));
        Assert.Contains("'l.element'", ex.Message);
    }

    [Fact]
    public async Task Write_Date64VisibleThroughAStruct_IsStillChecked()
    {
        var structType = new ArrowStructType([new Field("d", Date64Type.Default, true)]);
        var schema = Schema(new Field("s", structType, true));
        await using var table = await DeltaTable.CreateAsync(Fs, schema);

        var structs = new StructArray(structType, 1, [Date64s([Day + 1])], ArrowBuffer.Empty);
        var ex = await RefusedAsync(DeltaTableErrorCodes.UnwritableValue, async () =>
            await table.WriteAsync([new RecordBatch(schema, [structs], 1)]));
        Assert.Contains("'s.d'", ex.Message);
    }

    // ── Batch types against the declared types ────────────────────────────────────────────────────────

    private static Task<ArgumentException> MistypedAsync(Func<Task> write) =>
        Assert.ThrowsAsync<ArgumentException>(write);

    [Fact]
    public async Task Write_Int32IntoAStringColumn_IsRefusedOnEveryEntryPoint()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, Schema(new Field("s", StringType.Default, true)));
        long version = table.CurrentSnapshot.Version;
        var batch = new RecordBatch(Schema(new Field("s", Int32Type.Default, true)),
            [new Int32Array.Builder().Append(1).Build()], 1);

        var ex = await MistypedAsync(async () => await table.WriteAsync([batch]));
        Assert.Contains("'s' is declared string", ex.Message);
        await MistypedAsync(async () => await table.WriteDataFilesAsync([batch]));
        await MistypedAsync(async () => await table.StartTransaction().WriteAsync([batch]));

        Assert.Equal(version, table.CurrentSnapshot.Version);
        Assert.Empty(Directory.EnumerateFiles(_tempDir, "*.parquet", SearchOption.AllDirectories));
    }

    [Fact]
    public async Task Write_ConvertibleTypeForTheWrongDeclaredType_IsRefusedRatherThanConverted()
    {
        // Normalization would happily turn these into Binary / Decimal128; the declared type decides
        // whether that is what the column holds.
        await using var table = await DeltaTable.CreateAsync(Fs, Schema(
            new Field("d", new Decimal128Type(9, 2), true), new Field("b", BinaryType.Default, true)));

        var fsbForDecimal = new RecordBatch(Schema(new Field("d", new FixedSizeBinaryType(16), true)),
            [FixedSizeBinary(16, 1, new byte[16])], 1);
        await MistypedAsync(async () => await table.WriteAsync([fsbForDecimal]));

        var d32 = new Decimal32Type(9, 2);
        var decimalForBinary = new RecordBatch(Schema(new Field("b", d32, true)),
            [new Decimal32Array(new ArrayData(d32, 1, 0, 0, [ArrowBuffer.Empty, Int32s(1)]))], 1);
        await MistypedAsync(async () => await table.WriteAsync([decimalForBinary]));
    }

    [Fact]
    public async Task Write_DecimalOfAnotherPrecisionOrScale_IsRefused()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, Schema(new Field("d", new Decimal128Type(12, 2), true)));

        var type = new Decimal128Type(10, 2);
        var ex = await MistypedAsync(async () => await table.WriteAsync([new RecordBatch(
            Schema(new Field("d", type, true)), [new Decimal128Array.Builder(type).Append(1m).Build()], 1)]));
        Assert.Contains("decimal(12,2)", ex.Message);
        Assert.Contains("decimal(10,2)", ex.Message);
    }

    [Fact]
    public async Task Write_NestedMismatch_NamesItsPath()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, Schema(Struct("s", Long("a"), Long("b"))));

        var structType = new ArrowStructType([new Field("a", Int32Type.Default, true)]);
        var batch = new RecordBatch(Schema(new Field("s", structType, true)),
            [new StructArray(structType, 1, [new Int32Array.Builder().Append(1).Build()], ArrowBuffer.Empty)], 1);

        var ex = await MistypedAsync(async () => await table.WriteAsync([batch]));
        Assert.Contains("'s.a' is declared long", ex.Message);
    }

    [Fact]
    public async Task Write_EveryArrowFormOfTheDeclaredType_IsAccepted()
    {
        // The comparison is on Delta types, so the Arrow forms that map onto the declared one all pass, and
        // a struct may omit declared children (they read as null).
        await using var table = await DeltaTable.CreateAsync(Fs, Schema(
            new Field("s", StringType.Default, true), new Field("d", new Decimal128Type(9, 2), true),
            Struct("t", Long("a"), Long("b"))));

        var d32 = new Decimal32Type(9, 2);
        var partial = new ArrowStructType([Long("b")]);
        await table.WriteAsync([new RecordBatch(Schema(
                new Field("s", LargeStringType.Default, true), new Field("d", d32, true), new Field("t", partial, true)),
            [
                new LargeStringArray.Builder().Append("x").Build(),
                new Decimal32Array(new ArrayData(d32, 1, 0, 0, [ArrowBuffer.Empty, Int32s(125)])),
                new StructArray(partial, 1, [new Int64Array.Builder().Append(7).Build()], ArrowBuffer.Empty),
            ], 1)]);

        Assert.Single(await ReadAllAsync(table));
    }

    // ── The codec seam stays value-blind ──────────────────────────────────────────────────────────────

    private sealed class CapturingWriter : IDataFileWriter
    {
        public List<RecordBatch> Received { get; } = [];

        public async ValueTask<long> WriteAsync(
            IAsyncEnumerable<RecordBatch> batches, string relativePath, CancellationToken cancellationToken)
        {
            await foreach (var b in batches.WithCancellation(cancellationToken))
                Received.Add(b);
            return 1;
        }
    }

    [Fact]
    public async Task Write_UnderAHostWriter_ReachesItVerbatim()
    {
        // A host that owns the bytes may present its own representation for a declared column
        // (CodecSeamValueBlindnessTests), so neither the conversion nor the type check applies.
        var writer = new CapturingWriter();
#pragma warning disable EWDELTA0001 // codec seam is experimental
        var options = DeltaTableOptions.Default with { DataFileWriter = writer };
#pragma warning restore EWDELTA0001
        await using var table = await DeltaTable.CreateAsync(
            Fs, Schema(new Field("b", BinaryType.Default, true)), options);

        var batch = new RecordBatch(Schema(new Field("b", new FixedSizeBinaryType(4), true)),
            [FixedSizeBinary(4, 1, [1, 2, 3, 4])], 1);
        await table.WriteDataFilesAsync([batch]);

        Assert.IsType<FixedSizeBinaryArray>(Assert.Single(writer.Received).Column(0));
    }

    // ── Caller-supplied change rows get the same checks ───────────────────────────────────────────────

    private async Task<DeltaTable> CreateCdfTableAsync(Apache.Arrow.Schema schema) =>
        await DeltaTable.CreateAsync(Fs, schema,
            configuration: new Dictionary<string, string> { ["delta.enableChangeDataFeed"] = "true" });

    [Fact]
    public async Task ChangeData_NanosecondTimestampNestedInAStruct_IsRefusedOnBothEntryPoints()
    {
        var micros = new TimestampType(TimeUnit.Microsecond, "UTC");
        await using var table = await CreateCdfTableAsync(Schema(Struct("s", new Field("t", micros, true))));

        var nanos = new TimestampType(TimeUnit.Nanosecond, "UTC");
        var structType = new ArrowStructType([new Field("t", nanos, true)]);
        var rows = new RecordBatch(Schema(new Field("s", structType, true)),
            [new StructArray(structType, 1, [new TimestampArray.Builder(nanos).Append(new DateTimeOffset(1970, 1, 1, 0, 0, 0, TimeSpan.Zero)).Build()], ArrowBuffer.Empty)], 1);

        await Assert.ThrowsAsync<DeltaFormatException>(async () => await table.WriteChangeDataFileAsync(rows, "delete"));
        await Assert.ThrowsAsync<DeltaFormatException>(async () =>
            await table.StartTransaction().StageChangeDataAsync(rows, "delete"));
        Assert.Empty(Directory.EnumerateFiles(_tempDir, "*.parquet", SearchOption.AllDirectories));
    }

    [Fact]
    public async Task ChangeData_MistypedColumn_IsRefused()
    {
        await using var table = await CreateCdfTableAsync(Schema(new Field("s", StringType.Default, true)));

        var rows = new RecordBatch(Schema(new Field("s", Int32Type.Default, true)),
            [new Int32Array.Builder().Append(1).Build()], 1);
        await MistypedAsync(async () => await table.WriteChangeDataFileAsync(rows, "delete"));
    }

    // ── What an UPDATE's updater returns ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task Update_UpdaterReturningAMistypedColumn_IsRefusedAndNothingIsCommitted()
    {
        var schema = Schema(Long("id"), new Field("s", StringType.Default, true));
        await using var table = await DeltaTable.CreateAsync(Fs, schema);
        await table.WriteAsync([new RecordBatch(schema,
            [new Int64Array.Builder().Append(1).Build(), new StringArray.Builder().Append("a").Build()], 1)]);
        long version = table.CurrentSnapshot.Version;

        var ex = await MistypedAsync(async () => await table.UpdateAsync(
            b => new BooleanArray.Builder().AppendRange(Enumerable.Repeat(true, b.Length)).Build(),
            b => new RecordBatch(
                Schema(b.Schema.FieldsList[0], new Field("s", Int32Type.Default, true)),
                [b.Column(0), new Int32Array.Builder().AppendRange(Enumerable.Repeat(7, b.Length)).Build()],
                b.Length)));
        Assert.Contains("'s' is declared string", ex.Message);
        Assert.Equal(version, table.CurrentSnapshot.Version);
    }

    [Fact]
    public async Task Update_PassingThroughAColumnTheReaderNarrowed_IsNotRefused()
    {
        // A Spark INT96 timestamp reads back as a NAIVE timestamp[us] under a declared (zoned) `timestamp`.
        // Simulated with a file holding a naive timestamp, committed as another writer would. An UPDATE that
        // changes only `id` passes `t` through in the form it was read, as it always has, so `t` is not
        // re-checked against its declaration.
        var zoned = new TimestampType(TimeUnit.Microsecond, "UTC");
        var naive = new TimestampType(TimeUnit.Microsecond, (string?)null);
        await using var table = await DeltaTable.CreateAsync(Fs, Schema(Long("id"), new Field("t", zoned, true)));

        var fileSchema = Schema(Long("id"), new Field("t", naive, true));
        var when = new DateTimeOffset(2024, 3, 1, 12, 0, 0, TimeSpan.Zero);
        string path = Path.Combine(_tempDir, "foreign.parquet");
        await using (var file = new LocalSequentialFile(path))
        {
            await using var writer = new Parquet.ParquetFileWriter(file, ownsFile: false);
            await writer.WriteRowGroupAsync(new RecordBatch(fileSchema,
                [new Int64Array.Builder().Append(1).Build(), new TimestampArray.Builder(naive).Append(when).Build()], 1));
        }
        await table.CommitDataFilesAsync([new WrittenDataFile("foreign.parquet", new FileInfo(path).Length, 1, null, null)]);

        var naiveRead = (await ReadAllAsync(table)).Single().Schema.GetFieldByName("t").DataType;
        Assert.Null(((TimestampType)naiveRead).Timezone); // the premise: the reader hands back the naive form

        await table.UpdateAsync(
            b => new BooleanArray.Builder().AppendRange(Enumerable.Repeat(true, b.Length)).Build(),
            b => new RecordBatch(b.Schema,
                [new Int64Array.Builder().AppendRange(Enumerable.Repeat(2L, b.Length)).Build(), b.Column(1)], b.Length));

        var batch = (await ReadAllAsync(table)).Single();
        Assert.Equal(2L, ((Int64Array)batch.Column(0)).GetValue(0));
    }

    [Fact]
    public async Task ChangeData_UndeclaredColumn_IsRefused()
    {
        await using var table = await CreateCdfTableAsync(Schema(new Field("s", StringType.Default, true)));

        var rows = new RecordBatch(Schema(new Field("s", StringType.Default, true), Long("extra")),
            [new StringArray.Builder().Append("a").Build(), new Int64Array.Builder().Append(1).Build()], 1);
        var ex = await MistypedAsync(async () => await table.WriteChangeDataFileAsync(rows, "delete"));
        Assert.Contains("'extra'", ex.Message);
        await MistypedAsync(async () => await table.StartTransaction().StageChangeDataAsync(rows, "delete"));
    }

    // ── Maps stay maps ────────────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Write_MapWithAConvertibleValue_StaysAMap()
    {
        // In Arrow 23 MapType derives from NestedType, not ListType, so the normalizer's map arm is the one
        // that matches. Pinned so a future Arrow where the hierarchy changes fails here, not in a data file.
        var mapType = new Apache.Arrow.Types.MapType(
            new Field("key", StringType.Default, false), new Field("value", Date64Type.Default, true));
        var schema = Schema(new Field("m", mapType, true));
        await using var table = await DeltaTable.CreateAsync(Fs, schema);

        var builder = new MapArray.Builder(mapType);
        builder.Append();
        ((StringArray.Builder)builder.KeyBuilder).Append("k");
        ((Date64Array.Builder)builder.ValueBuilder).Append(new DateTime(2024, 3, 1));
        var map = builder.Build();

        var normalized = WriteTypeNormalization.Normalize(new RecordBatch(schema, [map], 1));
        var normalizedType = Assert.IsType<Apache.Arrow.Types.MapType>(normalized.Column(0).Data.DataType);
        Assert.IsType<Date32Type>(normalizedType.ValueField.DataType);

        await table.WriteAsync([new RecordBatch(schema, [map], 1)]);
        var read = Assert.IsType<MapArray>(Assert.Single(await ReadAllAsync(table)).Column(0));
        Assert.Equal(new DateTime(2024, 3, 1), ((Date32Array)read.Values).GetDateTime(0));
    }

    // ── Allocation stays proportional to the slice ────────────────────────────────────────────────────

    private const int Big = 100_000;

    private static RecordBatch LastRowOf(Field field, IArrowArray array) =>
        new RecordBatch(Schema(field), [array], array.Length).Slice(array.Length - 1, 1);

    [Fact]
    public void Normalize_ASliceAtTheEndOfALargeArray_AllocatesForTheSliceOnly()
    {
        // Each converted array comes out zero-offset and sized to its own length, so a one-row slice of a
        // 100k-row column costs one row, not the 99,999 before it.
        var fsbBytes = new byte[Big * 4];
        fsbBytes[^1] = 9;
        var fsb = WriteTypeNormalization.Normalize(LastRowOf(
            new Field("b", new FixedSizeBinaryType(4), true), FixedSizeBinary(4, Big, fsbBytes))).Column(0).Data;
        Assert.Equal((0, 1), (fsb.Offset, fsb.Length));
        Assert.Equal(2 * sizeof(int), fsb.Buffers[1].Length); // offsets
        Assert.Equal(4, fsb.Buffers[2].Length);                // the slice's bytes, shared
        Assert.Equal(new byte[] { 0, 0, 0, 9 }, ((BinaryArray)ArrowArrayFactory.BuildArray(fsb)).GetBytes(0).ToArray());

        var date = WriteTypeNormalization.Normalize(LastRowOf(
            new Field("d", Date64Type.Default, true), Date64s(Enumerable.Repeat(2 * Day, Big).ToArray()))).Column(0).Data;
        Assert.Equal((0, 1, sizeof(int)), (date.Offset, date.Length, date.Buffers[1].Length));

        var d32 = new Decimal32Type(9, 2);
        var dec = WriteTypeNormalization.Normalize(LastRowOf(new Field("d", d32, true),
            new Decimal32Array(new ArrayData(d32, Big, 0, 0, [ArrowBuffer.Empty, Int32s(new int[Big])])))).Column(0).Data;
        Assert.Equal((0, 1, 16), (dec.Offset, dec.Length, dec.Buffers[1].Length));

        var d256 = new Decimal256Type(20, 2);
        var wide = WriteTypeNormalization.Normalize(LastRowOf(new Field("d", d256, true),
            new Decimal256Array(new ArrayData(d256, Big, 0, 0, [ArrowBuffer.Empty, new ArrowBuffer(new byte[Big * 32])])))).Column(0).Data;
        Assert.Equal((0, 1, 16), (wide.Offset, wide.Length, wide.Buffers[1].Length));
    }

    [Fact]
    public void Normalize_ASlicedStruct_IsRebasedSoItsChildrenAreSlicedToo()
    {
        var structType = new ArrowStructType([new Field("d", Date64Type.Default, true)]);
        var structs = new StructArray(structType, Big, [Date64s(Enumerable.Repeat(3 * Day, Big).ToArray())], ArrowBuffer.Empty);

        var data = WriteTypeNormalization.Normalize(LastRowOf(new Field("s", structType, true), structs)).Column(0).Data;

        Assert.Equal((0, 1), (data.Offset, data.Length));
        var child = data.Children[0];
        Assert.Equal((0, 1, sizeof(int)), (child.Offset, child.Length, child.Buffers[1].Length));
        Assert.Equal(new DateTime(1970, 1, 4), ((Date32Array)ArrowArrayFactory.BuildArray(child)).GetDateTime(0));
    }

    [Theory]
    [InlineData(3)]  // not byte-aligned: the bits are copied
    [InlineData(8)]  // byte-aligned: the bitmap bytes are shared
    public void Normalize_ASliceWithNulls_KeepsEachRowsNullness(int offset)
    {
        // Every third row is valid: a period that does not divide 8, so each bitmap byte differs from the
        // next and a slice taken from the wrong byte shows.
        const int length = 20;
        var validity = new byte[(length + 7) / 8];
        for (int i = 0; i < length; i += 3)
            validity[i / 8] |= (byte)(1 << (i % 8));
        var array = Date64s(Enumerable.Range(0, length).Select(i => i * Day).ToArray(), validity);
        var batch = new RecordBatch(Schema(new Field("d", Date64Type.Default, true)), [array], length).Slice(offset, 6);

        var normalized = (Date32Array)WriteTypeNormalization.Normalize(batch).Column(0);

        for (int i = 0; i < 6; i++)
        {
            bool valid = (offset + i) % 3 == 0;
            Assert.Equal(!valid, normalized.IsNull(i));
            if (valid)
                Assert.Equal(new DateTime(1970, 1, 1).AddDays(offset + i), normalized.GetDateTime(i));
        }
    }

    [Fact]
    public void NormalizeAll_WithNothingToConvert_ReturnsTheCallersOwnList()
    {
        // The write hot path: nothing to convert must allocate nothing, not even a new list.
        var batches = new List<RecordBatch> { new(Schema(Long("id")), [new Int64Array.Builder().Append(1).Build()], 1) };
        Assert.Same(batches, WriteTypeNormalization.NormalizeAll(batches));

        var mixed = new List<RecordBatch>
        {
            batches[0],
            new(Schema(new Field("d", Date64Type.Default, true)), [Date64s([Day])], 1),
        };
        var result = WriteTypeNormalization.NormalizeAll(mixed);
        Assert.NotSame(mixed, result);
        Assert.Same(batches[0], result[0]);
        Assert.IsType<Date32Array>(result[1].Column(0));
    }

    // ── Rewrites refuse what no writer can store ──────────────────────────────────────────────────────

    /// <summary>Commits a two-row file holding a nanosecond timestamp under a declared (microsecond)
    /// `timestamp`, as a non-conforming writer could, and returns its path.</summary>
    private async Task<string> CommitForeignNanosecondFileAsync(DeltaTable table, string name, long id)
    {
        var nanos = new TimestampType(TimeUnit.Nanosecond, "UTC");
        var fileSchema = Schema(Long("id"), new Field("t", nanos, true));
        string path = Path.Combine(_tempDir, name);
        await using (var file = new LocalSequentialFile(path))
        {
            await using var writer = new Parquet.ParquetFileWriter(file, ownsFile: false);
            await writer.WriteRowGroupAsync(new RecordBatch(fileSchema,
                [
                    new Int64Array.Builder().Append(id).Append(id + 10).Build(),
                    new TimestampArray.Builder(nanos)
                        .Append(new DateTimeOffset(2024, 3, 1, 0, 0, 0, TimeSpan.Zero))
                        .Append(new DateTimeOffset(2024, 3, 2, 0, 0, 0, TimeSpan.Zero)).Build(),
                ], 2));
        }
        await table.CommitDataFilesAsync([new WrittenDataFile(name, new FileInfo(path).Length, 2, null, null)]);
        return name;
    }

    [Fact]
    public async Task Rewrites_OfAForeignNanosecondTimestamp_AreRefusedAndNothingIsCommitted()
    {
        var zoned = new TimestampType(TimeUnit.Microsecond, "UTC");
        await using var table = await DeltaTable.CreateAsync(Fs, Schema(Long("id"), new Field("t", zoned, true)));
        string first = await CommitForeignNanosecondFileAsync(table, "foreign1.parquet", 1);
        await CommitForeignNanosecondFileAsync(table, "foreign2.parquet", 2);
        long version = table.CurrentSnapshot.Version;

        // The premise: the reader keeps the file's nanosecond unit. Each file has two rows, so deleting one
        // leaves a survivor the copy-on-write DELETE has to rewrite.
        var read = (TimestampType)(await ReadAllAsync(table)).First().Schema.GetFieldByName("t").DataType;
        Assert.Equal(TimeUnit.Nanosecond, read.Unit);

        await Assert.ThrowsAsync<DeltaFormatException>(async () => await table.UpdateAsync(
            b => new BooleanArray.Builder().AppendRange(Enumerable.Repeat(true, b.Length)).Build(),
            b => b));
        await Assert.ThrowsAsync<DeltaFormatException>(async () => await table.DeleteRowsAsync(
            RowSelection.ByPath(new Dictionary<string, IReadOnlyCollection<long>> { [first] = [0L] }),
            RowDeleteMode.CopyOnWrite));
        await Assert.ThrowsAsync<DeltaFormatException>(async () =>
            await table.CompactAsync(new CompactionOptions { MinFileSize = long.MaxValue }));

        Assert.Equal(version, table.CurrentSnapshot.Version);
    }

    [Fact]
    public async Task ChangeData_OnATablePartitionedByADate64Column_IsWrittenUnderItsDay()
    {
        // The rows are split by partition value; before they were normalized first, Date64 had no partition
        // value form ("Partition values of Arrow type Date64 are not supported").
        var declared = Schema(new Field("p", Date32Type.Default, true), Long("v"));
        await using var table = await DeltaTable.CreateAsync(Fs, declared, partitionColumns: ["p"],
            configuration: new Dictionary<string, string> { ["delta.enableChangeDataFeed"] = "true" });

        var rows = new RecordBatch(Schema(new Field("p", Date64Type.Default, true), Long("v")),
            [new Date64Array.Builder().Append(new DateTime(2024, 3, 1)).Build(), new Int64Array.Builder().Append(1).Build()], 1);
        var txn = table.StartTransaction();
        await txn.StageChangeDataAsync(rows, "delete");
        await txn.CommitAsync();

        string log = File.ReadAllText(Directory.GetFiles(Path.Combine(_tempDir, "_delta_log"), "*.json").Max()!);
        Assert.Contains("\"p\":\"2024-03-01\"", log);
    }

    // ── Change files built from rows read back ────────────────────────────────────────────────────────

    [Fact]
    public async Task DeletionVectorDelete_WritingChangeRowsWithAForeignNanosecondTimestamp_IsRefused()
    {
        // A deletion-vector DELETE rewrites no data file, but with the change feed on it writes the deleted
        // rows, read back from the file, into _change_data.
        var zoned = new TimestampType(TimeUnit.Microsecond, "UTC");
        await using var table = await DeltaTable.CreateAsync(Fs, Schema(Long("id"), new Field("t", zoned, true)),
            enableDeletionVectors: true,
            configuration: new Dictionary<string, string> { ["delta.enableChangeDataFeed"] = "true" });
        await CommitForeignNanosecondFileAsync(table, "foreign.parquet", 1);
        long version = table.CurrentSnapshot.Version;

        await Assert.ThrowsAsync<DeltaFormatException>(async () => await table.DeleteAsync(
            b => new BooleanArray.Builder().AppendRange(
                Enumerable.Range(0, b.Length).Select(i => ((Int64Array)b.Column(b.Schema.GetFieldIndex("id"))).GetValue(i) == 1)).Build()));

        Assert.Equal(version, table.CurrentSnapshot.Version);
        Assert.DoesNotContain(Directory.EnumerateFiles(_tempDir, "*.parquet", SearchOption.AllDirectories),
            f => f.Contains("_change_data"));
    }

    // ── Undeclared nested fields ──────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Write_UndeclaredNestedStructField_IsRefused()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, Schema(Struct("s", Long("a"))));

        var structType = new ArrowStructType([Long("a"), new Field("extra", Int32Type.Default, true)]);
        var batch = new RecordBatch(Schema(new Field("s", structType, true)),
            [new StructArray(structType, 1,
                [new Int64Array.Builder().Append(1).Build(), new Int32Array.Builder().Append(2).Build()],
                ArrowBuffer.Empty)], 1);

        var ex = await MistypedAsync(async () => await table.WriteAsync([batch]));
        Assert.Contains("'s.extra'", ex.Message);
        await MistypedAsync(async () => await table.WriteDataFilesAsync([batch]));
        Assert.Empty(Directory.EnumerateFiles(_tempDir, "*.parquet", SearchOption.AllDirectories));
    }

    [Fact]
    public async Task Write_UndeclaredFieldInAListOfStructs_IsRefused()
    {
        var declaredElement = new ArrowStructType([Long("a")]);
        await using var table = await DeltaTable.CreateAsync(Fs,
            Schema(new Field("l", new ListType(new Field("element", declaredElement, true)), true)));

        var element = new ArrowStructType([Long("a"), Long("extra")]);
        var listType = new ListType(new Field("element", element, true));
        var values = new StructArray(element, 1,
            [new Int64Array.Builder().Append(1).Build(), new Int64Array.Builder().Append(2).Build()], ArrowBuffer.Empty);
        var batch = new RecordBatch(Schema(new Field("l", listType, true)),
            [new ListArray(listType, 1, new ArrowBuffer(new byte[] { 0, 0, 0, 0, 1, 0, 0, 0 }), values, ArrowBuffer.Empty)], 1);

        var ex = await MistypedAsync(async () => await table.WriteAsync([batch]));
        Assert.Contains("'l.element.extra'", ex.Message);
    }

    [Fact]
    public async Task Update_PostImageWithAnUndeclaredNestedField_IsRefused()
    {
        var schema = Schema(Struct("s", Long("a")));
        await using var table = await DeltaTable.CreateAsync(Fs, schema);
        var declaredType = (ArrowStructType)schema.FieldsList[0].DataType;
        await table.WriteAsync([new RecordBatch(schema,
            [new StructArray(declaredType, 1, [new Int64Array.Builder().Append(1).Build()], ArrowBuffer.Empty)], 1)]);

        var widened = new ArrowStructType([Long("a"), Long("extra")]);
        await MistypedAsync(async () => await table.UpdateAsync(
            b => new BooleanArray.Builder().AppendRange(Enumerable.Repeat(true, b.Length)).Build(),
            b => new RecordBatch(Schema(new Field("s", widened, true)),
                [new StructArray(widened, b.Length,
                    [((StructArray)b.Column(0)).Fields[0], new Int64Array.Builder().AppendRange(Enumerable.Repeat(9L, b.Length)).Build()],
                    ArrowBuffer.Empty)], b.Length)));
    }

    [Fact]
    public async Task Write_OmittingADeclaredNestedField_IsStillAllowed()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, Schema(Struct("s", Long("a"), Long("b"))));

        var partial = new ArrowStructType([Long("b")]);
        await table.WriteAsync([new RecordBatch(Schema(new Field("s", partial, true)),
            [new StructArray(partial, 1, [new Int64Array.Builder().Append(1).Build()], ArrowBuffer.Empty)], 1)]);
    }

    // ── A host writer's partition values ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task Write_UnderAHostWriter_ConvertsPartitionColumnsButNotData()
    {
        // The host owns the data bytes, so `b` reaches it as FixedSizeBinary. The partition VALUE is log
        // metadata the library formats, and the split has no form for Date64, so `p` is still converted.
        var writer = new CapturingWriter();
#pragma warning disable EWDELTA0001 // codec seam is experimental
        var options = DeltaTableOptions.Default with { DataFileWriter = writer };
#pragma warning restore EWDELTA0001
        await using var table = await DeltaTable.CreateAsync(Fs,
            Schema(new Field("p", Date32Type.Default, true), new Field("b", BinaryType.Default, true)),
            options, partitionColumns: ["p"]);

        var batch = new RecordBatch(
            Schema(new Field("p", Date64Type.Default, true), new Field("b", new FixedSizeBinaryType(4), true)),
            [new Date64Array.Builder().Append(new DateTime(2024, 3, 1)).Build(), FixedSizeBinary(4, 1, [1, 2, 3, 4])], 1);

        var files = await table.WriteDataFilesAsync([batch]);
        Assert.Equal("2024-03-01", Assert.Single(files).PartitionValues!["p"]);
        var handed = Assert.Single(writer.Received);
        Assert.IsType<FixedSizeBinaryArray>(handed.Column(handed.Schema.GetFieldIndex("b")));

        await table.WriteAsync([batch]);
        Assert.Equal(2, writer.Received.Count);
    }

    // ── Drops on a table another writer left with a duplicate pair ────────────────────────────────────

    /// <summary>Appends a metaData commit whose schema string is the latest one transformed by
    /// <paramref name="edit"/> — a schema EW refuses to commit itself, written as another writer could —
    /// and returns a fresh handle on the result.</summary>
    private async Task<DeltaTable> WithForeignSchemaAsync(Action<System.Text.Json.Nodes.JsonNode> edit)
    {
        var log = Path.Combine(_tempDir, "_delta_log");
        string latest = Directory.GetFiles(log, "*.json").OrderBy(f => f, StringComparer.Ordinal).Last();
        long version = long.Parse(Path.GetFileNameWithoutExtension(latest));
        var metadata = System.Text.Json.Nodes.JsonNode.Parse(
            Directory.GetFiles(log, "*.json").OrderBy(f => f, StringComparer.Ordinal)
                .SelectMany(File.ReadAllLines)
                .Last(line => line.StartsWith("{\"metaData\"", StringComparison.Ordinal)))!;
        var schema = System.Text.Json.Nodes.JsonNode.Parse((string)metadata["metaData"]!["schemaString"]!)!;
        edit(schema);
        metadata["metaData"]!["schemaString"] = schema.ToJsonString();
        File.WriteAllText(Path.Combine(log, $"{version + 1:D20}.json"), metadata.ToJsonString() + "\n");
        return await DeltaTable.OpenAsync(Fs);
    }

    private static void RenameInSchema(System.Text.Json.Nodes.JsonNode fields, string from, string to)
    {
        foreach (var field in fields.AsArray())
        {
            if ((string)field!["name"]! == from)
                field["name"] = to;
        }
    }

    [Fact]
    public async Task DropColumn_OnATableWithADuplicatePair_RepairsItOrIsRefused()
    {
        await using (var created = await DeltaTable.CreateAsync(
            Fs, Schema(Long("X"), Long("y"), Long("other")), columnMappingMode: ColumnMappingMode.Name)) { }
        await using var table = await WithForeignSchemaAsync(s => RenameInSchema(s["fields"]!, "y", "x"));
        Assert.Equal(["X", "x", "other"], table.CurrentSnapshot.Schema.Fields.Select(f => f.Name));

        // An unrelated drop would commit the duplicate pair again.
        await DuplicateAsync(async () => await table.DropColumnAsync("other"));
        await DuplicateAsync(() => { table.ComputeDropColumn("other"); return Task.CompletedTask; });

        // Dropping one member of the pair leaves a valid schema, so it is the way out.
        await table.DropColumnAsync("x");
        Assert.Equal(["X", "other"], table.CurrentSnapshot.Schema.Fields.Select(f => f.Name));
    }

    [Fact]
    public async Task DropField_OnATableWithANestedDuplicatePair_RepairsItOrIsRefused()
    {
        await using (var created = await DeltaTable.CreateAsync(
            Fs, Schema(Struct("s", Long("A"), Long("b"), Long("c"))), columnMappingMode: ColumnMappingMode.Name)) { }
        await using var table = await WithForeignSchemaAsync(
            s => RenameInSchema(s["fields"]![0]!["type"]!["fields"]!, "b", "a"));

        await DuplicateAsync(async () => await table.DropFieldAsync(["s", "c"]));
        await table.DropFieldAsync(["s", "a"]);
    }

    // ── Names and types are checked before anything is converted ──────────────────────────────────────

    [Fact]
    public async Task Write_UndeclaredDate64ColumnWithAPartDayValue_IsReportedAsUndeclared()
    {
        // Converting first would have refused the VALUE (not a whole day) and hidden the real problem.
        await using var table = await DeltaTable.CreateAsync(Fs, Schema(Long("id")));

        var batch = new RecordBatch(Schema(Long("id"), new Field("d", Date64Type.Default, true)),
            [new Int64Array.Builder().Append(1).Build(), Date64s([Day + 1])], 1);
        var ex = await MistypedAsync(async () => await table.WriteAsync([batch]));
        Assert.Contains("'d'", ex.Message);
        await MistypedAsync(async () => await table.WriteDataFilesAsync([batch]));
    }

    [Fact]
    public async Task Write_MistypedConvertibleColumn_IsDescribedAsSupplied()
    {
        // The message names the type the caller handed in, not the Binary it would have been converted to.
        await using var table = await DeltaTable.CreateAsync(Fs, Schema(new Field("d", new Decimal128Type(9, 2), true)));

        var batch = new RecordBatch(Schema(new Field("d", new FixedSizeBinaryType(16), true)),
            [FixedSizeBinary(16, 1, new byte[16])], 1);
        var ex = await MistypedAsync(async () => await table.WriteAsync([batch]));
        Assert.Contains(new FixedSizeBinaryType(16).ToString(), ex.Message);
        Assert.DoesNotContain("Arrow " + BinaryType.Default, ex.Message);
    }

    // ── Integer to decimal widening ───────────────────────────────────────────────────────────────────

    [Theory]
    [InlineData("integer", "decimal(10,0)", true)]
    [InlineData("integer", "decimal(15,5)", true)]   // k1 = 5 >= k2 = 5
    [InlineData("integer", "decimal(10,5)", false)]  // k1 = 0 < k2 = 5: 2147483647 no longer fits
    [InlineData("short", "decimal(12,3)", false)]
    [InlineData("long", "decimal(20,0)", true)]
    [InlineData("long", "decimal(25,5)", true)]
    [InlineData("long", "decimal(22,5)", false)]
    [InlineData("long", "decimal(19,0)", false)]
    public void IntegerToDecimalWidening_RequiresAPrecisionDigitPerScaleDigit(string from, string to, bool ok)
    {
        Assert.Equal(ok, EngineeredWood.DeltaLake.Schema.TypeWidening.IsDecimalWidening(from, to));
    }
}
