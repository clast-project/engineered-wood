// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
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

    // ── Arrow types with no Delta equivalent ──────────────────────────────────────────────────────────

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

    [Fact]
    public async Task Create_FixedSizeBinary_IsRefused()
    {
        var ex = await RefusedAsync(null, async () =>
        {
            await using var t = await DeltaTable.CreateAsync(
                Fs, Schema(new Field("b", new FixedSizeBinaryType(4), true)));
        });
        Assert.Contains("Binary", ex.Message);
    }

    [Fact]
    public async Task Create_Date64_IsRefused()
    {
        var ex = await RefusedAsync(null, async () =>
        {
            await using var t = await DeltaTable.CreateAsync(Fs, Schema(new Field("d", Date64Type.Default, true)));
        });
        Assert.Contains("Date32", ex.Message);
    }

    [Fact]
    public async Task AddColumn_FixedSizeBinaryInsideAStruct_IsRefused()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, Schema(Long("id")));

        await RefusedAsync(null, async () => await table.AddColumnAsync(
            Struct("s", new Field("b", new FixedSizeBinaryType(16), true))));
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
