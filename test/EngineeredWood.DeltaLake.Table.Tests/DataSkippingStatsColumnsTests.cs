// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.IO.Local;
using ArrowListType = Apache.Arrow.Types.ListType;
using ArrowStructType = Apache.Arrow.Types.StructType;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// #471: <c>delta.dataSkippingStatsColumns</c> names columns by name, and Spark validates it on every metadata
/// update, so EW's RENAME/DROP must keep it in step (Spark rewrites or removes the entries), and CREATE must
/// refuse a value Spark would. The expected values follow delta-spark 4.4.0's bytecode and were measured against
/// its real functions.
/// </summary>
public class DataSkippingStatsColumnsTests : IDisposable
{
    private const string Key = "delta.dataSkippingStatsColumns";
    private readonly string _tempDir;

    public DataSkippingStatsColumnsTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_dsstats_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            Directory.Delete(_tempDir, recursive: true);
        }
    }

    private LocalTableFileSystem Fs => new(_tempDir);

    // ── Parsing and writing the property ────────────────────────────────────────────────────────────

    [Theory]
    [InlineData(" a , b ", "a|b")]
    [InlineData("a.b", "a.b")]
    [InlineData("`a.b`", "[a.b]")]
    [InlineData("`a`.b", "a.b")]
    [InlineData("`a``b`", "[a`b]")]
    [InlineData("1a", "1a")]
    [InlineData("a.`b c`", "a.[b c]")]
    [InlineData("a/*c*/,b", "a|b")]
    // Comments, as Spark 4.1's parser measured: they nest, and an open one runs to the end of the value.
    [InlineData("a/* /* n */ */,b", "a|b")]
    [InlineData("a/* /* n */,b", "a")]
    [InlineData("id/*", "id")]
    [InlineData("a/**/,b", "a|b")]
    [InlineData("a/*/,b", "a")]
    [InlineData("a -- c\n,b", "a|b")]
    [InlineData("", "")]
    [InlineData("  ", "")]
    public void Parse_FollowsSparksGrammar(string value, string expected)
    {
        // Parts joined by '.', a part containing '.' or a backquote shown in brackets, entries by '|'.
        var entries = DataSkippingStatsColumns.Parse(value);

        Assert.NotNull(entries);
        Assert.Equal(expected, string.Join("|", entries!.Select(e =>
            string.Join(".", e.Select(p => p.Contains('.') || p.Contains('`') || p.Contains(' ') ? $"[{p}]" : p)))));
    }

    [Theory]
    [InlineData("a,,b")]
    [InlineData("a,")]
    [InlineData(",a")]
    [InlineData("a-b")]
    [InlineData("123")]
    [InlineData("`a")]
    [InlineData("a b")]
    [InlineData("a.")]
    [InlineData("a/*+ h */,b")] // a hint, not a comment: Spark stops reading there
    public void Parse_RefusesWhatIsNotAList(string value) => Assert.Null(DataSkippingStatsColumns.Parse(value));

    [Fact]
    public void Format_QuotesAPartOnlyWhenNeeded_AsSparksRename()
    {
        var entries = new List<IReadOnlyList<string>>
        {
            new[] { "plain_1" }, new[] { "my col" }, new[] { "x.y" }, new[] { "a`b" }, new[] { "s", "1a" },
        };

        string formatted = DataSkippingStatsColumns.Format(entries);

        Assert.Equal("plain_1,`my col`,`x.y`,`a``b`,s.`1a`", formatted);
        Assert.Equal(entries, DataSkippingStatsColumns.Parse(formatted));
    }

    // ── RENAME and DROP ─────────────────────────────────────────────────────────────────────────────

    private static Apache.Arrow.Schema TableSchema { get; } = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("payload", StringType.Default, true))
        .Field(new Field("s", new ArrowStructType(
        [
            new Field("x", Int64Type.Default, true),
            new Field("y", Int64Type.Default, true),
        ]), true))
        .Build();

    private async Task<DeltaTable> CreateAsync(string statsColumns) =>
        await DeltaTable.CreateAsync(Fs, TableSchema, columnMappingMode: ColumnMappingMode.Name,
            configuration: new Dictionary<string, string> { [Key] = statsColumns });

    private static string? Property(DeltaTable table) =>
        table.CurrentSnapshot.Metadata.Configuration is { } c && c.TryGetValue(Key, out var v) ? v : null;

    [Fact]
    public async Task DropColumn_RemovesItsEntriesAndThoseUnderIt()
    {
        await using var table = await CreateAsync("id,s.x,payload,s.y");

        await table.DropColumnAsync("s");

        Assert.Equal("id,payload", Property(table));
    }

    [Fact]
    public async Task DropColumn_OfEveryEntry_LeavesTheEmptyList()
    {
        // Spark writes "" (stats on no columns), not an absent property.
        await using var table = await CreateAsync("payload");

        await table.DropColumnAsync("payload");

        Assert.Equal("", Property(table));
    }

    [Fact]
    public async Task DropColumn_OfAnUnlistedColumn_LeavesTheValueAsWritten()
    {
        await using var table = await CreateAsync(" id , s.x ");

        await table.DropColumnAsync("payload");

        Assert.Equal(" id , s.x ", Property(table));
    }

    [Fact]
    public async Task DropField_RemovesTheFieldsEntry_NotItsParents()
    {
        await using var table = await CreateAsync("s.x,s.y,id");

        await table.DropFieldAsync(["s", "x"]);

        Assert.Equal("s.y,id", Property(table));
    }

    [Fact]
    public async Task RenameColumn_RewritesItsEntriesAndThoseUnderIt()
    {
        await using var table = await CreateAsync("s.x,id,s.y");

        await table.RenameColumnAsync("s", "my struct");

        Assert.Equal("`my struct`.x,id,`my struct`.y", Property(table));
        Assert.Equal(
            [["my struct", "x"], ["id"], ["my struct", "y"]],
            DataSkippingStatsColumns.Parse(Property(table)!)!.Select(e => e.ToArray()).ToArray());
    }

    [Fact]
    public async Task RenameField_RewritesTheFieldsEntry_NotItsSibling()
    {
        await using var table = await CreateAsync("s.y,s.x");

        await table.RenameFieldAsync(["s", "x"], "z");

        Assert.Equal("s.y,s.z", Property(table));
    }

    [Fact]
    public async Task RenameField_LeavesAnEntryForItsParent()
    {
        // The entry names s as a whole, which still exists, so Spark leaves it as written.
        await using var table = await CreateAsync("s,id");

        await table.RenameFieldAsync(["s", "x"], "z");

        Assert.Equal("s,id", Property(table));
    }

    [Fact]
    public async Task RenameAndDrop_LeaveATableWithoutTheProperty_WithoutIt()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, TableSchema, columnMappingMode: ColumnMappingMode.Name);

        await table.RenameColumnAsync("payload", "body");
        await table.DropColumnAsync("body");

        Assert.Null(Property(table));
    }

    [Fact]
    public async Task AnUnparseableValue_IsLeftAlone()
    {
        // Spark leaves a value its parser refuses unchanged, too; CREATE refuses one, so this is a table another
        // writer made.
        await using (var created = await CreateAsync("payload"))
        {
        }
        string commit = Path.Combine(_tempDir, "_delta_log", "00000000000000000000.json");
        File.WriteAllText(commit, File.ReadAllText(commit).Replace(
            $"\"{Key}\":\"payload\"", $"\"{Key}\":\"payload,,id\""));
        await using var table = await DeltaTable.OpenAsync(Fs);

        await table.DropColumnAsync("payload");

        Assert.Equal("payload,,id", Property(table));
    }

    [Fact]
    public async Task ComputeDropColumn_CarriesTheRewrite_ForABufferedTransaction()
    {
        await using var table = await CreateAsync("payload,id");

        var change = table.ComputeDropColumn("payload");

        Assert.Equal("id", change.Metadata.Configuration![Key]);
    }

    [Fact]
    public async Task SetSchema_RemovesEntriesTheNewSchemaLacks()
    {
        await using var table = await CreateAsync("id,s.x,payload");

        await table.SetSchemaAsync(new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("S", new ArrowStructType([new Field("X", Int64Type.Default, true)]), true))
            .Build());

        // Resolved as Spark does, case-insensitively.
        Assert.Equal("id,s.x", Property(table));
    }

    [Fact]
    public async Task SetSchema_RemovesEntriesRetypedToATypeWithNoStatistics()
    {
        await using var table = await CreateAsync("id,payload,s.x");

        await table.SetSchemaAsync(new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("payload", BooleanType.Default, true))
            .Field(new Field("s", new ArrowStructType([new Field("x", BooleanType.Default, true)]), true))
            .Build());

        // A nested field of such a type is accepted, as in Spark (which only warns).
        Assert.Equal("id,s.x", Property(table));
    }

    // ── CREATE validation ───────────────────────────────────────────────────────────────────────────

    private static Apache.Arrow.Schema WideSchema { get; } = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int32Type.Default, false))
        .Field(new Field("p", StringType.Default, true))
        .Field(new Field("Name", StringType.Default, true))
        .Field(new Field("flag", BooleanType.Default, true))
        .Field(new Field("s", new ArrowStructType(
        [
            new Field("x", Int32Type.Default, true),
            new Field("y", BooleanType.Default, true),
        ]), true))
        .Field(new Field("arr", new ArrowListType(new Field("element", new ArrowStructType(
            [new Field("e", Int32Type.Default, true)]), true)), true))
        .Field(new Field("dot.ted", Int32Type.Default, true))
        .Build();

    private Task<DeltaTable> CreateWideAsync(string statsColumns) =>
        DeltaTable.CreateAsync(Fs, WideSchema, partitionColumns: ["p"],
            configuration: new Dictionary<string, string> { [Key] = statsColumns }).AsTask();

    [Theory]
    [InlineData("ID,name")]
    [InlineData("s")]
    [InlineData("s.y")]
    [InlineData("arr.element.e")]
    [InlineData("`dot.ted`")]
    [InlineData("")]
    public async Task Create_AcceptsWhatSparkAccepts(string statsColumns)
    {
        await using var table = await CreateWideAsync(statsColumns);

        Assert.Equal(statsColumns, Property(table));
    }

    [Theory]
    [InlineData("P", DeltaTableErrorCodes.DataSkippingPartitionColumn)]
    [InlineData("missing", DeltaTableErrorCodes.ColumnNotFound)]
    [InlineData("dot.ted", DeltaTableErrorCodes.ColumnNotFound)]
    [InlineData("arr.e", DeltaTableErrorCodes.ColumnNotFound)]
    [InlineData("id.x", DeltaTableErrorCodes.ColumnNotFound)]
    [InlineData("flag", DeltaTableErrorCodes.DataSkippingUnsupportedType)]
    [InlineData("arr", DeltaTableErrorCodes.DataSkippingUnsupportedType)]
    [InlineData("id,id", DeltaTableErrorCodes.DuplicateDataSkippingColumns)]
    [InlineData("s.x,s", DeltaTableErrorCodes.DuplicateDataSkippingColumns)]
    [InlineData("a,,b", DeltaTableErrorCodes.InvalidDataSkippingStatsColumns)]
    public async Task Create_RefusesWhatSparkRefuses_WritingNothing(string statsColumns, string code)
    {
        var ex = await Assert.ThrowsAsync<DeltaFormatException>(async () => await CreateWideAsync(statsColumns));

        Assert.Equal(code, ex.ErrorCode);
        Assert.False(Directory.Exists(Path.Combine(_tempDir, "_delta_log"))
            && Directory.EnumerateFiles(Path.Combine(_tempDir, "_delta_log")).Any());
    }

    // Spark lets a wrongly cased entry skip its type and duplicate checks; this checks the column it resolves to,
    // as Spark's stats collection does.
    [Theory]
    [InlineData("FLAG", DeltaTableErrorCodes.DataSkippingUnsupportedType)]
    [InlineData("id,ID", DeltaTableErrorCodes.DuplicateDataSkippingColumns)]
    public async Task Create_ChecksTheResolvedColumn_WhateverTheEntrysCase(string statsColumns, string code)
    {
        var ex = await Assert.ThrowsAsync<DeltaFormatException>(async () => await CreateWideAsync(statsColumns));

        Assert.Equal(code, ex.ErrorCode);
    }
}
