// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// ADD / DROP CONSTRAINT on an existing table (#565), as Spark 4.0 / Delta 4.0.0 does them (measured, and pinned
/// against Spark by <see cref="Interop.ConstraintDdlInteropTests"/>): names stored lower-cased and matched in any
/// case, a NULL row counted as a violation, the protocol raised to writer 3 or given the feature, the history
/// parameters, and DROP IF EXISTS of a missing constraint still committing.
/// </summary>
public class ConstraintDdlTests : IDisposable
{
    private readonly string _tempDir;

    public ConstraintDdlTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_constraint_ddl_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private LocalTableFileSystem Fs => new(_tempDir);

    private static readonly Apache.Arrow.Schema TableSchema = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, true))
        .Field(new Field("v", StringType.Default, true))
        .Field(new Field("flag", BooleanType.Default, true))
        .Build();

    private static RecordBatch Rows(params long?[] ids)
    {
        var id = new Int64Array.Builder();
        var v = new StringArray.Builder();
        var flag = new BooleanArray.Builder();
        foreach (long? i in ids)
        {
            if (i is { } value) id.Append(value); else id.AppendNull();
            v.Append($"v{i}");
            flag.Append(true);
        }
        return new RecordBatch(TableSchema, [id.Build(), v.Build(), flag.Build()], ids.Length);
    }

    private async Task<DeltaTable> CreateAsync(
        DeltaTableOptions? options = null, bool deletionVectors = false,
        IReadOnlyDictionary<string, string>? configuration = null)
    {
        var table = await DeltaTable.CreateAsync(
            Fs, TableSchema, options ?? new DeltaTableOptions { CheckpointInterval = 0 },
            enableDeletionVectors: deletionVectors, configuration: configuration);
        return table;
    }

    private static async Task<DeltaTable.DeltaHistoryEntry> LastHistoryAsync(DeltaTable table)
    {
        DeltaTable.DeltaHistoryEntry last = default;
        await foreach (var entry in table.GetHistoryAsync())
            last = entry;
        return last;
    }

    private static Dictionary<string, string> Parameters(DeltaTable.DeltaHistoryEntry entry) =>
        JsonDocument.Parse(entry.OperationParameters!).RootElement.EnumerateObject()
            .ToDictionary(p => p.Name, p => p.Value.GetString()!);

    private static async Task<DeltaFormatException> RefusedAsync(string code, Func<Task> action)
    {
        var ex = await Assert.ThrowsAsync<DeltaFormatException>(action);
        Assert.Equal(code, ex.ErrorCode);
        return ex;
    }

    /// <summary>
    /// A constraint every row satisfies is stored lower-cased, raises a legacy writer-2 protocol to 3, records
    /// Spark's history parameters, and refuses a later write that violates it.
    /// </summary>
    [Fact]
    public async Task AddingASatisfiedConstraint_DeclaresItAndEnforcesIt()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Rows(1, 2, 3)]);
        Assert.Equal((1, 2), (table.CurrentSnapshot.Protocol.MinReaderVersion, table.CurrentSnapshot.Protocol.MinWriterVersion));

        long version = await table.AddConstraintAsync("Positive", "id > 0");

        var snapshot = table.CurrentSnapshot;
        Assert.Equal(version, snapshot.Version);
        Assert.Equal("id > 0", snapshot.Metadata.Configuration!["delta.constraints.positive"]);
        Assert.Equal((1, 3, null), (snapshot.Protocol.MinReaderVersion, snapshot.Protocol.MinWriterVersion,
            snapshot.Protocol.WriterFeatures));

        var entry = await LastHistoryAsync(table);
        Assert.Equal("ADD CONSTRAINT", entry.Operation);
        Assert.Equal(new Dictionary<string, string> { ["name"] = "Positive", ["expr"] = "id > 0" }, Parameters(entry));

        await RefusedAsync(DeltaTableErrorCodes.ConstraintViolated, async () => await table.WriteAsync([Rows(-1)]));
        await table.WriteAsync([Rows(4)]);
    }

    /// <summary>On a table-features protocol the feature is named, and the reader side is left as it was.</summary>
    [Fact]
    public async Task OnATableFeaturesProtocol_TheFeatureIsAdded()
    {
        await using var table = await CreateAsync(deletionVectors: true);
        await table.WriteAsync([Rows(1)]);
        var before = table.CurrentSnapshot.Protocol;
        Assert.DoesNotContain("checkConstraints", before.WriterFeatures!);

        await table.AddConstraintAsync("pos", "id > 0");

        var after = table.CurrentSnapshot.Protocol;
        Assert.Equal((3, 7), (after.MinReaderVersion, after.MinWriterVersion));
        Assert.Equal(before.ReaderFeatures, after.ReaderFeatures);
        Assert.Equal([.. before.WriterFeatures!, "checkConstraints"], after.WriterFeatures);
    }

    /// <summary>
    /// Rows that violate it refuse the constraint with their count, a NULL among them (Spark counts 2 here), and
    /// nothing is committed.
    /// </summary>
    [Fact]
    public async Task ViolatingRows_RefuseTheConstraint_CountingNull()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Rows(1, 2)]);
        await table.WriteAsync([Rows(-3, null)]);
        long before = table.CurrentSnapshot.Version;

        var ex = await RefusedAsync(DeltaTableErrorCodes.NewCheckConstraintViolation,
            async () => await table.AddConstraintAsync("pos", "id > 0"));
        Assert.StartsWith("2 rows", ex.Message);

        await table.RefreshAsync();
        Assert.Equal(before, table.CurrentSnapshot.Version);
        Assert.Null(table.CurrentSnapshot.Metadata.Configuration?.Keys.FirstOrDefault(k => k.StartsWith("delta.constraints.")));
    }

    /// <summary>A row a deletion vector hides is not the table's, so it does not violate.</summary>
    [Fact]
    public async Task ARowADeletionVectorHides_DoesNotViolate()
    {
        await using var table = await CreateAsync(deletionVectors: true);
        await table.WriteAsync([Rows(1, -2, 3)]);
        await table.DeleteAsync(Expressions.Expressions.Equal("id", -2L));
        Assert.NotNull(table.CurrentSnapshot.ActiveFiles.Values.Single().DeletionVector);

        await table.AddConstraintAsync("pos", "id > 0");
    }

    [Fact]
    public async Task AnExistingName_InAnyCase_IsRefused()
    {
        await using var table = await CreateAsync();
        await table.AddConstraintAsync("MyCheck", "v IS NOT NULL");

        var ex = await RefusedAsync(DeltaTableErrorCodes.ConstraintAlreadyExists,
            async () => await table.AddConstraintAsync("mycheck", "id IS NULL OR id IS NOT NULL"));
        Assert.Contains("'mycheck'", ex.Message);
        Assert.Contains("v IS NOT NULL", ex.Message);
    }

    /// <summary>
    /// What Spark refuses before reading a row: a column the table lacks, a value that is not a boolean, and here
    /// also SQL this library cannot parse. A boolean column on its own is a boolean expression.
    /// </summary>
    [Theory]
    [InlineData("nope > 0", DeltaTableErrorCodes.UnresolvedColumn)]
    [InlineData("id + 1", DeltaTableErrorCodes.NonBooleanCheckConstraint)]
    [InlineData("v", DeltaTableErrorCodes.NonBooleanCheckConstraint)]
    [InlineData("id >", DeltaTableErrorCodes.UnevaluableTableExpression)]
    public async Task AnExpressionThatCannotBeAConstraint_IsRefused(string expression, string code)
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Rows(1)]);
        long before = table.CurrentSnapshot.Version;

        await RefusedAsync(code, async () => await table.AddConstraintAsync("c", expression));
        Assert.Equal(before, table.CurrentSnapshot.Version);
    }

    [Theory]
    [InlineData("flag")]
    [InlineData("ID > 0")]
    [InlineData("1 = 1")]
    public async Task ABooleanExpression_IsAccepted(string expression)
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Rows(1, 2)]);

        await table.AddConstraintAsync("c", expression);
        Assert.Equal(expression, table.CurrentSnapshot.Metadata.Configuration!["delta.constraints.c"]);
    }

    /// <summary>
    /// DROP matches the name in any case, removes the constraint so a row it would refuse is written, records
    /// Spark's parameters, and leaves the protocol alone.
    /// </summary>
    [Fact]
    public async Task Drop_RemovesTheConstraint_AndLeavesTheProtocol()
    {
        await using var table = await CreateAsync();
        await table.AddConstraintAsync("MyCheck", "id > 0");
        var protocol = table.CurrentSnapshot.Protocol;

        await table.DropConstraintAsync("MYCHECK");

        Assert.DoesNotContain("delta.constraints.mycheck", table.CurrentSnapshot.Metadata.Configuration!.Keys);
        Assert.Equal(protocol, table.CurrentSnapshot.Protocol);
        var entry = await LastHistoryAsync(table);
        Assert.Equal("DROP CONSTRAINT", entry.Operation);
        Assert.Equal(new Dictionary<string, string> { ["name"] = "MYCHECK", ["expr"] = "id > 0", ["existed"] = "true" },
            Parameters(entry));

        await table.WriteAsync([Rows(-1)]);
    }

    /// <summary>
    /// Dropping a constraint the table lacks is refused, unless ifExists, when it commits without changing the
    /// metadata, as Spark does.
    /// </summary>
    [Fact]
    public async Task DroppingAMissingConstraint_IsRefused_UnlessIfExists()
    {
        await using var table = await CreateAsync();
        long before = table.CurrentSnapshot.Version;

        await RefusedAsync(DeltaTableErrorCodes.ConstraintDoesNotExist,
            async () => await table.DropConstraintAsync("nothere"));
        Assert.Equal(before, table.CurrentSnapshot.Version);

        long version = await table.DropConstraintAsync("nothere", ifExists: true);
        Assert.Equal(before + 1, version);
        var entry = await LastHistoryAsync(table);
        Assert.Equal(new Dictionary<string, string> { ["name"] = "nothere", ["existed"] = "false" }, Parameters(entry));
    }

    /// <summary>
    /// A constraint given at creation keeps the case it was given; ADD sees it as taken, and DROP finds it.
    /// </summary>
    [Fact]
    public async Task AConstraintGivenAtCreation_IsFoundInAnyCase()
    {
        await using var table = await CreateAsync(configuration: new Dictionary<string, string>
        {
            ["delta.constraints.Positive"] = "id > 0",
        });

        await RefusedAsync(DeltaTableErrorCodes.ConstraintAlreadyExists,
            async () => await table.AddConstraintAsync("positive", "id > 1"));
        await table.DropConstraintAsync("POSITIVE");
        Assert.DoesNotContain(table.CurrentSnapshot.Metadata.Configuration!.Keys, k => k.StartsWith("delta.constraints."));
    }

    /// <summary>
    /// A constraint given in the configuration at creation declares <c>checkConstraints</c>: writer version 3 on a
    /// legacy protocol, and the named feature on a table-features one. It used to declare nothing.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CreatingWithAConstraint_DeclaresTheFeature(bool deletionVectors)
    {
        await using var table = await CreateAsync(
            deletionVectors: deletionVectors,
            configuration: new Dictionary<string, string> { ["delta.constraints.pos"] = "id > 0" });

        var protocol = table.CurrentSnapshot.Protocol;
        if (deletionVectors)
            Assert.Contains("checkConstraints", protocol.WriterFeatures!);
        else
            Assert.Equal((1, 3), (protocol.MinReaderVersion, protocol.MinWriterVersion));
    }

    /// <summary>
    /// ADD reads the whole table, so a commit that lands after its read and adds data conflicts, a blind append
    /// included: those rows were never checked. A stale handle's ADD here is raced by an append through another.
    /// </summary>
    [Fact]
    public async Task AConcurrentBlindAppend_ConflictsWithAdd()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Rows(1)]);
        await using var stale = await DeltaTable.OpenAsync(Fs);

        await table.WriteAsync([Rows(-5)]);

        await Assert.ThrowsAsync<DeltaConflictException>(async () => await stale.AddConstraintAsync("pos", "id > 0"));
        await table.RefreshAsync();
        Assert.DoesNotContain(table.CurrentSnapshot.Metadata.Configuration?.Keys ?? [], k => k.StartsWith("delta.constraints."));
    }

    /// <summary>
    /// Under <see cref="DeltaTableOptions.TrustStatisticsProofs"/> a file whose statistics prove the constraint is
    /// not read. Shown by deleting that file's parquet: the ADD still succeeds with the option, and fails reading
    /// without it. A file with a NULL in the column is never proven.
    /// </summary>
    [Theory]
    [InlineData(true, false, true)]
    [InlineData(false, false, false)]
    [InlineData(true, true, false)]
    public async Task TrustStatisticsProofs_SkipsAProvenFile(bool trust, bool withNull, bool succeeds)
    {
        await using var table = await CreateAsync(new DeltaTableOptions { CheckpointInterval = 0, TrustStatisticsProofs = trust });
        await table.WriteAsync([withNull ? Rows(5, null) : Rows(5, 6)]);
        string proven = table.CurrentSnapshot.ActiveFiles.Keys.Single();
        await table.WriteAsync([Rows(1)]);
        File.Delete(Path.Combine(_tempDir, proven));

        if (succeeds)
        {
            await table.AddConstraintAsync("pos", "id > 0");
        }
        else
        {
            var ex = await Record.ExceptionAsync(async () => await table.AddConstraintAsync("pos", "id > 0"));
            Assert.NotNull(ex);
            Assert.IsNotType<DeltaFormatException>(ex);
        }
    }

    /// <summary>On a column-mapped table the constraint names the logical column, and the read resolves it.</summary>
    [Fact]
    public async Task OnAColumnMappedTable_TheLogicalNameIsChecked()
    {
        await using var table = await DeltaTable.CreateAsync(
            Fs, TableSchema, new DeltaTableOptions { CheckpointInterval = 0 },
            columnMappingMode: Schema.ColumnMappingMode.Name);
        await table.WriteAsync([Rows(1, -2)]);

        await RefusedAsync(DeltaTableErrorCodes.NewCheckConstraintViolation,
            async () => await table.AddConstraintAsync("pos", "id > 0"));
        await table.AddConstraintAsync("small", "id < 10");
    }
}
