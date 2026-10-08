// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// #473: a UniForm table (<c>delta.universalFormat.enabledFormats</c>) promises Iceberg readers metadata that tracks
/// it, which its writer regenerates after each commit. This library has no converter, and UniForm is no writer
/// feature, so every write used to commit and leave those readers on an older version without a word. Writes are
/// now refused unless the caller opts in.
/// </summary>
public class UniversalFormatWriteTests : IDisposable
{
    private readonly string _tempDir;

    public UniversalFormatWriteTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_uniform_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private static readonly Apache.Arrow.Schema Schema = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, true))
        .Field(new Field("name", StringType.Default, true))
        .Build();

    private static readonly DeltaTableOptions OptIn = new() { AllowWritesWithoutUniversalFormatConversion = true };

    private static Dictionary<string, string> UniForm(string formats = "iceberg") => new()
    {
        [IcebergCompat.EnableV2Key] = "true",
        [UniversalFormat.EnabledFormatsKey] = formats,
    };

    private static RecordBatch Rows(params long[] ids) => new(Schema,
    [
        new Int64Array.Builder().AppendRange(ids).Build(),
        new StringArray.Builder().AppendRange(ids.Select(i => $"n{i}")).Build(),
    ], ids.Length);

    // A UniForm table as another engine would have made it, with one data file in it.
    private async Task<LocalTableFileSystem> CreateUniFormTableAsync()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(
            fs, Schema, OptIn, columnMappingMode: ColumnMappingMode.Name, configuration: UniForm());
        await table.WriteAsync([Rows(1, 2)]);
        return fs;
    }

    private int ParquetFileCount() =>
        Directory.EnumerateFiles(_tempDir, "*.parquet", SearchOption.AllDirectories).Count();

    [Fact]
    public async Task Create_WithUniForm_IsRefused_AndWritesNothing()
    {
        var error = await Assert.ThrowsAsync<DeltaFormatException>(async () =>
            await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), Schema,
                columnMappingMode: ColumnMappingMode.Name, configuration: UniForm()));

        Assert.Equal(DeltaTableErrorCodes.UniversalFormatNotMaintained, error.ErrorCode);
        Assert.Contains(UniversalFormat.EnabledFormatsKey, error.Message);
        Assert.Contains(nameof(DeltaTableOptions.AllowWritesWithoutUniversalFormatConversion), error.Message);
        Assert.False(Directory.Exists(Path.Combine(_tempDir, "_delta_log")));
    }

    public static TheoryData<string> Operations() =>
    [
        "append", "overwrite", "delete", "update", "add column", "rename column", "drop column", "set schema",
        "clustering", "domain metadata", "compact", "write data files", "transaction",
    ];

    [Theory]
    [MemberData(nameof(Operations))]
    public async Task EveryWrite_ToAUniFormTable_IsRefused_BeforeWritingAnything(string operation)
    {
        var fs = await CreateUniFormTableAsync();
        await using var table = await DeltaTable.OpenAsync(fs);
        long version = table.CurrentSnapshot.Version;
        int files = ParquetFileCount();

        var error = await Assert.ThrowsAsync<DeltaFormatException>(async () =>
        {
            switch (operation)
            {
                case "append": await table.WriteAsync([Rows(3)]); break;
                case "overwrite": await table.WriteAsync([Rows(3)], DeltaWriteMode.Overwrite); break;
                case "delete": await table.DeleteAsync(b => new BooleanArray.Builder().AppendRange(
                    Enumerable.Repeat(true, b.Length)).Build()); break;
                case "update": await table.UpdateAsync(b => new BooleanArray.Builder().AppendRange(
                    Enumerable.Repeat(true, b.Length)).Build(), b => b); break;
                case "add column": await table.AddColumnAsync(new Field("extra", Int32Type.Default, true)); break;
                case "rename column": await table.RenameColumnAsync("name", "label"); break;
                case "drop column": await table.DropColumnAsync("name"); break;
                case "set schema": await table.SetSchemaAsync(new Apache.Arrow.Schema.Builder()
                    .Field(new Field("other", Int64Type.Default, true)).Build()); break;
                case "clustering": await table.SetClusteringColumnsAsync(["id"]); break;
                case "domain metadata": await table.SetDomainMetadataAsync("ew.test", "{}"); break;
                case "compact": await table.CompactAsync(); break;
                case "write data files": await table.WriteDataFilesAsync([Rows(3)]); break;
                case "transaction":
                {
                    // StageActions has no entry check of its own; the commit is gated.
                    await using var txn = table.StartTransaction();
                    txn.StageActions([new TransactionId { AppId = "ew.test", Version = 1, LastUpdated = 0 }]);
                    await txn.CommitAsync();
                    break;
                }
                default: throw new ArgumentOutOfRangeException(nameof(operation));
            }
        });

        Assert.Equal(DeltaTableErrorCodes.UniversalFormatNotMaintained, error.ErrorCode);
        await using var reopened = await DeltaTable.OpenAsync(fs);
        Assert.Equal(version, reopened.CurrentSnapshot.Version);
        Assert.Equal(files, ParquetFileCount());
    }

    [Fact]
    public async Task CommitDataFiles_ToATableThatBecameUniForm_IsRefused()
    {
        // Files written while the table was not UniForm may not be committed to it once it is. (The external
        // write path refuses any IcebergCompat table, which UniForm requires, so this is the only way in.)
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, Schema, columnMappingMode: ColumnMappingMode.Name);
        var written = await table.WriteDataFilesAsync([Rows(1)]);
        await using (var replaced = await DeltaTable.CreateOrReplaceAsync(
            fs, Schema, [], OptIn, columnMappingMode: ColumnMappingMode.Name, configuration: UniForm()))
        {
        }
        await table.RefreshAsync();

        var error = await Assert.ThrowsAsync<DeltaFormatException>(async () =>
            await table.CommitDataFilesAsync(written));
        Assert.Equal(DeltaTableErrorCodes.UniversalFormatNotMaintained, error.ErrorCode);
    }

    [Theory]
    [InlineData("clustering extraActions", false)]
    [InlineData("commit data files extraActions", false)]
    [InlineData("staged action", false)]
    [InlineData("clustering extraActions", true)]
    [InlineData("commit data files extraActions", true)]
    [InlineData("staged action", true)]
    public async Task ACallerMetadataThatEnablesUniForm_IsRefused(string seam, bool optIn)
    {
        // The table is not UniForm yet; the caller's own metaData turns it on. That commit would already be one no
        // converter saw, so it is refused like a commit to a table that already is.
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, Schema, optIn ? OptIn : null,
            columnMappingMode: ColumnMappingMode.Name,
            configuration: new Dictionary<string, string> { [IcebergCompat.EnableV2Key] = "true" });
        var current = table.CurrentSnapshot.Metadata;
        var enabling = current with
        {
            Configuration = current.Configuration!
                .Append(new KeyValuePair<string, string>(UniversalFormat.EnabledFormatsKey, "iceberg"))
                .ToDictionary(kv => kv.Key, kv => kv.Value),
        };
        long version = table.CurrentSnapshot.Version;

        async Task Commit()
        {
            switch (seam)
            {
                case "clustering extraActions":
                    await table.SetClusteringColumnsAsync(null, extraActions: [enabling]);
                    break;
                case "commit data files extraActions":
                    await table.CommitDataFilesAsync([], extraActions: [enabling], operation: "SET TBLPROPERTIES");
                    break;
                case "staged action":
                {
                    await using var txn = table.StartTransaction();
                    txn.StageActions([enabling]);
                    await txn.CommitAsync();
                    break;
                }
            }
        }

        if (optIn)
        {
            await Commit();
            Assert.Equal(["iceberg"],
                UniversalFormat.GetEnabledFormats(table.CurrentSnapshot.Metadata.Configuration));
            return;
        }

        var error = await Assert.ThrowsAsync<DeltaFormatException>(Commit);
        Assert.Equal(DeltaTableErrorCodes.UniversalFormatNotMaintained, error.ErrorCode);
        await using var reopened = await DeltaTable.OpenAsync(fs);
        Assert.Equal(version, reopened.CurrentSnapshot.Version);
    }

    [Fact]
    public async Task ReadsCheckpointsAndVacuum_AreNotRefused()
    {
        var fs = await CreateUniFormTableAsync();
        await using var table = await DeltaTable.OpenAsync(fs);

        long rows = 0;
        await foreach (var batch in table.ReadAllAsync())
            rows += batch.Length;
        Assert.Equal(2, rows);
        await table.CheckpointAsync();
        await table.VacuumAsync(dryRun: true);
    }

    [Fact]
    public async Task WithTheOptIn_WritesCommit()
    {
        var fs = await CreateUniFormTableAsync();
        await using var table = await DeltaTable.OpenAsync(fs, OptIn);

        await table.WriteAsync([Rows(3)]);
        await table.AddColumnAsync(new Field("extra", Int32Type.Default, true));

        Assert.Equal(3, table.CurrentSnapshot.Version);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Replace_OfAUniFormTable_IsRefused_EvenOneThatDropsIt(bool keepUniForm)
    {
        // Dropping the property would leave Iceberg readers on the old data for good, and a REPLACE carries no
        // configuration over, so the caller may not even know the table was UniForm. Empty: no batches to gate.
        var fs = await CreateUniFormTableAsync();
        var configuration = keepUniForm
            ? UniForm()
            : new Dictionary<string, string> { [IcebergCompat.EnableV2Key] = "true" };

        var error = await Assert.ThrowsAsync<DeltaFormatException>(async () =>
            await DeltaTable.CreateOrReplaceAsync(
                fs, Schema, [], columnMappingMode: ColumnMappingMode.Name, configuration: configuration));
        Assert.Equal(DeltaTableErrorCodes.UniversalFormatNotMaintained, error.ErrorCode);
    }

    [Fact]
    public async Task Replace_OfAUniFormTable_WithTheOptIn_IsAllowed()
    {
        var fs = await CreateUniFormTableAsync();

        await using var replaced = await DeltaTable.CreateOrReplaceAsync(
            fs, Schema, [Rows(9)], OptIn, columnMappingMode: ColumnMappingMode.Name,
            configuration: new Dictionary<string, string> { [IcebergCompat.EnableV2Key] = "true" });

        Assert.Empty(UniversalFormat.GetEnabledFormats(replaced.CurrentSnapshot.Metadata.Configuration));
    }

    [Theory]
    [InlineData(null, new string[0])]
    [InlineData("", new string[0])]
    [InlineData("iceberg", new[] { "iceberg" })]
    [InlineData("iceberg,hudi", new[] { "iceberg", "hudi" })]
    [InlineData("iceberg, hudi", new[] { "iceberg", " hudi" })] // Spark does not trim (and would refuse this)
    public void GetEnabledFormats_ParsesAsSparkDoes(string? value, string[] expected)
    {
        var configuration = value is null
            ? new Dictionary<string, string>()
            : new Dictionary<string, string> { [UniversalFormat.EnabledFormatsKey] = value };

        Assert.Equal(expected, UniversalFormat.GetEnabledFormats(configuration));
    }

    [Fact]
    public async Task AnEmptyValue_IsNotUniForm()
    {
        await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), Schema,
            columnMappingMode: ColumnMappingMode.Name, configuration: UniForm(formats: ""));

        await table.WriteAsync([Rows(1)]);
    }
}
