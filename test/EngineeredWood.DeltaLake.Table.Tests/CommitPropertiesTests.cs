// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.IO.Local;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// <see cref="CommitProperties"/>: the caller's own <c>commitInfo</c> metadata (#564). Every committing
/// one-shot method and a transaction record it, history reads <c>userMetadata</c> back, and a key that would
/// shadow, or be shadowed by, a field the commit itself records is refused.
/// </summary>
public class CommitPropertiesTests : IDisposable
{
    private readonly string _tempDir;

    public CommitPropertiesTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_commitprops_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private static readonly Apache.Arrow.Schema TableSchema = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("value", StringType.Default, true))
        .Build();

    private static RecordBatch Rows(params long[] ids)
    {
        var id = new Int64Array.Builder();
        var value = new StringArray.Builder();
        foreach (long i in ids)
        {
            id.Append(i);
            value.Append($"v{i}");
        }
        return new RecordBatch(TableSchema, [id.Build(), value.Build()], ids.Length);
    }

    private static CommitProperties Tagged(string tag) => new() { UserMetadata = tag };

    private static async Task<List<DeltaTable.DeltaHistoryEntry>> HistoryAsync(DeltaTable table)
    {
        var list = new List<DeltaTable.DeltaHistoryEntry>();
        await foreach (var entry in table.GetHistoryAsync())
            list.Add(entry);
        return list;
    }

    private JsonElement CommitInfoAt(long version)
    {
        string path = Path.Combine(_tempDir, "_delta_log", $"{version:D20}.json");
        foreach (string line in File.ReadAllLines(path))
        {
            using var doc = JsonDocument.Parse(line);
            if (doc.RootElement.TryGetProperty("commitInfo", out var info))
                return info.Clone();
        }
        throw new InvalidOperationException($"Version {version} has no commitInfo.");
    }

    /// <summary>
    /// Each committing one-shot method, and a transaction, writes its <c>userMetadata</c> into its own commit,
    /// and history reads it back against the right version. A commit made without properties records none.
    /// </summary>
    [Fact]
    public async Task EveryCommittingMethod_RecordsUserMetadata_AndHistoryReadsItBack()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var options = new DeltaTableOptions { CheckpointInterval = 0 };
        await using var table = await DeltaTable.CreateAsync(
            fs, TableSchema, options, commitProperties: Tagged("create"));

        await table.WriteAsync([Rows(1, 2)], commitProperties: Tagged("append"));
        await table.WriteAsync([Rows(3)]);
        await table.WriteAsync([Rows(1, 2, 3, 4)], DeltaWriteMode.Overwrite, commitProperties: Tagged("overwrite"));
        await table.WriteAsync([Rows(5)]);
        await table.DeleteAsync(Ex.Equal("id", 1L), Tagged("delete"));
        await table.DeleteAsync(Ex.Equal("id", 2L), Tagged("delete-cow"), RowDeleteMode.CopyOnWrite);
        await table.UpdateAsync(
            Ex.Equal("id", 3L), batch => batch, commitProperties: Tagged("update"));
        await table.MergeAsync(
            [Rows(4, 6)],
            new MergeOptions { Keys = ["id"], WhenMatched = MergeAction.Update, WhenNotMatched = MergeAction.Insert },
            commitProperties: Tagged("merge"));
        await table.CompactAsync(commitProperties: Tagged("optimize"));

        await using (var txn = table.StartTransaction())
        {
            txn.CommitProperties = Tagged("transaction");
            await txn.WriteAsync([Rows(7)]);
            await txn.CommitAsync();
        }

        await table.VacuumAsync(TimeSpan.Zero, commitProperties: Tagged("vacuum"));

        var recorded = (await HistoryAsync(table)).Select(h => (h.Operation, h.UserMetadata)).ToList();
        Assert.Equal(
            new (string?, string?)[]
            {
                ("CREATE TABLE", "create"),
                ("WRITE", "append"),
                ("WRITE", null),
                ("WRITE", "overwrite"),
                ("WRITE", null),
                ("DELETE", "delete"),
                ("DELETE", "delete-cow"),
                ("UPDATE", "update"),
                ("MERGE", "merge"),
                ("OPTIMIZE", "optimize"),
                ("WRITE", "transaction"),
                // Both of VACUUM's bookend commits, as Spark's carry the session's userMetadata on both.
                ("VACUUM START", "vacuum"),
                ("VACUUM END", "vacuum"),
            },
            recorded);
    }

    /// <summary>
    /// Extra keys land in commitInfo as given, beside <c>userMetadata</c> and the fields the commit records
    /// itself, which they leave alone.
    /// </summary>
    [Fact]
    public async Task ExtraKeys_AreWrittenAsGiven_BesideTheCommitsOwnFields()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, TableSchema);

        using var watermark = JsonDocument.Parse("""{"source":"orders","offset":42}""");
        long version = await table.WriteAsync([Rows(1)], commitProperties: new CommitProperties
        {
            UserMetadata = "nightly \"run\"",
            Metadata = new Dictionary<string, JsonElement>
            {
                ["jobId"] = JsonDocument.Parse("\"job-7\"").RootElement.Clone(),
                ["watermark"] = watermark.RootElement,
            },
        });

        var info = CommitInfoAt(version);
        Assert.Equal("nightly \"run\"", info.GetProperty("userMetadata").GetString());
        Assert.Equal("job-7", info.GetProperty("jobId").GetString());
        Assert.Equal(42, info.GetProperty("watermark").GetProperty("offset").GetInt32());
        Assert.Equal("WRITE", info.GetProperty("operation").GetString());
        Assert.Equal(JsonValueKind.Number, info.GetProperty("timestamp").ValueKind);
    }

    /// <summary>
    /// Every field of Spark's <c>CommitInfo</c> is refused as an extra key, whether this library writes it or
    /// not: Spark reads each one by type, so a wrongly typed value would break Spark's history of the table.
    /// <c>userMetadata</c> is refused there too, pointing at the property that sets it.
    /// </summary>
    [Theory]
    [InlineData("operation")]
    [InlineData("operationParameters")]
    [InlineData("operationMetrics")]
    [InlineData("timestamp")]
    [InlineData("inCommitTimestamp")]
    [InlineData("isBlindAppend")]
    [InlineData("engineInfo")]
    [InlineData("readVersion")]
    [InlineData("isolationLevel")]
    [InlineData("txnId")]
    [InlineData("tags")]
    [InlineData("userMetadata")]
    public void AReservedKey_IsRefused(string key)
    {
        var ex = Assert.Throws<ArgumentException>(() => new CommitProperties
        {
            Metadata = new Dictionary<string, JsonElement>
            {
                [key] = JsonDocument.Parse("\"x\"").RootElement.Clone(),
            },
        });
        Assert.Contains($"'{key}'", ex.Message);
        if (key == "userMetadata")
            Assert.Contains(nameof(CommitProperties.UserMetadata), ex.Message);
    }

    /// <summary>The keys are copied when set: changing the caller's dictionary afterwards changes nothing.</summary>
    [Fact]
    public void Metadata_IsCopiedWhenSet()
    {
        var source = new Dictionary<string, JsonElement>
        {
            ["jobId"] = JsonDocument.Parse("\"a\"").RootElement.Clone(),
        };
        var properties = new CommitProperties { Metadata = source };
        source["jobId"] = JsonDocument.Parse("\"b\"").RootElement.Clone();
        source["readVersion"] = JsonDocument.Parse("1").RootElement.Clone();

        Assert.Single(properties.Metadata!);
        Assert.Equal("a", properties.Metadata!["jobId"].GetString());
    }

    /// <summary>
    /// Neither the keys nor the reserved set can be changed through a cast, which would get a key past the
    /// validation: a string-valued <c>readVersion</c> in the first case, and every later instance in the second.
    /// </summary>
    [Fact]
    public void TheKeysAndTheReservedSet_CannotBeChangedThroughACast()
    {
        var properties = new CommitProperties
        {
            Metadata = new Dictionary<string, JsonElement>
            {
                ["jobId"] = JsonDocument.Parse("\"a\"").RootElement.Clone(),
            },
        };
        var keys = Assert.IsAssignableFrom<IDictionary<string, JsonElement>>(properties.Metadata);
        Assert.Throws<NotSupportedException>(
            () => keys["readVersion"] = JsonDocument.Parse("\"x\"").RootElement.Clone());

        if (CommitProperties.ReservedKeys is ICollection<string> reserved)
        {
            Assert.True(reserved.IsReadOnly);
            Assert.Throws<NotSupportedException>(() => reserved.Remove("readVersion"));
        }
        Assert.Throws<ArgumentException>(() => new CommitProperties
        {
            Metadata = new Dictionary<string, JsonElement>
            {
                ["readVersion"] = JsonDocument.Parse("\"x\"").RootElement.Clone(),
            },
        });
    }

    /// <summary>
    /// A commit that already carries a commitInfo (a host can pass one through) gets the properties added to it,
    /// and a key both name is refused rather than either side winning.
    /// </summary>
    [Fact]
    public void AnExistingCommitInfo_GetsThePropertiesAdded_AndACollisionIsRefused()
    {
        var existing = new CommitInfo
        {
            Values = new Dictionary<string, JsonElement>
            {
                ["operation"] = JsonDocument.Parse("\"WRITE\"").RootElement.Clone(),
                ["jobId"] = JsonDocument.Parse("\"host\"").RootElement.Clone(),
            },
        };
        IReadOnlyList<DeltaAction> actions = [existing];

        var merged = InCommitTimestamp.EnsureCommitInfo(
            actions, configuration: null, commitProperties: Tagged("run-1"));
        var info = Assert.IsType<CommitInfo>(Assert.Single(merged));
        Assert.Equal("run-1", info.GetValue("userMetadata")!.Value.GetString());
        Assert.Equal("host", info.GetValue("jobId")!.Value.GetString());

        var colliding = new CommitProperties
        {
            Metadata = new Dictionary<string, JsonElement>
            {
                ["jobId"] = JsonDocument.Parse("\"caller\"").RootElement.Clone(),
            },
        };
        var ex = Assert.Throws<ArgumentException>(() => InCommitTimestamp.EnsureCommitInfo(
            actions, configuration: null, commitProperties: colliding));
        Assert.Contains("commitInfo.jobId", ex.Message);
    }
}
