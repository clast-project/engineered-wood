// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Log;

namespace EngineeredWood.DeltaLake.Tests;

public class InCommitTimestampTests
{
    [Fact]
    public void IsEnabled_True()
    {
        var config = new Dictionary<string, string>
        {
            { InCommitTimestamp.EnableKey, "true" },
        };
        Assert.True(InCommitTimestamp.IsEnabled(config));
    }

    [Fact]
    public void IsEnabled_False()
    {
        Assert.False(InCommitTimestamp.IsEnabled(null));
        Assert.False(InCommitTimestamp.IsEnabled(new Dictionary<string, string>()));
        Assert.False(InCommitTimestamp.IsEnabled(new Dictionary<string, string>
        {
            { InCommitTimestamp.EnableKey, "false" },
        }));
    }

    [Fact]
    public void CreateCommitInfo_HasTimestamp()
    {
        var ci = InCommitTimestamp.CreateCommitInfo();

        var ts = InCommitTimestamp.GetTimestamp(ci);
        Assert.NotNull(ts);
        Assert.True(ts.Value > 0);

        // Should also have operation
        var op = ci.GetValue("operation");
        Assert.NotNull(op);
        Assert.Equal("WRITE", op!.Value.GetString());
    }

    [Fact]
    public void CreateCommitInfo_WithSpecificTimestamp()
    {
        long expected = 1700000000000;
        var ci = InCommitTimestamp.CreateCommitInfo(expected, "DELETE");

        Assert.Equal(expected, InCommitTimestamp.GetTimestamp(ci));
        Assert.Equal("DELETE", ci.GetValue("operation")!.Value.GetString());
    }

    [Fact]
    public void GetTimestampFromActions_FindsIt()
    {
        var actions = new List<DeltaAction>
        {
            InCommitTimestamp.CreateCommitInfo(1700000000000),
            new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 },
        };

        Assert.Equal(1700000000000L, InCommitTimestamp.GetTimestampFromActions(actions));
    }

    [Fact]
    public void GetTimestampFromActions_NoCommitInfo_ReturnsNull()
    {
        var actions = new List<DeltaAction>
        {
            new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 },
        };

        Assert.Null(InCommitTimestamp.GetTimestampFromActions(actions));
    }

    [Fact]
    public void EnsureCommitInfo_WhenEnabled_Prepends()
    {
        var config = new Dictionary<string, string>
        {
            { InCommitTimestamp.EnableKey, "true" },
        };

        var actions = new List<DeltaAction>
        {
            new AddFile
            {
                Path = "file.parquet",
                PartitionValues = new Dictionary<string, string>(),
                Size = 100, ModificationTime = 1000, DataChange = true,
            },
        };

        var result = InCommitTimestamp.EnsureCommitInfo(actions, config, version: 1);

        Assert.Equal(2, result.Count);
        Assert.IsType<CommitInfo>(result[0]); // CommitInfo is first
        Assert.IsType<AddFile>(result[1]);
        Assert.NotNull(InCommitTimestamp.GetTimestampFromActions(result));
    }

    [Fact]
    public void EnsureCommitInfo_WhenDisabled_StillPrependsCommitInfo()
    {
        // commitInfo is written on EVERY commit (operation + timestamp are standard feature-free
        // fields); the inCommitTimestamp field itself is added only when the feature is enabled.
        var actions = new List<DeltaAction>
        {
            new AddFile
            {
                Path = "file.parquet",
                PartitionValues = new Dictionary<string, string>(),
                Size = 100, ModificationTime = 1000, DataChange = true,
            },
        };

        var result = InCommitTimestamp.EnsureCommitInfo(actions, null, version: 1);
        Assert.Equal(2, result.Count);
        var ci = Assert.IsType<CommitInfo>(result[0]);
        Assert.False(ci.Values.ContainsKey("inCommitTimestamp"));
        Assert.IsType<AddFile>(result[1]);
    }

    [Fact]
    public void EnsureCommitInfo_AlreadyPresent_NoChange()
    {
        var config = new Dictionary<string, string>
        {
            { InCommitTimestamp.EnableKey, "true" },
        };

        var actions = new List<DeltaAction>
        {
            InCommitTimestamp.CreateCommitInfo(1700000000000),
            new AddFile
            {
                Path = "file.parquet",
                PartitionValues = new Dictionary<string, string>(),
                Size = 100, ModificationTime = 1000, DataChange = true,
            },
        };

        var result = InCommitTimestamp.EnsureCommitInfo(actions, config, version: 1);
        Assert.Same(actions, result); // No modification needed
    }

    private static CommitInfo CallerCommitInfo(string? txnId)
    {
        var values = new Dictionary<string, JsonElement>
        {
            ["operation"] = JsonDocument.Parse("\"HOST WRITE\"").RootElement.Clone(),
        };
        if (txnId is not null)
            values[InCommitTimestamp.TxnIdKey] = JsonDocument.Parse($"\"{txnId}\"").RootElement.Clone();
        return new CommitInfo { Values = values };
    }

    private static string? TxnIdOf(IReadOnlyList<DeltaAction> actions) =>
        actions.OfType<CommitInfo>().Single().GetValue(InCommitTimestamp.TxnIdKey)?.GetString();

    [Fact]
    public void EnsureCommitInfo_NewCommitInfo_CarriesTheGivenTxnId()
    {
        var result = InCommitTimestamp.EnsureCommitInfo([], null, version: 1, txnId: "given-id");
        Assert.Equal("given-id", TxnIdOf(result));
    }

    [Fact]
    public void EnsureCommitInfo_NewCommitInfo_MintsAUuidWhenNoneIsGiven()
    {
        string? first = TxnIdOf(InCommitTimestamp.EnsureCommitInfo([], null, version: 1));
        string? second = TxnIdOf(InCommitTimestamp.EnsureCommitInfo([], null, version: 1));
        Assert.True(Guid.TryParseExact(first, "D", out _));
        Assert.NotEqual(first, second);
    }

    [Fact]
    public void EnsureCommitInfo_CallersCommitInfo_GainsATxnId()
    {
        // A host that stages its own commitInfo still writes a commit that names its transaction.
        IReadOnlyList<DeltaAction> actions = [CallerCommitInfo(txnId: null)];
        var result = InCommitTimestamp.EnsureCommitInfo(actions, null, version: 1, txnId: "given-id");
        Assert.Equal("given-id", TxnIdOf(result));
        Assert.Equal("HOST WRITE", result.OfType<CommitInfo>().Single().GetValue("operation")?.GetString());
    }

    [Fact]
    public void EnsureCommitInfo_CallersCommitInfo_KeepsItsOwnTxnId()
    {
        IReadOnlyList<DeltaAction> actions = [CallerCommitInfo(txnId: "hosts-own")];
        var result = InCommitTimestamp.EnsureCommitInfo(actions, null, version: 1, txnId: "given-id");
        Assert.Same(actions, result);
        Assert.Equal("hosts-own", TxnIdOf(result));
    }
}
