// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Lance.Format;

namespace EngineeredWood.Lance.Tests.Format;

public class OffsetSizeTableTests
{
    [Fact]
    public void ParseTable_DecodesEntries()
    {
        var entries = new[]
        {
            new OffsetSizeEntry(0, 64),
            new OffsetSizeEntry(100, 200),
            new OffsetSizeEntry(500, 16),
        };

        byte[] buf = new byte[entries.Length * OffsetSizeEntry.Bytes];
        for (int i = 0; i < entries.Length; i++)
            entries[i].WriteTo(buf.AsSpan(i * OffsetSizeEntry.Bytes, OffsetSizeEntry.Bytes));

        OffsetSizeEntry[] parsed = OffsetSizeEntry.ParseTable(buf, entries.Length);
        Assert.Equal(entries, parsed);
    }

    [Fact]
    public void ParseTable_EmptyTable_Returns_Empty()
    {
        OffsetSizeEntry[] parsed = OffsetSizeEntry.ParseTable(Array.Empty<byte>(), 0);
        Assert.Empty(parsed);
    }

    [Fact]
    public void ParseTable_Truncated_Throws()
    {
        byte[] buf = new byte[OffsetSizeEntry.Bytes - 1];
        Assert.Throws<LanceFormatException>(() => OffsetSizeEntry.ParseTable(buf, 1));
    }
}
