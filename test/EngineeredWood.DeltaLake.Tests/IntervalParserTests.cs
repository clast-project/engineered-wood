// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake.Tests;

/// <summary>
/// Covers the Calendar-interval strings Delta uses for duration table properties. These values arrive
/// from other engines' table metadata, so the parser has to accept what Spark actually writes and
/// refuse everything else — a misparse silently changes how much data VACUUM deletes.
/// </summary>
public class IntervalParserTests
{
    [Theory]
    [InlineData("interval 1 week", 7, 0, 0)]
    [InlineData("interval 7 days", 7, 0, 0)]
    [InlineData("interval 0 seconds", 0, 0, 0)]
    [InlineData("interval 36 hours", 1, 12, 0)]
    [InlineData("interval 90 minutes", 0, 1, 30)]
    // The `interval` keyword is optional in practice.
    [InlineData("2 days", 2, 0, 0)]
    // Singular and plural spellings are interchangeable.
    [InlineData("interval 1 day", 1, 0, 0)]
    [InlineData("interval 1 hour", 0, 1, 0)]
    // Multiple unit/count pairs accumulate.
    [InlineData("interval 1 day 6 hours", 1, 6, 0)]
    public void Parses(string input, int days, int hours, int minutes)
    {
        Assert.True(IntervalParser.TryParse(input, out var actual));
        Assert.Equal(new TimeSpan(days, hours, minutes, 0), actual);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("interval")]
    // A count with no unit is malformed, not a partial success.
    [InlineData("interval 5")]
    [InlineData("5")]
    [InlineData("interval x days")]
    [InlineData("interval 1 fortnight")]
    // Calendar-relative units are refused rather than approximated: converting them to a fixed
    // TimeSpan needs an anchor date, and guessing would be wrong by up to three days a month.
    [InlineData("interval 1 month")]
    [InlineData("interval 1 year")]
    public void Rejects(string? input)
    {
        Assert.False(IntervalParser.TryParse(input, out _));
    }
}
