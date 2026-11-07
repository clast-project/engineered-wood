// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.DeltaLake.Log;

namespace EngineeredWood.DeltaLake.Tests;

/// <summary>
/// #740: when two listings saw the same log file. Size and ETag can prove a replacement; a modification time can
/// only confirm a match, and an identity that cannot tell is never "the same".
/// </summary>
public class LogFileIdentityTests
{
    private const string Path = "_delta_log/00000000000000000003.json";
    private static readonly DateTimeOffset T1 = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
    private static readonly DateTimeOffset T2 = T1.AddMilliseconds(1);

    private static LogFileMatch Compare(LogFileIdentity a, LogFileIdentity b) => a.Compare(b);

    [Fact]
    public void Same_size_and_time_without_ETags_is_the_same_file() =>
        Assert.Equal(LogFileMatch.Same, Compare(new(Path, 10, T1, null), new(Path, 10, T1, null)));

    [Fact]
    public void A_different_size_is_a_different_file() =>
        Assert.Equal(LogFileMatch.Different, Compare(new(Path, 10, T1, null), new(Path, 11, T1, null)));

    [Fact]
    public void A_different_time_alone_proves_nothing() =>
        Assert.Equal(LogFileMatch.Unknown, Compare(new(Path, 10, T1, null), new(Path, 10, T2, null)));

    [Fact]
    public void ETags_decide_when_both_sides_carry_one()
    {
        Assert.Equal(LogFileMatch.Same, Compare(new(Path, 10, T1, "\"a\""), new(Path, 10, T2, "\"a\"")));
        Assert.Equal(LogFileMatch.Different, Compare(new(Path, 10, T1, "\"a\""), new(Path, 10, T1, "\"b\"")));
    }

    [Fact]
    public void An_ETag_on_one_side_only_cannot_be_compared()
    {
        Assert.Equal(LogFileMatch.Unknown, Compare(new(Path, 10, T1, "\"a\""), new(Path, 10, T1, null)));
        Assert.Equal(LogFileMatch.Unknown, Compare(new(Path, 10, T1, null), new(Path, 10, T1, "\"a\"")));
    }

    [Fact]
    public void Size_still_counts_with_matching_ETags() =>
        Assert.Equal(LogFileMatch.Different, Compare(new(Path, 10, T1, "\"a\""), new(Path, 11, T1, "\"a\"")));

    [Fact]
    public void Unknown_time_and_no_ETag_is_never_the_same_file()
    {
        var unknown = new LogFileIdentity(Path, 10, default, null);
        Assert.Equal(LogFileMatch.Unknown, Compare(unknown, unknown));
    }

    [Fact]
    public void TableFileInfo_keeps_its_three_part_shape()
    {
        // The ETag is an init property, so the constructor and Deconstruct callers compiled against stay valid.
        var info = new EngineeredWood.IO.TableFileInfo(Path, 10, T1) { ETag = "\"a\"" };
        var (path, size, modified) = info;
        Assert.Equal((Path, 10L, T1), (path, size, modified));
        Assert.Equal(LogFileMatch.Same, LogFileIdentity.Of(info).Compare(new(Path, 10, T2, "\"a\"")));
    }

    [Fact]
    public void Different_paths_are_different_files() =>
        Assert.Equal(LogFileMatch.Different,
            Compare(new(Path, 10, T1, "\"a\""), new("_delta_log/00000000000000000004.json", 10, T1, "\"a\"")));
}
