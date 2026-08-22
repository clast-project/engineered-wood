// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Object = Google.Apis.Storage.v1.Data.Object;

namespace EngineeredWood.Tests.IO;

/// <summary>
/// <para>Pins the premise <c>GcsTableFileSystem.ListAsync</c>'s timestamp guard rests on: the Storage
/// SDK's <c>UpdatedDateTimeOffset</c> is not a field, it PARSES the raw JSON on every access, and it
/// rejects timestamps a real server can legitimately send.</para>
///
/// <para>Worth a test of its own because the obvious diagnosis is the wrong one. The rejected values from
/// fake-gcs-server carry a non-<c>Z</c> UTC offset, which makes the offset look like the culprit — it is
/// not. What the parser actually requires is a fractional-second digit count of EXACTLY 0, 3, 6 or 9;
/// <c>-07:00</c> parses perfectly well at six digits. Go's <c>RFC3339Nano</c> trims trailing zeros, so a
/// Go-based emulator emits five digits exactly when the microseconds happen to end in a zero, which is
/// why the failure presents as intermittent rather than constant and why chasing the offset finds
/// nothing.</para>
///
/// <para>The guard's own fallback ordering is not exercised here — reaching it needs a mixed pair
/// (<c>updated</c> unparseable, <c>timeCreated</c> valid) that no server produces, since a server formats
/// both the same way, and the helper is private. See the note on the PR.</para>
/// </summary>
public sealed class GcsTimestampParsingTests
{
    [Theory]
    [InlineData("2026-08-22T14:13:49-07:00", true)]           // 0 digits
    [InlineData("2026-08-22T14:13:49.8-07:00", false)]        // 1
    [InlineData("2026-08-22T14:13:49.85-07:00", false)]       // 2
    [InlineData("2026-08-22T14:13:49.855-07:00", true)]       // 3
    [InlineData("2026-08-22T14:13:49.8557-07:00", false)]     // 4
    [InlineData("2026-08-22T14:13:49.85578-07:00", false)]    // 5  <- what fake-gcs-server sends
    [InlineData("2026-08-22T14:13:49.855788-07:00", true)]    // 6  <- same offset, and it parses
    [InlineData("2026-08-22T14:13:49.8557881-07:00", false)]  // 7
    [InlineData("2026-08-22T14:13:49.855788123-07:00", true)] // 9
    [InlineData("2026-08-22T14:13:49.855788Z", true)]         // 6, with Z
    public void ParsesOnlyZeroThreeSixOrNineFractionalDigits(string raw, bool parses)
    {
        var obj = new Object { UpdatedRaw = raw };

        if (parses)
        {
            Assert.NotNull(obj.UpdatedDateTimeOffset);
            return;
        }

        // Not a null or a sentinel — it THROWS, which is why an unguarded read takes the whole
        // enumeration down rather than degrading one entry's metadata.
        Assert.Throws<FormatException>(() => obj.UpdatedDateTimeOffset);
    }
}
