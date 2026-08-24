// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text;

namespace EngineeredWood.DeltaLake.Log;

/// <summary>What comparing one field of a version checksum against a reconstructed snapshot found.</summary>
public enum VersionChecksumFieldOutcome
{
    /// <summary>The checksum and the snapshot say the same thing.</summary>
    Agrees,

    /// <summary>They say different things. Both values are carried on the comparison.</summary>
    Disagrees,

    /// <summary>
    /// The checksum does not record this field, so there was nothing to compare against. NOT the same as
    /// agreement: the writer declined to say, and only the log can answer.
    /// </summary>
    NotRecorded,

    /// <summary>
    /// The snapshot cannot supply this field, so there was nothing to compare. The recorded value may be
    /// perfectly good — this side is the one that came up empty.
    /// </summary>
    NotReconstructed,
}

/// <summary>
/// One field's worth of a <see cref="VersionChecksumValidation"/>: what the <c>.crc</c> said, what the
/// snapshot said, and whether those are the same.
/// </summary>
/// <remarks>
/// Both values are kept, rendered, and labelled by SOURCE. That is the whole point of the type. The
/// failure this comparison exists to prevent is delta-spark's <c>DELTA_STATE_RECOVER_ERROR</c>, which
/// fires on exactly this comparison and then names none of it — "the metadata of your Delta table could
/// not be recovered ... did you manually delete files in the _delta_log directory?", which sends the
/// reader hunting for a deleted log file when what actually happened was that one writer wrote <c>""</c>
/// where another wrote <c>null</c>.
/// </remarks>
public sealed record VersionChecksumFieldComparison
{
    /// <summary>
    /// The field, named as it is spelled ON DISK — <c>numFiles</c>, <c>metadata.name</c>,
    /// <c>setTransactions[my-app]</c> — so the report can be read against the <c>.crc</c> itself.
    /// </summary>
    public required string Field { get; init; }

    /// <summary>What the comparison found.</summary>
    public required VersionChecksumFieldOutcome Outcome { get; init; }

    /// <summary>The value the checksum recorded, rendered for display; null when it recorded none.</summary>
    public string? Recorded { get; init; }

    /// <summary>The value the snapshot holds, rendered for display; null when it has none.</summary>
    public string? Reconstructed { get; init; }

    /// <summary>One line naming the field, both values, and which side each came from.</summary>
    public override string ToString() => Outcome switch
    {
        VersionChecksumFieldOutcome.Disagrees =>
            $"{Field}: the checksum says {Recorded ?? "(absent)"}, " +
            $"the snapshot says {Reconstructed ?? "(absent)"}",
        VersionChecksumFieldOutcome.NotRecorded =>
            $"{Field}: not recorded by the checksum; the snapshot says {Reconstructed ?? "(absent)"}",
        VersionChecksumFieldOutcome.NotReconstructed =>
            $"{Field}: the checksum says {Recorded ?? "(absent)"}; the snapshot does not carry it",
        _ => $"{Field}: agrees ({Recorded ?? "(absent)"})",
    };
}

/// <summary>Whether a version could be validated against a checksum at all, and what came of it.</summary>
public enum VersionChecksumValidationOutcome
{
    /// <summary>
    /// There is no checksum to validate against. The spec makes the file optional and requires every
    /// reader to cope with its absence, so this is an ordinary answer and not a failure. Any failure to
    /// READ one lands here too: a checksum that cannot be fetched is worth exactly as much as one that
    /// was never written.
    /// </summary>
    Absent,

    /// <summary>
    /// A file is there and it is not a checksum this library can parse. Distinct from
    /// <see cref="Absent"/> on purpose — "nobody wrote one" and "somebody wrote one we cannot read" are
    /// different facts about a table, and collapsing them hides the second forever.
    /// </summary>
    Unreadable,

    /// <summary>Every field that could be compared agreed.</summary>
    Agrees,

    /// <summary>At least one field disagreed.</summary>
    Disagrees,
}

/// <summary>
/// The result of checking a reconstructed <see cref="Snapshot.Snapshot"/> against the
/// <c>_delta_log/&lt;version&gt;.crc</c> written beside that version.
///
/// <para>A checksum is an independent second copy of the state at a version, produced by whoever wrote
/// the commit. Comparing it against what we rebuild is the cheapest way to notice the two have drifted,
/// and it catches OUR bugs as readily as anyone else's — the coercion in <c>CheckpointWriter</c> that
/// wrote absent optional fields as <c>""</c> and <c>0</c> took a Spark round trip to find, and this
/// comparison names it in a unit test.</para>
/// </summary>
/// <remarks>
/// <para><b>This reports; it does not decide.</b> A disagreement can reasonably throw, be ignored in
/// favour of the log, or be surfaced without failing the read, and which one is right belongs to the
/// caller rather than to the comparison. So nothing here throws on a mismatch and nothing here runs by
/// default; what it guarantees is that a caller who does throw can say what disagreed and where each
/// value came from.</para>
/// <para><b>A checksum is not verified by anything.</b> Nothing signs one and nothing cross-checks it, so
/// a disagreement means the two sources differ — NOT that the snapshot is the wrong one. The log is the
/// table; the checksum is a claim about it.</para>
/// </remarks>
public sealed record VersionChecksumValidation
{
    /// <summary>The version that was validated.</summary>
    public required long Version { get; init; }

    /// <summary>Whether validation happened, and what it found.</summary>
    public required VersionChecksumValidationOutcome Outcome { get; init; }

    /// <summary>
    /// Every field considered, agreeing ones included — the full account of what this validation actually
    /// covered. Empty when the checksum was <see cref="VersionChecksumValidationOutcome.Absent"/> or
    /// <see cref="VersionChecksumValidationOutcome.Unreadable"/>.
    /// </summary>
    public IReadOnlyList<VersionChecksumFieldComparison> Fields { get; init; } = [];

    /// <summary>
    /// Why the checksum could not be used, when the outcome is
    /// <see cref="VersionChecksumValidationOutcome.Absent"/> or
    /// <see cref="VersionChecksumValidationOutcome.Unreadable"/>; null otherwise.
    /// </summary>
    public string? Reason { get; init; }

    /// <summary>The fields that disagreed.</summary>
    public IEnumerable<VersionChecksumFieldComparison> Disagreements =>
        Fields.Where(static f => f.Outcome == VersionChecksumFieldOutcome.Disagrees);

    /// <summary>
    /// The fields one side or the other could not supply, and which therefore prove nothing. A caller
    /// treating validation as coverage should read this: a checksum that records no
    /// <c>setTransactions</c> has not confirmed the app transactions, it has stayed silent about them.
    /// </summary>
    public IEnumerable<VersionChecksumFieldComparison> Unchecked =>
        Fields.Where(static f => f.Outcome is VersionChecksumFieldOutcome.NotRecorded
            or VersionChecksumFieldOutcome.NotReconstructed);

    /// <summary>
    /// True when nothing disagreed. Note that an <see cref="VersionChecksumValidationOutcome.Absent"/>
    /// checksum satisfies this: there was no second opinion, which is not the same as a matching one.
    /// Use <see cref="Outcome"/> where that difference matters.
    /// </summary>
    public bool IsConsistent => Outcome != VersionChecksumValidationOutcome.Disagrees;

    /// <summary>
    /// A multi-line account suitable for a test failure or an exception message: what disagreed, both
    /// values, and which side each came from.
    /// </summary>
    public string Describe()
    {
        var text = new StringBuilder();

        switch (Outcome)
        {
            case VersionChecksumValidationOutcome.Absent:
                text.Append($"Version {Version} has no version checksum to validate against");
                if (Reason is not null)
                    text.Append($" ({Reason})");
                text.Append('.');
                return text.ToString();

            case VersionChecksumValidationOutcome.Unreadable:
                return $"The version checksum for version {Version} could not be parsed: {Reason}";

            case VersionChecksumValidationOutcome.Agrees:
                int agreed = Fields.Count(static f => f.Outcome == VersionChecksumFieldOutcome.Agrees);
                text.Append(
                    $"The version checksum for version {Version} agrees with the reconstructed snapshot " +
                    $"on {agreed} field(s).");
                break;

            default:
                text.Append(
                    $"The version checksum for version {Version} disagrees with the reconstructed " +
                    "snapshot:");
                foreach (var field in Disagreements)
                    text.Append(Environment.NewLine).Append("  ").Append(field);
                break;
        }

        bool headed = false;
        foreach (var field in Unchecked)
        {
            if (!headed)
            {
                text.Append(Environment.NewLine).Append("Not compared:");
                headed = true;
            }

            text.Append(Environment.NewLine).Append("  ").Append(field);
        }

        return text.ToString();
    }
}
