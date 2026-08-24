// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;
using System.Text;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.IO;

namespace EngineeredWood.DeltaLake.Log;

/// <summary>
/// Compares a reconstructed <see cref="Snapshot.Snapshot"/> against the
/// <c>_delta_log/&lt;version&gt;.crc</c> written beside its version, and reports what disagrees.
///
/// <para>This is the half of version-checksum support that pays for itself immediately. The other half —
/// serving state OUT of a checksum to skip work — trusts the file; this one only ever uses it as a second
/// opinion, so a wrong checksum costs a report rather than a wrong answer. The two cannot share a
/// default, which is why they are separate surfaces and this one is the one that exists.</para>
/// </summary>
/// <remarks>
/// <para><b>What is compared, and what a difference means.</b> Strings are compared exactly, because the
/// difference this catches is precisely the one that is easy to call cosmetic: <c>metaData.name</c>
/// written as <c>""</c> where the log says <c>null</c> is what made delta-spark refuse a table this
/// library had written, and no amount of "they mean the same thing" made the read succeed. Collections
/// are compared leniently in one direction only — an absent collection and an empty one are the same
/// statement about a table with none (<c>configuration</c>, <c>readerFeatures</c>, format options), and
/// treating them as different would flag every Spark-written checksum, since Spark's model has no null
/// there to begin with.</para>
///
/// <para><b>Ordering.</b> <c>partitionColumns</c> is compared IN ORDER, because it is; configuration
/// maps, protocol feature lists, app transactions and domain metadata are compared as sets, because a
/// writer's enumeration order is not part of the state.</para>
///
/// <para><b>An absent field is not an agreeing field.</b> <c>setTransactions</c> and
/// <c>domainMetadata</c> are optional, and a writer that omits one is saying "I am not telling you", not
/// "there are none". Those come back as <see cref="VersionChecksumFieldOutcome.NotRecorded"/> and never
/// as agreement, so a caller cannot mistake a silent checksum for a confirming one. Getting this wrong is
/// how a reader concludes that a committed idempotent write never happened and runs it twice.</para>
/// </remarks>
public sealed class VersionChecksumValidator
{
    /// <summary>
    /// How much of a value the report shows. Elision is a DISPLAY bound and never a comparison one: the
    /// comparison always runs on the whole value, and where two long values differ past this length the
    /// window moves to the difference rather than cutting it away — see <see cref="Compared"/>. Without
    /// that, two schemas differing in their last field would be reported as disagreeing and then shown as
    /// two identical prefixes, which reads as a bug in the validator.
    /// </summary>
    private const int MaxRenderedLength = 400;

    private readonly ITableFileSystem _fs;

    /// <param name="fileSystem">The table's filesystem, rooted at the table directory.</param>
    public VersionChecksumValidator(ITableFileSystem fileSystem)
    {
        _fs = fileSystem ?? throw new ArgumentNullException(nameof(fileSystem));
    }

    /// <summary>
    /// Reads the checksum for <paramref name="snapshot"/>'s version and compares the two.
    /// </summary>
    /// <remarks>
    /// Never throws for a table that has no checksum, an unreadable one, or one that disagrees — all
    /// three are outcomes, not failures, and the caller decides what they are worth. Cancellation and a
    /// null argument still throw.
    /// </remarks>
    public async ValueTask<VersionChecksumValidation> ValidateAsync(
        Snapshot.Snapshot snapshot, CancellationToken cancellationToken = default)
    {
        if (snapshot is null)
            throw new ArgumentNullException(nameof(snapshot));

        byte[] json;
        try
        {
            json = await _fs.ReadAllBytesAsync(
                DeltaVersion.ChecksumPath(snapshot.Version), cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
        {
            // Absent and unfetchable are the same answer. The spec makes the file optional and requires
            // readers to cope without it, so a checksum we cannot get hold of leaves us exactly where
            // every version without one already leaves us — but the reason is kept, because "the store
            // refused us" and "nobody wrote one" look identical from here and only one is worth chasing.
            return new VersionChecksumValidation
            {
                Version = snapshot.Version,
                Outcome = VersionChecksumValidationOutcome.Absent,
                Reason = ex.Message,
            };
        }

        VersionChecksum recorded;
        try
        {
            recorded = VersionChecksumSerializer.Deserialize(json, snapshot.Version);
        }
        catch (DeltaFormatException ex)
        {
            return new VersionChecksumValidation
            {
                Version = snapshot.Version,
                Outcome = VersionChecksumValidationOutcome.Unreadable,
                Reason = ex.Message,
            };
        }

        return Compare(recorded, snapshot);
    }

    /// <summary>
    /// Compares an already-read checksum against the snapshot at the same version.
    /// </summary>
    /// <exception cref="ArgumentException"><paramref name="recorded"/> and
    /// <paramref name="reconstructed"/> are at different versions. Two versions of a table are SUPPOSED
    /// to differ, so comparing them answers nothing; catching it here keeps a caller from reading a real
    /// difference as drift.</exception>
    public static VersionChecksumValidation Compare(
        VersionChecksum recorded, Snapshot.Snapshot reconstructed)
    {
        if (recorded is null)
            throw new ArgumentNullException(nameof(recorded));
        if (reconstructed is null)
            throw new ArgumentNullException(nameof(reconstructed));

        if (recorded.Version != reconstructed.Version)
        {
            throw new ArgumentException(
                $"Version checksum is for version {recorded.Version} but the snapshot is at version " +
                $"{reconstructed.Version}; a checksum only describes the version it is named for.",
                nameof(recorded));
        }

        var fields = new List<VersionChecksumFieldComparison>();

        long tableSizeBytes = 0;
        foreach (var add in reconstructed.ActiveFiles.Values)
            tableSizeBytes += add.Size;

        Compare(fields, "tableSizeBytes", Number(recorded.TableSizeBytes), Number(tableSizeBytes));
        Compare(fields, "numFiles", Number(recorded.NumFiles), Number(reconstructed.ActiveFiles.Count));

        CompareInCommitTimestamp(fields, recorded, reconstructed);
        CompareMetadata(fields, recorded.Metadata, reconstructed.Metadata);
        CompareProtocol(fields, recorded.Protocol, reconstructed.Protocol);
        CompareSetTransactions(fields, recorded.SetTransactions, reconstructed.AppTransactions);
        CompareDomainMetadata(fields, recorded.DomainMetadata, reconstructed.DomainMetadata);

        bool disagrees = fields.Exists(
            static f => f.Outcome == VersionChecksumFieldOutcome.Disagrees);

        return new VersionChecksumValidation
        {
            Version = recorded.Version,
            Outcome = disagrees
                ? VersionChecksumValidationOutcome.Disagrees
                : VersionChecksumValidationOutcome.Agrees,
            Fields = fields,
        };
    }

    /// <summary>
    /// The in-commit timestamp, which is present if and only if the feature is enabled — so its ABSENCE
    /// carries meaning on both sides and neither absence is automatically a fault.
    /// </summary>
    /// <remarks>
    /// A snapshot bootstrapped from a checkpoint has no in-commit timestamp at all: a checkpoint holds no
    /// <c>commitInfo</c>, which is where the timestamp lives. That is the ordinary state of an ordinary
    /// read, not drift, so it is reported as
    /// <see cref="VersionChecksumFieldOutcome.NotReconstructed"/> — the recorded value is very likely
    /// correct and we simply have nothing to hold it against. With the feature OFF the field is not part
    /// of the state and is not reported at all; a snapshot may still carry a timestamp read off some
    /// earlier <c>commitInfo</c>, and comparing against a field the writer was right to omit would
    /// manufacture a difference out of a correctly written file.
    /// </remarks>
    private static void CompareInCommitTimestamp(
        List<VersionChecksumFieldComparison> fields,
        VersionChecksum recorded,
        Snapshot.Snapshot reconstructed)
    {
        bool ictEnabled = InCommitTimestamp.IsEnabled(recorded.Metadata.Configuration)
            || InCommitTimestamp.IsEnabled(reconstructed.Metadata.Configuration);

        if (!ictEnabled && recorded.InCommitTimestamp is null)
            return;

        if (recorded.InCommitTimestamp is null)
        {
            // Enabled, and the writer left out a field the spec makes mandatory when it is on. That is a
            // statement about the checksum rather than about the table, and it is worth reporting as a
            // difference: something wrote this file believing the feature was off.
            fields.Add(Compared(
                "inCommitTimestampOpt",
                VersionChecksumFieldOutcome.Disagrees,
                recorded: null,
                actual: reconstructed.InCommitTimestamp is { } value ? Number(value) : null));
            return;
        }

        if (reconstructed.InCommitTimestamp is null)
        {
            fields.Add(Compared(
                "inCommitTimestampOpt",
                VersionChecksumFieldOutcome.NotReconstructed,
                recorded: Number(recorded.InCommitTimestamp.Value),
                actual: null));
            return;
        }

        Compare(fields, "inCommitTimestampOpt",
            Number(recorded.InCommitTimestamp.Value),
            Number(reconstructed.InCommitTimestamp.Value));
    }

    private static void CompareMetadata(
        List<VersionChecksumFieldComparison> fields, MetadataAction recorded, MetadataAction actual)
    {
        Compare(fields, "metadata.id", Text(recorded.Id), Text(actual.Id));
        Compare(fields, "metadata.name", Text(recorded.Name), Text(actual.Name));
        Compare(fields, "metadata.description", Text(recorded.Description), Text(actual.Description));
        Compare(fields, "metadata.format.provider",
            Text(recorded.Format.Provider), Text(actual.Format.Provider));
        Compare(fields, "metadata.format.options",
            Map(recorded.Format.Options), Map(actual.Format.Options));
        CompareSchema(fields, recorded.SchemaString, actual.SchemaString);
        Compare(fields, "metadata.partitionColumns",
            Sequence(recorded.PartitionColumns), Sequence(actual.PartitionColumns));
        Compare(fields, "metadata.configuration",
            Map(recorded.Configuration), Map(actual.Configuration));
        Compare(fields, "metadata.createdTime",
            recorded.CreatedTime is { } a ? Number(a) : null,
            actual.CreatedTime is { } b ? Number(b) : null);
    }

    /// <summary>
    /// The schema, compared by what it MEANS rather than by how it was spelled.
    /// </summary>
    /// <remarks>
    /// <c>schemaString</c> is JSON embedded in a JSON string, so two engines can describe the same schema
    /// with different key order or whitespace and produce different bytes. Byte equality is tried first
    /// because it is the common case and free; only when it fails is each side parsed and re-serialized
    /// canonically, and a difference reported only if THAT differs. A side that will not parse falls back
    /// to the raw comparison — an unparseable schema is a real problem, and reporting it as a difference
    /// is closer to right than silently passing it.
    /// </remarks>
    private static void CompareSchema(
        List<VersionChecksumFieldComparison> fields, string recorded, string actual)
    {
        if (string.Equals(recorded, actual, StringComparison.Ordinal))
        {
            fields.Add(Compared(
                "metadata.schemaString", VersionChecksumFieldOutcome.Agrees, recorded, actual));
            return;
        }

        bool equivalent = false;
        try
        {
            equivalent = string.Equals(
                DeltaSchemaSerializer.Serialize(DeltaSchemaSerializer.Parse(recorded)),
                DeltaSchemaSerializer.Serialize(DeltaSchemaSerializer.Parse(actual)),
                StringComparison.Ordinal);
        }
        catch (Exception)
        {
            // Leave it as a difference; see the remarks.
        }

        fields.Add(Compared(
            "metadata.schemaString",
            equivalent ? VersionChecksumFieldOutcome.Agrees : VersionChecksumFieldOutcome.Disagrees,
            recorded,
            actual));
    }

    private static void CompareProtocol(
        List<VersionChecksumFieldComparison> fields, ProtocolAction recorded, ProtocolAction actual)
    {
        Compare(fields, "protocol.minReaderVersion",
            Number(recorded.MinReaderVersion), Number(actual.MinReaderVersion));
        Compare(fields, "protocol.minWriterVersion",
            Number(recorded.MinWriterVersion), Number(actual.MinWriterVersion));
        Compare(fields, "protocol.readerFeatures",
            FeatureSet(recorded.ReaderFeatures), FeatureSet(actual.ReaderFeatures));
        Compare(fields, "protocol.writerFeatures",
            FeatureSet(recorded.WriterFeatures), FeatureSet(actual.WriterFeatures));
    }

    private static void CompareSetTransactions(
        List<VersionChecksumFieldComparison> fields,
        IReadOnlyList<TransactionId>? recorded,
        IReadOnlyDictionary<string, TransactionId> actual)
    {
        if (recorded is null)
        {
            fields.Add(Compared(
                "setTransactions",
                VersionChecksumFieldOutcome.NotRecorded,
                recorded: null,
                actual: $"{actual.Count} live transaction(s)"));
            return;
        }

        var byApp = new Dictionary<string, TransactionId>(StringComparer.Ordinal);
        foreach (var txn in recorded)
            byApp[txn.AppId] = txn;

        foreach (string appId in Union(byApp.Keys, actual.Keys))
        {
            Compare(fields, $"setTransactions[{appId}]",
                byApp.TryGetValue(appId, out var left) ? Render(left) : null,
                actual.TryGetValue(appId, out var right) ? Render(right) : null);
        }

        static string Render(TransactionId txn) =>
            $"version={Number(txn.Version)}, lastUpdated=" +
            (txn.LastUpdated is { } at ? Number(at) : "(absent)");
    }

    /// <summary>
    /// Live domain metadata. Tombstones are dropped from the recorded side first: the field is defined as
    /// the live set, a snapshot holds only live domains, and a writer that includes a <c>removed</c>
    /// entry anyway is saying the same thing the snapshot's absence says.
    /// </summary>
    private static void CompareDomainMetadata(
        List<VersionChecksumFieldComparison> fields,
        IReadOnlyList<DomainMetadata>? recorded,
        IReadOnlyDictionary<string, DomainMetadata> actual)
    {
        if (recorded is null)
        {
            fields.Add(Compared(
                "domainMetadata",
                VersionChecksumFieldOutcome.NotRecorded,
                recorded: null,
                actual: $"{actual.Count} live domain(s)"));
            return;
        }

        var byDomain = new Dictionary<string, DomainMetadata>(StringComparer.Ordinal);
        foreach (var domain in recorded)
        {
            if (!domain.Removed)
                byDomain[domain.Domain] = domain;
        }

        foreach (string domain in Union(byDomain.Keys, actual.Keys))
        {
            Compare(fields, $"domainMetadata[{domain}]",
                byDomain.TryGetValue(domain, out var left) ? Text(left.Configuration) : null,
                actual.TryGetValue(domain, out var right) ? Text(right.Configuration) : null);
        }
    }

    /// <summary>
    /// Records one field. Both values are already rendered; null means the side has nothing there, and
    /// two nulls agree — an absent value is a value, and both sides saying "absent" is the two of them
    /// saying the same thing.
    /// </summary>
    private static void Compare(
        List<VersionChecksumFieldComparison> fields, string name, string? recorded, string? actual)
    {
        fields.Add(Compared(
            name,
            string.Equals(recorded, actual, StringComparison.Ordinal)
                ? VersionChecksumFieldOutcome.Agrees
                : VersionChecksumFieldOutcome.Disagrees,
            recorded,
            actual));
    }

    /// <summary>
    /// Builds one comparison from an already-decided outcome, eliding the two values for display.
    /// </summary>
    /// <remarks>
    /// <para>Every comparison this type produces goes through here, which is what keeps the eliding rule
    /// in one place. The decision itself is always made on the WHOLE value — by
    /// <see cref="Compare(List{VersionChecksumFieldComparison}, string, string, string)"/> before it calls this, or by <see cref="CompareSchema"/>, which decides equivalence by parsing both
    /// sides — so no amount of eliding can turn a difference into agreement.</para>
    ///
    /// <para>Two DIFFERING values are elided around their first difference rather than from the start.
    /// Cutting from the start is right when a value is merely long, and wrong the moment two long values
    /// share a prefix: a <c>schemaString</c> pair differing in their last field would be reported as
    /// disagreeing and then shown as two identical 400-character prefixes, which reads as the validator
    /// having lost its mind. That is the case that actually arises, because a schema string is the one
    /// field here routinely longer than the window.</para>
    /// </remarks>
    private static VersionChecksumFieldComparison Compared(
        string field, VersionChecksumFieldOutcome outcome, string? recorded, string? actual)
    {
        int from = outcome == VersionChecksumFieldOutcome.Disagrees
            && recorded is not null && actual is not null
            ? WindowStart(recorded, actual)
            : 0;

        return new VersionChecksumFieldComparison
        {
            Field = field,
            Outcome = outcome,
            Recorded = recorded is null ? null : Elide(recorded, from),
            Reconstructed = actual is null ? null : Elide(actual, from),
        };
    }

    /// <summary>
    /// Where to start showing two differing values so that what differs is on screen: far enough back
    /// from the first differing character to carry some context, and 0 whenever both values fit anyway.
    /// </summary>
    private static int WindowStart(string recorded, string actual)
    {
        if (recorded.Length <= MaxRenderedLength && actual.Length <= MaxRenderedLength)
            return 0;

        int shared = 0;
        int common = Math.Min(recorded.Length, actual.Length);
        while (shared < common && recorded[shared] == actual[shared])
            shared++;

        // Both values are windowed from the SAME offset. Windows taken from different offsets would line
        // up on screen while describing different parts of the value, which is a worse way to be wrong
        // than truncation is.
        return Math.Max(0, shared - (MaxRenderedLength / 4));
    }

    private static IEnumerable<string> Union(
        IEnumerable<string> left, IEnumerable<string> right) =>
        left.Concat(right).Distinct(StringComparer.Ordinal).OrderBy(static k => k, StringComparer.Ordinal);

    private static string Number(long value) => value.ToString(CultureInfo.InvariantCulture);

    /// <summary>
    /// A string value, quoted and escaped so that what it CONTAINS cannot be read as the report's own
    /// punctuation.
    /// </summary>
    /// <remarks>
    /// A table name holding a newline would otherwise split one field across two lines of
    /// <see cref="VersionChecksumValidation.Describe"/>, whose whole shape is one line per field, and one
    /// holding a quote would leave a reader unable to see where the value ended. Both are exactly the
    /// sort of value that turns up in a disagreement, since the fields most likely to differ are the
    /// free-text ones.
    /// </remarks>
    private static string? Text(string? value) => value is null ? null : $"\"{Escape(value)}\"";

    /// <summary>An ordered list, rendered in order — <c>partitionColumns</c> is ordered state.</summary>
    private static string Sequence(IReadOnlyList<string> values) =>
        $"[{string.Join(", ", values.Select(Escape))}]";

    /// <summary>
    /// A feature list, rendered as a SET: order is not part of what a protocol says, and null and empty
    /// both say "no features". Rendering both as <c>[]</c> is what makes them compare equal.
    /// </summary>
    private static string FeatureSet(IReadOnlyList<string>? values) =>
        values is null or []
            ? "[]"
            : $"[{string.Join(", ", values.OrderBy(static v => v, StringComparer.Ordinal).Select(Escape))}]";

    /// <summary>
    /// A configuration map, rendered key-sorted. Null and empty render alike for the same reason as
    /// <see cref="FeatureSet"/>, and for one more: this library models an empty configuration as null
    /// while delta-spark models it as an empty map, so distinguishing them here would report a difference
    /// on every Spark-written checksum of a table with no properties.
    /// </summary>
    private static string Map(IReadOnlyDictionary<string, string>? values)
    {
        if (values is null || values.Count == 0)
            return "{}";

        var pairs = values
            .OrderBy(static kvp => kvp.Key, StringComparer.Ordinal)
            .Select(static kvp => $"{Escape(kvp.Key)}={Text(kvp.Value)}");
        return $"{{{string.Join(", ", pairs)}}}";
    }

    /// <summary>
    /// Shortens a value for display: at most <see cref="MaxRenderedLength"/> characters starting at
    /// <paramref name="from"/>, saying so whenever anything was left out.
    /// </summary>
    /// <remarks>
    /// The window backs off a surrogate pair at either end. Splitting one leaves a lone surrogate that
    /// renders as a replacement character in the middle of a value a reader is being asked to compare by
    /// eye — which looks like the difference being reported.
    /// </remarks>
    private static string Elide(string value, int from)
    {
        int start = Math.Min(from, value.Length);
        if (start > 0 && start < value.Length && char.IsLowSurrogate(value[start]))
            start--;

        int length = Math.Min(MaxRenderedLength, value.Length - start);
        if (length > 0 && char.IsHighSurrogate(value[start + length - 1]))
            length--;

        if (start == 0 && length == value.Length)
            return value;

        string head = start > 0
            ? $"…(from char {start.ToString(CultureInfo.InvariantCulture)}) "
            : "";
        string tail = start + length < value.Length
            ? $"… ({value.Length.ToString(CultureInfo.InvariantCulture)} chars)"
            : "";

        return $"{head}{value.Substring(start, length)}{tail}";
    }

    /// <summary>
    /// Escapes what would otherwise be read as the report's own punctuation: backslashes, quotes, and the
    /// control characters — a newline above all, which would break the one-line-per-field shape
    /// <see cref="VersionChecksumValidation.Describe"/> is read through.
    /// </summary>
    private static string Escape(string value)
    {
        int first = -1;
        for (int i = 0; i < value.Length; i++)
        {
            if (value[i] is '\\' or '"' || char.IsControl(value[i]))
            {
                first = i;
                break;
            }
        }

        // Nothing to escape is the overwhelmingly common case, and it keeps its own string.
        if (first < 0)
            return value;

        var escaped = new StringBuilder(value.Length + 8);
        escaped.Append(value, 0, first);

        for (int i = first; i < value.Length; i++)
        {
            char c = value[i];
            switch (c)
            {
                case '\\': escaped.Append(@"\\"); break;
                case '"': escaped.Append("\\\""); break;
                case '\n': escaped.Append(@"\n"); break;
                case '\r': escaped.Append(@"\r"); break;
                case '\t': escaped.Append(@"\t"); break;
                default:
                    if (char.IsControl(c))
                    {
                        escaped.Append(@"\u")
                            .Append(((int)c).ToString("x4", CultureInfo.InvariantCulture));
                    }
                    else
                    {
                        escaped.Append(c);
                    }

                    break;
            }
        }

        return escaped.ToString();
    }
}
