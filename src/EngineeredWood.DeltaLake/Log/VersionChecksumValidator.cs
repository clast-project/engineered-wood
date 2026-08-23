// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;
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
    /// <summary>Rendered values longer than this are elided; schema strings are unbounded.</summary>
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
            fields.Add(new VersionChecksumFieldComparison
            {
                Field = "inCommitTimestampOpt",
                Outcome = VersionChecksumFieldOutcome.Disagrees,
                Recorded = null,
                Reconstructed = reconstructed.InCommitTimestamp is { } value ? Number(value) : null,
            });
            return;
        }

        if (reconstructed.InCommitTimestamp is null)
        {
            fields.Add(new VersionChecksumFieldComparison
            {
                Field = "inCommitTimestampOpt",
                Outcome = VersionChecksumFieldOutcome.NotReconstructed,
                Recorded = Number(recorded.InCommitTimestamp.Value),
            });
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
            fields.Add(new VersionChecksumFieldComparison
            {
                Field = "metadata.schemaString",
                Outcome = VersionChecksumFieldOutcome.Agrees,
                Recorded = Elide(recorded),
                Reconstructed = Elide(actual),
            });
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

        fields.Add(new VersionChecksumFieldComparison
        {
            Field = "metadata.schemaString",
            Outcome = equivalent
                ? VersionChecksumFieldOutcome.Agrees
                : VersionChecksumFieldOutcome.Disagrees,
            Recorded = Elide(recorded),
            Reconstructed = Elide(actual),
        });
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
            fields.Add(new VersionChecksumFieldComparison
            {
                Field = "setTransactions",
                Outcome = VersionChecksumFieldOutcome.NotRecorded,
                Reconstructed = $"{actual.Count} live transaction(s)",
            });
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
            fields.Add(new VersionChecksumFieldComparison
            {
                Field = "domainMetadata",
                Outcome = VersionChecksumFieldOutcome.NotRecorded,
                Reconstructed = $"{actual.Count} live domain(s)",
            });
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
                byDomain.TryGetValue(domain, out var left) ? left.Configuration : null,
                actual.TryGetValue(domain, out var right) ? right.Configuration : null);
        }
    }

    /// <summary>
    /// Records one field. Both values are already rendered; null means the side has nothing there, and
    /// two nulls agree — an absent value is a value, and both sides saying "absent" is the two of them
    /// saying the same thing.
    /// </summary>
    /// <remarks>
    /// The comparison runs on the FULL rendered values and only what is stored for display is elided, so
    /// a long value that differs past the cut is still reported as differing.
    /// </remarks>
    private static void Compare(
        List<VersionChecksumFieldComparison> fields, string name, string? recorded, string? actual)
    {
        fields.Add(new VersionChecksumFieldComparison
        {
            Field = name,
            Outcome = string.Equals(recorded, actual, StringComparison.Ordinal)
                ? VersionChecksumFieldOutcome.Agrees
                : VersionChecksumFieldOutcome.Disagrees,
            Recorded = recorded is null ? null : Elide(recorded),
            Reconstructed = actual is null ? null : Elide(actual),
        });
    }

    private static IEnumerable<string> Union(
        IEnumerable<string> left, IEnumerable<string> right) =>
        left.Concat(right).Distinct(StringComparer.Ordinal).OrderBy(static k => k, StringComparer.Ordinal);

    private static string Number(long value) => value.ToString(CultureInfo.InvariantCulture);

    private static string? Text(string? value) => value is null ? null : $"\"{value}\"";

    /// <summary>An ordered list, rendered in order — <c>partitionColumns</c> is ordered state.</summary>
    private static string Sequence(IReadOnlyList<string> values) =>
        $"[{string.Join(", ", values)}]";

    /// <summary>
    /// A feature list, rendered as a SET: order is not part of what a protocol says, and null and empty
    /// both say "no features". Rendering both as <c>[]</c> is what makes them compare equal.
    /// </summary>
    private static string FeatureSet(IReadOnlyList<string>? values) =>
        values is null or []
            ? "[]"
            : $"[{string.Join(", ", values.OrderBy(static v => v, StringComparer.Ordinal))}]";

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
            .Select(static kvp => $"{kvp.Key}={kvp.Value}");
        return $"{{{string.Join(", ", pairs)}}}";
    }

    /// <summary>
    /// Shortens a long value for display. Applied to the RENDERED form only — the comparison always sees
    /// the whole value, so eliding can hide what differs but can never decide that nothing does.
    /// </summary>
    private static string Elide(string value) =>
        value.Length <= MaxRenderedLength
            ? value
            : $"{value[..MaxRenderedLength]}… ({value.Length} chars)";
}
