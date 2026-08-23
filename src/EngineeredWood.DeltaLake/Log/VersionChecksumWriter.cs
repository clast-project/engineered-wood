// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.IO;

namespace EngineeredWood.DeltaLake.Log;

/// <summary>What a version-checksum write did.</summary>
public enum VersionChecksumWriteResult
{
    /// <summary>The <c>&lt;version&gt;.crc</c> file was created.</summary>
    Written,

    /// <summary>
    /// A checksum for this version already existed and was left alone. Not an error: the spec forbids
    /// overwriting one, and a concurrent writer describing the same version describes the same state.
    /// </summary>
    AlreadyExists,

    /// <summary>
    /// The snapshot could not describe its own version faithfully, so nothing was written rather than
    /// something wrong. See <see cref="VersionChecksum.TryFromSnapshot"/> for the one case.
    /// </summary>
    NotDescribable,

    /// <summary>
    /// Serializing or storing the file failed. The table is unharmed — the commit this describes is
    /// already durable and a missing checksum is the state of every version without one.
    /// </summary>
    Failed,
}

/// <summary>
/// Writes <c>_delta_log/&lt;version&gt;.crc</c> version-checksum files from a snapshot.
///
/// <para>A checksum is a pure function of a <see cref="Snapshot.Snapshot"/> and is named for the version
/// that snapshot is at — exactly like a checkpoint, and for the same reason. That is what makes the
/// contract statable in one line: <b>the file describes the version it is named for.</b> Not "the version
/// the caller just committed", which under a concurrent writer can be older than the snapshot in hand;
/// hand this the state you have and it names the file correctly.</para>
/// </summary>
/// <remarks>
/// <para><b>Never throws, cancellation excepted.</b> Every caller reaches this AFTER a commit has become
/// durable, and turning a housekeeping failure into a commit failure would report a failure that did not
/// happen — the same trade <see cref="LogCleanup"/> makes, and the same one delta-spark makes by running
/// its post-commit hooks inside a catch. The outcome is reported through the returned
/// <see cref="VersionChecksumWriteResult"/> instead of thrown.</para>
///
/// <para><b>Create-if-absent, never overwrite.</b> "Writers MUST NOT overwrite existing Version Checksum
/// files" is a spec requirement, and <see cref="ITableFileSystem.TryWriteAllBytesAsync"/> is the same
/// one-request primitive the commit itself is built on, so the rule costs nothing to honour.</para>
///
/// <para><b>What is not written.</b> <c>fileSizeHistogram</c> is omitted. It is optional, this library
/// has no consumer for it, and the two ecosystem writers disagree about its NAME — delta-spark emits it
/// as <c>histogramOpt</c>, which is not what the spec calls it, and delta-kernel-rs reads either but
/// rejects a file carrying both. Adding ours to that is a compatibility question, not a fill-in-a-field
/// question, so it waits for a reason. The other optional fields are listed on
/// <see cref="VersionChecksum"/>.</para>
/// </remarks>
public sealed class VersionChecksumWriter
{
    private readonly ITableFileSystem _fs;

    /// <param name="fileSystem">The table's filesystem, rooted at the table directory.</param>
    public VersionChecksumWriter(ITableFileSystem fileSystem)
    {
        _fs = fileSystem ?? throw new ArgumentNullException(nameof(fileSystem));
    }

    /// <summary>
    /// Writes the checksum describing <paramref name="snapshot"/>'s version, unless one already exists
    /// there or the snapshot cannot describe it.
    /// </summary>
    public ValueTask<VersionChecksumWriteResult> TryWriteAsync(
        Snapshot.Snapshot snapshot, CancellationToken cancellationToken = default)
    {
        if (snapshot is null)
            throw new ArgumentNullException(nameof(snapshot));

        var checksum = VersionChecksum.TryFromSnapshot(snapshot);
        return checksum is null
            ? new ValueTask<VersionChecksumWriteResult>(VersionChecksumWriteResult.NotDescribable)
            : TryWriteAsync(checksum, cancellationToken);
    }

    /// <summary>
    /// Writes an already-built checksum at the version it names. For a caller that assembled one itself;
    /// the snapshot overload is what the commit paths use.
    /// </summary>
    public async ValueTask<VersionChecksumWriteResult> TryWriteAsync(
        VersionChecksum checksum, CancellationToken cancellationToken = default)
    {
        if (checksum is null)
            throw new ArgumentNullException(nameof(checksum));

        try
        {
            byte[] json = VersionChecksumSerializer.Serialize(checksum);
            bool written = await _fs.TryWriteAllBytesAsync(
                DeltaVersion.ChecksumPath(checksum.Version), json, cancellationToken)
                .ConfigureAwait(false);

            return written
                ? VersionChecksumWriteResult.Written
                : VersionChecksumWriteResult.AlreadyExists;
        }
        catch (Exception) when (!cancellationToken.IsCancellationRequested)
        {
            return VersionChecksumWriteResult.Failed;
        }
    }

    /// <summary>
    /// Reads the checksum for <paramref name="version"/>, or null when there is none or it cannot be
    /// parsed.
    /// </summary>
    /// <remarks>
    /// Nothing in this library READS a checksum to shortcut work yet — a snapshot is still built by
    /// replaying the log — so this exists for callers that want to validate a version against a recorded
    /// summary, and for the tests that prove what is written round-trips. An unparseable checksum reads
    /// as an absent one for the same reason the spec makes the file optional: every version without one
    /// already works.
    /// </remarks>
    public async ValueTask<VersionChecksum?> TryReadAsync(
        long version, CancellationToken cancellationToken = default)
    {
        try
        {
            byte[] json = await _fs.ReadAllBytesAsync(
                DeltaVersion.ChecksumPath(version), cancellationToken).ConfigureAwait(false);
            return VersionChecksumSerializer.Deserialize(json, version);
        }
        catch (Exception) when (!cancellationToken.IsCancellationRequested)
        {
            return null;
        }
    }
}
