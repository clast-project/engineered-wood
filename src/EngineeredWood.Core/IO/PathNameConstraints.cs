// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.IO;

/// <summary>
/// <para>Naming rules a storage backend imposes on the path components written under it, declared by
/// <see cref="ITableFileSystem.PathConstraints"/>.</para>
///
/// <para><b>Why this belongs to the filesystem and not to the process.</b> A table format that derives a
/// directory name from data — Hive-style partition directories being the case in point — has to know what
/// the TARGET STORAGE will accept, and the operating system running the writer is only a proxy for that.
/// It is wrong in both directions: a Windows process writing to S3 would restrict itself for a store that
/// accepts everything, and a Linux process writing to a mounted NTFS or SMB volume would restrict itself
/// for nothing and produce a table that cannot be opened on Windows.</para>
///
/// <para><b>These are constraints, not escaping policy.</b> A backend states what it cannot hold; it does
/// not decide what a caller does about it. Honouring a constraint is generally a matter of escaping the
/// offending character, which is a naming decision belonging to the table format — see
/// <c>DeltaPath</c>'s partition-path spelling modes for how Delta resolves it, including the case where a
/// mode deliberately declines to honour a declared constraint in order to stay byte-identical with
/// another implementation.</para>
///
/// <para><b>Length limits are deliberately absent.</b> They are the one restriction escaping cannot
/// absorb — escaping LENGTHENS a name — so they need a different remedy (truncation, hashing) with a
/// different hazard (two names colliding into one path), and they are not modelled here.</para>
/// </summary>
[Flags]
public enum PathNameConstraints
{
    /// <summary>The backend accepts any byte sequence in a path component. All three object stores
    /// measured — Azure Blob, GCS and S3 — are effectively this, modulo the flags below.</summary>
    None = 0,

    /// <summary>
    /// The characters Win32 rejects outright in a path component: <c>&lt; &gt; | : * ? "</c> and
    /// 0x00-0x1F. MEASURED against a real NTFS volume: each raises <c>IOException</c> at create time
    /// rather than being normalised, so a name containing one cannot be written at all.
    /// </summary>
    Win32ReservedCharacters = 1 << 0,

    /// <summary>
    /// Control characters 0x00-0x1F are rejected. Documented by Azure Blob for the whole blob name, and
    /// by GCS for CR and LF specifically. Note that neither Azurite nor fake-gcs-server enforces this, so
    /// it is taken from the vendors' naming rules rather than from a probe.
    /// </summary>
    NoControlCharacters = 1 << 1,

    /// <summary>
    /// <para>A path component may not END with <c>.</c>. Win32 silently STRIPS it — MEASURED: creating
    /// <c>region=a.</c> yields <c>region=a</c>, so the directory a writer opens afterwards is not the one
    /// it asked for — and Azure Blob documents "no path segments should end with a dot", which Azurite
    /// does not enforce.</para>
    ///
    /// <para>This is the one constraint no character set can express, because it is POSITIONAL: <c>.</c>
    /// is unreserved in RFC 3986 and legal mid-name, and every implementation surveyed (Spark, delta-rs,
    /// delta-kernel-rs) therefore leaves it alone and inherits the bug.</para>
    /// </summary>
    NoTrailingDot = 1 << 2,

    /// <summary>
    /// A path component may not END with a space. MEASURED on Win32: the space is stripped at create
    /// time, so the component is created under a different name and the file written beneath it then
    /// fails to open, leaving a stray directory behind.
    /// </summary>
    NoTrailingSpace = 1 << 3,

    /// <summary>
    /// A path component may not be exactly <c>.</c> or <c>..</c>. MEASURED on Win32, where both collapse
    /// as relative-path navigation rather than naming anything; GCS documents the same for an object
    /// named <c>.</c> or <c>..</c>.
    /// </summary>
    NoDotOnlySegments = 1 << 4,

    /// <summary>
    /// Everything a Win32 volume imposes: <see cref="Win32ReservedCharacters"/> plus the three
    /// normalisation rules. This is what <c>LocalTableFileSystem</c> reports when running on Windows, and
    /// what a portable spelling has to satisfy in order to be copyable onto one.
    /// </summary>
    Win32 = Win32ReservedCharacters | NoControlCharacters | NoTrailingDot | NoTrailingSpace
        | NoDotOnlySegments,
}
