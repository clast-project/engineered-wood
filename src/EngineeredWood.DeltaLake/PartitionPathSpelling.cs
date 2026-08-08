// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake;

/// <summary>
/// <para>How <see cref="DeltaPath.BuildPartitionPath"/> spells a Hive-style partition directory. The two
/// modes make DIFFERENT PROMISES, and there is no spelling that keeps both.</para>
///
/// <para><b>Neither mode can corrupt a partition value, and choosing between them is not a data
/// decision.</b> MEASURED: nothing anywhere recovers a partition value from a directory name — readers take
/// values from <c>add.partitionValues</c> and locate files through <c>add.path</c> — and Spark on Windows,
/// pointed at a table whose directories were written on POSIX, read it correctly and appended into a SECOND
/// directory beside the first. Two physical spellings, one logical partition value. So the directory name
/// is cosmetic, and a table written half in one mode and half in the other is correct, merely untidy.</para>
///
/// <para><b>This is a writer-local option and NOT a table property.</b> Delta defines no property for it,
/// so persisting one would be a non-standard field every other engine ignores while implying a
/// table-wide guarantee EW cannot enforce.</para>
/// </summary>
public enum PartitionPathSpelling
{
    /// <summary>
    /// <para><b>Promise: byte-identical to what Spark would write.</b> Reproduces
    /// <c>ExternalCatalogUtils.escapePathName</c>, INCLUDING Spark's own platform branch — Spark escapes
    /// <c>' '</c>, <c>'&lt;'</c>, <c>'&gt;'</c> and <c>'|'</c> under <c>if (Shell.WINDOWS)</c> and not
    /// otherwise — except that the branch is taken from the constraints the TARGET STORAGE declares rather
    /// than from the operating system of the writing process. Those coincide in the case Spark actually
    /// cares about (a local volume on Windows) and differ where using the process OS is simply wrong: a
    /// Windows process writing to S3 escapes nothing extra here, because S3 has no such restriction.</para>
    ///
    /// <para><b>What this mode does NOT promise.</b> The spelling varies with the storage, so a table on a
    /// Win32-constrained volume and the same data on S3 do not have the same directory names. And a
    /// constraint the backend declares which Spark does not honour is NOT honoured here either — most
    /// visibly <see cref="EngineeredWood.IO.PathNameConstraints.NoTrailingDot"/>, which Azure Blob
    /// documents and Win32 enforces by silently stripping the dot, and which no implementation surveyed
    /// (Spark, delta-rs, delta-kernel-rs) escapes. Matching Spark means inheriting that. Use
    /// <see cref="Portable"/> if the table must be safe rather than identical.</para>
    /// </summary>
    SparkCompatible = 0,

    /// <summary>
    /// <para><b>Promise: one spelling on every platform, legal on every backend.</b> Escapes everything
    /// that is not RFC 3986 <i>unreserved</i> (<c>A-Z a-z 0-9 - . _ ~</c>), which is the rule delta-rs
    /// applies unconditionally, PLUS a <c>.</c> in final position. The result satisfies every constraint in
    /// <see cref="EngineeredWood.IO.PathNameConstraints"/>, so a tree written this way can be copied
    /// between backends — including onto a Win32 volume — without a name changing meaning.</para>
    ///
    /// <para>The trailing dot is the part no character set can express, and the reason this mode is not
    /// simply "what delta-rs does": <c>.</c> is RFC 3986 unreserved and perfectly legal mid-name, so
    /// delta-rs leaves it alone and a value ending in one still produces a directory Win32 silently
    /// renames. Escaping only the final dot also resolves a component that is exactly <c>.</c> or
    /// <c>..</c>, which would otherwise read as relative-path navigation rather than naming anything.</para>
    ///
    /// <para><b>Cost.</b> The directory name diverges from Spark's for very ordinary values — a space,
    /// <c>+</c>, <c>=</c> and <c>&amp;</c> all get escaped where Spark on POSIX leaves them — so exact
    /// comparison against a Spark-written tree no longer holds. delta-rs already pays this cost, which is
    /// evidence the ecosystem tolerates it: the spelling is cosmetic. Non-ASCII is left LITERAL, since it
    /// is legal on all four backends; this mode is about legality, not about matching delta-rs, which
    /// percent-encodes non-ASCII as UTF-8 bytes.</para>
    /// </summary>
    Portable = 1,
}
