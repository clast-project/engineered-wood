// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text;
using EngineeredWood.IO;

namespace EngineeredWood.DeltaLake;

/// <summary>
/// Path escaping for Delta tables. Two DISTINCT layers exist, and confusing them is easy because both
/// produce <c>%XX</c>:
/// <list type="number">
/// <item><see cref="EscapePathName"/> — Hive-style escaping applied to a partition VALUE to DERIVE the
/// directory name on disk. Nothing inverts it: readers take partition values from
/// <c>add.partitionValues</c>, never by parsing a directory name. So the spelling is a local choice, and
/// <see cref="PartitionPathSpelling"/> says which promise EW keeps when making it.</item>
/// <item><see cref="Encode"/>/<see cref="Decode"/> — a reversible CODEC between the on-disk relative path
/// and the <c>add.path</c> field, which is its URL-encoded (RFC 2396) form: a literal <c>%</c> from layer 1
/// becomes <c>%25</c> and a space becomes <c>%20</c>. This one is not a local choice. Every reader decodes
/// <c>add.path</c> to find the file, so writer and reader must agree or the file is not found — and Spark
/// rejects a malformed one outright rather than mis-resolving it.</item>
/// </list>
/// </summary>
public static class DeltaPath
{
    /// <summary>
    /// <para>Escapes one partition-path COMPONENT TAIL — a column name or a partition value — for use in a
    /// Hive-style directory name, under the given <paramref name="spelling"/>.</para>
    ///
    /// <para>"Component tail" matters for exactly one rule: <see cref="PartitionPathSpelling.Portable"/>
    /// escapes a <c>.</c> in final position, which is load-bearing when <paramref name="value"/> ends the
    /// directory component — the usual case, since <see cref="BuildPartitionPath"/> emits
    /// <c>name=value</c> — and merely redundant when it does not.</para>
    /// </summary>
    /// <param name="value">The column name or partition value to escape.</param>
    /// <param name="spelling">Which promise to keep — see <see cref="PartitionPathSpelling"/>.</param>
    /// <param name="storageConstraints">
    /// What the TARGET STORAGE cannot hold, from <see cref="ITableFileSystem.PathConstraints"/>. Read only
    /// under <see cref="PartitionPathSpelling.SparkCompatible"/>, where it stands in for Spark's
    /// <c>Shell.WINDOWS</c> test; <see cref="PartitionPathSpelling.Portable"/> satisfies every constraint
    /// already and so ignores it.
    /// </param>
    /// <remarks>
    /// <para><b>Why the storage and not the process.</b> Spark's <c>charToEscape</c> is Hive's bit set plus,
    /// under <c>if (Shell.WINDOWS)</c>, <c>' '</c>, <c>'&lt;'</c>, <c>'&gt;'</c> and <c>'|'</c> — exactly
    /// the characters Win32 rejects in a path component that the Hive-inherited set leaves alone. MEASURED
    /// against pyspark 4.0.1 / delta-spark 4.0.0 by enumerating <c>escapePathName</c> over the whole ASCII
    /// range; Hive's own <c>FileUtils.escapePathName</c> called from the SAME JVM does not add the four, so
    /// the branch is Spark's own rather than inherited from Hadoop. EW takes that branch from the storage
    /// instead, because the process OS is only a proxy for it and is wrong in both directions: a Windows
    /// process writing to S3 would escape characters S3 accepts (MEASURED — all three object stores
    /// round-trip <c>&lt; &gt; |</c> and a trailing space byte-identically, with correct content), and a
    /// POSIX process writing to a mounted NTFS or SMB volume would escape nothing and produce a table
    /// Windows cannot open.</para>
    ///
    /// <para>Escaping the four is also what makes a trailing space writable at all: Win32 silently strips a
    /// trailing space from a path component, so <c>region=a b </c> creates <c>region=a b</c> and then fails
    /// to open the file underneath it, leaving a stray directory behind. <c>region=a%20b%20</c> has no such
    /// problem.</para>
    ///
    /// <para>Two deliberate one-character notes on the Hive-inherited set.
    /// <c>'}'</c> is NOT escaped under <see cref="PartitionPathSpelling.SparkCompatible"/>: Spark's list is
    /// <c>{ [ ] ^</c> with no closing brace, and EW escaped it until the enumeration above said otherwise.
    /// (delta-rs does escape it — but for its own broader rule, not for Spark parity, which is why
    /// <see cref="PartitionPathSpelling.Portable"/> escapes it and this mode does not.) And <c>'\0'</c> IS
    /// escaped even though Spark's table starts at 0x01: no filesystem can hold a NUL in a name, so there
    /// is no Spark-written table to diverge from, and escaping keeps such a value writable rather than
    /// fatal.</para>
    /// </remarks>
    public static string EscapePathName(
        string value, PartitionPathSpelling spelling, PathNameConstraints storageConstraints)
    {
        // Spark's Shell.WINDOWS, resolved from the target storage rather than from the running process.
        bool win32Rules = (storageConstraints & PathNameConstraints.Win32ReservedCharacters) != 0;

        StringBuilder? sb = null;
        for (int i = 0; i < value.Length; i++)
        {
            char c = value[i];
            if (NeedsPathEscaping(c, spelling, win32Rules, isLastCharacter: i == value.Length - 1))
            {
                sb ??= new StringBuilder(value.Length + 8).Append(value, 0, i);
                sb.Append('%').Append(((int)c).ToString("X2", System.Globalization.CultureInfo.InvariantCulture));
            }
            else
            {
                sb?.Append(c);
            }
        }
        return sb?.ToString() ?? value;
    }

    private static bool NeedsPathEscaping(
        char c, PartitionPathSpelling spelling, bool win32Rules, bool isLastCharacter)
    {
        if (spelling == PartitionPathSpelling.Portable)
        {
            // Everything that is not RFC 3986 "unreserved" — delta-rs's rule — plus the one positional
            // case no character set can express. Non-ASCII stays literal: it is legal on every backend
            // measured, and this mode promises legality, not delta-rs byte-parity.
            if (c >= '\u0080')
                return false;
            if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9'))
                return false;
            return c switch
            {
                // A trailing '.' is stripped by Win32 and barred by Azure Blob. Escaping it in final
                // position also stops a component that is exactly "." or ".." reading as navigation.
                '.' => isLastCharacter,
                '-' or '_' or '~' => false,
                _ => true,
            };
        }

        return c switch
        {
            < ' ' or '\u007f' => true,
            '"' or '#' or '%' or '\'' or '*' or '/' or ':' or '=' or '?' or '\\' => true,
            '{' or '[' or ']' or '^' => true,
            ' ' or '<' or '>' or '|' => win32Rules,
            _ => false,
        };
    }

    /// <summary>
    /// URL-encodes an on-disk relative path into the <c>add.path</c> log form: control characters and
    /// <c>&#32;"#%&lt;&gt;?[]^`{|}</c> become <c>%XX</c>; <c>/</c>, non-ASCII and everything else stay
    /// literal.
    /// </summary>
    /// <remarks>
    /// <para>MEASURED, not derived: Spark's layer 2 is <c>new Path(rel).toUri().toString()</c>, i.e. Java
    /// URI quoting of the ASCII characters illegal in a URI path — and <c>toString()</c> rather than
    /// <c>toASCIIString()</c>, which is why <c>café</c> and <c>日本</c> stay literal here while delta-rs
    /// percent-encodes them as UTF-8 bytes. Enumerating <c>Path.toUri().toString()</c> over the ASCII
    /// range on pyspark 4.0.1 reproduces every <c>add.path</c> observed from real Delta writes, including
    /// the <c>%</c> → <c>%25</c> double-encoding of whatever layer 1 already escaped.</para>
    ///
    /// <para><b>Under-escaping here is a correctness bug, and it fails the whole table rather than one
    /// file.</b> It is invisible from inside EW: every character except <c>%</c> decodes to itself, so
    /// <see cref="Decode"/> recovers the right name regardless, and <c>%</c> — the one character where
    /// under-escaping resolves to the WRONG file — was covered even by the four-character set this
    /// replaced. Spark is not so forgiving. MEASURED on pyspark 4.0.1 / delta-spark 4.0.0: Delta passes
    /// <c>add.path</c> through <c>new URI(…)</c> inside its <c>CanonicalPathFunction</c> UDF, so one
    /// literal URI-illegal character raises <c>java.net.URISyntaxException: Illegal character in path</c>
    /// and the read of the ENTIRE TABLE fails with <c>FAILED_READ_FILE</c> — no partial result, no
    /// skipped file.</para>
    ///
    /// <para>Which characters reach layer 2 literally depends on the layer-1 spelling, so this set is
    /// deliberately the WHOLE of Spark's rather than the subset a given mode happens to leave through.
    /// Under <see cref="PartitionPathSpelling.SparkCompatible"/> that subset is <c>'}'</c> and <c>'`'</c>
    /// on any storage, plus <c>' '</c>, <c>'&lt;'</c>, <c>'&gt;'</c> and <c>'|'</c> on storage without
    /// <see cref="PathNameConstraints.Win32ReservedCharacters"/>. Under
    /// <see cref="PartitionPathSpelling.Portable"/> nothing reaches it but <c>%</c>.</para>
    ///
    /// <para>Two characters Hadoop treats specially are deliberately absent. It REJECTS a literal
    /// <c>':'</c> outright (it reads as a URI scheme) and rewrites <c>'\'</c> to <c>'/'</c> rather than
    /// escaping it, so there is no Spark encoding to match for either. Neither can arrive here in any
    /// case: layer 1 escapes both, and EW composes relative paths with <c>'/'</c>.</para>
    /// </remarks>
    public static string Encode(string fsRelativePath)
    {
        StringBuilder? sb = null;
        for (int i = 0; i < fsRelativePath.Length; i++)
        {
            char c = fsRelativePath[i];
            bool escape = c is ' ' or '"' or '#' or '%' or '<' or '>' or '?'
                or '[' or ']' or '^' or '`' or '{' or '|' or '}' or < ' ' or '\u007f';
            if (escape)
            {
                sb ??= new StringBuilder(fsRelativePath.Length + 8).Append(fsRelativePath, 0, i);
                sb.Append('%').Append(((int)c).ToString("X2", System.Globalization.CultureInfo.InvariantCulture));
            }
            else
            {
                sb?.Append(c);
            }
        }
        return sb?.ToString() ?? fsRelativePath;
    }

    /// <summary>Decodes an <c>add.path</c> log value into the on-disk relative path.</summary>
    public static string Decode(string logPath) =>
        logPath.IndexOf('%') < 0 ? logPath : Uri.UnescapeDataString(logPath);

    /// <summary>
    /// Builds the Hive-style partition directory a file with these partition values belongs under
    /// (<c>date=2024-01-01/region=us</c>), each component escaped by <see cref="EscapePathName"/> — layer 1.
    /// The result is an ON-DISK relative path fragment; <see cref="Encode"/> it before writing it into
    /// <c>add.path</c>.
    ///
    /// <para>A null value becomes Hive's <c>__HIVE_DEFAULT_PARTITION__</c> sentinel, which is how the Delta
    /// spec and Spark both spell "this partition column is null" in a directory name.</para>
    ///
    /// <para>Components come out in the ENUMERATION order of
    /// <paramref name="partitionValues"/>, so a caller that needs them in the table's declared partition
    /// order must supply a dictionary built in that order.</para>
    /// </summary>
    /// <param name="partitionValues">Partition column names to values, in the table's partition order.</param>
    /// <param name="spelling">Which promise to keep — see <see cref="PartitionPathSpelling"/>.</param>
    /// <param name="storageConstraints">
    /// What the target storage cannot hold, from <see cref="ITableFileSystem.PathConstraints"/>. Pass the
    /// constraints of the filesystem the table is actually being written to; passing
    /// <see cref="PathNameConstraints.None"/> under <see cref="PartitionPathSpelling.SparkCompatible"/>
    /// produces the spelling Spark uses on POSIX, which a Win32 volume may be unable to hold.
    /// </param>
    /// <returns>The directory fragment without a trailing separator, or the empty string when there are no
    /// partition values.</returns>
    public static string BuildPartitionPath(
        IReadOnlyDictionary<string, string> partitionValues,
        PartitionPathSpelling spelling,
        PathNameConstraints storageConstraints)
    {
        if (partitionValues.Count == 0)
            return "";

        return string.Join("/",
            partitionValues.Select(kv =>
                $"{EscapePathName(kv.Key, spelling, storageConstraints)}=" +
                (kv.Value is null
                    ? "__HIVE_DEFAULT_PARTITION__"
                    : EscapePathName(kv.Value, spelling, storageConstraints))));
    }
}
