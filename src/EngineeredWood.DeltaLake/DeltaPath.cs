// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text;

namespace EngineeredWood.DeltaLake;

/// <summary>
/// Path escaping for Delta tables. Two DISTINCT layers exist (both matching Spark):
/// <list type="number">
/// <item><see cref="EscapePathName(string)"/> — Hive-style escaping applied to a partition VALUE when
/// building the partition directory name on disk (escapes <c>/</c>, <c>:</c>, <c>%</c>, … as <c>%XX</c>).
/// This is what makes the physical directory name filesystem-safe, and its escape set is
/// PLATFORM-DEPENDENT — see the remarks there.</item>
/// <item><see cref="Encode"/>/<see cref="Decode"/> — the <c>add.path</c> field in the transaction log is the
/// URL-encoded (RFC 2396) form of the on-disk relative path, so a literal <c>%</c> from layer 1 becomes
/// <c>%25</c> and a space becomes <c>%20</c>. Readers (Spark, delta-kernel, delta-rs) URL-DECODE
/// <c>add.path</c> before opening the file.</item>
/// </list>
/// </summary>
public static class DeltaPath
{
    /// <summary>
    /// True when <see cref="EscapePathName(string)"/> should apply Spark's Windows-only additions to the
    /// escape table. Cached rather than re-tested because it gates a per-character branch.
    /// </summary>
    private static readonly bool WindowsEscapeRules =
#if NET5_0_OR_GREATER
        OperatingSystem.IsWindows();
#else
        System.Runtime.InteropServices.RuntimeInformation.IsOSPlatform(
            System.Runtime.InteropServices.OSPlatform.Windows);
#endif

    /// <summary>
    /// Escapes a partition value for use as a directory name, matching Spark's
    /// <c>ExternalCatalogUtils.escapePathName</c>: control characters and <c>"#%'*/:=?\{[]^</c> become
    /// <c>%XX</c>, and on Windows also <c>' '</c>, <c>'&lt;'</c>, <c>'&gt;'</c> and <c>'|'</c>.
    /// </summary>
    /// <remarks>
    /// <para><b>The escape set is platform-dependent because Spark's is.</b> Spark's
    /// <c>charToEscape</c> bit set is Hive's, plus — under <c>if (Shell.WINDOWS)</c> — <c>' '</c>,
    /// <c>'&lt;'</c>, <c>'&gt;'</c> and <c>'|'</c>, which are exactly the characters Win32 rejects in a
    /// path component that the Hive-inherited set leaves alone. The same value therefore yields
    /// <c>region=a b</c> on Linux and <c>region=a%20b</c> on Windows, and matching the reference
    /// implementation MEANS branching here rather than picking one set for every platform.
    /// </para>
    ///
    /// <para>MEASURED against pyspark 4.0.1 / delta-spark 4.0.0 on Windows 11 by enumerating
    /// <c>escapePathName</c> over the whole ASCII range. Hive's own <c>FileUtils.escapePathName</c>,
    /// called from the SAME JVM, does not add the four — so the branch is Spark's own, not something
    /// inherited from Hadoop. delta-rs escapes all four on every platform, so on Windows all three
    /// implementations agree on the directory name.</para>
    ///
    /// <para>Escaping the four is also what makes a trailing space writable at all: Win32 silently strips
    /// a trailing space from a path component, so <c>region=a b </c> creates <c>region=a b</c> and then
    /// fails to open the file underneath it, leaving a stray directory behind. <c>region=a%20b%20</c> has
    /// no such problem.</para>
    ///
    /// <para>The physical directory name is not a durable interop key in any case. MEASURED: Spark on
    /// Windows reads a table whose partition directories were written on POSIX, and appends to it in a
    /// SECOND directory beside the first — two physical names, one logical partition value, both read
    /// back correctly. Readers resolve files through <c>add.path</c>, never by parsing directory names.</para>
    ///
    /// <para>Two deliberate one-character notes. <c>'}'</c> is NOT escaped: Spark's list is
    /// <c>{ [ ] ^</c> with no closing brace, and EW escaped it until the enumeration above said
    /// otherwise. <c>'\0'</c> IS escaped even though Spark's table starts at <c></c> — no filesystem
    /// can hold a NUL in a name, so there is no Spark-written table to diverge from, and escaping keeps
    /// such a value writable rather than fatal.</para>
    /// </remarks>
    public static string EscapePathName(string value) => EscapePathName(value, WindowsEscapeRules);

    /// <summary>
    /// <see cref="EscapePathName(string)"/> with the platform gate supplied explicitly, so tests can pin
    /// BOTH escape sets whichever machine they happen to run on.
    /// </summary>
    internal static string EscapePathName(string value, bool windowsRules)
    {
        StringBuilder? sb = null;
        for (int i = 0; i < value.Length; i++)
        {
            char c = value[i];
            if (NeedsPathEscaping(c, windowsRules))
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

    private static bool NeedsPathEscaping(char c, bool windowsRules) => c switch
    {
        < ' ' or '\u007f' => true,
        '"' or '#' or '%' or '\'' or '*' or '/' or ':' or '=' or '?' or '\\' => true,
        '{' or '[' or ']' or '^' => true,
        ' ' or '<' or '>' or '|' => windowsRules,
        _ => false,
    };

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
    /// <para>Most of this set is unreachable from a partition directory because
    /// <see cref="EscapePathName(string)"/> escaped it first — but <c>'}'</c> and <c>'`'</c> are NOT in
    /// layer 1's table on any platform, and <c>' '</c>, <c>'&lt;'</c>, <c>'&gt;'</c>, <c>'|'</c> are not
    /// in it on POSIX. Those six reach layer 2 literally, and are why this set is wider than the four
    /// characters it used to hold.</para>
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
    /// <returns>The directory fragment without a trailing separator, or the empty string when there are no
    /// partition values.</returns>
    public static string BuildPartitionPath(IReadOnlyDictionary<string, string> partitionValues)
    {
        if (partitionValues.Count == 0)
            return "";

        return string.Join("/",
            partitionValues.Select(kv =>
                $"{EscapePathName(kv.Key)}={(kv.Value is null ? "__HIVE_DEFAULT_PARTITION__" : EscapePathName(kv.Value))}"));
    }
}
